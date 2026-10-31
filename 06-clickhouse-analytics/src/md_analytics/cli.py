"""Command line entry point for the make targets of section 19.3.

Every subcommand prints JSON to stdout and uses the exit code as the verdict, so
a CI job can gate on the code and keep the document as an artefact:

    0  PASS
    1  FAIL
    2  usage or configuration error
    3  NO_GO       -- refused to publish; often the correct behaviour
    4  INCONCLUSIVE -- not proven either way, which is not a pass

The distinct code for ``NO_GO`` matters. A pipeline that correctly refuses a
conflicting snapshot should not look identical to one that crashed, and a job
that treats every non-zero code as a crash will eventually be "fixed" by
removing the refusal.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from .benchmark import (
    CV_OPTIMIZED_P95_MS,
    WorkloadSpec,
    report_template,
    run_query_benchmark,
    verdict_for_target,
)
from .build_catalog import NO_ACTIVE_BUILD, BuildCatalog
from .config import load_config
from .errors import MdAnalyticsError, Verdict
from .fixtures import golden_six_event, synthetic_corpus
from .metrics import counters_to_alert_inputs, evaluate_alerts, release_gate_verdict
from .pipeline import run_build
from .query_service import QueryService
from .schema_preflight import (
    ColumnSpec,
    EXPECTED_RAW_COLUMNS,
    expected_canonical_digest,
    expected_raw_digest,
    preflight_raw,
)
from .sqlbank import SqlBank
from .store.base import AnalyticsStore
from .store.model import ModelStore
from .ttl_controller import evaluate_retention_gate

EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_USAGE = 2
EXIT_NO_GO = 3
EXIT_INCONCLUSIVE = 4

_EXIT_BY_VERDICT = {
    Verdict.PASS: EXIT_PASS,
    Verdict.FAIL: EXIT_FAIL,
    Verdict.NO_GO: EXIT_NO_GO,
    Verdict.INCONCLUSIVE: EXIT_INCONCLUSIVE,
    Verdict.INVALID: EXIT_FAIL,
    Verdict.NOT_RUN: EXIT_INCONCLUSIVE,
}


def _emit(payload: dict[str, Any]) -> None:
    json.dump(payload, sys.stdout, indent=2, sort_keys=True, ensure_ascii=False)
    sys.stdout.write("\n")


def _open_store(args: argparse.Namespace) -> AnalyticsStore:
    """Pick a backend, defaulting to the model and saying which was used.

    ``--clickhouse-url`` (or ``P06_CLICKHOUSE_URL``) selects the real server. The
    default is the model, and every command prints the backend and evidence class
    so a reader of the output never has to guess which one produced it.
    """
    url = getattr(args, "clickhouse_url", None) or os.environ.get("P06_CLICKHOUSE_URL")
    if not url:
        store = ModelStore()
        store.setup()
        return store
    from .store.clickhouse import ClickHouseHttpClient, ClickHouseStore

    client = ClickHouseHttpClient(
        url,
        user=os.environ.get("P06_CLICKHOUSE_USER", "default"),
        password=os.environ.get("P06_CLICKHOUSE_PASSWORD", ""),
    )
    store = ClickHouseStore(client)
    store.setup()
    return store


# --------------------------------------------------------------- subcommands


def cmd_schema_check(args: argparse.Namespace) -> int:
    """Print the pinned schema digests, or check a live table against them."""
    payload: dict[str, Any] = {
        "expected_raw_digest": expected_raw_digest(),
        "expected_canonical_digest": expected_canonical_digest(),
        "sql_digests": SqlBank().digests(),
        "expected_raw_columns": [
            {"name": c.name, "type": c.type, "position": c.position}
            for c in EXPECTED_RAW_COLUMNS
        ],
    }
    if not args.observed:
        payload["verdict"] = str(Verdict.INCONCLUSIVE)
        payload["note"] = (
            "no live schema supplied: this is the pinned expectation only, not a "
            "check of any server"
        )
        _emit(payload)
        return EXIT_INCONCLUSIVE

    observed = json.loads(Path(args.observed).read_text(encoding="utf-8"))
    columns = [
        ColumnSpec(c["name"], c["type"], int(c["position"])) for c in observed["columns"]
    ]
    try:
        digest = preflight_raw(
            columns=columns,
            engine=observed["engine"],
            pinned_digest=args.pinned_digest,
        )
    except MdAnalyticsError as exc:
        payload["verdict"] = str(Verdict.NO_GO)
        payload["error"] = exc.as_dict()
        _emit(payload)
        return EXIT_NO_GO
    payload["verdict"] = str(Verdict.PASS)
    payload["observed_digest"] = digest
    _emit(payload)
    return EXIT_PASS


def cmd_fixtures_generate(args: argparse.Namespace) -> int:
    corpus = (
        golden_six_event(args.dataset_id).corpus
        if args.golden
        else synthetic_corpus(
            dataset_id=args.dataset_id,
            symbols=args.symbols,
            ticks_per_symbol=args.ticks_per_symbol,
            quote_every=args.quote_every,
            duplicate_every=args.duplicate_every,
        )
    )
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "manifest.json").write_text(
        json.dumps(
            {
                "dataset_id": corpus.manifest.dataset_id,
                "declared_sha256": corpus.manifest.declared_sha256,
                "generator_version": corpus.manifest.generator_version,
                "accepted": [
                    {
                        "identity": list(e.identity),
                        "event_kind": e.event_kind,
                        "payload_digest": e.payload_digest,
                    }
                    for e in corpus.manifest.accepted
                ],
                "rejected": [[list(i), r] for i, r in corpus.manifest.rejected],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (out / "snapshot.json").write_text(
        json.dumps(corpus.cut.as_dict(), indent=2, sort_keys=True), encoding="utf-8"
    )
    with (out / "raw_rows.jsonl").open("w", encoding="utf-8") as handle:
        for row in corpus.raw_rows:
            handle.write(json.dumps(row.as_dict(), sort_keys=True) + "\n")

    _emit(
        {
            "verdict": str(Verdict.PASS),
            "out": str(out),
            "dataset_id": corpus.dataset_id,
            "accepted": len(corpus.manifest.accepted),
            "raw_rows": len(corpus.raw_rows),
            "manifest_sha256": corpus.cut.manifest_sha256,
            "cut_digest": corpus.cut.cut_digest(),
        }
    )
    return EXIT_PASS


def cmd_build_from_raw(args: argparse.Namespace) -> int:
    """Build from the real P05 landing table rather than a synthetic corpus.

    The difference from ``build`` is the input, and it is the whole point: the
    rows come from ``md_ingest.market_events_raw``, the expected set comes from
    P03's accepted-event ledger and the bound comes from P05's cut. All three
    are produced by components upstream of the table being audited, so a missing
    row is visible instead of agreeing with itself.
    """
    from .raw_source import (
        accepted_from_p03_handoff,
        corpus_from_raw,
        cut_from_p05_snapshot,
        load_raw_rows,
        RawSourceError,
    )

    store = _open_store(args)
    info = store.info()
    # The model backend has no raw table to read, so this command would be
    # auditing a corpus against itself. Matched on the prefix because the real
    # backend names its transport -- it reports "clickhouse-http", and an exact
    # comparison against "clickhouse" rejected the very server it requires.
    if not info.backend.startswith("clickhouse"):
        print(
            "refused: build-from-raw needs the real server; pass --clickhouse-url "
            "(backend was %s)" % info.backend
        )
        return 2

    try:
        accepted = accepted_from_p03_handoff(args.p03_handoff)
        cut = cut_from_p05_snapshot(
            args.p05_cut,
            dataset_id=args.dataset_id,
            manifest_sha256=args.manifest_sha256 or "",
        )
        rows = load_raw_rows(
            store.client, dataset_id=args.dataset_id, table=args.raw_table
        )
        corpus = corpus_from_raw(
            dataset_id=args.dataset_id,
            raw_rows=rows,
            accepted=accepted,
            cut=cut,
            declared_sha256=args.manifest_sha256,
        )
    except RawSourceError as exc:
        print("refused: %s" % exc)
        return 2

    catalog = BuildCatalog(Path(args.catalog))
    result = run_build(
        store,
        corpus,
        catalog=catalog,
        block_size=args.block_size,
        publish=not args.no_publish,
        expected_current=catalog.active_build_id() or NO_ACTIVE_BUILD,
    )
    alerts = evaluate_alerts(counters_to_alert_inputs(result.counters()))
    payload = result.as_dict()
    payload["backend"] = info.backend
    payload["evidence_class"] = info.evidence_class
    payload["server_version"] = info.server_version
    payload["alerts"] = [a.as_dict() for a in alerts]
    payload["alert_gate"] = str(release_gate_verdict(alerts))
    payload["source"] = {
        "raw_table": args.raw_table,
        "raw_rows_read": len(rows),
        "accepted_events": len(accepted),
        "cut_partitions": len(cut.cuts),
    }
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if result.published else 1


def cmd_build(args: argparse.Namespace) -> int:
    store = _open_store(args)
    catalog = BuildCatalog(Path(args.catalog))
    corpus = (
        golden_six_event(args.dataset_id).corpus
        if args.golden
        else synthetic_corpus(
            dataset_id=args.dataset_id,
            symbols=args.symbols,
            ticks_per_symbol=args.ticks_per_symbol,
            quote_every=args.quote_every,
            duplicate_every=args.duplicate_every,
        )
    )
    expected = catalog.active_build_id() or NO_ACTIVE_BUILD
    result = run_build(
        store,
        corpus,
        catalog=catalog,
        block_size=args.block_size,
        publish=not args.no_publish,
        expected_current=expected,
    )
    info = store.info()
    alerts = evaluate_alerts(counters_to_alert_inputs(result.counters()))
    payload = result.as_dict()
    payload["backend"] = info.backend
    payload["evidence_class"] = info.evidence_class
    payload["server_version"] = info.server_version
    payload["alerts"] = [a.as_dict() for a in alerts]
    payload["alert_gate"] = str(release_gate_verdict(alerts))
    _emit(payload)
    return _EXIT_BY_VERDICT[result.verdict]


def cmd_query_bars(args: argparse.Namespace) -> int:
    store = _open_store(args)
    catalog = BuildCatalog(Path(args.catalog))
    config = load_config(args.config) if args.config else None
    service = QueryService(
        store,
        catalog,
        limits=config.query_limits() if config else None,
        symbol_allowlist=args.symbols or None,
    )
    try:
        response = service.get_bars(
            build_id=args.build_id or catalog.active_build_id(),
            dataset_id=args.dataset_id,
            symbol=args.symbol,
            from_ns=args.from_ns,
            to_ns=args.to_ns,
            limit=args.limit,
        )
    except MdAnalyticsError as exc:
        _emit({"verdict": str(Verdict.NO_GO), "error": exc.as_dict()})
        return EXIT_NO_GO
    _emit({"verdict": str(Verdict.PASS), **response.as_dict()})
    return EXIT_PASS


def cmd_retention_gate(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    policy = config.retention_policy()
    if args.restore_drill_passed:
        policy = type(policy)(
            raw_ttl_days=policy.raw_ttl_days,
            recovery_window_days=policy.recovery_window_days,
            audit_window_days=policy.audit_window_days,
            rebuild_window_days=policy.rebuild_window_days,
            late_arrival_window_days=policy.late_arrival_window_days,
            restore_drill_passed=True,
            backup_age_days=args.backup_age_days,
            backup_max_age_days=policy.backup_max_age_days,
            enabled=policy.enabled,
        )
    gate = evaluate_retention_gate(policy)
    _emit({"config_sha": config.sha256(), **gate.as_dict()})
    return _EXIT_BY_VERDICT[gate.verdict()]


def cmd_benchmark(args: argparse.Namespace) -> int:
    """Measure the bar query over a freshly built corpus.

    Reports with ``measured_on_new_profile`` unless the caller asserts the whole
    reference environment is in place, which on a laptop it is not. Section 12 is
    explicit that the CV figures cannot be reproduced without the original
    hardware and SQL, so the default is the honest label.
    """
    store = _open_store(args)
    catalog = BuildCatalog(Path(args.catalog))
    corpus = synthetic_corpus(
        dataset_id=args.dataset_id,
        symbols=args.symbols,
        ticks_per_symbol=args.ticks_per_symbol,
    )
    result = run_build(
        store,
        corpus,
        catalog=catalog,
        block_size=args.block_size,
        expected_current=catalog.active_build_id() or NO_ACTIVE_BUILD,
    )
    if result.build_id is None:
        _emit({"verdict": str(result.verdict), "reasons": result.reasons})
        return _EXIT_BY_VERDICT[result.verdict]

    build_id = result.build_id
    symbol = args.symbols[0]
    bars = sorted(k.bar_start_ns for k in result.stored_bars if k.symbol == symbol)
    start_ns, end_ns = bars[0], bars[-1] + 60_000_000_000

    def run_once() -> int:
        return len(
            store.select_bars_1m(
                build_id=build_id,
                dataset_id=args.dataset_id,
                symbol=symbol,
                start_ns=start_ns,
                end_ns=end_ns,
                limit=args.limit,
            )
        )

    measurement = run_query_benchmark(
        WorkloadSpec("ohlc_1m", run_once, "1m OHLC over the whole build"),
        repetitions=args.repetitions,
    )
    info = store.info()
    verdict, reason = verdict_for_target(
        measurement,
        target_p95_ms=args.target_p95_ms,
        correctness_verdict=result.verdict,
        dataset_unique_rows=len(result.canonical),
        required_unique_rows=args.required_unique_rows,
        same_profile_as_reference=False,
        evidence_class=info.evidence_class,
    )
    _emit(
        {
            "verdict": str(verdict),
            "reason": reason,
            "backend": info.backend,
            "evidence_class": info.evidence_class,
            "server_version": info.server_version,
            "correctness_verdict": str(result.verdict),
            "measurement": measurement.as_dict(),
            "report": report_template(
                status=str(verdict),
                build_id=build_id,
                dataset_unique_events=len(result.canonical),
                canonical_unique_ids=len(result.canonical),
                bar_count=result.bars.stored_bars if result.bars else "TO_FILL",
                bar_tick_count=result.bars.stored_tick_total if result.bars else "TO_FILL",
                workload_sql_sha256=SqlBank().sha256("bars_1m"),
                p95_client_query_ms=round(measurement.warm.p95_ms, 3),
                p99_client_query_ms=round(measurement.warm.p99_ms, 3),
                query_error_rate=measurement.error_rate,
                notes=reason,
            ),
        }
    )
    return _EXIT_BY_VERDICT[verdict]


# ------------------------------------------------------------------- parsing


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="md-analytics",
        description="Project 06 -- ClickHouse analytics and 1m OHLC bars",
    )
    parser.add_argument(
        "--clickhouse-url",
        default=None,
        help="use a real ClickHouse server (default: the in-process model, whose "
        "results are evidence about this pipeline and not about ClickHouse)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_corpus_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--dataset-id", default="DEV-01")
        p.add_argument("--golden", action="store_true", help="use the section 16.1 corpus")
        p.add_argument("--symbols", nargs="+", default=["EURUSD"])
        p.add_argument("--ticks-per-symbol", type=int, default=1_000)
        p.add_argument("--quote-every", type=int, default=0)
        p.add_argument("--duplicate-every", type=int, default=0)

    p = sub.add_parser("schema-check", help="print or verify pinned schema digests")
    p.add_argument("--observed", default=None, help="JSON file with columns and engine")
    p.add_argument("--pinned-digest", default=None)
    p.set_defaults(func=cmd_schema_check)

    p = sub.add_parser("fixtures-generate", help="write a corpus to disk")
    add_corpus_args(p)
    p.add_argument("--out", required=True)
    p.set_defaults(func=cmd_fixtures_generate)

    p = sub.add_parser("build", help="audit, stage, verify and publish a build")
    add_corpus_args(p)
    p.add_argument("--catalog", required=True)
    p.add_argument("--block-size", type=int, default=5_000)
    p.add_argument("--no-publish", action="store_true")
    p.set_defaults(func=cmd_build)

    p = sub.add_parser(
        "build-from-raw",
        help="audit, stage, verify and publish from the real P05 raw table",
    )
    p.add_argument("--catalog", required=True)
    p.add_argument("--dataset-id", required=True)
    p.add_argument(
        "--p03-handoff", required=True,
        help="P03 p04_stub NDJSON: the accepted-event ledger")
    p.add_argument(
        "--p05-cut", required=True,
        help="P05 snapshot JSON: the sealed topic-partition cut")
    p.add_argument("--raw-table", default="md_ingest.market_events_raw")
    p.add_argument("--manifest-sha256", default=None)
    p.add_argument("--block-size", type=int, default=5_000)
    p.add_argument("--no-publish", action="store_true")
    p.set_defaults(func=cmd_build_from_raw)

    p = sub.add_parser("query-bars", help="read 1m bars from a READY build")
    p.add_argument("--catalog", required=True)
    p.add_argument("--config", default=None)
    p.add_argument("--build-id", default=None)
    p.add_argument("--dataset-id", required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--symbols", nargs="*", default=None, help="symbol allowlist")
    p.add_argument("--from-ns", required=True)
    p.add_argument("--to-ns", required=True)
    p.add_argument("--limit", type=int, default=None)
    p.set_defaults(func=cmd_query_bars)

    p = sub.add_parser("retention-gate", help="may TTL be enabled? (NFR-11)")
    p.add_argument("--config", required=True)
    p.add_argument("--restore-drill-passed", action="store_true")
    p.add_argument("--backup-age-days", type=int, default=None)
    p.set_defaults(func=cmd_retention_gate)

    p = sub.add_parser("benchmark", help="measure the 1m bar query")
    add_corpus_args(p)
    p.add_argument("--catalog", required=True)
    p.add_argument("--block-size", type=int, default=5_000)
    p.add_argument("--repetitions", type=int, default=200)
    p.add_argument("--limit", type=int, default=10_000)
    p.add_argument("--target-p95-ms", type=float, default=CV_OPTIMIZED_P95_MS)
    p.add_argument("--required-unique-rows", type=int, default=0)
    p.set_defaults(func=cmd_benchmark)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except MdAnalyticsError as exc:
        _emit({"verdict": str(Verdict.FAIL), "error": exc.as_dict()})
        return EXIT_FAIL
    except BrokenPipeError:
        return EXIT_PASS


if __name__ == "__main__":
    raise SystemExit(main())
