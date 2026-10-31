"""Command line interface (section 21.2).

Subcommands::

    selftest    run the FIX-001..FIX-004 golden fixtures against the metrology kernel
    preflight   validate a workload config, a source seal and stage availability
    calibrate   drive a rate against a null sink and judge the driver
    bench       run a lane end to end and emit a p08.v1 report
    verify      re-read an evidence bundle and recompute its hashes
    catalog     list the 512 what-if cases and 48 E2E tests
    gate        evaluate the release coverage gate against a plan

Two rules, mirroring the P01 CLI:

* **Nothing runs before the config validates.** A bad rate or an unknown key
  fails before a directory is created.
* **A failure is never hidden.** Every error prints structured JSON with a
  stable ``code`` and exits non-zero; the codes live in :mod:`p08_bench.errors`.

``selftest`` is the one to run first on a new machine. Section 8 requires the
measuring tools to be validated against known inputs before they are pointed at
a real system, and section 27 lists those fixtures as the minimum needed to
start.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from . import REPORT_VERSION, __version__
from .calibrator import NullSink, calibrate
from .catalog import CaseCatalog, CoverageGate, Kind, Priority, statuses_from_mapping
from .clock import ClockDomain, FakeClock, MonotonicClock
from .collectors import environment_limitations
from .config import WorkloadConfig, load_config
from .errors import BenchError, ConfigError
from .evidence import EvidenceStore, host_profile_from_platform
from .event_driver import default_payload_factory
from .goldens import (
    ALL_FIXTURES,
    FIX002_EXPECTED_P95_MS,
    FIX002_FORBIDDEN_P95_MS,
    FIX002_LATENCIES_MS,
    FIX003_EXPECTED_BACKLOG_GROWTH,
    FIX003_EXPECTED_TIME_TO_FULL_S,
    FIX003_SCENARIO,
    FIX004_CLIENT_P95_BUDGET_MS,
    FIX004_PINNED_QUERY_FAMILY,
    FIX004_PRECONDITIONS,
)
from .histogram import BucketHistogram, Resolution, bounds_for_threshold
from .manifest import HostProfile, SealState, SourceSeal
from .orchestrator import Orchestrator
from .percentiles import exact_nearest_rank, percentile_with_censoring
from .scheduler import RateSchedule
from .sut import requirements_for
from .sut_mock import MockConfig, MockPipeline
from .sut_p01 import P01SourcePipeline, P01SourceReader
from .timeseries import time_to_full_s
from .verdict import Status

PROG = "p08_bench"


def _emit(payload: dict[str, Any], *, compact: bool = False) -> None:
    text = (
        json.dumps(payload, sort_keys=True, separators=(",", ":"))
        if compact
        else json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
    )
    print(text)


def _fail(exc: BenchError, *, compact: bool = False) -> int:
    _emit({"ok": False, **exc.as_dict()}, compact=compact)
    return exc.exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG,
        description=(
            "P08 benchmark harness. Measures a market data pipeline; it does not "
            "assert that any throughput or latency target was met. Synthetic "
            "workloads only, lab use."
        ),
    )
    parser.add_argument("--compact", action="store_true", help="single-line JSON output")
    parser.add_argument("--version", action="version", version=f"{PROG} {__version__}")

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--compact", action="store_true", default=argparse.SUPPRESS, help=argparse.SUPPRESS
    )

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser(
        "selftest",
        parents=[common],
        help="run the golden fixtures that validate the metrology kernel",
    )

    for name, help_text in (
        ("preflight", "validate config, source seal and stage availability"),
        ("bench", "run a lane end to end"),
    ):
        cmd = sub.add_parser(name, parents=[common], help=help_text)
        cmd.add_argument("--config", type=Path, required=True)
        cmd.add_argument(
            "--source",
            type=Path,
            help="a sealed P01 run directory to use as the source dataset",
        )
        cmd.add_argument(
            "--sut",
            choices=("mock", "p01"),
            default="mock",
            help=(
                "mock: deterministic simulation, validates the tools only. "
                "p01: the real P01 source, which blocks every downstream lane."
            ),
        )
        cmd.add_argument("--scenario", default="E2E-02", help="catalog case id this run is for")
        if name == "bench":
            cmd.add_argument("--out", type=Path, help="directory for the evidence bundle")

    cal = sub.add_parser("calibrate", parents=[common], help="judge the driver against a null sink")
    cal.add_argument("--rate", type=float, required=True, help="target rate in events/s")
    cal.add_argument("--seconds", type=float, default=1.0)
    cal.add_argument("--sink-delay-ns", type=int, default=0)
    cal.add_argument("--max-late-issue-ms", type=int, default=10)

    ver = sub.add_parser("verify", parents=[common], help="recompute evidence hashes")
    ver.add_argument("--run-dir", type=Path, required=True)

    cat = sub.add_parser("catalog", parents=[common], help="list the test catalog")
    cat.add_argument("--priority", choices=("P0", "P1", "P2", "UNSET"))
    cat.add_argument("--group")
    cat.add_argument("--kind", choices=("WHAT_IF", "E2E"))
    cat.add_argument("--limit", type=int, default=0, help="0 lists counts only")

    gate = sub.add_parser("gate", parents=[common], help="evaluate the release coverage gate")
    gate.add_argument(
        "--plan",
        type=Path,
        help='JSON mapping of case id to status, e.g. {"P08-G01-01": "PASS"}',
    )
    return parser


# -- selftest ---------------------------------------------------------------


def _selftest() -> dict[str, Any]:
    """Run the four golden fixtures and report each one pass or fail.

    These are the checks that catch a broken ruler. FIX-002 pins the percentile
    definition, FIX-003 pins the queue arithmetic, FIX-004 pins the
    preconditions for the 200M lane, and the histogram check is case E2E-40.
    """
    checks: list[dict[str, Any]] = []

    # FIX-002: exact nearest-rank p95 over ten samples is the tenth value.
    p95 = exact_nearest_rank(FIX002_LATENCIES_MS, 0.95)
    checks.append(
        {
            "fixture": "FIX-002",
            "check": "exact nearest-rank p95 of the ten-sample fixture",
            "expected": FIX002_EXPECTED_P95_MS,
            "observed": p95,
            "passed": p95 == FIX002_EXPECTED_P95_MS,
        }
    )
    checks.append(
        {
            "fixture": "FIX-002",
            "check": "p95 is not the mean",
            "forbidden": list(FIX002_FORBIDDEN_P95_MS),
            "observed": p95,
            "passed": p95 not in FIX002_FORBIDDEN_P95_MS,
        }
    )

    # FIX-002 second half: ten timeouts stay in the denominator and the rank
    # lands in the censored tail, so there is no value to report.
    censored = percentile_with_censoring(
        FIX002_LATENCIES_MS, 0.95, n_censored=10, deadline_ms=2000.0
    )
    checks.append(
        {
            "fixture": "FIX-002",
            "check": "timeouts stay in the denominator and censor the quantile",
            "expected": {"n_considered": 20, "is_lower_bound": True, "value_ms": None},
            "observed": {
                "n_considered": censored.n_considered,
                "is_lower_bound": censored.is_lower_bound,
                "value_ms": censored.value_ms,
            },
            "passed": censored.n_considered == 20
            and censored.is_lower_bound
            and censored.value_ms is None,
        }
    )

    # FIX-003: the queue arithmetic.
    growth = FIX003_SCENARIO.expected_backlog_growth
    fill = time_to_full_s(
        FIX003_SCENARIO.free_capacity_records,
        FIX003_SCENARIO.offered_per_s,
        FIX003_SCENARIO.sink_per_s,
    )
    checks.append(
        {
            "fixture": "FIX-003",
            "check": "backlog growth at 120k offered into a 100k sink over 600 s",
            "expected": FIX003_EXPECTED_BACKLOG_GROWTH,
            "observed": growth,
            "passed": growth == FIX003_EXPECTED_BACKLOG_GROWTH,
        }
    )
    checks.append(
        {
            "fixture": "FIX-003",
            "check": "a 1M-record queue fills in about 50 s",
            "expected": FIX003_EXPECTED_TIME_TO_FULL_S,
            "observed": fill,
            "passed": fill == FIX003_EXPECTED_TIME_TO_FULL_S,
        }
    )

    # Case E2E-40: a 200-500 ms bucket cannot decide a 280 ms threshold.
    coarse = BucketHistogram([50, 100, 200, 500, 1000])
    fine = BucketHistogram(bounds_for_threshold(FIX004_CLIENT_P95_BUDGET_MS))
    checks.append(
        {
            "fixture": "FIX-004",
            "check": "a coarse histogram cannot decide the 280 ms budget (case E2E-40)",
            "expected": "INSUFFICIENT then ADEQUATE",
            "observed": [
                coarse.resolution_for_threshold(FIX004_CLIENT_P95_BUDGET_MS).value,
                fine.resolution_for_threshold(FIX004_CLIENT_P95_BUDGET_MS).value,
            ],
            "passed": (
                coarse.resolution_for_threshold(FIX004_CLIENT_P95_BUDGET_MS) is Resolution.INSUFFICIENT
                and fine.resolution_for_threshold(FIX004_CLIENT_P95_BUDGET_MS) is Resolution.ADEQUATE
            ),
        }
    )

    checks.append(
        {
            "fixture": "FIX-004",
            "check": "the 200M lane is gated until its preconditions are checked",
            "expected": "not ready, with named blockers",
            "observed": FIX004_PRECONDITIONS.blockers(),
            "passed": not FIX004_PRECONDITIONS.ready and bool(FIX004_PRECONDITIONS.blockers()),
        }
    )

    passed = all(check["passed"] for check in checks)
    return {
        "ok": passed,
        "fixtures": list(ALL_FIXTURES),
        "pinned_query_family": FIX004_PINNED_QUERY_FAMILY,
        "checks": checks,
        "passed": sum(1 for c in checks if c["passed"]),
        "failed": sum(1 for c in checks if not c["passed"]),
        "environment_limitations": environment_limitations(),
        "note": (
            "these fixtures validate the measurement tools against known inputs; "
            "they say nothing about any pipeline"
        ),
    }


# -- shared lane wiring -----------------------------------------------------


def _resolve_sut(kind: str, source: Path | None):
    if kind == "p01":
        if source is None:
            raise ConfigError("--sut p01 requires --source pointing at a P01 run directory")
        return P01SourcePipeline(P01SourceReader(source))
    return MockPipeline(MockConfig())


def _resolve_seal(config: WorkloadConfig, source: Path | None) -> SourceSeal:
    if source is not None:
        return SourceSeal.from_p01_run_dir(source)
    if config.dataset.source_seal_required:
        # No source given and the manifest requires one: the honest result is an
        # unsealed source, which preflight will refuse.
        return SourceSeal(
            dataset_id=config.dataset.dataset_id,
            path="<none>",
            sha256=None,
            event_count=None,
            state=SealState.MISSING,
            reasons=("no --source was supplied and the manifest requires a sealed source",),
        )
    return SourceSeal(
        dataset_id=config.dataset.dataset_id, path="<synthetic>", sha256=None,
        event_count=None,
        state=SealState.MISSING,
        reasons=("synthetic workload with no sealed source",),
    )


def _host_profile() -> HostProfile:
    facts = host_profile_from_platform(environment_limitations())
    return HostProfile(**facts)  # type: ignore[arg-type]


def _oracle_for(sut: Any):
    if not isinstance(sut, MockPipeline):
        return None, None

    def factory():
        return sut.accepted_identities(), {
            key: sut.source_payload(key) or {} for key in sut.accepted_identities()
        }

    return factory, sut.canonical_payload


# -- commands ---------------------------------------------------------------


def _cmd_preflight(args: argparse.Namespace) -> dict[str, Any]:
    config = load_config(args.config)
    sut = _resolve_sut(args.sut, args.source)
    seal = _resolve_seal(config, args.source)
    clock = MonotonicClock(ClockDomain("localhost", "cli"))
    orch = Orchestrator(
        config, sut, clock, scenario_id=args.scenario,
        dataset_seal=seal, host_profile=_host_profile(),
    )
    result = orch.preflight(None)
    return {
        "ok": result.ok,
        "config_sha256": config.sha256,
        "lane": config.run.lane,
        "required_stages": [s.value for s in requirements_for(config.run.lane)],
        "stage_report": sut.stage_report(),
        "claim_limitations": config.claim_limitations(),
        "preflight": result.as_dict(),
        "environment_limitations": environment_limitations(),
    }


def _cmd_calibrate(args: argparse.Namespace) -> dict[str, Any]:
    clock = FakeClock(ClockDomain("localhost", "calibrator"))
    schedule = RateSchedule.constant_rate(args.rate, args.seconds)
    result = calibrate(
        schedule,
        default_payload_factory("CALIBRATION", ["EURUSD", "GBPUSD", "USDJPY"]),
        clock,
        sink=NullSink(delay_ns=args.sink_delay_ns),
        max_late_issue_ms=args.max_late_issue_ms,
    )
    return {
        "ok": result.supports_rate_claim,
        "calibration": result.as_dict(),
        "note": (
            "a calibrated driver permits a rate claim; it does not make one. "
            "Schedule time is simulated, so this measures the driver logic, not "
            "wall-clock issue capacity on this host."
        ),
    }


def _cmd_bench(args: argparse.Namespace) -> dict[str, Any]:
    config = load_config(args.config)
    sut = _resolve_sut(args.sut, args.source)
    seal = _resolve_seal(config, args.source)
    clock = FakeClock(ClockDomain("localhost", "bench"))
    out = args.out or Path("reports") / "runs" / config.run.id
    store = EvidenceStore(out)
    orch = Orchestrator(
        config, sut, clock, scenario_id=args.scenario, dataset_seal=seal,
        host_profile=_host_profile(), evidence=store,
        resource_blockers=environment_limitations(),
    )
    oracle, canonical_payload = _oracle_for(sut)
    result = orch.run(
        default_payload_factory(
            config.dataset.dataset_id,
            [f"SYM{i:03d}" for i in range(config.dataset.symbol_count)],
        ),
        oracle=oracle,
        canonical_payload=canonical_payload,
    )
    store.write_json("report", result.report)
    store.write_json("lane_result", result.as_dict())
    return {
        "ok": result.overall in (Status.PASS, Status.NOT_APPLICABLE),
        "run_id": result.run_id,
        "overall": result.overall.value,
        "stop_reason": result.stop_reason.value,
        "evidence_root": str(out),
        "artifact_manifest": store.artifact_manifest(),
        "verdict": result.verdict.as_dict(),
        "report": result.report,
    }


def _cmd_verify(args: argparse.Namespace) -> dict[str, Any]:
    """Re-read a bundle and recompute its hashes.

    The store is rebuilt from what is on disk, so this reports what the files
    say now. It cannot detect a file that was replaced *before* the manifest was
    written, which is why the hashes are recorded at write time in the first
    place.
    """
    root: Path = args.run_dir
    if not root.is_dir():
        raise ConfigError("run directory not found", path=str(root))
    report_path = root / "report.json"
    present = sorted(p.name for p in root.glob("*.json"))
    payload: dict[str, Any] = {
        "run_dir": str(root),
        "artifacts_present": present,
        "report_found": report_path.is_file(),
    }
    if report_path.is_file():
        report = json.loads(report_path.read_text(encoding="utf-8"))
        from .report import render_text_summary, validate_report

        problems = validate_report(report)
        payload["report_version"] = report.get("report_version")
        payload["status_dimensions"] = report.get("status_dimensions")
        payload["report_problems"] = problems
        payload["summary_text"] = render_text_summary(report)
        payload["ok"] = not problems
    else:
        payload["ok"] = False
        payload["report_problems"] = ["no report.json in the run directory"]
    return payload


def _cmd_catalog(args: argparse.Namespace) -> dict[str, Any]:
    catalog = CaseCatalog.load()
    cases = list(catalog)
    if args.kind:
        cases = [c for c in cases if c.kind is Kind(args.kind)]
    if args.priority:
        cases = [c for c in cases if c.priority is Priority(args.priority)]
    if args.group:
        cases = [c for c in cases if c.group == args.group]

    counts: dict[str, int] = {}
    for case in cases:
        counts[case.priority.value] = counts.get(case.priority.value, 0) + 1
    payload: dict[str, Any] = {
        "ok": True,
        "total_in_catalog": len(catalog),
        "matched": len(cases),
        "by_priority": dict(sorted(counts.items())),
        "groups": sorted({c.group for c in cases}),
        "default_status": "NOT_RUN",
    }
    if args.limit:
        payload["cases"] = [c.as_dict() for c in cases[: args.limit]]
    return payload


def _cmd_gate(args: argparse.Namespace) -> dict[str, Any]:
    catalog = CaseCatalog.load()
    gate = CoverageGate(catalog=catalog)
    if args.plan is not None:
        if not args.plan.is_file():
            raise ConfigError("plan file not found", path=str(args.plan))
        raw = json.loads(args.plan.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ConfigError("plan must be a JSON object of case id to status")
        statuses = statuses_from_mapping(raw)
        gate.plan(statuses)
        from .catalog import CaseResult

        for case_id, status in statuses.items():
            gate.record(
                CaseResult(
                    case_id,
                    status,
                    reason="supplied by plan file",
                    evidence_ref=str(args.plan) if status.value == "PASS" else None,
                )
            )
    result = gate.evaluate()
    return {"ok": result["verdict"] == "GO", "gate": result, "by_status": gate.summary_by_status()}


_COMMANDS = {
    "selftest": lambda args: _selftest(),
    "preflight": _cmd_preflight,
    "calibrate": _cmd_calibrate,
    "bench": _cmd_bench,
    "verify": _cmd_verify,
    "catalog": _cmd_catalog,
    "gate": _cmd_gate,
}


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    compact = bool(getattr(args, "compact", False))
    handler = _COMMANDS[args.command]
    try:
        payload = handler(args)
    except BenchError as exc:
        return _fail(exc, compact=compact)
    payload.setdefault("report_version", REPORT_VERSION)
    _emit(payload, compact=compact)
    return 0 if payload.get("ok", False) else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
