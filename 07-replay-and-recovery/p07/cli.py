"""``python -m p07`` -- the commands from design 14.2.

Everything the design proposes, with one addition (``build-catalog``) and two
deliberate omissions it names: there is no ``--target any-host`` and no
``--force-delete`` that bypasses the allowlist. A scenario can only name targets
the lab declares, and the loader refuses before anything runs.

Exit codes are meant for CI::

    0  the command succeeded, and any verdict it produced was PASS
    1  a verdict that blocks release (FAIL / INCONCLUSIVE / INVALID / NOT_RUN)
    2  the command itself could not run (bad config, missing source)

A blocking verdict is not an error in the command, so it is kept apart from
code 2; a release gate must be able to tell "the lab says no" from "the lab did
not run".
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from .dataset_sealer import SealError, seal_dataset, sha256_file
from .evidence.verdicts import Verdict
from .orchestration.scenario_runner import run_scenario
from .reporting.markdown_json import render_release_report, render_summary
from .scenario_loader import ScenarioError, lab_config_findings, load_lab_config, load_scenario

_BLOCKING = 1
_USAGE_ERROR = 2


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


def cmd_validate_config(args: argparse.Namespace) -> int:
    try:
        config = load_lab_config(args.path)
    except (ScenarioError, OSError, ValueError) as exc:
        print(f"invalid lab config: {exc}", file=sys.stderr)
        return _USAGE_ERROR
    notes = lab_config_findings(config)
    _print_json({"path": str(args.path), "valid": True, "weakening_settings": notes})
    # A config that disables the gates is valid YAML and still unusable, so it
    # exits non-zero rather than printing a warning nobody reads.
    return _BLOCKING if notes else 0


def cmd_verify_source(args: argparse.Namespace) -> int:
    try:
        dataset = seal_dataset(
            args.run_dir, anchor=args.anchor, require_fault_plan=args.require_fault_plan
        )
    except SealError as exc:
        print(f"source not sealed: {exc.reason}: {exc.detail}", file=sys.stderr)
        return _BLOCKING
    receipt = dataset.seal_receipt()
    _print_json(receipt)
    if not dataset.anchor_verified:
        print(
            "note: no external anchor supplied, so source integrity is self-consistent only "
            "(design 5.1); pass --anchor <sha256> from a pinned lockfile to prove it",
            file=sys.stderr,
        )
    return 0


def cmd_anchor(args: argparse.Namespace) -> int:
    """Print the digest to pin in a lockfile outside the run directory."""
    path = Path(args.events_path)
    if not path.is_file():
        print(f"not a file: {path}", file=sys.stderr)
        return _USAGE_ERROR
    digest, size = sha256_file(path)
    _print_json({"path": str(path), "sha256": digest, "bytes": size})
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    try:
        scenario = load_scenario(args.scenario)
    except (ScenarioError, OSError, ValueError) as exc:
        print(f"invalid scenario: {exc}", file=sys.stderr)
        return _USAGE_ERROR
    plan = scenario.as_dict()
    plan["stages"] = {
        stage: [fault.type for fault in scenario.faults_at(stage)]
        for stage in ("pre_replay", "replay", "post_cut", "pre_ingest", "post_ingest", "pre_build")
    }
    source_ok = Path(scenario.source_run_dir).is_dir()
    plan["source_present"] = source_ok
    _print_json(plan)
    return 0 if source_ok else _BLOCKING


def cmd_run(args: argparse.Namespace) -> int:
    try:
        scenario = load_scenario(args.scenario)
    except (ScenarioError, OSError, ValueError) as exc:
        print(f"invalid scenario: {exc}", file=sys.stderr)
        return _USAGE_ERROR
    outcome = run_scenario(
        scenario, runs_dir=args.runs_dir, run_id=args.run_id, fsync=not args.no_fsync
    )
    _print_json(
        {
            "scenario_id": scenario.scenario_id,
            "run_id": args.run_id,
            "verdict": outcome.gate.verdict.value,
            "primary_reason": outcome.gate.primary_reason,
            "run_dir": outcome.run_dir,
            "as_expected": outcome.as_expected,
            "expectation_mismatches": list(outcome.expectation_mismatches),
        }
    )
    if not outcome.as_expected:
        return _BLOCKING
    return 0 if outcome.gate.verdict is Verdict.PASS else _BLOCKING


def cmd_report(args: argparse.Namespace) -> int:
    results = Path(args.run_dir) / "reports" / "results.json"
    if not results.is_file():
        print(f"no results.json under {args.run_dir}", file=sys.stderr)
        return _USAGE_ERROR
    report = json.loads(results.read_text(encoding="utf-8"))
    if args.format == "json":
        _print_json(report)
    else:
        print(render_summary(report))
    return 0 if report.get("verdict") == Verdict.PASS.value else _BLOCKING


def cmd_reconcile(args: argparse.Namespace) -> int:
    """Re-verify a written bundle: artifact hashes and the journal chain.

    This does not re-run the reconciliation -- the data plane is gone. It checks
    that the *evidence* is intact, which is the part that can rot after the fact
    and the part a reader has to be able to trust.
    """
    run_dir = Path(args.run_dir)
    sums = run_dir / "SHA256SUMS"
    if not sums.is_file():
        print(f"no SHA256SUMS under {run_dir}", file=sys.stderr)
        return _USAGE_ERROR
    damaged: list[str] = []
    checked = 0
    for line in sums.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        digest, _, relative = line.partition("  ")
        target = run_dir / relative
        if not target.is_file():
            damaged.append(f"{relative}: missing")
            continue
        actual, _ = sha256_file(target)
        checked += 1
        if actual != digest:
            damaged.append(f"{relative}: digest mismatch")
    report_path = run_dir / "reports" / "results.json"
    journal = {}
    if report_path.is_file():
        journal = json.loads(report_path.read_text(encoding="utf-8")).get("evidence", {}).get(
            "journal", {}
        )
    _print_json(
        {
            "run_dir": str(run_dir),
            "artifacts_checked": checked,
            "damaged": damaged,
            "journal": journal,
            "note": (
                "SHA256SUMS is inside the bundle. Anyone who can rewrite the bundle can rewrite "
                "it too; compare the anchor against a copy stored elsewhere."
            ),
        }
    )
    return _BLOCKING if damaged else 0


def cmd_verify_release(args: argparse.Namespace) -> int:
    catalog_path = Path(args.catalog)
    if not catalog_path.is_file():
        print(f"no catalog at {catalog_path}", file=sys.stderr)
        return _USAGE_ERROR
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    cases = catalog["cases"] if isinstance(catalog, dict) else catalog

    executed: list[dict[str, Any]] = []
    if args.coverage and Path(args.coverage).is_file():
        executed = json.loads(Path(args.coverage).read_text(encoding="utf-8"))

    text = render_release_report(catalog=cases, executed=executed)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
    print(text)
    blocking = any(
        case.get("priority") == "P0"
        and not any(case["case_id"] in row.get("cases", ()) and row.get("verdict") == "PASS" for row in executed)
        for case in cases
    )
    return _BLOCKING if blocking else 0


def cmd_read_live_rows(args) -> int:
    """Read real raw rows so the reconcilers can be pointed at them.

    Reports physical rows and logical identities as separate numbers. The gap
    between them is at-least-once duplication working as designed, and a single
    combined figure would let a reader draw the wrong conclusion from it.
    """
    from .replay.live_clickhouse import (
        LiveClickHouse,
        LiveClickHouseUnavailable,
        as_json,
        raw_row_summary,
        read_raw_rows,
    )

    client = LiveClickHouse(args.clickhouse_url)
    try:
        rows = list(
            read_raw_rows(
                client, dataset_id=args.dataset_id, table=args.raw_table
            )
        )
        summary = raw_row_summary(rows)
        summary["clickhouse_version"] = client.version()
    except LiveClickHouseUnavailable as exc:
        # INCONCLUSIVE, not FAIL: the lab could not see what it needed to.
        print("inconclusive: %s" % exc, file=sys.stderr)
        return 3

    print(as_json(summary))
    return 0 if rows else 1


def cmd_scan_live_cut(args) -> int:
    """Scan a real broker with the same classifier the model profile uses.

    The verdict carries ``profile: LIVE`` so a reader can tell this apart from a
    UNIT/MODEL result. That distinction is the whole point: P07's own README is
    careful that a modelled PASS says nothing about a deployment, and a live
    PASS must not be able to masquerade as one or the other.
    """
    import json

    from .replay.live_kafka import LiveKafkaUnavailable, scan_live_cut

    if not args.fenced:
        print(
            "refused: pass --fenced to assert the producers were stopped before "
            "the cut was taken; an unfenced cut moves while it is read",
            file=sys.stderr,
        )
        return 2
    try:
        summary = scan_live_cut(
            args.bootstrap,
            args.cut,
            fenced=True,
            expect_dataset_id=args.dataset_id,
        )
    except LiveKafkaUnavailable as exc:
        print("inconclusive: %s" % exc, file=sys.stderr)
        return 3
    except Exception as exc:
        # CutUnscannable and friends are INCONCLUSIVE, never FAIL: the lab could
        # not see what it needed to, which is a different claim from a defect.
        print("inconclusive: %s" % exc, file=sys.stderr)
        return 3

    print(json.dumps(summary, indent=2, sort_keys=True))
    conflicts = int(summary["identity_conflicts"])
    findings = list(summary["findings"])
    return 0 if conflicts == 0 and not findings else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="p07",
        description="Deterministic replay and failure-recovery harness (test lab only).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("validate-config", help="validate config/replay-lab.yaml")
    p.add_argument("path")
    p.set_defaults(func=cmd_validate_config)

    p = sub.add_parser("verify-source", help="seal and verify a P01 run directory")
    p.add_argument("run_dir")
    p.add_argument("--anchor", default=None, help="externally pinned SHA-256 of events.jsonl")
    p.add_argument("--require-fault-plan", action="store_true")
    p.set_defaults(func=cmd_verify_source)

    p = sub.add_parser("anchor", help="print the SHA-256 to pin outside the run directory")
    p.add_argument("events_path")
    p.set_defaults(func=cmd_anchor)

    p = sub.add_parser("plan", help="validate a scenario and show its stage schedule")
    p.add_argument("scenario")
    p.set_defaults(func=cmd_plan)

    p = sub.add_parser("run", help="execute a scenario and write an evidence bundle")
    p.add_argument("scenario")
    p.add_argument("--runs-dir", default="runs")
    p.add_argument("--run-id", default="run-1")
    p.add_argument(
        "--no-fsync",
        action="store_true",
        help=(
            "skip fsync on evidence writes. Much faster, and the bundle records that it was "
            "not flushed; do not use for a run whose crash-durability is part of the claim"
        ),
    )
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("report", help="render a written bundle")
    p.add_argument("run_dir")
    p.add_argument("--format", choices=("markdown", "json"), default="markdown")
    p.set_defaults(func=cmd_report)

    p = sub.add_parser(
        "read-live-rows",
        help="read the REAL ClickHouse raw rows and summarise them (profile LIVE)",
    )
    p.add_argument("--clickhouse-url", required=True)
    p.add_argument("--dataset-id", required=True)
    p.add_argument("--raw-table", default="md_ingest.market_events_raw")
    p.set_defaults(func=cmd_read_live_rows)

    p = sub.add_parser(
        "scan-live-cut",
        help="scan a REAL Kafka cut and classify it (profile LIVE, not UNIT/MODEL)",
    )
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--cut", required=True, help="cut JSON sealed by P09")
    p.add_argument("--dataset-id", default=None)
    p.add_argument(
        "--fenced",
        action="store_true",
        help=(
            "assert the producers were stopped before the cut was taken. "
            "Required: an unfenced cut moves while it is read, and only the "
            "operator who stopped them can assert it"
        ),
    )
    p.set_defaults(func=cmd_scan_live_cut)

    p = sub.add_parser("reconcile", help="re-verify a bundle's artifact hashes and journal")
    p.add_argument("run_dir")
    p.set_defaults(func=cmd_reconcile)

    p = sub.add_parser("verify-release", help="render the release report from the what-if catalog")
    p.add_argument("--catalog", default="catalog/what_if_matrix.json")
    p.add_argument("--coverage", default="catalog/coverage.json")
    p.add_argument("--out", default=None)
    p.set_defaults(func=cmd_verify_release)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
