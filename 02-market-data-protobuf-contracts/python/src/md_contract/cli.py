"""Command line interface (section 12.2).

Exit codes are part of the contract, because a CI step reads them:

    0  everything asked for succeeded
    1  the work ran and something was rejected or mismatched
    2  the request itself was refused (bad config, unverified source, bad args)

A run with rejected input never exits 0. That is the whole reason to distinguish
1 from 2: "it worked but 37 lines were refused" and "it would not start" need
different operator responses, and both need to be distinguishable from success.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Sequence

from . import __version__
from .benchmark import run_all_lanes, run_ladder
from .config import Config
from .conformance import (
    CppTools,
    Pipeline,
    cross_language_matrix,
    decode_pbstream,
    environment_metadata,
    verify_pbstream,
    verify_toolchain,
    version_matrix,
)
from .errors import ContractError
from .report import TestReport, Verdict
from .schema_gate import Severity, gate
from .source import SourceDataset
from .validation import ValidationMode

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BASELINE = PROJECT_ROOT / "toolchain" / "descriptor-baselines" / "marketdata-v1.desc"


def _emit(payload: dict[str, Any], *, compact: bool = False) -> None:
    print(json.dumps(payload, indent=None if compact else 2, sort_keys=True, default=str))


def _config(args: argparse.Namespace) -> Config:
    config = Config.load(args.config) if getattr(args, "config", None) else Config()
    mode = getattr(args, "mode", None)
    return config.with_mode(mode) if mode else config


# ---------------------------------------------------------------------------
# commands
# ---------------------------------------------------------------------------


def cmd_encode_jsonl(args: argparse.Namespace) -> int:
    config = _config(args)
    pipeline = Pipeline(config=config, mode=config.validation.mode)

    if args.run_dir:
        result, dataset = pipeline.run_dataset(args.run_dir, args.output)
        source_info = {
            "run_dir": str(dataset.run_dir),
            "dataset_id": dataset.dataset_id,
            "source_sha256": dataset.events_sha256,
            "source_completed": dataset.completed,
            "fault_records": len(dataset.fault_index.records),
        }
    else:
        result = pipeline.run_file(args.source, args.output)
        source_info = {"source": str(args.source)}

    rejected = (
        result.metrics.get("json_rejected_total")
        + result.metrics.get("policy_rejected_total")
    )
    _emit(
        {
            "command": "encode-jsonl",
            "mode": config.validation.mode.value,
            "source": source_info,
            "result": result.as_dict(),
            "rejected_total": rejected,
            "ok": rejected == 0 and result.metrics.reconciles,
        }
    )
    return 0 if rejected == 0 and result.metrics.reconciles else 1


def cmd_decode_pbstream(args: argparse.Namespace) -> int:
    config = _config(args)
    result = decode_pbstream(
        args.input, args.output, config=config, continue_on_error=args.continue_on_error
    )
    rejected = (
        result.metrics.get("frame_rejected_total")
        + result.metrics.get("decode_rejected_total")
        + result.metrics.get("envelope_rejected_total")
    )
    _emit(
        {
            "command": "decode-pbstream",
            "input": str(args.input),
            "result": result.as_dict(),
            "rejected_total": rejected,
            "ok": rejected == 0,
        }
    )
    return 0 if rejected == 0 else 1


def cmd_verify_pbstream(args: argparse.Namespace) -> int:
    config = _config(args)
    result = verify_pbstream(args.input, args.expected_jsonl, config=config)
    _emit(
        {
            "command": "verify-pbstream",
            "input": str(args.input),
            "expected_jsonl": str(args.expected_jsonl),
            "result": result.as_dict(),
            "ok": result.passed,
        }
    )
    return 0 if result.passed else 1


def cmd_cross_lang(args: argparse.Namespace) -> int:
    config = _config(args)
    work = Path(args.work_dir)
    entries = cross_language_matrix(
        args.source, work, config=config, tools=CppTools(), limit=args.limit
    )
    if args.include_versions:
        entries += version_matrix(args.source, work / "versions", limit=args.limit or 500)

    failed = [e for e in entries if e.verdict == "FAIL"]
    skipped = [e for e in entries if e.verdict == "SKIPPED_WITH_REASON"]
    payload = {
        "command": "cross-lang-conformance",
        "environment": environment_metadata(),
        "matrix": [e.as_dict() for e in entries],
        "passed": len([e for e in entries if e.passed]),
        "failed": len(failed),
        "skipped_with_reason": len(skipped),
        "ok": not failed,
    }
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
    _emit(payload)
    return 0 if not failed else 1


def cmd_schema_gate(args: argparse.Namespace) -> int:
    passed, findings = gate(args.baseline, args.candidate)
    _emit(
        {
            "command": "schema-gate",
            "baseline": str(args.baseline),
            "candidate": str(args.candidate),
            "findings": [f.as_dict() for f in findings],
            "blocking": [
                f.as_dict() for f in findings if f.severity is Severity.FORBIDDEN
            ],
            "ok": passed,
        }
    )
    return 0 if passed else 1


def cmd_toolchain_check(args: argparse.Namespace) -> int:
    problems = verify_toolchain()
    tools = CppTools()
    _emit(
        {
            "command": "toolchain-check",
            "environment": environment_metadata(),
            "cpp_tools_available": tools.available,
            "cpp_tools_dir": str(tools.bin_dir),
            "problems": problems,
            "ok": not problems,
        }
    )
    return 0 if not problems else 1


def cmd_benchmark(args: argparse.Namespace) -> int:
    config = _config(args)
    payload: dict[str, Any] = {"command": "benchmark", "source": str(args.source)}
    if args.lanes:
        payload["lane_results"] = run_all_lanes(
            args.source,
            args.work_dir,
            config=config,
            sample_events=args.sample_events,
            repeat=args.repeat,
        )
    if args.ladder:
        payload["ladder"] = run_ladder(
            args.source,
            config=config,
            targets=args.targets or None,
            duration_s=args.duration_s,
            sample_events=args.sample_events,
        )
    payload["ok"] = True
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
    _emit(payload)
    return 0


def cmd_source_check(args: argparse.Namespace) -> int:
    try:
        dataset = SourceDataset.load(args.run_dir, require_completed=not args.allow_incomplete)
    except ContractError as exc:
        _emit({"command": "source-check", "ok": False, "error": exc.as_dict()})
        return 2
    _emit(
        {
            "command": "source-check",
            "run_dir": str(dataset.run_dir),
            "dataset_id": dataset.dataset_id,
            "schema_version": dataset.schema_version,
            "completed": dataset.completed,
            "declared_lines": dataset.events_lines,
            "events_sha256": dataset.events_sha256,
            "faults_enabled": dataset.faults_enabled,
            "fault_records": len(dataset.fault_index.records),
            "fault_ids": list(dataset.fault_index.fault_ids()),
            "ok": True,
        }
    )
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    """Render the section 15 report from a previous run's JSON fragments."""
    report = TestReport(run_id=args.run_id or time.strftime("%Y%m%dT%H%M%SZ"))
    report.add_section("environment", environment_metadata())
    report.add_section("toolchain_problems", verify_toolchain())
    for fragment in args.include or ():
        path = Path(fragment)
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        report.add_section(path.stem, payload)

        # A case-results fragment is not just another section: its verdicts are
        # what turn the matrix from 170 NOT_RUN rows into a record of what
        # actually ran. Without applying them the report would say "no case was
        # executed" after a fully green suite -- the opposite of the failure
        # section 11.2 warns about, and just as misleading.
        if isinstance(payload, dict) and isinstance(payload.get("cases"), dict):
            for case, entry in payload["cases"].items():
                try:
                    tests = entry.get("tests", [])
                    report.record(
                        case,
                        entry["verdict"],
                        evidence="; ".join(tests[:5]),
                        test=", ".join(tests[:3]),
                    )
                except (KeyError, ValueError, TypeError):
                    # An unparseable row stays NOT_RUN rather than being guessed.
                    continue
    report.known_limitations = [
        "MDP1 has no authentication, no encryption and no application-level ACK.",
        "Protobuf cannot detect a bit flip that still parses; the oracle catches it "
        "in test, a production path would need authenticated transport.",
        "Synthetic data only. Not market data and not usable for trading.",
    ]
    out = report.write_json(args.output)
    for line in report.summary_lines():
        print(line, file=sys.stderr)
    _emit({"command": "report", "written": str(out), "ok": True})
    return 0


# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="md_contract",
        description=(
            "marketdata.v1 Protobuf contract tools. Synthetic data only: nothing "
            "here is market data and nothing here is usable for trading."
        ),
    )
    parser.add_argument("--version", action="version", version=f"md_contract {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_config(p: argparse.ArgumentParser) -> None:
        p.add_argument("--config", help="path to a config TOML")
        p.add_argument(
            "--mode",
            choices=[m.value for m in ValidationMode],
            help="override [validation].mode",
        )

    p = sub.add_parser("encode-jsonl", help="sim.v1 JSONL -> MDP1 .pbstream")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--source", help="a sim.v1 JSONL file")
    group.add_argument(
        "--run-dir",
        help="a Project 01 run directory; its COMPLETED marker and hash are verified",
    )
    p.add_argument("--output", help="output .pbstream path")
    add_config(p)
    p.set_defaults(func=cmd_encode_jsonl)

    p = sub.add_parser("decode-pbstream", help=".pbstream -> core projection JSONL")
    p.add_argument("--input", required=True)
    p.add_argument("--output")
    p.add_argument(
        "--continue-on-error",
        action="store_true",
        help="keep going past a rejected payload (offline corpus only; framing "
        "failures always stop the stream)",
    )
    add_config(p)
    p.set_defaults(func=cmd_decode_pbstream)

    p = sub.add_parser(
        "verify-pbstream", help="compare a stream against the source JSONL it came from"
    )
    p.add_argument("--input", required=True)
    p.add_argument("--expected-jsonl", required=True)
    add_config(p)
    p.set_defaults(func=cmd_verify_pbstream)

    p = sub.add_parser("cross-lang-conformance", help="run the C++/Python matrix")
    p.add_argument("--source", required=True)
    p.add_argument("--work-dir", default="build/conformance")
    p.add_argument("--limit", type=int, help="use only the first N source lines")
    p.add_argument("--include-versions", action="store_true", help="also run v1/v1.1/v2")
    p.add_argument("--report", help="write the JSON report here")
    add_config(p)
    p.set_defaults(func=cmd_cross_lang)

    p = sub.add_parser("schema-gate", help="diff a candidate schema against the baseline")
    p.add_argument("--baseline", default=str(DEFAULT_BASELINE))
    p.add_argument("--candidate", required=True)
    p.set_defaults(func=cmd_schema_gate)

    p = sub.add_parser("toolchain-check", help="verify the pinned toolchain and hashes")
    p.set_defaults(func=cmd_toolchain_check)

    p = sub.add_parser("source-check", help="verify a Project 01 run directory")
    p.add_argument("--run-dir", required=True)
    p.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="accept a run with no COMPLETED marker (for the negative test only)",
    )
    p.set_defaults(func=cmd_source_check)

    p = sub.add_parser("benchmark", help="run benchmark lanes and/or the load ladder")
    p.add_argument("--source", required=True)
    p.add_argument("--work-dir", default="build/benchmark")
    p.add_argument("--lanes", action="store_true", help="run lanes B1/B2/B5/B8")
    p.add_argument("--ladder", action="store_true", help="run the load ladder")
    p.add_argument("--targets", type=int, nargs="*", help="override ladder targets")
    p.add_argument("--duration-s", type=float, default=2.0)
    p.add_argument("--sample-events", type=int, default=20000)
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--report")
    add_config(p)
    p.set_defaults(func=cmd_benchmark)

    p = sub.add_parser("report", help="render the section 15 test report")
    p.add_argument("--output", default="reports/test-report.json")
    p.add_argument("--run-id")
    p.add_argument("--include", nargs="*", help="JSON fragments to fold in")
    p.set_defaults(func=cmd_report)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except ContractError as exc:
        # A refusal, not a crash: the request was understood and declined, so it
        # gets a structured error and exit 2 rather than a traceback.
        _emit({"command": args.command, "ok": False, "error": exc.as_dict()})
        return 2
    except FileNotFoundError as exc:
        _emit({"command": args.command, "ok": False, "error": {"reason": "NOT_FOUND", "detail": str(exc)}})
        return 2


if __name__ == "__main__":
    sys.exit(main())
