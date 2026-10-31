"""Command line interface (section 6).

Subcommands::

    record     generate -> JSONL only
    serve      generate -> TCP only
    both       generate -> JSONL + TCP
    replay     recorded JSONL -> JSONL and/or TCP, at a chosen speed
    verify     re-read a run directory and render a verdict
    benchmark  record/serve with a load profile and a benchmark report

Two CLI rules worth stating:

* **Nothing is produced before the config validates.** Overrides are folded in
  and the whole config is re-validated, so ``--rate -1`` fails before a socket is
  bound or a directory is created (case E2E-11).
* **A fatal error is never hidden.** Every failure prints a structured JSON error
  with a stable ``code`` and exits non-zero; the exit codes are listed in
  :mod:`market_sim.errors`.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any, Sequence

from .config import load_config
from .errors import ConfigError, SimError
from .faults import FaultPlan, load_fault_plan
from .verify import Verdict, verify_run

PROG = "market_sim"


def _emit(payload: dict[str, Any], *, compact: bool = False) -> None:
    text = (
        json.dumps(payload, sort_keys=True, separators=(",", ":"))
        if compact
        else json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
    )
    print(text)


def _fail(exc: SimError, *, compact: bool = False) -> int:
    _emit({"ok": False, **exc.as_dict()}, compact=compact)
    return exc.exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG,
        description=(
            "FX/CFD market data simulator. Generates SYNTHETIC tick/quote data "
            "for testing. Not market data; not for trading."
        ),
    )
    parser.add_argument("--compact", action="store_true", help="single-line JSON output")
    # Shared parent so --compact works before *or* after the subcommand; having
    # it accepted in only one position is a needless papercut in scripts.
    # default=SUPPRESS matters: a plain store_true default would reset the
    # top-level flag when the subcommand is parsed.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--compact",
        action="store_true",
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_run_args(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--config", required=True, help="path to a config TOML")
        sp.add_argument("--count", type=int, help="override run.count (logical slots)")
        sp.add_argument(
            "--duration", type=float, dest="duration_s", help="override run.duration_s"
        )
        sp.add_argument(
            "--rate", type=int, dest="rate_per_sec", help="override run.rate_per_sec"
        )
        sp.add_argument("--seed", type=int, help="override run.seed")
        sp.add_argument(
            "--start-time-ns", type=int, help="override run.start_time_ns (logical t0)"
        )
        sp.add_argument(
            "--fault-plan", help="path to a fault plan TOML (faults must be enabled)"
        )
        sp.add_argument(
            "--run-id", help="explicit run_instance_id (default: generated, unique)"
        )

    for name, help_text in (
        ("record", "generate and record JSONL"),
        ("serve", "generate and stream over TCP"),
        ("both", "record JSONL and stream over TCP"),
    ):
        add_run_args(sub.add_parser(name, help=help_text, parents=[common]))

    sp_bench = sub.add_parser(
        "benchmark", help="run a load profile and report", parents=[common]
    )
    add_run_args(sp_bench)
    sp_bench.add_argument(
        "--repeat", type=int, default=1, help="repetitions (median and worst reported)"
    )
    sp_bench.add_argument(
        "--warmup", action="store_true", help="discard the first repetition"
    )

    sp_replay = sub.add_parser(
        "replay", help="replay a recorded dataset", parents=[common]
    )
    sp_replay.add_argument("--config", required=True, help="path to a config TOML")
    sp_replay.add_argument(
        "--input", required=True, help="run directory or events.jsonl to replay"
    )
    sp_replay.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="1.0 = recorded pace, 10 = ten times faster, 0 = no pacing",
    )
    sp_replay.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="replay a dataset with no COMPLETED marker (explicit opt-in)",
    )
    sp_replay.add_argument("--run-id", help="explicit run_instance_id")

    sp_verify = sub.add_parser(
        "verify", help="verify a recorded run directory", parents=[common]
    )
    sp_verify.add_argument("--run-dir", required=True)
    sp_verify.add_argument(
        "--max-line-bytes",
        type=int,
        default=1 << 20,
        help="reject any line longer than this (bounds verifier memory)",
    )
    sp_verify.add_argument(
        "--no-anomalies",
        action="store_true",
        help="skip per-symbol gap/duplicate tracking (constant memory on huge files)",
    )
    sp_verify.add_argument("--quiet", action="store_true", help="print the verdict only")
    return parser


def _overrides(args: argparse.Namespace) -> dict[str, Any]:
    over: dict[str, Any] = {}
    if args.command in ("record", "serve", "both"):
        over["mode"] = args.command
    for key in ("count", "duration_s", "rate_per_sec", "seed", "start_time_ns"):
        value = getattr(args, key, None)
        if value is not None:
            over[key] = value
    # An explicit --count must clear a config-file duration_s, and vice versa, or
    # the mutual-exclusion check would reject a perfectly sensible override.
    if over.get("count") is not None:
        over["duration_s"] = None
    elif over.get("duration_s") is not None:
        over["count"] = None
    return over


def _load(args: argparse.Namespace) -> tuple[Any, FaultPlan]:
    overrides = _overrides(args)
    plan_path = getattr(args, "fault_plan", None)

    # Pass 1: validate the config as written, so a bad config is reported before
    # a fault plan is even read.
    config = load_config(args.config, overrides=overrides)

    if plan_path and not config.faults.enabled:
        raise ConfigError(
            "a --fault-plan was given but [faults].enabled is false; set "
            "faults.enabled = true so dataset_id records that this run injects "
            "faults, instead of a faulted dataset sharing an id with a clean one",
            code="CONFIG_FAULTS_DISABLED",
            plan_path=str(plan_path),
        )

    if not config.faults.enabled:
        return config, FaultPlan.empty()

    source = plan_path or config.faults.plan_path
    if source is None:
        raise ConfigError(
            "faults.enabled = true but no plan path is available",
            code="CONFIG_FAULT_PLAN_MISSING",
        )
    plan = load_fault_plan(source)
    # Pass 2: re-resolve with the plan fingerprint folded into dataset_id.
    config = load_config(
        args.config, overrides=overrides, fault_plan_fingerprint=plan.fingerprint()
    )
    return config, plan


async def _run_command(args: argparse.Namespace) -> int:
    from .engine import run_simulation

    config, plan = _load(args)
    result = await run_simulation(
        config, fault_plan=plan, run_instance_id=getattr(args, "run_id", None)
    )
    _emit(
        {
            "ok": result.ok,
            "state": result.state.value,
            "run_dir": str(result.run_dir),
            "report": result.report,
        },
        compact=args.compact,
    )
    return result.exit_code


async def _replay_command(args: argparse.Namespace) -> int:
    from .replay import replay_dataset

    config = load_config(args.config, overrides={"mode": "replay"})
    result = await replay_dataset(
        args.input,
        config,
        speed=args.speed,
        require_completed=not args.allow_incomplete,
        run_instance_id=args.run_id,
    )
    _emit(
        {
            "ok": result.ok,
            "state": result.status,
            "run_dir": str(result.run_dir),
            "report": result.report,
        },
        compact=args.compact,
    )
    return result.exit_code


def _verify_command(args: argparse.Namespace) -> int:
    result = verify_run(
        args.run_dir,
        max_line_bytes=args.max_line_bytes,
        check_anomalies=not args.no_anomalies,
    )
    if args.quiet:
        _emit(
            {
                "ok": result.ok,
                "verdict": result.verdict.value,
                "lines": result.lines,
                "sha256": result.sha256,
            },
            compact=args.compact,
        )
    else:
        _emit({"ok": result.ok, **result.as_dict()}, compact=args.compact)
    return 0 if result.verdict is Verdict.COMPLETE else 4


async def _benchmark_command(args: argparse.Namespace) -> int:
    from .engine import run_simulation

    config, plan = _load(args)
    if config.faults.enabled:
        # A benchmark with faults on measures something other than the system;
        # refuse rather than publish a number nobody can interpret (case C12).
        raise ConfigError(
            "benchmark refuses to run with faults enabled: the result would not "
            "describe normal-mode throughput",
            code="CONFIG_BENCHMARK_WITH_FAULTS",
        )

    runs: list[dict[str, Any]] = []
    exit_code = 0
    for index in range(max(1, args.repeat)):
        result = await run_simulation(config, fault_plan=plan)
        runs.append(
            {
                "repetition": index + 1,
                "warmup": bool(args.warmup and index == 0),
                "state": result.state.value,
                "run_dir": str(result.run_dir),
                "counts": result.report.get("counts"),
                "rates": result.report.get("rates"),
                "scheduler_lag_ms": result.report.get("scheduler_lag_ms"),
                "peak_rss_mb": (result.report.get("resources") or {}).get("peak_rss_mb"),
                "file": {
                    k: v
                    for k, v in (result.report.get("file") or {}).items()
                    if k in ("events_written", "bytes_written", "queue")
                },
            }
        )
        if not result.ok:
            exit_code = result.exit_code
            break

    measured = [r for r in runs if not r["warmup"] and r["state"] == "COMPLETED"]
    rates = sorted(
        r["rates"]["actual_generated_rate_per_sec"]
        for r in measured
        if (r.get("rates") or {}).get("actual_generated_rate_per_sec") is not None
    )
    summary = {
        "target_rate_per_sec": config.run.rate_per_sec,
        "repetitions": len(runs),
        "measured_repetitions": len(measured),
        "median_generated_rate_per_sec": rates[len(rates) // 2] if rates else None,
        "worst_generated_rate_per_sec": rates[0] if rates else None,
        "best_generated_rate_per_sec": rates[-1] if rates else None,
        "interpretation": (
            "Generated rate is logical slots produced per second of monotonic "
            "time. It does not claim any TCP client received anything, and it is "
            "only valid for the machine named in the run environment."
        ),
    }
    _emit({"ok": exit_code == 0, "summary": summary, "runs": runs}, compact=args.compact)
    return exit_code


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    compact = bool(getattr(args, "compact", False))
    try:
        if args.command in ("record", "serve", "both"):
            return asyncio.run(_run_command(args))
        if args.command == "replay":
            return asyncio.run(_replay_command(args))
        if args.command == "benchmark":
            return asyncio.run(_benchmark_command(args))
        if args.command == "verify":
            return _verify_command(args)
        parser.error(f"unknown command {args.command!r}")
        return 2
    except SimError as exc:
        return _fail(exc, compact=compact)
    except KeyboardInterrupt:
        _emit(
            {
                "ok": False,
                "code": "INTERRUPTED",
                "message": "interrupted before a clean drain; the run is incomplete",
                "exit_code": 6,
            },
            compact=compact,
        )
        return 6


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
