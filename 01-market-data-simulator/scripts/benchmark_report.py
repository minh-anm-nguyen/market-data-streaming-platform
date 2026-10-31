"""Run the load ladder from section 9.3 and emit a benchmark report.

    python scripts/benchmark_report.py --levels 100,1000,10000 --repeat 3
    python scripts/benchmark_report.py --levels 50000 --duration 10 --out report.json

What this prints is a measurement of *this machine*, with the environment
recorded alongside it. Three things it deliberately does not do:

* claim a level "passed" -- it reports the achieved rate and the lag, and leaves
  the comparison against the target to a human;
* measure record and serve together, because a combined number cannot be
  attributed to either path (section 9.3);
* run with faults enabled, which the CLI refuses outright (case C12).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from market_sim.config import load_config, runtime_fingerprint  # noqa: E402
from market_sim.engine import run_simulation  # noqa: E402


async def one_level(config_path: Path, rate: int, count: int, repeats: int) -> dict:
    runs = []
    for index in range(repeats):
        cfg = load_config(
            config_path,
            overrides={
                "mode": "record",
                "rate_per_sec": rate,
                "count": count,
                "duration_s": None,
            },
        )
        result = await run_simulation(cfg)
        runs.append(
            {
                "repetition": index + 1,
                "state": result.state.value,
                "achieved_rate": result.report["rates"]["actual_generated_rate_per_sec"],
                "recorded_events": result.report["counts"]["recorded_events"],
                "p95_lag_ms": result.report["p95_scheduler_lag_ms"],
                "p99_lag_ms": result.report["p99_scheduler_lag_ms"],
                "current_rss_mb": result.report["resources"]["current_rss_mb"],
                "peak_rss_mb_process_lifetime": result.report["resources"][
                    "peak_rss_mb"
                ],
                "peak_queue_bytes": (result.report["file"]["queue"] or {}).get(
                    "peak_bytes"
                ),
                "lag_breaches": result.report["metrics"]["counters"][
                    "scheduler_lag_breach_total"
                ],
                "error": (result.report.get("error") or {}).get("code"),
            }
        )
    ok = [r for r in runs if r["state"] == "COMPLETED"]
    achieved = sorted(r["achieved_rate"] for r in ok if r["achieved_rate"])
    median = statistics.median(achieved) if achieved else None
    return {
        "target_rate_per_sec": rate,
        "slots": count,
        "repetitions": repeats,
        "completed": len(ok),
        "median_achieved_rate": median,
        "worst_achieved_rate": achieved[0] if achieved else None,
        "best_achieved_rate": achieved[-1] if achieved else None,
        "median_p95_lag_ms": (
            statistics.median([r["p95_lag_ms"] or 0 for r in ok]) if ok else None
        ),
        # Process lifetime, not per level: it never decreases, so across
        # several levels in one process it describes the process.
        "peak_rss_mb_process_lifetime": (
            max((r["peak_rss_mb_process_lifetime"] or 0) for r in runs) if runs else None
        ),
        "max_current_rss_mb": max((r["current_rss_mb"] or 0) for r in runs) if runs else None,
        "achieved_fraction_of_target": round(median / rate, 4) if median else None,
        "runs": runs,
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", default=str(ROOT / "config" / "bench.toml"), help="base config"
    )
    parser.add_argument(
        "--levels",
        default="100,1000,10000",
        help="comma-separated target rates (section 9.3 uses 1k, 10k, 50k, 120k)",
    )
    parser.add_argument(
        "--duration", type=float, default=3.0, help="seconds of logical time per level"
    )
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--out", help="write the full report here as JSON")
    args = parser.parse_args()

    levels = [int(x) for x in args.levels.split(",") if x.strip()]
    report = {
        "project": "fx-cfd-market-data-simulator",
        "profile": "record-only",
        "environment": runtime_fingerprint(),
        "method": (
            "Each level runs 'repeat' times at a fixed slot count. The median and "
            "the worst run are both reported. A level is NOT marked pass or fail: "
            "the achieved fraction of the target is the result."
        ),
        "levels": [],
    }
    for rate in levels:
        count = max(1, int(rate * args.duration))
        summary = await one_level(Path(args.config), rate, count, args.repeat)
        report["levels"].append(summary)
        median = summary["median_achieved_rate"] or 0
        fraction = summary["achieved_fraction_of_target"]
        print(
            f"target={rate:>8}/s  median={median:>10.0f}/s  "
            f"({fraction if fraction is not None else 'n/a'} of target)  "
            f"p95_lag={summary['median_p95_lag_ms']}ms  "
            f"rss={summary['max_current_rss_mb']}MB"
        )

    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"\nfull report written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
