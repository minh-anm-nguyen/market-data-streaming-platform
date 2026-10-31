"""Verify one or many run directories and print a summary table.

    python scripts/verify_dataset.py runs/run-...            # one run
    python scripts/verify_dataset.py runs --all              # every run under a root
    python scripts/verify_dataset.py runs --all --json       # machine-readable

Exit status is 0 only if every run verified COMPLETE, so this is usable as a
release gate (section 12.1).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from market_sim.recorder import MANIFEST_NAME  # noqa: E402
from market_sim.verify import Verdict, verify_run  # noqa: E402


def discover(root: Path) -> list[Path]:
    """Every directory under ``root`` that looks like a run directory."""
    if (root / MANIFEST_NAME).is_file():
        return [root]
    return sorted(
        p for p in root.iterdir() if p.is_dir() and (p / MANIFEST_NAME).is_file()
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", help="a run directory, or a root containing them")
    parser.add_argument(
        "--all", action="store_true", help="verify every run directory under path"
    )
    parser.add_argument("--json", action="store_true", help="emit JSON, not a table")
    parser.add_argument("--max-line-bytes", type=int, default=1 << 20)
    parser.add_argument(
        "--no-anomalies",
        action="store_true",
        help="skip per-symbol gap tracking (constant memory, for huge files)",
    )
    args = parser.parse_args()

    root = Path(args.path).resolve()
    if not root.exists():
        print(f"not found: {root}", file=sys.stderr)
        return 2
    targets = discover(root) if args.all else [root]
    if not targets:
        print(f"no run directories found under {root}", file=sys.stderr)
        return 2

    results = [
        verify_run(
            target,
            max_line_bytes=args.max_line_bytes,
            check_anomalies=not args.no_anomalies,
        )
        for target in targets
    ]

    if args.json:
        print(json.dumps([r.as_dict() for r in results], indent=2))
    else:
        width = max(len(r.run_dir.name) for r in results)
        print(f"{'run':<{width}}  {'verdict':<10} {'lines':>9}  anomalies")
        for result in results:
            totals = result.totals or {}
            anomalies = ", ".join(
                f"{key}={totals[key]}"
                for key in (
                    "gaps",
                    "duplicates",
                    "conflicting_duplicates",
                    "out_of_order",
                    "event_time_regressions",
                )
                if totals.get(key)
            )
            print(
                f"{result.run_dir.name:<{width}}  {result.verdict.value:<10} "
                f"{result.lines:>9}  {anomalies or '-'}"
            )
            for reason in result.reasons:
                print(f"    FAIL  {reason}")
            for warning in result.warnings:
                print(f"    warn  {warning}")

    failed = [r for r in results if r.verdict is not Verdict.COMPLETE]
    if failed:
        print(
            f"\n{len(failed)} of {len(results)} run(s) did not verify COMPLETE",
            file=sys.stderr,
        )
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
