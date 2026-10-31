#!/usr/bin/env python3
"""Diff a candidate schema's descriptor against the released baseline.

A standalone entry point for the gate, so CI can call it without the rest of
the package on the path:

    python scripts/compare_descriptor.py build/descriptors/marketdata-v1_1.desc

Exit codes: 0 if nothing is forbidden, 1 if the change is blocked, 2 on bad
usage. CONDITIONAL findings are printed and do **not** block -- they need a
human decision, which is the point of the severity (section 5.3).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python" / "src"))

from md_contract.schema_gate import Severity, gate  # noqa: E402

DEFAULT_BASELINE = ROOT / "toolchain" / "descriptor-baselines" / "marketdata-v1.desc"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("candidate", help="descriptor set to check")
    parser.add_argument("--baseline", default=str(DEFAULT_BASELINE))
    parser.add_argument(
        "--expect-blocked",
        action="store_true",
        help="invert the exit code: for the negative fixtures, which MUST be blocked",
    )
    args = parser.parse_args(argv)

    passed, findings = gate(args.baseline, args.candidate)
    width = max((len(f.severity.value) for f in findings), default=0)
    for finding in findings:
        print(f"[{finding.severity.value:<{width}}] {finding.case or '--':<4} {finding.message}")
    if not findings:
        print("no differences from the baseline")

    blocking = [f for f in findings if f.severity is Severity.FORBIDDEN]
    print(
        f"\n{len(findings)} finding(s), {len(blocking)} blocking -> "
        f"{'PASS' if passed else 'BLOCKED'}"
    )
    if args.expect_blocked:
        if passed:
            print("FAILURE: this schema was expected to be blocked and was not")
            return 1
        return 0
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
