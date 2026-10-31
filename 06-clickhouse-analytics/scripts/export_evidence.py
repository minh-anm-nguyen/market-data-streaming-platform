"""Run the suite and export an evidence ledger plus a GO/NO-GO summary.

Section 14.2 and 21. The summary is deliberately pessimistic: it reports
``INCONCLUSIVE`` whenever any ClickHouse-semantics or scale case was not run,
because an un-run test is ``NOT_RUN`` and a release gate that treats silence as
success is worse than no gate.

Run::

    python scripts/export_evidence.py --out .artifacts/evidence
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / "src"), str(REPO / "tests")]

from md_analytics.config import load_config  # noqa: E402
from md_analytics.errors import Verdict  # noqa: E402
from md_analytics.schema_preflight import (  # noqa: E402
    expected_canonical_digest,
    expected_raw_digest,
)
from md_analytics.sqlbank import SqlBank  # noqa: E402


def run_pytest(args: list[str]) -> tuple[int, str]:
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", *args],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    return completed.returncode, completed.stdout + completed.stderr


def parse_counts(output: str) -> dict[str, int]:
    """Pull the pass/fail/skip counts out of pytest's summary line."""
    counts: dict[str, int] = {}
    for line in reversed(output.splitlines()):
        if " passed" in line or " failed" in line or " skipped" in line:
            for token in line.replace("=", " ").split():
                if token.isdigit():
                    current = int(token)
                elif token.rstrip(",") in ("passed", "failed", "skipped", "error", "errors"):
                    counts[token.rstrip(",")] = current
            if counts:
                break
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=".artifacts/evidence")
    parser.add_argument(
        "--clickhouse-url",
        default=None,
        help="if set, also run tests/clickhouse against it",
    )
    args = parser.parse_args(argv)

    out = REPO / args.out
    out.mkdir(parents=True, exist_ok=True)

    suites: dict[str, dict] = {}
    for name, target in (
        ("unit", "tests/unit"),
        ("e2e", "tests/e2e"),
        ("property", "tests/property"),
        ("chaos", "tests/chaos"),
        ("registry", "tests/registry"),
    ):
        code, output = run_pytest([target])
        suites[name] = {"exit_code": code, "counts": parse_counts(output)}
        (out / f"{name}.log").write_text(output, encoding="utf-8")

    clickhouse_ran = False
    if args.clickhouse_url:
        import os

        env_before = os.environ.get("P06_CLICKHOUSE_URL")
        os.environ["P06_CLICKHOUSE_URL"] = args.clickhouse_url
        code, output = run_pytest(["tests/clickhouse"])
        if env_before is None:
            os.environ.pop("P06_CLICKHOUSE_URL", None)
        else:
            os.environ["P06_CLICKHOUSE_URL"] = env_before
        counts = parse_counts(output)
        suites["clickhouse"] = {"exit_code": code, "counts": counts}
        (out / "clickhouse.log").write_text(output, encoding="utf-8")
        clickhouse_ran = counts.get("passed", 0) > 0

    coverage_path = REPO / "tests" / "registry" / "coverage_report.json"
    coverage = (
        json.loads(coverage_path.read_text(encoding="utf-8"))
        if coverage_path.exists()
        else {}
    )

    failed = [name for name, result in suites.items() if result["exit_code"] != 0]
    if failed:
        verdict = Verdict.FAIL
        reason = f"suites failed: {', '.join(failed)}"
    elif not clickhouse_ran:
        verdict = Verdict.INCONCLUSIVE
        reason = (
            "every correctness suite passed, but no ClickHouse-backed test ran. "
            "ClickHouse-semantics, partition/index, scale and latency cases are "
            "NOT_RUN, which is not the same as passing (section 14.2). A GO needs "
            "the integration and capacity profiles of section 2.2."
        )
    else:
        verdict = Verdict.INCONCLUSIVE
        reason = (
            "correctness and ClickHouse integration passed. A GO still needs the "
            "200M benchmark, the mixed-load profile and the HA cases of section "
            "21.1, which this host cannot run."
        )

    bank = SqlBank(REPO / "sql")
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "release_verdict": str(verdict),
        "release_verdict_reason": reason,
        "suites": suites,
        "coverage": {
            "executed_here": coverage.get("executed_here"),
            "matrix_cases": coverage.get("matrix", {}).get("cases"),
            "declared_unexecutable": coverage.get("declared_unexecutable"),
        },
        "provenance": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "raw_schema_digest": expected_raw_digest(),
            "canonical_schema_digest": expected_canonical_digest(),
            "sql_digests": bank.digests(),
            "config_sha_dev": load_config(REPO / "config" / "dev.toml").sha256(),
            "clickhouse_backed": clickhouse_ran,
        },
        "notes": [
            "A skipped test is NOT_RUN. It is never a pass.",
            "Results produced against the in-process model are evidence about "
            "this pipeline's logic, never about ClickHouse (sections 4 and 22).",
            "No performance claim appears here: the benchmark is a separate "
            "target and reports measured_on_new_profile.",
        ],
    }
    (out / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    json.dump(summary, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")

    return 0 if verdict is Verdict.PASS else (1 if verdict is Verdict.FAIL else 4)


if __name__ == "__main__":
    raise SystemExit(main())
