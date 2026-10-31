"""Produce TEST_REPORT.yaml from a pytest run plus one measured reference run.

    python scripts/make_test_report.py --pytest-output suite.txt

The report follows the template in section 13 of the design document. Fields it
cannot measure are left null rather than filled with a plausible-looking number:
a null says "not measured", which is a different and more useful statement than
a guess.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

from conftest import config_dict  # noqa: E402
from market_sim.config import build_config, runtime_fingerprint  # noqa: E402
from market_sim.engine import run_simulation  # noqa: E402
from market_sim.verify import verify_run  # noqa: E402

SUMMARY = re.compile(
    r"(?:(?P<failed>\d+) failed,?\s*)?(?P<passed>\d+) passed"
    r"(?:,\s*(?P<skipped>\d+) skipped)?"
    r".*?in (?P<seconds>[\d.]+)s"
)
MEASUREMENT = re.compile(r"^(L0|L1|L2|CEILING|E2E-13) (\{.*\})$", re.M)


def parse_pytest(text: str) -> dict:
    counts = {"passed": None, "failed": None, "skipped": None, "duration_s": None}
    for match in SUMMARY.finditer(text):
        counts = {
            "passed": int(match.group("passed")),
            "failed": int(match.group("failed") or 0),
            "skipped": int(match.group("skipped") or 0),
            "duration_s": float(match.group("seconds")),
        }
    measurements = {}
    for label, blob in MEASUREMENT.findall(text):
        try:
            measurements[label] = json.loads(blob)
        except ValueError:
            pass
    return {"counts": counts, "measurements": measurements}


#: Target rate for the reference run. Deliberately *within* this machine's
#: measured capacity (see README section 6) so the rate and lag fields describe
#: a normal run. A saturating target would fill them with the numbers of an
#: overload test, which is a different thing to report.
REFERENCE_RATE = 5_000
REFERENCE_SLOTS = 50_000


async def reference_run(tmp: Path) -> dict:
    """One clean record run at a sustainable rate, measured in a fresh process."""
    cfg = build_config(
        config_dict(mode="record", count=REFERENCE_SLOTS, rate=REFERENCE_RATE),
        base_dir=tmp,
    )
    result = await run_simulation(cfg)
    verdict = verify_run(result.run_dir)
    report = result.report
    return {
        "note": (
            "A clean record-only run at a rate this machine can sustain. "
            "For saturation numbers see load_ladder below."
        ),
        "run_instance_id": report["run_instance_id"],
        "dataset_id": report["dataset_id"],
        "config_sha256": report["config_sha256"],
        "seed": report["seed"],
        "logical_start_time_ns": report["logical_start_time_ns"],
        "mode": report["mode"],
        "target_rate_per_sec": report["target_rate_per_sec"],
        "actual_generated_rate_per_sec": report["rates"][
            "actual_generated_rate_per_sec"
        ],
        "actual_recorded_rate_per_sec": report["rates"]["actual_recorded_rate_per_sec"],
        "logical_slots": report["counts"]["logical_slots"],
        "emitted_events": report["counts"]["emitted_events"],
        "intentional_omissions": report["counts"]["intentional_omissions"],
        "duplicates": report["counts"]["intentional_duplicates"],
        "recorded_events": report["counts"]["recorded_events"],
        "fsynced_events": report["counts"]["fsynced_events"],
        "sha256_events_jsonl": report["file"]["sha256"],
        "p95_scheduler_lag_ms": report["p95_scheduler_lag_ms"],
        "p99_scheduler_lag_ms": report["p99_scheduler_lag_ms"],
        "current_rss_mb": report["resources"]["current_rss_mb"],
        "peak_rss_mb_process_lifetime": report["resources"]["peak_rss_mb"],
        "peak_file_queue_bytes": report["file"]["queue"]["peak_bytes"],
        "peak_tcp_queue_bytes": None,
        "tcp_disconnects": report["metrics"]["counters"][
            "tcp_clients_disconnected_total"
        ],
        "file_errors": report["metrics"]["counters"]["file_write_errors_total"],
        "verify_verdict": verdict.verdict.value,
        "counts_reconcile": report["counts"]["reconciles"],
    }


def to_yaml(value, indent: int = 0) -> str:
    """A small YAML emitter for the shapes this report uses."""
    pad = "  " * indent
    if isinstance(value, dict):
        if not value:
            return "{}"
        lines = []
        for key, item in value.items():
            if isinstance(item, (dict, list)) and item:
                lines.append(f"{pad}{key}:")
                lines.append(to_yaml(item, indent + 1))
            else:
                lines.append(f"{pad}{key}: {to_yaml(item, indent + 1)}")
        return "\n".join(lines)
    if isinstance(value, list):
        if not value:
            return "[]"
        return "\n".join(f"{pad}- {to_yaml(item, 0)}" for item in value)
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if any(ch in text for ch in ':#') or text != text.strip() or not text:
        return json.dumps(text)
    return text


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pytest-output", required=True, help="file holding the pytest run output"
    )
    parser.add_argument("--out", default=str(ROOT / "TEST_REPORT.yaml"))
    args = parser.parse_args()

    parsed = parse_pytest(
        Path(args.pytest_output).read_text(encoding="utf-8", errors="replace")
    )
    counts = parsed["counts"]
    if counts["passed"] is None:
        status = "NOT_RUN"
    elif counts["failed"]:
        status = "FAIL"
    else:
        status = "PASS"

    with tempfile.TemporaryDirectory() as tmp:
        reference = await reference_run(Path(tmp))

    env = runtime_fingerprint()
    report = {
        "project": "fx-cfd-market-data-simulator",
        "version": env["schema_version"],
        "generator_version": env["generator_version"],
        "prng": "splitmix64",
        "status": status,
        "note": (
            "Generated by scripts/make_test_report.py. A null means not measured, "
            "which is a different statement from zero."
        ),
        "environment": {
            "python_version": env["python_version"],
            "python_version_in_design_document": "3.12",
            "platform": env["platform"],
            "cpu": env["processor"],
            "cpu_count": env["cpu_count"],
            "ram_gb": None,
            "disk_type": None,
        },
        "test_suite": {
            "command": "pytest tests -q",
            "passed": counts["passed"],
            "failed": counts["failed"],
            "skipped": counts["skipped"],
            "wall_clock_s": counts["duration_s"],
        },
        "reference_run": reference,
        "load_ladder": parsed["measurements"],
        "not_achieved": [
            "L3 50,000 events/s: not reached on this machine",
            "L4 120,000 events/s: not reached on this machine",
            "6-24 hour soak (section 9.3): not run",
            "real full filesystem: ENOSPC injected at the syscall boundary instead",
            "power-loss durability: not run",
            "byte-for-byte equivalence on Python 3.12: not measured",
        ],
        "known_limitations": [
            "No crash-resume; a killed run is regenerated from seed (ADR-009).",
            "No application ACK on TCP, so no event is proven received.",
            "One TCP client; a second connection is rejected, not fanned out.",
            "Synthetic prices. Not market data, not usable for trading.",
        ],
    }
    Path(args.out).write_text(to_yaml(report) + "\n", encoding="utf-8")
    print(
        f"{args.out}: status={status} passed={counts['passed']} "
        f"failed={counts['failed']} skipped={counts['skipped']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
