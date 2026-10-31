"""Regenerate the checked-in fixtures and the external anchor file.

Run from the project root::

    python tools/make_fixtures.py

Writes ``fixtures/<dataset>/`` and ``fixtures/ANCHORS.json``. The anchor file
lives *beside* the run directories rather than inside them, which is the whole
point: a digest stored in the directory it vouches for proves nothing once
someone can write to that directory (design 5.1, case ``P07-G01-03``).

The generator is seeded, so the digests are stable and the scenario files can
pin them. If a digest here changes without the generator changing, something
else did -- which is exactly what the pinned value is for.
"""

from __future__ import annotations

import json
from pathlib import Path

from .source_factory import FaultOp, write_run_dir

FIXTURE_ROOT = Path("fixtures")


def build() -> dict[str, dict[str, object]]:
    anchors: dict[str, dict[str, object]] = {}

    # A clean two-minute dataset across three symbols and two price scales.
    clean = write_run_dir(
        FIXTURE_ROOT / "demo-fx-001",
        dataset_id="demo-fx-001",
        count=2000,
        interval_ns=50_000_000,
        run_instance_id="demo-fx-001",
    )
    anchors["demo-fx-001"] = {
        "events_sha256": clean["events_sha256"],
        "events": clean["events"],
        "purpose": "clean baseline; every accepted identity should reach READY",
    }

    # The same shape with declared anomalies, so the policy oracle has something
    # to classify and the fault manifest has something to explain.
    faulted = write_run_dir(
        FIXTURE_ROOT / "demo-fx-002-faults",
        dataset_id="demo-fx-002",
        count=1200,
        interval_ns=50_000_000,
        run_instance_id="demo-fx-002-faults",
        faults=(
            FaultOp("missing_sequence", "EURUSD", 40),
            FaultOp("duplicate_exact", "GBPUSD", 20),
            FaultOp("stale_timestamp", "USDJPY", 15, params={"back_ns": 10_000_000_000}),
            FaultOp("invalid_spread", "EURUSD", 60),
            FaultOp("invalid_price", "GBPUSD", 35),
        ),
    )
    anchors["demo-fx-002"] = {
        "events_sha256": faulted["events_sha256"],
        "events": faulted["events"],
        "purpose": "declared anomalies; suppressed and rejected events must not reach Kafka",
    }

    # A dataset with an *undeclared* duplicate conflict: the fixture claims to be
    # clean and is not, which the oracle must notice (case P07-G01-05).
    untracked = write_run_dir(
        FIXTURE_ROOT / "demo-fx-003-untracked",
        dataset_id="demo-fx-003",
        count=400,
        interval_ns=50_000_000,
        run_instance_id="demo-fx-003-untracked",
        faults=(FaultOp("duplicate_conflict", "EURUSD", 10, declared=False),),
    )
    anchors["demo-fx-003"] = {
        "events_sha256": untracked["events_sha256"],
        "events": untracked["events"],
        "purpose": "undeclared identity conflict; sealing succeeds, the oracle must still refuse",
    }

    (FIXTURE_ROOT / "ANCHORS.json").write_text(
        json.dumps(
            {
                "note": (
                    "External trust anchors for the fixtures. Kept outside each run directory on "
                    "purpose; a bundle that vouches for itself proves nothing."
                ),
                "datasets": anchors,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return anchors


if __name__ == "__main__":
    for name, info in build().items():
        print(f"{name}  {info['events_sha256']}  {info['events']} events")
