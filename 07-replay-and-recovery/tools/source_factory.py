"""Fabricate P01-shaped sealed run directories for tests and fixtures.

P07's tests need a *sealed source* and need to be able to damage it in precise
ways. Depending on P01 being installed would couple P07's unit tests to another
project's build, so this writes the same on-disk shape P01 commits:
``manifest.json``, ``events.jsonl``, ``report.json``, ``COMPLETED`` and an
optional ``fault-manifest.jsonl``.

Two things it is careful about, because tests built on a sloppy fixture prove
nothing:

* **The JSONL is canonical ``sim.v1``** -- sorted keys, no spaces, the exact
  field allowlist per event kind. Written through ``json.dumps(sort_keys=True,
  separators=(",", ":"))``, which is the reference form P01's own tests compare
  their fast path against.
* **Sequences run per symbol across both kinds**, which is P01's contract and
  the reason a tick-only Kafka scan sees apparent gaps (case ``E2E-24``).

It is a fabricator, not a simulator: the price walk is a seeded integer random
walk with no market model in it, and ``tests/integration`` separately checks the
sealer and oracle against output from the real P01 when it is importable.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

SCHEMA_VERSION = "sim.v1"

#: Mirrors the three-symbol fixture P01's ``bench.toml`` uses, including the
#: different ``price_scale`` on USDJPY -- which is what makes a mixed-scale bug
#: visible instead of theoretical.
DEFAULT_SYMBOLS: tuple[dict[str, Any], ...] = (
    {
        "symbol": "EURUSD",
        "price_scale": 100000,
        "initial_price_ticks": 108347,
        "min_price_ticks": 100000,
        "max_price_ticks": 120000,
        "min_spread_ticks": 1,
        "max_spread_ticks": 5,
        "max_step_ticks": 8,
        "weight": 3,
    },
    {
        "symbol": "GBPUSD",
        "price_scale": 100000,
        "initial_price_ticks": 126540,
        "min_price_ticks": 115000,
        "max_price_ticks": 140000,
        "min_spread_ticks": 1,
        "max_spread_ticks": 8,
        "max_step_ticks": 10,
        "weight": 2,
    },
    {
        "symbol": "USDJPY",
        "price_scale": 1000,
        "initial_price_ticks": 151234,
        "min_price_ticks": 140000,
        "max_price_ticks": 160000,
        "min_spread_ticks": 1,
        "max_spread_ticks": 6,
        "max_step_ticks": 12,
        "weight": 1,
    },
)

DEFAULT_START_NS = 1_793_322_000_000_000_000


@dataclass(frozen=True, slots=True)
class FaultOp:
    """One edit to apply to the generated event list.

    ``declared`` controls whether the edit is written to
    ``fault-manifest.jsonl``. An *undeclared* edit is how cases ``P07-G01-05``
    and ``P07-G01-06`` are built: the oracle must notice an anomaly the manifest
    does not explain.
    """

    kind: str
    symbol: str
    sequence: int
    declared: bool = True
    params: dict[str, Any] = field(default_factory=dict)


def _canonical(event: dict[str, Any]) -> str:
    return json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def generate_events(
    *,
    dataset_id: str,
    count: int,
    symbols: Sequence[dict[str, Any]] = DEFAULT_SYMBOLS,
    quote_ratio_pct: int = 80,
    seed: int = 20261030,
    start_time_ns: int = DEFAULT_START_NS,
    interval_ns: int = 1_000_000,
) -> list[dict[str, Any]]:
    """Generate ``count`` canonical records, one per logical slot.

    ``interval_ns`` is the gap between slots, so a 1 ms interval and 120000
    slots span two 1-minute bars -- which is how the bar tests get more than one
    bucket without a huge fixture.
    """
    rng = random.Random(seed)
    weighted: list[dict[str, Any]] = []
    for config in symbols:
        weighted.extend([config] * int(config.get("weight", 1)))
    state = {config["symbol"]: int(config["initial_price_ticks"]) for config in symbols}
    sequences = {config["symbol"]: 0 for config in symbols}

    events: list[dict[str, Any]] = []
    for slot in range(count):
        config = weighted[slot % len(weighted)]
        symbol = config["symbol"]
        step = rng.randint(-config["max_step_ticks"], config["max_step_ticks"])
        mid = min(max(state[symbol] + step, config["min_price_ticks"]), config["max_price_ticks"])
        state[symbol] = mid
        sequences[symbol] += 1
        sequence = sequences[symbol]
        event_time_ns = start_time_ns + slot * interval_ns

        if rng.randrange(100) < quote_ratio_pct:
            spread = rng.randint(config["min_spread_ticks"], config["max_spread_ticks"])
            half = spread // 2
            bid = min(
                max(mid - half, config["min_price_ticks"]),
                config["max_price_ticks"] - (spread - half),
            )
            events.append(
                {
                    "ask_ticks": bid + spread,
                    "bid_ticks": bid,
                    "dataset_id": dataset_id,
                    "event_time_ns": event_time_ns,
                    "event_type": "quote",
                    "price_scale": config["price_scale"],
                    "schema_version": SCHEMA_VERSION,
                    "sequence": sequence,
                    "symbol": symbol,
                }
            )
        else:
            events.append(
                {
                    "dataset_id": dataset_id,
                    "event_time_ns": event_time_ns,
                    "event_type": "tick",
                    "price_scale": config["price_scale"],
                    "price_ticks": mid,
                    "schema_version": SCHEMA_VERSION,
                    "sequence": sequence,
                    "symbol": symbol,
                }
            )
    return events


def apply_faults(
    events: Sequence[dict[str, Any]], faults: Sequence[FaultOp]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Apply ``faults`` to ``events``. Returns ``(emitted, manifest_records)``.

    A fault aimed at a slot that does not exist is recorded as planned but **not
    applied**, exactly as P01 does (its cases S11 and C04). Silently counting it
    as a success is how a fixture ends up claiming an anomaly it never contained.
    """
    emitted = [dict(event) for event in events]
    records: list[dict[str, Any]] = []

    def find(symbol: str, sequence: int) -> int | None:
        for index, event in enumerate(emitted):
            if event["symbol"] == symbol and event["sequence"] == sequence:
                return index
        return None

    for number, fault in enumerate(faults, start=1):
        fault_id = f"f{number:04d}"
        index = find(fault.symbol, fault.sequence)
        applied = False
        detail: dict[str, Any] = dict(fault.params)

        if index is None:
            applied = False
        elif fault.kind == "missing_sequence":
            emitted.pop(index)
            applied = True
        elif fault.kind == "duplicate_exact":
            emitted.insert(index + 1, dict(emitted[index]))
            applied = True
        elif fault.kind == "duplicate_conflict":
            clone = dict(emitted[index])
            if clone["event_type"] == "quote":
                delta = int(fault.params.get("bid_delta_ticks", 7))
                clone["bid_ticks"] += delta
                clone["ask_ticks"] += delta
            else:
                clone["price_ticks"] += int(fault.params.get("price_delta_ticks", 7))
            emitted.insert(index + 1, clone)
            applied = True
        elif fault.kind == "reorder_adjacent":
            if index + 1 < len(emitted):
                emitted[index], emitted[index + 1] = emitted[index + 1], emitted[index]
                applied = True
        elif fault.kind == "stale_timestamp":
            back = int(fault.params.get("back_ns", 10_000_000_000))
            emitted[index]["event_time_ns"] -= back
            applied = True
        elif fault.kind == "invalid_spread":
            if emitted[index]["event_type"] == "quote":
                emitted[index]["ask_ticks"] = emitted[index]["bid_ticks"] - 1
                applied = True
        elif fault.kind == "invalid_price":
            if emitted[index]["event_type"] == "tick":
                emitted[index]["price_ticks"] = int(fault.params.get("price_ticks", -1))
            else:
                emitted[index]["bid_ticks"] = int(fault.params.get("bid_ticks", -1))
            applied = True
        elif fault.kind == "bad_scale":
            emitted[index]["price_scale"] = int(fault.params.get("price_scale", 0))
            applied = True
        elif fault.kind == "unknown_symbol":
            emitted[index]["symbol"] = str(fault.params.get("symbol", "ZZZZZZ"))
            applied = True
        elif fault.kind == "same_nanosecond":
            if index > 0:
                emitted[index]["event_time_ns"] = emitted[index - 1]["event_time_ns"]
                applied = True
        else:
            raise ValueError(f"unknown fault kind {fault.kind!r}")

        if fault.declared:
            records.append(
                {
                    "fault_id": fault_id,
                    "kind": fault.kind,
                    "applied": applied,
                    "symbol": fault.symbol,
                    "sequence": fault.sequence,
                    **detail,
                }
            )
    return emitted, records


def write_run_dir(
    run_dir: str | Path,
    *,
    dataset_id: str = "demo-fx-001",
    count: int = 2000,
    symbols: Sequence[dict[str, Any]] = DEFAULT_SYMBOLS,
    quote_ratio_pct: int = 80,
    seed: int = 20261030,
    start_time_ns: int = DEFAULT_START_NS,
    interval_ns: int = 1_000_000,
    faults: Sequence[FaultOp] = (),
    fault_plan_fingerprint: str | None = None,
    run_instance_id: str = "fixture-1",
    completed: bool = True,
    leave_part_file: bool = False,
    corrupt_report_hash: bool = False,
    report_line_delta: int = 0,
    schema_version: str = SCHEMA_VERSION,
) -> dict[str, Any]:
    """Write a complete sealed run directory. Returns its anchor metadata.

    The last four arguments exist to build the *unsealable* fixtures the matrix
    needs: a leftover ``.part`` (``P07-G01-01``), a report whose line count
    disagrees with the file (``P07-G01-02``), a wrong recorded hash, and a run
    with no ``COMPLETED`` marker.
    """
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)

    events = generate_events(
        dataset_id=dataset_id,
        count=count,
        symbols=symbols,
        quote_ratio_pct=quote_ratio_pct,
        seed=seed,
        start_time_ns=start_time_ns,
        interval_ns=interval_ns,
    )
    emitted, fault_records = apply_faults(events, faults)

    payload = "".join(_canonical(event) + "\n" for event in emitted).encode("utf-8")
    events_path = run_dir / "events.jsonl"
    events_path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()

    if leave_part_file:
        (run_dir / "events.jsonl.part").write_bytes(b"{partial")

    fingerprint = fault_plan_fingerprint or (
        hashlib.sha256(
            json.dumps([f.kind + f.symbol + str(f.sequence) for f in faults], sort_keys=True).encode()
        ).hexdigest()[:16]
        if faults
        else "none"
    )

    manifest = {
        "config_resolved": {
            "data": {
                "count": count,
                "faults_enabled": bool(faults),
                "fault_plan_fingerprint": fingerprint,
                "generator_version": "fixture.1",
                "quote_ratio_pct": quote_ratio_pct,
                "schema_version": schema_version,
                "seed": seed,
                "start_time_ns": start_time_ns,
                "symbol_order": [config["symbol"] for config in symbols],
                "symbols": [dict(config) for config in symbols],
            }
        },
        "config_sha256": hashlib.sha256(
            json.dumps([dict(config) for config in symbols], sort_keys=True).encode()
        ).hexdigest(),
        "dataset_id": dataset_id,
        "fault_plan_fingerprint": fingerprint,
        "faults_enabled": bool(faults),
        "faults_planned": len(faults),
        "logical_start_time_ns": start_time_ns,
        "mode": "record",
        "planned_logical_slots": count,
        "run_instance_id": run_instance_id,
        "runtime": {
            "generator_version": "fixture.1",
            "platform": "p07-test-fixture",
            "python_implementation": "CPython",
            "schema_version": schema_version,
        },
        "schema_version": schema_version,
        "seed": seed,
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )

    report = {
        "counts": {
            "emitted_events": len(emitted),
            "logical_slots": count,
            "intentional_omissions": sum(1 for f in faults if f.kind == "missing_sequence"),
            "intentional_duplicates": sum(
                1 for f in faults if f.kind in {"duplicate_exact", "duplicate_conflict"}
            ),
        },
        "dataset_id": dataset_id,
        "faults": {
            "enabled": bool(faults),
            "planned": len(faults),
            "applied": sum(1 for record in fault_records if record["applied"]),
            "records": fault_records,
        },
        "file": {
            "commit": {
                "completed_marker": completed,
                "directory_fsync": os.name == "posix",
                "directory_fsync_supported": os.name == "posix",
                "events_bytes": len(payload),
                "events_lines": len(emitted) + report_line_delta,
                "events_renamed": True,
                "events_sha256": ("0" * 64) if corrupt_report_hash else digest,
                "report_written": True,
            },
            "events_path": str(events_path),
            "sha256": digest,
        },
    }
    (run_dir / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
    )

    if fault_records:
        (run_dir / "fault-manifest.jsonl").write_text(
            "".join(json.dumps(record, sort_keys=True) + "\n" for record in fault_records),
            encoding="utf-8",
        )
    if completed:
        (run_dir / "COMPLETED").write_text("", encoding="utf-8")

    return {
        "run_dir": str(run_dir),
        "dataset_id": dataset_id,
        "events_sha256": digest,
        "events": len(emitted),
        "bytes": len(payload),
        "fault_records": fault_records,
    }


def tamper_events(run_dir: str | Path, *, line_index: int, replacement: str) -> None:
    """Rewrite one line of a sealed dataset *without* fixing the report.

    Produces the fixture for case ``P07-G01-03``'s first half: the file no
    longer matches its own report, which sealing must catch. Rewriting the
    report as well -- the second half of that case -- is what the external
    anchor is for.
    """
    path = Path(run_dir) / "events.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines()
    lines[line_index] = replacement
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def reseal_after_tamper(run_dir: str | Path) -> str:
    """Recompute the report's hash and line count after tampering.

    This is the attacker who can edit everything inside the bundle. Sealing then
    succeeds and only a digest pinned *outside* the directory can tell -- which
    is the point the function exists to demonstrate.
    """
    run_dir = Path(run_dir)
    payload = (run_dir / "events.jsonl").read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    report = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
    report["file"]["commit"]["events_sha256"] = digest
    report["file"]["commit"]["events_bytes"] = len(payload)
    report["file"]["commit"]["events_lines"] = payload.decode("utf-8").count("\n")
    report["file"]["sha256"] = digest
    (run_dir / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
    )
    return digest
