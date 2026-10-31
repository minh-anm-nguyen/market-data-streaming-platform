"""Sealing: what the harness refuses to call a source of truth."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

import pytest

from p07.dataset_sealer import (
    SealError,
    check_dataset_id_collision,
    seal_dataset,
)
from p07.evidence.verdicts import Reason
from tools.source_factory import FaultOp, reseal_after_tamper, tamper_events


def test_clean_source_seals_with_an_anchor(make_source: Callable[..., dict[str, Any]]) -> None:
    info = make_source(dataset_id="ok-1")
    dataset = seal_dataset(info["run_dir"], anchor=info["events_sha256"])
    assert dataset.anchor_verified is True
    assert dataset.event_lines == info["events"]
    assert dataset.events_sha256 == info["events_sha256"]
    assert set(dataset.symbols) == {"EURUSD", "GBPUSD", "USDJPY"}
    receipt = dataset.seal_receipt()
    assert receipt["anchor_verified"] is True
    assert receipt["dataset_id"] == "ok-1"


@pytest.mark.whatif("P07-G01-03")
def test_sealing_without_an_anchor_records_the_gap(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    """No external anchor means self-consistent only, and the receipt says so."""
    info = make_source()
    dataset = seal_dataset(info["run_dir"])
    assert dataset.anchor_verified is False
    assert dataset.seal_receipt()["anchor_verified"] is False


@pytest.mark.whatif("P07-G01-01")
def test_part_file_blocks_sealing(make_source: Callable[..., dict[str, Any]]) -> None:
    """A leftover ``.part`` is an open writer's buffer, never a dataset."""
    info = make_source(leave_part_file=True)
    with pytest.raises(SealError) as exc:
        seal_dataset(info["run_dir"])
    assert exc.value.reason == Reason.SOURCE_PART_FILE_PRESENT


def test_missing_completed_marker_blocks_sealing(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    info = make_source(completed=False)
    with pytest.raises(SealError) as exc:
        seal_dataset(info["run_dir"])
    assert exc.value.reason == Reason.SOURCE_NOT_COMPLETED


@pytest.mark.whatif("P07-G01-02")
def test_line_count_disagreement_blocks_sealing(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    """The report claims one more event than the file holds."""
    info = make_source(report_line_delta=1)
    with pytest.raises(SealError) as exc:
        seal_dataset(info["run_dir"])
    assert exc.value.reason == Reason.SOURCE_COUNT_MISMATCH


def test_recorded_hash_disagreement_blocks_sealing(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    info = make_source(corrupt_report_hash=True)
    with pytest.raises(SealError) as exc:
        seal_dataset(info["run_dir"])
    assert exc.value.reason == Reason.SOURCE_HASH_MISMATCH


@pytest.mark.whatif("P07-G01-03")
def test_tampering_the_file_alone_is_caught(make_source: Callable[..., dict[str, Any]]) -> None:
    info = make_source()
    tamper_events(info["run_dir"], line_index=0, replacement='{"tampered":true}')
    with pytest.raises(SealError) as exc:
        seal_dataset(info["run_dir"])
    assert exc.value.reason == Reason.SOURCE_HASH_MISMATCH


@pytest.mark.whatif("P07-G01-03")
def test_tampering_both_file_and_report_needs_an_external_anchor(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    """The attacker who can rewrite the whole bundle.

    Sealing *succeeds*, because the bundle is self-consistent again. Only the
    digest pinned outside the directory notices -- which is the entire argument
    for keeping one, and the reason ``anchor_verified`` exists.
    """
    info = make_source()
    original = info["events_sha256"]
    tamper_events(info["run_dir"], line_index=0, replacement='{"tampered":true}')
    reseal_after_tamper(info["run_dir"])

    dataset = seal_dataset(info["run_dir"])  # no anchor: passes, unproven
    assert dataset.anchor_verified is False

    with pytest.raises(SealError) as exc:
        seal_dataset(info["run_dir"], anchor=original)
    assert exc.value.reason == Reason.SOURCE_ANCHOR_MISMATCH


@pytest.mark.whatif("P07-G01-12")
def test_schema_skew_blocks_sealing(make_source: Callable[..., dict[str, Any]]) -> None:
    info = make_source(schema_version="sim.v2")
    with pytest.raises(SealError) as exc:
        seal_dataset(info["run_dir"])
    assert exc.value.reason == Reason.SOURCE_SCHEMA_SKEW


@pytest.mark.whatif("P07-G01-14")
def test_fault_replay_requires_a_hashed_plan(make_source: Callable[..., dict[str, Any]]) -> None:
    """A run with no fault plan cannot support a deterministic fault-replay claim."""
    info = make_source()  # no faults -> fingerprint "none"
    with pytest.raises(SealError) as exc:
        seal_dataset(info["run_dir"], require_fault_plan=True)
    assert exc.value.reason == Reason.SOURCE_FAULT_PLAN_UNHASHED

    faulted = make_source(name="withplan", faults=(FaultOp("duplicate_exact", "EURUSD", 5),))
    dataset = seal_dataset(faulted["run_dir"], require_fault_plan=True)
    assert dataset.fault_plan_fingerprint != "none"


def test_fault_manifest_is_parsed_and_unapplied_faults_are_visible(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    """A fault aimed at a sequence that never existed is planned, not applied."""
    info = make_source(
        faults=(
            FaultOp("duplicate_exact", "EURUSD", 5),
            FaultOp("missing_sequence", "EURUSD", 10_000_000),
        )
    )
    dataset = seal_dataset(info["run_dir"], anchor=info["events_sha256"])
    applied = {record.kind: record.applied for record in dataset.fault_records}
    assert applied["duplicate_exact"] is True
    assert applied["missing_sequence"] is False


@pytest.mark.whatif("P07-G01-04")
def test_dataset_id_collision_is_reported(make_source: Callable[..., dict[str, Any]]) -> None:
    """Two different sources sharing a ``dataset_id`` make identity ambiguous."""
    first = make_source(name="a", dataset_id="same-id", count=120, seed=1)
    second = make_source(name="b", dataset_id="same-id", count=120, seed=2)
    datasets = [
        seal_dataset(first["run_dir"], anchor=first["events_sha256"]),
        seal_dataset(second["run_dir"], anchor=second["events_sha256"]),
    ]
    collisions = check_dataset_id_collision(datasets)
    assert len(collisions) == 1
    assert collisions[0][0] == "same-id"

    # The same dataset listed twice is not a collision.
    assert check_dataset_id_collision([datasets[0], datasets[0]]) == []


def test_missing_directory_is_unavailable_not_a_crash(tmp_path: Path) -> None:
    with pytest.raises(SealError) as exc:
        seal_dataset(tmp_path / "nope")
    assert exc.value.reason == Reason.SOURCE_UNAVAILABLE


def test_malformed_manifest_is_rejected(make_source: Callable[..., dict[str, Any]]) -> None:
    info = make_source()
    manifest_path = Path(info["run_dir"]) / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    del manifest["config_resolved"]["data"]["symbols"]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(SealError) as exc:
        seal_dataset(info["run_dir"])
    assert exc.value.reason == Reason.SOURCE_MANIFEST_INVALID


def test_sealing_never_modifies_the_source(make_source: Callable[..., dict[str, Any]]) -> None:
    """Quarantine, never repair. A sealer that edits the source can fake a pass."""
    info = make_source(report_line_delta=1)
    run_dir = Path(info["run_dir"])
    before = {path.name: path.read_bytes() for path in sorted(run_dir.iterdir())}
    with pytest.raises(SealError):
        seal_dataset(run_dir)
    after = {path.name: path.read_bytes() for path in sorted(run_dir.iterdir())}
    assert before == after
