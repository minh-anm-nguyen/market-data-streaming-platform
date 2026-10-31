"""The journal's hash chain and the six-fact fault receipt."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from p07.evidence.artifact_store import ArtifactStore
from p07.evidence.proof_ledger import (
    GENESIS_HASH,
    JOURNAL_PATH,
    FaultReceipt,
    Phase,
    ProofLedger,
    summarise_receipts,
)


@pytest.fixture
def ledger(tmp_path: Path) -> ProofLedger:
    store = ArtifactStore(tmp_path, scenario_id="sc", run_id="r1", fsync=False)
    return ProofLedger(store, scenario_id="sc", run_id="r1", dataset_id="ds", test_case_id="T-1")


def receipt(**overrides: object) -> FaultReceipt:
    base = dict(
        fault_id="f1",
        intended_fault="broker_stop",
        target="kafka_broker",
        trigger_precondition_verified=True,
        intended_target_matched=True,
        injection_command_acknowledged=True,
        independent_observation_of_effect=True,
        observed_effect="live_brokers=[0,1]",
        started_at_ns=1,
        ended_at_ns=2,
        cleanup_recorded=True,
    )
    base.update(overrides)
    return FaultReceipt(**base)  # type: ignore[arg-type]


def test_first_entry_links_to_genesis(ledger: ProofLedger) -> None:
    entry = ledger.record(phase=Phase.CREATED, actor="orchestrator", operation="start")
    assert entry.sha256_prev_entry == GENESIS_HASH
    assert entry.sha256_this_entry == entry.compute_hash()
    assert ledger.head_hash == entry.sha256_this_entry


def test_chain_links_each_entry_to_the_last(ledger: ProofLedger) -> None:
    first = ledger.record(phase=Phase.CREATED, actor="a", operation="one")
    second = ledger.record(phase=Phase.RUNNING, actor="a", operation="two")
    assert second.sha256_prev_entry == first.sha256_this_entry
    report = ledger.verify()
    assert report["chain_intact"] is True
    assert report["entries_verified"] == 2
    assert report["head_sha256"] == ledger.head_hash


@pytest.mark.whatif("P07-G21-04")
def test_editing_an_entry_breaks_the_chain(ledger: ProofLedger) -> None:
    for index in range(3):
        ledger.record(phase=Phase.RUNNING, actor="a", operation=f"op{index}")
    path = ledger._store.path_for(JOURNAL_PATH)  # noqa: SLF001 - inspecting the artifact
    lines = path.read_text(encoding="utf-8").splitlines()
    record = json.loads(lines[1])
    record["operation"] = "tampered"
    lines[1] = json.dumps(record, sort_keys=True, separators=(",", ":"))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    report = ledger.verify()
    assert report["chain_intact"] is False
    assert report["broken_at_index"] == 1


@pytest.mark.whatif("P07-G21-05")
def test_a_torn_tail_is_reported_not_hidden(ledger: ProofLedger) -> None:
    """A kill mid-append leaves half a line. That is information."""
    ledger.record(phase=Phase.RUNNING, actor="a", operation="one")
    path = ledger._store.path_for(JOURNAL_PATH)  # noqa: SLF001
    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"index": 1, "partial"')
    report = ledger.verify()
    assert report["torn_tail"] is True
    assert report["entries_verified"] == 1


def test_the_chain_is_not_a_signature(ledger: ProofLedger) -> None:
    """Someone who can rewrite the whole file can recompute the chain.

    The verification passes after a full rewrite, which is precisely why
    :meth:`ProofLedger.anchor` exists and why the report tells the reader to
    store it elsewhere. Asserting this keeps the limitation documented in code.
    """
    ledger.record(phase=Phase.RUNNING, actor="a", operation="one")
    path = ledger._store.path_for(JOURNAL_PATH)  # noqa: SLF001
    record = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    record["operation"] = "rewritten"
    body = {key: value for key, value in record.items() if key != "sha256_this_entry"}
    import hashlib

    recomputed = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
    record["sha256_this_entry"] = recomputed
    path.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")

    report = ledger.verify()
    assert report["chain_intact"] is True  # the chain cannot detect this
    assert report["head_sha256"] != ledger.head_hash  # but an external anchor can
    assert "not a signature" in report["note"]


def test_anchor_carries_the_head(ledger: ProofLedger) -> None:
    ledger.record(phase=Phase.RUNNING, actor="a", operation="one")
    anchor = ledger.anchor()
    assert anchor["head_sha256"] == ledger.head_hash
    assert anchor["entries"] == 1
    assert anchor["run_id"] == "r1"


@pytest.mark.whatif("P07-G27-02")
def test_a_receipt_needs_all_six_facts() -> None:
    """Design 6.3's conjunction, computed rather than trusted."""
    assert receipt().confirmed is True
    for missing in (
        "trigger_precondition_verified",
        "intended_target_matched",
        "injection_command_acknowledged",
        "independent_observation_of_effect",
        "cleanup_recorded",
    ):
        broken = receipt(**{missing: False})
        assert broken.confirmed is False
        assert missing in broken.missing_conditions()


def test_an_unknown_time_range_makes_a_receipt_unconfirmed() -> None:
    assert receipt(ended_at_ns=None).confirmed is False
    assert receipt(started_at_ns=0).confirmed is False
    assert "timestamp_range_known" in receipt(ended_at_ns=None).missing_conditions()


def test_receipts_are_journalled_and_change_the_phase(ledger: ProofLedger) -> None:
    ledger.record_receipt(receipt())
    entries = list(ledger.read_entries())
    assert entries[-1]["operation"] == "fault_receipt"
    assert entries[-1]["phase"] == Phase.FAULT_CONFIRMED.value
    assert entries[-1]["result_code"] == "CONFIRMED"


def test_an_unconfirmed_receipt_does_not_claim_the_confirmed_phase(
    ledger: ProofLedger,
) -> None:
    ledger.record_receipt(receipt(independent_observation_of_effect=False))
    entries = list(ledger.read_entries())
    assert entries[-1]["phase"] == Phase.RUNNING.value
    assert entries[-1]["result_code"] == "NOT_CONFIRMED"
    assert ledger.all_faults_confirmed() is False
    assert len(ledger.unconfirmed_faults()) == 1


def test_a_run_with_no_faults_is_vacuously_confirmed(ledger: ProofLedger) -> None:
    """Otherwise the clean baseline would be permanently INVALID."""
    assert ledger.all_faults_confirmed() is True


def test_summary_reports_the_trigger_success_ratio() -> None:
    summary = summarise_receipts([receipt(), receipt(fault_id="f2", cleanup_recorded=False)])
    assert summary["faults_total"] == 2
    assert summary["faults_confirmed"] == 1
    assert summary["trigger_success_ratio"] == 0.5
    assert summary["unconfirmed"][0]["fault_id"] == "f2"
    assert summarise_receipts([])["trigger_success_ratio"] is None
