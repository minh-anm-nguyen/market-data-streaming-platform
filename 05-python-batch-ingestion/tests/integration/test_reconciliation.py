"""E2E-10 identity conflict, E2E-20 false-PASS detector, E2E-21 count collision.

E2E-21 is the test that justifies the whole reconciler. One expected event is
removed and another is duplicated, so ``count(*)`` matches the expectation
exactly while the data is wrong. A counts-based oracle passes; an
identity-level oracle must fail.
"""

from __future__ import annotations

import json

import pytest

from p05_ingest.canonical_digest import payload_digest
from p05_ingest.contract_validator import ContractValidator
from p05_ingest.model import EventIdentity, EventKind, LogicalProjection
from p05_ingest.reconciler import (
    EvidenceGaps,
    ObservedEvent,
    Verdict,
    observed_from_kafka_scan,
    observed_from_sink_rows,
    reconcile,
    verify_stored_digests,
)
from p05_ingest.testkit import TICKS_TOPIC, conflict_pair, duplicate_pair, make_tick
from p05_ingest.testkit.workload import (
    manifest_as_expected,
    produce_market_workload,
)
from tests.conftest import build_harness, make_config


def observed(dataset: str, symbol: str, sequence: int, digest: str) -> ObservedEvent:
    return ObservedEvent(EventIdentity(dataset, symbol, sequence), digest)


def ticks_only(dev_config, **batch):
    return make_config(dev_config, kafka={"topics": ["md.ticks.v1"]}, batch=batch)


# ---------------------------------------------------------------- E2E-21


def test_e2e_21_counts_match_but_identity_level_oracle_fails():
    """The canonical false-PASS trap: one missing, one duplicated."""
    expected = [observed("d", "EURUSD", n, f"{n:064d}") for n in range(5)]

    # Drop identity 2, write identity 3 twice. Five rows either way.
    sink = [e for e in expected if e.identity.sequence != 2]
    sink.append(observed("d", "EURUSD", 3, f"{3:064d}"))

    assert len(sink) == len(expected), "a counts-only check would pass here"

    report = reconcile(expected=expected, kafka_observed=expected, sink_observed=sink)

    assert report.verdict is Verdict.NO_GO
    assert [i.sequence for i in report.missing_from_sink] == [2]
    assert report.duplicate_rows == 1
    assert report.sink_physical_count == report.expected_count
    assert report.sink_logical_count == 4, "five rows, four distinct identities"
    assert any("absent from ClickHouse" in r for r in report.reasons)


def test_e2e_21_the_same_data_through_the_real_pipeline(dev_config, clock):
    """Build the trap by tampering with the table after a clean run."""
    cfg = ticks_only(dev_config, per_partition_max_rows=5)
    harness = build_harness(cfg, clock=clock, partitions=1)
    for sequence in range(5):
        event = make_tick(sequence=sequence)
        harness.cluster.produce(
            TICKS_TOPIC, 0, key=b"EURUSD", value=event.SerializeToString()
        )
    harness.finish()
    assert harness.sink.row_count == 5

    kafka_observed, refused = observed_from_kafka_scan(
        harness.cluster.scan(),
        validator=ContractValidator(cfg.contracts, cfg.project),
        mapper_digest=payload_digest,
    )
    assert refused == []

    # Tamper: delete the row for sequence 2 and duplicate the one for 3.
    rows = [r for r in harness.sink.rows if r["sequence"] != 2]
    rows.append(next(r for r in harness.sink.rows if r["sequence"] == 3))
    assert len(rows) == 5

    report = reconcile(
        expected=kafka_observed,
        kafka_observed=kafka_observed,
        sink_observed=observed_from_sink_rows(rows),
    )
    assert report.verdict is Verdict.NO_GO
    assert len(report.missing_from_sink) == 1


# ---------------------------------------------------------------- E2E-10


def test_e2e_10_identity_conflict_is_reported_and_never_resolved(dev_config, clock):
    """Same identity, two different prices. No winner may be chosen."""
    cfg = ticks_only(dev_config, per_partition_max_rows=2)
    harness = build_harness(cfg, clock=clock, partitions=1)
    first, second = conflict_pair()
    for entry in (first, second):
        harness.cluster.produce(TICKS_TOPIC, 0, key=entry.key, value=entry.value)

    status = harness.finish()
    # P05 writes both: detecting the conflict is the reconciler's job, and
    # discarding one here would be the "arbitrary winner" the spec forbids.
    assert status.complete, status.reason
    assert harness.sink.row_count == 2

    digests = harness.sink.digests_by_identity()
    assert len(digests) == 1
    ((identity, variants),) = digests.items()
    assert len(variants) == 2, "one identity, two semantic payloads"

    kafka_observed, _ = observed_from_kafka_scan(
        harness.cluster.scan(),
        validator=ContractValidator(cfg.contracts, cfg.project),
        mapper_digest=payload_digest,
    )
    report = reconcile(
        expected=kafka_observed,
        kafka_observed=kafka_observed,
        sink_observed=observed_from_sink_rows(harness.sink.rows),
    )
    assert report.verdict is Verdict.NO_GO
    assert len(report.conflicts) == 1
    assert len(next(iter(report.conflicts.values()))) == 2
    assert any("no winner may be chosen" in r for r in report.reasons)


def test_duplicate_pair_is_not_a_conflict(dev_config, clock):
    """Same identity, same payload: one logical event, two physical rows."""
    cfg = ticks_only(dev_config, per_partition_max_rows=2)
    harness = build_harness(cfg, clock=clock, partitions=1)
    first, second = duplicate_pair()
    for entry in (first, second):
        harness.cluster.produce(TICKS_TOPIC, 0, key=entry.key, value=entry.value)
    harness.finish()

    digests = harness.sink.digests_by_identity()
    ((_identity, variants),) = digests.items()
    assert len(variants) == 1

    kafka_observed, _ = observed_from_kafka_scan(
        harness.cluster.scan(),
        validator=ContractValidator(cfg.contracts, cfg.project),
        mapper_digest=payload_digest,
    )
    report = reconcile(
        expected=kafka_observed,
        kafka_observed=kafka_observed,
        sink_observed=observed_from_sink_rows(harness.sink.rows),
    )
    assert report.verdict is Verdict.GO, report.summary()
    assert report.conflicts == {}
    assert report.duplicate_rows == 1


def test_conflict_in_kafka_is_attributed_upstream():
    """A conflict the producer created must not be blamed on ingestion."""
    kafka = [
        observed("d", "EURUSD", 1, "a" * 64),
        observed("d", "EURUSD", 1, "b" * 64),
    ]
    report = reconcile(expected=None, kafka_observed=kafka, sink_observed=[])
    assert report.verdict is Verdict.NO_GO
    assert EventIdentity("d", "EURUSD", 1) in report.conflicts


def test_same_sequence_in_a_different_dataset_is_a_different_event():
    """Spec 3.2: never de-duplicate across datasets."""
    events = [
        observed("dataset-a", "EURUSD", 1, "a" * 64),
        observed("dataset-b", "EURUSD", 1, "b" * 64),
    ]
    report = reconcile(expected=events, kafka_observed=events, sink_observed=events)
    assert report.verdict is Verdict.GO, report.summary()
    assert report.sink_logical_count == 2
    assert report.conflicts == {}


# ---------------------------------------------------------------- E2E-20


def test_e2e_20_missing_evidence_yields_inconclusive_not_go():
    """A clean comparison with incomplete evidence must not be a PASS."""
    events = [observed("d", "EURUSD", n, f"{n:064d}") for n in range(3)]

    clean = reconcile(expected=events, kafka_observed=events, sink_observed=events)
    assert clean.verdict is Verdict.GO

    for gaps in (
        EvidenceGaps(kafka_cut_not_frozen=True),
        EvidenceGaps(sink_readback_incomplete=True),
        EvidenceGaps(settle_window_not_elapsed=True),
        EvidenceGaps(retention_loss_detected=True),
        EvidenceGaps(commit_ahead_of_sink_proof=True),
        EvidenceGaps(unknown_sink_outcomes=1),
    ):
        report = reconcile(
            expected=events,
            kafka_observed=events,
            sink_observed=events,
            gaps=gaps,
        )
        assert report.verdict is Verdict.INCONCLUSIVE, gaps
        assert report.reasons


def test_e2e_20_absent_source_manifest_can_never_be_a_go():
    """Without the manifest, "nothing missing" is unobservable."""
    events = [observed("d", "EURUSD", n, f"{n:064d}") for n in range(3)]
    report = reconcile(expected=None, kafka_observed=events, sink_observed=events)
    assert report.verdict is Verdict.INCONCLUSIVE
    assert report.gaps.missing_source_manifest
    assert any("no source manifest" in r for r in report.reasons)


def test_a_fault_that_did_not_fire_cannot_pass(dev_config, clock):
    """Spec 9.1: a chaos test with no confirmed stimulus is NOT_RUN.

    Here the fault injector is armed but the scripted behaviour never runs,
    so the fault-confirmation counter stays at zero and the scenario must not
    be reported as having proven anything.
    """
    from p05_ingest.metrics import FAULT_INJECTION_CONFIRMED

    cfg = ticks_only(dev_config, per_partition_max_rows=5)
    harness = build_harness(cfg, clock=clock, partitions=1)
    for sequence in range(5):
        event = make_tick(sequence=sequence)
        harness.cluster.produce(
            TICKS_TOPIC, 0, key=b"EURUSD", value=event.SerializeToString()
        )
    status = harness.finish()

    assert status.complete, "the run itself succeeded"
    assert harness.metrics.counter_total(FAULT_INJECTION_CONFIRMED) == 0, (
        "no fault was injected, so no fault-based claim may be made from this run"
    )


# ------------------------------------------------------- digest verification


def test_stored_digest_must_describe_its_own_row(dev_config, clock):
    cfg = ticks_only(dev_config, per_partition_max_rows=5)
    harness = build_harness(cfg, clock=clock, partitions=1)
    for sequence in range(5):
        event = make_tick(sequence=sequence)
        harness.cluster.produce(
            TICKS_TOPIC, 0, key=b"EURUSD", value=event.SerializeToString()
        )
    harness.finish()

    assert verify_stored_digests(harness.sink.rows) == []

    # Tamper with one price and leave the digest alone: a row that lies about
    # its own contents. Every digest-based comparison would otherwise accept it.
    harness.sink.rows[0].values["price_ticks"] = 999_999
    mismatches = verify_stored_digests(harness.sink.rows)
    assert len(mismatches) == 1
    identity, stored, recomputed = mismatches[0]
    assert stored != recomputed


def test_field_level_mangling_is_caught_even_with_matching_identities():
    """Spec NFR-01: presence in the table is not the same as correctness."""
    source_projection = LogicalProjection(
        dataset_id="d",
        symbol="EURUSD",
        sequence=1,
        event_time_ns=1_704_171_845_123_456_789,
        price_scale=100_000,
        kind=EventKind.TICK,
        source_schema_version="marketdata.v1",
        price_ticks=109_385,
    )
    from dataclasses import replace

    # One tick of difference in the price: same identity, different meaning.
    mangled = replace(source_projection, price_ticks=109_386)

    kafka = [
        ObservedEvent(source_projection.identity, payload_digest(source_projection))
    ]
    sink = [ObservedEvent(mangled.identity, payload_digest(mangled))]

    report = reconcile(expected=kafka, kafka_observed=kafka, sink_observed=sink)
    assert report.verdict is Verdict.NO_GO
    assert report.digest_mismatches == [source_projection.identity]
    assert any("a field was altered in transit" in r for r in report.reasons)


# -------------------------------------------------------------- full-run GO


def test_a_clean_finite_run_reconciles_to_go(dev_config, clock):
    cfg = make_config(dev_config, batch={"per_partition_max_rows": 100})
    harness = build_harness(cfg, clock=clock, partitions=4)
    manifest = produce_market_workload(
        harness.cluster, events=800, partitions=4, seed=21
    )
    cut = harness.cluster.end_offsets()
    harness.finish()

    kafka_observed, refused = observed_from_kafka_scan(
        harness.cluster.scan(cut),
        validator=ContractValidator(cfg.contracts, cfg.project),
        mapper_digest=payload_digest,
    )
    assert refused == []

    report = reconcile(
        expected=manifest_as_expected(manifest),
        kafka_observed=kafka_observed,
        sink_observed=observed_from_sink_rows(harness.sink.rows),
    )
    assert report.verdict is Verdict.GO, report.summary()
    assert report.missing_from_kafka == []
    assert report.missing_from_sink == []
    assert report.extra_in_sink == []
    assert report.conflicts == {}
    assert report.duplicate_rows == 0


def test_extra_rows_not_in_the_kafka_cut_are_reported():
    """Rows the pipeline could not have produced from the cut are a finding."""
    kafka = [observed("d", "EURUSD", n, f"{n:064d}") for n in range(3)]
    sink = kafka + [observed("d", "EURUSD", 99, f"{99:064d}")]
    report = reconcile(expected=kafka, kafka_observed=kafka, sink_observed=sink)
    assert report.verdict is Verdict.NO_GO
    assert [i.sequence for i in report.extra_in_sink] == [99]


def test_report_serialises_to_json_for_the_artifact(tmp_path):
    events = [observed("d", "EURUSD", n, f"{n:064d}") for n in range(3)]
    report = reconcile(expected=events, kafka_observed=events, sink_observed=events)
    path = tmp_path / "nested" / "report.json"
    report.write_json(path)

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["verdict"] == "GO"
    assert payload["counts"]["expected"] == 3
    assert "evidence_gaps" in payload
    assert "summary" not in payload
    assert report.summary().startswith("verdict: GO")


def test_empty_everything_is_a_go_but_proves_nothing_interesting():
    report = reconcile(expected=[], kafka_observed=[], sink_observed=[])
    assert report.verdict is Verdict.GO
    assert report.expected_count == 0
