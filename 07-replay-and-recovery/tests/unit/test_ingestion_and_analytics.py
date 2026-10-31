"""P05's frontier and P06's build gate -- the two places data goes missing quietly."""

from __future__ import annotations

from dataclasses import replace

import pytest

from p07.contracts import TICK_TOPIC, EventKind, SemanticCore
from p07.dataplane.analytics import BuildPipeline, BuildRefused
from p07.dataplane.clickhouse_fake import (
    BuildStatus,
    DiskFull,
    FakeClickHouse,
    RawRow,
    SinkAckUnknown,
    SinkError,
    SinkUnavailable,
)
from p07.dataplane.codec import DEFAULT_CODEC, frame_mdp1
from p07.dataplane.ingestion import (
    BatchIngestor,
    PartitionProgress,
    PoisonRecord,
    identities_in_raw,
    rebalance,
)
from p07.dataplane.kafka_fake import FakeKafka, KafkaError, OffsetExpired
from p07.oracle.semantic_digest import digest_core


def core(sequence: int, *, kind: EventKind = EventKind.TICK, price: int = 108000) -> SemanticCore:
    return SemanticCore(
        dataset_id="ds",
        symbol="EURUSD",
        sequence=sequence,
        event_time_ns=1_000_000 * sequence,
        price_scale=100000,
        event_kind=kind,
        source_schema_version="sim.v1",
        bid_ticks=None,
        ask_ticks=None,
        price_ticks=price,
    )


def publish(kafka: FakeKafka, count: int, *, partition: int = 0) -> None:
    for sequence in range(1, count + 1):
        kafka.produce(
            TICK_TOPIC,
            partition,
            key=b"EURUSD",
            value=DEFAULT_CODEC.encode(core(sequence)),
            headers={"dataset_id": "ds"},
        )


def row(sequence: int, *, offset: int = 0, price: int = 108000) -> RawRow:
    body = core(sequence, price=price)
    return RawRow(
        dataset_id=body.dataset_id,
        symbol=body.symbol,
        sequence=body.sequence,
        event_time_ns=body.event_time_ns,
        price_scale=body.price_scale,
        event_kind=body.event_kind.wire_name,
        source_schema_version=body.source_schema_version,
        bid_ticks=None,
        ask_ticks=None,
        price_ticks=body.price_ticks,
        payload_digest=digest_core(body),
        topic_generation="gen-1",
        topic=TICK_TOPIC,
        partition=0,
        offset=offset,
        batch_id="b1",
        ingested_at_ns=sequence,
    )


def verified_proof() -> dict[str, object]:
    return {
        "canonical_verified": True,
        "bars_verified": True,
        "source_cut_sealed": True,
        "evidence_ref": "analytics/canonical_diff.json",
    }


# -- the frontier ---------------------------------------------------------


@pytest.mark.whatif("P07-G14-01")
def test_the_contiguous_frontier_stops_at_the_first_hole() -> None:
    """``INV-03``'s arithmetic, in isolation."""
    progress = PartitionProgress(start_offset=0, next_to_fetch=0)
    progress.completed.update({0, 1, 2, 4, 5})
    assert progress.completed_contiguous_next() == 3
    progress.completed.add(3)
    assert progress.completed_contiguous_next() == 6


def test_the_frontier_counts_from_the_resume_point_not_the_commit() -> None:
    """A resumed worker starts at the committed offset, and counts from there.

    The cursor is ``start_offset``, deliberately not ``committed_next``: basing
    it on the commit would let a worker that committed past a hole report a
    frontier at its own commit, and ``INV-03`` could then never fire. The
    mutation test in ``tests/model/test_mutations.py`` is the other half of this.
    """
    resumed = PartitionProgress(start_offset=10, next_to_fetch=10, committed_next=10)
    resumed.completed.update({10, 11})
    assert resumed.completed_contiguous_next() == 12

    committed_past_a_hole = PartitionProgress(start_offset=0, next_to_fetch=3, committed_next=3)
    committed_past_a_hole.completed.update({1, 2})
    assert committed_past_a_hole.completed_contiguous_next() == 0


@pytest.mark.whatif("P07-G14-02")
def test_a_clean_drain_commits_exactly_the_cut(kafka: FakeKafka, sink: FakeClickHouse) -> None:
    publish(kafka, 10)
    ingestor = BatchIngestor(kafka, sink, assignment=[(TICK_TOPIC, 0)], batch_max_records=4)
    ingestor.resume_from_broker(cut_start={(TICK_TOPIC, 0): 0})
    report = ingestor.drain_to_cut({(TICK_TOPIC, 0): 10})
    assert report["frontier_reached_cut"] is True
    assert kafka.committed("p05-ingest", TICK_TOPIC, 0) == 10
    assert sink.raw_row_count(dataset_id="ds") == 10


@pytest.mark.whatif("P07-G13-01", "P07-G14-03")
def test_an_ambiguous_sink_ack_blocks_the_frontier(kafka: FakeKafka, sink: FakeClickHouse) -> None:
    """The rows may be there. Until that is resolved, the offset cannot move."""
    publish(kafka, 4)
    sink.ack_loss_once = True
    ingestor = BatchIngestor(
        kafka, sink, assignment=[(TICK_TOPIC, 0)], batch_max_records=4, max_insert_retries=1
    )
    ingestor.resume_from_broker(cut_start={(TICK_TOPIC, 0): 0})
    with pytest.raises(SinkAckUnknown):
        ingestor.poll_once(up_to={(TICK_TOPIC, 0): 4})
    progress = ingestor.progress[(TICK_TOPIC, 0)]
    assert progress.unknown == {0, 1, 2, 3}
    assert progress.completed == set()
    assert kafka.committed("p05-ingest", TICK_TOPIC, 0) is None
    # The rows did land, which only an independent scan can establish.
    assert sink.raw_row_count(dataset_id="ds") == 4


@pytest.mark.whatif("P07-G13-02")
def test_a_retry_after_an_ambiguous_ack_duplicates_physically_but_completes(
    kafka: FakeKafka, sink: FakeClickHouse
) -> None:
    publish(kafka, 3)
    sink.ack_loss_once = True
    ingestor = BatchIngestor(
        kafka, sink, assignment=[(TICK_TOPIC, 0)], batch_max_records=3, max_insert_retries=3
    )
    ingestor.resume_from_broker(cut_start={(TICK_TOPIC, 0): 0})
    ingestor.poll_once(up_to={(TICK_TOPIC, 0): 3})
    assert kafka.committed("p05-ingest", TICK_TOPIC, 0) == 3
    counts = identities_in_raw(sink.scan_raw(dataset_id="ds"))
    assert set(counts.values()) == {2}  # every identity written twice
    assert len(counts) == 3  # three logical identities


def test_a_pre_commit_failure_writes_nothing_and_does_not_commit(
    kafka: FakeKafka, sink: FakeClickHouse
) -> None:
    publish(kafka, 3)
    sink.fail_before_commit_once = True
    ingestor = BatchIngestor(kafka, sink, assignment=[(TICK_TOPIC, 0)], batch_max_records=3)
    ingestor.resume_from_broker(cut_start={(TICK_TOPIC, 0): 0})
    with pytest.raises(SinkError):
        ingestor.poll_once(up_to={(TICK_TOPIC, 0): 3})
    assert sink.raw_row_count() == 0
    assert kafka.committed("p05-ingest", TICK_TOPIC, 0) is None


@pytest.mark.whatif("P07-G25-02")
def test_disk_full_writes_nothing_and_does_not_commit(
    kafka: FakeKafka, sink: FakeClickHouse
) -> None:
    publish(kafka, 3)
    sink.set_disk_free_bytes(0)
    ingestor = BatchIngestor(kafka, sink, assignment=[(TICK_TOPIC, 0)], batch_max_records=3)
    ingestor.resume_from_broker(cut_start={(TICK_TOPIC, 0): 0})
    with pytest.raises(DiskFull):
        ingestor.poll_once(up_to={(TICK_TOPIC, 0): 3})
    assert sink.raw_row_count() == 0
    assert kafka.committed("p05-ingest", TICK_TOPIC, 0) is None


@pytest.mark.whatif("P07-G11-01", "P07-G11-03")
def test_a_fenced_worker_cannot_commit_but_its_writes_still_landed(
    kafka: FakeKafka, sink: FakeClickHouse
) -> None:
    """``INV-04``: fencing stops the commit, not the side effect.

    This asymmetry is the whole point of the rebalance cases -- the stale
    worker's rows are in the sink, and only the raw audit will find them.
    """
    publish(kafka, 4)
    stale = BatchIngestor(kafka, sink, assignment=[(TICK_TOPIC, 0)], batch_max_records=4, epoch=0)
    stale.resume_from_broker(cut_start={(TICK_TOPIC, 0): 0})
    fresh_epoch = rebalance(kafka, group="p05-ingest", workers=[], assignments=[])
    assert fresh_epoch == 1
    stale.poll_once(up_to={(TICK_TOPIC, 0): 4})
    assert sink.raw_row_count(dataset_id="ds") == 4  # side effects happened
    assert kafka.committed("p05-ingest", TICK_TOPIC, 0) is None  # commit refused
    assert stale.counters.commits_refused == 1


def test_rebalance_reassigns_and_drops_unowned_progress(
    kafka: FakeKafka, sink: FakeClickHouse
) -> None:
    publish(kafka, 2, partition=0)
    publish(kafka, 2, partition=1)
    worker = BatchIngestor(kafka, sink, assignment=[(TICK_TOPIC, 0), (TICK_TOPIC, 1)])
    worker.resume_from_broker(cut_start={(TICK_TOPIC, 0): 0, (TICK_TOPIC, 1): 0})
    assert len(worker.progress) == 2
    rebalance(kafka, group="p05-ingest", workers=[worker], assignments=[[(TICK_TOPIC, 0)]])
    assert worker.assignment == [(TICK_TOPIC, 0)]
    assert set(worker.progress) == {(TICK_TOPIC, 0)}
    assert worker.epoch == 1


@pytest.mark.whatif("P07-G10-02")
def test_resume_refuses_to_guess_a_position(kafka: FakeKafka, sink: FakeClickHouse) -> None:
    """``INV-08``: no implicit ``auto.offset.reset``."""
    publish(kafka, 3)
    ingestor = BatchIngestor(kafka, sink, assignment=[(TICK_TOPIC, 0)])
    with pytest.raises(KafkaError, match="OFFSET_RESET_FORBIDDEN"):
        ingestor.resume_from_broker()


def test_resume_uses_the_committed_offset_when_there_is_one(
    kafka: FakeKafka, sink: FakeClickHouse
) -> None:
    publish(kafka, 5)
    kafka.commit("p05-ingest", TICK_TOPIC, 0, 2, epoch=0)
    ingestor = BatchIngestor(kafka, sink, assignment=[(TICK_TOPIC, 0)])
    ingestor.resume_from_broker()
    progress = ingestor.progress[(TICK_TOPIC, 0)]
    assert progress.start_offset == 2 and progress.next_to_fetch == 2


@pytest.mark.whatif("P07-G16-03")
def test_a_poison_record_stops_ingestion_in_a_strict_topic(
    kafka: FakeKafka, sink: FakeClickHouse
) -> None:
    """Case ``E2E-18``: skipping it would be an untracked drop."""
    publish(kafka, 1)
    kafka.produce(TICK_TOPIC, 0, key=b"EURUSD", value=b"\xff\xff\xff\xff\xff\xff")
    ingestor = BatchIngestor(kafka, sink, assignment=[(TICK_TOPIC, 0)], batch_max_records=10)
    ingestor.resume_from_broker(cut_start={(TICK_TOPIC, 0): 0})
    with pytest.raises(PoisonRecord) as exc:
        ingestor.poll_once(up_to={(TICK_TOPIC, 0): 2})
    assert exc.value.offset == 1


@pytest.mark.whatif("P07-G03-12")
def test_an_mdp1_prefixed_value_is_a_poison_record(kafka: FakeKafka, sink: FakeClickHouse) -> None:
    kafka.produce(TICK_TOPIC, 0, key=b"EURUSD", value=frame_mdp1(DEFAULT_CODEC.encode(core(1))))
    ingestor = BatchIngestor(kafka, sink, assignment=[(TICK_TOPIC, 0)])
    ingestor.resume_from_broker(cut_start={(TICK_TOPIC, 0): 0})
    with pytest.raises(PoisonRecord) as exc:
        ingestor.poll_once(up_to={(TICK_TOPIC, 0): 1})
    assert exc.value.reason == "KAFKA_VALUE_HAS_MDP1_PREFIX"


def test_an_expired_offset_during_a_drain_is_raised_not_reset(
    kafka: FakeKafka, sink: FakeClickHouse
) -> None:
    publish(kafka, 5)
    ingestor = BatchIngestor(kafka, sink, assignment=[(TICK_TOPIC, 0)])
    ingestor.resume_from_broker(cut_start={(TICK_TOPIC, 0): 0})
    kafka.expire_to(TICK_TOPIC, 0, 3)
    with pytest.raises(OffsetExpired):
        ingestor.poll_once(up_to={(TICK_TOPIC, 0): 5})


def test_crash_hooks_fire_at_the_named_boundaries(kafka: FakeKafka, sink: FakeClickHouse) -> None:
    """Design 5.3's boundaries, not an interval (design 6.3 forbids a sleep)."""
    publish(kafka, 2)
    seen: list[str] = []

    def hook(stage: str, **_: object) -> None:
        seen.append(stage)

    ingestor = BatchIngestor(
        kafka, sink, assignment=[(TICK_TOPIC, 0)], batch_max_records=2, crash_after=hook
    )
    ingestor.resume_from_broker(cut_start={(TICK_TOPIC, 0): 0})
    ingestor.poll_once(up_to={(TICK_TOPIC, 0): 2})
    assert seen == ["before_insert", "after_insert_before_commit", "after_commit"]


# -- the build pipeline ---------------------------------------------------


def test_the_audit_separates_duplicates_from_conflicts(sink: FakeClickHouse) -> None:
    sink.insert_raw([row(1, offset=0), row(1, offset=1), row(2, offset=2)])
    audit = BuildPipeline(sink).audit_raw("ds")
    assert audit.physical_rows == 3
    assert audit.identities == 2
    assert len(audit.duplicate_identities) == 1
    assert audit.conflicts == ()
    assert audit.complete is True


@pytest.mark.whatif("P07-G17-02")
def test_a_conflict_blocks_the_canonical_build(sink: FakeClickHouse) -> None:
    """No winner is picked. Design 7.3: an identity conflict is terminal."""
    sink.insert_raw([row(1, offset=0), row(1, offset=1, price=999)])
    pipeline = BuildPipeline(sink)
    audit = pipeline.audit_raw("ds")
    assert len(audit.conflicts) == 1
    assert audit.complete is False
    with pytest.raises(BuildRefused, match="IDENTITY_CONFLICT"):
        pipeline.build_canonical(
            "b1", audit=audit, source_manifest_sha256="x", expected_identity_count=1
        )


def test_a_row_whose_digest_disagrees_with_its_columns_blocks_the_build(
    sink: FakeClickHouse,
) -> None:
    bad = replace(row(1), payload_digest="0" * 64)
    sink.insert_raw([bad])
    pipeline = BuildPipeline(sink)
    audit = pipeline.audit_raw("ds")
    assert audit.digest_mismatches
    with pytest.raises(BuildRefused, match="SEMANTIC_MISMATCH"):
        pipeline.build_canonical(
            "b1", audit=audit, source_manifest_sha256="x", expected_identity_count=1
        )


def test_a_clean_build_produces_one_canonical_row_per_identity(sink: FakeClickHouse) -> None:
    sink.insert_raw([row(1, offset=0), row(1, offset=1), row(2, offset=2)])
    pipeline = BuildPipeline(sink)
    audit = pipeline.audit_raw("ds")
    written = pipeline.build_canonical(
        "b1", audit=audit, source_manifest_sha256="x", expected_identity_count=2
    )
    assert written == 2
    assert sink.build_status("b1") is BuildStatus.CANDIDATE
    assert len(list(sink.scan_canonical("b1"))) == 2


def test_bars_are_materialised_from_canonical_ticks_only(sink: FakeClickHouse) -> None:
    sink.insert_raw([row(1, offset=0), row(2, offset=1, price=108500)])
    pipeline = BuildPipeline(sink)
    audit = pipeline.audit_raw("ds")
    pipeline.build_canonical("b1", audit=audit, source_manifest_sha256="x", expected_identity_count=2)
    assert pipeline.materialize_bars("b1") == 1
    bar = next(iter(sink.scan_bars("b1")))
    assert (bar.open_ticks, bar.close_ticks, bar.tick_count) == (108000, 108500, 2)


@pytest.mark.whatif("P07-G19-02")
def test_publication_requires_an_external_proof_token(sink: FakeClickHouse) -> None:
    """``INV-05``: a component cannot declare its own output verified."""
    sink.insert_raw([row(1)])
    pipeline = BuildPipeline(sink)
    audit = pipeline.audit_raw("ds")
    pipeline.build_canonical("b1", audit=audit, source_manifest_sha256="x", expected_identity_count=1)
    pipeline.materialize_bars("b1")

    with pytest.raises(SinkError, match="BUILD_NOT_VERIFIED"):
        sink.publish_ready("b1", expected_current=None)
    with pytest.raises(BuildRefused, match="BUILD_NOT_VERIFIED"):
        pipeline.mark_verified("b1", proof={"canonical_verified": True})
    with pytest.raises(BuildRefused, match="EVIDENCE_INCOMPLETE"):
        pipeline.mark_verified(
            "b1",
            proof={"canonical_verified": True, "bars_verified": True, "source_cut_sealed": True},
        )
    pipeline.mark_verified("b1", proof=verified_proof())
    assert pipeline.publish("b1", expected_current=None) is True
    assert sink.ready_build == "b1"


@pytest.mark.whatif("P07-G19-04")
def test_two_builders_racing_to_publish_cannot_both_win(sink: FakeClickHouse) -> None:
    """Compare-and-swap on the pointer (case ``E2E-31``)."""
    pipeline = BuildPipeline(sink)
    sink.insert_raw([row(1)])
    audit = pipeline.audit_raw("ds")
    for build_id in ("b1", "b2"):
        pipeline.build_canonical(
            build_id, audit=audit, source_manifest_sha256="x", expected_identity_count=1
        )
        pipeline.materialize_bars(build_id)
        pipeline.mark_verified(build_id, proof=verified_proof())
    assert pipeline.publish("b1", expected_current=None) is True
    # The loser's expectation is stale, so its swap is refused.
    assert pipeline.publish("b2", expected_current=None) is False
    assert sink.ready_build == "b1"
    assert pipeline.counters.publish_races_lost == 1


@pytest.mark.whatif("P07-G19-05")
def test_a_published_build_is_immutable(sink: FakeClickHouse) -> None:
    """``INV-13``: correcting history means a new build, never an edit."""
    pipeline = BuildPipeline(sink)
    sink.insert_raw([row(1)])
    audit = pipeline.audit_raw("ds")
    pipeline.build_canonical("b1", audit=audit, source_manifest_sha256="x", expected_identity_count=1)
    pipeline.materialize_bars("b1")
    pipeline.mark_verified("b1", proof=verified_proof())
    pipeline.publish("b1", expected_current=None)
    with pytest.raises(SinkError, match="READY_IMMUTABILITY_VIOLATED"):
        sink.insert_canonical("b1", [])
    with pytest.raises(SinkError, match="READY_IMMUTABILITY_VIOLATED"):
        sink.insert_bars("b1", [])
    with pytest.raises(SinkError, match="READY_IMMUTABILITY_VIOLATED"):
        sink.set_build_status("b1", BuildStatus.DIRTY)


@pytest.mark.whatif("P07-G19-03")
def test_republishing_the_same_build_is_idempotent(sink: FakeClickHouse) -> None:
    """Case ``E2E-30``: the pointer moved but the caller never learned it."""
    pipeline = BuildPipeline(sink)
    sink.insert_raw([row(1)])
    audit = pipeline.audit_raw("ds")
    pipeline.build_canonical("b1", audit=audit, source_manifest_sha256="x", expected_identity_count=1)
    pipeline.mark_verified("b1", proof=verified_proof())
    assert pipeline.publish("b1", expected_current=None) is True
    assert sink.publish_ready("b1", expected_current="b1") is True
    assert sink.ready_build == "b1"


def test_a_dirty_build_cannot_be_published(sink: FakeClickHouse) -> None:
    pipeline = BuildPipeline(sink)
    sink.insert_raw([row(1)])
    audit = pipeline.audit_raw("ds")
    pipeline.build_canonical("b1", audit=audit, source_manifest_sha256="x", expected_identity_count=1)
    pipeline.mark_dirty("b1", "reconciliation findings")
    with pytest.raises(SinkError, match="BUILD_NOT_VERIFIED"):
        pipeline.publish("b1", expected_current=None)


@pytest.mark.whatif("P07-G20-03")
def test_ttl_removes_raw_rows_and_the_audit_sees_fewer(sink: FakeClickHouse) -> None:
    sink.insert_raw([row(1, offset=0), row(2, offset=1), row(3, offset=2)])
    removed = sink.ttl_expire_raw(before_ingested_at_ns=3)
    assert removed == 2
    assert BuildPipeline(sink).audit_raw("ds").physical_rows == 1


@pytest.mark.whatif("P07-G20-05")
def test_backup_and_restore_round_trips(sink: FakeClickHouse) -> None:
    """Taking a backup is not proof; restoring and comparing is (``INV-18``)."""
    sink.insert_raw([row(1), row(2, offset=1)])
    snapshot = sink.backup()
    sink.drop_everything()
    assert sink.raw_row_count() == 0
    sink.restore(snapshot)
    assert sink.raw_row_count() == 2


def test_an_unavailable_server_fails_both_reads_and_writes(sink: FakeClickHouse) -> None:
    sink.available = False
    with pytest.raises(SinkUnavailable):
        sink.insert_raw([row(1)])
    with pytest.raises(SinkUnavailable):
        list(sink.scan_raw())


def test_describe_reports_the_store_state(sink: FakeClickHouse) -> None:
    sink.insert_raw([row(1)])
    described = sink.describe()
    assert described["raw_rows"] == 1
    assert described["ready_build"] is None
    assert described["insert_calls"] == 1
