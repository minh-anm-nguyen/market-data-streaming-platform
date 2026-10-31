"""Batch assembly: thresholds, FIFO order, immutability and isolation."""

from __future__ import annotations

import pytest

from p05_ingest.errors import InvariantViolation
from p05_ingest.event_mapper import EventMapper
from p05_ingest.model import EventKind, LogicalProjection, SealReason, TopicPartition
from p05_ingest.partition_batcher import PartitionBatcher
from p05_ingest.testkit import FakeClock

TP0 = TopicPartition("md.ticks.v1", 0)
TP1 = TopicPartition("md.ticks.v1", 1)
TPQ = TopicPartition("md.quotes.v1", 0)


@pytest.fixture
def mapper() -> EventMapper:
    return EventMapper(
        ingest_run_id="run-1", kafka_cluster_id="c1", topic_generation="gen-a"
    )


def make_row(mapper, tp: TopicPartition, offset: int, *, sequence: int | None = None):
    projection = LogicalProjection(
        dataset_id="demo",
        symbol="EURUSD",
        sequence=offset if sequence is None else sequence,
        event_time_ns=1_704_171_845_000_000_000 + offset,
        price_scale=100_000,
        kind=EventKind.TICK,
        source_schema_version="marketdata.v1",
        price_ticks=100_000 + offset,
    )
    return mapper.to_row(
        projection,
        topic=tp.topic,
        partition=tp.partition,
        offset=offset,
        raw_value_len=40,
    )


def build(dev_config, clock, **batch_overrides) -> PartitionBatcher:
    from tests.conftest import make_config

    cfg = make_config(dev_config, batch=batch_overrides) if batch_overrides else dev_config
    return PartitionBatcher(
        cfg.batch,
        cfg.clickhouse,
        topic_generation=cfg.project.topic_generation,
        monotonic=clock.monotonic,
    )


# ----------------------------------------------------------------- thresholds


def test_seals_on_the_row_cap(dev_config, clock, mapper):
    batcher = build(dev_config, clock, per_partition_max_rows=3)
    for offset in range(2):
        batcher.append(TP0, 1, make_row(mapper, TP0, offset))
    assert batcher.seal_reason(TP0) is None

    batcher.append(TP0, 1, make_row(mapper, TP0, 2))
    assert batcher.seal_reason(TP0) is SealReason.MAX_ROWS
    batch = batcher.seal_if_ready(TP0)
    assert len(batch) == 3
    assert batch.seal_reason is SealReason.MAX_ROWS


def test_seals_on_the_byte_cap(dev_config, clock, mapper):
    row_bytes = make_row(mapper, TP0, 0).accounted_bytes
    batcher = build(
        dev_config,
        clock,
        per_partition_max_rows=100_000,
        per_partition_max_serialized_bytes=row_bytes * 2,
    )
    batcher.append(TP0, 1, make_row(mapper, TP0, 0))
    assert batcher.seal_reason(TP0) is None
    batcher.append(TP0, 1, make_row(mapper, TP0, 1))
    assert batcher.seal_reason(TP0) is SealReason.MAX_BYTES


def test_seals_on_age_without_any_new_record(dev_config, clock, mapper):
    """Spec 5.1: an idle partition must still flush on its timer.

    The clock is injected, so this runs instantly instead of sleeping for the
    2-second default.
    """
    batcher = build(dev_config, clock, max_wait_ms=2000)
    batcher.append(TP0, 1, make_row(mapper, TP0, 0))

    clock.advance_ms(1999)
    assert batcher.seal_reason(TP0) is None, "not yet due"

    clock.advance_ms(2)
    assert batcher.seal_reason(TP0) is SealReason.MAX_WAIT


def test_age_is_measured_from_the_first_row(dev_config, clock, mapper):
    batcher = build(dev_config, clock, max_wait_ms=1000)
    batcher.append(TP0, 1, make_row(mapper, TP0, 0))
    clock.advance_ms(600)
    batcher.append(TP0, 1, make_row(mapper, TP0, 1))
    clock.advance_ms(500)
    # 1100 ms since the batch opened, even though the newest row is 500 ms old.
    assert batcher.seal_reason(TP0) is SealReason.MAX_WAIT


def test_empty_partition_never_seals(dev_config, clock):
    batcher = build(dev_config, clock)
    clock.advance_ms(10_000)
    assert batcher.seal_reason(TP0) is None
    assert batcher.seal(TP0, SealReason.FORCED) is None


# --------------------------------------------------------------- FIFO order


def test_rows_keep_consumption_order(dev_config, clock, mapper):
    batcher = build(dev_config, clock, per_partition_max_rows=5)
    offsets = [3, 4, 7, 8, 11]
    for offset in offsets:
        batcher.append(TP0, 1, make_row(mapper, TP0, offset))
    batch = batcher.seal_if_ready(TP0)
    assert list(batch.offsets) == offsets
    assert batch.first_offset == 3
    assert batch.last_offset == 11


def test_non_advancing_offset_is_refused(dev_config, clock, mapper):
    batcher = build(dev_config, clock)
    batcher.append(TP0, 1, make_row(mapper, TP0, 5))
    with pytest.raises(InvariantViolation, match="strict FIFO"):
        batcher.append(TP0, 1, make_row(mapper, TP0, 5))
    with pytest.raises(InvariantViolation, match="strict FIFO"):
        batcher.append(TP0, 1, make_row(mapper, TP0, 4))


def test_row_from_another_partition_is_refused(dev_config, clock, mapper):
    batcher = build(dev_config, clock)
    with pytest.raises(InvariantViolation, match="appended to the batch for"):
        batcher.append(TP0, 1, make_row(mapper, TP1, 0))


def test_epoch_change_mid_batch_is_refused(dev_config, clock, mapper):
    """A partition that changed hands must not contribute to an open batch."""
    batcher = build(dev_config, clock)
    batcher.append(TP0, 1, make_row(mapper, TP0, 0))
    with pytest.raises(InvariantViolation, match="changed hands"):
        batcher.append(TP0, 2, make_row(mapper, TP0, 1))


# ------------------------------------------------------------- isolation


def test_partitions_do_not_mix(dev_config, clock, mapper):
    """Spec 5.1: a batch holds exactly one topic-partition."""
    batcher = build(dev_config, clock, per_partition_max_rows=2)
    batcher.append(TP0, 1, make_row(mapper, TP0, 0))
    batcher.append(TP1, 1, make_row(mapper, TP1, 0))
    batcher.append(TPQ, 1, make_row(mapper, TPQ, 0))
    assert batcher.seal_reason(TP0) is None

    batcher.append(TP0, 1, make_row(mapper, TP0, 1))
    batch = batcher.seal_if_ready(TP0)
    assert all(r.coordinate.partition == 0 for r in batch.rows)
    assert all(r.coordinate.topic == TP0.topic for r in batch.rows)
    assert batcher.open_rows(TP1) == 1, "TP1 untouched"
    assert batcher.open_rows(TPQ) == 1


def test_seal_all_drains_every_partition_oldest_first(dev_config, clock, mapper):
    batcher = build(dev_config, clock)
    batcher.append(TP1, 1, make_row(mapper, TP1, 0))
    clock.advance_ms(10)
    batcher.append(TP0, 1, make_row(mapper, TP0, 0))
    clock.advance_ms(10)
    batcher.append(TPQ, 1, make_row(mapper, TPQ, 0))

    batches = batcher.seal_all(SealReason.SHUTDOWN)
    assert [b.tp for b in batches] == [TP1, TP0, TPQ]
    assert all(b.seal_reason is SealReason.SHUTDOWN for b in batches)
    assert batcher.open_partitions == frozenset()


# ----------------------------------------------------------- immutability


def test_sealed_batch_is_immutable(dev_config, clock, mapper):
    """Spec I-04: a retry must re-send identical bytes, so rows is a tuple."""
    batcher = build(dev_config, clock, per_partition_max_rows=1)
    batcher.append(TP0, 1, make_row(mapper, TP0, 0))
    batch = batcher.seal_if_ready(TP0)

    assert isinstance(batch.rows, tuple)
    with pytest.raises((AttributeError, TypeError)):
        batch.rows = ()
    with pytest.raises(TypeError):
        batch.rows[0] = None


def test_appending_after_seal_starts_a_fresh_batch(dev_config, clock, mapper):
    """The sealed batch must not acquire rows added afterwards."""
    batcher = build(dev_config, clock, per_partition_max_rows=1)
    batcher.append(TP0, 1, make_row(mapper, TP0, 0))
    first = batcher.seal_if_ready(TP0)

    batcher.append(TP0, 1, make_row(mapper, TP0, 1))
    second = batcher.seal_if_ready(TP0)

    assert len(first) == 1 and first.offsets == (0,)
    assert len(second) == 1 and second.offsets == (1,)
    assert first.insert_token != second.insert_token


def test_identical_content_reproduces_the_same_token(dev_config, clock, mapper):
    """A re-read after a crash regroups identically, so the token matches.

    This is what makes ClickHouse block de-duplication able to help at all.
    """
    tokens = []
    for _ in range(2):
        batcher = build(dev_config, clock, per_partition_max_rows=2)
        batcher.append(TP0, 1, make_row(mapper, TP0, 0))
        batcher.append(TP0, 1, make_row(mapper, TP0, 1))
        tokens.append(batcher.seal_if_ready(TP0).insert_token)
    assert tokens[0] == tokens[1]


def test_different_grouping_of_the_same_rows_gives_a_different_token(
    dev_config, clock, mapper
):
    """Spec 5.3: a restart may regroup, and then de-duplication cannot help.

    The token must reflect that honestly rather than collide and silently
    suppress a write.
    """
    batcher = build(dev_config, clock, per_partition_max_rows=2)
    batcher.append(TP0, 1, make_row(mapper, TP0, 0))
    batcher.append(TP0, 1, make_row(mapper, TP0, 1))
    two_rows = batcher.seal_if_ready(TP0).insert_token

    batcher = build(dev_config, clock, per_partition_max_rows=1)
    batcher.append(TP0, 1, make_row(mapper, TP0, 0))
    one_row = batcher.seal_if_ready(TP0).insert_token

    assert two_rows != one_row


def test_token_changes_when_payload_changes_at_the_same_offsets(
    dev_config, clock, mapper
):
    def token_for(sequence_base: int) -> str:
        batcher = build(dev_config, clock, per_partition_max_rows=2)
        for offset in (0, 1):
            batcher.append(
                TP0,
                1,
                make_row(mapper, TP0, offset, sequence=sequence_base + offset),
            )
        return batcher.seal_if_ready(TP0).insert_token

    assert token_for(0) != token_for(1000)


# --------------------------------------------------------------- discard


def test_discard_returns_held_bytes_and_drops_the_batch(dev_config, clock, mapper):
    batcher = build(dev_config, clock)
    row = make_row(mapper, TP0, 0)
    batcher.append(TP0, 1, row)
    assert batcher.has_open(TP0)

    released = batcher.discard(TP0)
    assert released == row.accounted_bytes
    assert not batcher.has_open(TP0)
    assert batcher.discard(TP0) == 0, "discarding twice is harmless"


def test_accounted_bytes_sum_over_rows(dev_config, clock, mapper):
    batcher = build(dev_config, clock, per_partition_max_rows=3)
    rows = [make_row(mapper, TP0, offset) for offset in range(3)]
    for row in rows:
        batcher.append(TP0, 1, row)
    batch = batcher.seal_if_ready(TP0)
    assert batch.accounted_bytes == sum(r.accounted_bytes for r in rows)


def test_oldest_open_age_tracks_the_clock(dev_config, clock, mapper):
    batcher = build(dev_config, clock)
    assert batcher.oldest_open_age_s(TP0) == 0.0
    batcher.append(TP0, 1, make_row(mapper, TP0, 0))
    clock.advance(1.5)
    assert batcher.oldest_open_age_s(TP0) == pytest.approx(1.5)
