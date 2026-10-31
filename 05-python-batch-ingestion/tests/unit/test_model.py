"""The domain model: immutability, column mapping and the enum wire values.

Small but load-bearing. The ``EventKind`` integer values and the insert column
order are part of the stored contract, so a careless edit would silently
invalidate every row already in the table. These tests pin them.
"""

from __future__ import annotations

import datetime as dt

import pytest

from p05_ingest.model import (
    CH_DATE_MAX,
    CH_DATE_MIN,
    CH_INSERT_COLUMNS,
    INT64_MAX,
    INT64_MIN,
    NS_PER_DAY,
    ConsumedRecord,
    EventIdentity,
    EventKind,
    EventRow,
    LogicalProjection,
    PartitionState,
    RecordDisposition,
    RunStatus,
    SealedBatch,
    SealReason,
    SinkOutcome,
    SinkResult,
    TopicPartition,
    TransportCoordinate,
)


def projection(**kwargs) -> LogicalProjection:
    base = {
        "dataset_id": "demo",
        "symbol": "EURUSD",
        "sequence": 1,
        "event_time_ns": 1_704_171_845_123_456_789,
        "price_scale": 100_000,
        "kind": EventKind.TICK,
        "source_schema_version": "marketdata.v1",
        "price_ticks": 109_385,
    }
    base.update(kwargs)
    return LogicalProjection(**base)


def row(offset: int = 0) -> EventRow:
    return EventRow(
        projection=projection(sequence=offset),
        coordinate=TransportCoordinate("c1", "gen-a", "md.ticks.v1", 0, offset),
        payload_digest=f"{offset:064d}",
        ingest_run_id="run-1",
        accounted_bytes=10,
    )


# ------------------------------------------------------------------ constants


def test_int64_bounds_are_exact():
    assert INT64_MIN == -(2**63)
    assert INT64_MAX == 2**63 - 1


def test_nanoseconds_per_day():
    assert NS_PER_DAY == 24 * 60 * 60 * 1_000_000_000


def test_clickhouse_date_range_matches_the_server_limits():
    assert CH_DATE_MIN == dt.date(1970, 1, 1)
    assert CH_DATE_MAX == dt.date(2149, 6, 6)


def test_event_kind_wire_values_are_frozen():
    """These integers appear in the DDL and in the canonical digest."""
    assert int(EventKind.TICK) == 1
    assert int(EventKind.QUOTE) == 2
    assert EventKind.TICK.ch_name == "tick"
    assert EventKind.QUOTE.ch_name == "quote"


def test_insert_column_order_is_frozen():
    """Spec 6.1: inserts name their columns, so this order is the contract."""
    assert CH_INSERT_COLUMNS == (
        "dataset_id",
        "symbol",
        "sequence",
        "event_kind",
        "event_time_ns",
        "event_date",
        "price_scale",
        "bid_ticks",
        "ask_ticks",
        "price_ticks",
        "source_schema_version",
        "payload_digest",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "topic_generation",
        "ingest_run_id",
    )
    assert len(set(CH_INSERT_COLUMNS)) == len(CH_INSERT_COLUMNS)


def test_row_tuple_length_matches_the_column_list():
    assert len(row().to_ch_tuple()) == len(CH_INSERT_COLUMNS)


# --------------------------------------------------------------- immutability


def test_projection_is_frozen_and_hashable():
    p = projection()
    with pytest.raises((AttributeError, TypeError)):
        p.price_ticks = 1
    assert {p, projection()} == {p}, "equal projections hash alike"


def test_event_row_is_frozen():
    r = row()
    with pytest.raises((AttributeError, TypeError)):
        r.payload_digest = "x"


def test_sealed_batch_is_frozen_with_a_tuple_of_rows():
    batch = SealedBatch(
        tp=TopicPartition("md.ticks.v1", 0),
        assignment_epoch=1,
        rows=(row(0), row(1), row(2)),
        batch_id="b",
        insert_token="t" * 64,
        seal_reason=SealReason.MAX_ROWS,
        accounted_bytes=30,
        sealed_at_monotonic=0.0,
    )
    assert isinstance(batch.rows, tuple)
    assert len(batch) == 3
    assert batch.first_offset == 0
    assert batch.last_offset == 2
    assert batch.offsets == (0, 1, 2)
    with pytest.raises((AttributeError, TypeError)):
        batch.rows = ()


def test_an_empty_sealed_batch_is_refused():
    with pytest.raises(ValueError, match="at least one row"):
        SealedBatch(
            tp=TopicPartition("md.ticks.v1", 0),
            assignment_epoch=1,
            rows=(),
            batch_id="b",
            insert_token="t" * 64,
            seal_reason=SealReason.FORCED,
            accounted_bytes=0,
            sealed_at_monotonic=0.0,
        )


# ---------------------------------------------------------------- identities


def test_identity_excludes_the_event_kind():
    """Spec 3.2: the kind is not part of the identity, so these collide."""
    tick = projection(kind=EventKind.TICK, price_ticks=5)
    quote = projection(
        kind=EventKind.QUOTE, price_ticks=None, bid_ticks=1, ask_ticks=2
    )
    assert tick.identity == quote.identity == EventIdentity("demo", "EURUSD", 1)


def test_topic_partition_is_hashable_and_readable():
    tp = TopicPartition("md.ticks.v1", 3)
    assert {tp: 1}[TopicPartition("md.ticks.v1", 3)] == 1
    assert str(tp) == "md.ticks.v1[3]"


def test_topic_partitions_sort_by_topic_then_partition():
    unsorted = [
        TopicPartition("md.ticks.v1", 1),
        TopicPartition("md.quotes.v1", 2),
        TopicPartition("md.ticks.v1", 0),
    ]
    assert sorted(unsorted) == [
        TopicPartition("md.quotes.v1", 2),
        TopicPartition("md.ticks.v1", 0),
        TopicPartition("md.ticks.v1", 1),
    ]


# ------------------------------------------------------------------- outcomes


def test_only_acked_is_a_definitive_success():
    assert SinkOutcome.ACKED.is_definitive_success
    for outcome in (
        SinkOutcome.AMBIGUOUS,
        SinkOutcome.TRANSIENT,
        SinkOutcome.PERMANENT,
    ):
        assert not outcome.is_definitive_success


def test_only_ambiguous_and_transient_may_retry():
    assert SinkOutcome.AMBIGUOUS.may_retry_same_batch
    assert SinkOutcome.TRANSIENT.may_retry_same_batch
    assert not SinkOutcome.ACKED.may_retry_same_batch
    assert not SinkOutcome.PERMANENT.may_retry_same_batch


def test_sink_result_carries_the_fencing_epoch():
    result = SinkResult(
        tp=TopicPartition("md.ticks.v1", 0),
        assignment_epoch=9,
        batch_id="b",
        outcome=SinkOutcome.ACKED,
        attempts=1,
        latency_s=0.01,
    )
    assert result.assignment_epoch == 9
    assert result.detail == ""


def test_dispositions_and_states_are_string_enums():
    """They appear as metric label values, so they must render stably."""
    assert str(RecordDisposition.SINK_ACKED) == "sink_acked"
    assert str(PartitionState.BLOCKED) == "blocked"
    assert str(SealReason.MAX_WAIT) == "max_wait"
    assert str(SinkOutcome.AMBIGUOUS) == "ambiguous"


# --------------------------------------------------------------- run status


def test_run_status_defaults_to_not_run():
    """Spec 14.1: pessimistic by default."""
    status = RunStatus()
    assert not status.complete
    assert status.reason == "NOT_RUN"
    assert status.verdict == "INCOMPLETE"
    assert status.blocked_partitions == []


def test_run_status_verdict_reflects_completeness():
    assert RunStatus(complete=True, reason="ok").verdict == "COMPLETE"


# ------------------------------------------------------------ consumed record


def test_consumed_record_defaults_have_no_headers():
    record = ConsumedRecord(
        tp=TopicPartition("md.ticks.v1", 0), offset=0, key=b"EURUSD", value=b"x"
    )
    assert record.headers == ()
    assert record.timestamp_ms is None
