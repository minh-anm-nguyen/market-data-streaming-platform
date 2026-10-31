"""E2E-11 cross-topic ordering and event-time semantics.

Spec 3.3: Kafka orders records only inside one topic-partition. Ticks and
quotes live in different topics, so for one symbol ``sequence=102`` can be
written before ``sequence=101``. Three things must therefore be true, and each
has a test here:

  * a gap in the *ticks* sequence is not evidence of loss, because the missing
    numbers are quotes;
  * the table may hold rows in arrival order, so reconstruction must order by
    ``event_time_ns`` and ``sequence``, never by ``ingested_at`` or offset;
  * the same source sequence appearing as both a tick and a quote is a
    conflict, not two valid events.
"""

from __future__ import annotations

from p05_ingest.canonical_digest import payload_digest
from p05_ingest.contract_validator import ContractValidator
from p05_ingest.model import EventIdentity, TopicPartition
from p05_ingest.reconciler import (
    Verdict,
    observed_from_kafka_scan,
    observed_from_sink_rows,
    reconcile,
)
from p05_ingest.testkit import (
    QUOTES_TOPIC,
    TICKS_TOPIC,
    make_quote,
    make_tick,
)
from tests.conftest import build_harness, make_config


def test_e2e_11_interleaved_sequence_across_two_topics_is_complete(
    dev_config, clock
):
    """Sequences 0..5 for one symbol, alternating tick and quote topics."""
    cfg = make_config(dev_config, batch={"per_partition_max_rows": 2})
    harness = build_harness(cfg, clock=clock, partitions=1)

    for sequence in range(6):
        if sequence % 2 == 0:
            event = make_tick(sequence=sequence, event_time_ns=1_704_000_000_000_000_000 + sequence)
            topic = TICKS_TOPIC
        else:
            event = make_quote(sequence=sequence, event_time_ns=1_704_000_000_000_000_000 + sequence)
            topic = QUOTES_TOPIC
        harness.cluster.produce(
            topic, 0, key=b"EURUSD", value=event.SerializeToString()
        )

    status = harness.finish()
    assert status.complete, status.reason

    sequences = sorted(row["sequence"] for row in harness.sink.rows)
    assert sequences == list(range(6)), "the union across topics is contiguous"

    # Within each topic the sequences are NOT contiguous, and that is correct.
    ticks = sorted(
        r["sequence"] for r in harness.sink.rows if r["kafka_topic"] == TICKS_TOPIC
    )
    quotes = sorted(
        r["sequence"] for r in harness.sink.rows if r["kafka_topic"] == QUOTES_TOPIC
    )
    assert ticks == [0, 2, 4]
    assert quotes == [1, 3, 5]
    assert ticks != list(range(len(ticks))), (
        "a per-topic contiguity check would wrongly report loss here"
    )


def test_e2e_11_delayed_quote_topic_does_not_create_a_false_gap(dev_config, clock):
    """Ingest the ticks topic first, then the quotes topic much later.

    The intermediate state has visible sequence gaps. Nothing may conclude
    loss from that, and the final state must be complete.
    """
    cfg = make_config(
        dev_config, kafka={"topics": ["md.ticks.v1"]}, batch={"per_partition_max_rows": 3}
    )
    harness = build_harness(cfg, clock=clock, partitions=1)
    for sequence in (0, 2, 4):
        event = make_tick(sequence=sequence)
        harness.cluster.produce(
            TICKS_TOPIC, 0, key=b"EURUSD", value=event.SerializeToString()
        )
    harness.finish()

    mid_run = sorted(row["sequence"] for row in harness.sink.rows)
    assert mid_run == [0, 2, 4], "gaps are expected while the other topic lags"

    # Now the quotes arrive. A fresh consumer group over both topics.
    both = make_config(dev_config, batch={"per_partition_max_rows": 3})
    second = build_harness(both, clock=clock, partitions=1)
    for sequence in (1, 3, 5):
        event = make_quote(sequence=sequence)
        second.cluster.produce(
            QUOTES_TOPIC, 0, key=b"EURUSD", value=event.SerializeToString()
        )
    second.finish()

    combined = sorted(mid_run + [r["sequence"] for r in second.sink.rows])
    assert combined == list(range(6))


def test_arrival_order_may_differ_from_event_order(dev_config, clock):
    """The table can hold rows out of event order; analytics must sort.

    Spec 3.3: ``ingested_at`` must never be used to infer market chronology.
    """
    cfg = make_config(dev_config, batch={"per_partition_max_rows": 1})
    harness = build_harness(cfg, clock=clock, partitions=1)

    base = 1_704_000_000_000_000_000
    # Produce the *later* event on the quotes topic first.
    harness.cluster.produce(
        QUOTES_TOPIC,
        0,
        key=b"EURUSD",
        value=make_quote(sequence=2, event_time_ns=base + 200).SerializeToString(),
    )
    harness.cluster.produce(
        TICKS_TOPIC,
        0,
        key=b"EURUSD",
        value=make_tick(sequence=1, event_time_ns=base + 100).SerializeToString(),
    )

    harness.finish()
    assert harness.sink.row_count == 2

    # Event-time order is well defined and recoverable from the columns.
    by_event_time = [
        r["sequence"]
        for r in sorted(harness.sink.rows, key=lambda r: r["event_time_ns"])
    ]
    assert by_event_time == [1, 2]

    # Insert order, by contrast, depends on which partition poll visited first
    # and is therefore not a usable chronology. Both rows exist either way.
    assert {r["sequence"] for r in harness.sink.rows} == {1, 2}

    # The canonical reconstruction uses event_time_ns and sequence.
    canonical = sorted(
        harness.sink.rows, key=lambda r: (r["event_time_ns"], r["sequence"])
    )
    assert [r["sequence"] for r in canonical] == [1, 2]


def test_same_sequence_as_both_tick_and_quote_is_a_conflict(dev_config, clock):
    """Spec 3.2: event_kind is not part of the identity, so this conflicts."""
    cfg = make_config(dev_config, batch={"per_partition_max_rows": 2})
    harness = build_harness(cfg, clock=clock, partitions=1)

    harness.cluster.produce(
        TICKS_TOPIC,
        0,
        key=b"EURUSD",
        value=make_tick(sequence=7).SerializeToString(),
    )
    harness.cluster.produce(
        QUOTES_TOPIC,
        0,
        key=b"EURUSD",
        value=make_quote(sequence=7).SerializeToString(),
    )

    harness.finish()

    digests = harness.sink.digests_by_identity()
    assert list(digests) == [EventIdentity("demo-a1b2c3", "EURUSD", 7)]
    assert len(next(iter(digests.values()))) == 2, "two kinds, one identity"

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


def test_equal_event_timestamps_are_preserved_not_deduplicated(dev_config, clock):
    """Two distinct events may share a nanosecond; neither may be dropped."""
    cfg = make_config(
        dev_config, kafka={"topics": ["md.ticks.v1"]}, batch={"per_partition_max_rows": 2}
    )
    harness = build_harness(cfg, clock=clock, partitions=1)
    same_ns = 1_704_000_000_000_000_000
    for sequence in (1, 2):
        harness.cluster.produce(
            TICKS_TOPIC,
            0,
            key=b"EURUSD",
            value=make_tick(sequence=sequence, event_time_ns=same_ns).SerializeToString(),
        )

    harness.finish()

    assert harness.sink.row_count == 2
    assert {r["event_time_ns"] for r in harness.sink.rows} == {same_ns}
    assert sorted(r["sequence"] for r in harness.sink.rows) == [1, 2]


def test_per_symbol_partitioning_preserves_relative_order(dev_config, clock):
    """All events for one symbol share a partition, so their order survives."""
    cfg = make_config(
        dev_config, kafka={"topics": ["md.ticks.v1"]}, batch={"per_partition_max_rows": 50}
    )
    harness = build_harness(cfg, clock=clock, partitions=1)
    for sequence in range(20):
        harness.cluster.produce(
            TICKS_TOPIC,
            0,
            key=b"EURUSD",
            value=make_tick(sequence=sequence).SerializeToString(),
        )

    harness.finish()

    rows = sorted(harness.sink.rows, key=lambda r: r["kafka_offset"])
    assert [r["sequence"] for r in rows] == list(range(20)), (
        "offset order and sequence order agree inside one partition"
    )


def test_cross_topic_ordering_is_not_claimed_in_any_committed_state(
    dev_config, clock
):
    """The two topics commit independently; neither waits for the other."""
    cfg = make_config(dev_config, batch={"per_partition_max_rows": 2})
    harness = build_harness(cfg, clock=clock, partitions=1)
    for sequence in range(4):
        harness.cluster.produce(
            TICKS_TOPIC,
            0,
            key=b"EURUSD",
            value=make_tick(sequence=sequence * 2).SerializeToString(),
        )
    for sequence in range(4):
        harness.cluster.produce(
            QUOTES_TOPIC,
            0,
            key=b"EURUSD",
            value=make_quote(sequence=sequence * 2 + 1).SerializeToString(),
        )

    harness.finish()

    committed = harness.committed()
    assert committed[TopicPartition(TICKS_TOPIC, 0)] == 4
    assert committed[TopicPartition(QUOTES_TOPIC, 0)] == 4
