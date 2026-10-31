"""Combined fault campaigns (spec group V).

Single faults are covered elsewhere. These cases overlap them, because the
failure modes that survive isolated testing tend to be the ones that need two
things to go wrong at once: a lost ACK *while* a rebalance is happening, a
commit timeout *while* the sink is refusing work.

Every case asserts the same two invariants after every single step, so a
violation is attributed to the step that caused it rather than discovered at
the end:

  * I-01 -- the committed offset never exceeds what the sink acknowledged;
  * I-07 -- no invalid record is ever written to the market table.
"""

from __future__ import annotations

import random

import pytest

from p05_ingest.model import TopicPartition
from p05_ingest.testkit import (
    QUOTES_TOPIC,
    TICKS_TOPIC,
    Behaviour,
    CommitFault,
    make_tick,
)
from p05_ingest.testkit.workload import produce_market_workload
from tests.conftest import build_harness, make_config

pytestmark = pytest.mark.chaos

BEHAVIOURS = [
    Behaviour.ACK,
    Behaviour.ACK,
    Behaviour.WRITE_THEN_LOSE_ACK,
    Behaviour.REFUSE_BEFORE_WRITE,
    Behaviour.UNKNOWN_ERROR,
]
COMMIT_FAULTS = [
    CommitFault.NONE,
    CommitFault.NONE,
    CommitFault.REJECT,
    CommitFault.LOST_RESPONSE,
    CommitFault.UNREACHABLE,
]


def assert_invariants(harness) -> None:
    """I-01 and I-07, checked against the broker and the table directly."""
    for tp, committed in harness.committed().items():
        written = {
            row["kafka_offset"]
            for row in harness.sink.rows
            if row["kafka_topic"] == tp.topic
            and row["kafka_partition"] == tp.partition
        }
        log = harness.cluster.log(tp)
        delivered = {r.offset for r in log.records if not r.is_control}
        unproven = {o for o in delivered if o < committed} - written
        assert not unproven, (
            f"I-01 violated on {tp}: offsets {sorted(unproven)} are below the "
            f"committed position {committed} but absent from the table"
        )

    for row in harness.sink.rows:
        assert row["dataset_id"], "I-07: a row with no dataset_id was written"
        assert row["price_scale"] > 0
        assert row["sequence"] >= 0
        if row["event_kind"] == "tick":
            assert row["price_ticks"] is not None and row["price_ticks"] > 0
        else:
            assert row["bid_ticks"] is not None and row["ask_ticks"] is not None
            assert row["bid_ticks"] <= row["ask_ticks"]


@pytest.mark.parametrize("seed", [1, 7, 13, 42, 99])
def test_random_fault_campaign_preserves_the_invariants(dev_config, clock, seed):
    """A seeded storm of sink, commit and rebalance faults."""
    rng = random.Random(seed)
    cfg = make_config(
        dev_config,
        batch={"per_partition_max_rows": 4, "max_inflight_batches_global": 2},
        retry={"max_attempts": 2, "overall_deadline_s": 5},
    )
    harness = build_harness(cfg, clock=clock, partitions=2, dedup_window=50)
    produce_market_workload(harness.cluster, events=300, partitions=2, seed=seed)

    for step in range(500):
        action = rng.randrange(10)
        if action == 0:
            harness.sink.set_script(
                [rng.choice(BEHAVIOURS) for _ in range(rng.randrange(1, 4))]
            )
        elif action == 1:
            harness.consumer.commit_fault = rng.choice(COMMIT_FAULTS)
            harness.consumer.commit_fault_remaining = rng.randrange(1, 3)
        elif action == 2 and harness.consumer.assignment():
            tp = rng.choice(sorted(harness.consumer.assignment()))
            if rng.random() < 0.5:
                harness.consumer.revoke([tp])
            else:
                harness.consumer.lose([tp])
            harness.engine.step()
            harness.consumer.assign([tp])
        elif action == 3:
            harness.tick_timer()

        harness.engine.step()
        assert_invariants(harness)

    harness.engine.shutdown()
    assert_invariants(harness)


def test_lost_ack_during_a_rebalance(dev_config, clock):
    """The two hardest faults at once, in the order that hurts most."""
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={"per_partition_max_rows": 4},
        retry={"max_attempts": 3, "overall_deadline_s": 10},
    )
    harness = build_harness(
        cfg,
        clock=clock,
        partitions=1,
        sink_script=(Behaviour.WRITE_THEN_LOSE_ACK, Behaviour.ACK),
        dedup_window=0,
    )
    for sequence in range(12):
        harness.cluster.produce(
            TICKS_TOPIC,
            0,
            key=b"EURUSD",
            value=make_tick(sequence=sequence).SerializeToString(),
        )

    tp0 = TopicPartition(TICKS_TOPIC, 0)
    for _ in range(40):
        harness.engine.step()
        assert_invariants(harness)
        if harness.sink.attempts:
            break
        harness.tick_timer()

    harness.consumer.lose([tp0])
    harness.engine.step()
    harness.consumer.assign([tp0])

    harness.drain()
    harness.engine.shutdown()
    assert_invariants(harness)

    # The data ends up complete, with duplicates allowed and no conflicts.
    assert len(harness.logical_identities()) == 12
    digests = harness.sink.digests_by_identity()
    assert all(len(v) == 1 for v in digests.values())


def test_sink_failure_and_commit_failure_together(dev_config, clock):
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={"per_partition_max_rows": 3},
        retry={"max_attempts": 4, "overall_deadline_s": 10},
    )
    harness = build_harness(
        cfg,
        clock=clock,
        partitions=1,
        sink_script=(
            Behaviour.REFUSE_BEFORE_WRITE,
            Behaviour.ACK,
            Behaviour.WRITE_THEN_LOSE_ACK,
            Behaviour.ACK,
        ),
        sink_default=Behaviour.ACK,
    )
    harness.consumer.commit_fault = CommitFault.REJECT
    harness.consumer.commit_fault_remaining = 3

    for sequence in range(15):
        harness.cluster.produce(
            TICKS_TOPIC,
            0,
            key=b"EURUSD",
            value=make_tick(sequence=sequence).SerializeToString(),
        )

    for _ in range(200):
        harness.engine.step()
        assert_invariants(harness)
        harness.tick_timer()

    harness.engine.shutdown()
    assert_invariants(harness)
    assert len(harness.logical_identities()) == 15


def test_poison_record_during_a_sink_outage(dev_config, clock):
    """Two independent reasons to stop, arriving together."""
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={"per_partition_max_rows": 2},
        retry={"max_attempts": 1, "overall_deadline_s": 2},
    )
    harness = build_harness(
        cfg, clock=clock, partitions=2, sink_default=Behaviour.REFUSE_BEFORE_WRITE
    )
    for sequence in range(6):
        harness.cluster.produce(
            TICKS_TOPIC,
            0,
            key=b"EURUSD",
            value=make_tick(sequence=sequence).SerializeToString(),
        )
    harness.cluster.produce(TICKS_TOPIC, 1, key=b"EURUSD", value=b"\xff\xff\xff")

    status = harness.finish()
    assert_invariants(harness)

    assert not status.complete
    assert len(harness.engine.blocked_partitions) == 2
    assert harness.sink.row_count == 0
    assert harness.committed() == {}


def test_a_long_mixed_campaign_across_both_topics(dev_config, clock):
    """A longer soak-shaped run with repeated faults on both topics."""
    cfg = make_config(
        dev_config,
        batch={"per_partition_max_rows": 8, "max_inflight_batches_global": 3},
        retry={"max_attempts": 3, "overall_deadline_s": 10},
    )
    harness = build_harness(cfg, clock=clock, partitions=4, dedup_window=200)
    produce_market_workload(harness.cluster, events=1200, partitions=4, seed=5)

    rng = random.Random(5)
    for step in range(1200):
        if step % 37 == 0:
            harness.sink.set_script([Behaviour.WRITE_THEN_LOSE_ACK, Behaviour.ACK])
        if step % 53 == 0:
            harness.consumer.commit_fault = CommitFault.LOST_RESPONSE
            harness.consumer.commit_fault_remaining = 1
        if step % 71 == 0 and harness.consumer.assignment():
            tp = rng.choice(sorted(harness.consumer.assignment()))
            harness.consumer.revoke([tp])
            harness.engine.step()
            harness.consumer.assign([tp])
        harness.engine.step()
        if step % 10 == 0:
            harness.tick_timer()
            assert_invariants(harness)

    harness.engine.shutdown()
    assert_invariants(harness)
    # No conflicts may appear under any amount of churn.
    digests = harness.sink.digests_by_identity()
    conflicted = {k: v for k, v in digests.items() if len(v) > 1}
    assert not conflicted, f"churn produced identity conflicts: {conflicted}"
