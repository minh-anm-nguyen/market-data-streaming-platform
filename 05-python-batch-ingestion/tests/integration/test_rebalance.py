"""E2E-04 lost ownership mid-insert, and E2E-22 rolling deployment.

The hard case the design exists for: a partition is taken away while a
ClickHouse insert is in flight. The write can still land -- nothing can stop it
-- but the old owner must not turn it into committed progress.
"""

from __future__ import annotations

from p05_ingest.metrics import REBALANCES, STALE_COMPLETIONS_FENCED
from p05_ingest.model import TopicPartition
from p05_ingest.testkit import TICKS_TOPIC, Behaviour, make_tick
from tests.conftest import build_harness, make_config

TP0 = TopicPartition(TICKS_TOPIC, 0)
TP1 = TopicPartition(TICKS_TOPIC, 1)


def ticks_only(dev_config, **batch):
    return make_config(dev_config, kafka={"topics": ["md.ticks.v1"]}, batch=batch)


def produce_ticks(cluster, count: int, partition: int = 0, start: int = 0) -> None:
    for i in range(count):
        event = make_tick(sequence=start + i)
        cluster.produce(
            TICKS_TOPIC, partition, key=b"EURUSD", value=event.SerializeToString()
        )


# ---------------------------------------------------------------- E2E-04


def test_e2e_04_lost_ownership_forbids_the_commit(dev_config, clock):
    """on_lost: no commit, run marked incomplete for that partition (I-02)."""
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_ticks(harness.cluster, 4)

    # Let the batch land in ClickHouse but intercept before the commit, then
    # take the partition away.
    for _ in range(50):
        harness.engine.step()
        if harness.sink.row_count >= 4 and not harness.committed():
            break
        harness.tick_timer()

    harness.consumer.lose([TP0])
    status = harness.engine.shutdown()

    assert harness.cluster.committed(cfg.kafka.group_id, TP0) is None, (
        "a lost partition must never be committed"
    )
    assert not status.complete
    assert "lost ownership without commit" in status.reason
    assert harness.metrics.counter(REBALANCES, kind="lost") == 1


def test_e2e_04_stale_completion_after_loss_is_fenced(dev_config, clock):
    """A worker result arriving after the loss must not advance anything.

    The sink is scripted to lose the ACK, so the batch is still outstanding
    when ownership goes. The retry then completes against a partition this
    consumer no longer owns.
    """
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
    )
    produce_ticks(harness.cluster, 4)

    # Seal and submit, then lose ownership before draining the result.
    for _ in range(30):
        harness.engine.step()
        if harness.sink.attempts:
            break
        harness.tick_timer()

    harness.consumer.lose([TP0])
    # Re-acquire under a brand-new epoch; the old completion is now stale.
    harness.consumer.assign([TP0])
    harness.engine.step()

    assert harness.cluster.committed(cfg.kafka.group_id, TP0) in (None, 0)

    # The new owner re-reads and finishes the work itself, so the data ends up
    # complete...
    status = harness.finish()
    assert len(harness.logical_identities()) == 4

    # ...but the run is still reported INCOMPLETE, because a partition was
    # lost during it. Spec I-02 requires that verdict even when the data
    # happens to come out right: the process cannot prove it did not miss
    # anything while it did not own the partition.
    assert not status.complete
    assert "lost ownership without commit" in status.reason


def test_e2e_04_new_epoch_replays_and_stays_logically_unique(dev_config, clock):
    cfg = ticks_only(dev_config, per_partition_max_rows=3)
    harness = build_harness(cfg, clock=clock, partitions=1, dedup_window=0)
    produce_ticks(harness.cluster, 6)

    for _ in range(40):
        harness.engine.step()
        if harness.sink.row_count >= 3:
            break
        harness.tick_timer()

    epoch_before = harness.engine.assignments.epoch_of(TP0)
    harness.consumer.lose([TP0])
    harness.consumer.assign([TP0])
    epoch_after = harness.engine.assignments.epoch_of(TP0)
    assert epoch_after > epoch_before, "a re-assignment must get a fresh epoch"

    harness.finish()

    assert len(harness.logical_identities()) == 6
    digests = harness.sink.digests_by_identity()
    assert all(len(v) == 1 for v in digests.values()), "replay, not corruption"


def test_cooperative_revoke_commits_the_proven_prefix(dev_config, clock):
    """A revoke has a drain window, so proven progress may still be saved."""
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_ticks(harness.cluster, 4)

    for _ in range(50):
        harness.engine.step()
        if harness.sink.row_count >= 4 and not harness.committed():
            break
        harness.tick_timer()

    harness.consumer.revoke([TP0])

    assert harness.cluster.committed(cfg.kafka.group_id, TP0) == 4, (
        "a cooperative revoke should save the already-acknowledged prefix"
    )
    assert harness.metrics.counter(REBALANCES, kind="revoke") == 1


def test_revoke_discards_an_unsent_batch_without_committing_it(dev_config, clock):
    """Rows buffered but never sent must be re-read by the next owner."""
    cfg = ticks_only(dev_config, per_partition_max_rows=100)
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_ticks(harness.cluster, 5)

    harness.engine.step()  # buffers without reaching the row cap
    assert harness.engine.batcher.open_rows(TP0) == 5
    assert harness.sink.row_count == 0

    harness.consumer.revoke([TP0])

    assert harness.cluster.committed(cfg.kafka.group_id, TP0) is None
    assert harness.sink.row_count == 0
    assert harness.engine.budget.reserved_for(TP0) == 0, "budget was released"

    # A new owner reads them from the beginning and completes the work.
    harness.consumer.assign([TP0])
    harness.finish()
    assert len(harness.logical_identities()) == 5


def test_revoking_one_partition_does_not_disturb_another(dev_config, clock):
    cfg = ticks_only(dev_config, per_partition_max_rows=3)
    harness = build_harness(cfg, clock=clock, partitions=2)
    produce_ticks(harness.cluster, 3, partition=0, start=0)
    produce_ticks(harness.cluster, 3, partition=1, start=100)

    harness.drain()
    harness.consumer.lose([TP0])
    status = harness.engine.shutdown()

    assert harness.cluster.committed(cfg.kafka.group_id, TP1) == 3, (
        "the untouched partition keeps its commit"
    )
    assert not status.complete, "the lost partition makes the run incomplete"


def test_records_for_an_unowned_partition_are_dropped_not_buffered(dev_config, clock):
    """A record arriving for a partition we no longer own must be fenced."""
    cfg = ticks_only(dev_config, per_partition_max_rows=100)
    harness = build_harness(cfg, clock=clock, partitions=1, assign_on_subscribe=False)
    produce_ticks(harness.cluster, 3)

    # Assign, then lose, then force-feed a record through the consumer.
    harness.consumer.assign([TP0])
    harness.consumer.lose([TP0])
    harness.consumer._assigned.append(TP0)  # the broker still delivers briefly
    harness.consumer._positions[TP0] = 0

    report = harness.engine.step()

    assert report.stale_fenced >= 1
    assert harness.metrics.counter(STALE_COMPLETIONS_FENCED, kind="record") >= 1
    assert harness.engine.batcher.open_rows(TP0) == 0


# ---------------------------------------------------------------- E2E-22


def test_e2e_22_rolling_deployment_does_not_commit_ahead(dev_config, clock):
    """Two consumers in sequence over one partition, as in a rolling restart.

    Partitions move from the old owner to the new one while work is in
    progress. Neither may commit past what it personally proved.
    """
    cfg = ticks_only(dev_config, per_partition_max_rows=3)
    old = build_harness(cfg, clock=clock, partitions=1, dedup_window=1000)
    produce_ticks(old.cluster, 12)

    for _ in range(12):
        old.engine.step()
        old.tick_timer()
    committed_by_old = old.cluster.committed(cfg.kafka.group_id, TP0) or 0

    # The new binary takes over the partition mid-flight.
    old.consumer.revoke([TP0])
    new = build_harness(cfg, clock=clock, partitions=0, dedup_window=1000)
    # Point the new harness at the same cluster and sink.
    new.cluster = old.cluster
    new = _rebuild_against(old)

    status = new.finish()

    assert status.complete, status.reason
    assert len(new.logical_identities()) == 12
    final_committed = new.cluster.committed(cfg.kafka.group_id, TP0)
    assert final_committed == 12
    assert final_committed >= committed_by_old, "progress never moves backwards"

    # Everything below the committed offset is physically present.
    written = {
        row["kafka_offset"]
        for row in new.sink.rows
        if row["kafka_topic"] == TP0.topic
    }
    assert set(range(12)) <= written


def _rebuild_against(previous):
    """A second consumer over the same cluster and table (new binary)."""
    from tests.integration.test_crash_recovery import restart_on

    return restart_on(previous)
