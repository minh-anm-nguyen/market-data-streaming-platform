"""E2E-02 crash after ACK before commit, and E2E-05 Kafka commit timeout.

Both scenarios restart the consumer against the *same* cluster, so the second
run resumes from whatever the broker actually holds. That is the only honest
way to test recovery: the new engine gets no information from the old one.
"""

from __future__ import annotations

from p05_ingest.metrics import KAFKA_COMMIT_ATTEMPTS
from p05_ingest.model import TopicPartition
from p05_ingest.testkit import (
    TICKS_TOPIC,
    Behaviour,
    CommitFault,
    FakeClickHouse,
    FakeClock,
    FakeConsumer,
    make_tick,
)
from tests.conftest import Harness, build_harness, make_config

TP0 = TopicPartition(TICKS_TOPIC, 0)


def ticks_only(dev_config, **batch):
    return make_config(dev_config, kafka={"topics": ["md.ticks.v1"]}, batch=batch)


def produce_ticks(cluster, count: int) -> None:
    for sequence in range(count):
        event = make_tick(sequence=sequence)
        cluster.produce(
            TICKS_TOPIC, 0, key=b"EURUSD", value=event.SerializeToString()
        )


def restart_on(
    harness: Harness,
    *,
    sink: FakeClickHouse | None = None,
    dlq_publisher=None,
) -> Harness:
    """Build a fresh engine over the same cluster and the same table.

    Everything in-process is discarded: ledgers, batches, epochs. Only the
    Kafka committed offsets and the ClickHouse rows survive, which is exactly
    what survives a real crash.
    """
    from p05_ingest.contract_validator import ContractValidator
    from p05_ingest.engine import IngestEngine
    from p05_ingest.metrics import MetricsRegistry
    from p05_ingest.quarantine import QuarantinePolicy
    from p05_ingest.retry_policy import RetryPolicy
    from p05_ingest.sink_worker import InlineSinkExecutor

    cfg = harness.cfg
    clock = FakeClock(now=harness.clock.now + 60.0)
    sink = sink if sink is not None else harness.sink
    metrics = MetricsRegistry()
    consumer = FakeConsumer(
        harness.cluster,
        group_id=cfg.kafka.group_id,
        auto_offset_reset=cfg.kafka.auto_offset_reset,
    )
    executor = InlineSinkExecutor(
        sink=sink,
        policy=RetryPolicy(cfg.retry),
        metrics=metrics,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    engine = IngestEngine(
        cfg,
        source=consumer,
        executor=executor,
        metrics=metrics,
        monotonic=clock.monotonic,
        quarantine=QuarantinePolicy(
            cfg.quarantine,
            ingest_run_id=cfg.project.run_id,
            publisher=dlq_publisher,
        ),
        validator=ContractValidator(cfg.contracts, cfg.project),
    )
    engine.start()
    return Harness(
        cfg=cfg,
        clock=clock,
        cluster=harness.cluster,
        consumer=consumer,
        sink=sink,
        metrics=metrics,
        engine=engine,
        executor=executor,
    )


# ---------------------------------------------------------------- E2E-02


def test_e2e_02_crash_after_ack_before_commit_replays_without_loss(
    dev_config, clock
):
    """The rows are in ClickHouse, the offset was never committed.

    PASS oracle (spec 9.5): replay happens, physical rows may duplicate, the
    logical set is unique with no conflicts, and nothing is missing.
    """
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(cfg, clock=clock, partitions=1, dedup_window=0)
    produce_ticks(harness.cluster, 8)

    # Drive until the first batch is acknowledged, then abandon the process
    # before the commit can happen -- the state of a crash at CRASH_04.
    for _ in range(50):
        harness.engine.step()
        if harness.sink.row_count >= 4:
            break
        harness.tick_timer()
    assert harness.sink.row_count == 4, "the first batch landed"

    # Simulate the crash: throw the engine away without committing. The
    # committed offset the broker holds is whatever it was before.
    committed_before = harness.cluster.committed(cfg.kafka.group_id, TP0)

    restarted = restart_on(harness)
    status = restarted.finish()

    assert status.complete, status.reason

    # All eight events are present logically.
    assert len(restarted.logical_identities()) == 8

    # The first four were re-read and written again, because the commit never
    # happened and the de-duplication window is disabled here.
    counts = restarted.sink.physical_counts_by_identity()
    duplicated = {k: v for k, v in counts.items() if v > 1}
    assert duplicated, "a replay after an uncommitted ACK must be able to duplicate"
    assert all(v == 2 for v in duplicated.values())

    # Duplicates, never conflicts: the same event has one semantic digest.
    digests = restarted.sink.digests_by_identity()
    assert all(len(v) == 1 for v in digests.values())

    assert restarted.committed()[TP0] == 8
    assert committed_before in (None, 0)


def test_e2e_02_no_event_is_lost_at_any_crash_point(dev_config, clock):
    """Crash after every possible step count; nothing may go missing.

    This is the loop that would catch a commit running ahead of the sink: at
    whatever point the first engine is abandoned, the restarted one must still
    end up with every event.
    """
    cfg = ticks_only(dev_config, per_partition_max_rows=3)
    total = 9

    for crash_after in range(0, 30):
        harness = build_harness(
            cfg, clock=FakeClock(), partitions=1, dedup_window=1000
        )
        produce_ticks(harness.cluster, total)

        for _ in range(crash_after):
            harness.engine.step()
            harness.tick_timer()

        committed = harness.cluster.committed(cfg.kafka.group_id, TP0) or 0
        written = {
            row["kafka_offset"]
            for row in harness.sink.rows
            if row["kafka_topic"] == TP0.topic
        }
        # I-01 as observed after an abrupt stop: everything below the committed
        # offset must already be in the table.
        unproven = {o for o in range(committed) if o not in written}
        assert not unproven, (
            f"crash_after={crash_after}: offsets {sorted(unproven)} were "
            "committed but never written"
        )

        restarted = restart_on(harness)
        status = restarted.finish()
        assert status.complete, f"crash_after={crash_after}: {status.reason}"
        assert len(restarted.logical_identities()) == total, (
            f"crash_after={crash_after}: lost events"
        )


# ---------------------------------------------------------------- E2E-05


def test_e2e_05_commit_response_lost_is_reconciled_on_restart(dev_config, clock):
    """The commit was applied; the response never arrived.

    The engine must treat the position as unknown rather than as committed,
    and a restart must re-read the authoritative broker position.
    """
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(cfg, clock=clock, partitions=1, dedup_window=1000)
    produce_ticks(harness.cluster, 4)

    harness.consumer.commit_fault = CommitFault.LOST_RESPONSE
    harness.consumer.commit_fault_remaining = 1

    for _ in range(30):
        report = harness.engine.step()
        if report.commits_unknown:
            break
        harness.tick_timer()
    else:
        raise AssertionError("the lost-response commit never happened")

    # The broker did apply it, even though the engine does not know that.
    assert harness.cluster.committed(cfg.kafka.group_id, TP0) == 4
    assert harness.metrics.counter(KAFKA_COMMIT_ATTEMPTS, status="unknown") == 1
    # The in-process view stayed honest: it does not claim the commit.
    assert harness.engine.commits.committed_next(TP0) == 0

    status = harness.finish()
    assert status.complete, status.reason

    restarted = restart_on(harness)
    restarted.finish()
    assert len(restarted.logical_identities()) == 4
    assert restarted.sink.row_count == 4, "nothing was re-read, nothing was lost"


def test_e2e_05_rejected_commit_is_retried_and_nothing_is_skipped(dev_config, clock):
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_ticks(harness.cluster, 8)

    harness.consumer.commit_fault = CommitFault.REJECT
    harness.consumer.commit_fault_remaining = 2

    status = harness.finish()

    assert status.complete, status.reason
    assert harness.metrics.counter(KAFKA_COMMIT_ATTEMPTS, status="unknown") == 2
    assert harness.metrics.counter(KAFKA_COMMIT_ATTEMPTS, status="confirmed") >= 1
    assert harness.committed()[TP0] == 8
    assert len(harness.logical_identities()) == 8


def test_commit_is_never_claimed_when_the_broker_is_unreachable(dev_config, clock):
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_ticks(harness.cluster, 4)

    harness.consumer.commit_fault = CommitFault.UNREACHABLE
    harness.consumer.commit_fault_remaining = 1000

    harness.drain()
    harness.engine.shutdown()

    assert harness.cluster.committed(cfg.kafka.group_id, TP0) is None
    assert harness.engine.commits.committed_next(TP0) == 0
    assert harness.sink.row_count == 4, "the rows are written; only the commit failed"

    # A restart re-reads everything, which is correct: duplicates beat loss.
    restarted = restart_on(harness, sink=FakeClickHouse())
    restarted.finish()
    assert len(restarted.logical_identities()) == 4


def test_restart_after_a_clean_run_reads_nothing_again(dev_config, clock):
    """The checkpoint is honoured: a clean restart is not a full re-read.

    Spec I-01 note: starting the frontier from zero would re-read the whole
    partition and imply every earlier commit was wrong.
    """
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_ticks(harness.cluster, 8)
    harness.finish()
    assert harness.committed()[TP0] == 8
    first_rows = harness.sink.row_count

    restarted = restart_on(harness)
    status = restarted.finish()

    assert status.complete, status.reason
    assert restarted.sink.row_count == first_rows, "nothing was re-read"
    assert restarted.engine.commits.proven_frontier_next(TP0) == 8
