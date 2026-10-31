"""E2E-08 sink backpressure.

PASS oracle (spec 9.5): memory stays bounded, Kafka lag rises, the operator
gets a signal, and no false throughput claim is produced. The last clause is
the important one: the engine must not report success while a backlog grows.
"""

from __future__ import annotations

from p05_ingest.metrics import BUFFER_BYTES, KAFKA_LAG_OFFSETS
from p05_ingest.model import TopicPartition
from p05_ingest.testkit import TICKS_TOPIC, Behaviour, make_tick
from tests.conftest import build_harness, make_config

TP0 = TopicPartition(TICKS_TOPIC, 0)
TP1 = TopicPartition(TICKS_TOPIC, 1)


def produce_ticks(cluster, count: int, *, partition: int = 0, start: int = 0) -> None:
    for i in range(count):
        event = make_tick(sequence=start + i)
        cluster.produce(
            TICKS_TOPIC, partition, key=b"EURUSD", value=event.SerializeToString()
        )


def test_e2e_08_buffer_stays_within_the_budget_while_the_sink_stalls(
    dev_config, clock
):
    """A stalled sink must not translate into unbounded buffering (I-05)."""
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={
            "per_partition_max_rows": 50,
            "per_partition_max_serialized_bytes": 32_768,
            "app_buffer_budget_bytes": 65_536,
            "pause_high_watermark": 0.8,
            "resume_low_watermark": 0.5,
        },
        retry={"max_attempts": 1, "overall_deadline_s": 1},
    )
    harness = build_harness(
        cfg, clock=clock, partitions=2, sink_default=Behaviour.REFUSE_BEFORE_WRITE
    )
    produce_ticks(harness.cluster, 2000, partition=0)
    produce_ticks(harness.cluster, 2000, partition=1, start=10_000)

    budget = harness.engine.budget
    for _ in range(400):
        harness.engine.step()
        assert budget.reserved_bytes <= budget.budget_bytes, (
            "the accounted buffer exceeded its budget"
        )
        harness.tick_timer()

    harness.engine.shutdown()
    assert harness.metrics.gauge(BUFFER_BYTES, component="app") <= budget.budget_bytes


def test_e2e_08_slow_sink_pauses_rather_than_dropping(dev_config, clock):
    """Backpressure is expressed as not-fetching, never as discarding."""
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={
            "per_partition_max_rows": 10,
            "per_partition_max_serialized_bytes": 8_192,
            "app_buffer_budget_bytes": 32_768,
        },
        retry={"max_attempts": 1, "overall_deadline_s": 1},
    )
    harness = build_harness(
        cfg,
        clock=clock,
        partitions=1,
        sink_script=(Behaviour.REFUSE_BEFORE_WRITE, Behaviour.REFUSE_BEFORE_WRITE),
        sink_default=Behaviour.ACK,
    )
    produce_ticks(harness.cluster, 500)

    paused_at_least_once = False
    for _ in range(200):
        report = harness.engine.step()
        if report.paused or harness.consumer.paused:
            paused_at_least_once = True
        harness.tick_timer()
        if harness.engine.blocked_partitions:
            break

    assert paused_at_least_once or harness.engine.blocked_partitions, (
        "a sink that refuses work must cause a pause or a block, not silent loss"
    )
    harness.engine.shutdown()

    lag = harness.metrics.gauge(KAFKA_LAG_OFFSETS, topic=TICKS_TOPIC, partition="0")
    assert lag > 0, "the backlog must be reported"
    assert harness.sink.row_count <= 500


def test_e2e_08_no_false_success_while_a_backlog_remains(dev_config, clock):
    """The run must not report COMPLETE with records left unfinished."""
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={"per_partition_max_rows": 5},
        retry={"max_attempts": 1, "overall_deadline_s": 1},
    )
    harness = build_harness(
        cfg, clock=clock, partitions=1, sink_default=Behaviour.REFUSE_BEFORE_WRITE
    )
    produce_ticks(harness.cluster, 100)

    status = harness.finish()

    assert not status.complete
    assert harness.sink.row_count == 0
    assert harness.cluster.committed(cfg.kafka.group_id, TP0) in (None, 0)
    # The backlog is still in Kafka: nothing was lost, it simply was not done.
    assert harness.cluster.log(TP0).high_watermark == 100


def test_pause_and_resume_cycle_under_a_recovering_sink(dev_config, clock):
    """When the sink recovers, paused partitions must resume and finish."""
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={
            "per_partition_max_rows": 4,
            "per_partition_max_serialized_bytes": 8_192,
            "app_buffer_budget_bytes": 24_576,
        },
        retry={"max_attempts": 3, "overall_deadline_s": 20},
    )
    harness = build_harness(
        cfg,
        clock=clock,
        partitions=1,
        sink_script=(Behaviour.REFUSE_BEFORE_WRITE, Behaviour.ACK),
        sink_default=Behaviour.ACK,
    )
    produce_ticks(harness.cluster, 40)

    status = harness.finish()

    assert status.complete, status.reason
    assert len(harness.logical_identities()) == 40
    assert harness.consumer.paused == frozenset(), "everything resumed"


def test_a_hot_partition_does_not_starve_a_quiet_one(dev_config, clock):
    """Spec group M: fairness across partitions under load."""
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={"per_partition_max_rows": 10},
    )
    harness = build_harness(cfg, clock=clock, partitions=2)
    produce_ticks(harness.cluster, 400, partition=0)
    produce_ticks(harness.cluster, 10, partition=1, start=100_000)

    status = harness.finish()

    assert status.complete, status.reason
    assert harness.cluster.committed(cfg.kafka.group_id, TP1) == 10, (
        "the quiet partition completed despite the hot one"
    )
    assert harness.cluster.committed(cfg.kafka.group_id, TP0) == 400


def test_global_inflight_cap_is_respected(dev_config, clock):
    cfg = make_config(
        dev_config,
        batch={"per_partition_max_rows": 2, "max_inflight_batches_global": 2},
    )
    harness = build_harness(cfg, clock=clock, partitions=4)
    for partition in range(4):
        produce_ticks(
            harness.cluster, 20, partition=partition, start=partition * 1000
        )

    for _ in range(200):
        report = harness.engine.step()
        assert report.batches_submitted <= 2, "more batches submitted than slots"
        harness.tick_timer()
    harness.engine.shutdown()


def test_fetching_stops_when_a_worst_case_row_would_not_fit(dev_config):
    """Spec 5.4: the cap is never raised to admit a record.

    The budget is set so small that the worst-case row estimate exceeds it, so
    the pre-poll headroom check refuses to fetch at all. The engine stalls
    honestly instead of buffering something it cannot bound.
    """
    cfg = make_config(
        dev_config,
        batch={
            "per_partition_max_serialized_bytes": 2048,
            "app_buffer_budget_bytes": 2048,
        },
    )
    harness = build_harness(cfg, partitions=1)
    produce_ticks(harness.cluster, 5)

    report = harness.engine.step()

    assert report.fetch_withheld_for_budget
    assert report.records_fetched == 0
    assert harness.engine.budget.reserved_bytes == 0
