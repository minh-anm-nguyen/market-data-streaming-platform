"""E2E-09 retention exhaustion, and offset-gap handling.

PASS oracle (spec 9.5): the out-of-range condition is detected, the verdict is
NO-GO, and the exact missing offsets are reportable. There is deliberately no
recovery path here: once Kafka has deleted the data and no replay source
exists, the only honest action is to stop and say so (spec 5.2
``RETENTION_LOST``, I-11).
"""

from __future__ import annotations

import pytest

from p05_ingest.errors import RetentionLostError
from p05_ingest.model import TopicPartition
from p05_ingest.preflight import check_retention_headroom
from p05_ingest.testkit import TICKS_TOPIC, Behaviour, make_tick
from tests.conftest import build_harness, make_config

TP0 = TopicPartition(TICKS_TOPIC, 0)


def produce_ticks(cluster, count: int, *, start: int = 0) -> None:
    for i in range(count):
        event = make_tick(sequence=start + i)
        cluster.produce(
            TICKS_TOPIC, 0, key=b"EURUSD", value=event.SerializeToString()
        )


def ticks_only(dev_config, **batch):
    return make_config(dev_config, kafka={"topics": ["md.ticks.v1"]}, batch=batch)


def test_e2e_09_expired_offsets_stop_the_run_as_no_go(dev_config, clock):
    """The sink was down long enough for retention to delete what we needed."""
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(
        cfg, clock=clock, partitions=1, sink_default=Behaviour.REFUSE_BEFORE_WRITE
    )
    produce_ticks(harness.cluster, 20)

    # The consumer never got past offset 0; retention now removes 0..9.
    harness.cluster.expire_to(TP0, 10)

    status = harness.finish()

    assert not status.complete
    assert harness.engine.fatal_reason is not None
    assert "RETENTION_LOST" in harness.engine.fatal_reason
    assert harness.cluster.committed(cfg.kafka.group_id, TP0) in (None, 0)


def test_e2e_09_missing_offsets_are_reportable(dev_config, clock):
    """The operator needs the exact range that was lost, not just an alarm."""
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(
        cfg, clock=clock, partitions=1, sink_default=Behaviour.REFUSE_BEFORE_WRITE
    )
    produce_ticks(harness.cluster, 20)
    harness.cluster.expire_to(TP0, 10)
    harness.finish()

    log = harness.cluster.log(TP0)
    committed = harness.cluster.committed(cfg.kafka.group_id, TP0) or 0
    lost = list(range(committed, log.log_start_offset))
    assert lost == list(range(0, 10))
    assert harness.engine.fatal_reason
    assert str(TP0) in harness.engine.fatal_reason


def test_retention_loss_is_never_auto_reset(dev_config, clock):
    """Spec I-11: silently jumping to latest would skip real data."""
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_ticks(harness.cluster, 10)
    harness.cluster.expire_to(TP0, 5)
    harness.consumer.seek(TP0, 0)

    with pytest.raises(RetentionLostError):
        harness.consumer.poll(0.01)


def test_engine_turns_retention_loss_into_a_fatal_stop(dev_config, clock):
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_ticks(harness.cluster, 10)
    harness.consumer.poll_raises_retention_lost.add(TP0)

    report = harness.engine.step()

    assert report.records_fetched == 0
    assert harness.engine.fatal_reason is not None
    assert "RETENTION_LOST" in harness.engine.fatal_reason

    status = harness.engine.shutdown()
    assert not status.complete
    assert "RETENTION_LOST" in status.reason


def test_a_fatal_stop_prevents_any_further_fetching(dev_config, clock):
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_ticks(harness.cluster, 10)
    harness.consumer.poll_raises_retention_lost.add(TP0)
    harness.engine.step()

    harness.consumer.poll_raises_retention_lost.clear()
    report = harness.engine.step()
    assert report.records_fetched == 0, "a fatal run must not quietly resume"


def test_retention_within_headroom_is_harmless(dev_config, clock):
    """Expiring data the group has already committed past is fine."""
    cfg = ticks_only(dev_config, per_partition_max_rows=5)
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_ticks(harness.cluster, 10)
    harness.drain()
    assert harness.cluster.committed(cfg.kafka.group_id, TP0) == 10

    harness.cluster.expire_to(TP0, 10)
    status = harness.engine.shutdown()

    assert status.complete, status.reason
    assert harness.engine.fatal_reason is None


def test_preflight_reports_the_retention_margin(dev_config, clock):
    cfg = ticks_only(dev_config, per_partition_max_rows=5)
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_ticks(harness.cluster, 10)
    harness.drain()

    result = check_retention_headroom(cfg, harness.consumer)
    assert result.ok
    assert "margin above log start" in result.detail


def test_preflight_fails_when_the_committed_offset_is_already_gone(
    dev_config, clock
):
    cfg = ticks_only(dev_config, per_partition_max_rows=5)
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_ticks(harness.cluster, 10)
    harness.cluster.set_committed(cfg.kafka.group_id, TP0, 2)
    harness.cluster.expire_to(TP0, 5)

    result = check_retention_headroom(cfg, harness.consumer)
    assert not result.ok
    assert "below log start offset" in result.detail


def test_preflight_warns_rather_than_passing_without_a_broker(dev_config):
    """An unverified claim is a WARN, never a PASS (spec 0.2)."""
    result = check_retention_headroom(dev_config, None)
    assert not result.ok
    assert not result.required
    assert result.status == "WARN"
    assert "not evaluated" in result.detail


# -------------------------------------------------------------- offset gaps


def test_control_records_create_offset_gaps_that_are_not_holes(dev_config, clock):
    """Spec 4.3: a skipped integer offset is not a missing record.

    Transaction markers consume offset space without being delivered. A
    frontier that insisted on contiguous integers would stall here forever.
    """
    cfg = ticks_only(dev_config, per_partition_max_rows=2)
    harness = build_harness(cfg, clock=clock, partitions=1)

    produce_ticks(harness.cluster, 2, start=0)
    harness.cluster.produce_control_record(TICKS_TOPIC, 0)
    harness.cluster.produce_control_record(TICKS_TOPIC, 0)
    produce_ticks(harness.cluster, 2, start=100)

    status = harness.finish()

    assert status.complete, status.reason
    assert len(harness.logical_identities()) == 4
    # Offsets 0,1,4,5 were delivered; 2 and 3 were control records.
    delivered = sorted(row["kafka_offset"] for row in harness.sink.rows)
    assert delivered == [0, 1, 4, 5]
    assert harness.cluster.committed(cfg.kafka.group_id, TP0) == 6


def test_a_gap_at_the_start_of_the_log_is_handled(dev_config, clock):
    cfg = ticks_only(dev_config, per_partition_max_rows=2)
    harness = build_harness(cfg, clock=clock, partitions=1, assign_on_subscribe=False)
    harness.cluster.produce_control_record(TICKS_TOPIC, 0)
    produce_ticks(harness.cluster, 2)
    harness.consumer.assign([TP0])

    status = harness.finish()
    assert status.complete, status.reason
    assert harness.cluster.committed(cfg.kafka.group_id, TP0) == 3
