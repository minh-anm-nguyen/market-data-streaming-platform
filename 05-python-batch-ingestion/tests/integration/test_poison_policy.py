"""E2E-06 strict poison stop, and E2E-07 optional durable DLQ.

The shape of E2E-06 in the spec: ten good records, one invalid, ten more good
records, all in one partition. The oracle is that nothing is committed past the
poison record, no invalid row reaches the market table, and the run ends
INCOMPLETE. In particular the ten *valid* records after the poison one must not
be committed either -- committing them would skip the poison record silently.
"""

from __future__ import annotations

from dataclasses import replace

from p05_ingest.metrics import QUARANTINED_RECORDS, RECORDS_VALIDATED
from p05_ingest.model import TopicPartition
from p05_ingest.quarantine import QuarantinePolicy
from p05_ingest.testkit import TICKS_TOPIC, make_quote, make_tick
from tests.conftest import build_harness, make_config

TP0 = TopicPartition(TICKS_TOPIC, 0)


def ticks_only(dev_config, **extra):
    return make_config(
        dev_config, kafka={"topics": ["md.ticks.v1"]}, **extra
    )


def produce_sandwich(cluster, *, poison: bytes, good_each: int = 10) -> int:
    """good records, one poison, good records. Returns the poison offset."""
    for sequence in range(good_each):
        event = make_tick(sequence=sequence)
        cluster.produce(
            TICKS_TOPIC, 0, key=b"EURUSD", value=event.SerializeToString()
        )
    poison_offset = cluster.produce(TICKS_TOPIC, 0, key=b"EURUSD", value=poison)
    for sequence in range(good_each, good_each * 2):
        event = make_tick(sequence=sequence)
        cluster.produce(
            TICKS_TOPIC, 0, key=b"EURUSD", value=event.SerializeToString()
        )
    return poison_offset


# ---------------------------------------------------------------- E2E-06


def test_e2e_06_poison_stops_the_partition_and_commits_nothing_past_it(
    dev_config, clock
):
    cfg = ticks_only(dev_config, batch={"per_partition_max_rows": 4})
    harness = build_harness(cfg, clock=clock, partitions=1)
    poison_offset = produce_sandwich(harness.cluster, poison=b"\xff\xff\xff\xff\xff")

    status = harness.finish()

    # Nothing committed at or past the poison record.
    committed = harness.cluster.committed(cfg.kafka.group_id, TP0) or 0
    assert committed <= poison_offset, (
        f"committed next offset {committed} reached or passed the poison record "
        f"at offset {poison_offset}"
    )

    # No invalid row in the market table, and nothing from after the poison.
    written_offsets = {row["kafka_offset"] for row in harness.sink.rows}
    assert poison_offset not in written_offsets
    assert all(o < poison_offset for o in written_offsets), (
        "records after the poison one must not be written while it is unresolved"
    )

    assert not status.complete
    assert TP0 in harness.engine.blocked_partitions
    assert "fail-closed" in harness.engine.blocked_partitions[TP0]
    assert status.blocked_partitions == [str(TP0)]


def test_e2e_06_later_valid_records_are_not_committed_past_the_poison(
    dev_config, clock
):
    """The subtle half of I-03: a valid record after a hole stays uncommitted."""
    cfg = ticks_only(dev_config, batch={"per_partition_max_rows": 1})
    harness = build_harness(cfg, clock=clock, partitions=1)
    poison_offset = produce_sandwich(
        harness.cluster, poison=b"\xff\xff\xff\xff\xff", good_each=3
    )

    harness.finish()

    committed = harness.cluster.committed(cfg.kafka.group_id, TP0) or 0
    assert committed == poison_offset, (
        "the frontier should sit exactly at the poison record, having committed "
        "every good record before it and nothing after"
    )
    frontier = harness.engine.commits.proven_frontier_next(TP0)
    assert frontier == poison_offset


def test_e2e_06_run_is_incomplete_even_though_the_sink_is_healthy(dev_config, clock):
    """A healthy sink must not make a poisoned run look successful.

    One row per batch, so the records before the poison one really are written
    and acknowledged. The sink did its job perfectly and the run still fails,
    which is the point: success is not "the database accepted everything we
    sent", it is "every consumed record reached a terminal durable result".
    """
    cfg = ticks_only(dev_config, batch={"per_partition_max_rows": 1})
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_sandwich(harness.cluster, poison=b"\x08", good_each=2)

    status = harness.finish()

    assert harness.sink.row_count == 2, "the pre-poison records were written"
    assert all(a.behaviour.value == "ack" for a in harness.sink.attempts)
    assert not status.complete
    assert TP0 in harness.engine.blocked_partitions


def test_open_batch_is_released_when_a_partition_blocks(dev_config, clock):
    """A blocked partition must not strand a batch that can never be sent.

    The sealer skips blocked partitions, so an open batch left behind would
    keep the engine from ever reaching a quiescent state. Those rows were
    never sent, so a restart simply re-reads them.
    """
    cfg = ticks_only(dev_config, batch={"per_partition_max_rows": 100})
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_sandwich(harness.cluster, poison=b"\x08", good_each=3)

    status = harness.finish()

    assert harness.engine.batcher.open_partitions == frozenset()
    assert harness.engine.budget.reserved_for(TP0) == 0
    assert harness.sink.row_count == 0, "the buffered rows were never sent"
    assert not status.complete
    assert harness.cluster.committed(cfg.kafka.group_id, TP0) in (None, 0)


def test_other_partitions_keep_working_while_one_is_poisoned(dev_config, clock):
    """A poison record stops its own partition, not the whole topic."""
    cfg = ticks_only(dev_config, batch={"per_partition_max_rows": 2})
    harness = build_harness(cfg, clock=clock, partitions=2)
    harness.cluster.produce(TICKS_TOPIC, 0, key=b"EURUSD", value=b"\xff\xff\xff")
    for sequence in range(4):
        event = make_tick(sequence=sequence)
        harness.cluster.produce(
            TICKS_TOPIC, 1, key=b"EURUSD", value=event.SerializeToString()
        )

    status = harness.finish()

    tp1 = TopicPartition(TICKS_TOPIC, 1)
    assert harness.cluster.committed(cfg.kafka.group_id, tp1) == 4
    assert harness.cluster.committed(cfg.kafka.group_id, TP0) in (None, 0)
    assert not status.complete, "one blocked partition still fails the run"
    assert list(harness.engine.blocked_partitions) == [TP0]


def test_semantically_invalid_but_well_formed_record_also_stops(dev_config, clock):
    """A crossed quote parses perfectly and must still be refused."""
    cfg = make_config(dev_config, batch={"per_partition_max_rows": 4})
    harness = build_harness(cfg, clock=clock, partitions=1)
    crossed = make_quote(sequence=0, bid_ticks=109_400, ask_ticks=109_380)
    harness.cluster.produce(
        "md.quotes.v1", 0, key=b"EURUSD", value=crossed.SerializeToString()
    )

    status = harness.finish()

    assert harness.sink.row_count == 0
    assert not status.complete
    assert harness.metrics.counter(
        RECORDS_VALIDATED, kind="refused", result="QUOTE_CROSSED"
    ) == 1


def test_refusal_reason_is_recorded_per_code(dev_config, clock):
    """Metrics must distinguish *why* records were refused, not just that."""
    cfg = ticks_only(dev_config, batch={"per_partition_max_rows": 4})
    harness = build_harness(cfg, clock=clock, partitions=2)
    harness.cluster.produce(TICKS_TOPIC, 0, key=b"EURUSD", value=b"\xff\xff\xff")
    # A key that does not match its symbol, on a different partition.
    harness.cluster.produce(
        TICKS_TOPIC, 1, key=b"GBPUSD", value=make_tick().SerializeToString()
    )

    harness.finish()

    assert (
        harness.metrics.counter(
            RECORDS_VALIDATED, kind="refused", result="PROTO_DECODE_ERROR"
        )
        == 1
    )
    assert (
        harness.metrics.counter(
            RECORDS_VALIDATED, kind="refused", result="KEY_SYMBOL_MISMATCH"
        )
        == 1
    )


# ---------------------------------------------------------------- E2E-07


class AckingDlq:
    """A DLQ that acknowledges, standing in for acks=all."""

    def __init__(self) -> None:
        self.published = []

    def publish(self, record) -> None:
        self.published.append(record)

    def close(self) -> None:
        pass


class SilentDlq:
    """A DLQ that never acknowledges."""

    def publish(self, record) -> None:
        raise TimeoutError("no acknowledgement from the DLQ topic")

    def close(self) -> None:
        pass


def fault_lane_config(dev_config):
    """The isolated profile DLQ mode requires (spec I-12)."""
    return make_config(
        dev_config,
        project={"mode": "isolated_fault_test"},
        clickhouse={"table": "fault_events_raw"},
        quarantine={"mode": "durable_dlq", "topic": "md.ingest.dlq.v1"},
        batch={"per_partition_max_rows": 4},
    )


def test_e2e_07_acked_dlq_allows_the_frontier_to_pass(dev_config, clock):
    """Only a durably acknowledged quarantine permits a skip (I-07)."""
    cfg = fault_lane_config(dev_config)
    dlq = AckingDlq()
    harness = build_harness(cfg, clock=clock, partitions=1, dlq_publisher=dlq)
    poison_offset = produce_sandwich(
        harness.cluster, poison=b"\xff\xff\xff\xff", good_each=3
    )

    status = harness.finish()

    assert len(dlq.published) == 1
    quarantined = dlq.published[0]
    assert quarantined.offset == poison_offset
    assert quarantined.topic == TICKS_TOPIC
    assert quarantined.reason_code == "PROTO_DECODE_ERROR"

    # The run completes, the poison record is not in the table, and the good
    # records on both sides of it are.
    assert status.complete, status.reason
    written = {row["kafka_offset"] for row in harness.sink.rows}
    assert poison_offset not in written
    assert len(written) == 6
    assert harness.cluster.committed(cfg.kafka.group_id, TP0) == 7
    assert harness.metrics.counter(
        QUARANTINED_RECORDS, reason="PROTO_DECODE_ERROR"
    ) == 1


def test_e2e_07_unacked_dlq_keeps_the_record_blocked(dev_config, clock):
    """If the DLQ itself is down, the record must not be skipped."""
    cfg = fault_lane_config(dev_config)
    harness = build_harness(
        cfg, clock=clock, partitions=1, dlq_publisher=SilentDlq()
    )
    poison_offset = produce_sandwich(
        harness.cluster, poison=b"\xff\xff\xff\xff", good_each=3
    )

    status = harness.finish()

    assert not status.complete
    assert TP0 in harness.engine.blocked_partitions
    assert "not acknowledged" in harness.engine.blocked_partitions[TP0]
    committed = harness.cluster.committed(cfg.kafka.group_id, TP0) or 0
    assert committed <= poison_offset
    assert harness.metrics.counter_total(QUARANTINED_RECORDS) == 0


def test_e2e_07_quarantine_is_idempotent_across_a_replay(dev_config, clock):
    """Replaying the same poison record must not multiply DLQ entries."""
    cfg = fault_lane_config(dev_config)
    dlq = AckingDlq()
    harness = build_harness(cfg, clock=clock, partitions=1, dlq_publisher=dlq)
    harness.cluster.produce(TICKS_TOPIC, 0, key=b"EURUSD", value=b"\xff\xff\xff")
    harness.finish()

    first_id = dlq.published[0].quarantine_id

    # A second run over the same record, as after a crash before the commit.
    from tests.integration.test_crash_recovery import restart_on

    harness.cluster.set_committed(cfg.kafka.group_id, TP0, 0)
    second = restart_on(harness, dlq_publisher=dlq)
    second.finish()

    assert len(dlq.published) == 2, "it was genuinely published twice"
    assert dlq.published[1].quarantine_id == first_id, (
        "but under the same identity, so the DLQ stays logically idempotent"
    )


def test_market_lane_refuses_dlq_mode_at_startup(dev_config):
    """Spec I-12: a DLQ is only permitted in an explicitly isolated profile."""
    from p05_ingest.errors import ConfigError
    import pytest

    with pytest.raises(ConfigError, match="isolated profile"):
        make_config(
            dev_config, quarantine={"mode": "durable_dlq", "topic": "md.dlq.v1"}
        )


def test_fault_lane_must_use_a_separate_table(dev_config):
    from p05_ingest.errors import ConfigError
    import pytest

    with pytest.raises(ConfigError, match="market"):
        make_config(
            dev_config,
            project={"mode": "isolated_fault_test"},
            quarantine={"mode": "durable_dlq", "topic": "md.dlq.v1"},
        )
