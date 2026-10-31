"""The broker state machine and the producer's three-state delivery ledger."""

from __future__ import annotations

import pytest

from p07.contracts import QUOTE_TOPIC, TICK_TOPIC, EventKind, SemanticCore, partition_for_symbol
from p07.dataplane.codec import DEFAULT_CODEC, looks_like_mdp1
from p07.dataplane.kafka_fake import (
    AckUnknown,
    BrokerUnavailable,
    FakeKafka,
    KafkaError,
    NotEnoughReplicas,
    OffsetExpired,
)
from p07.dataplane.producer import Delivery, Producer, QueueFull


def core(sequence: int = 1, *, kind: EventKind = EventKind.TICK, symbol: str = "EURUSD") -> SemanticCore:
    return SemanticCore(
        dataset_id="ds",
        symbol=symbol,
        sequence=sequence,
        event_time_ns=1000 + sequence,
        price_scale=100000,
        event_kind=kind,
        source_schema_version="sim.v1",
        bid_ticks=108000 if kind is EventKind.QUOTE else None,
        ask_ticks=108003 if kind is EventKind.QUOTE else None,
        price_ticks=108001 if kind is EventKind.TICK else None,
    )


def produce(kafka: FakeKafka, topic: str = TICK_TOPIC, partition: int = 0, **kwargs: object) -> int:
    return kafka.produce(topic, partition, key=b"EURUSD", value=b"payload", **kwargs)  # type: ignore[arg-type]


# -- offsets and generations ---------------------------------------------


def test_offsets_are_assigned_per_partition(kafka: FakeKafka) -> None:
    assert produce(kafka, partition=0) == 0
    assert produce(kafka, partition=0) == 1
    assert produce(kafka, partition=1) == 0  # independent sequence
    assert kafka.high_watermark(TICK_TOPIC, 0) == 2
    assert kafka.high_watermark(TICK_TOPIC, 1) == 1


@pytest.mark.whatif("P07-G08-04")
def test_the_two_topics_have_independent_offset_spaces(kafka: FakeKafka) -> None:
    """There is no global order across ticks and quotes to lean on."""
    produce(kafka, TICK_TOPIC, 0)
    produce(kafka, QUOTE_TOPIC, 0)
    assert kafka.high_watermark(TICK_TOPIC, 0) == kafka.high_watermark(QUOTE_TOPIC, 0) == 1


@pytest.mark.whatif("P07-G10-04")
def test_a_new_generation_empties_the_log_and_restarts_offsets(kafka: FakeKafka) -> None:
    produce(kafka)
    first = kafka.topic_generation
    second = kafka.new_generation()
    assert second != first
    assert kafka.high_watermark(TICK_TOPIC, 0) == 0
    assert produce(kafka) == 0  # the offset means something else now


def test_committed_offsets_do_not_survive_a_new_generation(kafka: FakeKafka) -> None:
    produce(kafka)
    kafka.commit("g", TICK_TOPIC, 0, 1, epoch=0)
    assert kafka.committed("g", TICK_TOPIC, 0) == 1
    kafka.new_generation()
    assert kafka.committed("g", TICK_TOPIC, 0) is None


@pytest.mark.whatif("P07-G08-06")
def test_auto_topic_creation_is_off_by_default(kafka: FakeKafka) -> None:
    """A typo'd topic name must be an error, not a new empty topic."""
    with pytest.raises(KafkaError, match="UNKNOWN_TOPIC"):
        kafka.produce("md.typo.v1", 0, key=b"k", value=b"v")
    permissive = FakeKafka(partitions=2, allow_auto_create=True)
    assert permissive.produce("md.extra.v1", 0, key=b"k", value=b"v") == 0


def test_an_unknown_partition_is_an_error(kafka: FakeKafka) -> None:
    with pytest.raises(KafkaError, match="UNKNOWN_PARTITION"):
        produce(kafka, partition=99)


# -- scanning -------------------------------------------------------------


def test_scan_returns_exactly_the_requested_range(kafka: FakeKafka) -> None:
    for _ in range(5):
        produce(kafka)
    records = list(kafka.scan(TICK_TOPIC, 0, 1, 4))
    assert [record.offset for record in records] == [1, 2, 3]


@pytest.mark.whatif("P07-G10-01")
def test_scanning_an_expired_range_refuses_instead_of_shortening(kafka: FakeKafka) -> None:
    """A short read is how "nothing is missing" gets claimed about a deleted log."""
    for _ in range(5):
        produce(kafka)
    removed = kafka.expire_to(TICK_TOPIC, 0, 3)
    assert removed == 3
    assert kafka.log_start_offset(TICK_TOPIC, 0) == 3
    with pytest.raises(OffsetExpired):
        list(kafka.scan(TICK_TOPIC, 0, 0, 5))
    # The surviving range is still readable.
    assert [record.offset for record in kafka.scan(TICK_TOPIC, 0, 3, 5)] == [3, 4]


def test_scanning_past_the_high_watermark_is_a_hole(kafka: FakeKafka) -> None:
    produce(kafka)
    with pytest.raises(KafkaError, match="CUT_OFFSET_HOLE"):
        list(kafka.scan(TICK_TOPIC, 0, 0, 5))


def test_retention_cannot_delete_past_the_end(kafka: FakeKafka) -> None:
    produce(kafka)
    assert kafka.expire_to(TICK_TOPIC, 0, 99) == 1
    assert kafka.log_start_offset(TICK_TOPIC, 0) == 1
    assert kafka.expire_to(TICK_TOPIC, 0, 0) == 0


# -- availability and durability ------------------------------------------


@pytest.mark.whatif("P07-G09-03")
def test_a_write_is_refused_when_in_sync_replicas_drop_below_min(kafka: FakeKafka) -> None:
    """Never a weakened ACK: design runbook RB-01."""
    kafka.stop_broker(0)
    kafka.stop_broker(1)
    with pytest.raises(NotEnoughReplicas, match="KAFKA_MIN_ISR_UNAVAILABLE"):
        produce(kafka)
    kafka.start_broker(1)
    assert produce(kafka) == 0


def test_losing_every_broker_makes_the_cluster_unavailable(kafka: FakeKafka) -> None:
    for broker in list(kafka.live_brokers):
        kafka.stop_broker(broker)
    with pytest.raises(BrokerUnavailable):
        produce(kafka)
    with pytest.raises(BrokerUnavailable):
        kafka.fetch(TICK_TOPIC, 0, 0, 10)


@pytest.mark.whatif("P07-G09-01")
def test_leadership_moves_only_to_a_live_in_sync_replica(kafka: FakeKafka) -> None:
    leader = kafka._topics[TICK_TOPIC][0].leader  # noqa: SLF001 - inspecting topology
    kafka.stop_broker(leader)
    assert kafka.leader_available(TICK_TOPIC, 0) is False
    new_leader = kafka.elect_new_leader(TICK_TOPIC, 0)
    assert new_leader is not None and new_leader in kafka.live_brokers
    for broker in list(kafka.live_brokers):
        kafka.stop_broker(broker)
    assert kafka.elect_new_leader(TICK_TOPIC, 0) is None


def test_an_rf1_lab_reports_that_it_cannot_prove_ha() -> None:
    """A restart on RF=1 is not evidence of high availability (design 0.2)."""
    single = FakeKafka(brokers=1, replication_factor=1, min_insync_replicas=1, partitions=2)
    assert single.ha_capable is False
    assert FakeKafka(partitions=2).ha_capable is True
    assert single.describe()["ha_capable"] is False


# -- ambiguous ACK and idempotence ----------------------------------------


@pytest.mark.whatif("P07-G07-02")
def test_a_lost_ack_still_appended_the_record(kafka: FakeKafka) -> None:
    kafka.ack_loss_once.add((TICK_TOPIC, 0))
    with pytest.raises(AckUnknown) as exc:
        produce(kafka)
    assert exc.value.offset == 0
    # The lab knows; the producer does not. The record is really there.
    assert kafka.high_watermark(TICK_TOPIC, 0) == 1


@pytest.mark.whatif("P07-G15-02")
def test_an_idempotent_retry_does_not_append_twice(kafka: FakeKafka) -> None:
    first = produce(kafka, producer_id="p1", producer_seq=0)
    again = produce(kafka, producer_id="p1", producer_seq=0)
    assert first == again
    assert kafka.high_watermark(TICK_TOPIC, 0) == 1


@pytest.mark.whatif("P07-G15-03")
def test_without_dedupe_a_retry_really_duplicates() -> None:
    kafka = FakeKafka(partitions=2, idempotent_dedupe=False)
    produce(kafka, producer_id="p1", producer_seq=0)
    produce(kafka, producer_id="p1", producer_seq=0)
    assert kafka.high_watermark(TICK_TOPIC, 0) == 2


# -- consumer group state --------------------------------------------------


def test_an_uncommitted_group_returns_none_not_zero(kafka: FakeKafka) -> None:
    """``None`` must never be silently read as "start from the beginning/latest"."""
    assert kafka.committed("fresh", TICK_TOPIC, 0) is None


@pytest.mark.whatif("P07-G11-02")
def test_a_stale_epoch_cannot_commit(kafka: FakeKafka) -> None:
    produce(kafka)
    kafka.commit("g", TICK_TOPIC, 0, 1, epoch=0)
    new_epoch = kafka.bump_group_epoch("g")
    assert new_epoch == 1
    with pytest.raises(KafkaError, match="COMMIT_BY_STALE_OWNER"):
        kafka.commit("g", TICK_TOPIC, 0, 2, epoch=0)
    assert kafka.committed("g", TICK_TOPIC, 0) == 1
    kafka.commit("g", TICK_TOPIC, 0, 2, epoch=new_epoch)
    assert kafka.committed("g", TICK_TOPIC, 0) == 2


# -- producer -------------------------------------------------------------


def test_routing_sends_each_kind_to_its_own_topic(kafka: FakeKafka) -> None:
    producer = Producer(kafka)
    producer.enqueue(core(1, kind=EventKind.TICK))
    producer.enqueue(core(2, kind=EventKind.QUOTE))
    assert producer.flush() == {"delivered": 2, "failed": 0, "unknown": 0}
    topics = {entry.topic for entry in producer.ledger}
    assert topics == {TICK_TOPIC, QUOTE_TOPIC}


@pytest.mark.whatif("P07-G08-02")
def test_the_key_is_the_symbol_and_affinity_is_stable(kafka: FakeKafka) -> None:
    producer = Producer(kafka)
    for sequence in range(1, 6):
        producer.enqueue(core(sequence))
    producer.flush()
    partitions = {entry.partition for entry in producer.ledger}
    assert len(partitions) == 1
    assert partitions == {partition_for_symbol("EURUSD", kafka.partition_count(TICK_TOPIC))}
    records = list(kafka.scan(TICK_TOPIC, partitions.pop(), 0, 5))
    assert {record.key for record in records} == {b"EURUSD"}


@pytest.mark.whatif("P07-G03-12")
def test_the_record_value_is_raw_protobuf_with_no_mdp1_prefix(kafka: FakeKafka) -> None:
    producer = Producer(kafka)
    producer.enqueue(core(1))
    producer.flush()
    entry = producer.ledger[0]
    record = next(iter(kafka.scan(entry.topic, entry.partition, entry.offset, entry.offset + 1)))
    assert not looks_like_mdp1(record.value)
    assert DEFAULT_CODEC.decode(record.value) == core(1)
    assert record.headers["protobuf_package"] == DEFAULT_CODEC.package


def test_an_oversize_value_is_refused_at_enqueue(kafka: FakeKafka) -> None:
    producer = Producer(kafka)
    huge = core(1)._replace(symbol="E" * 4200)
    with pytest.raises(ValueError, match="contract cap"):
        producer.enqueue(huge)
    assert producer.counters.rejected_oversize == 1


@pytest.mark.whatif("P07-G24-01")
def test_a_full_queue_raises_instead_of_dropping(kafka: FakeKafka) -> None:
    """Design 8: pause or fail, never a silent drop."""
    producer = Producer(kafka, max_queue_events=2)
    producer.enqueue(core(1))
    producer.enqueue(core(2))
    with pytest.raises(QueueFull):
        producer.enqueue(core(3))
    assert producer.queue_depth == 2
    assert producer.counters.peak_queue_depth == 2


@pytest.mark.whatif("P07-G07-03")
def test_an_unresolvable_ack_is_reported_as_unknown_not_failed(kafka: FakeKafka) -> None:
    """Design 3.3: UNKNOWN is carried forward; only a scan can resolve it."""
    producer = Producer(kafka, max_retries=1)
    kafka.ack_loss_once.add((TICK_TOPIC, partition_for_symbol("EURUSD", kafka.partition_count(TICK_TOPIC))))
    producer.enqueue(core(1))
    result = producer.flush()
    assert result["unknown"] == 1
    assert producer.ledger[0].status is Delivery.UNKNOWN
    assert producer.unresolved_identities() == (core(1).identity,)
    assert producer.ledger_receipt()["unknown_identities"] == [list(core(1).identity)]


def test_a_pre_append_refusal_is_failed_not_unknown(kafka: FakeKafka) -> None:
    """minISR refused before appending, so the record is definitely not there."""
    producer = Producer(kafka, max_retries=2)
    kafka.stop_broker(0)
    kafka.stop_broker(1)
    producer.enqueue(core(1))
    result = producer.flush()
    assert result["failed"] == 1
    assert producer.ledger[0].status is Delivery.FAILED
    assert producer.ledger[0].reason == "KAFKA_MIN_ISR_UNAVAILABLE"
    assert producer.unresolved_identities() == ()


def test_a_permanent_error_is_not_retried(kafka: FakeKafka) -> None:
    """NFR-10: no unbounded retry on something that cannot succeed."""
    producer = Producer(kafka, max_retries=10)
    producer.enqueue(core(1))
    kafka._topics.pop(TICK_TOPIC)  # noqa: SLF001 - simulate a vanished topic
    result = producer.flush()
    assert result["failed"] == 1
    assert producer.ledger[0].attempts == 1


def test_transient_failures_are_retried_within_budget(kafka: FakeKafka) -> None:
    producer = Producer(kafka, max_retries=5)
    producer.transient_failures_remaining = 2
    producer.enqueue(core(1))
    assert producer.flush()["delivered"] == 1
    assert producer.ledger[0].attempts == 3
    assert producer.counters.retries == 2


def test_the_ledger_receipt_states_it_is_only_supporting_evidence(kafka: FakeKafka) -> None:
    producer = Producer(kafka)
    producer.enqueue(core(1))
    producer.flush()
    receipt = producer.ledger_receipt()
    assert receipt["entries_total"] == 1
    assert receipt["counters"]["delivered"] == 1
    assert receipt["unknown_identities"] == []
