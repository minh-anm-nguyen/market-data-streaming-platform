"""Kafka-side faults: broker loss, ISR degradation, lost ACKs, retention expiry.

Each one reads broker state back to confirm. Stopping a broker is observed by
re-reading the live set and the partition's in-sync count, not by assuming the
call worked; expiring retention is observed by re-reading
``log_start_offset``. That is the difference design 6.3 draws between an
injection and a hopeful call.
"""

from __future__ import annotations

from ..dataplane.kafka_fake import FakeKafka
from ..evidence.proof_ledger import FaultReceipt
from .base import DEFAULT_ALLOWLIST, LabAllowlist, inject


def stop_broker(
    kafka: FakeKafka,
    broker_id: int,
    *,
    fault_id: str = "kafka-stop-broker",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Take one broker out of the live set."""
    return inject(
        fault_id=fault_id,
        intended_fault="broker_stop",
        target="kafka_broker",
        allowlist=allowlist,
        precondition=lambda: broker_id in kafka.live_brokers,
        action=lambda: kafka.stop_broker(broker_id),
        observe=lambda: (
            broker_id not in kafka.live_brokers,
            f"live_brokers={sorted(kafka.live_brokers)}",
        ),
        detail={"broker_id": broker_id, "ha_capable": kafka.ha_capable},
    )


def degrade_isr_below_min(
    kafka: FakeKafka,
    topic: str,
    partition: int,
    *,
    fault_id: str = "kafka-isr-degrade",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Stop brokers until this partition has fewer in-sync replicas than minISR.

    The fault is "writes must now be refused", so the observation checks the ISR
    count against the configured minimum rather than checking that some brokers
    went away. ``E2E-12`` then asserts the producer holds instead of weakening
    durability.
    """
    minimum = kafka.config.min_insync_replicas

    def act() -> list[int]:
        stopped: list[int] = []
        for broker in sorted(kafka.live_brokers):
            if kafka.in_sync_replicas(topic, partition) < minimum:
                break
            kafka.stop_broker(broker)
            stopped.append(broker)
        return stopped

    return inject(
        fault_id=fault_id,
        intended_fault="isr_below_min",
        target="kafka_broker",
        allowlist=allowlist,
        precondition=lambda: kafka.in_sync_replicas(topic, partition) >= minimum,
        action=act,
        observe=lambda: (
            kafka.in_sync_replicas(topic, partition) < minimum,
            f"isr={kafka.in_sync_replicas(topic, partition)} min={minimum}",
        ),
        detail={"topic": topic, "partition": partition, "min_insync_replicas": minimum},
    )


def lose_next_ack(
    kafka: FakeKafka,
    topic: str,
    partition: int,
    *,
    fault_id: str = "kafka-ack-loss",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Arm a lost ACK on the next append to this partition.

    Arming is what is injected here; the *effect* arrives on the next produce.
    The observation therefore confirms the switch is armed, and the scenario
    confirms the consequence afterwards via the producer ledger and the scan.
    Pretending the effect is already observable would be the false confirmation
    design 6.1 warns about.
    """
    key = (topic, partition)
    return inject(
        fault_id=fault_id,
        intended_fault="ack_loss_after_append",
        target="kafka_topic",
        allowlist=allowlist,
        precondition=lambda: key not in kafka.ack_loss_once,
        action=lambda: kafka.ack_loss_once.add(key),
        observe=lambda: (key in kafka.ack_loss_once, "ack_loss armed"),
        detail={"topic": topic, "partition": partition, "armed_only": True},
    )


def expire_retention(
    kafka: FakeKafka,
    topic: str,
    partition: int,
    new_log_start: int,
    *,
    fault_id: str = "kafka-retention-expiry",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
    confirm: bool = False,
) -> FaultReceipt:
    """Delete the prefix of a partition's log. Destructive, so it needs ``confirm``."""
    before = kafka.log_start_offset(topic, partition)
    return inject(
        fault_id=fault_id,
        intended_fault="retention_expiry",
        target="kafka_topic",
        allowlist=allowlist,
        destructive=True,
        confirm_destructive=confirm,
        precondition=lambda: new_log_start > before,
        action=lambda: kafka.expire_to(topic, partition, new_log_start),
        observe=lambda: (
            kafka.log_start_offset(topic, partition) > before,
            f"log_start {before} -> {kafka.log_start_offset(topic, partition)}",
        ),
        detail={"topic": topic, "partition": partition, "requested_log_start": new_log_start},
    )


def force_rebalance(
    kafka: FakeKafka,
    group: str,
    *,
    fault_id: str = "kafka-rebalance",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Bump the group epoch, fencing every worker that does not get the new one."""
    before = kafka.group_epoch(group)
    return inject(
        fault_id=fault_id,
        intended_fault="consumer_rebalance",
        target="p05_ingestor",
        allowlist=allowlist,
        precondition=lambda: True,
        action=lambda: kafka.bump_group_epoch(group),
        observe=lambda: (
            kafka.group_epoch(group) > before,
            f"group epoch {before} -> {kafka.group_epoch(group)}",
        ),
        detail={"group": group, "epoch_before": before},
    )


def inject_transient_produce_failures(
    producer: object,
    count: int,
    *,
    fault_id: str = "kafka-transient-failures",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Make the next ``count`` publish attempts fail transiently."""

    def act() -> int:
        setattr(producer, "transient_failures_remaining", count)
        return count

    return inject(
        fault_id=fault_id,
        intended_fault="transient_produce_failure",
        target="p04_producer",
        allowlist=allowlist,
        precondition=lambda: getattr(producer, "transient_failures_remaining", 0) == 0,
        action=act,
        observe=lambda: (
            getattr(producer, "transient_failures_remaining", 0) == count,
            f"armed {count} transient failures",
        ),
        detail={"count": count, "armed_only": True},
    )
