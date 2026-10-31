"""P04's broker, modelled as a state machine: offsets, generations, ISR, retention.

What this models faithfully, because the recovery protocol depends on it:

* **Offsets are per-partition metadata**, allocated by the log and unrelated to
  the source ``sequence``. Nothing here can return a global order across
  ``md.ticks.v1`` and ``md.quotes.v1``; :meth:`FakeKafka.scan` only ever works
  within one ``(topic, partition)`` (design 4.3).
* **A topic generation is part of the coordinate.** :meth:`FakeKafka.new_generation`
  gives a fresh, empty set of topics with the same names, which is what a
  republish after data loss gets. Offsets in the new generation restart at 0 and
  mean something different, so a cut captured in one generation is useless in
  another (case ``P07-G10-*``).
* **Retention deletes the beginning of the log.** After
  :meth:`FakeKafka.expire_to`, a scan that starts below ``log_start_offset``
  raises :class:`OffsetExpired` instead of returning fewer records. Returning a
  short read is exactly how a harness convinces itself nothing was lost
  (case ``E2E-20``).
* **Durability depends on in-sync replicas.** With ``min_insync_replicas=2``,
  stopping brokers until fewer than two are in sync makes
  :meth:`FakeKafka.produce` raise rather than accept a weaker write. The design
  forbids "lower minISR to make the benchmark green" (runbook RB-01).
* **An ACK can be lost after the append.** ``ack_loss`` makes the append happen
  and *then* raise :class:`AckUnknown`. The record is in the log; the producer
  does not know. That ambiguity is the subject of ``E2E-09`` and ``E2E-14``, and
  the only honest resolution is an independent scan.

What it does not model: replication timing, leader election mechanics, the
produce/fetch protocol, batching, compression, or librdkafka's partitioner. See
:mod:`p07.dataplane` for why that is stated so loudly.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Iterator, Mapping, Sequence

from ..contracts import QUOTE_TOPIC, TICK_TOPIC

#: Default lab topology. RF=3/minISR=2 is the ``RELIABILITY-LAB`` profile; a
#: single-broker RF=1 lab can restart a process but proves nothing about HA, and
#: :attr:`FakeKafka.ha_capable` says so.
DEFAULT_BROKERS = 3
DEFAULT_REPLICATION = 3
DEFAULT_MIN_ISR = 2
DEFAULT_PARTITIONS = 8


class KafkaError(Exception):
    """Base class, carrying a reason code."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


class BrokerUnavailable(KafkaError):
    """No leader, or the cluster is down."""


class NotEnoughReplicas(KafkaError):
    """Fewer in-sync replicas than ``min_insync_replicas``."""


class OffsetExpired(KafkaError):
    """The requested offset is below ``log_start_offset``."""


class AckUnknown(KafkaError):
    """The append happened; the acknowledgement did not arrive.

    Carries ``offset`` because the *lab* knows where the record went even though
    the producer does not. Tests assert that the producer does **not** use it:
    it exists so the scenario can later show what the independent scan should
    find.
    """

    def __init__(self, topic: str, partition: int, offset: int) -> None:
        super().__init__("PRODUCER_ACK_UNKNOWN", f"{topic}/{partition}@{offset}")
        self.topic = topic
        self.partition = partition
        self.offset = offset


@dataclass(frozen=True, slots=True)
class Record:
    """One physical record in a partition."""

    offset: int
    key: bytes
    value: bytes
    headers: Mapping[str, str]
    timestamp_ns: int
    producer_id: str | None = None
    producer_seq: int | None = None


@dataclass(slots=True)
class Partition:
    """One partition's log and its bounds."""

    records: list[Record] = field(default_factory=list)
    log_start_offset: int = 0
    leader: int = 0
    #: Idempotent-producer bookkeeping: highest accepted ``producer_seq`` per
    #: producer id, and the offset each accepted sequence landed at. Kafka's
    #: real idempotence is per ``(pid, epoch, partition)`` with a 5-deep window;
    #: this keeps the part that matters for a retry after a lost ACK -- the
    #: re-send is recognised and does **not** append a second copy.
    producer_seq_high: dict[str, int] = field(default_factory=dict)
    producer_seq_offset: dict[tuple[str, int], int] = field(default_factory=dict)

    @property
    def high_watermark(self) -> int:
        """Next offset to be assigned. Equals the end of the committed log."""
        return self.log_start_offset + len(self.records)

    def record_at(self, offset: int) -> Record:
        return self.records[offset - self.log_start_offset]


@dataclass(frozen=True, slots=True)
class TopicConfig:
    partitions: int
    replication_factor: int
    min_insync_replicas: int


class FakeKafka:
    """An in-process broker with offsets, generations, ISR and retention."""

    def __init__(
        self,
        *,
        cluster_id: str = "lab-cluster-1",
        brokers: int = DEFAULT_BROKERS,
        partitions: int = DEFAULT_PARTITIONS,
        replication_factor: int = DEFAULT_REPLICATION,
        min_insync_replicas: int = DEFAULT_MIN_ISR,
        topics: Sequence[str] = (TICK_TOPIC, QUOTE_TOPIC),
        allow_auto_create: bool = False,
        idempotent_dedupe: bool = True,
    ) -> None:
        if replication_factor > brokers:
            raise ValueError("replication_factor cannot exceed broker count")
        if min_insync_replicas > replication_factor:
            raise ValueError("min_insync_replicas cannot exceed replication_factor")
        self.cluster_id = cluster_id
        self.allow_auto_create = allow_auto_create
        #: Turn off to model a non-idempotent producer, where a retry after a
        #: lost ACK really does leave two physical copies (case ``E2E-09``).
        self.idempotent_dedupe = idempotent_dedupe
        self._brokers_total = brokers
        self._live_brokers: set[int] = set(range(brokers))
        self._config = TopicConfig(partitions, replication_factor, min_insync_replicas)
        self._generation_counter = itertools.count(1)
        self.topic_generation = f"gen-{next(self._generation_counter)}"
        self._topics: dict[str, list[Partition]] = {}
        for name in topics:
            self._create_topic(name)
        self._committed: dict[tuple[str, str, int], int] = {}
        self._group_epoch: dict[str, int] = {}
        #: Set by the Kafka injector to make the next append lose its ACK.
        self.ack_loss_once: set[tuple[str, int]] = set()
        #: Produce latency injection hook: a callable returning None, or raising.
        self.produce_barrier = None

    # -- topology -------------------------------------------------------

    def _create_topic(self, name: str) -> None:
        self._topics[name] = [
            Partition(leader=index % max(1, self._brokers_total))
            for index in range(self._config.partitions)
        ]

    @property
    def config(self) -> TopicConfig:
        return self._config

    @property
    def ha_capable(self) -> bool:
        """Whether this topology can survive a broker loss at all.

        RF=1 cannot. Reporting a successful restart on an RF=1 lab as evidence
        of high availability is the claim design 0.2 puts out of scope.
        """
        return self._config.replication_factor >= 2 and self._config.min_insync_replicas >= 2

    def topics(self) -> tuple[str, ...]:
        return tuple(sorted(self._topics))

    def partition_count(self, topic: str) -> int:
        return len(self._require_topic(topic))

    def _require_topic(self, topic: str) -> list[Partition]:
        try:
            return self._topics[topic]
        except KeyError:
            if self.allow_auto_create:
                self._create_topic(topic)
                return self._topics[topic]
            # Auto-creation is off by default: a typo'd topic name that springs
            # into existence turns a configuration error into silent data loss
            # (lab config ``forbid_auto_topic_creation``).
            raise KafkaError("UNKNOWN_TOPIC", topic) from None

    def new_generation(self) -> str:
        """Discard the logs and start a fresh generation with empty topics."""
        self.topic_generation = f"gen-{next(self._generation_counter)}"
        for name in list(self._topics):
            self._create_topic(name)
        self._committed.clear()
        return self.topic_generation

    # -- broker availability --------------------------------------------

    def stop_broker(self, broker_id: int) -> None:
        self._live_brokers.discard(broker_id)

    def start_broker(self, broker_id: int) -> None:
        if 0 <= broker_id < self._brokers_total:
            self._live_brokers.add(broker_id)

    @property
    def live_brokers(self) -> frozenset[int]:
        return frozenset(self._live_brokers)

    def in_sync_replicas(self, topic: str, partition: int) -> int:
        """How many replicas are currently in sync for this partition.

        Modelled simply: replicas live on consecutive broker ids starting at the
        partition's assigned leader, and a replica is in sync when its broker is
        up. Enough to drive the minISR decision, and not pretending to be the
        replication protocol.
        """
        self._require_topic(topic)
        assigned = [
            (self._topics[topic][partition].leader + offset) % self._brokers_total
            for offset in range(self._config.replication_factor)
        ]
        return sum(1 for broker in assigned if broker in self._live_brokers)

    def leader_available(self, topic: str, partition: int) -> bool:
        partitions = self._require_topic(topic)
        return partitions[partition].leader in self._live_brokers

    def elect_new_leader(self, topic: str, partition: int) -> int | None:
        """Move leadership to a live broker. Returns the new leader, or ``None``.

        Only an in-sync replica may take over: electing an out-of-sync replica
        is unclean leader election, which loses data silently and is exactly
        what a durability test must not quietly permit.
        """
        partitions = self._require_topic(topic)
        target = partitions[partition]
        for offset in range(1, self._config.replication_factor):
            candidate = (target.leader + offset) % self._brokers_total
            if candidate in self._live_brokers:
                target.leader = candidate
                return candidate
        return None

    # -- produce / fetch -------------------------------------------------

    def produce(
        self,
        topic: str,
        partition: int,
        *,
        key: bytes,
        value: bytes,
        headers: Mapping[str, str] | None = None,
        timestamp_ns: int = 0,
        producer_id: str | None = None,
        producer_seq: int | None = None,
    ) -> int:
        """Append and return the assigned offset.

        Raises :class:`NotEnoughReplicas` or :class:`BrokerUnavailable` *before*
        appending, and :class:`AckUnknown` *after* appending.
        """
        partitions = self._require_topic(topic)
        if not 0 <= partition < len(partitions):
            raise KafkaError("UNKNOWN_PARTITION", f"{topic}/{partition}")
        if not self._live_brokers:
            raise BrokerUnavailable("KAFKA_UNAVAILABLE", "no live brokers")
        if not self.leader_available(topic, partition) and self.elect_new_leader(topic, partition) is None:
            raise BrokerUnavailable("KAFKA_UNAVAILABLE", f"no leader for {topic}/{partition}")
        if self.in_sync_replicas(topic, partition) < self._config.min_insync_replicas:
            raise NotEnoughReplicas(
                "KAFKA_MIN_ISR_UNAVAILABLE",
                f"{topic}/{partition} isr={self.in_sync_replicas(topic, partition)}"
                f" < {self._config.min_insync_replicas}",
            )
        if self.produce_barrier is not None:
            self.produce_barrier(topic, partition)

        target = partitions[partition]
        if self.idempotent_dedupe and producer_id is not None and producer_seq is not None:
            known = target.producer_seq_offset.get((producer_id, producer_seq))
            if known is not None:
                # A retry of an already-appended record. Kafka answers with the
                # original offset and writes nothing; a lab that appended again
                # here would hide the very duplicate it is meant to detect.
                if (topic, partition) in self.ack_loss_once:
                    self.ack_loss_once.discard((topic, partition))
                    raise AckUnknown(topic, partition, known)
                return known
        offset = target.high_watermark
        target.records.append(
            Record(
                offset=offset,
                key=key,
                value=value,
                headers=dict(headers or {}),
                timestamp_ns=timestamp_ns,
                producer_id=producer_id,
                producer_seq=producer_seq,
            )
        )
        if producer_id is not None and producer_seq is not None:
            target.producer_seq_offset[(producer_id, producer_seq)] = offset
            previous_high = target.producer_seq_high.get(producer_id, -1)
            target.producer_seq_high[producer_id] = max(previous_high, producer_seq)
        if (topic, partition) in self.ack_loss_once:
            self.ack_loss_once.discard((topic, partition))
            raise AckUnknown(topic, partition, offset)
        return offset

    def high_watermark(self, topic: str, partition: int) -> int:
        return self._require_topic(topic)[partition].high_watermark

    def log_start_offset(self, topic: str, partition: int) -> int:
        return self._require_topic(topic)[partition].log_start_offset

    def scan(self, topic: str, partition: int, start: int, end_exclusive: int) -> Iterator[Record]:
        """Read-only scan of ``[start, end_exclusive)``.

        Used by the evidence plane. It refuses rather than truncates: a start
        below ``log_start_offset`` raises :class:`OffsetExpired`, and an end past
        the high watermark raises, because silently returning a shorter range
        would make a missing record look like an empty one.
        """
        partitions = self._require_topic(topic)
        if not 0 <= partition < len(partitions):
            raise KafkaError("UNKNOWN_PARTITION", f"{topic}/{partition}")
        target = partitions[partition]
        if start < target.log_start_offset:
            raise OffsetExpired(
                "CUT_RANGE_EXPIRED",
                f"{topic}/{partition} start={start} < log_start={target.log_start_offset}",
            )
        if end_exclusive > target.high_watermark:
            raise KafkaError(
                "CUT_OFFSET_HOLE",
                f"{topic}/{partition} end={end_exclusive} > hwm={target.high_watermark}",
            )
        for offset in range(start, end_exclusive):
            yield target.record_at(offset)

    def fetch(self, topic: str, partition: int, offset: int, max_records: int) -> list[Record]:
        """Consumer-side read. Returns fewer records at the end of the log."""
        partitions = self._require_topic(topic)
        if not self._live_brokers:
            raise BrokerUnavailable("KAFKA_UNAVAILABLE", "no live brokers")
        target = partitions[partition]
        if offset < target.log_start_offset:
            raise OffsetExpired(
                "CUT_RANGE_EXPIRED",
                f"{topic}/{partition} offset={offset} < log_start={target.log_start_offset}",
            )
        end = min(offset + max_records, target.high_watermark)
        return [target.record_at(current) for current in range(offset, end)]

    def expire_to(self, topic: str, partition: int, new_log_start: int) -> int:
        """Delete records below ``new_log_start``. Returns how many went.

        This is retention, not a delete API: it only ever removes a prefix, and
        once removed those offsets cannot be read again in this generation.
        """
        partitions = self._require_topic(topic)
        target = partitions[partition]
        new_log_start = min(new_log_start, target.high_watermark)
        if new_log_start <= target.log_start_offset:
            return 0
        removed = new_log_start - target.log_start_offset
        del target.records[:removed]
        target.log_start_offset = new_log_start
        return removed

    # -- consumer group state --------------------------------------------

    def commit(self, group: str, topic: str, partition: int, next_offset: int, *, epoch: int) -> None:
        """Commit ``next_offset`` for the group, fenced by ``epoch``.

        A stale epoch is refused. That is the broker-side half of fencing; the
        consumer-side half is in :mod:`p07.dataplane.ingestion`, and
        ``INV-04`` is about both holding at once.
        """
        current_epoch = self._group_epoch.setdefault(group, epoch)
        if epoch < current_epoch:
            raise KafkaError("COMMIT_BY_STALE_OWNER", f"{group} epoch={epoch} < {current_epoch}")
        self._group_epoch[group] = epoch
        self._require_topic(topic)
        self._committed[(group, topic, partition)] = next_offset

    def committed(self, group: str, topic: str, partition: int) -> int | None:
        """Committed next offset, or ``None`` when the group never committed.

        ``None`` is deliberately not 0 and never silently becomes "latest": an
        implicit reset is case ``E2E-20`` and invariant ``INV-08``.
        """
        return self._committed.get((group, topic, partition))

    def bump_group_epoch(self, group: str) -> int:
        """Simulate a rebalance: the group gets a new, higher epoch."""
        epoch = self._group_epoch.get(group, 0) + 1
        self._group_epoch[group] = epoch
        return epoch

    def group_epoch(self, group: str) -> int:
        return self._group_epoch.get(group, 0)

    # -- metadata for the evidence bundle --------------------------------

    def describe(self) -> dict[str, object]:
        return {
            "cluster_id": self.cluster_id,
            "topic_generation": self.topic_generation,
            "brokers_total": self._brokers_total,
            "brokers_live": sorted(self._live_brokers),
            "partitions_per_topic": self._config.partitions,
            "replication_factor": self._config.replication_factor,
            "min_insync_replicas": self._config.min_insync_replicas,
            "ha_capable": self.ha_capable,
            "idempotent_dedupe": self.idempotent_dedupe,
            "topics": {
                name: [
                    {
                        "partition": index,
                        "log_start_offset": partition.log_start_offset,
                        "high_watermark": partition.high_watermark,
                        "leader": partition.leader,
                        "isr": self.in_sync_replicas(name, index),
                    }
                    for index, partition in enumerate(partitions)
                ]
                for name, partitions in sorted(self._topics.items())
            },
        }
