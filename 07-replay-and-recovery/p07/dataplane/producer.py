"""P04's producer side: routing, a bounded queue, retries and a delivery ledger.

The ledger is the interesting part. Every publish attempt ends in exactly one of
three states and the third is not an error to be smoothed over:

``DELIVERED``
    the broker acknowledged, and the ledger holds the offset.
``FAILED``
    the broker refused before appending -- no leader, minISR unavailable. The
    record is *not* in the log.
``UNKNOWN``
    the retry budget ran out while the outcome was ambiguous. The record may or
    may not be in the log.

Design 3.3 step 2 requires that an unresolved ACK be carried as ``UNKNOWN``
rather than collapsed to "not published", and design 1.1 FR-07 forbids inferring
an outcome from a timeout. So :meth:`Producer.flush` returns counts including
``unknown``, and the only thing that can resolve an ``UNKNOWN`` is the
independent Kafka scan in :mod:`p07.replay.kafka_cut_scanner`.

A note on what the ledger is *for*: design 3.3 calls it "evidence phụ trợ" --
supporting evidence. A record appearing here as ``DELIVERED`` does not prove the
record is in the log, because this is the producer's own account. The scanner
decides.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

from ..contracts import MAX_EVENT_VALUE_BYTES, SemanticCore, SourceIdentity, partition_for_symbol, topic_for_kind
from .codec import Codec, DEFAULT_CODEC
from .kafka_fake import AckUnknown, BrokerUnavailable, FakeKafka, KafkaError, NotEnoughReplicas


class Delivery(str, Enum):
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    """One publish attempt's terminal state, as the producer saw it."""

    identity: SourceIdentity
    topic: str
    partition: int
    status: Delivery
    offset: int | None
    attempts: int
    reason: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "identity": list(self.identity),
            "topic": self.topic,
            "partition": self.partition,
            "status": self.status.value,
            "offset": self.offset,
            "attempts": self.attempts,
            "reason": self.reason,
        }


class QueueFull(Exception):
    """The bounded queue is full. Never silently dropped (design 8)."""


@dataclass(slots=True)
class ProducerCounters:
    enqueued: int = 0
    delivered: int = 0
    failed: int = 0
    unknown: int = 0
    retries: int = 0
    rejected_oversize: int = 0
    peak_queue_depth: int = 0
    peak_queue_bytes: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "enqueued": self.enqueued,
            "delivered": self.delivered,
            "failed": self.failed,
            "unknown": self.unknown,
            "retries": self.retries,
            "rejected_oversize": self.rejected_oversize,
            "peak_queue_depth": self.peak_queue_depth,
            "peak_queue_bytes": self.peak_queue_bytes,
        }


class Producer:
    """Serialises accepted events, routes them, and publishes with a retry budget.

    The Kafka record ``value`` is **raw Protobuf with no MDP1 prefix**. MDP1 is
    TCP framing for P03's input; shipping it into a Kafka value is case
    ``P07-G03-12`` and the scanner fails on it. ``key`` is the symbol, so a
    symbol keeps partition affinity within one generation -- which is affinity,
    not a global order (design 3.2).
    """

    def __init__(
        self,
        kafka: FakeKafka,
        *,
        codec: Codec = DEFAULT_CODEC,
        producer_id: str = "p04-producer-1",
        max_queue_events: int = 50_000,
        max_queue_bytes: int = 64 * 1024 * 1024,
        max_retries: int = 10,
        idempotent: bool = True,
    ) -> None:
        self._kafka = kafka
        self._codec = codec
        self._producer_id = producer_id
        self._max_queue_events = max_queue_events
        self._max_queue_bytes = max_queue_bytes
        self._max_retries = max_retries
        self._idempotent = idempotent
        self._queue: deque[tuple[SemanticCore, bytes]] = deque()
        self._queue_bytes = 0
        self._seq_by_partition: dict[tuple[str, int], int] = {}
        self.ledger: list[LedgerEntry] = []
        self.counters = ProducerCounters()
        #: Faults the Kafka injector sets: identities whose first attempt should
        #: see a lost ACK, and how many transient failures to simulate.
        self.transient_failures_remaining = 0

    # -- enqueue ---------------------------------------------------------

    @property
    def queue_depth(self) -> int:
        return len(self._queue)

    @property
    def queue_bytes(self) -> int:
        return self._queue_bytes

    def enqueue(self, core: SemanticCore) -> None:
        """Serialise and queue one event. Raises :class:`QueueFull` at the cap.

        Raising rather than dropping is the whole contract of a bounded queue in
        design 8: "Pause source/consumer; không silent drop".
        """
        value = self._codec.encode(core)
        if len(value) > MAX_EVENT_VALUE_BYTES:
            self.counters.rejected_oversize += 1
            raise ValueError(
                f"encoded value {len(value)}B exceeds the {MAX_EVENT_VALUE_BYTES}B contract cap"
            )
        if len(self._queue) >= self._max_queue_events or self._queue_bytes + len(value) > self._max_queue_bytes:
            raise QueueFull(
                f"queue at {len(self._queue)} events / {self._queue_bytes} bytes"
            )
        self._queue.append((core, value))
        self._queue_bytes += len(value)
        self.counters.enqueued += 1
        self.counters.peak_queue_depth = max(self.counters.peak_queue_depth, len(self._queue))
        self.counters.peak_queue_bytes = max(self.counters.peak_queue_bytes, self._queue_bytes)

    # -- publish ---------------------------------------------------------

    def _headers(self, core: SemanticCore) -> Mapping[str, str]:
        return {
            "dataset_id": core.dataset_id,
            "source_schema_version": core.source_schema_version,
            "event_kind": core.event_kind.wire_name,
            "protobuf_package": self._codec.package,
            "descriptor_sha256": self._codec.descriptor_sha256,
        }

    def flush(self) -> dict[str, int]:
        """Publish everything queued. Returns ``{delivered, failed, unknown}``.

        Each record gets its own retry budget. A retry reuses the same
        ``producer_seq``, so an idempotent broker recognises it and a
        non-idempotent one appends a second physical copy -- which is the
        difference ``E2E-09`` exists to show.
        """
        delivered = failed = unknown = 0
        while self._queue:
            core, value = self._queue[0]
            topic = topic_for_kind(core.event_kind)
            try:
                partition = partition_for_symbol(core.symbol, self._kafka.partition_count(topic))
            except KafkaError as exc:
                # Routing itself failed -- an unknown topic, or a cluster that
                # cannot describe itself. ``flush`` must still return rather than
                # raise: propagating here would lose the ledger for every record
                # already published in this call, which is the evidence the
                # reconciler needs most when something is this wrong.
                self._queue.popleft()
                self._queue_bytes -= len(value)
                self.ledger.append(
                    LedgerEntry(core.identity, topic, -1, Delivery.FAILED, None, 1, exc.reason)
                )
                failed += 1
                self.counters.failed += 1
                continue
            key = (topic, partition)
            seq = self._seq_by_partition.get(key, 0)
            entry = self._publish_one(core, value, topic, partition, seq)
            self._queue.popleft()
            self._queue_bytes -= len(value)
            self._seq_by_partition[key] = seq + 1
            self.ledger.append(entry)
            if entry.status is Delivery.DELIVERED:
                delivered += 1
                self.counters.delivered += 1
            elif entry.status is Delivery.FAILED:
                failed += 1
                self.counters.failed += 1
            else:
                unknown += 1
                self.counters.unknown += 1
        return {"delivered": delivered, "failed": failed, "unknown": unknown}

    def _publish_one(
        self, core: SemanticCore, value: bytes, topic: str, partition: int, seq: int
    ) -> LedgerEntry:
        attempts = 0
        last_reason: str | None = None
        while attempts < self._max_retries:
            attempts += 1
            if self.transient_failures_remaining > 0:
                self.transient_failures_remaining -= 1
                self.counters.retries += 1
                last_reason = "INJECTED_TRANSIENT"
                continue
            try:
                offset = self._kafka.produce(
                    topic,
                    partition,
                    key=core.symbol.encode("utf-8"),
                    value=value,
                    headers=self._headers(core),
                    timestamp_ns=core.event_time_ns,
                    producer_id=self._producer_id if self._idempotent else None,
                    producer_seq=seq if self._idempotent else None,
                )
            except AckUnknown as exc:
                # The append happened; this side does not know that. Retry
                # within budget, exactly as a real producer would, and do NOT
                # read ``exc.offset`` -- that is lab-only knowledge.
                self.counters.retries += 1
                last_reason = exc.reason
                continue
            except (NotEnoughReplicas, BrokerUnavailable) as exc:
                # Refused before the append: nothing is in the log. Retry within
                # budget; the record is recoverable either way.
                self.counters.retries += 1
                last_reason = exc.reason
                continue
            except KafkaError as exc:
                # A permanent error (unknown topic/partition). Retrying it would
                # be the unbounded retry storm NFR-10 forbids.
                return LedgerEntry(
                    core.identity, topic, partition, Delivery.FAILED, None, attempts, exc.reason
                )
            return LedgerEntry(
                core.identity, topic, partition, Delivery.DELIVERED, offset, attempts, None
            )

        status = (
            Delivery.UNKNOWN
            if last_reason in {"PRODUCER_ACK_UNKNOWN", "INJECTED_TRANSIENT"}
            else Delivery.FAILED
        )
        return LedgerEntry(core.identity, topic, partition, status, None, attempts, last_reason)

    # -- evidence --------------------------------------------------------

    def unresolved_identities(self) -> tuple[SourceIdentity, ...]:
        """Identities whose outcome the producer could not determine.

        These are the ones the Kafka scan has to resolve before any verdict. A
        non-empty result with no scan means ``INCONCLUSIVE``, not ``FAIL``.
        """
        return tuple(
            entry.identity for entry in self.ledger if entry.status is Delivery.UNKNOWN
        )

    def ledger_receipt(self, limit: int = 200) -> dict[str, Any]:
        return {
            "producer_id": self._producer_id,
            "idempotent": self._idempotent,
            "counters": self.counters.as_dict(),
            "unknown_identities": [list(identity) for identity in self.unresolved_identities()],
            "entries_sample": [entry.as_dict() for entry in self.ledger[:limit]],
            "entries_total": len(self.ledger),
        }
