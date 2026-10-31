"""A deterministic in-memory Kafka, faithful to the behaviours the spec tests.

This is not a general Kafka emulator. It reproduces exactly the behaviours the
correctness argument depends on, and nothing else:

  * a per-partition append-only log with real offsets;
  * ``log_start_offset`` that can be moved forward to simulate retention
    expiry, after which a required offset raises ``RetentionLostError`` rather
    than silently resetting (spec I-11, section 5.2 ``RETENTION_LOST``);
  * **offset gaps**. ``produce_control_record`` consumes offset space without
    ever being returned to the application, which is what transaction and
    control records do. Code that assumes the consumed offsets are contiguous
    integers breaks against this, and spec section 4.3 says such a gap is not
    a hole;
  * group committed offsets, with ``commit_offset`` able to fail in three
    distinguishable ways: rejected, lost response (unknown), and silently
    accepted;
  * pause/resume honoured by ``poll``;
  * cooperative revoke, hard loss and re-assignment, driven explicitly by the
    test so a rebalance happens at a chosen instant rather than by luck.

What it deliberately does NOT model: replication, ISR, leader election, the
librdkafka fetch queue and its memory, or real network behaviour. Those cannot
be faked honestly and belong to the live-infrastructure tests, which skip when
no broker is present.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum

from ..errors import CommitNotConfirmedError, RetentionLostError
from ..model import ConsumedRecord, TopicPartition


class CommitFault(StrEnum):
    """How the next commit should misbehave."""

    NONE = "none"
    #: Broker rejects the commit; the caller learns it did not happen.
    REJECT = "reject"
    #: The commit is applied but the response is lost. The caller must treat
    #: the position as UNKNOWN, which is the nastiest of the three.
    LOST_RESPONSE = "lost_response"
    #: The call raises before the broker saw anything.
    UNREACHABLE = "unreachable"


@dataclass(slots=True)
class StoredRecord:
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[str, bytes], ...] = ()
    timestamp_ms: int | None = None
    #: Control records occupy an offset but are never delivered.
    is_control: bool = False


@dataclass(slots=True)
class PartitionLog:
    records: list[StoredRecord] = field(default_factory=list)
    log_start_offset: int = 0
    next_offset: int = 0

    @property
    def high_watermark(self) -> int:
        return self.next_offset

    def append(self, record: StoredRecord) -> int:
        record.offset = self.next_offset
        self.next_offset += 1
        self.records.append(record)
        return record.offset

    def readable_from(self, offset: int) -> list[StoredRecord]:
        return [r for r in self.records if r.offset >= offset and not r.is_control]


class FakeKafkaCluster:
    """Holds topics, logs and one consumer group's committed offsets."""

    def __init__(self, *, cluster_id: str = "fake-cluster") -> None:
        self.cluster_id = cluster_id
        self._logs: dict[TopicPartition, PartitionLog] = {}
        self._committed: dict[str, dict[TopicPartition, int]] = {}

    # ----------------------------------------------------------------- topics

    def create_topic(self, topic: str, partitions: int) -> None:
        for p in range(partitions):
            self._logs.setdefault(TopicPartition(topic, p), PartitionLog())

    def partitions_of(self, topic: str) -> list[TopicPartition]:
        return sorted(tp for tp in self._logs if tp.topic == topic)

    def log(self, tp: TopicPartition) -> PartitionLog:
        if tp not in self._logs:
            raise KeyError(f"{tp} does not exist; create_topic first")
        return self._logs[tp]

    # --------------------------------------------------------------- produce

    def produce(
        self,
        topic: str,
        partition: int,
        *,
        key: bytes | None,
        value: bytes | None,
        headers: Sequence[tuple[str, bytes]] = (),
        timestamp_ms: int | None = None,
    ) -> int:
        tp = TopicPartition(topic, partition)
        return self.log(tp).append(
            StoredRecord(
                offset=-1,
                key=key,
                value=value,
                headers=tuple(headers),
                timestamp_ms=timestamp_ms,
            )
        )

    def produce_control_record(self, topic: str, partition: int) -> int:
        """Burn one offset without producing a deliverable record.

        Models a transaction marker. The consumer will see the offsets jump,
        and must not conclude that a record went missing.
        """
        tp = TopicPartition(topic, partition)
        return self.log(tp).append(
            StoredRecord(offset=-1, key=None, value=None, is_control=True)
        )

    # ------------------------------------------------------------- retention

    def expire_to(self, tp: TopicPartition, new_log_start_offset: int) -> None:
        """Move ``log_start_offset`` forward, dropping older records."""
        log = self.log(tp)
        if new_log_start_offset < log.log_start_offset:
            raise ValueError("retention only moves forward")
        log.log_start_offset = new_log_start_offset
        log.records = [r for r in log.records if r.offset >= new_log_start_offset]

    # --------------------------------------------------------------- offsets

    def committed(self, group_id: str, tp: TopicPartition) -> int | None:
        return self._committed.get(group_id, {}).get(tp)

    def set_committed(self, group_id: str, tp: TopicPartition, next_offset: int) -> None:
        self._committed.setdefault(group_id, {})[tp] = next_offset

    def all_committed(self, group_id: str) -> dict[TopicPartition, int]:
        return dict(self._committed.get(group_id, {}))

    # ---------------------------------------------------------------- cuts

    def end_offsets(self) -> dict[TopicPartition, int]:
        """A frozen cut: next readable position per partition (spec 9.2)."""
        return {tp: log.high_watermark for tp, log in sorted(self._logs.items())}

    def scan(
        self, cut: dict[TopicPartition, int] | None = None
    ) -> list[tuple[TopicPartition, StoredRecord]]:
        """Read-only scan up to ``cut``, for the independent verifier.

        Separate from the consumer on purpose: the reconciler must not share
        state with the thing it is checking (spec 9.1, verifier row).
        """
        out: list[tuple[TopicPartition, StoredRecord]] = []
        for tp, log in sorted(self._logs.items()):
            limit = log.high_watermark if cut is None else cut.get(tp, 0)
            for record in log.records:
                if record.is_control or record.offset >= limit:
                    continue
                out.append((tp, record))
        return out


class FakeConsumer:
    """A ``KafkaSource`` over ``FakeKafkaCluster``, with fault injection.

    Assignment is explicit. ``subscribe`` assigns every partition of the
    subscribed topics by default; a test that wants a rebalance calls
    ``revoke``, ``lose`` or ``assign`` directly at the moment it chooses.
    """

    def __init__(
        self,
        cluster: FakeKafkaCluster,
        *,
        group_id: str,
        auto_offset_reset: str = "error",
        assign_on_subscribe: bool = True,
    ) -> None:
        self._cluster = cluster
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._assign_on_subscribe = assign_on_subscribe

        self._assigned: list[TopicPartition] = []
        self._paused: set[TopicPartition] = set()
        self._positions: dict[TopicPartition, int] = {}
        self._subscribed: list[str] = []
        self._round_robin = 0
        self.closed = False

        self._on_assign: Callable | None = None
        self._on_revoke: Callable | None = None
        self._on_lost: Callable | None = None

        # ---- fault injection knobs, all inert by default
        self.commit_fault: CommitFault = CommitFault.NONE
        #: Apply the fault only this many more times, then revert to NONE.
        self.commit_fault_remaining: int = 0
        #: Partitions whose required offset has fallen out of retention.
        self.poll_raises_retention_lost: set[TopicPartition] = set()
        #: Counters for test assertions.
        self.commit_calls: list[tuple[TopicPartition, int]] = []
        self.pause_calls: list[tuple[TopicPartition, ...]] = []
        self.resume_calls: list[tuple[TopicPartition, ...]] = []

    # -------------------------------------------------------------- subscribe

    def subscribe(
        self,
        topics: Sequence[str],
        *,
        on_assign: Callable,
        on_revoke: Callable,
        on_lost: Callable,
    ) -> None:
        self._subscribed = list(topics)
        self._on_assign, self._on_revoke, self._on_lost = on_assign, on_revoke, on_lost
        if self._assign_on_subscribe:
            tps: list[TopicPartition] = []
            for topic in topics:
                tps.extend(self._cluster.partitions_of(topic))
            self.assign(sorted(tps))

    # --------------------------------------------------- assignment control

    def assign(self, tps: Sequence[TopicPartition]) -> None:
        """Grant ownership and fire ``on_assign``."""
        fresh = [tp for tp in tps if tp not in self._assigned]
        self._assigned.extend(fresh)
        self._assigned.sort()
        for tp in fresh:
            committed = self._cluster.committed(self._group_id, tp)
            if committed is None:
                if self._auto_offset_reset in ("error", "none"):
                    # No committed offset. A production-like group must not
                    # guess; the test chooses an explicit start position by
                    # calling seek_to_beginning or set_committed first.
                    committed = self._cluster.log(tp).log_start_offset
                else:  # pragma: no cover - unsafe policies are config-rejected
                    committed = self._cluster.log(tp).high_watermark
            self._positions[tp] = committed
        if self._on_assign is not None and fresh:
            self._on_assign(fresh)

    def revoke(self, tps: Sequence[TopicPartition]) -> None:
        """Cooperative revoke: the callback may still commit proven prefixes."""
        tps = [tp for tp in tps if tp in self._assigned]
        if self._on_revoke is not None and tps:
            self._on_revoke(list(tps))
        for tp in tps:
            self._assigned.remove(tp)
            self._paused.discard(tp)
            self._positions.pop(tp, None)

    def lose(self, tps: Sequence[TopicPartition]) -> None:
        """Hard loss: the callback must not commit anything."""
        tps = [tp for tp in tps if tp in self._assigned]
        if self._on_lost is not None and tps:
            self._on_lost(list(tps))
        for tp in tps:
            self._assigned.remove(tp)
            self._paused.discard(tp)
            self._positions.pop(tp, None)

    def seek(self, tp: TopicPartition, offset: int) -> None:
        self._positions[tp] = offset

    # ------------------------------------------------------------------ poll

    def poll(self, timeout_s: float) -> ConsumedRecord | None:
        """Round-robin across assigned, unpaused partitions. One record."""
        if self.closed:
            raise RuntimeError("consumer is closed")
        candidates = [tp for tp in self._assigned if tp not in self._paused]
        if not candidates:
            return None
        for i in range(len(candidates)):
            tp = candidates[(self._round_robin + i) % len(candidates)]
            if tp in self.poll_raises_retention_lost:
                raise RetentionLostError(
                    f"{tp}: required offset {self._positions.get(tp)} is below "
                    f"log start offset {self._cluster.log(tp).log_start_offset}"
                )
            log = self._cluster.log(tp)
            position = self._positions.get(tp, log.log_start_offset)
            if position < log.log_start_offset:
                # The position the group needs has been deleted by retention.
                raise RetentionLostError(
                    f"{tp}: required offset {position} is below log start offset "
                    f"{log.log_start_offset}; the data is gone from Kafka"
                )
            # Index directly rather than scanning. Offsets are contiguous
            # within `records` after retention trimming, so the position maps
            # to an index. A linear scan here would make every poll O(log
            # size) and the whole harness quadratic, which would silently
            # dominate any throughput measurement taken against it.
            index = position - log.log_start_offset
            if index < 0:  # pragma: no cover - guarded above
                index = 0
            while index < len(log.records):
                record = log.records[index]
                index += 1
                self._positions[tp] = record.offset + 1
                if record.is_control:
                    continue  # occupies an offset, never delivered
                self._round_robin = (self._round_robin + i + 1) % max(
                    1, len(candidates)
                )
                return ConsumedRecord(
                    tp=tp,
                    offset=record.offset,
                    key=record.key,
                    value=record.value,
                    headers=record.headers,
                    timestamp_ms=record.timestamp_ms,
                )
        return None

    # ---------------------------------------------------------------- commit

    def commit_offset(self, tp: TopicPartition, next_offset: int) -> None:
        self.commit_calls.append((tp, next_offset))
        fault = self.commit_fault
        if fault is not CommitFault.NONE:
            if self.commit_fault_remaining > 0:
                self.commit_fault_remaining -= 1
                if self.commit_fault_remaining == 0:
                    self.commit_fault = CommitFault.NONE
            if fault is CommitFault.REJECT:
                raise CommitNotConfirmedError(f"{tp}: commit rejected by broker")
            if fault is CommitFault.UNREACHABLE:
                raise CommitNotConfirmedError(f"{tp}: broker unreachable")
            if fault is CommitFault.LOST_RESPONSE:
                # Applied server-side, but the caller never finds out.
                self._cluster.set_committed(self._group_id, tp, next_offset)
                raise CommitNotConfirmedError(
                    f"{tp}: commit response lost; the committed position is unknown"
                )
        self._cluster.set_committed(self._group_id, tp, next_offset)

    def committed_next(self, tp: TopicPartition) -> int:
        committed = self._cluster.committed(self._group_id, tp)
        if committed is None:
            return self._cluster.log(tp).log_start_offset
        return committed

    def watermarks(self, tp: TopicPartition) -> tuple[int, int]:
        log = self._cluster.log(tp)
        return log.log_start_offset, log.high_watermark

    # ----------------------------------------------------------- flow control

    def pause(self, tps: Sequence[TopicPartition]) -> None:
        tps = tuple(tps)
        if tps:
            self.pause_calls.append(tps)
        self._paused.update(tps)

    def resume(self, tps: Sequence[TopicPartition]) -> None:
        tps = tuple(tps)
        if tps:
            self.resume_calls.append(tps)
        self._paused.difference_update(tps)

    @property
    def paused(self) -> frozenset[TopicPartition]:
        return frozenset(self._paused)

    def assignment(self) -> list[TopicPartition]:
        return list(self._assigned)

    def position(self, tp: TopicPartition) -> int:
        return self._positions.get(tp, 0)

    def close(self) -> None:
        self.closed = True
