"""Partition-local FIFO batch assembly.

Spec section 5.1. The rules, and why each one is load-bearing:

  * **one partition per batch.** Mixing partitions would make the committed
    offset of one partition depend on the sink result of another, destroying
    the per-partition contiguous-frontier argument.
  * **FIFO, in consumption order.** Rows are appended to a list and sealed into
    a tuple. No dict, no set, no sort -- the insert must replay the order the
    consumer read (spec 5.1: do not use an unordered container to hold row
    sequence).
  * **seal on the first threshold reached** -- rows, bytes, age, or an external
    event (revoke/shutdown).
  * **age is monotonic, not poll-driven.** An idle partition must still flush
    on its timer, so the clock is injected and read with a monotonic source.
    An empty poll never means the dataset ended (spec I-08).
  * **at most one in-flight batch per partition.** A second batch may be
    *built* while the first is in flight, but it cannot be handed to a worker
    until the first reaches a terminal state.

A sealed batch is a frozen ``SealedBatch``; the builder keeps no reference that
would let a retry see different bytes (spec I-04).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from .canonical_digest import batch_id as make_batch_id
from .canonical_digest import batch_insert_token
from .config import BatchConfig, ClickHouseConfig
from .errors import InvariantViolation
from .model import EventRow, SealedBatch, SealReason, TopicPartition


@dataclass(slots=True)
class _OpenBatch:
    """Rows accumulated for one partition but not yet sealed."""

    tp: TopicPartition
    assignment_epoch: int
    rows: list[EventRow] = field(default_factory=list)
    accounted_bytes: int = 0
    opened_at_monotonic: float = 0.0

    def __len__(self) -> int:
        return len(self.rows)


class PartitionBatcher:
    """Accumulates rows per topic-partition and seals immutable batches."""

    def __init__(
        self,
        batch_cfg: BatchConfig,
        clickhouse_cfg: ClickHouseConfig,
        *,
        topic_generation: str,
        monotonic: Callable[[], float],
    ) -> None:
        self._cfg = batch_cfg
        self._ch = clickhouse_cfg
        self._topic_generation = topic_generation
        self._monotonic = monotonic
        self._open: dict[TopicPartition, _OpenBatch] = {}

    # ----------------------------------------------------------------- append

    def append(self, tp: TopicPartition, assignment_epoch: int, row: EventRow) -> None:
        """Add one row to the open batch for ``tp``.

        Rejects a row whose offset does not advance: duplicated or out-of-order
        delivery inside a single partition would corrupt the FIFO assumption,
        and silently accepting it would make the sealed offset range a lie.
        """
        open_batch = self._open.get(tp)
        if open_batch is None:
            open_batch = _OpenBatch(
                tp=tp,
                assignment_epoch=assignment_epoch,
                opened_at_monotonic=self._monotonic(),
            )
            self._open[tp] = open_batch
        elif open_batch.assignment_epoch != assignment_epoch:
            raise InvariantViolation(
                f"{tp}: cannot append epoch {assignment_epoch} row to a batch "
                f"opened in epoch {open_batch.assignment_epoch}; the partition "
                "changed hands while the batch was open"
            )

        if open_batch.rows and row.coordinate.offset <= open_batch.rows[-1].coordinate.offset:
            raise InvariantViolation(
                f"{tp}: offset {row.coordinate.offset} does not advance past "
                f"{open_batch.rows[-1].coordinate.offset}; batches are strict FIFO"
            )
        if row.coordinate.topic != tp.topic or row.coordinate.partition != tp.partition:
            raise InvariantViolation(
                f"row from {row.coordinate.topic}[{row.coordinate.partition}] "
                f"appended to the batch for {tp}"
            )

        open_batch.rows.append(row)
        open_batch.accounted_bytes += row.accounted_bytes

    # ------------------------------------------------------------------- seal

    def seal_reason(self, tp: TopicPartition) -> SealReason | None:
        """Why ``tp`` should seal now, or None if it should keep accumulating."""
        open_batch = self._open.get(tp)
        if open_batch is None or not open_batch.rows:
            return None
        if len(open_batch.rows) >= self._cfg.per_partition_max_rows:
            return SealReason.MAX_ROWS
        if open_batch.accounted_bytes >= self._cfg.per_partition_max_serialized_bytes:
            return SealReason.MAX_BYTES
        age_ms = (self._monotonic() - open_batch.opened_at_monotonic) * 1000.0
        if age_ms >= self._cfg.max_wait_ms:
            return SealReason.MAX_WAIT
        return None

    def seal(self, tp: TopicPartition, reason: SealReason) -> SealedBatch | None:
        """Freeze the open batch for ``tp``. Returns None when it is empty.

        The insert token is derived from the content, so a retry of this batch
        reproduces it exactly while a differently-grouped batch covering the
        same offsets does not collide with it.
        """
        open_batch = self._open.pop(tp, None)
        if open_batch is None or not open_batch.rows:
            return None

        rows = tuple(open_batch.rows)
        token = batch_insert_token(
            topic_generation=self._topic_generation,
            topic=tp.topic,
            partition=tp.partition,
            coordinates_and_digests=tuple(
                (r.coordinate.offset, r.payload_digest) for r in rows
            ),
            table_fqn=self._ch.table_fqn,
            schema_version=self._ch.schema_version,
            insert_mode=self._ch.insert_mode,
        )
        return SealedBatch(
            tp=tp,
            assignment_epoch=open_batch.assignment_epoch,
            rows=rows,
            batch_id=make_batch_id(
                topic=tp.topic, partition=tp.partition, insert_token=token
            ),
            insert_token=token,
            seal_reason=reason,
            accounted_bytes=open_batch.accounted_bytes,
            sealed_at_monotonic=self._monotonic(),
        )

    def seal_if_ready(self, tp: TopicPartition) -> SealedBatch | None:
        reason = self.seal_reason(tp)
        if reason is None:
            return None
        return self.seal(tp, reason)

    def seal_all(self, reason: SealReason) -> list[SealedBatch]:
        """Seal every open partition, oldest-opened first (drain/shutdown)."""
        order = sorted(self._open, key=lambda tp: (self._open[tp].opened_at_monotonic, tp))
        sealed = []
        for tp in order:
            batch = self.seal(tp, reason)
            if batch is not None:
                sealed.append(batch)
        return sealed

    # ------------------------------------------------------------------ state

    def discard(self, tp: TopicPartition) -> int:
        """Throw away the open batch for ``tp``; returns the bytes it held.

        Used on ``on_lost``: those rows were never sealed, so no sink request
        exists for them and the new owner will re-read them from Kafka.
        """
        open_batch = self._open.pop(tp, None)
        return 0 if open_batch is None else open_batch.accounted_bytes

    def open_rows(self, tp: TopicPartition) -> int:
        open_batch = self._open.get(tp)
        return 0 if open_batch is None else len(open_batch.rows)

    def open_bytes(self, tp: TopicPartition) -> int:
        open_batch = self._open.get(tp)
        return 0 if open_batch is None else open_batch.accounted_bytes

    def has_open(self, tp: TopicPartition) -> bool:
        return bool(self._open.get(tp) and self._open[tp].rows)

    @property
    def open_partitions(self) -> frozenset[TopicPartition]:
        return frozenset(tp for tp, b in self._open.items() if b.rows)

    def oldest_open_age_s(self, tp: TopicPartition) -> float:
        open_batch = self._open.get(tp)
        if open_batch is None or not open_batch.rows:
            return 0.0
        return self._monotonic() - open_batch.opened_at_monotonic
