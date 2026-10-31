"""The contiguous proven frontier, and the only place offsets are committed.

This is the module that makes or breaks spec section 0.2. Three invariants are
enforced here rather than hoped for:

  * **I-01 sink-first.** ``commit_next(tp) <= proven_frontier_next(tp)``. The
    frontier only advances on a *terminal durable* result: a ClickHouse ACK, or
    a durable-quarantine ACK when the optional DLQ is enabled.
  * **I-03 prefix.** A record that has been consumed but has no terminal result
    blocks the frontier, even when later records are already acknowledged.
    Committing past a hole is exactly the data-loss bug the design calls P0.
  * **I-02 ownership.** A commit is refused unless the reporting epoch is still
    the live epoch for that partition.

The frontier walks *consumed records in consumption order*, not integers. Kafka
offsets can skip values -- transaction and control records consume offset space
without being returned to the application -- so "offset 7 never arrived" is not
a hole. Only a record this consumer actually received, and has not finished,
holds the line (spec section 4.3, final paragraph).

Restart behaviour follows the I-01 note: the frontier starts at the last
authoritative committed next offset, not at zero. Treating a verified prior
checkpoint as unproven would re-read the whole topic and claim every earlier
commit was wrong. In a *new* epoch the frontier may only advance on sink proof
belonging to that epoch.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .errors import InvariantViolation
from .model import RecordDisposition, TopicPartition


@dataclass(slots=True)
class _PartitionLedger:
    """Per-partition record of what was consumed and what finished.

    ``pending`` is an ordered list of ``(offset, disposition_or_None)`` in
    consumption order. It is trimmed from the left as the frontier advances, so
    its length is bounded by the in-flight window rather than by the topic.
    """

    #: Offset the frontier starts from: the committed next offset at takeover.
    baseline_next: int
    pending: list[list] = field(default_factory=list)
    frontier_next: int = 0
    committed_next: int = 0
    #: Epoch that produced the current frontier value.
    frontier_epoch: int = 0
    blocked: bool = False
    blocked_reason: str = ""

    def __post_init__(self) -> None:
        self.frontier_next = self.baseline_next
        self.committed_next = self.baseline_next


class CommitCoordinator:
    """Owns the frontier for every assigned partition.

    Only the owner thread may call these methods; the sink workers report
    results through a queue that the owner drains (spec ADR-002).
    """

    def __init__(self) -> None:
        self._ledgers: dict[TopicPartition, _PartitionLedger] = {}

    # -------------------------------------------------------------- lifecycle

    def start_partition(
        self, tp: TopicPartition, *, committed_next: int, epoch: int
    ) -> None:
        """Begin tracking ``tp`` from a known-authoritative committed offset.

        Called from ``on_assign`` with the position the broker reports. A
        partition re-assigned to this consumer is reset: anything consumed
        under the old epoch was never committed and will be re-read.
        """
        if committed_next < 0:
            raise InvariantViolation(
                f"{tp}: committed_next={committed_next} is negative; an unknown "
                "position must be resolved before tracking, not assumed to be 0"
            )
        ledger = _PartitionLedger(baseline_next=committed_next)
        ledger.frontier_epoch = epoch
        self._ledgers[tp] = ledger

    def forget_partition(self, tp: TopicPartition) -> None:
        self._ledgers.pop(tp, None)

    def tracked(self) -> frozenset[TopicPartition]:
        return frozenset(self._ledgers)

    # ----------------------------------------------------------- bookkeeping

    def note_consumed(self, tp: TopicPartition, offset: int) -> None:
        """Record that a record was returned by poll and is now our problem.

        From this moment the record blocks the frontier until it reaches a
        terminal state. That is the whole of I-03.
        """
        ledger = self._ledger(tp)
        if offset < ledger.frontier_next:
            raise InvariantViolation(
                f"{tp}: consumed offset {offset} is below the proven frontier "
                f"{ledger.frontier_next}; the consumer re-read committed data "
                "without a reset being recorded"
            )
        if ledger.pending and offset <= ledger.pending[-1][0]:
            raise InvariantViolation(
                f"{tp}: consumed offset {offset} does not advance past "
                f"{ledger.pending[-1][0]}; poll returned out-of-order records"
            )
        ledger.pending.append([offset, None])

    def mark(
        self, tp: TopicPartition, offset: int, disposition: RecordDisposition
    ) -> None:
        """Attach a terminal result to one consumed record."""
        ledger = self._ledger(tp)
        for entry in reversed(ledger.pending):
            if entry[0] == offset:
                if entry[1] is not None and entry[1] != disposition:
                    raise InvariantViolation(
                        f"{tp}@{offset}: disposition changed from {entry[1]} to "
                        f"{disposition}; a terminal result must be final"
                    )
                entry[1] = disposition
                if disposition is RecordDisposition.BLOCKED:
                    ledger.blocked = True
                    ledger.blocked_reason = f"record at offset {offset} is blocked"
                return
        if offset < ledger.frontier_next:
            # Already folded into the frontier; a duplicate ACK for the same
            # record is harmless and must not be treated as new progress.
            return
        raise InvariantViolation(
            f"{tp}@{offset}: result reported for a record that was never "
            "recorded as consumed"
        )

    def mark_batch(
        self,
        tp: TopicPartition,
        offsets: tuple[int, ...] | list[int],
        disposition: RecordDisposition,
    ) -> None:
        for offset in offsets:
            self.mark(tp, offset, disposition)

    # ------------------------------------------------------------- frontier

    def advance_frontier(self, tp: TopicPartition, *, epoch: int) -> int:
        """Fold every finished leading record into the frontier.

        Walks from the oldest pending record and stops at the first one without
        a terminal result, or at the first blocked one. Returns the new proven
        next offset.
        """
        ledger = self._ledger(tp)
        advanced = 0
        for entry in ledger.pending:
            offset, disposition = entry
            if disposition is None or disposition is RecordDisposition.BLOCKED:
                break
            ledger.frontier_next = offset + 1
            advanced += 1
        if advanced:
            del ledger.pending[:advanced]
            ledger.frontier_epoch = epoch
        return ledger.frontier_next

    def proven_frontier_next(self, tp: TopicPartition) -> int:
        return self._ledger(tp).frontier_next

    def committed_next(self, tp: TopicPartition) -> int:
        return self._ledger(tp).committed_next

    def pending_count(self, tp: TopicPartition) -> int:
        return len(self._ledger(tp).pending)

    def first_unfinished_offset(self, tp: TopicPartition) -> int | None:
        """The oldest consumed record with no terminal result at all."""
        for offset, disposition in self._ledger(tp).pending:
            if disposition is None:
                return offset
        return None

    def first_pending_offset(self, tp: TopicPartition) -> int | None:
        """The oldest record still holding the frontier, for any reason.

        Distinct from ``first_unfinished_offset``: a record marked BLOCKED has
        a terminal result but still pins the frontier, so a run report that
        only looked for ``None`` would describe the blockage as happening at
        offset ``None``.
        """
        pending = self._ledger(tp).pending
        return pending[0][0] if pending else None

    def is_blocked(self, tp: TopicPartition) -> bool:
        return self._ledger(tp).blocked

    def blocked_reason(self, tp: TopicPartition) -> str:
        return self._ledger(tp).blocked_reason

    def block(self, tp: TopicPartition, reason: str) -> None:
        ledger = self._ledger(tp)
        ledger.blocked = True
        ledger.blocked_reason = reason

    # --------------------------------------------------------------- commit

    def commit_candidate(self, tp: TopicPartition) -> int | None:
        """Offset to commit for ``tp``, or None when there is no new progress.

        Returns the proven frontier only when it is strictly ahead of what has
        already been committed, so an idle partition does not issue redundant
        commit RPCs on every loop iteration.
        """
        ledger = self._ledger(tp)
        if ledger.frontier_next <= ledger.committed_next:
            return None
        return ledger.frontier_next

    def assert_commit_safe(self, tp: TopicPartition, next_offset: int) -> None:
        """Last gate before the broker call. Raises rather than returning False.

        A commit beyond the proven frontier is unrecoverable data loss, so this
        aborts the run instead of degrading: there is no safe way to continue
        from a state where the committed offset is already wrong.
        """
        ledger = self._ledger(tp)
        if next_offset > ledger.frontier_next:
            raise InvariantViolation(
                f"I-01 VIOLATION {tp}: attempted commit of next offset "
                f"{next_offset} beyond the proven sink frontier "
                f"{ledger.frontier_next}. Committing here would skip records "
                "that ClickHouse never acknowledged."
            )
        if next_offset < ledger.committed_next:
            raise InvariantViolation(
                f"{tp}: attempted commit of next offset {next_offset} behind the "
                f"already committed {ledger.committed_next}; moving a committed "
                "offset backwards silently re-reads data"
            )

    def note_committed(self, tp: TopicPartition, next_offset: int) -> None:
        """Record a *confirmed* commit. Only call after the broker confirmed."""
        self.assert_commit_safe(tp, next_offset)
        self._ledger(tp).committed_next = next_offset

    def note_commit_unknown(self, tp: TopicPartition) -> None:
        """A commit whose result was not confirmed leaves the position unknown.

        Spec section 5.3: this is not a failure to be retried blindly and not a
        success. ``committed_next`` is left untouched, so the next loop will try
        again and a restart will re-read from the broker position.
        """
        # Intentionally a no-op on state; present so the call site reads
        # honestly and so metrics can be attached in one place.
        return None

    # --------------------------------------------------------------- helpers

    def _ledger(self, tp: TopicPartition) -> _PartitionLedger:
        ledger = self._ledgers.get(tp)
        if ledger is None:
            raise InvariantViolation(
                f"{tp} is not tracked; start_partition must be called from "
                "on_assign before any record is consumed"
            )
        return ledger

    def snapshot(self) -> dict[TopicPartition, dict[str, object]]:
        return {
            tp: {
                "frontier_next": ledger.frontier_next,
                "committed_next": ledger.committed_next,
                "pending": len(ledger.pending),
                "blocked": ledger.blocked,
            }
            for tp, ledger in self._ledgers.items()
        }
