"""Assignment epochs and the ownership fence.

Spec I-02 and section 4.3. Every partition carries a monotonically increasing
*epoch*, bumped on each assignment. Work handed to a sink worker records the
epoch it was created under, and a completion whose epoch no longer matches is
refused the right to commit.

The honest limit, worth stating in the code because it is easy to forget: an
epoch fences **this program's commit**, nothing else. A ClickHouse INSERT that
was already dispatched can still land after the partition has moved to another
consumer. The fence prevents a stale owner from *claiming* that write as
committed progress; it cannot retract the write. Duplicates arising this way
are found later by logical reconciliation, not prevented here.

``revoke`` and ``lose`` are deliberately different:

  * ``revoke`` is cooperative. The partition is still owned for a short drain
    window, so an already-proven prefix may still be committed.
  * ``lose`` means ownership is already gone. No commit is permitted at all,
    and the run is marked incomplete for that partition.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .model import PartitionState, TopicPartition


@dataclass(slots=True)
class _Assignment:
    epoch: int
    state: PartitionState


@dataclass(slots=True)
class AssignmentManager:
    """Tracks which partitions this consumer owns, and under which epoch."""

    _assignments: dict[TopicPartition, _Assignment] = field(default_factory=dict)
    _next_epoch: int = 1

    # -------------------------------------------------------------- lifecycle

    def assign(self, tps: list[TopicPartition] | tuple[TopicPartition, ...]) -> dict[TopicPartition, int]:
        """Take ownership, allocating a fresh epoch per partition.

        A partition re-assigned to this same consumer gets a *new* epoch, not
        its old one. Reusing the epoch would let a completion from before the
        rebalance pass the fence, and that completion was computed against a
        batch the new assignment has not re-read.
        """
        granted: dict[TopicPartition, int] = {}
        for tp in tps:
            epoch = self._next_epoch
            self._next_epoch += 1
            self._assignments[tp] = _Assignment(epoch=epoch, state=PartitionState.ASSIGNED)
            granted[tp] = epoch
        return granted

    def revoke(self, tps: list[TopicPartition] | tuple[TopicPartition, ...]) -> None:
        """Cooperative revoke: mark for drain but keep the epoch valid."""
        for tp in tps:
            assignment = self._assignments.get(tp)
            if assignment is not None:
                assignment.state = PartitionState.REVOKING

    def complete_revoke(self, tps: list[TopicPartition] | tuple[TopicPartition, ...]) -> None:
        """Drain window closed; ownership released."""
        for tp in tps:
            assignment = self._assignments.pop(tp, None)
            if assignment is not None:
                assignment.state = PartitionState.REVOKED

    def lose(self, tps: list[TopicPartition] | tuple[TopicPartition, ...]) -> None:
        """Ownership gone without a drain window. Commits are now forbidden."""
        for tp in tps:
            self._assignments.pop(tp, None)

    def block(self, tp: TopicPartition) -> None:
        """Partition stopped on a fatal condition; it keeps its epoch so that
        in-flight completions can still be matched and logged."""
        assignment = self._assignments.get(tp)
        if assignment is not None:
            assignment.state = PartitionState.BLOCKED

    def set_state(self, tp: TopicPartition, state: PartitionState) -> None:
        assignment = self._assignments.get(tp)
        if assignment is not None:
            assignment.state = state

    # ----------------------------------------------------------------- checks

    def epoch_of(self, tp: TopicPartition) -> int | None:
        assignment = self._assignments.get(tp)
        return None if assignment is None else assignment.epoch

    def state_of(self, tp: TopicPartition) -> PartitionState:
        assignment = self._assignments.get(tp)
        return PartitionState.UNASSIGNED if assignment is None else assignment.state

    def is_current(self, tp: TopicPartition, epoch: int) -> bool:
        """True when ``epoch`` is still the live epoch for ``tp``."""
        assignment = self._assignments.get(tp)
        return assignment is not None and assignment.epoch == epoch

    def may_commit(self, tp: TopicPartition, epoch: int) -> bool:
        """Commit permission for a completion reported under ``epoch``.

        Allowed while owned and while draining a cooperative revoke; refused
        once ownership is gone, and refused for a blocked partition because its
        prefix is not provably complete.
        """
        assignment = self._assignments.get(tp)
        if assignment is None or assignment.epoch != epoch:
            return False
        return assignment.state in (
            PartitionState.ASSIGNED,
            PartitionState.PAUSED,
            PartitionState.INFLIGHT,
            PartitionState.REVOKING,
        )

    @property
    def assigned(self) -> frozenset[TopicPartition]:
        return frozenset(self._assignments)

    @property
    def owned_for_fetch(self) -> frozenset[TopicPartition]:
        """Partitions that may still accept new records."""
        return frozenset(
            tp
            for tp, a in self._assignments.items()
            if a.state in (PartitionState.ASSIGNED, PartitionState.PAUSED, PartitionState.INFLIGHT)
        )

    def snapshot(self) -> dict[TopicPartition, tuple[int, PartitionState]]:
        return {tp: (a.epoch, a.state) for tp, a in self._assignments.items()}
