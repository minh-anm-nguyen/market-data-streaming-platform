"""Assignment epochs and the ownership fence (I-02)."""

from __future__ import annotations

from p05_ingest.assignment_manager import AssignmentManager
from p05_ingest.model import PartitionState, TopicPartition

TP0 = TopicPartition("md.ticks.v1", 0)
TP1 = TopicPartition("md.ticks.v1", 1)


def test_assignment_grants_increasing_epochs():
    am = AssignmentManager()
    first = am.assign([TP0, TP1])
    assert sorted(first.values()) == [1, 2]
    assert am.epoch_of(TP0) == first[TP0]
    assert am.state_of(TP0) is PartitionState.ASSIGNED


def test_reassignment_allocates_a_fresh_epoch():
    """Reusing the old epoch would let a pre-rebalance completion pass.

    That completion was computed against work the new assignment has not
    re-read, so it must not be able to advance the frontier.
    """
    am = AssignmentManager()
    old_epoch = am.assign([TP0])[TP0]
    am.lose([TP0])
    new_epoch = am.assign([TP0])[TP0]
    assert new_epoch > old_epoch
    assert not am.is_current(TP0, old_epoch)
    assert am.is_current(TP0, new_epoch)


def test_unassigned_partition_has_no_epoch():
    am = AssignmentManager()
    assert am.epoch_of(TP0) is None
    assert am.state_of(TP0) is PartitionState.UNASSIGNED
    assert not am.is_current(TP0, 1)
    assert not am.may_commit(TP0, 1)


def test_cooperative_revoke_still_permits_committing_a_proven_prefix():
    """Spec 4.3: a revoke has a drain window; a loss does not."""
    am = AssignmentManager()
    epoch = am.assign([TP0])[TP0]
    am.revoke([TP0])
    assert am.state_of(TP0) is PartitionState.REVOKING
    assert am.may_commit(TP0, epoch), "the drain window allows a proven commit"

    am.complete_revoke([TP0])
    assert not am.may_commit(TP0, epoch)
    assert am.epoch_of(TP0) is None


def test_lost_ownership_forbids_committing_immediately():
    am = AssignmentManager()
    epoch = am.assign([TP0])[TP0]
    am.lose([TP0])
    assert not am.may_commit(TP0, epoch)
    assert am.epoch_of(TP0) is None


def test_blocked_partition_may_not_commit():
    """Its prefix is not provably complete, so progress must not be claimed."""
    am = AssignmentManager()
    epoch = am.assign([TP0])[TP0]
    am.block(TP0)
    assert am.state_of(TP0) is PartitionState.BLOCKED
    assert not am.may_commit(TP0, epoch)


def test_paused_and_inflight_partitions_may_still_commit():
    am = AssignmentManager()
    epoch = am.assign([TP0])[TP0]
    for state in (PartitionState.PAUSED, PartitionState.INFLIGHT):
        am.set_state(TP0, state)
        assert am.may_commit(TP0, epoch), state


def test_stale_epoch_is_fenced_even_while_owned():
    am = AssignmentManager()
    epoch = am.assign([TP0])[TP0]
    assert not am.may_commit(TP0, epoch - 1)
    assert not am.may_commit(TP0, epoch + 1)


def test_owned_for_fetch_excludes_blocked_and_revoking():
    am = AssignmentManager()
    am.assign([TP0, TP1])
    am.block(TP0)
    assert am.owned_for_fetch == frozenset({TP1})
    am.revoke([TP1])
    assert am.owned_for_fetch == frozenset()
    assert am.assigned == frozenset({TP0, TP1})


def test_snapshot_reports_epoch_and_state():
    am = AssignmentManager()
    epoch = am.assign([TP0])[TP0]
    assert am.snapshot() == {TP0: (epoch, PartitionState.ASSIGNED)}


def test_operations_on_unknown_partitions_are_harmless():
    am = AssignmentManager()
    am.revoke([TP0])
    am.complete_revoke([TP0])
    am.lose([TP0])
    am.block(TP0)
    am.set_state(TP0, PartitionState.PAUSED)
    assert am.assigned == frozenset()
