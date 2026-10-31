"""The contiguous proven frontier: I-01, I-03 and restart behaviour."""

from __future__ import annotations

import pytest

from p05_ingest.commit_coordinator import CommitCoordinator
from p05_ingest.errors import InvariantViolation
from p05_ingest.model import RecordDisposition, TopicPartition

TP = TopicPartition("md.ticks.v1", 0)
ACK = RecordDisposition.SINK_ACKED
QUARANTINED = RecordDisposition.QUARANTINED
BLOCKED = RecordDisposition.BLOCKED


@pytest.fixture
def cc() -> CommitCoordinator:
    coordinator = CommitCoordinator()
    coordinator.start_partition(TP, committed_next=0, epoch=1)
    return coordinator


# ------------------------------------------------------------------ baseline


def test_frontier_starts_at_the_committed_offset_not_zero():
    """Spec I-01 note: a verified checkpoint is honoured, not re-derived.

    Starting from 0 would re-read the whole partition and imply every earlier
    commit was unproven.
    """
    cc = CommitCoordinator()
    cc.start_partition(TP, committed_next=5000, epoch=1)
    assert cc.proven_frontier_next(TP) == 5000
    assert cc.committed_next(TP) == 5000
    assert cc.commit_candidate(TP) is None, "no new progress to commit yet"


def test_negative_committed_offset_is_refused():
    """An unknown position must be resolved, never assumed to be zero."""
    cc = CommitCoordinator()
    with pytest.raises(InvariantViolation, match="negative"):
        cc.start_partition(TP, committed_next=-1, epoch=1)


def test_untracked_partition_raises(cc):
    other = TopicPartition("md.quotes.v1", 3)
    with pytest.raises(InvariantViolation, match="not tracked"):
        cc.note_consumed(other, 0)


# ------------------------------------------------------------- I-03 prefix


def test_frontier_does_not_pass_an_unfinished_record(cc):
    """The core of I-03: a hole blocks, even with later records acknowledged."""
    for offset in (0, 1, 2):
        cc.note_consumed(TP, offset)
    cc.mark(TP, 0, ACK)
    cc.mark(TP, 2, ACK)  # offset 1 deliberately left pending

    assert cc.advance_frontier(TP, epoch=1) == 1
    assert cc.first_unfinished_offset(TP) == 1
    assert cc.commit_candidate(TP) == 1, "only offset 0 is proven"

    cc.mark(TP, 1, ACK)
    assert cc.advance_frontier(TP, epoch=1) == 3, "the hole closed; 0..2 now proven"


def test_blocked_record_pins_the_frontier_permanently(cc):
    for offset in (0, 1, 2):
        cc.note_consumed(TP, offset)
    cc.mark(TP, 0, ACK)
    cc.mark(TP, 1, BLOCKED)
    cc.mark(TP, 2, ACK)

    assert cc.advance_frontier(TP, epoch=1) == 1
    assert cc.is_blocked(TP)
    # Repeated attempts must not creep forward.
    for _ in range(5):
        assert cc.advance_frontier(TP, epoch=1) == 1


def test_quarantined_record_advances_the_frontier(cc):
    """A durable DLQ ACK is a terminal result, so the prefix may close (I-07)."""
    cc.note_consumed(TP, 0)
    cc.note_consumed(TP, 1)
    cc.mark(TP, 0, QUARANTINED)
    cc.mark(TP, 1, ACK)
    assert cc.advance_frontier(TP, epoch=1) == 2


# ---------------------------------------------------- non-contiguous offsets


def test_offset_gaps_are_not_holes(cc):
    """Spec 4.3: control records consume offset space and are never delivered.

    The frontier walks *consumed records*, so offsets 0, 5, 9 form a complete
    prefix and the committed next offset is 10.
    """
    for offset in (0, 5, 9):
        cc.note_consumed(TP, offset)
        cc.mark(TP, offset, ACK)
    assert cc.advance_frontier(TP, epoch=1) == 10
    assert cc.commit_candidate(TP) == 10


# ----------------------------------------------------------- I-01 hard gate


def test_commit_beyond_the_frontier_raises(cc):
    cc.note_consumed(TP, 0)
    cc.note_consumed(TP, 1)
    cc.mark(TP, 0, ACK)
    cc.advance_frontier(TP, epoch=1)

    with pytest.raises(InvariantViolation, match="I-01 VIOLATION"):
        cc.assert_commit_safe(TP, 2)


def test_commit_at_the_frontier_is_allowed(cc):
    cc.note_consumed(TP, 0)
    cc.mark(TP, 0, ACK)
    cc.advance_frontier(TP, epoch=1)
    cc.assert_commit_safe(TP, 1)
    cc.note_committed(TP, 1)
    assert cc.committed_next(TP) == 1


def test_commit_cannot_move_backwards(cc):
    cc.note_consumed(TP, 0)
    cc.note_consumed(TP, 1)
    cc.mark(TP, 0, ACK)
    cc.mark(TP, 1, ACK)
    cc.advance_frontier(TP, epoch=1)
    cc.note_committed(TP, 2)
    with pytest.raises(InvariantViolation, match="behind the already committed"):
        cc.assert_commit_safe(TP, 1)


def test_note_committed_itself_enforces_the_gate(cc):
    """The gate is not bypassable by calling note_committed directly."""
    cc.note_consumed(TP, 0)
    with pytest.raises(InvariantViolation, match="I-01 VIOLATION"):
        cc.note_committed(TP, 1)


def test_commit_candidate_is_none_without_new_progress(cc):
    cc.note_consumed(TP, 0)
    cc.mark(TP, 0, ACK)
    cc.advance_frontier(TP, epoch=1)
    assert cc.commit_candidate(TP) == 1
    cc.note_committed(TP, 1)
    assert cc.commit_candidate(TP) is None, "no redundant commit RPC"


def test_unknown_commit_leaves_the_position_unchanged(cc):
    """Spec 5.3: an unconfirmed commit is neither success nor failure."""
    cc.note_consumed(TP, 0)
    cc.mark(TP, 0, ACK)
    cc.advance_frontier(TP, epoch=1)
    before = cc.committed_next(TP)
    cc.note_commit_unknown(TP)
    assert cc.committed_next(TP) == before
    assert cc.commit_candidate(TP) == 1, "it will be retried"


# ----------------------------------------------------------- ledger hygiene


def test_out_of_order_consumption_is_refused(cc):
    cc.note_consumed(TP, 5)
    with pytest.raises(InvariantViolation, match="does not advance"):
        cc.note_consumed(TP, 4)


def test_re_reading_below_the_frontier_is_refused(cc):
    cc.note_consumed(TP, 0)
    cc.mark(TP, 0, ACK)
    cc.advance_frontier(TP, epoch=1)
    with pytest.raises(InvariantViolation, match="below the proven frontier"):
        cc.note_consumed(TP, 0)


def test_result_for_an_unconsumed_record_is_refused(cc):
    with pytest.raises(InvariantViolation, match="never recorded as consumed"):
        cc.mark(TP, 99, ACK)


def test_duplicate_ack_below_the_frontier_is_ignored(cc):
    """A late duplicate callback must not count as new progress."""
    cc.note_consumed(TP, 0)
    cc.mark(TP, 0, ACK)
    cc.advance_frontier(TP, epoch=1)
    cc.mark(TP, 0, ACK)  # arrives again, already folded in
    assert cc.proven_frontier_next(TP) == 1


def test_changing_a_terminal_disposition_is_refused(cc):
    cc.note_consumed(TP, 0)
    cc.mark(TP, 0, ACK)
    with pytest.raises(InvariantViolation, match="must be final"):
        cc.mark(TP, 0, BLOCKED)


def test_repeating_the_same_disposition_is_idempotent(cc):
    cc.note_consumed(TP, 0)
    cc.mark(TP, 0, ACK)
    cc.mark(TP, 0, ACK)
    assert cc.pending_count(TP) == 1


def test_pending_list_is_trimmed_as_the_frontier_advances(cc):
    """Memory must be bounded by the in-flight window, not the topic size."""
    for offset in range(1000):
        cc.note_consumed(TP, offset)
        cc.mark(TP, offset, ACK)
        cc.advance_frontier(TP, epoch=1)
    assert cc.pending_count(TP) == 0
    assert cc.proven_frontier_next(TP) == 1000


def test_mark_batch_marks_every_offset(cc):
    offsets = (0, 1, 2, 3)
    for offset in offsets:
        cc.note_consumed(TP, offset)
    cc.mark_batch(TP, offsets, ACK)
    assert cc.advance_frontier(TP, epoch=1) == 4


# ----------------------------------------------------------------- restart


def test_reassignment_resets_the_ledger_to_the_broker_position(cc):
    """A new epoch may only advance on sink proof belonging to that epoch."""
    cc.note_consumed(TP, 0)
    cc.note_consumed(TP, 1)
    cc.mark(TP, 0, ACK)
    cc.advance_frontier(TP, epoch=1)
    cc.note_committed(TP, 1)

    cc.forget_partition(TP)
    cc.start_partition(TP, committed_next=1, epoch=2)

    assert cc.proven_frontier_next(TP) == 1
    assert cc.pending_count(TP) == 0, "offset 1 was never committed and is re-read"
    cc.note_consumed(TP, 1)
    assert cc.commit_candidate(TP) is None


def test_snapshot_reports_frontier_and_committed(cc):
    cc.note_consumed(TP, 0)
    cc.mark(TP, 0, ACK)
    cc.advance_frontier(TP, epoch=1)
    snap = cc.snapshot()[TP]
    assert snap["frontier_next"] == 1
    assert snap["committed_next"] == 0
    assert snap["blocked"] is False
