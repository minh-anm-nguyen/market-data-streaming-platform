"""Frontier invariants under randomly generated schedules.

Spec 9.3 asks for random sequences of fetch / ACK / timeout / rebalance /
commit and the assertion that no commit ever runs ahead of the sink proof.
These tests generate the schedules directly against the coordinator, which is
where the invariant lives; ``test_state_machine_model`` then does the same
against the whole engine.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from p05_ingest.commit_coordinator import CommitCoordinator
from p05_ingest.errors import InvariantViolation
from p05_ingest.model import RecordDisposition, TopicPartition

pytestmark = pytest.mark.property

TP = TopicPartition("md.ticks.v1", 0)

DISPOSITIONS = [
    RecordDisposition.SINK_ACKED,
    RecordDisposition.QUARANTINED,
    RecordDisposition.BLOCKED,
]

#: Offsets that need not be contiguous: control records burn offset space.
offset_sequences = st.lists(
    st.integers(min_value=1, max_value=4), min_size=1, max_size=25
).map(lambda gaps: _cumulative(gaps))


def _cumulative(gaps: list[int]) -> list[int]:
    offsets: list[int] = []
    current = 0
    for gap in gaps:
        current += gap
        offsets.append(current)
    return offsets


@given(
    offsets=offset_sequences,
    outcomes=st.lists(st.sampled_from(DISPOSITIONS + [None]), min_size=1, max_size=25),
    baseline=st.integers(min_value=0, max_value=50),
)
@settings(max_examples=400, deadline=None)
def test_frontier_never_passes_an_unfinished_or_blocked_record(
    offsets, outcomes, baseline
):
    """I-03, over arbitrary completion orders."""
    cc = CommitCoordinator()
    cc.start_partition(TP, committed_next=baseline, epoch=1)
    offsets = [o + baseline for o in offsets]

    for offset in offsets:
        cc.note_consumed(TP, offset)

    # Apply results in a shuffled-but-deterministic order derived from the
    # generated outcome list, so completions arrive out of order.
    pairs = list(zip(offsets, outcomes, strict=False))
    for offset, disposition in reversed(pairs):
        if disposition is not None:
            cc.mark(TP, offset, disposition)

    frontier = cc.advance_frontier(TP, epoch=1)

    # Compute the expected frontier independently: walk the prefix and stop at
    # the first record with no terminal result or a blocked one.
    expected = baseline
    for offset, disposition in pairs:
        if disposition is None or disposition is RecordDisposition.BLOCKED:
            break
        expected = offset + 1
    assert frontier == expected


@given(
    offsets=offset_sequences,
    ack_mask=st.lists(st.booleans(), min_size=1, max_size=25),
)
@settings(max_examples=400, deadline=None)
def test_commit_candidate_is_always_at_or_below_the_frontier(offsets, ack_mask):
    """I-01, over arbitrary acknowledgement patterns."""
    cc = CommitCoordinator()
    cc.start_partition(TP, committed_next=0, epoch=1)
    for offset in offsets:
        cc.note_consumed(TP, offset)
    for offset, acked in zip(offsets, ack_mask, strict=False):
        if acked:
            cc.mark(TP, offset, RecordDisposition.SINK_ACKED)

    cc.advance_frontier(TP, epoch=1)
    candidate = cc.commit_candidate(TP)
    frontier = cc.proven_frontier_next(TP)

    if candidate is not None:
        assert candidate <= frontier
        cc.assert_commit_safe(TP, candidate)
        cc.note_committed(TP, candidate)
        assert cc.committed_next(TP) <= cc.proven_frontier_next(TP)


@given(
    offsets=offset_sequences,
    over_by=st.integers(min_value=1, max_value=100),
)
@settings(max_examples=200, deadline=None)
def test_any_commit_above_the_frontier_is_always_refused(offsets, over_by):
    cc = CommitCoordinator()
    cc.start_partition(TP, committed_next=0, epoch=1)
    for offset in offsets:
        cc.note_consumed(TP, offset)
    cc.mark(TP, offsets[0], RecordDisposition.SINK_ACKED)
    frontier = cc.advance_frontier(TP, epoch=1)

    with pytest.raises(InvariantViolation, match="I-01 VIOLATION"):
        cc.assert_commit_safe(TP, frontier + over_by)


@given(
    offsets=offset_sequences,
    acked_count=st.integers(min_value=0, max_value=25),
)
@settings(max_examples=300, deadline=None)
def test_committed_offset_is_monotonic_across_repeated_advances(offsets, acked_count):
    cc = CommitCoordinator()
    cc.start_partition(TP, committed_next=0, epoch=1)
    for offset in offsets:
        cc.note_consumed(TP, offset)

    committed_history = []
    for i, offset in enumerate(offsets):
        if i >= acked_count:
            break
        cc.mark(TP, offset, RecordDisposition.SINK_ACKED)
        cc.advance_frontier(TP, epoch=1)
        candidate = cc.commit_candidate(TP)
        if candidate is not None:
            cc.note_committed(TP, candidate)
            committed_history.append(candidate)

    assert committed_history == sorted(committed_history)
    assert len(set(committed_history)) == len(committed_history), "no re-commits"


@given(
    offsets=offset_sequences,
    restart_after=st.integers(min_value=0, max_value=25),
)
@settings(max_examples=300, deadline=None)
def test_restart_resumes_at_the_committed_offset_and_loses_nothing(
    offsets, restart_after
):
    """A restart may re-read, but must never skip past an uncommitted record."""
    cc = CommitCoordinator()
    cc.start_partition(TP, committed_next=0, epoch=1)

    processed = []
    for i, offset in enumerate(offsets):
        if i >= restart_after:
            break
        cc.note_consumed(TP, offset)
        cc.mark(TP, offset, RecordDisposition.SINK_ACKED)
        cc.advance_frontier(TP, epoch=1)
        candidate = cc.commit_candidate(TP)
        if candidate is not None:
            cc.note_committed(TP, candidate)
        processed.append(offset)

    committed = cc.committed_next(TP)

    # Restart: a fresh coordinator anchored at the broker position.
    cc2 = CommitCoordinator()
    cc2.start_partition(TP, committed_next=committed, epoch=2)

    # Every offset at or above the committed position is still readable, so
    # nothing was skipped.
    remaining = [o for o in offsets if o >= committed]
    for offset in remaining:
        cc2.note_consumed(TP, offset)

    skipped = [o for o in offsets if o not in processed and o < committed]
    assert skipped == [], f"records {skipped} were committed past without a result"
