"""Byte budget: the cap, and pause/resume hysteresis."""

from __future__ import annotations

import pytest

from p05_ingest.budget_manager import BudgetManager
from p05_ingest.model import TopicPartition

TP0 = TopicPartition("md.ticks.v1", 0)
TP1 = TopicPartition("md.ticks.v1", 1)
TP2 = TopicPartition("md.quotes.v1", 0)


@pytest.fixture
def budget() -> BudgetManager:
    return BudgetManager(budget_bytes=1000, high_watermark=0.8, low_watermark=0.5)


# ----------------------------------------------------------------- the cap


def test_reservations_accumulate(budget):
    assert budget.try_reserve(TP0, 300)
    assert budget.try_reserve(TP1, 200)
    assert budget.reserved_bytes == 500
    assert budget.reserved_for(TP0) == 300
    assert budget.reserved_for(TP1) == 200


def test_reservation_at_exactly_the_budget_is_allowed(budget):
    assert budget.try_reserve(TP0, 1000)
    assert budget.utilisation == 1.0


def test_reservation_past_the_budget_is_refused(budget):
    assert budget.try_reserve(TP0, 900)
    assert not budget.try_reserve(TP1, 101)
    assert budget.reserved_bytes == 900, "a refused reservation charges nothing"


def test_a_single_row_larger_than_the_budget_can_never_be_admitted(budget):
    """Spec 5.4: fail the record, never raise the cap."""
    assert not budget.try_reserve(TP0, 1001)
    assert budget.reserved_bytes == 0


def test_release_frees_capacity(budget):
    budget.try_reserve(TP0, 800)
    budget.release(TP0, 800)
    assert budget.reserved_bytes == 0
    assert budget.try_reserve(TP1, 1000)


def test_over_release_is_clamped_rather_than_creating_budget(budget):
    """Releasing more than was held would invent capacity out of nothing."""
    budget.try_reserve(TP0, 100)
    budget.release(TP0, 500)
    assert budget.reserved_bytes == 0
    assert budget.reserved_for(TP0) == 0


def test_release_all_drops_one_partition_only(budget):
    budget.try_reserve(TP0, 400)
    budget.try_reserve(TP1, 300)
    released = budget.release_all(TP0)
    assert released == 400
    assert budget.reserved_bytes == 300
    assert budget.reserved_for(TP1) == 300


def test_negative_amounts_are_refused(budget):
    with pytest.raises(ValueError):
        budget.try_reserve(TP0, -1)
    with pytest.raises(ValueError):
        budget.release(TP0, -1)


def test_zero_byte_reservation_is_allowed(budget):
    assert budget.try_reserve(TP0, 0)
    assert budget.reserved_bytes == 0


# -------------------------------------------------------------- hysteresis


def test_watermarks_must_leave_a_gap():
    """Equal watermarks would flap between pause and resume every iteration."""
    with pytest.raises(ValueError, match="hysteresis"):
        BudgetManager(budget_bytes=1000, high_watermark=0.5, low_watermark=0.5)
    with pytest.raises(ValueError, match="hysteresis"):
        BudgetManager(budget_bytes=1000, high_watermark=0.4, low_watermark=0.6)


def test_pause_fires_at_the_high_watermark(budget):
    assigned = frozenset({TP0, TP1})
    budget.try_reserve(TP0, 700)
    assert budget.partitions_to_pause(assigned) == []

    budget.try_reserve(TP1, 150)  # 850/1000 = 85%
    assert budget.should_pause()
    assert set(budget.partitions_to_pause(assigned)) == {TP0, TP1}


def test_pause_targets_the_hottest_partition_first(budget):
    assigned = frozenset({TP0, TP1, TP2})
    budget.try_reserve(TP0, 100)
    budget.try_reserve(TP1, 600)
    budget.try_reserve(TP2, 200)
    assert budget.partitions_to_pause(assigned) == [TP1, TP2, TP0]


def test_idle_partition_is_not_paused(budget):
    """Pausing a partition holding nothing cannot relieve pressure."""
    assigned = frozenset({TP0, TP1})
    budget.try_reserve(TP0, 900)
    assert budget.partitions_to_pause(assigned) == [TP0]
    assert TP1 not in budget.paused


def test_resume_waits_for_the_low_watermark(budget):
    assigned = frozenset({TP0})
    budget.try_reserve(TP0, 900)
    budget.partitions_to_pause(assigned)
    assert budget.paused == frozenset({TP0})

    # Still above the low mark: no resume, which is the whole point of
    # hysteresis -- resuming at 79% would re-pause immediately.
    budget.release(TP0, 200)  # 700/1000 = 70%
    assert budget.partitions_to_resume() == []
    assert budget.paused == frozenset({TP0})

    budget.release(TP0, 250)  # 450/1000 = 45%
    assert budget.partitions_to_resume() == [TP0]
    assert budget.paused == frozenset()


def test_no_pause_or_resume_churn_when_nothing_changed(budget):
    assigned = frozenset({TP0})
    budget.try_reserve(TP0, 850)
    assert budget.partitions_to_pause(assigned) == [TP0]
    assert budget.partitions_to_pause(assigned) == [], "already paused"
    assert budget.partitions_to_resume() == [], "still above the low mark"


def test_release_all_clears_the_paused_flag(budget):
    assigned = frozenset({TP0})
    budget.try_reserve(TP0, 900)
    budget.partitions_to_pause(assigned)
    budget.release_all(TP0)
    assert budget.paused == frozenset()


def test_snapshot_reports_the_full_state(budget):
    budget.try_reserve(TP0, 900)
    budget.partitions_to_pause(frozenset({TP0}))
    snap = budget.snapshot()
    assert snap.reserved_bytes == 900
    assert snap.budget_bytes == 1000
    assert snap.available_bytes == 100
    assert snap.utilisation == pytest.approx(0.9)
    assert snap.paused == frozenset({TP0})


def test_rejects_a_non_positive_budget():
    with pytest.raises(ValueError):
        BudgetManager(budget_bytes=0)
