"""Budget invariants under random reserve/release/pause sequences."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from p05_ingest.budget_manager import BudgetManager
from p05_ingest.model import TopicPartition

pytestmark = pytest.mark.property

TPS = [
    TopicPartition("md.ticks.v1", 0),
    TopicPartition("md.ticks.v1", 1),
    TopicPartition("md.quotes.v1", 0),
]
ASSIGNED = frozenset(TPS)


@given(
    budget=st.integers(min_value=100, max_value=10_000),
    requests=st.lists(
        st.tuples(st.sampled_from(TPS), st.integers(min_value=0, max_value=3000)),
        max_size=60,
    ),
)
@settings(max_examples=400, deadline=None)
def test_reserved_never_exceeds_the_budget(budget, requests):
    """I-05 for the accounted bytes, under any admission sequence."""
    manager = BudgetManager(budget_bytes=budget)
    for tp, nbytes in requests:
        manager.try_reserve(tp, nbytes)
        assert manager.reserved_bytes <= budget
        assert manager.reserved_bytes >= 0


@given(
    budget=st.integers(min_value=100, max_value=10_000),
    operations=st.lists(
        st.tuples(
            st.sampled_from(["reserve", "release", "release_all"]),
            st.sampled_from(TPS),
            st.integers(min_value=0, max_value=3000),
        ),
        max_size=80,
    ),
)
@settings(max_examples=400, deadline=None)
def test_per_partition_sums_always_equal_the_global_total(budget, operations):
    """A drift between the two would silently leak or invent capacity."""
    manager = BudgetManager(budget_bytes=budget)
    for op, tp, nbytes in operations:
        if op == "reserve":
            manager.try_reserve(tp, nbytes)
        elif op == "release":
            manager.release(tp, nbytes)
        else:
            manager.release_all(tp)
        assert sum(manager.reserved_for(t) for t in TPS) == manager.reserved_bytes


@given(
    budget=st.integers(min_value=1000, max_value=10_000),
    operations=st.lists(
        st.tuples(
            st.booleans(), st.sampled_from(TPS), st.integers(min_value=1, max_value=900)
        ),
        max_size=60,
    ),
)
@settings(max_examples=300, deadline=None)
def test_pause_and_resume_respect_the_watermarks(budget, operations):
    manager = BudgetManager(budget_bytes=budget, high_watermark=0.8, low_watermark=0.5)
    for reserve, tp, nbytes in operations:
        if reserve:
            manager.try_reserve(tp, nbytes)
        else:
            manager.release(tp, nbytes)

        paused = manager.partitions_to_pause(ASSIGNED)
        if paused:
            assert manager.utilisation >= 0.8, "paused below the high watermark"
        resumed = manager.partitions_to_resume()
        if resumed:
            assert manager.utilisation <= 0.5, "resumed above the low watermark"


class BudgetMachine(RuleBasedStateMachine):
    """Shadow model: an independent tally kept alongside the real manager."""

    def __init__(self) -> None:
        super().__init__()
        self.manager = BudgetManager(
            budget_bytes=5000, high_watermark=0.8, low_watermark=0.5
        )
        self.shadow: dict[TopicPartition, int] = {tp: 0 for tp in TPS}

    @rule(tp=st.sampled_from(TPS), nbytes=st.integers(min_value=0, max_value=2000))
    def reserve(self, tp, nbytes):
        before = sum(self.shadow.values())
        granted = self.manager.try_reserve(tp, nbytes)
        if granted:
            assert before + nbytes <= 5000
            self.shadow[tp] += nbytes
        else:
            assert before + nbytes > 5000

    @rule(tp=st.sampled_from(TPS), nbytes=st.integers(min_value=0, max_value=2000))
    def release(self, tp, nbytes):
        self.manager.release(tp, nbytes)
        self.shadow[tp] = max(0, self.shadow[tp] - nbytes)

    @rule(tp=st.sampled_from(TPS))
    def release_all(self, tp):
        released = self.manager.release_all(tp)
        assert released == self.shadow[tp]
        self.shadow[tp] = 0

    @rule()
    def flow_control(self):
        self.manager.partitions_to_pause(ASSIGNED)
        self.manager.partitions_to_resume()

    @invariant()
    def matches_the_shadow_model(self):
        assert self.manager.reserved_bytes == sum(self.shadow.values())
        for tp in TPS:
            assert self.manager.reserved_for(tp) == self.shadow[tp]

    @invariant()
    def stays_within_the_budget(self):
        assert 0 <= self.manager.reserved_bytes <= 5000


TestBudgetMachine = BudgetMachine.TestCase
TestBudgetMachine.settings = settings(
    max_examples=80, stateful_step_count=40, deadline=None
)
