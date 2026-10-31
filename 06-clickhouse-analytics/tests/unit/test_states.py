"""Aggregate-state algebra: the monoid laws the read path depends on (group I).

The claim "a ``*Merge`` read is correct whether or not background merges have
run" is only true because the states are associative and commutative. These tests
check that directly, because the alternative is for it to be true by luck until
someone adds a sixth state column that is not.
"""

from __future__ import annotations

import itertools
import random

import pytest

from _coverage import covers
from md_analytics.contracts import SourceOrder
from md_analytics.errors import ContractError
from md_analytics.oracle import Bar, reference_bars
from md_analytics.store.base import (
    ArgMaxState,
    ArgMinState,
    BarState,
    BarStateRow,
    bar_states_from_block,
    merge_state_rows,
    states_to_bars,
)
from tests.unit.test_oracle import NOON, tick  # noqa: F401 -- shared tick factory


def state(order_ns: int, sequence: int, price: int) -> BarState:
    return BarState.from_tick(SourceOrder(order_ns, sequence), price)


class TestArgStates:
    @covers("P06-I-08")
    def test_argmin_keeps_the_earliest_order(self):
        a = ArgMinState(SourceOrder(10, 1), 100)
        b = ArgMinState(SourceOrder(10, 2), 200)
        assert a.merge(b) == a
        assert b.merge(a) == a, "commutative"

    @covers("P06-I-08")
    def test_argmax_keeps_the_latest_order(self):
        a = ArgMaxState(SourceOrder(10, 1), 100)
        b = ArgMaxState(SourceOrder(10, 2), 200)
        assert a.merge(b) == b
        assert b.merge(a) == b

    @covers("P06-I-04")
    def test_identical_orders_with_different_values_is_an_error(self):
        a = ArgMinState(SourceOrder(10, 1), 100)
        b = ArgMinState(SourceOrder(10, 1), 101)
        with pytest.raises(ContractError) as exc:
            a.merge(b)
        assert exc.value.code == "ARGMIN_STATE_COLLISION"

    def test_identical_orders_with_identical_values_merge_cleanly(self):
        a = ArgMaxState(SourceOrder(10, 1), 100)
        assert a.merge(a) == a


class TestMonoidLaws:
    @pytest.fixture
    def states(self):
        rng = random.Random(11)
        return [
            state(NOON + rng.randrange(0, 60_000_000_000), i + 1, rng.randrange(1, 1_000))
            for i in range(6)
        ]

    @covers("P06-I-01", "P06-I-09")
    def test_merge_is_associative(self, states):
        a, b, c = states[0], states[1], states[2]
        assert a.merge(b).merge(c) == a.merge(b.merge(c))

    @covers("P06-I-01", "P06-I-09")
    def test_merge_is_commutative(self, states):
        a, b = states[0], states[1]
        assert a.merge(b) == b.merge(a)

    @covers("P06-I-09", "P06-I-12")
    def test_every_merge_order_finalizes_identically(self, states):
        results = set()
        for ordering in itertools.permutations(states):
            merged = ordering[0]
            for item in ordering[1:]:
                merged = merged.merge(item)
            results.add(merged.finalize())
        assert len(results) == 1, "merge order must not affect the finalized bar"

    def test_count_state_sums(self, states):
        merged = states[0]
        for item in states[1:]:
            merged = merged.merge(item)
        assert merged.count_state == len(states)


class TestMaterializedViewBlocks:
    @covers("P06-I-01")
    def test_one_block_yields_one_state_row_per_bar(self):
        block = [tick(1, 0, 100), tick(2, 1_000_000_000, 200)]
        rows = bar_states_from_block(block)
        assert len(rows) == 1, "both ticks are in the same minute"
        assert rows[0].state.count_state == 2

    @covers("P06-I-01", "P06-I-07")
    def test_two_blocks_covering_one_minute_yield_two_state_rows(self):
        rows = bar_states_from_block([tick(1, 0, 100)]) + bar_states_from_block(
            [tick(2, 1_000_000_000, 200)]
        )
        assert len(rows) == 2, "the MV appends; it does not update the earlier row"
        merged = states_to_bars(merge_state_rows(rows))
        assert only(merged) == Bar(100, 200, 100, 200, 2)

    @covers("P06-I-01", "P06-L-09")
    def test_reading_without_merge_would_see_several_rows_for_one_minute(self):
        rows = [
            r
            for t in (tick(1, 0, 100), tick(2, 1_000_000_000, 200), tick(3, 2_000_000_000, 150))
            for r in bar_states_from_block([t])
        ]
        distinct_keys = {row.key for row in rows}
        assert len(rows) == 3 and len(distinct_keys) == 1, (
            "three physical state rows for one logical bar: this is exactly why a "
            "reader must group and use *Merge"
        )
        assert len(merge_state_rows(rows)) == 1

    @covers("P06-I-07")
    def test_replaying_the_same_block_double_counts_and_nothing_undoes_it(self):
        block = [tick(1, 0, 100)]
        rows = bar_states_from_block(block) + bar_states_from_block(block)
        merged = states_to_bars(merge_state_rows(rows))
        assert only(merged).tick_count == 2, (
            "an aggregate state cannot be subtracted: preventing the duplicate "
            "insert is the builder's job, not the store's"
        )

    @covers("P06-H-05")
    def test_states_match_the_independent_oracle(self):
        ticks = [tick(i + 1, i * 7_000_000_000, 100_000 + (i % 11) * 13) for i in range(40)]
        rows = [r for t in ticks for r in bar_states_from_block([t])]
        assert states_to_bars(merge_state_rows(rows)) == reference_bars(ticks)

    @covers("P06-I-10")
    def test_group_by_keys_come_from_the_rows_not_a_constant(self):
        rows = bar_states_from_block([tick(1, 0, 100, symbol="EURUSD"), tick(1, 0, 900, symbol="USDJPY")])
        assert {row.key.symbol for row in rows} == {"EURUSD", "USDJPY"}


def only(mapping):
    assert len(mapping) == 1, f"expected one entry, got {len(mapping)}"
    return next(iter(mapping.values()))


class TestStateRowIdentity:
    def test_state_rows_are_comparable_and_sortable_for_evidence(self):
        rows = sorted(
            bar_states_from_block([tick(2, 60_000_000_000, 1), tick(1, 0, 2)]),
            key=lambda r: r.key,
        )
        assert [r.key.bar_start_ns for r in rows] == [NOON, NOON + 60_000_000_000]
        assert isinstance(rows[0], BarStateRow)
