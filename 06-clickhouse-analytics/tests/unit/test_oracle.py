"""The independent OHLC oracle (group H, and group AD for oracle integrity)."""

from __future__ import annotations

import itertools
import random
from datetime import date

import pytest

from _coverage import covers
from md_analytics.contracts import NS_PER_MINUTE
from md_analytics.errors import ContractError
from md_analytics.oracle import (
    Bar,
    BarKey,
    bars_in_range,
    reference_bars,
    reference_bars_sorted,
    total_tick_count,
)
from md_analytics.rows import CanonicalTick

NOON = 1_793_361_600_000_000_000
BUILD = "build-1"
DATASET = "DS-1"


def tick(sequence: int, offset_ns: int, price: int, *, symbol="EURUSD", scale=100_000, dataset=DATASET):
    ns = NOON + offset_ns
    return CanonicalTick(
        build_id=BUILD,
        dataset_id=dataset,
        symbol=symbol,
        sequence=sequence,
        event_time_ns=ns,
        event_date=date(2026, 10, 30),
        price_scale=scale,
        price_ticks=price,
        payload_digest="0" * 64,
        source_schema_version="marketdata.v1",
    )


def only_bar(bars: dict[BarKey, Bar]) -> Bar:
    assert len(bars) == 1, f"expected one bar, got {len(bars)}"
    return next(iter(bars.values()))


class TestOHLCSemantics:
    @covers("P06-H-10")
    def test_single_tick_minute_has_all_four_prices_equal(self):
        bar = only_bar(reference_bars([tick(1, 0, 100_500)]))
        assert bar == Bar(100_500, 100_500, 100_500, 100_500, 1)

    @covers("P06-H-01", "P06-H-02")
    def test_open_and_close_follow_event_time_not_arrival(self):
        ticks = [tick(i + 1, i * 1_000_000_000, 100 + i) for i in range(5)]
        forward = only_bar(reference_bars(ticks))
        reversed_arrival = only_bar(reference_bars(list(reversed(ticks))))
        assert forward == reversed_arrival
        assert forward.open_ticks == 100, "earliest event time"
        assert forward.close_ticks == 104, "latest event time"

    @covers("P06-H-11", "P06-I-08")
    def test_equal_timestamps_break_the_tie_on_sequence(self):
        a = tick(3, 30_000_000_000, 100_200)
        b = tick(4, 30_000_000_000, 99_900)
        for ordering in itertools.permutations([a, b]):
            bar = only_bar(reference_bars(list(ordering)))
            assert bar.open_ticks == 100_200, "sequence 3 opens"
            assert bar.close_ticks == 99_900, "sequence 4 closes, despite the lower price"

    @covers("P06-H-12")
    def test_extrema_come_from_prices_not_from_time_order(self):
        ticks = [
            tick(1, 0, 100),
            tick(2, 1_000_000_000, 200),
            tick(3, 2_000_000_000, 50),
            tick(4, 3_000_000_000, 150),
        ]
        bar = only_bar(reference_bars(ticks))
        assert (bar.open_ticks, bar.high_ticks, bar.low_ticks, bar.close_ticks) == (
            100,
            200,
            50,
            150,
        )

    @covers("P06-H-04")
    def test_low_is_the_minimum_across_a_random_permutation(self):
        rng = random.Random(20261030)
        prices = rng.sample(range(90_000, 110_000), 100)
        ticks = [tick(i + 1, i * 100_000_000, p) for i, p in enumerate(prices)]
        rng.shuffle(ticks)
        bar = only_bar(reference_bars(ticks))
        assert bar.low_ticks == min(prices)
        assert bar.high_ticks == max(prices)

    @covers("P06-H-14")
    def test_constant_price_still_counts_every_update(self):
        ticks = [tick(i + 1, i * 10_000_000, 100_000) for i in range(1_000)]
        bar = only_bar(reference_bars(ticks))
        assert bar == Bar(100_000, 100_000, 100_000, 100_000, 1_000)

    @covers("P06-H-09")
    def test_an_empty_minute_produces_no_bar(self):
        bars = reference_bars([tick(1, 0, 100), tick(2, 2 * NS_PER_MINUTE, 200)])
        starts = sorted(key.bar_start_ns for key in bars)
        assert starts == [NOON, NOON + 2 * NS_PER_MINUTE]
        assert NOON + NS_PER_MINUTE not in starts, "no synthetic fill-forward bar"

    @covers("P06-H-07")
    def test_datasets_sharing_a_symbol_stay_separate(self):
        bars = reference_bars(
            [tick(1, 0, 100, dataset="DS-A"), tick(1, 0, 200, dataset="DS-B")]
        )
        assert len(bars) == 2
        assert {b.open_ticks for b in bars.values()} == {100, 200}

    @covers("P06-H-08", "P06-C-03", "P06-C-11")
    def test_price_scales_never_blend_into_one_bar(self):
        bars = reference_bars([tick(1, 0, 100_000, scale=100_000), tick(2, 1, 1_000, scale=1_000)])
        assert len(bars) == 2
        assert {key.price_scale for key in bars} == {1_000, 100_000}

    @covers("P06-C-11")
    def test_symbols_are_never_aggregated_together(self):
        bars = reference_bars([tick(1, 0, 100, symbol="EURUSD"), tick(1, 0, 900, symbol="USDJPY")])
        assert len(bars) == 2

    @covers("P06-H-06")
    def test_a_bar_has_no_volume_field(self):
        assert "volume" not in Bar._fields
        assert "tick_count" in Bar._fields


class TestOracleIntegrity:
    @covers("P06-AD-01", "P06-AD-02")
    def test_the_two_independent_implementations_agree(self):
        rng = random.Random(7)
        ticks = [
            tick(
                i + 1,
                rng.randrange(0, 5 * NS_PER_MINUTE),
                rng.randrange(80_000, 120_000),
            )
            for i in range(400)
        ]
        # Distinct sequences guarantee a total order, so both must agree exactly.
        assert reference_bars(ticks) == reference_bars_sorted(ticks)

    @covers("P06-I-04")
    def test_an_order_key_collision_stops_rather_than_resolving(self):
        a = tick(5, 0, 100)
        b = tick(5, 0, 200)  # same identity key AND same order: not a clean input
        for oracle in (reference_bars, reference_bars_sorted):
            with pytest.raises(ContractError) as exc:
                oracle([a, b])
            assert exc.value.code == "ORDER_KEY_COLLISION"

    @covers("P06-C-02")
    def test_non_positive_scale_is_refused_by_the_oracle_too(self):
        with pytest.raises(ContractError):
            reference_bars([tick(1, 0, 100, scale=0)])

    def test_bounds_check_catches_an_impossible_bar(self):
        with pytest.raises(ContractError):
            Bar(open_ticks=50, high_ticks=40, low_ticks=30, close_ticks=35, tick_count=2).check_bounds()

    def test_total_tick_count_sums_every_bar(self):
        ticks = [tick(i + 1, i * NS_PER_MINUTE, 100 + i) for i in range(5)]
        assert total_tick_count(reference_bars(ticks)) == 5


class TestScopedSlice:
    @covers("P06-R-02")
    def test_range_slice_is_half_open_and_ordered(self):
        bars = reference_bars([tick(i + 1, i * NS_PER_MINUTE, 100 + i) for i in range(5)])
        selected = bars_in_range(
            bars,
            build_id=BUILD,
            dataset_id=DATASET,
            symbol="EURUSD",
            start_ns=NOON + NS_PER_MINUTE,
            end_ns=NOON + 3 * NS_PER_MINUTE,
        )
        assert [key.bar_start_ns for key, _ in selected] == [
            NOON + NS_PER_MINUTE,
            NOON + 2 * NS_PER_MINUTE,
        ]

    @covers("P06-R-05")
    def test_range_slice_never_leaks_another_dataset(self):
        bars = reference_bars([tick(1, 0, 100, dataset="DS-A"), tick(1, 0, 200, dataset="DS-B")])
        selected = bars_in_range(
            bars,
            build_id=BUILD,
            dataset_id="DS-A",
            symbol="EURUSD",
            start_ns=NOON,
            end_ns=NOON + NS_PER_MINUTE,
        )
        assert len(selected) == 1
        assert selected[0][1].open_ticks == 100
