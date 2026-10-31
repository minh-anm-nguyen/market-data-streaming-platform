"""Reference OHLC: tie-breaks, exclusions, and the things it refuses to invent."""

from __future__ import annotations

import pytest

from p07.contracts import BAR_WIDTH_NS, EventKind, SemanticCore
from p07.oracle.reference_bars import (
    BarKey,
    PriceScaleConflict,
    bar_start_ns,
    diff_bars,
    reference_bars,
    reference_bars_from_identity_map,
)


def tick(sequence: int, event_time_ns: int, price: int, *, symbol: str = "EURUSD", scale: int = 100000) -> SemanticCore:
    return SemanticCore(
        dataset_id="ds",
        symbol=symbol,
        sequence=sequence,
        event_time_ns=event_time_ns,
        price_scale=scale,
        event_kind=EventKind.TICK,
        source_schema_version="sim.v1",
        bid_ticks=None,
        ask_ticks=None,
        price_ticks=price,
    )


def quote(sequence: int, event_time_ns: int, bid: int, ask: int) -> SemanticCore:
    return SemanticCore(
        dataset_id="ds",
        symbol="EURUSD",
        sequence=sequence,
        event_time_ns=event_time_ns,
        price_scale=100000,
        event_kind=EventKind.QUOTE,
        source_schema_version="sim.v1",
        bid_ticks=bid,
        ask_ticks=ask,
        price_ticks=None,
    )


@pytest.mark.whatif("P07-G18-01")
def test_open_high_low_close_and_count() -> None:
    bars = reference_bars([tick(1, 0, 100), tick(2, 1, 130), tick(3, 2, 90), tick(4, 3, 110)])
    assert len(bars) == 1
    bar = next(iter(bars.values()))
    assert (bar.open_ticks, bar.high_ticks, bar.low_ticks, bar.close_ticks) == (100, 130, 90, 110)
    assert bar.tick_count == 4


def test_input_order_does_not_matter() -> None:
    events = [tick(1, 0, 100), tick(2, 1, 130), tick(3, 2, 90), tick(4, 3, 110)]
    assert reference_bars(events) == reference_bars(list(reversed(events)))


@pytest.mark.whatif("P07-G18-03")
def test_same_nanosecond_ticks_tie_break_on_sequence() -> None:
    """Open and close must be deterministic when timestamps collide."""
    events = [tick(7, 1000, 500), tick(5, 1000, 100), tick(6, 1000, 300)]
    bar = next(iter(reference_bars(events).values()))
    assert bar.open_ticks == 100  # sequence 5
    assert bar.close_ticks == 500  # sequence 7
    assert bar.tick_count == 3


@pytest.mark.whatif("P07-G18-02")
def test_quotes_never_enter_a_price_bar() -> None:
    bars = reference_bars([quote(1, 0, 90, 95), tick(2, 1, 100)])
    bar = next(iter(bars.values()))
    assert bar.tick_count == 1
    assert bar.open_ticks == bar.close_ticks == 100


def test_quotes_alone_produce_no_bars() -> None:
    assert reference_bars([quote(1, 0, 90, 95), quote(2, 1, 91, 96)]) == {}


@pytest.mark.whatif("P07-G18-05")
def test_a_minute_with_no_ticks_produces_no_row() -> None:
    """Fill-forward is undefined in P06's MVP; inventing a bar invents a price."""
    bars = reference_bars([tick(1, 0, 100), tick(2, 2 * BAR_WIDTH_NS, 200)])
    starts = sorted(key.bar_start_ns for key in bars)
    assert starts == [0, 2 * BAR_WIDTH_NS]
    assert len(bars) == 2


def test_bars_split_at_the_minute_boundary() -> None:
    bars = reference_bars([tick(1, BAR_WIDTH_NS - 1, 100), tick(2, BAR_WIDTH_NS, 200)])
    assert len(bars) == 2
    assert sorted(key.bar_start_ns for key in bars) == [0, BAR_WIDTH_NS]


def test_symbols_and_scales_are_separate_bars() -> None:
    bars = reference_bars(
        [tick(1, 0, 100), tick(1, 0, 150, symbol="USDJPY", scale=1000)]
    )
    assert len(bars) == 2
    assert {key.symbol for key in bars} == {"EURUSD", "USDJPY"}


@pytest.mark.whatif("P07-G18-07")
def test_mixed_price_scale_for_one_symbol_is_a_conflict() -> None:
    """P06 section 3: STOP/CONFLICT, never a silent rescale."""
    with pytest.raises(PriceScaleConflict) as exc:
        reference_bars([tick(1, 0, 100), tick(2, 1, 100, scale=1000)])
    assert exc.value.symbol == "EURUSD"
    assert set(exc.value.scales) == {100000, 1000}


@pytest.mark.whatif("P07-G18-04", "P07-G15-05")
def test_identity_keyed_input_cannot_double_count() -> None:
    """A physical duplicate reduced by identity before aggregation (``INV-09``)."""
    first = tick(1, 0, 100)
    duplicate = tick(1, 0, 100)
    by_identity = {first.identity: first}
    by_identity[duplicate.identity] = duplicate  # same key, replaces
    bars = reference_bars_from_identity_map(by_identity)
    assert next(iter(bars.values())).tick_count == 1


def test_accepted_tick_without_a_price_is_refused() -> None:
    """The oracle will not substitute a zero for an absent price."""
    broken = tick(1, 0, 100)._replace(price_ticks=None)
    with pytest.raises(ValueError):
        reference_bars([broken])


def test_bar_start_helper_matches_the_core_property() -> None:
    for value in (-BAR_WIDTH_NS - 5, -1, 0, 1, BAR_WIDTH_NS + 5):
        assert bar_start_ns(value) == tick(1, value, 100).bar_start_ns


def test_diff_reports_missing_extra_and_field_level_mismatch() -> None:
    expected = reference_bars([tick(1, 0, 100), tick(2, BAR_WIDTH_NS, 200)])
    observed = dict(expected)
    key = BarKey("ds", "EURUSD", 100000, 0)
    observed[key] = observed[key]._replace(close_ticks=999, tick_count=5)
    extra_key = BarKey("ds", "GBPUSD", 100000, 0)
    observed[extra_key] = expected[key]._replace(key=extra_key)
    del observed[BarKey("ds", "EURUSD", 100000, BAR_WIDTH_NS)]

    delta = diff_bars(expected, observed)
    assert len(delta["missing"]) == 1
    assert len(delta["extra"]) == 1
    assert len(delta["mismatched"]) == 1
    assert set(delta["mismatched"][0]["fields"]) == {"close_ticks", "tick_count"}


def test_diff_of_equal_maps_is_empty() -> None:
    bars = reference_bars([tick(1, 0, 100)])
    assert diff_bars(bars, dict(bars)) == {"missing": [], "extra": [], "mismatched": []}
