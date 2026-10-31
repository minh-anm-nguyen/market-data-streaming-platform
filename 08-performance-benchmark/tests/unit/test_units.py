"""Unit tagging (INV-12, sections 1.2 and 13).

The test that matters most is the last one in :class:`TestTickVersusEventRate`:
the 120k/150k substitution of section 1.2 has to be impossible to perform by
accident, and the conversion has to demand a measured mix rather than assume
4:1.
"""

from __future__ import annotations

import pytest

from p08_bench.errors import UnitError
from p08_bench.units import (
    GB,
    GIB,
    MB,
    MIB,
    Quantity,
    Unit,
    bytes_per_s,
    events_per_s,
    milliseconds,
    rate,
    records_per_s,
    seconds,
    ticks_per_s,
)


class TestUnitArithmetic:
    def test_same_unit_addition(self):
        assert (ticks_per_s(1000) + ticks_per_s(200)).value == 1200

    def test_cross_unit_addition_is_refused(self):
        with pytest.raises(UnitError, match="different units"):
            ticks_per_s(120_000) + events_per_s(30_000)

    def test_cross_unit_subtraction_is_refused(self):
        # Section 5.2's "delta backlog rate" PromQL subtracts an issued tick rate
        # from a canonical verified rate; both must carry the same unit first.
        with pytest.raises(UnitError, match="different units"):
            records_per_s(5) - ticks_per_s(5)

    def test_cross_unit_comparison_is_refused(self):
        with pytest.raises(UnitError, match="compare"):
            ticks_per_s(120_000).exceeds(events_per_s(120_000))

    def test_scaling_preserves_unit(self):
        scaled = ticks_per_s(1000).scaled(1.5)
        assert scaled.unit is Unit.TICKS_PER_S
        assert scaled.value == 1500

    def test_ratio_requires_same_unit(self):
        assert ticks_per_s(50).ratio_to(ticks_per_s(100)) == 0.5
        with pytest.raises(UnitError):
            ticks_per_s(50).ratio_to(events_per_s(100))

    def test_ratio_by_zero_is_an_error_not_an_infinity(self):
        with pytest.raises(UnitError, match="denominator is zero"):
            ticks_per_s(1).ratio_to(ticks_per_s(0))

    def test_nan_is_refused_at_construction(self):
        with pytest.raises(UnitError, match="NaN"):
            Quantity(float("nan"), Unit.TICKS_PER_S)

    def test_quantity_is_frozen(self):
        # A relabelable quantity is how events/s becomes ticks/s after the fact.
        quantity = ticks_per_s(1)
        with pytest.raises(Exception):
            quantity.unit = Unit.EVENTS_PER_S  # type: ignore[misc]


class TestTickVersusEventRate:
    def test_tick_rate_converts_only_with_a_measured_mix(self):
        # Section 1.2: 120k ticks/s at 4:1 tick:quote is 150k events/s. The 0.25
        # has to be supplied; there is no default ratio.
        converted = ticks_per_s(120_000).to_events_per_s(quotes_per_tick=0.25)
        assert converted.unit is Unit.EVENTS_PER_S
        assert converted.value == 150_000

    def test_conversion_needs_a_keyword_so_it_cannot_be_passed_by_accident(self):
        with pytest.raises(TypeError):
            ticks_per_s(120_000).to_events_per_s(0.25)  # type: ignore[misc]

    def test_event_rate_does_not_convert_back(self):
        with pytest.raises(UnitError, match="only a tick rate"):
            events_per_s(150_000).to_events_per_s(quotes_per_tick=0.25)

    def test_negative_mix_is_refused(self):
        with pytest.raises(UnitError, match="negative"):
            ticks_per_s(1).to_events_per_s(quotes_per_tick=-0.1)

    def test_ticks_only_lane_is_an_identity(self):
        # Lane L07 is ticks-only precisely so the rate identity is unambiguous.
        assert ticks_per_s(120_000).to_events_per_s(quotes_per_tick=0.0).value == 120_000


class TestBitsAndBytes:
    def test_bits_convert_to_bytes_by_eight(self):
        assert Quantity(800, Unit.BITS_PER_S).bits_to_bytes().value == 100

    def test_byte_rate_does_not_pretend_to_be_a_bit_rate(self):
        with pytest.raises(UnitError, match="not a bit rate"):
            bytes_per_s(100).bits_to_bytes()

    def test_binary_and_decimal_prefixes_are_different_numbers(self):
        # Section 13 requires MiB/GiB to be reported separately from MB/GB.
        assert MIB == 1_048_576
        assert MB == 1_000_000
        assert GIB != GB


class TestDurations:
    def test_milliseconds_to_seconds(self):
        assert milliseconds(280).to_seconds() == pytest.approx(0.28)

    def test_seconds_to_milliseconds(self):
        assert seconds(1.5).to_milliseconds() == pytest.approx(1500)

    def test_a_rate_is_not_a_duration(self):
        with pytest.raises(UnitError, match="not a duration"):
            ticks_per_s(1).to_seconds()


class TestRateConstruction:
    def test_rate_is_count_over_measured_seconds(self):
        assert rate(60_000, 0.5, Unit.TICKS_PER_S).value == 120_000

    def test_zero_elapsed_is_an_error_not_a_zero_rate(self):
        # Returning 0.0 here would let a run with no measured window publish a
        # rate; section 4.2 derives every rate from measured_seconds.
        with pytest.raises(UnitError, match="must be > 0"):
            rate(1000, 0.0, Unit.TICKS_PER_S)

    def test_negative_elapsed_is_an_error(self):
        with pytest.raises(UnitError):
            rate(1000, -1.0, Unit.TICKS_PER_S)

    def test_count_units_are_not_rate_units(self):
        with pytest.raises(UnitError, match="not a rate unit"):
            rate(10, 1.0, Unit.TICKS)

    def test_is_rate_flag(self):
        assert ticks_per_s(1).is_rate
        assert not Quantity(1, Unit.TICKS).is_rate


class TestBudgetChecks:
    def test_within_is_inclusive(self):
        assert milliseconds(280).within(milliseconds(280))

    def test_exceeds_is_strict(self):
        assert not milliseconds(280).exceeds(milliseconds(280))
        assert milliseconds(281).exceeds(milliseconds(280))

    def test_as_dict_carries_the_unit(self):
        assert ticks_per_s(120_000).as_dict() == {"value": 120_000, "unit": "ticks/s"}

    def test_str_shows_the_unit(self):
        assert str(ticks_per_s(120_000)) == "120000 ticks/s"
