"""Minute bucketing and query range validation (group D, R)."""

from __future__ import annotations

import pytest

from _coverage import covers
from md_analytics.contracts import NS_PER_MINUTE
from md_analytics.errors import QueryRejected
from md_analytics.timebars import (
    BAR_INTERVAL_NS,
    bar_end_ns,
    bar_start_ns,
    is_minute_aligned,
    validate_range,
)

NOON = 1_793_361_600_000_000_000  # 2026-10-30T12:00:00Z, minute-aligned


class TestBucketing:
    @covers("P06-D-01")
    def test_one_nanosecond_before_the_edge_stays_in_the_earlier_minute(self):
        assert bar_start_ns(NOON + NS_PER_MINUTE - 1) == NOON

    @covers("P06-D-02")
    def test_the_edge_itself_starts_the_next_minute(self):
        assert bar_start_ns(NOON + NS_PER_MINUTE) == NOON + NS_PER_MINUTE

    @covers("P06-D-01", "P06-D-02")
    def test_two_ticks_one_nanosecond_apart_land_in_different_minutes(self):
        edge = NOON + NS_PER_MINUTE
        assert bar_start_ns(edge - 1) != bar_start_ns(edge)
        assert bar_start_ns(edge) - bar_start_ns(edge - 1) == NS_PER_MINUTE

    @covers("P06-D-03")
    def test_millisecond_truncation_would_have_merged_distinct_orderings(self):
        # Two ticks 1 ns apart inside the same millisecond. Bucketing from ns
        # keeps them distinguishable; a DateTime64(3) round trip would not.
        a, b = NOON + 1, NOON + 2
        assert a // 1_000_000 == b // 1_000_000, "same millisecond, by construction"
        assert a != b, "but distinct nanoseconds, which is what the tie-break uses"
        assert bar_start_ns(a) == bar_start_ns(b) == NOON

    def test_bucket_is_idempotent(self):
        once = bar_start_ns(NOON + 31_000_000_000)
        assert bar_start_ns(once) == once

    def test_bucket_start_is_always_aligned(self):
        for offset in (0, 1, 59_999_999_999, NS_PER_MINUTE, 7 * NS_PER_MINUTE + 3):
            assert is_minute_aligned(bar_start_ns(NOON + offset))

    def test_bar_end_is_exactly_one_interval_later(self):
        assert bar_end_ns(NOON) - NOON == BAR_INTERVAL_NS

    def test_epoch_zero_buckets_to_zero(self):
        assert bar_start_ns(0) == 0
        assert bar_start_ns(NS_PER_MINUTE - 1) == 0

    def test_rejects_non_positive_interval(self):
        with pytest.raises(ValueError):
            bar_start_ns(NOON, interval_ns=0)


class TestRangeValidation:
    @covers("P06-D-12")
    def test_two_adjacent_windows_partition_their_union_exactly_once(self):
        first = validate_range(
            NOON, NOON + NS_PER_MINUTE, require_alignment=True, max_window_minutes=10
        )
        second = validate_range(
            NOON + NS_PER_MINUTE,
            NOON + 2 * NS_PER_MINUTE,
            require_alignment=True,
            max_window_minutes=10,
        )
        edge = NOON + NS_PER_MINUTE
        assert first.contains(edge - 1) and not first.contains(edge)
        assert second.contains(edge) and not second.contains(edge + NS_PER_MINUTE)

    @covers("P06-R-01")
    @pytest.mark.parametrize("start,end", [(NOON, NOON), (NOON + 1, NOON)])
    def test_empty_or_inverted_range_is_rejected(self, start, end):
        with pytest.raises(QueryRejected) as exc:
            validate_range(start, end, require_alignment=False, max_window_minutes=None)
        assert exc.value.code == "RANGE_NOT_ASCENDING"

    @covers("P06-R-09")
    def test_unaligned_bar_range_is_rejected(self):
        with pytest.raises(QueryRejected) as exc:
            validate_range(
                NOON + 1, NOON + NS_PER_MINUTE, require_alignment=True, max_window_minutes=None
            )
        assert exc.value.code == "RANGE_NOT_ALIGNED"

    def test_unaligned_range_is_accepted_for_tick_reads(self):
        window = validate_range(
            NOON + 1, NOON + 7, require_alignment=False, max_window_minutes=None
        )
        assert window.duration_ns == 6

    @covers("P06-R-03")
    def test_window_wider_than_the_limit_is_rejected(self):
        with pytest.raises(QueryRejected) as exc:
            validate_range(
                NOON,
                NOON + 11 * NS_PER_MINUTE,
                require_alignment=True,
                max_window_minutes=10,
            )
        assert exc.value.code == "RANGE_TOO_WIDE"
        assert exc.value.context["window_minutes"] == 11

    @covers("P06-D-13", "P06-B-12")
    def test_nanoseconds_arrive_as_strings_without_loss(self):
        exact = 1_700_000_000_000_000_001
        window = validate_range(
            str(exact), str(exact + 10), require_alignment=False, max_window_minutes=None
        )
        assert window.start_ns == exact, "the last digit must survive the parse"
        assert window.end_ns == exact + 10

    @covers("P06-D-13")
    def test_float_bounds_are_refused_rather_than_rounded(self):
        with pytest.raises(QueryRejected) as exc:
            validate_range(
                float(NOON), float(NOON + NS_PER_MINUTE),
                require_alignment=False,
                max_window_minutes=None,
            )
        assert exc.value.code == "PARAM_NOT_INTEGER"

    @covers("P06-D-14")
    @pytest.mark.parametrize("text", ["23:59:60", "not-a-number", "", "1e9", "0x10", "1_000"])
    def test_non_decimal_text_is_refused(self, text):
        with pytest.raises(QueryRejected) as exc:
            validate_range(text, "1", require_alignment=False, max_window_minutes=None)
        assert exc.value.code == "PARAM_NOT_INTEGER"

    @covers("P06-D-11")
    def test_negative_bound_is_outside_the_domain(self):
        from md_analytics.errors import ContractError

        with pytest.raises((QueryRejected, ContractError)):
            validate_range("-1", "1", require_alignment=False, max_window_minutes=None)

    def test_booleans_are_not_accepted_as_bounds(self):
        with pytest.raises(QueryRejected):
            validate_range(True, 10, require_alignment=False, max_window_minutes=None)

    def test_minutes_property_counts_whole_buckets(self):
        window = validate_range(
            NOON, NOON + 5 * NS_PER_MINUTE, require_alignment=True, max_window_minutes=None
        )
        assert window.minutes == 5
