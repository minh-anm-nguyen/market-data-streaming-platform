"""Backlog slopes and stability (section 4.2, FIX-003, INV-09).

:class:`TestFix003` reproduces the worked example from section 10 verbatim: at
120k offered against a 100k sink the backlog grows by 12M records over ten
minutes and a 1M-record queue fills in about 50 seconds. The surrounding tests
pin the three ways a stability check can be fooled -- too few windows, a tail
plateau after sustained growth, and a backlog that fell because retention
deleted the data.
"""

from __future__ import annotations

import pytest

from p08_bench.errors import MetrologyError
from p08_bench.goldens import (
    FIX003_EXPECTED_BACKLOG_GROWTH,
    FIX003_EXPECTED_TIME_TO_FULL_S,
    FIX003_SCENARIO,
)
from p08_bench.timeseries import (
    MIN_STABILITY_WINDOWS,
    PartitionedBacklog,
    Stability,
    WindowedSeries,
    assess_stability,
    time_to_full_s,
)
from p08_bench.units import Unit

from ..conftest import ramp

SEC = 1_000_000_000


def series(values: list[float], *, step_s: float = 60.0, name: str = "backlog") -> WindowedSeries:
    out = WindowedSeries(name, Unit.RECORDS)
    out.extend(ramp(values, step_s=step_s))
    return out


class TestWindowedSeries:
    def test_slope_of_a_flat_series_is_zero(self):
        assert series([100.0] * 6).slope_per_s() == pytest.approx(0.0)

    def test_slope_of_a_linear_climb(self):
        # +60 records per 60 s window is +1 record/s.
        assert series([0, 60, 120, 180, 240]).slope_per_s() == pytest.approx(1.0)

    def test_slope_of_a_drain_is_negative(self):
        assert series([240, 180, 120, 60, 0]).slope_per_s() == pytest.approx(-1.0)

    def test_a_single_point_has_no_slope_rather_than_a_zero_slope(self):
        # "No trend measured" and "a measured trend of zero" are different.
        assert series([100.0]).slope_per_s() is None

    def test_an_empty_series_has_no_slope(self):
        assert WindowedSeries("s", Unit.RECORDS).slope_per_s() is None

    def test_samples_sharing_one_timestamp_have_no_slope(self):
        s = WindowedSeries("s", Unit.RECORDS)
        s.add(0, 1.0)
        s.add(0, 2.0)
        assert s.slope_per_s() is None

    def test_out_of_order_timestamps_are_refused(self):
        s = WindowedSeries("s", Unit.RECORDS)
        s.add(100, 1.0)
        with pytest.raises(MetrologyError, match="non-decreasing"):
            s.add(50, 2.0)

    def test_r_squared_is_one_for_a_perfect_line(self):
        assert series([0, 60, 120, 180]).r_squared() == pytest.approx(1.0)

    def test_r_squared_is_lower_for_noise(self):
        assert series([0, 100, 10, 90, 20]).r_squared() < 0.5

    def test_tail_keeps_only_the_last_windows(self):
        tail = series([0, 10, 20, 30, 40]).tail(2)
        assert len(tail) == 2
        assert tail.last.value == 40

    def test_tail_needs_at_least_one_window(self):
        with pytest.raises(MetrologyError, match="at least one"):
            series([1.0, 2.0]).tail(0)

    def test_span_and_as_dict(self):
        payload = series([0, 60, 120]).as_dict()
        assert payload["n_windows"] == 3
        assert payload["span_s"] == pytest.approx(120.0)
        assert payload["slope_per_s"] == pytest.approx(1.0)


class TestStability:
    def test_a_flat_series_over_enough_windows_is_stable(self):
        assessment = assess_stability(series([100.0] * 10), tolerance_per_s=0.5)
        assert assessment.verdict is Stability.STABLE
        assert assessment.stable

    def test_two_windows_cannot_establish_stability(self):
        # FIX-003: a final snapshot proves nothing; two points are a line
        # through anything.
        assessment = assess_stability(series([100.0, 100.0]), tolerance_per_s=0.5)
        assert assessment.verdict is Stability.INSUFFICIENT_WINDOWS
        assert not assessment.decided
        assert "window floor" in assessment.reasons[0]

    def test_the_window_floor_is_five_by_default(self):
        assert MIN_STABILITY_WINDOWS == 5
        assert assess_stability(series([1.0] * 5), tolerance_per_s=0.1).decided

    def test_a_growing_backlog_is_not_stable(self):
        assessment = assess_stability(series([0, 60, 120, 180, 240, 300]), tolerance_per_s=0.5)
        assert assessment.verdict is Stability.GROWING
        assert assessment.full_slope_per_s == pytest.approx(1.0)

    def test_growth_within_tolerance_is_stable(self):
        assessment = assess_stability(series([0, 6, 12, 18, 24, 30]), tolerance_per_s=0.5)
        assert assessment.verdict is Stability.STABLE

    def test_sustained_growth_then_a_plateau_still_fails(self):
        # Section 4.2 wants no growth across the whole steady phase *and* none
        # in its tail. 25 minutes of climbing followed by a flat finish fails
        # the first test even though the tail looks calm.
        values = [60.0 * i for i in range(25)] + [1440.0] * 5
        assessment = assess_stability(series(values), tolerance_per_s=0.5)
        assert assessment.verdict is Stability.GROWING
        assert any("full-phase slope" in r for r in assessment.reasons)

    def test_a_flat_start_with_a_climbing_tail_also_fails(self):
        values = [100.0] * 25 + [100.0 + 60.0 * i for i in range(1, 6)]
        assessment = assess_stability(series(values), tolerance_per_s=0.2)
        assert assessment.verdict is Stability.GROWING
        assert any("tail slope" in r for r in assessment.reasons)

    def test_a_draining_series_is_reported_as_shrinking(self):
        assessment = assess_stability(series([600, 480, 360, 240, 120, 0]), tolerance_per_s=0.5)
        assert assessment.verdict is Stability.SHRINKING
        assert not assessment.stable
        assert assessment.decided

    def test_retention_truncation_invalidates_rather_than_passes(self):
        # FIX-003: a backlog that went to zero because Kafka deleted the
        # segments did not get processed.
        assessment = assess_stability(
            series([600, 480, 0, 0, 0, 0]), tolerance_per_s=0.5, retention_truncated=True
        )
        assert assessment.verdict is Stability.INVALIDATED
        assert not assessment.decided
        assert "retention deleted segments" in assessment.reasons[0]

    def test_a_missing_window_invalidates(self):
        # Case E2E-44: the exporter disappearing during the busiest minutes
        # cannot be interpolated green.
        assessment = assess_stability(series([1.0] * 10), tolerance_per_s=0.5, gap_windows=2)
        assert assessment.verdict is Stability.INVALIDATED
        assert "no observation" in assessment.reasons[0]

    def test_a_negative_tolerance_is_refused(self):
        with pytest.raises(MetrologyError, match="negative"):
            assess_stability(series([1.0] * 6), tolerance_per_s=-1)

    def test_the_tolerance_is_published_in_the_assessment(self):
        # Section 4.2: "near zero" has to be defined against something.
        assessment = assess_stability(series([1.0] * 6), tolerance_per_s=0.25)
        assert assessment.as_dict()["tolerance_per_s"] == 0.25


class TestFix003:
    def test_backlog_growth_matches_the_worked_example(self):
        assert FIX003_SCENARIO.expected_backlog_growth == FIX003_EXPECTED_BACKLOG_GROWTH

    def test_time_to_full_matches_the_worked_example(self):
        assert FIX003_SCENARIO.expected_time_to_full_s == FIX003_EXPECTED_TIME_TO_FULL_S

    def test_time_to_full_helper_reproduces_the_fifty_seconds(self):
        assert time_to_full_s(
            FIX003_SCENARIO.free_capacity_records,
            FIX003_SCENARIO.offered_per_s,
            FIX003_SCENARIO.sink_per_s,
        ) == pytest.approx(50.0)

    def test_the_growing_backlog_is_detected_as_growing(self):
        # One sample per second for the first minute of the FIX-003 scenario.
        values = [FIX003_SCENARIO.excess_per_s * i for i in range(60)]
        assessment = assess_stability(series(values, step_s=1.0), tolerance_per_s=100.0)
        assert assessment.verdict is Stability.GROWING
        assert assessment.full_slope_per_s == pytest.approx(20_000.0)

    def test_no_time_to_full_when_the_sink_keeps_up(self):
        # Returning infinity here would let a report print "fills in infinity
        # seconds" as a safety argument.
        assert time_to_full_s(1_000_000, 100_000, 120_000) is None

    def test_no_time_to_full_at_exactly_balanced_rates(self):
        assert time_to_full_s(1_000_000, 100_000, 100_000) is None

    def test_negative_free_capacity_is_refused(self):
        with pytest.raises(MetrologyError, match="negative"):
            time_to_full_s(-1, 120_000, 100_000)


class TestPartitionedBacklog:
    def _backlog(self) -> PartitionedBacklog:
        backlog = PartitionedBacklog()
        for partition in range(8):
            for window in range(8):
                # Partition 3 is the hot one; the rest are flat.
                value = 60.0 * window if partition == 3 else 100.0
                backlog.add("md.ticks.v1", partition, window * 60 * SEC, value)
        return backlog

    def test_one_hot_partition_governs_the_verdict(self):
        # INV-09: a global average would hide this behind seven idle partitions.
        worst = self._backlog().worst(tolerance_per_s=0.5)
        assert worst.series_name == "md.ticks.v1-3"
        assert worst.verdict is Stability.GROWING

    def test_the_mean_slope_looks_reassuring_and_is_never_consulted(self):
        backlog = self._backlog()
        mean = backlog.mean_slope_per_s()
        # The mean of one climbing and seven flat partitions is small...
        assert mean == pytest.approx(0.125)
        # ...while the verdict is still GROWING.
        assert backlog.worst(tolerance_per_s=0.5).verdict is Stability.GROWING

    def test_every_partition_gets_its_own_assessment(self):
        assessments = self._backlog().assess_all(tolerance_per_s=0.5)
        assert len(assessments) == 8
        assert assessments["md.ticks.v1-0"].verdict is Stability.STABLE

    def test_invalidated_outranks_growing_in_the_worst_case(self):
        backlog = self._backlog()
        backlog.mark_retention_truncated("md.ticks.v1", 5)
        worst = backlog.worst(tolerance_per_s=0.5)
        assert worst.series_name == "md.ticks.v1-5"
        assert worst.verdict is Stability.INVALIDATED

    def test_a_gap_on_one_partition_invalidates_that_partition(self):
        backlog = self._backlog()
        backlog.mark_gap("md.ticks.v1", 6, windows=1)
        assessments = backlog.assess_all(tolerance_per_s=0.5)
        assert assessments["md.ticks.v1-6"].verdict is Stability.INVALIDATED

    def test_an_empty_backlog_has_no_worst_partition(self):
        assert PartitionedBacklog().worst(tolerance_per_s=0.5) is None

    def test_keys_are_topic_and_partition(self):
        backlog = PartitionedBacklog()
        backlog.add("md.quotes.v1", 2, 0, 1.0)
        assert backlog.keys == ["md.quotes.v1-2"]

    def test_as_dict_labels_the_mean_as_diagnostic_only(self):
        payload = self._backlog().as_dict(tolerance_per_s=0.5)
        assert "mean_slope_per_s_diagnostic_only" in payload
        assert payload["worst"]["verdict"] == "GROWING"
