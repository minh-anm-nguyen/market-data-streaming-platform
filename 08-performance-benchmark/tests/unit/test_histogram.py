"""Bucketed histograms and resolution adequacy (section 4.3, case E2E-40).

The central test is :meth:`TestResolutionForThreshold.test_e2e_40_200_to_500ms_bucket_cannot_decide_280ms`:
a histogram whose buckets straddle the threshold cannot answer the question, and
section 4.3 requires that to surface as insufficient resolution rather than as a
confident estimate.
"""

from __future__ import annotations

import pytest

from p08_bench.errors import MetrologyError
from p08_bench.histogram import (
    BucketHistogram,
    Resolution,
    bounds_for_threshold,
    exponential_bounds,
    linear_bounds,
)


class TestConstruction:
    def test_bounds_must_be_ascending(self):
        with pytest.raises(MetrologyError, match="ascending"):
            BucketHistogram([100, 50])

    def test_duplicate_bounds_are_refused(self):
        with pytest.raises(MetrologyError, match="ascending"):
            BucketHistogram([10, 10])

    def test_bounds_must_be_positive(self):
        with pytest.raises(MetrologyError, match="positive"):
            BucketHistogram([0, 10])

    def test_at_least_one_bound_is_required(self):
        with pytest.raises(MetrologyError, match="at least one"):
            BucketHistogram([])

    def test_there_is_always_an_implicit_overflow_bucket(self):
        hist = BucketHistogram([10])
        assert len(hist.counts) == 2


class TestObservation:
    def test_values_land_in_the_bucket_whose_upper_bound_contains_them(self):
        hist = BucketHistogram([10, 20, 30])
        hist.observe_many([5, 10, 11, 25, 100])
        # Bounds are inclusive upper edges: 10 belongs to the first bucket.
        assert hist.counts == [2, 1, 1, 1]

    def test_overflow_counts_separately(self):
        hist = BucketHistogram([10])
        hist.observe_many([1, 50])
        assert hist.overflow_count == 1

    def test_nan_is_refused(self):
        with pytest.raises(MetrologyError, match="NaN"):
            BucketHistogram([10]).observe(float("nan"))

    def test_count_tracks_every_observation(self):
        hist = BucketHistogram([10, 20])
        hist.observe_many([1] * 7)
        assert hist.count == 7

    def test_memory_is_bounded_by_the_bucket_count(self):
        # The whole reason to use a histogram instead of keeping samples.
        hist = BucketHistogram([1, 2, 3])
        hist.observe_many([1.5] * 100_000)
        assert len(hist.counts) == 4


class TestQuantileInterval:
    def test_quantile_is_returned_as_an_interval_not_a_point(self):
        hist = BucketHistogram([100, 200, 500, 1000])
        hist.observe_many([50] * 95 + [300] * 5)
        interval = hist.quantile_interval(0.99)
        assert (interval.low, interval.high) == (200, 500)
        assert interval.width == 300

    def test_an_interval_in_the_open_bucket_has_no_upper_edge(self):
        hist = BucketHistogram([100])
        hist.observe_many([500])
        interval = hist.quantile_interval(0.95)
        assert interval.is_open_ended
        assert interval.width is None

    def test_interval_decides_a_threshold_when_it_is_entirely_below(self):
        hist = BucketHistogram([100, 200])
        hist.observe_many([50] * 10)
        assert hist.quantile_interval(0.95).decides_threshold(280.0) is True

    def test_interval_decides_a_threshold_when_it_is_entirely_above(self):
        hist = BucketHistogram([100, 200, 500])
        hist.observe_many([450] * 10)
        assert hist.quantile_interval(0.95).decides_threshold(150.0) is False

    def test_a_straddling_interval_decides_nothing(self):
        # This is the E2E-40 shape: the answer is "unknown", not a number.
        hist = BucketHistogram([200, 500])
        hist.observe_many([250] * 10)
        assert hist.quantile_interval(0.95).decides_threshold(280.0) is None

    def test_contains_excludes_the_lower_edge(self):
        hist = BucketHistogram([200, 500])
        hist.observe_many([300] * 5)
        interval = hist.quantile_interval(0.95)
        assert not interval.contains(200)
        assert interval.contains(201)
        assert interval.contains(500)

    def test_a_quantile_over_nothing_is_refused(self):
        with pytest.raises(MetrologyError, match="no observations"):
            BucketHistogram([10]).quantile_interval(0.95)

    def test_quantile_must_be_a_fraction(self):
        hist = BucketHistogram([10])
        hist.observe(1)
        with pytest.raises(MetrologyError, match="fraction"):
            hist.quantile_interval(95)

    def test_interval_is_always_marked_an_estimate(self):
        hist = BucketHistogram([10])
        hist.observe(1)
        assert hist.quantile_interval(0.95).as_dict()["estimate"] is True


class TestResolutionForThreshold:
    def test_e2e_40_200_to_500ms_bucket_cannot_decide_280ms(self):
        # Case E2E-40 verbatim: a Prometheus histogram with a 200-500 ms bucket
        # has precision insufficient for a 280 ms threshold.
        hist = BucketHistogram([50, 100, 200, 500, 1000])
        assert hist.resolution_for_threshold(280.0) is Resolution.INSUFFICIENT

    def test_fine_buckets_can_decide_280ms(self):
        hist = BucketHistogram(bounds_for_threshold(280.0))
        assert hist.resolution_for_threshold(280.0) is Resolution.ADEQUATE

    def test_a_threshold_in_the_open_bucket_is_out_of_range(self):
        hist = BucketHistogram([10, 20])
        assert hist.resolution_for_threshold(280.0) is Resolution.OUT_OF_RANGE

    def test_the_tolerance_is_a_parameter_not_a_constant(self):
        hist = BucketHistogram([200, 280, 500])
        # 280 lands on the (200, 280] bucket, 80 ms wide: 28.6% of the
        # threshold, so adequate at a 30% tolerance and not at the 10% default.
        assert hist.resolution_for_threshold(280.0) is Resolution.INSUFFICIENT
        assert hist.resolution_for_threshold(280.0, max_width_fraction=0.30) is Resolution.ADEQUATE

    def test_a_non_positive_threshold_is_refused(self):
        with pytest.raises(MetrologyError, match="positive"):
            BucketHistogram([10]).resolution_for_threshold(0)

    def test_resolution_does_not_depend_on_having_observations(self):
        # The bucket layout is wrong before the run starts; the preflight should
        # be able to say so without any data.
        hist = BucketHistogram([200, 500])
        assert hist.resolution_for_threshold(280.0) is Resolution.INSUFFICIENT


class TestMaximum:
    def test_max_is_not_reportable_when_the_open_bucket_is_occupied(self):
        # Section 4.3: max cannot be derived from an open final bucket.
        hist = BucketHistogram([100])
        hist.observe(500)
        assert hist.max_reportable() is None

    def test_max_is_the_top_bound_when_nothing_overflowed(self):
        hist = BucketHistogram([100])
        hist.observe(50)
        assert hist.max_reportable() == 100

    def test_the_observed_max_is_tracked_separately_as_a_diagnostic(self):
        hist = BucketHistogram([100])
        hist.observe(500)
        assert hist.observed_max == 500
        assert hist.max_reportable() is None

    def test_an_empty_histogram_has_no_maximum(self):
        assert BucketHistogram([100]).max_reportable() is None
        assert BucketHistogram([100]).observed_max is None


class TestBoundHelpers:
    def test_exponential_bounds(self):
        assert exponential_bounds(1, 2, 5) == [1, 2, 4, 8, 16]

    def test_exponential_factor_must_exceed_one(self):
        with pytest.raises(MetrologyError, match="factor"):
            exponential_bounds(1, 1.0, 3)

    def test_exponential_start_must_be_positive(self):
        with pytest.raises(MetrologyError, match="start"):
            exponential_bounds(0, 2, 3)

    def test_exponential_needs_a_step(self):
        with pytest.raises(MetrologyError, match="step"):
            exponential_bounds(1, 2, 0)

    def test_linear_bounds(self):
        assert linear_bounds(10, 10, 3) == [10, 20, 30]

    def test_linear_step_must_be_positive(self):
        with pytest.raises(MetrologyError, match="step"):
            linear_bounds(10, 0, 3)

    def test_bounds_for_threshold_spans_above_the_threshold(self):
        bounds = bounds_for_threshold(280.0)
        assert bounds[0] == pytest.approx(28.0)
        assert max(bounds) > 280.0

    def test_bucket_edges_out_of_range_is_refused(self):
        with pytest.raises(MetrologyError, match="out of range"):
            BucketHistogram([10]).bucket_edges(5)

    def test_first_bucket_starts_at_zero(self):
        assert BucketHistogram([10, 20]).bucket_edges(0) == (0.0, 10)

    def test_last_bucket_has_no_upper_edge(self):
        assert BucketHistogram([10, 20]).bucket_edges(2) == (20, None)
