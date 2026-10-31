"""Percentiles, censoring and refusals (FIX-002, E2E-39, E2E-41, E2E-46, section 19.2).

:class:`TestFix002` is the fixture the design document pins by hand, so it is
the one test in the suite whose expected values are quoted rather than derived.
"""

from __future__ import annotations

import pytest

from p08_bench.errors import MetrologyError
from p08_bench.goldens import FIX002_LATENCIES_MS
from p08_bench.percentiles import (
    MIN_COMPLETED_SAMPLES_FOR_P95,
    ConfidenceInterval,
    PercentileBasis,
    PercentileClaim,
    average_of_percentiles,
    bootstrap_ci,
    describe_distribution,
    exact_nearest_rank,
    median,
    nearest_rank_index,
    percentile_completed_only,
    percentile_with_censoring,
    pooled_claim,
    sample_count_adequate,
)


class TestNearestRank:
    def test_rank_formula_is_ceil_q_times_n(self):
        # ceil(0.95 * 10) = 10 -> zero-based index 9.
        assert nearest_rank_index(10, 0.95) == 9

    def test_rank_never_falls_below_one(self):
        assert nearest_rank_index(10, 0.01) == 0

    def test_q_of_one_is_the_last_value(self):
        assert nearest_rank_index(10, 1.0) == 9

    def test_quantile_must_be_a_fraction(self):
        # Passing 95 instead of 0.95 is the classic slip and must not silently
        # clamp to the maximum.
        with pytest.raises(MetrologyError, match="0.95 not 95"):
            nearest_rank_index(10, 95)

    def test_zero_quantile_is_refused(self):
        with pytest.raises(MetrologyError):
            nearest_rank_index(10, 0.0)

    def test_empty_population_has_no_quantile(self):
        with pytest.raises(MetrologyError, match="no samples"):
            exact_nearest_rank([], 0.95)

    def test_input_is_not_mutated(self):
        values = [3.0, 1.0, 2.0]
        exact_nearest_rank(values, 0.5)
        assert values == [3.0, 1.0, 2.0]


class TestFix002:
    """Section 10, FIX-002, quoted: p95 over these ten samples is 1000 ms."""

    def test_exact_p95_is_the_outlier(self):
        assert exact_nearest_rank(FIX002_LATENCIES_MS, 0.95) == 1000.0

    def test_p95_is_not_the_mean(self):
        # The document names 226 ms explicitly as the wrong answer.
        mean = sum(FIX002_LATENCIES_MS) / len(FIX002_LATENCIES_MS)
        assert mean == pytest.approx(226.0)
        assert exact_nearest_rank(FIX002_LATENCIES_MS, 0.95) != pytest.approx(mean)

    def test_p50_is_the_fifth_value(self):
        # ceil(0.5 * 10) = 5 -> 140 ms, the fifth of the ten ordered values.
        assert exact_nearest_rank(FIX002_LATENCIES_MS, 0.50) == 140.0

    def test_ten_timeouts_added_keep_the_conditional_p95_at_1000ms(self):
        # FIX-002 second half: scheduled=20, completed=10, timeout=10, and the
        # conditional p95 stays 1000 ms. It must not become "50% at 0 ms".
        result = percentile_completed_only(FIX002_LATENCIES_MS, 0.95, n_censored=10)
        assert result.value_ms == 1000.0
        assert result.n_considered == 20
        assert result.n_completed == 10
        assert result.n_censored == 10
        assert result.basis is PercentileBasis.COMPLETED_ONLY
        assert result.censored_fraction == 0.5

    def test_with_censoring_the_same_data_yields_a_lower_bound(self):
        # Over all 20 attempts, rank ceil(0.95*20)=19 lands in the censored tail,
        # so there is no value to report -- only "at least the deadline".
        result = percentile_with_censoring(
            FIX002_LATENCIES_MS, 0.95, n_censored=10, deadline_ms=2000.0
        )
        assert result.value_ms is None
        assert result.is_lower_bound
        assert result.lower_bound_ms == 2000.0
        assert not result.reportable
        assert "censored tail" in result.reason


class TestCensoring:
    def test_rank_inside_the_completed_region_is_exact(self):
        # 90 completed + 10 censored: ceil(0.5*100)=50 is well inside the
        # completed values, so the quantile is a real measurement.
        completed = [float(i) for i in range(1, 91)]
        result = percentile_with_censoring(
            completed, 0.50, n_censored=10, deadline_ms=1000.0
        )
        assert result.value_ms == 50.0
        assert not result.is_lower_bound
        assert result.reportable

    def test_a_censored_bound_can_fail_a_budget_but_never_pass_one(self):
        result = percentile_with_censoring([1.0], 0.95, n_censored=99, deadline_ms=5000.0)
        assert result.meets_budget_ms(280.0) is False
        # Below the deadline the comparison is genuinely unknown, not a pass.
        assert result.meets_budget_ms(9000.0) is None

    def test_no_attempts_yields_no_quantile_rather_than_zero(self):
        result = percentile_with_censoring([], 0.95, n_censored=0, deadline_ms=100.0)
        assert result.value_ms is None
        assert result.reason == "no attempts"

    def test_all_attempts_censored(self):
        result = percentile_with_censoring([], 0.95, n_censored=7, deadline_ms=100.0)
        assert result.is_lower_bound
        assert result.n_considered == 7
        assert result.n_completed == 0

    def test_negative_censored_count_is_refused(self):
        with pytest.raises(MetrologyError, match="negative"):
            percentile_with_censoring([1.0], 0.95, n_censored=-1, deadline_ms=1.0)

    def test_non_positive_deadline_is_refused(self):
        with pytest.raises(MetrologyError, match="deadline"):
            percentile_with_censoring([1.0], 0.95, n_censored=0, deadline_ms=0.0)

    def test_completed_only_with_no_samples_reports_the_censored_population(self):
        result = percentile_completed_only([], 0.95, n_censored=4)
        assert result.value_ms is None
        assert result.n_considered == 4
        assert result.reason == "no completed samples"


class TestRefusedArithmetic:
    def test_averaging_percentiles_always_raises(self):
        # Section 19.2: the mean of per-partition p95s is not a p95.
        with pytest.raises(MetrologyError, match="does not produce a percentile"):
            average_of_percentiles(100.0, 200.0, 300.0)

    def test_it_raises_even_with_one_input(self):
        with pytest.raises(MetrologyError):
            average_of_percentiles(100.0)


class TestPercentileClaims:
    def _claim(self, **kwargs) -> PercentileClaim:
        defaults = dict(
            query_family="Q02",
            concurrency=4,
            cache_profile="warm",
            dataset_build_id="BUILD-200M",
        )
        defaults.update(kwargs)
        return PercentileClaim(
            result=percentile_completed_only([100.0, 200.0], 0.95), **defaults
        )

    def test_matching_signatures_are_comparable(self):
        assert self._claim().comparable_with(self._claim())

    def test_different_family_is_not_comparable(self):
        # Case E2E-39: a mixed Q01+Q06 p95 may not be compared with Q02-280ms.
        assert not self._claim().comparable_with(self._claim(query_family="Q06"))

    def test_different_concurrency_is_not_comparable(self):
        assert not self._claim().comparable_with(self._claim(concurrency=32))

    def test_different_cache_profile_is_not_comparable(self):
        # Case E2E-20: warm and cold are two labelled distributions.
        assert not self._claim().comparable_with(self._claim(cache_profile="cold"))

    def test_require_comparable_raises_with_both_signatures(self):
        with pytest.raises(MetrologyError, match="different workloads") as excinfo:
            self._claim().require_comparable(self._claim(query_family="Q01"))
        assert "Q02" in str(excinfo.value.context["left"])

    def test_as_dict_carries_the_labels_a_p95_needs(self):
        payload = self._claim().as_dict()
        for key in ("query_family", "concurrency", "cache_profile", "n_completed"):
            assert key in payload


class TestPooling:
    def _claim(self, family: str = "Q02") -> PercentileClaim:
        return PercentileClaim(
            result=percentile_completed_only([1.0], 0.95),
            query_family=family,
            concurrency=4,
            cache_profile="warm",
            dataset_build_id="BUILD-200M",
        )

    def test_pooling_homogeneous_claims_uses_the_raw_samples(self):
        pooled = pooled_claim(
            [self._claim(), self._claim()], list(FIX002_LATENCIES_MS), 0.95
        )
        assert pooled.result.value_ms == 1000.0
        assert pooled.query_family == "Q02"

    def test_pooling_heterogeneous_claims_is_refused(self):
        with pytest.raises(MetrologyError, match="different workloads"):
            pooled_claim([self._claim("Q02"), self._claim("Q06")], [1.0], 0.95)

    def test_pooling_with_a_deadline_applies_censoring(self):
        pooled = pooled_claim(
            [self._claim()], [1.0], 0.95, n_censored=99, deadline_ms=500.0
        )
        assert pooled.result.is_lower_bound

    def test_pooling_nothing_is_refused(self):
        with pytest.raises(MetrologyError, match="no claims"):
            pooled_claim([], [1.0], 0.95)


class TestBootstrap:
    def test_interval_is_deterministic_for_a_seed(self):
        runs = [100.0, 102.0, 98.0, 101.0, 99.0]
        first = bootstrap_ci(runs, seed=7, iterations=500)
        second = bootstrap_ci(runs, seed=7, iterations=500)
        assert (first.low, first.point, first.high) == (second.low, second.point, second.high)

    def test_interval_brackets_the_point_estimate(self):
        runs = [100.0, 102.0, 98.0, 101.0, 99.0]
        ci = bootstrap_ci(runs, iterations=500)
        assert ci.low <= ci.point <= ci.high
        assert ci.n_runs == 5

    def test_identical_runs_give_a_degenerate_interval(self):
        ci = bootstrap_ci([50.0] * 5, iterations=200)
        assert ci.low == ci.high == 50.0
        assert ci.width == 0.0

    def test_a_single_run_is_not_a_confidence_interval(self):
        # Section 4.4: millions of requests in one run are not independent
        # samples, and one run is not five.
        with pytest.raises(MetrologyError, match="at least 2 independent runs"):
            bootstrap_ci([100.0])

    def test_too_few_iterations_is_refused(self):
        with pytest.raises(MetrologyError, match="iterations"):
            bootstrap_ci([1.0, 2.0], iterations=10)

    def test_confidence_outside_the_open_unit_interval_is_refused(self):
        with pytest.raises(MetrologyError, match="confidence"):
            bootstrap_ci([1.0, 2.0], confidence=1.0)

    def test_unsupported_statistic_is_refused(self):
        with pytest.raises(MetrologyError, match="statistic"):
            bootstrap_ci([1.0, 2.0], statistic="p95")

    def test_relative_width_and_zero_exclusion(self):
        ci = ConfidenceInterval(
            point=10.0, low=5.0, high=15.0, confidence=0.95,
            n_runs=5, iterations=1000, seed=1,
        )
        assert ci.relative_width() == 1.0
        assert ci.excludes_zero()
        assert not ConfidenceInterval(
            point=0.0, low=-1.0, high=1.0, confidence=0.95,
            n_runs=5, iterations=1000, seed=1,
        ).excludes_zero()

    def test_relative_width_of_a_zero_point_is_undefined(self):
        ci = ConfidenceInterval(
            point=0.0, low=-1.0, high=1.0, confidence=0.95,
            n_runs=5, iterations=1000, seed=1,
        )
        assert ci.relative_width() is None


class TestHelpers:
    def test_median_of_odd_and_even_counts(self):
        assert median([3.0, 1.0, 2.0]) == 2.0
        assert median([1.0, 2.0, 3.0, 4.0]) == 2.5

    def test_median_of_nothing_is_refused(self):
        with pytest.raises(MetrologyError):
            median([])

    def test_sample_count_guardrail_matches_nfr08(self):
        assert MIN_COMPLETED_SAMPLES_FOR_P95 == 10_000
        assert not sample_count_adequate(9_999)
        assert sample_count_adequate(10_000)

    def test_describe_distribution_omits_the_mean(self):
        # A mean beside a p95 invites exactly the FIX-002 substitution.
        described = describe_distribution(FIX002_LATENCIES_MS)
        assert "mean" not in described
        assert described["p95"] == 1000.0
        assert described["n"] == 10

    def test_describe_empty_distribution(self):
        described = describe_distribution([])
        assert described["n"] == 0
        assert described["p95"] is None
