"""Benchmark statistics and the section 12.1 reporting rules (group U).

The arithmetic here is simple; the rules about what may be *reported* are the
point. Every test in this file is about refusing to publish a flattering number.
"""

from __future__ import annotations

import pytest

from _coverage import covers
from md_analytics.benchmark import (
    CV_BASELINE_P95_MS,
    CV_OPTIMIZED_P95_MS,
    LatencyStats,
    WorkloadSpec,
    percentile_nearest_rank,
    report_template,
    run_query_benchmark,
    storage_estimate,
    summarise,
    verdict_for_target,
)
from md_analytics.errors import Verdict


def result(**overrides):
    """A benchmark result with controllable statistics."""
    from md_analytics.benchmark import BenchmarkResult

    kwargs = dict(
        template="ohlc_1m",
        repetitions=200,
        cold=LatencyStats.of([500.0]),
        warm=LatencyStats.of([100.0] * 199),
        rows_returned=1_000,
        errors=0,
    )
    kwargs.update(overrides)
    return BenchmarkResult(**kwargs)


#: Keyword names that belong to the measurement rather than to the classification.
_MEASUREMENT_KEYS = ("warm", "cold", "errors", "repetitions")


def classify(**overrides):
    """Classify a result, splitting overrides between measurement and rules."""
    measurement = {k: overrides.pop(k) for k in _MEASUREMENT_KEYS if k in overrides}
    rules = dict(
        target_p95_ms=CV_OPTIMIZED_P95_MS,
        correctness_verdict=Verdict.PASS,
        dataset_unique_rows=200_000_000,
        required_unique_rows=200_000_000,
        same_profile_as_reference=False,
    )
    rules.update(overrides)
    return verdict_for_target(result(**measurement), **rules)


class TestPercentiles:
    @covers("P06-U-03")
    def test_nearest_rank_returns_an_observed_value(self):
        samples = [float(i) for i in range(1, 201)]
        assert percentile_nearest_rank(samples, 0.95) == 190.0
        assert percentile_nearest_rank(samples, 0.50) == 100.0
        assert percentile_nearest_rank(samples, 1.0) == 200.0

    def test_a_single_sample_is_every_percentile(self):
        assert percentile_nearest_rank([7.0], 0.5) == 7.0
        assert percentile_nearest_rank([7.0], 0.99) == 7.0

    def test_percentiles_do_not_depend_on_input_order(self):
        import random

        samples = [float(i) for i in range(100)]
        shuffled = samples[:]
        random.Random(3).shuffle(shuffled)
        assert percentile_nearest_rank(samples, 0.95) == percentile_nearest_rank(shuffled, 0.95)

    @pytest.mark.parametrize("q", [0.0, -0.1, 1.1])
    def test_an_out_of_range_quantile_is_an_error(self, q):
        with pytest.raises(ValueError):
            percentile_nearest_rank([1.0], q)

    def test_an_empty_sample_has_no_percentile(self):
        with pytest.raises(ValueError):
            percentile_nearest_rank([], 0.95)

    def test_stats_of_an_empty_sample_are_zeroed_and_counted_as_zero(self):
        stats = LatencyStats.of([])
        assert stats.count == 0 and stats.p95_ms == 0.0


class TestHarness:
    @covers("P06-U-05")
    def test_cold_and_warm_samples_are_kept_apart(self):
        calls = {"n": 0}

        def run() -> int:
            calls["n"] += 1
            return 1

        measurement = run_query_benchmark(
            WorkloadSpec("t", run), repetitions=10, cold_repetitions=2
        )
        assert calls["n"] == 10
        assert measurement.cold.count == 2
        assert measurement.warm.count == 8

    @covers("P06-U-06")
    def test_errors_are_counted_and_not_timed(self):
        state = {"n": 0}

        def run() -> int:
            state["n"] += 1
            if state["n"] % 2 == 0:
                raise RuntimeError("injected query failure")
            return 1

        measurement = run_query_benchmark(WorkloadSpec("t", run), repetitions=10)
        assert measurement.errors == 5
        assert measurement.cold.count + measurement.warm.count == 5
        assert measurement.error_rate == 0.5
        assert "RuntimeError" in measurement.error_samples[0]

    def test_zero_repetitions_is_an_error(self):
        with pytest.raises(ValueError):
            run_query_benchmark(WorkloadSpec("t", lambda: 0), repetitions=0)


class TestReportingRules:
    @covers("P06-U-01")
    def test_a_fast_result_over_a_verified_build_passes(self):
        verdict, reason = classify()
        assert verdict is Verdict.PASS
        assert "measured_on_new_profile" in reason, (
            "a laptop run is a new benchmark, not a reproduction of the CV figure"
        )

    @covers("P06-U-01")
    def test_the_new_profile_label_is_dropped_only_when_asserted(self):
        verdict, reason = classify(same_profile_as_reference=True)
        assert verdict is Verdict.PASS
        assert "measured_on_new_profile" not in reason

    @covers("P06-U-01", "P06-AD-09")
    def test_a_fast_result_over_a_wrong_build_is_a_global_no_go(self):
        verdict, reason = classify(correctness_verdict=Verdict.FAIL)
        assert verdict is Verdict.NO_GO
        assert "not reportable" in reason

    @covers("P06-U-06")
    def test_any_query_error_makes_the_result_inconclusive(self):
        verdict, reason = classify(errors=3)
        assert verdict is Verdict.INCONCLUSIVE
        assert "manufacture a better p95" in reason

    @covers("P06-U-04", "P06-AC-01")
    def test_a_dataset_below_the_required_size_voids_the_comparison(self):
        verdict, reason = classify(dataset_unique_rows=1_000_000)
        assert verdict is Verdict.INCONCLUSIVE
        assert "physical duplicates and quotes do not count" in reason

    @covers("P06-U-01")
    def test_a_slow_result_is_a_performance_miss_not_a_correctness_failure(self):
        verdict, reason = classify(warm=LatencyStats.of([900.0] * 199))
        assert verdict is Verdict.FAIL
        assert "performance NOT_MET" in reason
        assert "not a correctness failure" in reason

    def test_no_warm_samples_is_inconclusive(self):
        verdict, reason = classify(warm=LatencyStats.of([]))
        assert verdict is Verdict.INCONCLUSIVE
        assert "no warm samples" in reason

    @covers("P06-U-02")
    def test_the_cv_reference_numbers_are_recorded_not_assumed(self):
        assert CV_BASELINE_P95_MS == 2_400.0
        assert CV_OPTIMIZED_P95_MS == 280.0

    @covers("P06-AD-08", "P06-U-01")
    def test_a_model_backed_latency_result_cannot_be_a_pass(self):
        verdict, reason = classify(evidence_class="MODEL")
        assert verdict is Verdict.INCONCLUSIVE
        assert "timed the in-process model, not ClickHouse" in reason, (
            "a fast number from Python dictionaries is a real measurement of the "
            "wrong thing"
        )

    @covers("P06-AD-08")
    def test_a_server_backed_latency_result_is_classified_normally(self):
        verdict, _ = classify(evidence_class="REAL_CLICKHOUSE")
        assert verdict is Verdict.PASS

    @covers("P06-AD-08")
    def test_the_evidence_check_runs_before_the_correctness_check(self):
        """Order matters: the weakest claim should surface the clearest reason."""
        verdict, reason = classify(
            evidence_class="MODEL", correctness_verdict=Verdict.FAIL
        )
        assert verdict is Verdict.INCONCLUSIVE
        assert "in-process model" in reason


class TestReportTemplate:
    @covers("P06-AD-05", "P06-Z-01")
    def test_the_template_defaults_to_not_run_and_to_fill(self):
        template = report_template()
        assert template["status"] == "NOT_RUN"
        assert template["clickhouse_version"] == "TO_FILL"
        assert "No performance claims before execution" in template["notes"]

    def test_the_template_covers_every_section_18_2_field(self):
        template = report_template()
        for field in (
            "run_id",
            "schema_hashes",
            "manifest_sha256",
            "dataset_unique_events",
            "raw_duplicate_id_count",
            "canonical_unique_ids",
            "bar_tick_count",
            "workload_sql_sha256",
            "p95_client_query_ms",
            "query_error_rate",
            "parts_per_partition_peak",
            "fault_trigger_proof",
            "bar_reference_mismatch_count",
            "missing_ids",
            "recovery_result",
            "release_verdict",
        ):
            assert field in template, field

    def test_an_unknown_override_is_refused(self):
        with pytest.raises(ValueError):
            report_template(nonexistent_field=1)

    def test_overrides_are_applied(self):
        assert report_template(build_id="b1")["build_id"] == "b1"


class TestStorageEstimate:
    @covers("P06-AC-02")
    def test_the_section_18_1_example_arithmetic(self):
        estimate = storage_estimate(offered_rate_events_s=120_000, bytes_per_event=60)
        assert estimate["raw_bytes_per_day"] == 86_400 * 120_000 * 60
        assert estimate["raw_gb_per_day_decimal"] == 622
        assert estimate["gb_for_200m_rows_decimal"] == 12

    @pytest.mark.parametrize("rate,size", [(0, 60), (120_000, 0), (-1, 60)])
    def test_non_positive_inputs_are_refused(self, rate, size):
        with pytest.raises(ValueError):
            storage_estimate(offered_rate_events_s=rate, bytes_per_event=size)


class TestSummary:
    def test_a_summary_totals_errors_and_queries(self):
        summary = summarise([result(), result(errors=2)])
        assert summary["total_queries"] == 400
        assert summary["total_errors"] == 2
        assert len(summary["templates"]) == 2
