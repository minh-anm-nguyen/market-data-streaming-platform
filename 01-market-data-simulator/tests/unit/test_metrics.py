"""Metric registry and histogram (section 10, cases R07, invariant 10).

Two properties matter here: counters cannot go backwards, and a histogram on a
long run must stay bounded in memory while still saying honestly whether its
percentiles are exact or estimated.
"""

from __future__ import annotations

import pytest

from market_sim.metrics import RUN_COUNTERS, Histogram, Metrics


class TestCounters:
    def test_incr_accumulates(self):
        metrics = Metrics()
        assert metrics.incr("a") == 1
        assert metrics.incr("a", 5) == 6
        assert metrics.counter("a") == 6

    def test_zero_delta_is_allowed(self):
        metrics = Metrics()
        metrics.incr("a", 0)
        assert metrics.counter("a") == 0

    def test_negative_delta_is_refused(self):
        # Invariant 10: a counter that can decrease would let a report print a
        # negative count, which is never a true statement about a run.
        metrics = Metrics()
        with pytest.raises(ValueError, match="monotonic"):
            metrics.incr("a", -1)

    def test_unset_counter_reads_as_zero_but_is_absent_from_the_snapshot(self):
        metrics = Metrics()
        assert metrics.counter("never_touched") == 0
        assert "never_touched" not in metrics.snapshot()["counters"]

    def test_ensure_counters_makes_the_zero_explicit(self):
        # "Not measured" and "measured, never happened" are different claims.
        metrics = Metrics()
        metrics.ensure_counters(["x", "y"])
        assert metrics.snapshot()["counters"] == {"x": 0, "y": 0}

    def test_ensure_counters_does_not_reset_an_existing_value(self):
        metrics = Metrics()
        metrics.incr("x", 7)
        metrics.ensure_counters(["x"])
        assert metrics.counter("x") == 7

    def test_run_counters_cover_every_boundary_the_report_names(self):
        # The naming is the contract: enqueued is not written, written is not
        # drained, and none of them is "the client received it".
        assert "tcp_events_enqueued_total" in RUN_COUNTERS
        assert "tcp_events_written_total" in RUN_COUNTERS
        assert "tcp_bytes_drained_total" in RUN_COUNTERS
        assert not any("received" in name for name in RUN_COUNTERS)
        assert not any("acknowledged" in name for name in RUN_COUNTERS)
        assert len(set(RUN_COUNTERS)) == len(RUN_COUNTERS)


class TestGauges:
    def test_set_and_read(self):
        metrics = Metrics()
        metrics.set_gauge("depth", 5)
        assert metrics.gauge("depth") == 5

    def test_gauges_may_fall_but_peaks_do_not(self):
        metrics = Metrics()
        metrics.set_gauge("depth", 10)
        metrics.set_gauge("depth", 2)
        assert metrics.gauge("depth") == 2
        assert metrics.peak("depth") == 10

    def test_unset_gauge_and_peak_read_as_zero(self):
        metrics = Metrics()
        assert metrics.gauge("nope") == 0.0 and metrics.peak("nope") == 0.0


class TestHistogram:
    def test_empty_histogram(self):
        hist = Histogram()
        assert hist.count == 0
        assert hist.percentile(50) is None
        assert hist.mean == 0.0
        assert hist.as_dict()["exact"] is True

    def test_exact_statistics_for_a_small_sample(self):
        hist = Histogram()
        for value in range(1, 101):
            hist.observe(value)
        assert hist.count == 100
        assert hist.minimum == 1 and hist.maximum == 100
        assert hist.mean == pytest.approx(50.5)
        assert hist.percentile(50) == 50
        assert hist.percentile(95) == 95
        assert hist.percentile(99) == 99
        assert hist.percentile(100) == 100
        assert hist.percentile(0) == 1
        assert hist.exact is True

    def test_count_min_max_stay_exact_after_decimation(self):
        # A soak run cannot keep every sample, but these four must never be
        # approximations -- they are what a resource-growth claim rests on.
        hist = Histogram(capacity=64)
        for value in range(10_000):
            hist.observe(value)
        assert hist.count == 10_000
        assert hist.minimum == 0 and hist.maximum == 9999
        assert hist.total == sum(range(10_000))

    def test_retained_samples_are_bounded(self):
        hist = Histogram(capacity=64)
        for value in range(100_000):
            hist.observe(value)
        assert hist.as_dict()["samples_retained"] <= 64

    def test_decimation_is_disclosed_not_hidden(self):
        hist = Histogram(capacity=64)
        for value in range(10_000):
            hist.observe(value)
        assert hist.exact is False
        assert hist.as_dict()["exact"] is False

    def test_decimation_is_deterministic(self):
        # Random sampling would make a report irreproducible, which defeats the
        # purpose of a reproducible benchmark.
        def run():
            hist = Histogram(capacity=32)
            for value in range(5000):
                hist.observe(value * 7 % 1000)
            return hist.as_dict()

        assert run() == run()

    def test_percentiles_remain_representative_after_decimation(self):
        hist = Histogram(capacity=128)
        for value in range(10_000):
            hist.observe(value)
        # An even sweep over the whole run, so p95 should land near 9500.
        assert 9000 <= hist.percentile(95) <= 10_000

    @pytest.mark.parametrize("q", [-1, 101])
    def test_percentile_bounds_checked(self, q):
        hist = Histogram()
        hist.observe(1)
        with pytest.raises(ValueError):
            hist.percentile(q)

    def test_capacity_floor(self):
        with pytest.raises(ValueError):
            Histogram(capacity=1)


class TestSnapshot:
    def test_snapshot_shape_and_sorting(self):
        metrics = Metrics()
        metrics.incr("zz")
        metrics.incr("aa")
        metrics.set_gauge("g", 1.5)
        metrics.observe("h", 2.0)
        snapshot = metrics.snapshot()
        assert set(snapshot) == {"counters", "gauges", "peaks", "histograms"}
        assert list(snapshot["counters"]) == ["aa", "zz"]
        assert snapshot["histograms"]["h"]["count"] == 1

    def test_histogram_accessor_creates_on_demand(self):
        metrics = Metrics()
        assert metrics.histogram("new").count == 0
        assert "new" in metrics.snapshot()["histograms"]
