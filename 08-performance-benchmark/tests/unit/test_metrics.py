"""Stage counters, conservation and cardinality control (sections 4.1, 5.2).

Two things are being pinned here. First, that the nine stage counters stay nine
separate quantities: section 4.1 explicitly forbids an
``offered == raw_rows == canonical_rows`` identity, so
:func:`~p08_bench.metrics.conservation_report` must describe without asserting.
Second, that ``None`` propagates: an unmeasured counter never becomes a zero and
never becomes a pass.
"""

from __future__ import annotations

import pytest

from p08_bench.errors import MetrologyError
from p08_bench.metrics import (
    ALLOWED_LABEL_KEYS,
    FORBIDDEN_LABEL_KEYS,
    RUN_COUNTERS,
    STAGE_COUNTER_SCOPES,
    MetricRegistry,
    StageCounterName,
    StageCounters,
    conservation_report,
)


class TestStageCounters:
    def test_every_counter_starts_unmeasured(self):
        # "Not measured" is the honest default; zero would be a claim.
        counters = StageCounters()
        assert counters.measured == ()
        assert len(counters.unmeasured) == 9

    def test_setting_and_reading_by_name(self):
        counters = StageCounters()
        counters.set(StageCounterName.ISSUED, 1000)
        assert counters.get(StageCounterName.ISSUED) == 1000
        assert StageCounterName.ISSUED in counters.measured

    def test_an_unknown_counter_name_is_refused(self):
        with pytest.raises(MetrologyError, match="unknown stage counter"):
            StageCounters().set("made_up_count", 1)

    def test_negative_counts_are_refused(self):
        with pytest.raises(MetrologyError, match="negative"):
            StageCounters().set(StageCounterName.ISSUED, -1)

    def test_explicit_none_clears_back_to_unmeasured(self):
        counters = StageCounters(issued_count=5)
        counters.set(StageCounterName.ISSUED, None)
        assert counters.get(StageCounterName.ISSUED) is None

    def test_the_nine_counters_of_section_4_1_are_all_present(self):
        assert len(StageCounterName.ORDERED) == 9
        assert set(StageCounterName.ORDERED) == set(STAGE_COUNTER_SCOPES)

    def test_as_dict_lists_every_counter_including_the_unknown_ones(self):
        payload = StageCounters(issued_count=3).as_dict()
        assert payload["issued_count"] == 3
        assert payload["canonical_unique_count"] is None


class TestConservationReport:
    def test_no_simple_equality_is_asserted(self):
        # Section 4.1: rejects, duplicates and partial reads break any
        # offered == raw == canonical identity.
        report = conservation_report(
            StageCounters(
                offered_count=1000, issued_count=1000, source_sealed_count=1000,
                adapter_accepted_count=900, producer_ack_count=900,
                kafka_physical_count=905, raw_physical_count=905,
                canonical_unique_count=900, oracle_expected_count=900,
            )
        )
        assert "No offered==raw==canonical identity is asserted" in report["note"]

    def test_gaps_are_computed_between_adjacent_stages(self):
        report = conservation_report(
            StageCounters(offered_count=1000, issued_count=700)
        )
        gap = report["gaps"][0]
        assert gap["upstream"] == StageCounterName.OFFERED
        assert gap["downstream"] == StageCounterName.ISSUED
        assert gap["delta"] == 300
        assert gap["shrinkage_fraction"] == pytest.approx(0.3)

    def test_an_unmeasured_stage_makes_its_gap_unknown_not_zero(self):
        report = conservation_report(StageCounters(offered_count=1000))
        gap = report["gaps"][0]
        assert gap["known"] is False
        assert gap["delta"] is None

    def test_retry_duplicates_above_producer_ack_are_expected(self):
        report = conservation_report(
            StageCounters(producer_ack_count=100, kafka_physical_count=105)
        )
        assert report["expected_relations"]["kafka_physical_ge_producer_ack"] is True

    def test_dedup_below_raw_rows_is_expected(self):
        report = conservation_report(
            StageCounters(raw_physical_count=105, canonical_unique_count=100)
        )
        assert report["expected_relations"]["canonical_le_raw_physical"] is True

    def test_issuing_more_than_offered_is_flagged_false(self):
        # The driver inventing work is a driver bug, not throughput.
        report = conservation_report(
            StageCounters(offered_count=100, issued_count=120)
        )
        assert report["expected_relations"]["issued_le_offered"] is False

    def test_an_unmeasured_relation_is_none_not_true(self):
        report = conservation_report(StageCounters())
        assert all(v is None for v in report["expected_relations"].values())

    def test_unmeasured_counters_are_listed(self):
        report = conservation_report(StageCounters(offered_count=1))
        assert StageCounterName.CANONICAL_UNIQUE in report["unmeasured"]

    def test_scopes_travel_with_the_numbers(self):
        report = conservation_report(StageCounters())
        assert "retry duplicates" in report["scopes"][StageCounterName.KAFKA_PHYSICAL]


class TestRegistryCounters:
    def test_counters_accumulate(self):
        registry = MetricRegistry()
        assert registry.incr("a") == 1
        assert registry.incr("a", 5) == 6

    def test_a_negative_delta_is_refused(self):
        registry = MetricRegistry()
        with pytest.raises(MetrologyError, match="monotonic"):
            registry.incr("a", -1)

    def test_zero_delta_is_allowed(self):
        registry = MetricRegistry()
        registry.incr("a", 0)
        assert registry.counter("a") == 0

    def test_an_untouched_counter_reads_zero_but_is_absent_from_the_snapshot(self):
        registry = MetricRegistry()
        assert registry.counter("never") == 0
        assert "never" not in registry.snapshot()["counters"]

    def test_ensure_counters_makes_a_real_zero_explicit(self):
        registry = MetricRegistry()
        registry.ensure_counters(["x", "y"])
        assert registry.snapshot()["counters"] == {"x": 0, "y": 0}

    def test_ensure_counters_does_not_reset(self):
        registry = MetricRegistry()
        registry.incr("x", 7)
        registry.ensure_counters(["x"])
        assert registry.counter("x") == 7


class TestCardinalityControl:
    @pytest.mark.parametrize("label", sorted(FORBIDDEN_LABEL_KEYS))
    def test_unbounded_labels_are_refused(self, label):
        # Section 5.2: these melt the scraper and change the behaviour of the
        # system being measured. The detail belongs in the evidence store.
        registry = MetricRegistry()
        with pytest.raises(MetrologyError, match="high-cardinality"):
            registry.incr("events_total", labels={label: "whatever"})

    def test_unlisted_labels_are_refused_too(self):
        registry = MetricRegistry()
        with pytest.raises(MetrologyError, match="not in the allowed set"):
            registry.incr("events_total", labels={"hostname": "box-1"})

    def test_allowed_labels_work(self):
        registry = MetricRegistry()
        registry.incr("records_total", labels={"topic": "md.ticks.v1", "partition": "3"})
        assert registry.counter(
            "records_total", labels={"topic": "md.ticks.v1", "partition": "3"}
        ) == 1

    def test_label_order_does_not_create_two_series(self):
        registry = MetricRegistry()
        registry.incr("r", labels={"topic": "t", "partition": "1"})
        registry.incr("r", labels={"partition": "1", "topic": "t"})
        assert registry.counter("r", labels={"topic": "t", "partition": "1"}) == 2
        assert registry.series_cardinality("r") == 1

    def test_distinct_label_values_are_distinct_series(self):
        registry = MetricRegistry()
        for partition in range(8):
            registry.incr("r", labels={"partition": str(partition)})
        assert registry.series_cardinality("r") == 8

    def test_the_allowed_and_forbidden_sets_do_not_overlap(self):
        assert not (ALLOWED_LABEL_KEYS & FORBIDDEN_LABEL_KEYS)


class TestGaugesAndHistograms:
    def test_a_gauge_keeps_a_high_water_mark(self):
        registry = MetricRegistry()
        registry.set_gauge("rss_bytes", 100)
        registry.set_gauge("rss_bytes", 50)
        assert registry.gauge("rss_bytes") == 50
        assert registry.peak("rss_bytes") == 100

    def test_an_unset_gauge_is_none_not_zero(self):
        # A gauge at zero and a gauge never scraped are different facts.
        assert MetricRegistry().gauge("missing") is None
        assert MetricRegistry().peak("missing") is None

    def test_a_new_histogram_requires_explicit_bounds(self):
        registry = MetricRegistry()
        with pytest.raises(MetrologyError, match="explicit bucket bounds"):
            registry.histogram("latency_ms")

    def test_observations_land_in_the_named_histogram(self):
        registry = MetricRegistry()
        registry.observe("latency_ms", 5, bounds=[10, 100])
        registry.observe("latency_ms", 50)
        assert registry.histogram("latency_ms").count == 2

    def test_snapshot_is_sorted_and_complete(self):
        registry = MetricRegistry()
        registry.incr("b")
        registry.incr("a")
        registry.set_gauge("g", 1)
        registry.observe("h", 1, bounds=[10])
        snapshot = registry.snapshot()
        assert list(snapshot["counters"]) == ["a", "b"]
        assert "g" in snapshot["gauges"]
        assert "h" in snapshot["histograms"]


class TestRunCounters:
    def test_naming_keeps_scheduled_apart_from_issued(self):
        assert "driver_slots_scheduled_total" in RUN_COUNTERS
        assert "driver_events_issued_total" in RUN_COUNTERS

    def test_query_accounting_keeps_the_denominator(self):
        for name in ("query_scheduled_total", "query_timeout_total", "query_error_total"):
            assert name in RUN_COUNTERS

    def test_correct_and_completed_are_separate_counters(self):
        # INV-13: a fast wrong answer is not a success.
        assert "query_completed_total" in RUN_COUNTERS
        assert "query_correct_total" in RUN_COUNTERS

    def test_fault_injection_separates_requested_injected_and_verified(self):
        # Section 14: a script exiting 0 is not proof the fault landed.
        for name in ("faults_requested_total", "faults_injected_total", "faults_verified_total"):
            assert name in RUN_COUNTERS

    def test_probe_failures_are_countable(self):
        # Case E2E-44: a missing exporter is an event, not an absence.
        assert "probe_scrape_failures_total" in RUN_COUNTERS
        assert "probe_gap_windows_total" in RUN_COUNTERS

    def test_no_counter_claims_delivery_it_cannot_prove(self):
        assert not any("delivered" in name for name in RUN_COUNTERS)
        assert len(set(RUN_COUNTERS)) == len(RUN_COUNTERS)
