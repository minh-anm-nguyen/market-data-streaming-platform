"""Metrics: cardinality guard, histogram quantiles, exposition format."""

from __future__ import annotations

import pytest

from p05_ingest.metrics import (
    FORBIDDEN_LABEL_NAMES,
    Histogram,
    MetricsRegistry,
)


@pytest.fixture
def registry() -> MetricsRegistry:
    return MetricsRegistry()


# ------------------------------------------------------------- cardinality


@pytest.mark.parametrize("label", sorted(FORBIDDEN_LABEL_NAMES))
def test_unbounded_labels_are_refused(registry, label):
    """Spec 11.1: a series per event or dataset is not survivable."""
    with pytest.raises(ValueError, match="forbidden"):
        registry.inc("p05_test_total", **{label: "anything"})
    with pytest.raises(ValueError, match="forbidden"):
        registry.set_gauge("p05_test_gauge", 1.0, **{label: "anything"})
    with pytest.raises(ValueError, match="forbidden"):
        registry.observe("p05_test_hist", 1.0, **{label: "anything"})


def test_bounded_labels_are_allowed(registry):
    registry.inc("p05_test_total", topic="md.ticks.v1", partition="0")
    assert registry.counter("p05_test_total", topic="md.ticks.v1", partition="0") == 1


def test_label_order_does_not_create_separate_series(registry):
    registry.inc("p05_test_total", topic="t", partition="0")
    registry.inc("p05_test_total", partition="0", topic="t")
    assert registry.counter("p05_test_total", topic="t", partition="0") == 2


# ---------------------------------------------------------------- counters


def test_counters_accumulate_and_total_across_labels(registry):
    registry.inc("p05_x_total", topic="a")
    registry.inc("p05_x_total", topic="a")
    registry.inc("p05_x_total", topic="b", value=3)
    assert registry.counter("p05_x_total", topic="a") == 2
    assert registry.counter("p05_x_total", topic="b") == 3
    assert registry.counter_total("p05_x_total") == 5


def test_unset_counter_reads_zero(registry):
    assert registry.counter("p05_never_touched_total") == 0
    assert registry.counter_total("p05_never_touched_total") == 0


def test_gauges_overwrite(registry):
    registry.set_gauge("p05_g", 1.0, topic="a")
    registry.set_gauge("p05_g", 7.0, topic="a")
    assert registry.gauge("p05_g", topic="a") == 7.0


# -------------------------------------------------------------- histograms


def test_histogram_counts_and_sums(registry):
    for value in (0.001, 0.03, 0.3, 3.0):
        registry.observe("p05_lat", value)
    hist = registry.histogram("p05_lat")
    assert hist.total == 4
    assert hist.sum_value == pytest.approx(3.331)


def test_histogram_quantile_is_an_upper_bound():
    """Documented behaviour: the bucket boundary, so it over-estimates."""
    hist = Histogram(buckets=(1.0, 2.0, 4.0))
    for _ in range(90):
        hist.observe(0.5)
    for _ in range(10):
        hist.observe(3.0)
    assert hist.quantile(0.5) == 1.0
    assert hist.quantile(0.95) == 4.0, "the 95th sample is in the (2, 4] bucket"


def test_empty_histogram_quantile_is_zero():
    assert Histogram().quantile(0.95) == 0.0


def test_value_above_every_bucket_lands_in_the_overflow():
    hist = Histogram(buckets=(1.0,))
    hist.observe(1000.0)
    assert hist.counts[-1] == 1
    assert hist.quantile(0.5) == float("inf")


# ------------------------------------------------------------- exposition


def test_prometheus_exposition_shape(registry):
    registry.inc("p05_a_total", topic="t")
    registry.set_gauge("p05_b", 2.5)
    registry.observe("p05_c", 0.3)
    text = registry.render_prometheus()

    assert 'p05_a_total{topic="t"} 1' in text
    assert "p05_b 2.5" in text
    assert 'p05_c_bucket{le="0.5"} 1' in text
    assert 'p05_c_bucket{le="+Inf"} 1' in text
    assert "p05_c_count 1" in text
    assert text.endswith("\n")


def test_exposition_is_deterministically_ordered(registry):
    for name in ("p05_z_total", "p05_a_total", "p05_m_total"):
        registry.inc(name)
    first = registry.render_prometheus()
    assert first == registry.render_prometheus()
    lines = [line for line in first.splitlines() if line]
    assert lines == sorted(lines)


def test_as_dict_includes_quantiles(registry):
    for value in (0.01, 0.02, 5.0):
        registry.observe("p05_lat", value, table="market_events_raw")
    snapshot = registry.as_dict()
    key = "p05_lat|table=market_events_raw"
    assert snapshot["histograms"][key]["count"] == 3
    assert "p95" in snapshot["histograms"][key]


def test_as_dict_is_json_serialisable(registry):
    import json

    registry.inc("p05_a_total", topic="t")
    registry.set_gauge("p05_b", 1.0)
    registry.observe("p05_c", 0.1)
    json.dumps(registry.as_dict())


def test_registry_is_thread_safe():
    """Sink workers increment from their own threads (spec ADR-002)."""
    import threading

    registry = MetricsRegistry()
    iterations = 2000

    def worker():
        for _ in range(iterations):
            registry.inc("p05_concurrent_total")
            registry.observe("p05_concurrent_lat", 0.01)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert registry.counter("p05_concurrent_total") == 4 * iterations
    assert registry.histogram("p05_concurrent_lat").total == 4 * iterations
