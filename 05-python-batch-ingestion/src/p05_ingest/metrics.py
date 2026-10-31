"""In-process metrics with deliberately bounded label cardinality.

Spec section 11. The metric names and meanings come straight from the
dictionary there, and two rules are enforced by the registry rather than left
to reviewers:

  * **no unbounded labels.** ``dataset_id``, ``symbol`` and any event id are
    refused as label values. A Prometheus series per symbol is survivable; a
    series per event is not, and the spec calls it out explicitly.
  * **the fault-injection counter is load-bearing.** A chaos test that cannot
    show ``p05_fault_injection_confirmed_total`` increasing is reported
    ``NOT_RUN``, never PASS (spec 9.1). So it is a first-class metric, not a
    log line.

No Prometheus client dependency: a plain registry with a text exposition
renderer keeps the test suite free of a network server, and the format is
simple enough to assert on.
"""

from __future__ import annotations

import threading
from bisect import bisect_left
from dataclasses import dataclass, field

#: Label names that may never appear. These identify an individual event or
#: dataset and would make cardinality grow with the data.
FORBIDDEN_LABEL_NAMES = frozenset(
    {"dataset_id", "event_id", "symbol", "payload_digest", "offset", "batch_id"}
)

#: Latency buckets in seconds, chosen around the 4 s discovery target in
#: NFR-10 so that the p95 question can actually be answered from them.
DEFAULT_BUCKETS: tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.0,
    4.0,
    8.0,
    16.0,
    32.0,
)


def _check_labels(labels: dict[str, str] | None) -> tuple[tuple[str, str], ...]:
    if not labels:
        return ()
    for name in labels:
        if name in FORBIDDEN_LABEL_NAMES:
            raise ValueError(
                f"label {name!r} is forbidden: it would make metric cardinality "
                "grow with the data (spec section 11.1)"
            )
    return tuple(sorted((k, str(v)) for k, v in labels.items()))


@dataclass(slots=True)
class Histogram:
    buckets: tuple[float, ...] = DEFAULT_BUCKETS
    counts: list[int] = field(default_factory=list)
    total: int = 0
    sum_value: float = 0.0

    def __post_init__(self) -> None:
        if not self.counts:
            self.counts = [0] * (len(self.buckets) + 1)

    def observe(self, value: float) -> None:
        self.total += 1
        self.sum_value += value
        index = bisect_left(self.buckets, value)
        self.counts[index] += 1

    def quantile(self, q: float) -> float:
        """Bucket-boundary estimate of a quantile.

        Returns the *upper bound* of the bucket containing the quantile, so it
        over-estimates. That is the safe direction for a latency claim, but it
        means a reported p95 is a bound rather than a measurement -- the
        performance tests record raw samples when the exact figure matters.
        """
        if self.total == 0:
            return 0.0
        target = q * self.total
        seen = 0
        for i, count in enumerate(self.counts):
            seen += count
            if seen >= target:
                return self.buckets[i] if i < len(self.buckets) else float("inf")
        return float("inf")  # pragma: no cover


class MetricsRegistry:
    """Thread-safe counters, gauges and histograms.

    Sink workers increment the insert counters from their own threads, so every
    mutation takes a lock. The lock is uncontended in the common case and the
    cost is irrelevant next to a network insert.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], int] = {}
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._histograms: dict[tuple[str, tuple[tuple[str, str], ...]], Histogram] = {}

    # --------------------------------------------------------------- counters

    def inc(self, name: str, value: int = 1, **labels: str) -> None:
        key = (name, _check_labels(labels))
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + value

    def counter(self, name: str, **labels: str) -> int:
        return self._counters.get((name, _check_labels(labels)), 0)

    def counter_total(self, name: str) -> int:
        """Sum across every label combination of one counter."""
        return sum(v for (n, _), v in self._counters.items() if n == name)

    # ----------------------------------------------------------------- gauges

    def set_gauge(self, name: str, value: float, **labels: str) -> None:
        key = (name, _check_labels(labels))
        with self._lock:
            self._gauges[key] = value

    def gauge(self, name: str, **labels: str) -> float:
        return self._gauges.get((name, _check_labels(labels)), 0.0)

    # ------------------------------------------------------------- histograms

    def observe(self, name: str, value: float, **labels: str) -> None:
        key = (name, _check_labels(labels))
        with self._lock:
            hist = self._histograms.get(key)
            if hist is None:
                hist = Histogram()
                self._histograms[key] = hist
            hist.observe(value)

    def histogram(self, name: str, **labels: str) -> Histogram | None:
        return self._histograms.get((name, _check_labels(labels)))

    # ------------------------------------------------------------- exposition

    def render_prometheus(self) -> str:
        """Minimal text exposition. Stable ordering so tests can diff it."""
        lines: list[str] = []
        with self._lock:
            for (name, labels), value in sorted(self._counters.items()):
                lines.append(f"{name}{_fmt_labels(labels)} {value}")
            for (name, labels), value in sorted(self._gauges.items()):
                lines.append(f"{name}{_fmt_labels(labels)} {value:g}")
            for (name, labels), hist in sorted(self._histograms.items()):
                cumulative = 0
                for i, bound in enumerate(hist.buckets):
                    cumulative += hist.counts[i]
                    bucket_labels = labels + (("le", f"{bound:g}"),)
                    lines.append(
                        f"{name}_bucket{_fmt_labels(bucket_labels)} {cumulative}"
                    )
                lines.append(
                    f"{name}_bucket{_fmt_labels(labels + (('le', '+Inf'),))} {hist.total}"
                )
                lines.append(f"{name}_sum{_fmt_labels(labels)} {hist.sum_value:g}")
                lines.append(f"{name}_count{_fmt_labels(labels)} {hist.total}")
        return "\n".join(lines) + "\n"

    def as_dict(self) -> dict[str, object]:
        """Flat snapshot for a run report artifact."""
        return {
            "counters": {
                _flat_key(name, labels): value
                for (name, labels), value in sorted(self._counters.items())
            },
            "gauges": {
                _flat_key(name, labels): value
                for (name, labels), value in sorted(self._gauges.items())
            },
            "histograms": {
                _flat_key(name, labels): {
                    "count": hist.total,
                    "sum": hist.sum_value,
                    "p50": hist.quantile(0.50),
                    "p95": hist.quantile(0.95),
                    "p99": hist.quantile(0.99),
                }
                for (name, labels), hist in sorted(self._histograms.items())
            },
        }


def _fmt_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    inner = ",".join(f'{k}="{v}"' for k, v in labels)
    return "{" + inner + "}"


def _flat_key(name: str, labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return name
    return name + "|" + ",".join(f"{k}={v}" for k, v in labels)


# Metric name constants, so a typo is an ImportError rather than a silent
# second series that nobody graphs.
KAFKA_RECORDS_FETCHED = "p05_kafka_records_fetched_total"
RECORDS_VALIDATED = "p05_records_validated_total"
BATCHES_SEALED = "p05_batches_sealed_total"
BATCHES_INSERT_ATTEMPTED = "p05_batches_insert_attempted_total"
CH_BATCHES_ACKED = "p05_clickhouse_batches_acked_total"
CH_BATCHES_UNKNOWN = "p05_clickhouse_batches_unknown_total"
CH_INSERT_LATENCY = "p05_clickhouse_insert_latency_seconds"
KAFKA_COMMIT_ATTEMPTS = "p05_kafka_commit_attempts_total"
COMMITTED_NEXT_OFFSET = "p05_committed_next_offset"
PROVEN_FRONTIER_NEXT_OFFSET = "p05_proven_frontier_next_offset"
BUFFER_BYTES = "p05_buffer_bytes"
INFLIGHT_BATCHES = "p05_inflight_batches"
KAFKA_LAG_OFFSETS = "p05_kafka_lag_offsets"
ESTIMATED_LAG_AGE_SECONDS = "p05_estimated_lag_age_seconds"
PARTITION_STATE = "p05_partition_state"
IDENTITY_CONFLICTS = "p05_identity_conflicts_total"
RECONCILIATION_MISSING = "p05_reconciliation_missing_total"
CH_PARTS_ACTIVE = "p05_ch_parts_active"
PROCESS_RSS_BYTES = "p05_process_rss_bytes"
FAULT_INJECTION_CONFIRMED = "p05_fault_injection_confirmed_total"
QUARANTINED_RECORDS = "p05_quarantined_records_total"
REBALANCES = "p05_rebalances_total"
STALE_COMPLETIONS_FENCED = "p05_stale_completions_fenced_total"
