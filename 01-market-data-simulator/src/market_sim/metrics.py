"""Counters, gauges and histograms (section 10 of SYSTEM_DESIGN).

Two rules shape this module:

* **Nothing is logged per event.** At 10k events/s a per-event log line is both a
  throughput problem and a way to fill the very disk the dataset is being written
  to (cases F12, R07). Only aggregates live here.
* **Names distinguish what was actually proven.** ``tcp_events_enqueued_total``
  is not ``tcp_bytes_drained_total``, and neither one means "the client received
  it". The naming is deliberate so a report cannot imply a delivery guarantee the
  transport never gave (NFR-08, case N09).

Counters are non-decreasing by construction: :meth:`Metrics.incr` rejects a
negative delta, so a bad accounting change fails fast instead of printing a
negative count in a report (invariant 10 of section 3.2).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Iterable


class Histogram:
    """Sampled distribution with exact count/min/max/sum and decimated samples.

    Keeping every sample would be unbounded memory on a soak run. Keeping a
    *random* sample would make the report non-reproducible. So samples are
    decimated deterministically: when the reservoir fills, every other retained
    sample is dropped and the stride doubles. The kept set stays an even sweep
    over the whole run and depends only on arrival order.

    Percentiles are therefore **estimates** beyond ``capacity`` samples, and
    :meth:`as_dict` reports ``exact`` so a reader knows which they are looking at.
    """

    __slots__ = (
        "_samples",
        "_capacity",
        "_stride",
        "_seen_in_stride",
        "count",
        "total",
        "minimum",
        "maximum",
    )

    def __init__(self, capacity: int = 100_000) -> None:
        if capacity < 2:
            raise ValueError(f"capacity must be >= 2, got {capacity}")
        self._samples: list[float] = []
        self._capacity = capacity
        self._stride = 1
        self._seen_in_stride = 0
        self.count = 0
        self.total = 0.0
        self.minimum: float | None = None
        self.maximum: float | None = None

    def observe(self, value: float) -> None:
        self.count += 1
        self.total += value
        if self.minimum is None or value < self.minimum:
            self.minimum = value
        if self.maximum is None or value > self.maximum:
            self.maximum = value

        self._seen_in_stride += 1
        if self._seen_in_stride < self._stride:
            return
        self._seen_in_stride = 0
        self._samples.append(value)
        if len(self._samples) >= self._capacity:
            self._samples = self._samples[::2]
            self._stride *= 2

    @property
    def exact(self) -> bool:
        """True while every observation is still retained."""
        return self._stride == 1

    @property
    def mean(self) -> float:
        return self.total / self.count if self.count else 0.0

    def percentile(self, q: float) -> float | None:
        """Nearest-rank percentile over the retained samples. ``q`` in [0, 100]."""
        if not (0.0 <= q <= 100.0):
            raise ValueError(f"q must be in [0, 100], got {q}")
        if not self._samples:
            return None
        ordered = sorted(self._samples)
        if q <= 0:
            return ordered[0]
        rank = math.ceil(q / 100.0 * len(ordered))
        return ordered[min(max(rank, 1), len(ordered)) - 1]

    def as_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "mean": round(self.mean, 6),
            "min": self.minimum,
            "max": self.maximum,
            "p50": self.percentile(50),
            "p95": self.percentile(95),
            "p99": self.percentile(99),
            "samples_retained": len(self._samples),
            "exact": self.exact,
        }


@dataclass
class Metrics:
    """The run's metric registry. One instance per run, no globals."""

    counters: dict[str, int] = field(default_factory=dict)
    gauges: dict[str, float] = field(default_factory=dict)
    high_water: dict[str, float] = field(default_factory=dict)
    histograms: dict[str, Histogram] = field(default_factory=dict)

    def incr(self, name: str, delta: int = 1) -> int:
        if delta < 0:
            raise ValueError(
                f"counter {name!r} may not decrease (delta={delta}); counters are "
                "monotonic by contract"
            )
        value = self.counters.get(name, 0) + delta
        self.counters[name] = value
        return value

    def counter(self, name: str) -> int:
        return self.counters.get(name, 0)

    def ensure_counters(self, names: Iterable[str]) -> None:
        """Materialise counters at 0.

        An absent counter and a zero counter are different claims: the first says
        "not measured", the second says "measured, never happened". A report must
        make the zero explicit.
        """
        for name in names:
            self.counters.setdefault(name, 0)

    def set_gauge(self, name: str, value: float) -> None:
        self.gauges[name] = value
        if value > self.high_water.get(name, float("-inf")):
            self.high_water[name] = value

    def gauge(self, name: str) -> float:
        return self.gauges.get(name, 0.0)

    def peak(self, name: str) -> float:
        return self.high_water.get(name, 0.0)

    def observe(self, name: str, value: float, *, capacity: int = 100_000) -> None:
        hist = self.histograms.get(name)
        if hist is None:
            hist = Histogram(capacity)
            self.histograms[name] = hist
        hist.observe(value)

    def histogram(self, name: str) -> Histogram:
        hist = self.histograms.get(name)
        if hist is None:
            hist = Histogram()
            self.histograms[name] = hist
        return hist

    def snapshot(self) -> dict[str, Any]:
        return {
            "counters": dict(sorted(self.counters.items())),
            "gauges": {k: round(v, 6) for k, v in sorted(self.gauges.items())},
            "peaks": {k: round(v, 6) for k, v in sorted(self.high_water.items())},
            "histograms": {k: v.as_dict() for k, v in sorted(self.histograms.items())},
        }


#: Every counter a run must report, even when it stays at zero. Grouped by the
#: boundary each one describes, because that boundary is exactly what the number
#: does and does not prove.
RUN_COUNTERS: tuple[str, ...] = (
    # generation
    "logical_slots_total",
    "events_emitted_total",
    # faults
    "faults_planned_total",
    "faults_applied_total",
    "faults_not_applied_total",
    "intentional_omissions_total",
    "intentional_duplicates_total",
    "intentional_reorders_total",
    "intentional_mutations_total",
    # file sink
    "file_events_enqueued_total",
    "file_events_written_total",
    "file_events_fsynced_total",
    "file_bytes_written_total",
    "file_write_errors_total",
    "file_fsync_total",
    # tcp sink: enqueued != written != drained != received
    "tcp_events_enqueued_total",
    "tcp_events_written_total",
    "tcp_bytes_drained_total",
    "tcp_no_client_events_total",
    "tcp_queue_overflow_total",
    "tcp_slow_disconnect_total",
    "tcp_clients_accepted_total",
    "tcp_clients_rejected_total",
    "tcp_clients_disconnected_total",
    "tcp_send_errors_total",
    "tcp_transport_faults_total",
    # scheduler
    "scheduler_burst_capped_total",
    "scheduler_lag_breach_total",
)
