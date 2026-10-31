"""Counters, histograms and the reconciliation identities (sections 6.3, 11.1).

The identities are the reason this module is not just a dict of counters. Each
stage's input must equal its rejections plus its output:

    json_lines_read      = json_rejected   + mapped
    mapped               = policy_rejected + encoded
    frames_received      = frame_rejected  + submitted_to_decoder
    submitted_to_decoder = decode_errors   + parsed
    parsed               = envelope_rejected + accepted

:meth:`Metrics.reconcile` checks all five. An unbalanced identity means events
went missing between two stages, which is the failure mode a single
"events processed" counter cannot show -- and which is how a benchmark ends up
reporting throughput for events it quietly dropped (section 10.3).

Two counters are deliberately absent. There is no ``peer_accepted_total``,
because MDP1 has no application-level ACK and a counter with that name would be
a lie about what a socket write proves (FR-13). And labels never carry
``dataset_id``, ``symbol`` or a free-form fault string: that is unbounded
cardinality, so diagnosis uses bounded sample logs instead (section 11.1).
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from typing import Iterable


@dataclass
class Histogram:
    """Keeps every sample, so percentiles are exact rather than interpolated.

    Exact is affordable here because a benchmark run is bounded and a sample is
    8 bytes; the ``p99`` of a latency distribution is the number most worth
    getting right, and a bucketed approximation of it can be off by the width of
    a bucket. ``cap`` bounds the memory for a soak run, and ``exact`` reports
    truthfully whether the cap was hit.
    """

    name: str
    samples: list[float] = field(default_factory=list)
    cap: int = 2_000_000
    dropped: int = 0

    def observe(self, value: float) -> None:
        if len(self.samples) < self.cap:
            self.samples.append(value)
        else:
            self.dropped += 1

    @property
    def exact(self) -> bool:
        return self.dropped == 0

    def percentile(self, q: float) -> float | None:
        if not self.samples:
            return None
        ordered = sorted(self.samples)
        if len(ordered) == 1:
            return ordered[0]
        # Nearest-rank on the sorted samples. Stated explicitly because "p99"
        # is meaningless without saying how it was computed.
        rank = q * (len(ordered) - 1)
        low = math.floor(rank)
        high = math.ceil(rank)
        if low == high:
            return ordered[int(rank)]
        return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)

    def summary(self) -> dict[str, object]:
        if not self.samples:
            return {"count": 0, "exact": self.exact}
        return {
            "count": len(self.samples),
            "exact": self.exact,
            "dropped": self.dropped,
            "min": min(self.samples),
            "max": max(self.samples),
            "mean": sum(self.samples) / len(self.samples),
            "p50": self.percentile(0.50),
            "p95": self.percentile(0.95),
            "p99": self.percentile(0.99),
        }


@dataclass
class ReconciliationResult:
    name: str
    left: int
    right: int

    @property
    def balanced(self) -> bool:
        return self.left == self.right

    def as_dict(self) -> dict[str, object]:
        return {
            "identity": self.name,
            "left": self.left,
            "right": self.right,
            "balanced": self.balanced,
        }


@dataclass
class Metrics:
    """Stage counters for one run."""

    counters: Counter[str] = field(default_factory=Counter)
    reasons: Counter[str] = field(default_factory=Counter)
    gauges: dict[str, float] = field(default_factory=dict)
    histograms: dict[str, Histogram] = field(default_factory=dict)
    peaks: dict[str, float] = field(default_factory=dict)

    # --- counters ---------------------------------------------------------
    def incr(self, name: str, amount: int = 1) -> None:
        self.counters[name] += amount

    def reject(self, stage: str, reason: str) -> None:
        """Record a rejection against both its stage and its reason.

        Both, not either: the stage total is what the reconciliation identities
        balance against, and the reason breakdown is what tells an operator
        whether they are looking at a corrupt source or a misconfigured limit.
        """
        self.counters[f"{stage}_rejected_total"] += 1
        self.reasons[reason] += 1

    def gauge(self, name: str, value: float) -> None:
        self.gauges[name] = value
        if value > self.peaks.get(name, float("-inf")):
            self.peaks[name] = value

    def observe(self, name: str, value: float) -> None:
        histogram = self.histograms.get(name)
        if histogram is None:
            histogram = Histogram(name)
            self.histograms[name] = histogram
        histogram.observe(value)

    def get(self, name: str) -> int:
        return self.counters[name]

    # --- reconciliation ---------------------------------------------------
    def reconcile(self) -> list[ReconciliationResult]:
        """Check every stage identity. All must balance for a clean run."""
        c = self.counters
        return [
            ReconciliationResult(
                "json_lines_read == json_rejected + mapped",
                c["json_lines_read_total"],
                c["json_rejected_total"] + c["mapped_total"],
            ),
            ReconciliationResult(
                "mapped == policy_rejected + encoded",
                c["mapped_total"],
                c["policy_rejected_total"] + c["protobuf_encoded_total"],
            ),
            ReconciliationResult(
                "frames_received == frame_rejected + submitted_to_decoder",
                c["frames_received_total"],
                c["frame_rejected_total"] + c["payloads_submitted_total"],
            ),
            ReconciliationResult(
                "submitted_to_decoder == decode_errors + parsed",
                c["payloads_submitted_total"],
                c["decode_rejected_total"] + c["protobuf_parsed_total"],
            ),
            ReconciliationResult(
                "parsed == envelope_rejected + accepted",
                c["protobuf_parsed_total"],
                c["envelope_rejected_total"] + c["events_accepted_total"],
            ),
        ]

    @property
    def reconciles(self) -> bool:
        return all(r.balanced for r in self.reconcile())

    def unbalanced(self) -> list[ReconciliationResult]:
        return [r for r in self.reconcile() if not r.balanced]

    # --- reporting --------------------------------------------------------
    def as_dict(self) -> dict[str, object]:
        return {
            "counters": dict(sorted(self.counters.items())),
            "rejected_by_reason": dict(sorted(self.reasons.items())),
            "gauges": dict(sorted(self.gauges.items())),
            "peaks": dict(sorted(self.peaks.items())),
            "histograms": {
                name: histogram.summary()
                for name, histogram in sorted(self.histograms.items())
            },
            "reconciliation": [r.as_dict() for r in self.reconcile()],
            "reconciles": self.reconciles,
            # Spelled out rather than omitted silently, so nobody reads the
            # absence of a delivery counter as a delivery guarantee.
            "peer_accepted_total": None,
            "peer_accepted_note": (
                "MDP1 has no application-level ACK; bytes written to a socket "
                "are not proof a peer parsed or accepted them"
            ),
        }


def merge(metrics: Iterable[Metrics]) -> Metrics:
    """Combine metrics from several workers into one.

    Counters and reasons add; gauges take the last value and peaks take the
    maximum, because a peak across workers is the maximum of their peaks but a
    gauge has no meaningful sum.
    """
    out = Metrics()
    for m in metrics:
        out.counters.update(m.counters)
        out.reasons.update(m.reasons)
        out.gauges.update(m.gauges)
        for name, value in m.peaks.items():
            if value > out.peaks.get(name, float("-inf")):
                out.peaks[name] = value
        for name, histogram in m.histograms.items():
            target = out.histograms.setdefault(name, Histogram(name))
            for sample in histogram.samples:
                target.observe(sample)
            target.dropped += histogram.dropped
    return out
