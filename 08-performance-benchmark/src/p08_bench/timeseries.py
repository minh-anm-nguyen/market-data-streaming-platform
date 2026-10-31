"""Windowed series, slopes and what counts as a stable backlog (section 4.2, FIX-003).

FIX-003 is the reason this module is not a one-liner. At 120k offered against a
100k sink, the backlog grows by 20k/s; a queue with 1M free slots fills in about
50 seconds. A run that samples only the *final* backlog can see a small number
-- because the queue capped, or because retention deleted the segments, or
because the drain happened after the measurement window -- and call it stable.

So stability here requires three things that a snapshot cannot give:

* **enough windows** (:data:`MIN_STABILITY_WINDOWS`), because two points are a
  line through anything;
* **both slopes**: over the whole steady phase *and* over its tail. Section 4.2
  asks for "no meaningful growth in the last five minutes **and** no systematic
  growth across the whole steady phase", and a backlog that climbs for 25
  minutes then plateaus satisfies only the second;
* **no retention truncation**, because a backlog that went to zero when Kafka
  deleted the segments did not get processed (FIX-003, case E2E-30).

Per-partition series are kept separate on purpose. INV-09: a global average hides
one saturated hot partition behind seven idle ones, so decisions are taken on
the worst partition, and :meth:`PartitionedBacklog.mean_slope_per_s` exists only
as a diagnostic that is never consulted for a verdict.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Sequence

from .errors import MetrologyError
from .units import Unit

#: A slope fitted over fewer windows than this is not evidence of stability.
#: Five is a floor, not a sufficiency claim; the run records the value used.
MIN_STABILITY_WINDOWS = 5

#: How many trailing windows form the "last five minutes" check of section 4.2
#: when the caller does not say.
DEFAULT_TAIL_WINDOWS = 5


@dataclass(frozen=True)
class Point:
    """One sample: a monotonic timestamp and a value in a declared unit."""

    t_ns: int
    value: float


class WindowedSeries:
    """An ordered series of samples with an ordinary least-squares slope.

    Timestamps must be non-decreasing: an out-of-order sample would silently
    change the fitted slope, and the usual cause is two probes writing into one
    series.
    """

    __slots__ = ("name", "unit", "_points")

    def __init__(self, name: str, unit: Unit) -> None:
        self.name = name
        self.unit = unit
        self._points: list[Point] = []

    def add(self, t_ns: int, value: float) -> None:
        if self._points and t_ns < self._points[-1].t_ns:
            raise MetrologyError(
                "series timestamps must be non-decreasing",
                series=self.name,
                previous_ns=self._points[-1].t_ns,
                t_ns=t_ns,
            )
        self._points.append(Point(int(t_ns), float(value)))

    def extend(self, samples: Sequence[tuple[int, float]]) -> None:
        for t_ns, value in samples:
            self.add(t_ns, value)

    # -- shape ------------------------------------------------------------
    @property
    def points(self) -> list[Point]:
        return list(self._points)

    def __len__(self) -> int:
        return len(self._points)

    @property
    def span_s(self) -> float:
        if len(self._points) < 2:
            return 0.0
        return (self._points[-1].t_ns - self._points[0].t_ns) / 1e9

    @property
    def first(self) -> Point | None:
        return self._points[0] if self._points else None

    @property
    def last(self) -> Point | None:
        return self._points[-1] if self._points else None

    def tail(self, windows: int) -> "WindowedSeries":
        if windows < 1:
            raise MetrologyError("tail needs at least one window", windows=windows)
        out = WindowedSeries(f"{self.name}.tail{windows}", self.unit)
        out._points = list(self._points[-windows:])
        return out

    # -- fitting ----------------------------------------------------------
    def slope_per_s(self) -> float | None:
        """OLS slope in ``unit`` per second, or ``None`` with under two points.

        ``None`` rather than 0.0: "no trend measured" and "a measured trend of
        zero" are the two claims section 4.2 refuses to conflate.
        """
        n = len(self._points)
        if n < 2:
            return None
        t0 = self._points[0].t_ns
        xs = [(p.t_ns - t0) / 1e9 for p in self._points]
        ys = [p.value for p in self._points]
        mean_x = sum(xs) / n
        mean_y = sum(ys) / n
        denom = sum((x - mean_x) ** 2 for x in xs)
        if denom == 0:
            return None
        num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        return num / denom

    def r_squared(self) -> float | None:
        slope = self.slope_per_s()
        if slope is None:
            return None
        t0 = self._points[0].t_ns
        xs = [(p.t_ns - t0) / 1e9 for p in self._points]
        ys = [p.value for p in self._points]
        mean_x = sum(xs) / len(xs)
        mean_y = sum(ys) / len(ys)
        intercept = mean_y - slope * mean_x
        ss_tot = sum((y - mean_y) ** 2 for y in ys)
        ss_res = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, ys))
        if ss_tot == 0:
            return 1.0 if ss_res == 0 else 0.0
        return 1.0 - ss_res / ss_tot

    def as_dict(self) -> dict[str, Any]:
        return {
            "series": self.name,
            "unit": self.unit.value,
            "n_windows": len(self._points),
            "span_s": round(self.span_s, 6),
            "first_value": self._points[0].value if self._points else None,
            "last_value": self._points[-1].value if self._points else None,
            "slope_per_s": self.slope_per_s(),
            "r_squared": self.r_squared(),
        }


class Stability(str, Enum):
    """Verdict for one backlog series."""

    STABLE = "STABLE"
    GROWING = "GROWING"
    SHRINKING = "SHRINKING"
    INSUFFICIENT_WINDOWS = "INSUFFICIENT_WINDOWS"
    INVALIDATED = "INVALIDATED"


@dataclass(frozen=True)
class StabilityAssessment:
    """Both slopes, the reason, and the verdict they produce together."""

    series_name: str
    verdict: Stability
    full_slope_per_s: float | None
    tail_slope_per_s: float | None
    tolerance_per_s: float
    n_windows: int
    min_windows: int
    tail_windows: int
    reasons: list[str] = field(default_factory=list)

    @property
    def stable(self) -> bool:
        return self.verdict is Stability.STABLE

    @property
    def decided(self) -> bool:
        """False when the data could not settle the question either way."""
        return self.verdict not in (Stability.INSUFFICIENT_WINDOWS, Stability.INVALIDATED)

    def as_dict(self) -> dict[str, Any]:
        return {
            "series": self.series_name,
            "verdict": self.verdict.value,
            "full_slope_per_s": self.full_slope_per_s,
            "tail_slope_per_s": self.tail_slope_per_s,
            "tolerance_per_s": self.tolerance_per_s,
            "n_windows": self.n_windows,
            "min_windows": self.min_windows,
            "tail_windows": self.tail_windows,
            "reasons": list(self.reasons),
        }


def assess_stability(
    series: WindowedSeries,
    *,
    tolerance_per_s: float,
    min_windows: int = MIN_STABILITY_WINDOWS,
    tail_windows: int = DEFAULT_TAIL_WINDOWS,
    retention_truncated: bool = False,
    gap_windows: int = 0,
) -> StabilityAssessment:
    """Decide whether a backlog series is stable.

    ``tolerance_per_s`` is the measurement noise band, in the series unit per
    second. Section 4.2 requires it to be published rather than assumed: "near
    zero" has to be defined against something.

    ``retention_truncated`` and ``gap_windows`` both invalidate instead of
    passing. A series missing samples during the busiest window (case E2E-44)
    cannot be interpolated green.
    """
    if tolerance_per_s < 0:
        raise MetrologyError("tolerance may not be negative", tolerance_per_s=tolerance_per_s)

    reasons: list[str] = []
    n = len(series)

    if retention_truncated:
        reasons.append(
            "retention deleted segments during the window; a backlog that fell "
            "because data was discarded is not a processed backlog"
        )
        return StabilityAssessment(
            series.name, Stability.INVALIDATED, series.slope_per_s(),
            None, tolerance_per_s, n, min_windows, tail_windows, reasons,
        )

    if gap_windows > 0:
        reasons.append(f"{gap_windows} window(s) have no observation; evidence is incomplete")
        return StabilityAssessment(
            series.name, Stability.INVALIDATED, series.slope_per_s(),
            None, tolerance_per_s, n, min_windows, tail_windows, reasons,
        )

    if n < min_windows:
        reasons.append(
            f"{n} window(s) is below the {min_windows}-window floor; a final "
            "snapshot cannot establish stability (FIX-003)"
        )
        return StabilityAssessment(
            series.name, Stability.INSUFFICIENT_WINDOWS, series.slope_per_s(),
            None, tolerance_per_s, n, min_windows, tail_windows, reasons,
        )

    full = series.slope_per_s()
    tail = series.tail(min(tail_windows, n)).slope_per_s()
    if full is None or tail is None:
        reasons.append("slope undefined: all samples share one timestamp")
        return StabilityAssessment(
            series.name, Stability.INSUFFICIENT_WINDOWS, full, tail,
            tolerance_per_s, n, min_windows, tail_windows, reasons,
        )

    if full > tolerance_per_s:
        reasons.append(
            f"full-phase slope {full:.6g}/s exceeds tolerance {tolerance_per_s:.6g}/s"
        )
    if tail > tolerance_per_s:
        reasons.append(
            f"tail slope {tail:.6g}/s over the last {min(tail_windows, n)} windows "
            f"exceeds tolerance {tolerance_per_s:.6g}/s"
        )
    if reasons:
        return StabilityAssessment(
            series.name, Stability.GROWING, full, tail, tolerance_per_s,
            n, min_windows, tail_windows, reasons,
        )

    if full < -tolerance_per_s and tail < -tolerance_per_s:
        reasons.append("backlog is draining across the whole window")
        return StabilityAssessment(
            series.name, Stability.SHRINKING, full, tail, tolerance_per_s,
            n, min_windows, tail_windows, reasons,
        )

    return StabilityAssessment(
        series.name, Stability.STABLE, full, tail, tolerance_per_s,
        n, min_windows, tail_windows, reasons,
    )


class PartitionedBacklog:
    """Per-topic-partition backlog series, judged on the worst member (INV-09)."""

    __slots__ = ("unit", "_series", "_retention_truncated", "_gaps")

    def __init__(self, unit: Unit = Unit.RECORDS) -> None:
        self.unit = unit
        self._series: dict[str, WindowedSeries] = {}
        self._retention_truncated: set[str] = set()
        self._gaps: dict[str, int] = {}

    @staticmethod
    def key(topic: str, partition: int) -> str:
        return f"{topic}-{partition}"

    def series(self, topic: str, partition: int) -> WindowedSeries:
        key = self.key(topic, partition)
        existing = self._series.get(key)
        if existing is None:
            existing = WindowedSeries(key, self.unit)
            self._series[key] = existing
        return existing

    def add(self, topic: str, partition: int, t_ns: int, backlog: float) -> None:
        self.series(topic, partition).add(t_ns, backlog)

    def mark_retention_truncated(self, topic: str, partition: int) -> None:
        self._retention_truncated.add(self.key(topic, partition))

    def mark_gap(self, topic: str, partition: int, windows: int = 1) -> None:
        key = self.key(topic, partition)
        self._gaps[key] = self._gaps.get(key, 0) + windows

    @property
    def keys(self) -> list[str]:
        return sorted(self._series)

    def assess_all(
        self,
        *,
        tolerance_per_s: float,
        min_windows: int = MIN_STABILITY_WINDOWS,
        tail_windows: int = DEFAULT_TAIL_WINDOWS,
    ) -> dict[str, StabilityAssessment]:
        return {
            key: assess_stability(
                series,
                tolerance_per_s=tolerance_per_s,
                min_windows=min_windows,
                tail_windows=tail_windows,
                retention_truncated=key in self._retention_truncated,
                gap_windows=self._gaps.get(key, 0),
            )
            for key, series in sorted(self._series.items())
        }

    def worst(
        self,
        *,
        tolerance_per_s: float,
        min_windows: int = MIN_STABILITY_WINDOWS,
        tail_windows: int = DEFAULT_TAIL_WINDOWS,
    ) -> StabilityAssessment | None:
        """The partition whose verdict governs the run.

        Severity order puts the undecidable verdicts above GROWING: an
        invalidated series means the run cannot be judged at all, which is a
        stronger statement than one saturated partition.
        """
        assessments = self.assess_all(
            tolerance_per_s=tolerance_per_s,
            min_windows=min_windows,
            tail_windows=tail_windows,
        )
        if not assessments:
            return None
        severity = {
            Stability.INVALIDATED: 4,
            Stability.INSUFFICIENT_WINDOWS: 3,
            Stability.GROWING: 2,
            Stability.SHRINKING: 1,
            Stability.STABLE: 0,
        }
        return max(
            assessments.values(),
            key=lambda a: (severity[a.verdict], a.full_slope_per_s or 0.0),
        )

    def mean_slope_per_s(self) -> float | None:
        """Diagnostic only. Never consulted for a verdict (INV-09).

        Kept because it is useful when eyeballing a fleet, and named so that its
        appearance in a decision path is obvious in review.
        """
        slopes = [s.slope_per_s() for s in self._series.values()]
        known = [s for s in slopes if s is not None]
        return sum(known) / len(known) if known else None

    def as_dict(self, *, tolerance_per_s: float) -> dict[str, Any]:
        assessments = self.assess_all(tolerance_per_s=tolerance_per_s)
        worst = self.worst(tolerance_per_s=tolerance_per_s)
        return {
            "unit": self.unit.value,
            "per_partition": {k: v.as_dict() for k, v in assessments.items()},
            "worst": worst.as_dict() if worst else None,
            "mean_slope_per_s_diagnostic_only": self.mean_slope_per_s(),
            "retention_truncated": sorted(self._retention_truncated),
        }


def time_to_full_s(free_capacity: float, arrival_rate: float, service_rate: float) -> float | None:
    """``free / (lambda - mu)`` when arrivals exceed service, else ``None``.

    Section 4.2's formula, with its precondition made explicit: the same unit on
    all three arguments, and no answer at all when the queue is not filling. The
    caller gets ``None`` rather than ``inf`` so a report cannot print "the queue
    fills in infinity seconds" as a safety argument.
    """
    if free_capacity < 0:
        raise MetrologyError("free capacity may not be negative", free_capacity=free_capacity)
    excess = arrival_rate - service_rate
    if excess <= 0:
        return None
    return free_capacity / excess
