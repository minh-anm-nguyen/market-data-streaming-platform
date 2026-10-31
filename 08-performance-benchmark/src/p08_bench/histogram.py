"""Bucketed histograms and whether their resolution can answer the question.

A Prometheus-style histogram gives an *estimated* quantile whose error is set by
the bucket layout (section 4.3, reference 4). That is fine for dashboards and
fatal for a 280 ms gate: with buckets at ``[200, 500]`` ms, every value from
201 to 500 looks the same, so the histogram cannot distinguish 270 ms from
450 ms. Case E2E-40 requires that situation to come out ``INCONCLUSIVE`` rather
than as a confident green.

So this module does two things and refuses a third:

* it returns a quantile as an **interval** (:meth:`BucketHistogram.quantile_interval`),
  never a single interpolated number;
* it judges its own fitness for a threshold
  (:meth:`BucketHistogram.resolution_for_threshold`);
* it will not report a maximum when the open-ended top bucket is occupied,
  because the last bucket has no upper edge to report (section 4.3).

Exact percentiles come from :mod:`p08_bench.percentiles` over retained samples.
A histogram is a secondary, memory-bounded view and is labelled as one
everywhere it appears.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from enum import Enum
from typing import Any, Sequence

from .errors import MetrologyError

#: Default ceiling on bucket width relative to the threshold being tested.
#: 0.10 means a 280 ms gate needs buckets no wider than 28 ms where it matters.
#: A lab guardrail, recorded per run, not a universal standard.
DEFAULT_MAX_BUCKET_WIDTH_FRACTION = 0.10


class Resolution(str, Enum):
    ADEQUATE = "ADEQUATE"
    INSUFFICIENT = "INSUFFICIENT"
    OUT_OF_RANGE = "OUT_OF_RANGE"


@dataclass(frozen=True)
class QuantileInterval:
    """A bucketed quantile: the answer is somewhere in ``(low, high]``."""

    q: float
    low: float
    high: float | None
    bucket_index: int
    n: int

    @property
    def is_open_ended(self) -> bool:
        return self.high is None

    @property
    def width(self) -> float | None:
        return None if self.high is None else self.high - self.low

    def contains(self, value: float) -> bool:
        if value <= self.low:
            return False
        return self.high is None or value <= self.high

    def decides_threshold(self, threshold_ms: float) -> bool | None:
        """Whether the interval alone settles a threshold comparison.

        ``True``/``False`` when the whole interval is on one side of the
        threshold; ``None`` when the threshold falls inside it, which is the
        E2E-40 case and must become ``INCONCLUSIVE`` upstream.
        """
        if self.high is not None and self.high <= threshold_ms:
            return True
        if self.low >= threshold_ms:
            return False
        return None

    def as_dict(self) -> dict[str, Any]:
        return {
            "q": self.q,
            "low": self.low,
            "high": self.high,
            "bucket_index": self.bucket_index,
            "n": self.n,
            "estimate": True,
            "method": "bucket_interval",
        }


class BucketHistogram:
    """Fixed-bound histogram with cumulative counts.

    ``bounds`` are inclusive upper edges in ascending order; an implicit
    ``+inf`` bucket catches the rest. Memory is O(len(bounds)) regardless of
    observation count, which is the reason to use one at all.
    """

    __slots__ = ("_bounds", "_counts", "count", "_total", "_min", "_max_observed")

    def __init__(self, bounds: Sequence[float]) -> None:
        if not bounds:
            raise MetrologyError("a histogram needs at least one bucket bound")
        ordered = list(bounds)
        if any(b <= 0 for b in ordered):
            raise MetrologyError("bucket bounds must be positive", bounds=ordered)
        if ordered != sorted(ordered) or len(set(ordered)) != len(ordered):
            raise MetrologyError(
                "bucket bounds must be strictly ascending", bounds=ordered
            )
        self._bounds = ordered
        self._counts = [0] * (len(ordered) + 1)
        self.count = 0
        self._total = 0.0
        self._min: float | None = None
        self._max_observed: float | None = None

    # -- recording --------------------------------------------------------
    def observe(self, value: float) -> None:
        if value != value:
            raise MetrologyError("cannot observe NaN")
        index = bisect.bisect_left(self._bounds, value)
        self._counts[index] += 1
        self.count += 1
        self._total += value
        if self._min is None or value < self._min:
            self._min = value
        if self._max_observed is None or value > self._max_observed:
            self._max_observed = value

    def observe_many(self, values: Sequence[float]) -> None:
        for value in values:
            self.observe(value)

    # -- shape ------------------------------------------------------------
    @property
    def bounds(self) -> list[float]:
        return list(self._bounds)

    @property
    def counts(self) -> list[int]:
        return list(self._counts)

    @property
    def overflow_count(self) -> int:
        """Observations above the highest bound, in the open ``+inf`` bucket."""
        return self._counts[-1]

    def bucket_edges(self, index: int) -> tuple[float, float | None]:
        if not (0 <= index < len(self._counts)):
            raise MetrologyError("bucket index out of range", index=index)
        low = 0.0 if index == 0 else self._bounds[index - 1]
        high = None if index == len(self._bounds) else self._bounds[index]
        return low, high

    # -- reading ----------------------------------------------------------
    def quantile_interval(self, q: float) -> QuantileInterval:
        """Bucket containing the nearest-rank quantile, as an interval."""
        if not (0.0 < q <= 1.0):
            raise MetrologyError("quantile must be a fraction in (0, 1]", q=q)
        if self.count == 0:
            raise MetrologyError("histogram has no observations")
        import math

        target = min(max(math.ceil(q * self.count), 1), self.count)
        seen = 0
        for index, bucket_count in enumerate(self._counts):
            seen += bucket_count
            if seen >= target:
                low, high = self.bucket_edges(index)
                return QuantileInterval(q=q, low=low, high=high, bucket_index=index, n=self.count)
        raise MetrologyError("cumulative count did not reach the target rank")

    def max_reportable(self) -> float | None:
        """The maximum, or ``None`` when it cannot be bounded from buckets.

        Section 4.3: ``max`` cannot be derived from an open final bucket. The
        exact observed maximum is tracked separately and exposed as
        :attr:`observed_max` for diagnostics, but the *bucket-derived* answer is
        honestly ``None``.
        """
        if self.overflow_count > 0:
            return None
        return self._bounds[-1] if self.count else None

    @property
    def observed_max(self) -> float | None:
        """Exact maximum of the observed values (not a bucket inference)."""
        return self._max_observed

    @property
    def observed_min(self) -> float | None:
        return self._min

    def resolution_for_threshold(
        self,
        threshold: float,
        *,
        max_width_fraction: float = DEFAULT_MAX_BUCKET_WIDTH_FRACTION,
    ) -> Resolution:
        """Can this bucket layout decide a comparison against ``threshold``?

        ``INSUFFICIENT`` when the bucket containing the threshold is wider than
        ``threshold * max_width_fraction`` -- the E2E-40 case. ``OUT_OF_RANGE``
        when the threshold sits in the open top bucket, where no width exists.
        """
        if threshold <= 0:
            raise MetrologyError("threshold must be positive", threshold=threshold)
        index = bisect.bisect_left(self._bounds, threshold)
        low, high = self.bucket_edges(index)
        if high is None:
            return Resolution.OUT_OF_RANGE
        width = high - low
        return (
            Resolution.ADEQUATE
            if width <= threshold * max_width_fraction
            else Resolution.INSUFFICIENT
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "bounds": self.bounds,
            "counts": self.counts,
            "n": self.count,
            "overflow_count": self.overflow_count,
            "observed_min": self._min,
            "observed_max": self._max_observed,
            "bucket_max_reportable": self.max_reportable(),
            "estimate": True,
        }


def exponential_bounds(start: float, factor: float, steps: int) -> list[float]:
    """Exponential bucket bounds, e.g. ``exponential_bounds(1, 2, 10)``."""
    if start <= 0:
        raise MetrologyError("start must be positive", start=start)
    if factor <= 1:
        raise MetrologyError("factor must be > 1", factor=factor)
    if steps < 1:
        raise MetrologyError("need at least one step", steps=steps)
    bounds = []
    value = start
    for _ in range(steps):
        bounds.append(value)
        value *= factor
    return bounds


def linear_bounds(start: float, step: float, steps: int) -> list[float]:
    if step <= 0:
        raise MetrologyError("step must be positive", step=step)
    return [start + step * i for i in range(steps)]


def bounds_for_threshold(
    threshold: float,
    *,
    max_width_fraction: float = DEFAULT_MAX_BUCKET_WIDTH_FRACTION,
    span_factor: float = 4.0,
) -> list[float]:
    """Linear bounds fine enough to decide ``threshold``, covering a span around it.

    Provided so a run that needs a 280 ms gate can build a layout that satisfies
    :meth:`BucketHistogram.resolution_for_threshold` instead of discovering
    after the fact that its buckets were too coarse.
    """
    width = threshold * max_width_fraction
    steps = int(threshold * span_factor / width) + 1
    return linear_bounds(width, width, steps)
