"""Exact percentiles, censoring and the arithmetic P08 refuses to do.

Four rules from sections 4.3, 4.4 and 19.2 are implemented here, and three of
them are implemented as *refusals*:

1. **Nearest-rank, exact.** FIX-002 pins the definition: with the ten samples
   ``[100..180, 1000]`` ms, ``p95`` is the element at rank ``ceil(0.95x10)=10``,
   so **1000 ms**. Not the mean (226 ms), not an interpolated 820 ms.
2. **Timeouts stay in the denominator.** A request that never finished has no
   latency; it has a *lower bound*. :func:`percentile_with_censoring` keeps it
   in ``n_considered`` and, when the requested rank lands inside the censored
   tail, returns a lower bound with ``value_ms=None`` rather than a number.
3. **No percentile of percentiles.** Averaging per-second or per-partition p95s
   is not a p95 of anything (section 19.2), so :func:`average_of_percentiles`
   exists only to raise.
4. **No heterogeneous claims.** A p95 is meaningless without its query family,
   concurrency, cache profile and N (INV-17), so :class:`PercentileClaim`
   carries all four and refuses to merge claims that disagree (case E2E-39).

Confidence intervals are bootstrapped over **run-level** values, never over the
millions of requests inside one run: those share a host, a cache and a minute of
wall-clock, so treating them as independent samples manufactures a tight
interval out of nothing (section 4.4, point 4).
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Sequence

from .errors import MetrologyError

#: NFR-08 guardrail: completed samples below this make a p95 a discovery
#: number, not a claim. It is a floor on obvious nonsense, not evidence of a
#: confidence interval.
MIN_COMPLETED_SAMPLES_FOR_P95 = 10_000


class PercentileBasis(str, Enum):
    """Which population a percentile was taken over.

    The distinction is the whole point: ``COMPLETED_ONLY`` is a *conditional*
    distribution ("given that it finished"), and reporting it without the
    timeout fraction beside it is the survivorship bias section 4.3 names.
    """

    COMPLETED_ONLY = "COMPLETED_ONLY"
    ATTEMPTED_WITH_CENSORING = "ATTEMPTED_WITH_CENSORING"
    SCHEDULE_TO_COMPLETION = "SCHEDULE_TO_COMPLETION"


def _validate_q(q: float) -> None:
    if not (0.0 < q <= 1.0):
        raise MetrologyError(
            "quantile must be a fraction in (0, 1]; 0.95 not 95", q=q
        )


def nearest_rank_index(n: int, q: float) -> int:
    """Zero-based index of the nearest-rank quantile over ``n`` ordered values.

    ``rank = ceil(q * n)`` with a floor of 1, per FIX-002. Returned zero-based
    for indexing, so ``n=10, q=0.95`` gives index 9 -- the tenth value.
    """
    _validate_q(q)
    if n <= 0:
        raise MetrologyError("no samples to take a quantile over", n=n)
    rank = math.ceil(q * n)
    return min(max(rank, 1), n) - 1


def exact_nearest_rank(values: Sequence[float], q: float) -> float:
    """Exact nearest-rank quantile. Sorts a copy; never mutates the input."""
    if not values:
        raise MetrologyError("no samples to take a quantile over", q=q)
    ordered = sorted(values)
    return ordered[nearest_rank_index(len(ordered), q)]


@dataclass(frozen=True)
class PercentileResult:
    """A quantile plus everything needed to judge whether it means anything."""

    q: float
    value_ms: float | None
    basis: PercentileBasis
    n_considered: int
    n_completed: int
    n_censored: int
    is_lower_bound: bool = False
    lower_bound_ms: float | None = None
    method: str = "nearest_rank"
    reason: str | None = None

    @property
    def reportable(self) -> bool:
        """True when this result is a number a report may publish as a p-value."""
        return self.value_ms is not None and not self.is_lower_bound

    @property
    def censored_fraction(self) -> float:
        return self.n_censored / self.n_considered if self.n_considered else 0.0

    def meets_budget_ms(self, budget_ms: float) -> bool | None:
        """Whether the quantile is within budget, or ``None`` if unknowable.

        A censored lower bound can still *fail* a budget: if the 95th percentile
        is known to be at least 5000 ms, a 280 ms budget is missed regardless of
        the exact value. It can never *pass* one.
        """
        if self.reportable:
            return self.value_ms <= budget_ms
        if self.lower_bound_ms is not None and self.lower_bound_ms > budget_ms:
            return False
        return None

    def as_dict(self) -> dict[str, Any]:
        return {
            "q": self.q,
            "value_ms": self.value_ms,
            "basis": self.basis.value,
            "n_considered": self.n_considered,
            "n_completed": self.n_completed,
            "n_censored": self.n_censored,
            "is_lower_bound": self.is_lower_bound,
            "lower_bound_ms": self.lower_bound_ms,
            "method": self.method,
            "reason": self.reason,
        }


def percentile_completed_only(
    completed_ms: Sequence[float], q: float, *, n_censored: int = 0
) -> PercentileResult:
    """Conditional quantile over requests that finished.

    ``n_censored`` is carried through even though it does not affect the value,
    so a reader always sees how much of the population this number excludes
    (case E2E-41).
    """
    if not completed_ms:
        return PercentileResult(
            q=q,
            value_ms=None,
            basis=PercentileBasis.COMPLETED_ONLY,
            n_considered=n_censored,
            n_completed=0,
            n_censored=n_censored,
            reason="no completed samples",
        )
    value = exact_nearest_rank(completed_ms, q)
    return PercentileResult(
        q=q,
        value_ms=value,
        basis=PercentileBasis.COMPLETED_ONLY,
        n_considered=len(completed_ms) + n_censored,
        n_completed=len(completed_ms),
        n_censored=n_censored,
    )


def percentile_with_censoring(
    completed_ms: Sequence[float],
    q: float,
    *,
    n_censored: int,
    deadline_ms: float,
) -> PercentileResult:
    """Quantile over *all* attempts, treating timeouts as right-censored.

    Censored observations are known only to be ``>= deadline_ms``. They are the
    largest values in the population, so they occupy the top ranks. If the
    requested rank lands among them the honest answer is "at least the
    deadline", which is returned as ``is_lower_bound=True`` with
    ``value_ms=None`` -- a report cannot accidentally print it as a measurement.

    Assigning ``deadline_ms`` as if it were an observed latency would understate
    the tail; assigning 0, or dropping the timeouts, would understate it
    catastrophically (section 4.3).
    """
    _validate_q(q)
    if n_censored < 0:
        raise MetrologyError("censored count may not be negative", n_censored=n_censored)
    if deadline_ms <= 0:
        raise MetrologyError("deadline must be positive", deadline_ms=deadline_ms)

    n_completed = len(completed_ms)
    n_total = n_completed + n_censored
    if n_total == 0:
        return PercentileResult(
            q=q,
            value_ms=None,
            basis=PercentileBasis.ATTEMPTED_WITH_CENSORING,
            n_considered=0,
            n_completed=0,
            n_censored=0,
            reason="no attempts",
        )

    index = nearest_rank_index(n_total, q)
    if index >= n_completed:
        return PercentileResult(
            q=q,
            value_ms=None,
            basis=PercentileBasis.ATTEMPTED_WITH_CENSORING,
            n_considered=n_total,
            n_completed=n_completed,
            n_censored=n_censored,
            is_lower_bound=True,
            lower_bound_ms=deadline_ms,
            reason=(
                f"rank {index + 1} of {n_total} falls in the censored tail; "
                f"quantile is >= deadline {deadline_ms} ms"
            ),
        )
    ordered = sorted(completed_ms)
    return PercentileResult(
        q=q,
        value_ms=ordered[index],
        basis=PercentileBasis.ATTEMPTED_WITH_CENSORING,
        n_considered=n_total,
        n_completed=n_completed,
        n_censored=n_censored,
    )


def average_of_percentiles(*values: float) -> float:
    """Always raises. Kept as a named refusal so the mistake has a home.

    Section 19.2: per-partition or per-second p95s come from distributions with
    different volumes; their mean is not a quantile of the pooled population.
    The fix is to pool the raw samples and take one quantile.
    """
    raise MetrologyError(
        "averaging percentiles does not produce a percentile; pool the raw "
        "samples and take a single nearest-rank quantile instead",
        inputs=len(values),
    )


@dataclass(frozen=True)
class PercentileClaim:
    """A quantile bound to the workload it describes (INV-17).

    Without these labels a "p95 = 280 ms" is not checkable: it could be Q01 at
    concurrency 1 on a warm cache, or Q06 cold. Case E2E-39 is the test that a
    mixed-family claim is refused rather than averaged.
    """

    result: PercentileResult
    query_family: str
    concurrency: int
    cache_profile: str
    dataset_build_id: str | None = None
    labels: dict[str, str] = field(default_factory=dict)

    @property
    def signature(self) -> tuple[str, int, str, str | None]:
        return (self.query_family, self.concurrency, self.cache_profile, self.dataset_build_id)

    def comparable_with(self, other: "PercentileClaim") -> bool:
        return self.signature == other.signature

    def require_comparable(self, other: "PercentileClaim") -> None:
        if not self.comparable_with(other):
            raise MetrologyError(
                "refusing to compare percentile claims from different workloads",
                left=list(self.signature),
                right=list(other.signature),
            )

    def meets_budget_ms(self, budget_ms: float) -> bool | None:
        return self.result.meets_budget_ms(budget_ms)

    def as_dict(self) -> dict[str, Any]:
        return {
            "query_family": self.query_family,
            "concurrency": self.concurrency,
            "cache_profile": self.cache_profile,
            "dataset_build_id": self.dataset_build_id,
            "labels": dict(sorted(self.labels.items())),
            **self.result.as_dict(),
        }


def pooled_claim(
    claims: Sequence[PercentileClaim],
    raw_samples_ms: Sequence[float],
    q: float,
    *,
    n_censored: int = 0,
    deadline_ms: float | None = None,
) -> PercentileClaim:
    """One quantile over pooled raw samples from homogeneous claims.

    This is the sanctioned way to combine: the claims must share a signature,
    and the value comes from the pooled *samples*, never from the claims'
    quantiles. Heterogeneous input raises (case E2E-39).
    """
    if not claims:
        raise MetrologyError("no claims to pool")
    head = claims[0]
    for other in claims[1:]:
        head.require_comparable(other)
    if deadline_ms is None:
        result = percentile_completed_only(raw_samples_ms, q, n_censored=n_censored)
    else:
        result = percentile_with_censoring(
            raw_samples_ms, q, n_censored=n_censored, deadline_ms=deadline_ms
        )
    return PercentileClaim(
        result=result,
        query_family=head.query_family,
        concurrency=head.concurrency,
        cache_profile=head.cache_profile,
        dataset_build_id=head.dataset_build_id,
        labels={"pooled_from_claims": str(len(claims))},
    )


@dataclass(frozen=True)
class ConfidenceInterval:
    """A bootstrap interval over run-level statistics."""

    point: float
    low: float
    high: float
    confidence: float
    n_runs: int
    iterations: int
    seed: int
    statistic: str = "median"

    @property
    def width(self) -> float:
        return self.high - self.low

    def relative_width(self) -> float | None:
        if self.point == 0:
            return None
        return self.width / abs(self.point)

    def excludes_zero(self) -> bool:
        return self.low > 0 or self.high < 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "statistic": self.statistic,
            "point": self.point,
            "low": self.low,
            "high": self.high,
            "confidence": self.confidence,
            "n_runs": self.n_runs,
            "iterations": self.iterations,
            "seed": self.seed,
        }


def median(values: Sequence[float]) -> float:
    if not values:
        raise MetrologyError("median of an empty sequence")
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def bootstrap_ci(
    run_values: Sequence[float],
    *,
    confidence: float = 0.95,
    iterations: int = 2000,
    seed: int = 20261030,
    statistic: str = "median",
) -> ConfidenceInterval:
    """Percentile-bootstrap interval over **run-level** values.

    One value per run. Feeding per-request latencies here would be the error
    section 4.4 warns about, so the parameter is named ``run_values`` and the
    result records ``n_runs`` -- a report showing ``n_runs=1`` is visibly not
    evidence of anything.

    Seeded, so the same inputs give the same interval in CI.
    """
    if len(run_values) < 2:
        raise MetrologyError(
            "a bootstrap interval needs at least 2 independent runs",
            n_runs=len(run_values),
        )
    if not (0.0 < confidence < 1.0):
        raise MetrologyError("confidence must be in (0, 1)", confidence=confidence)
    if iterations < 100:
        raise MetrologyError("too few bootstrap iterations", iterations=iterations)

    stat = {"median": median, "mean": lambda v: sum(v) / len(v)}.get(statistic)
    if stat is None:
        raise MetrologyError("unsupported statistic", statistic=statistic)

    rng = random.Random(seed)
    n = len(run_values)
    draws = []
    for _ in range(iterations):
        resample = [run_values[rng.randrange(n)] for _ in range(n)]
        draws.append(stat(resample))
    draws.sort()
    alpha = (1.0 - confidence) / 2.0
    low = draws[nearest_rank_index(iterations, alpha)]
    high = draws[nearest_rank_index(iterations, 1.0 - alpha)]
    return ConfidenceInterval(
        point=stat(list(run_values)),
        low=low,
        high=high,
        confidence=confidence,
        n_runs=n,
        iterations=iterations,
        seed=seed,
        statistic=statistic,
    )


def sample_count_adequate(n_completed: int, *, minimum: int = MIN_COMPLETED_SAMPLES_FOR_P95) -> bool:
    return n_completed >= minimum


def describe_distribution(values: Iterable[float]) -> dict[str, Any]:
    """p50/p95/p99/min/max/N for a completed-sample set.

    No mean. A mean next to a p95 invites the FIX-002 substitution, and nothing
    in P08 needs one.
    """
    ordered = sorted(values)
    if not ordered:
        return {"n": 0, "p50": None, "p95": None, "p99": None, "min": None, "max": None}
    return {
        "n": len(ordered),
        "p50": exact_nearest_rank(ordered, 0.50),
        "p95": exact_nearest_rank(ordered, 0.95),
        "p99": exact_nearest_rank(ordered, 0.99),
        "min": ordered[0],
        "max": ordered[-1],
        "method": "nearest_rank_exact",
    }
