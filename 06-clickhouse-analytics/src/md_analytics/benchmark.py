"""Query benchmark harness and the section 18.2 report template.

Section 12. The measurement rules are stricter than the measurement:

* **Correctness first.** A latency number from a run whose bars disagree with the
  oracle is not a fast system, it is a wrong one. :func:`run_query_benchmark`
  takes a ``correctness_verdict`` and refuses to report ``PASS`` without it
  (section 12.1).
* **Errors are never dropped from the statistics.** A timeout excluded from a p95
  is how a failing system reports a good p95 (section 21.2). Errors are counted
  and the verdict goes ``INCONCLUSIVE``.
* **Cold and warm are separate populations**, never pooled -- their mixture ratio
  would silently set the result.
* **280 ms is a reference, not an SLO.** The CV gives two numbers and no SQL, no
  hardware and no workload, so a re-run here is a *new* benchmark. Anything
  measured under a different setup is labelled ``measured_on_new_profile``, which
  is what :func:`verdict_for_target` emits.
* **A monotonic clock.** Wall clock jumps; :func:`time.perf_counter_ns` does not.

Percentiles use nearest-rank on the sorted sample, stated explicitly because
"p95" is ambiguous between a dozen interpolation conventions and a benchmark
comparison across runs needs them computed the same way.
"""

from __future__ import annotations

import math
import statistics
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from .errors import EvidenceClass, Verdict

#: The CV's reference figures for the full programme, not a target for this lab.
CV_BASELINE_P95_MS = 2_400.0
CV_OPTIMIZED_P95_MS = 280.0


def percentile_nearest_rank(samples: Sequence[float], q: float) -> float:
    """Nearest-rank percentile: ``ceil(q * n)``-th smallest, 1-indexed.

    No interpolation. With 200 samples the 95th percentile is the 190th value --
    an actual observation, which is easier to defend than a number that no query
    ever produced.
    """
    if not samples:
        raise ValueError("percentile of an empty sample")
    if not 0 < q <= 1:
        raise ValueError(f"q must be in (0, 1], got {q}")
    ordered = sorted(samples)
    rank = max(1, math.ceil(q * len(ordered)))
    return ordered[rank - 1]


@dataclass(frozen=True, slots=True)
class LatencyStats:
    count: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float
    mean_ms: float
    stdev_ms: float

    @classmethod
    def of(cls, samples_ms: Sequence[float]) -> "LatencyStats":
        if not samples_ms:
            return cls(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        return cls(
            count=len(samples_ms),
            p50_ms=percentile_nearest_rank(samples_ms, 0.50),
            p95_ms=percentile_nearest_rank(samples_ms, 0.95),
            p99_ms=percentile_nearest_rank(samples_ms, 0.99),
            min_ms=min(samples_ms),
            max_ms=max(samples_ms),
            mean_ms=statistics.fmean(samples_ms),
            stdev_ms=statistics.stdev(samples_ms) if len(samples_ms) > 1 else 0.0,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "p50_ms": round(self.p50_ms, 3),
            "p95_ms": round(self.p95_ms, 3),
            "p99_ms": round(self.p99_ms, 3),
            "min_ms": round(self.min_ms, 3),
            "max_ms": round(self.max_ms, 3),
            "mean_ms": round(self.mean_ms, 3),
            "stdev_ms": round(self.stdev_ms, 3),
        }


@dataclass(frozen=True, slots=True)
class WorkloadSpec:
    """One named query template plus the callable that runs it."""

    template: str
    run: Callable[[], int]
    description: str = ""


@dataclass(slots=True)
class BenchmarkResult:
    """Measurements for one template, with errors kept in view."""

    template: str
    repetitions: int
    cold: LatencyStats
    warm: LatencyStats
    rows_returned: int = 0
    errors: int = 0
    error_samples: list[str] = field(default_factory=list)

    @property
    def error_rate(self) -> float:
        total = self.repetitions
        return 0.0 if total == 0 else self.errors / total

    def as_dict(self) -> dict[str, Any]:
        return {
            "template": self.template,
            "repetitions": self.repetitions,
            "rows_returned": self.rows_returned,
            "errors": self.errors,
            "error_rate": round(self.error_rate, 6),
            "error_samples": self.error_samples[:5],
            "cold": self.cold.as_dict(),
            "warm": self.warm.as_dict(),
        }


def run_query_benchmark(
    workload: WorkloadSpec,
    *,
    repetitions: int,
    cold_repetitions: int = 1,
) -> BenchmarkResult:
    """Time a workload, keeping cold and warm samples apart.

    The first ``cold_repetitions`` calls are recorded as cold and the rest as
    warm. No warm-up is silently discarded: a discarded measurement is still a
    measurement, and dropping it unannounced is how a report flatters itself.
    """
    if repetitions < 1:
        raise ValueError(f"repetitions must be >= 1, got {repetitions}")
    cold_samples: list[float] = []
    warm_samples: list[float] = []
    errors: list[str] = []
    rows = 0

    for index in range(repetitions):
        started = time.perf_counter_ns()
        try:
            rows = workload.run()
        except Exception as exc:  # noqa: BLE001 -- an error is a result here
            errors.append(f"{type(exc).__name__}: {exc}")
            continue
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
        (cold_samples if index < cold_repetitions else warm_samples).append(elapsed_ms)

    return BenchmarkResult(
        template=workload.template,
        repetitions=repetitions,
        cold=LatencyStats.of(cold_samples),
        warm=LatencyStats.of(warm_samples),
        rows_returned=rows,
        errors=len(errors),
        error_samples=errors,
    )


def verdict_for_target(
    result: BenchmarkResult,
    *,
    target_p95_ms: float,
    correctness_verdict: Verdict,
    dataset_unique_rows: int,
    required_unique_rows: int,
    same_profile_as_reference: bool,
    evidence_class: str | None = None,
) -> tuple[Verdict, str]:
    """Classify a latency result exactly as section 12.1 prescribes.

    The ordering is the content: the evidence has to be of the right kind,
    correctness outranks latency, an error rate voids the sample, a dataset that
    is not the agreed size voids the comparison, and only then does the number
    matter. A good p95 over a wrong build is a global NO-GO, not a performance
    pass.

    ``evidence_class`` must be ``REAL_CLICKHOUSE`` for a latency verdict to mean
    anything. A query timed against the in-process model measures Python
    dictionaries; the number is real and the *claim* would not be, so it comes
    back ``INCONCLUSIVE``. Passing ``None`` skips the check and is for unit tests
    of the other rules only.
    """
    if evidence_class is not None and evidence_class != EvidenceClass.REAL_CLICKHOUSE:
        return (
            Verdict.INCONCLUSIVE,
            f"evidence class is {evidence_class}: this timed the in-process model, "
            f"not ClickHouse, so it cannot support a latency claim (sections 4, 22)",
        )
    if correctness_verdict is not Verdict.PASS:
        return (
            Verdict.NO_GO,
            f"correctness verdict is {correctness_verdict}; a latency result over an "
            f"unverified build is not reportable",
        )
    if result.errors:
        return (
            Verdict.INCONCLUSIVE,
            f"{result.errors} of {result.repetitions} queries errored; excluding them "
            f"from the distribution would manufacture a better p95",
        )
    if result.warm.count == 0:
        return Verdict.INCONCLUSIVE, "no warm samples were collected"
    if dataset_unique_rows < required_unique_rows:
        return (
            Verdict.INCONCLUSIVE,
            f"dataset has {dataset_unique_rows} unique rows, below the "
            f"{required_unique_rows} the comparison requires; physical duplicates "
            f"and quotes do not count towards it",
        )

    observed = result.warm.p95_ms
    if observed <= target_p95_ms:
        if not same_profile_as_reference:
            return (
                Verdict.PASS,
                f"p95 {observed:.1f} ms <= {target_p95_ms:.0f} ms, "
                f"measured_on_new_profile: this is a new benchmark, not a "
                f"reproduction of the CV figure",
            )
        return Verdict.PASS, f"p95 {observed:.1f} ms <= {target_p95_ms:.0f} ms"
    return (
        Verdict.FAIL,
        f"performance NOT_MET: p95 {observed:.1f} ms > {target_p95_ms:.0f} ms. "
        f"This is a performance result and not a correctness failure",
    )


def report_template(**overrides: Any) -> dict[str, Any]:
    """The section 18.2 YAML report as a dict, defaulting to ``TO_FILL``.

    Shipping the skeleton with ``NOT_RUN`` and ``TO_FILL`` in place means an
    unfinished report is obviously unfinished, rather than looking like a result
    with some blanks.
    """
    template: dict[str, Any] = {
        "run_id": "P06-YYYYMMDD-001",
        "status": str(Verdict.NOT_RUN),
        "clickhouse_version": "TO_FILL",
        "clickhouse_image_digest": "TO_FILL",
        "hostname_cpu_ram_disk_fs": "TO_FILL",
        "cgroup_limits": "TO_FILL",
        "schema_hashes": {"raw": "TO_FILL", "canonical": "TO_FILL", "mv": "TO_FILL", "bars": "TO_FILL"},
        "manifest_sha256": "TO_FILL",
        "build_id": "TO_FILL",
        "dataset_unique_events": "TO_FILL",
        "raw_physical_rows": "TO_FILL",
        "raw_duplicate_id_count": "TO_FILL",
        "raw_conflict_id_count": "TO_FILL",
        "canonical_unique_ids": "TO_FILL",
        "bar_count": "TO_FILL",
        "bar_tick_count": "TO_FILL",
        "source_kafka_cuts": "TO_FILL",
        "workload_sql_sha256": "TO_FILL",
        "query_templates": ["range_1m", "range_1h", "range_1d", "ohlc_1m"],
        "cache_profile": "cold_or_warm",
        "writers_offered_events_s": "TO_FILL",
        "p05_acknowledged_events_s": "TO_FILL",
        "writer_kafka_lag_age_p95_ms": "TO_FILL",
        "p95_client_query_ms": "TO_FILL",
        "p95_server_query_ms": "TO_FILL",
        "p99_client_query_ms": "TO_FILL",
        "query_error_rate": "TO_FILL",
        "read_rows_by_template": "TO_FILL",
        "parts_per_partition_peak": "TO_FILL",
        "merge_backlog_age_s": "TO_FILL",
        "cpu_memory_disk_peak": "TO_FILL",
        "fault_trigger_proof": "TO_FILL",
        "bar_reference_mismatch_count": "TO_FILL",
        "missing_ids": "TO_FILL",
        "extra_ids": "TO_FILL",
        "recovery_result": "TO_FILL",
        "release_verdict": "TO_FILL",
        "notes": "No performance claims before execution",
    }
    unknown = sorted(set(overrides) - set(template))
    if unknown:
        raise ValueError(f"unknown report fields: {unknown}")
    template.update(overrides)
    return template


def storage_estimate(*, offered_rate_events_s: int, bytes_per_event: int) -> dict[str, int]:
    """The section 18.1 planning arithmetic. Assumption in, assumption out.

    ``bytes_per_event`` must come from ``system.parts`` on the host in question.
    The design is explicit that no fixed per-row size may be assumed, so this
    function computes what the caller asserted and labels it an estimate -- it
    does not know anything about any real table.
    """
    if offered_rate_events_s <= 0 or bytes_per_event <= 0:
        raise ValueError("rate and bytes_per_event must be positive")
    per_day = 86_400 * offered_rate_events_s * bytes_per_event
    return {
        "raw_bytes_per_day": per_day,
        "raw_gb_per_day_decimal": per_day // 1_000_000_000,
        "bytes_for_200m_rows": 200_000_000 * bytes_per_event,
        "gb_for_200m_rows_decimal": (200_000_000 * bytes_per_event) // 1_000_000_000,
    }


def summarise(results: Iterable[BenchmarkResult]) -> dict[str, Any]:
    results = list(results)
    return {
        "templates": [r.as_dict() for r in results],
        "total_errors": sum(r.errors for r in results),
        "total_queries": sum(r.repetitions for r in results),
    }
