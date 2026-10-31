"""Stage counters, the metric registry and cardinality control (sections 4.1, 5.2).

Section 4.1 names nine counters and then spends a paragraph saying what cannot
be done with them: there is no ``offered == raw_rows == canonical_rows``
identity, because rejects, physical retries and partial reads all break it. The
counters are therefore kept as nine *separate* quantities with their own scopes,
and :func:`conservation_report` describes the relationships between them without
asserting an equality that is not true.

Two further rules shape the module:

* **A missing counter is not a zero.** ``None`` means "not measured" and stays
  ``None`` through every derivation, because section 15 forbids an unknown from
  defaulting to the passing value. :meth:`MetricRegistry.ensure_counters` is how
  a real zero becomes explicit.
* **High-cardinality labels never go on a counter.** Section 5.2 is blunt about
  ``dataset_id``, ``event_id``, ``query_id`` and ``symbol``: those belong in the
  evidence store with sampling, not in a metric label where they melt the
  scraper and change the behaviour of the system being measured.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from .errors import MetrologyError
from .histogram import BucketHistogram

#: Label keys that must never appear on a counter or gauge (section 5.2).
#: Each one is unbounded in a benchmark: 200M event ids, one query id per
#: request, one build id per rebuild.
FORBIDDEN_LABEL_KEYS = frozenset(
    {"dataset_id", "event_id", "query_id", "build_id", "symbol", "sequence", "offset"}
)

#: Labels that are safe because their cardinality is fixed by the manifest.
ALLOWED_LABEL_KEYS = frozenset(
    {"stage", "topic", "partition", "query_family", "lane", "profile", "reason", "cache_profile"}
)


class StageCounterName:
    """The nine counters of section 4.1, as names rather than ad-hoc strings.

    The order below is the order of the pipeline, and the gaps between
    neighbours are the diagnostic signal of section 6.1: ``issued`` far below
    ``offered`` is a generator problem, ``producer_ack`` far below
    ``adapter_accepted`` is a broker problem, and so on.
    """

    OFFERED = "offered_count"
    ISSUED = "issued_count"
    SOURCE_SEALED = "source_sealed_count"
    ADAPTER_ACCEPTED = "adapter_accepted_count"
    PRODUCER_ACK = "producer_ack_count"
    KAFKA_PHYSICAL = "kafka_physical_count"
    RAW_PHYSICAL = "raw_physical_count"
    CANONICAL_UNIQUE = "canonical_unique_count"
    ORACLE_EXPECTED = "oracle_expected_count"

    ORDERED: tuple[str, ...] = (
        OFFERED,
        ISSUED,
        SOURCE_SEALED,
        ADAPTER_ACCEPTED,
        PRODUCER_ACK,
        KAFKA_PHYSICAL,
        RAW_PHYSICAL,
        CANONICAL_UNIQUE,
        ORACLE_EXPECTED,
    )


#: What each stage counter does and does not prove. Printed in reports so a
#: reader never has to guess the scope of a number.
STAGE_COUNTER_SCOPES: dict[str, str] = {
    StageCounterName.OFFERED: "schedule slots requested; proves intent only",
    StageCounterName.ISSUED: "events the source actually emitted; not receipt",
    StageCounterName.SOURCE_SEALED: "events with durable manifest; a .part file does not count",
    StageCounterName.ADAPTER_ACCEPTED: "business-admissible per P03 policy; rejects are not loss",
    StageCounterName.PRODUCER_ACK: "producer received an ACK; not canonical READY",
    StageCounterName.KAFKA_PHYSICAL: "log offsets appended, including retry duplicates",
    StageCounterName.RAW_PHYSICAL: "raw rows inserted, may contain physical duplicates",
    StageCounterName.CANONICAL_UNIQUE: "distinct valid identities in the READY build",
    StageCounterName.ORACLE_EXPECTED: "expected accepted unique set, derived independently",
}


@dataclass
class StageCounters:
    """The nine counters, each either measured or explicitly unknown."""

    offered_count: int | None = None
    issued_count: int | None = None
    source_sealed_count: int | None = None
    adapter_accepted_count: int | None = None
    producer_ack_count: int | None = None
    kafka_physical_count: int | None = None
    raw_physical_count: int | None = None
    canonical_unique_count: int | None = None
    oracle_expected_count: int | None = None

    def get(self, name: str) -> int | None:
        if name not in StageCounterName.ORDERED:
            raise MetrologyError("unknown stage counter", name=name)
        return getattr(self, name)

    def set(self, name: str, value: int | None) -> None:
        if name not in StageCounterName.ORDERED:
            raise MetrologyError("unknown stage counter", name=name)
        if value is not None and value < 0:
            raise MetrologyError("stage counters may not be negative", name=name, value=value)
        setattr(self, name, value)

    @property
    def measured(self) -> tuple[str, ...]:
        return tuple(n for n in StageCounterName.ORDERED if self.get(n) is not None)

    @property
    def unmeasured(self) -> tuple[str, ...]:
        return tuple(n for n in StageCounterName.ORDERED if self.get(n) is None)

    def as_dict(self) -> dict[str, Any]:
        return {name: self.get(name) for name in StageCounterName.ORDERED}


@dataclass(frozen=True)
class StageGap:
    """The difference between two adjacent stages, with its interpretation."""

    upstream: str
    downstream: str
    upstream_value: int | None
    downstream_value: int | None

    @property
    def known(self) -> bool:
        return self.upstream_value is not None and self.downstream_value is not None

    @property
    def delta(self) -> int | None:
        if not self.known:
            return None
        return self.upstream_value - self.downstream_value

    @property
    def shrinkage_fraction(self) -> float | None:
        if not self.known or not self.upstream_value:
            return None
        return self.delta / self.upstream_value

    def as_dict(self) -> dict[str, Any]:
        return {
            "upstream": self.upstream,
            "downstream": self.downstream,
            "upstream_value": self.upstream_value,
            "downstream_value": self.downstream_value,
            "delta": self.delta,
            "shrinkage_fraction": self.shrinkage_fraction,
            "known": self.known,
        }


def conservation_report(counters: StageCounters) -> dict[str, Any]:
    """Describe the stage counters without asserting a false identity.

    Section 4.1: the only equality P08 is allowed to require is between the
    independently derived expected set and the READY canonical set, and that
    comparison is over *identities*, not counts -- it lives in
    :mod:`p08_bench.reconciliation`. What this function produces is the gap
    structure that section 6.1 uses to localise a bottleneck, plus the
    observations that are legitimate:

    * ``kafka_physical >= producer_ack`` is expected (retries append more).
    * ``canonical_unique <= raw_physical`` is expected (dedup collapses rows).
    * ``issued <= offered`` is expected; the reverse means the driver invented work.

    Each observation is reported as ``True``/``False``/``None``, and ``None``
    means a counter was not measured.
    """
    gaps = []
    ordered = StageCounterName.ORDERED
    for upstream, downstream in zip(ordered, ordered[1:]):
        gaps.append(
            StageGap(
                upstream=upstream,
                downstream=downstream,
                upstream_value=counters.get(upstream),
                downstream_value=counters.get(downstream),
            )
        )

    def compare(left: str, right: str, op: str) -> bool | None:
        a, b = counters.get(left), counters.get(right)
        if a is None or b is None:
            return None
        return a <= b if op == "<=" else a >= b

    return {
        "counters": counters.as_dict(),
        "scopes": dict(STAGE_COUNTER_SCOPES),
        "gaps": [gap.as_dict() for gap in gaps],
        "expected_relations": {
            "issued_le_offered": compare(
                StageCounterName.ISSUED, StageCounterName.OFFERED, "<="
            ),
            "sealed_le_issued": compare(
                StageCounterName.SOURCE_SEALED, StageCounterName.ISSUED, "<="
            ),
            "kafka_physical_ge_producer_ack": compare(
                StageCounterName.KAFKA_PHYSICAL, StageCounterName.PRODUCER_ACK, ">="
            ),
            "canonical_le_raw_physical": compare(
                StageCounterName.CANONICAL_UNIQUE, StageCounterName.RAW_PHYSICAL, "<="
            ),
        },
        "unmeasured": list(counters.unmeasured),
        "note": (
            "No offered==raw==canonical identity is asserted; correctness is "
            "decided by identity-level reconciliation, not by these counts."
        ),
    }


class MetricRegistry:
    """Monotonic counters, gauges with high-water marks and histograms.

    One registry per run, no module-level globals: two concurrent runs sharing a
    registry would merge their counters, which is case G01-02 (one ``run_id``,
    two workloads) wearing a different hat.
    """

    __slots__ = ("_counters", "_gauges", "_peaks", "_histograms", "_label_sets")

    def __init__(self) -> None:
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}
        self._peaks: dict[str, float] = {}
        self._histograms: dict[str, BucketHistogram] = {}
        self._label_sets: dict[str, set[tuple[tuple[str, str], ...]]] = {}

    # -- naming -----------------------------------------------------------
    @staticmethod
    def _check_labels(name: str, labels: dict[str, str] | None) -> tuple[tuple[str, str], ...]:
        if not labels:
            return ()
        bad = sorted(set(labels) & FORBIDDEN_LABEL_KEYS)
        if bad:
            raise MetrologyError(
                "high-cardinality label refused on a metric; put the detail in "
                "the evidence store instead",
                metric=name,
                labels=bad,
            )
        unknown = sorted(set(labels) - ALLOWED_LABEL_KEYS)
        if unknown:
            raise MetrologyError(
                "label key is not in the allowed set; add it deliberately after "
                "bounding its cardinality",
                metric=name,
                labels=unknown,
            )
        return tuple(sorted((k, str(v)) for k, v in labels.items()))

    @staticmethod
    def _key(name: str, labels: tuple[tuple[str, str], ...]) -> str:
        if not labels:
            return name
        rendered = ",".join(f"{k}={v}" for k, v in labels)
        return f"{name}{{{rendered}}}"

    # -- counters ---------------------------------------------------------
    def incr(self, name: str, delta: int = 1, *, labels: dict[str, str] | None = None) -> int:
        if delta < 0:
            raise MetrologyError(
                "counters are monotonic by contract and may not decrease",
                metric=name,
                delta=delta,
            )
        checked = self._check_labels(name, labels)
        key = self._key(name, checked)
        self._label_sets.setdefault(name, set()).add(checked)
        value = self._counters.get(key, 0) + delta
        self._counters[key] = value
        return value

    def counter(self, name: str, *, labels: dict[str, str] | None = None) -> int:
        return self._counters.get(self._key(name, self._check_labels(name, labels)), 0)

    def ensure_counters(self, names: Iterable[str]) -> None:
        """Materialise counters at zero.

        "Not measured" and "measured, never happened" are different claims; a
        report has to be able to print the second one.
        """
        for name in names:
            self._counters.setdefault(name, 0)

    def series_cardinality(self, name: str) -> int:
        return len(self._label_sets.get(name, ()))

    # -- gauges -----------------------------------------------------------
    def set_gauge(self, name: str, value: float, *, labels: dict[str, str] | None = None) -> None:
        key = self._key(name, self._check_labels(name, labels))
        self._gauges[key] = value
        if value > self._peaks.get(key, float("-inf")):
            self._peaks[key] = value

    def gauge(self, name: str, *, labels: dict[str, str] | None = None) -> float | None:
        return self._gauges.get(self._key(name, self._check_labels(name, labels)))

    def peak(self, name: str, *, labels: dict[str, str] | None = None) -> float | None:
        return self._peaks.get(self._key(name, self._check_labels(name, labels)))

    # -- histograms -------------------------------------------------------
    def histogram(self, name: str, bounds: list[float] | None = None) -> BucketHistogram:
        hist = self._histograms.get(name)
        if hist is None:
            if bounds is None:
                raise MetrologyError("new histogram needs explicit bucket bounds", metric=name)
            hist = BucketHistogram(bounds)
            self._histograms[name] = hist
        return hist

    def observe(self, name: str, value: float, *, bounds: list[float] | None = None) -> None:
        self.histogram(name, bounds).observe(value)

    # -- output -----------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        return {
            "counters": dict(sorted(self._counters.items())),
            "gauges": {k: round(v, 6) for k, v in sorted(self._gauges.items())},
            "peaks": {k: round(v, 6) for k, v in sorted(self._peaks.items())},
            "histograms": {k: v.as_dict() for k, v in sorted(self._histograms.items())},
            "series_cardinality": {
                k: len(v) for k, v in sorted(self._label_sets.items()) if v != {()}
            },
        }


#: Counters every P08 run reports, even at zero. Grouped by the boundary each
#: one describes; the naming keeps apart things that a report must not conflate.
RUN_COUNTERS: tuple[str, ...] = (
    # driver: a schedule slot is not an emitted event
    "driver_slots_scheduled_total",
    "driver_events_issued_total",
    "driver_late_issue_total",
    "driver_dropped_issue_total",
    "driver_issue_errors_total",
    # query driver: scheduled is the denominator, forever
    "query_scheduled_total",
    "query_completed_total",
    "query_correct_total",
    "query_incorrect_total",
    "query_timeout_total",
    "query_error_total",
    # evidence: a missing probe is an event in its own right
    "probe_scrapes_total",
    "probe_scrape_failures_total",
    "probe_gap_windows_total",
    "evidence_artifacts_total",
    "evidence_write_failures_total",
    # faults: injected is not the same as verified-to-have-landed
    "faults_requested_total",
    "faults_injected_total",
    "faults_verified_total",
    "faults_not_triggered_total",
    "faults_cleanup_failures_total",
    # safety
    "safety_refusals_total",
    "stop_condition_hits_total",
)
