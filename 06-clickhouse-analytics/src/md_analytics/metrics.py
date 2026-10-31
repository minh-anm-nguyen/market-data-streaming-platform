"""Counters, gauges and the alert table of section 11.

Two rules from the design shape the API:

* **Labels are low cardinality.** Table, phase, status, query template -- never an
  ``event_id``, a ``build_id`` or a symbol from an untrusted source. One label
  per event turns a metrics backend into a slow, expensive log (section 11).
  :func:`check_label_cardinality` enforces that instead of trusting reviewers.
* **Absence of a metric is not evidence.** ``query_log`` is asynchronous and can
  be disabled, so a gauge that was never observed reads as ``None`` rather than
  zero, and the alert evaluator reports it as unobserved instead of healthy.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Final

from .errors import Verdict

#: Label keys permitted anywhere in this package.
ALLOWED_LABELS: Final = frozenset(
    {"table", "database", "phase", "status", "query_template", "backend", "partition"}
)

#: Label values must be bounded too: a hostile symbol set would otherwise blow up
#: the series count through a legitimate key (case I-13).
MAX_LABEL_VALUE_LEN: Final = 64


class MetricLabelError(ValueError):
    """Raised for a label key or value that would explode cardinality."""


def check_label_cardinality(labels: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
    """Validate and normalise a label set, or raise."""
    normalised: list[tuple[str, str]] = []
    for key, value in labels.items():
        if key not in ALLOWED_LABELS:
            raise MetricLabelError(
                f"label {key!r} is not in the allowlist {sorted(ALLOWED_LABELS)}; "
                f"high-cardinality labels make a metrics store unusable"
            )
        if not isinstance(value, str):
            raise MetricLabelError(f"label {key!r} value must be a string")
        if len(value) > MAX_LABEL_VALUE_LEN:
            raise MetricLabelError(
                f"label {key}={value[:16]}... exceeds {MAX_LABEL_VALUE_LEN} characters"
            )
        normalised.append((key, value))
    return tuple(sorted(normalised))


@dataclass(slots=True)
class MetricRegistry:
    """A tiny in-process registry. No network, no background thread."""

    counters: dict[tuple[str, tuple[tuple[str, str], ...]], int] = field(default_factory=dict)
    gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = field(default_factory=dict)

    def increment(self, name: str, *, value: int = 1, **labels: str) -> None:
        key = (name, check_label_cardinality(labels))
        self.counters[key] = self.counters.get(key, 0) + value

    def set_gauge(self, name: str, value: float, **labels: str) -> None:
        self.gauges[(name, check_label_cardinality(labels))] = value

    def counter(self, name: str, **labels: str) -> int:
        return self.counters.get((name, check_label_cardinality(labels)), 0)

    def gauge(self, name: str, **labels: str) -> float | None:
        """``None`` for a gauge that was never set: unobserved, not zero."""
        return self.gauges.get((name, check_label_cardinality(labels)))

    def snapshot(self) -> dict[str, Any]:
        def render(key: tuple[str, tuple[tuple[str, str], ...]]) -> str:
            name, labels = key
            if not labels:
                return name
            rendered = ",".join(f"{k}={v}" for k, v in labels)
            return f"{name}{{{rendered}}}"

        return {
            "counters": {render(k): v for k, v in sorted(self.counters.items())},
            "gauges": {render(k): v for k, v in sorted(self.gauges.items())},
        }


@dataclass(frozen=True, slots=True)
class AlertRule:
    """One row of the section 11 alert table."""

    metric: str
    #: Fires when the observed value is strictly greater than this.
    max_value: float
    severity: str
    meaning: str
    #: Release-blocking rules must be zero and must have been observed. The
    #: "any > 0" rows of section 11 -- conflicts, duplicate canonical ids, wrong
    #: bar counts, READY without proof -- are all of this kind.
    blocking: bool = False


SECTION_11_ALERTS: Final = (
    AlertRule("raw_conflicts", 0, "P0", "identity carries conflicting payloads", blocking=True),
    AlertRule(
        "canonical_duplicate_ids", 0, "P0", "an identity has several canonical rows", blocking=True
    ),
    AlertRule("bars_wrong_count", 0, "P0", "a bar disagrees with the oracle", blocking=True),
    AlertRule(
        "build_ready_without_proof", 0, "P0", "READY without a passing proof", blocking=True
    ),
    AlertRule("missing_ids", 0, "P0", "an accepted source identity is absent", blocking=True),
    AlertRule("extra_ids", 0, "P0", "a raw identity is not in the accepted ledger", blocking=True),
    AlertRule(
        "active_parts_per_partition", 300, "P1", "part pressure is rising in steady state"
    ),
    AlertRule("merge_backlog_age_s", 600, "P1", "merges are falling behind"),
    AlertRule("p05_kafka_lag_age_s", 300, "P1", "ingestion lag risks retention loss"),
    AlertRule("query_p95_ms", 2_400, "P2", "query latency drifted from the baseline"),
    AlertRule("ttl_age_gap_days", 0, "P1", "data needed for rebuild is past retention"),
)


@dataclass(frozen=True, slots=True)
class AlertEvaluation:
    rule: AlertRule
    observed: float | None
    firing: bool
    unobserved: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "metric": self.rule.metric,
            "severity": self.rule.severity,
            "max_value": self.rule.max_value,
            "observed": self.observed,
            "firing": self.firing,
            "unobserved": self.unobserved,
            "blocking": self.rule.blocking,
            "meaning": self.rule.meaning,
        }


def evaluate_alerts(
    observed: Mapping[str, float],
    *,
    rules: Iterable[AlertRule] = SECTION_11_ALERTS,
) -> list[AlertEvaluation]:
    """Evaluate alert rules against observed values.

    A blocking rule with no observation is *not* satisfied. Silence from a metric
    that is supposed to prove zero conflicts tells you nothing about conflicts.
    """
    out: list[AlertEvaluation] = []
    for rule in rules:
        value = observed.get(rule.metric)
        unobserved = value is None
        firing = (not unobserved and value > rule.max_value) or (unobserved and rule.blocking)
        out.append(AlertEvaluation(rule, value, firing, unobserved))
    return out


def release_gate_verdict(evaluations: Iterable[AlertEvaluation]) -> Verdict:
    """``PASS`` only if no blocking rule fires and none is unobserved."""
    evaluations = list(evaluations)
    blocking = [e for e in evaluations if e.rule.blocking]
    if any(e.firing and not e.unobserved for e in blocking):
        return Verdict.NO_GO
    if any(e.unobserved for e in blocking):
        return Verdict.INCONCLUSIVE
    return Verdict.PASS


def counters_to_alert_inputs(counters: Mapping[str, int]) -> dict[str, float]:
    """Map pipeline counters onto the alert metric names of section 11."""
    return {
        "raw_conflicts": float(counters.get("raw_conflict_id_count", 0)),
        "canonical_duplicate_ids": float(
            counters.get("canonical_duplicate_ids", counters.get("bars_extra", 0))
        ),
        "bars_wrong_count": float(counters.get("bar_reference_mismatch_count", 0)),
        "missing_ids": float(counters.get("missing_ids", 0)),
        "extra_ids": float(counters.get("extra_ids", 0)),
    }
