"""The ``p08.v1`` report: build it, and refuse to publish a dishonest one (section 25).

Section 25 ships a template that is almost entirely ``null``, with the
instruction: do not replace null with a fabricated measurement. That makes the
template a contract rather than a form, and this module enforces it in one
direction -- a report may be as empty as the run was, but it may not *claim*
more than it recorded.

:func:`validate_report` is where that lives. If ``status_dimensions.capacity`` is
``PASS`` then the fields a capacity claim rests on must be present: the
provenance digests, the verified unique count, the per-partition backlog slopes.
If ``latency`` is ``PASS`` then the query family, concurrency, cache profile,
sample counts and percentile must all be there. A report that passes a dimension
without its evidence is rejected, which is case E2E-44 and case E2E-43 caught at
the last possible moment.

The emitted structure keeps section 25's field names exactly, so a report from
this code and the template in the document are diffable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from . import REPORT_VERSION
from .errors import ConfigError
from .verdict import Dimension, RunVerdict, Status

#: Fields required before ``capacity`` may be PASS.
CAPACITY_EVIDENCE_FIELDS: tuple[tuple[str, str], ...] = (
    ("provenance", "source_manifest_sha256"),
    ("provenance", "config_sha256"),
    ("provenance", "hardware_profile_sha256"),
    ("workload", "steady_seconds"),
    ("workload", "issued_ticks"),
    ("results", "steady_verified_ticks_per_s"),
    ("results", "per_tp_backlog_slopes"),
    ("proof", "expected_accepted_ID_count"),
    ("proof", "ready_canonical_ID_count"),
)

#: Fields required before ``latency`` may be PASS.
LATENCY_EVIDENCE_FIELDS: tuple[tuple[str, str], ...] = (
    ("workload", "query_family"),
    ("workload", "query_concurrency"),
    ("workload", "query_cache_profile"),
    ("results", "query_scheduled_attempts"),
    ("results", "query_correct_completed"),
    ("results", "query_client_p95_ms"),
    ("statistics", "p95_method"),
)

#: Fields required before ``correctness`` may be PASS.
CORRECTNESS_EVIDENCE_FIELDS: tuple[tuple[str, str], ...] = (
    ("proof", "expected_accepted_ID_count"),
    ("proof", "ready_canonical_ID_count"),
    ("proof", "missing_ID_count"),
    ("proof", "unexpected_extra_ID_count"),
    ("proof", "conflict_ID_count"),
    ("proof", "semantic_mismatch_count"),
    ("proof", "oracle_diff_path"),
)


def blank_report(run_id: str, scenario_id: str) -> dict[str, Any]:
    """Section 25's template, verbatim, with every measurement null.

    This is the honest starting point for any run: ``NOT_RUN`` everywhere, and
    the summary line the document supplies.
    """
    return {
        "report_version": REPORT_VERSION,
        "status": Status.NOT_RUN.value,
        "run_id": run_id,
        "scenario_id": scenario_id,
        "status_dimensions": {
            "validity": Status.NOT_RUN.value,
            "correctness": Status.NOT_RUN.value,
            "capacity": Status.NOT_RUN.value,
            "latency": Status.NOT_RUN.value,
            "recoverability": Status.NOT_RUN.value,
            "safety": Status.NOT_RUN.value,
            "overall": Status.NOT_RUN.value,
        },
        "provenance": {
            "dataset_id": None,
            "source_manifest_sha256": None,
            "schema_version": "marketdata.v1",
            "source_commit": None,
            "docker_image_digests": [],
            "kafka_topic_generations": {},
            "clickhouse_ready_build_id": None,
            "hardware_profile_sha256": None,
            "config_sha256": None,
            "query_catalog_sha256": None,
        },
        "workload": {
            "mode": None,
            "target_market_ticks_per_s": None,
            "target_quotes_per_s": None,
            "scheduled_ticks": None,
            "issued_ticks": None,
            "kafka_physical_records": None,
            "raw_physical_rows": None,
            "canonical_verified_unique_ticks": None,
            "symbols": None,
            "partitions_per_topic": None,
            "query_family": None,
            "query_concurrency": None,
            "query_cache_profile": None,
            "warmup_seconds": None,
            "steady_seconds": None,
            "drain_seconds": None,
        },
        "results": {
            "steady_verified_ticks_per_s": None,
            "per_tp_backlog_slopes": {},
            "query_scheduled_attempts": None,
            "query_correct_completed": None,
            "query_errors": None,
            "query_timeouts": None,
            "query_client_p50_ms": None,
            "query_client_p95_ms": None,
            "query_client_p99_ms": None,
            "query_server_p95_ms": None,
            "clock_sync_uncertainty_ms": None,
            "memory_peak_bytes": None,
            "cgroup_oom_kill_count": None,
            "cpu_throttled_usec": None,
            "clickhouse_active_parts_peak": None,
            "kafka_lag_max_records": None,
        },
        "proof": {
            "expected_accepted_ID_count": None,
            "ready_canonical_ID_count": None,
            "missing_ID_count": None,
            "unexpected_extra_ID_count": None,
            "conflict_ID_count": None,
            "semantic_mismatch_count": None,
            "ready_publish_receipt": None,
            "fault_receipt": None,
            "oracle_diff_path": None,
            "raw_samples_path": None,
            "metrics_snapshot_path": None,
            "sha256_artifact_manifest": None,
        },
        "statistics": {
            "repeat_count": None,
            "run_level_confidence_interval": None,
            "p95_method": None,
            "histogram_resolution": None,
            "exclusion_policy_pre_registered": False,
        },
        "blockers": [],
        "limitations": [],
        "summary": "No experiment has been run. Null may not be replaced with a guessed measurement.",
    }


@dataclass
class ReportBuilder:
    """Fills a ``p08.v1`` report, section by section.

    Deliberately not a constructor taking every value: a run fills what it
    measured and leaves the rest null, and the only thing that promotes a
    dimension to PASS is a :class:`~p08_bench.verdict.RunVerdict`.
    """

    run_id: str
    scenario_id: str
    payload: dict[str, Any] = field(init=False)

    def __post_init__(self) -> None:
        self.payload = blank_report(self.run_id, self.scenario_id)

    def _section(self, name: str) -> dict[str, Any]:
        section = self.payload.get(name)
        if not isinstance(section, dict):
            raise ConfigError("unknown report section", section=name)
        return section

    def set(self, section: str, **values: Any) -> "ReportBuilder":
        """Set fields in a section, rejecting names the template does not have.

        Rejecting unknown fields keeps a report diffable against the document
        template: a typo would otherwise add a field nobody reads while leaving
        the real one null.
        """
        target = self._section(section)
        unknown = sorted(set(values) - set(target))
        if unknown:
            raise ConfigError(
                "field is not part of the p08.v1 report template",
                section=section,
                fields=unknown,
            )
        target.update(values)
        return self

    def add_blockers(self, blockers: Iterable[str]) -> "ReportBuilder":
        self.payload["blockers"].extend(blockers)
        return self

    def add_limitations(self, limitations: Iterable[str]) -> "ReportBuilder":
        """Append limitations, skipping repeats.

        The same limitation reaches the report from more than one place -- the
        config declares it and the verdict notes it -- and printing it twice
        makes a report look like it found two problems.
        """
        existing = self.payload["limitations"]
        for item in limitations:
            if item not in existing:
                existing.append(item)
        return self

    def set_summary(self, summary: str) -> "ReportBuilder":
        self.payload["summary"] = summary
        return self

    def apply_verdict(self, verdict: RunVerdict) -> "ReportBuilder":
        dimensions = self.payload["status_dimensions"]
        # A verdict that does not cover a dimension leaves it at NOT_RUN rather
        # than raising: a partial verdict is a thin result, not a crash.
        covered = {v.dimension: v for v in verdict.dimensions}
        for dimension in Dimension:
            found = covered.get(dimension)
            if found is not None:
                dimensions[dimension.value] = found.status.value
        dimensions["overall"] = verdict.overall.value
        self.payload["status"] = verdict.overall.value
        self.payload["blockers"].extend(
            f"{finding['dimension']}: {finding['detail']}"
            for finding in verdict.blocking_findings
        )
        self.add_limitations(verdict.notes)
        return self

    def build(self) -> dict[str, Any]:
        """Return the report, refusing to emit one that overclaims."""
        problems = validate_report(self.payload)
        if problems:
            raise ConfigError(
                "report claims a dimension it has no evidence for", problems=problems
            )
        return dict(self.payload)

    def build_unchecked(self) -> dict[str, Any]:
        """Return the report without validation, for inspecting a bad one."""
        return dict(self.payload)


def _missing_fields(
    payload: Mapping[str, Any], required: Iterable[tuple[str, str]]
) -> list[str]:
    missing: list[str] = []
    for section, field_name in required:
        value = payload.get(section, {}).get(field_name)
        if value is None or value == {} or value == []:
            missing.append(f"{section}.{field_name}")
    return missing


def validate_report(payload: Mapping[str, Any]) -> list[str]:
    """Problems that make a report dishonest rather than merely incomplete.

    Incompleteness is fine and expected: section 25's template is mostly null.
    What is refused is a ``PASS`` on a dimension whose supporting fields were
    never filled in.
    """
    problems: list[str] = []
    if payload.get("report_version") != REPORT_VERSION:
        problems.append(
            f"report_version is {payload.get('report_version')!r}, expected {REPORT_VERSION!r}"
        )
    dimensions = payload.get("status_dimensions", {})
    if not dimensions:
        return problems + ["status_dimensions is missing"]

    checks = (
        ("capacity", CAPACITY_EVIDENCE_FIELDS),
        ("latency", LATENCY_EVIDENCE_FIELDS),
        ("correctness", CORRECTNESS_EVIDENCE_FIELDS),
    )
    for dimension, required in checks:
        if dimensions.get(dimension) != Status.PASS.value:
            continue
        missing = _missing_fields(payload, required)
        if missing:
            problems.append(
                f"{dimension} is PASS but these evidence fields are empty: "
                + ", ".join(missing)
            )

    if dimensions.get("overall") == Status.PASS.value:
        if not payload.get("proof", {}).get("sha256_artifact_manifest"):
            problems.append("overall is PASS without an artifact manifest hash (FR-15)")
        if not payload.get("statistics", {}).get("p95_method") and dimensions.get("latency") == Status.PASS.value:
            problems.append("latency is PASS without a declared p95 method (INV-17)")

    # A PASS on any dimension while the overall verdict is INVALID would be a
    # report that reads as partially successful when the experiment was broken.
    if dimensions.get("validity") == Status.INVALID.value:
        overclaims = [
            name for name in ("capacity", "latency")
            if dimensions.get(name) == Status.PASS.value
        ]
        if overclaims:
            problems.append(
                "validity is INVALID, so these dimensions may not be PASS: "
                + ", ".join(overclaims)
            )
    return problems


def render_text_summary(payload: Mapping[str, Any]) -> str:
    """The short README-style block of section 25.

    Prints ``[not measured]`` rather than omitting a field, so the shape of what
    is missing is visible at a glance.
    """

    def show(section: str, name: str) -> str:
        value = payload.get(section, {}).get(name)
        return "[not measured]" if value is None else str(value)

    dimensions = payload.get("status_dimensions", {})
    lines = [
        f"Run: {payload.get('run_id')} / scenario {payload.get('scenario_id')}",
        f"Profile config sha256: {show('provenance', 'config_sha256')}",
        f"Dataset: {show('provenance', 'dataset_id')} "
        f"(sha256={show('provenance', 'source_manifest_sha256')})",
        f"Offered: {show('workload', 'target_market_ticks_per_s')} ticks/s; "
        f"Issued: {show('workload', 'issued_ticks')} ticks",
        f"Useful verified: {show('results', 'steady_verified_ticks_per_s')} unique ticks/s; "
        f"{show('workload', 'partitions_per_topic')} partitions per topic",
        f"Query: family={show('workload', 'query_family')} "
        f"concurrency={show('workload', 'query_concurrency')} "
        f"cache={show('workload', 'query_cache_profile')}",
        f"Query attempts: scheduled={show('results', 'query_scheduled_attempts')} "
        f"correct={show('results', 'query_correct_completed')} "
        f"errors={show('results', 'query_errors')} "
        f"timeouts={show('results', 'query_timeouts')}",
        f"Client p95: {show('results', 'query_client_p95_ms')} ms "
        f"(method={show('statistics', 'p95_method')})",
        f"Correctness: missing={show('proof', 'missing_ID_count')} "
        f"extra={show('proof', 'unexpected_extra_ID_count')} "
        f"conflicts={show('proof', 'conflict_ID_count')}",
        "Verdict: " + ", ".join(f"{k}={v}" for k, v in sorted(dimensions.items())),
    ]
    blockers = payload.get("blockers") or []
    if blockers:
        lines.append("Blockers:")
        lines.extend(f"  - {item}" for item in blockers)
    limitations = payload.get("limitations") or []
    if limitations:
        lines.append("Limitations:")
        lines.extend(f"  - {item}" for item in limitations)
    return "\n".join(lines)
