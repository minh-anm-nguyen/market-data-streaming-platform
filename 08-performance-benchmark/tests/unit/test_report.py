"""The p08.v1 report (section 25).

Section 25's template is almost entirely ``null`` with the instruction not to
replace null with a guessed measurement. Incompleteness is therefore fine; what
:func:`validate_report` refuses is a ``PASS`` on a dimension whose supporting
evidence fields were never filled in, which is how cases E2E-43 and E2E-44 get
caught at the last possible moment.
"""

from __future__ import annotations

import pytest

from p08_bench import REPORT_VERSION
from p08_bench.errors import ConfigError
from p08_bench.report import (
    CAPACITY_EVIDENCE_FIELDS,
    ReportBuilder,
    blank_report,
    render_text_summary,
    validate_report,
)
from p08_bench.verdict import Dimension, DimensionVerdict, RunVerdict, Status


def passing_verdict(**overrides) -> RunVerdict:
    statuses = {dimension: Status.PASS for dimension in Dimension}
    statuses.update(overrides)
    dimensions = tuple(DimensionVerdict(d, s) for d, s in statuses.items())
    return RunVerdict(dimensions=dimensions, overall=Status.PASS)


class TestBlankReport:
    def test_everything_starts_not_run(self):
        report = blank_report("R1", "E2E-02")
        assert report["status"] == "NOT_RUN"
        assert set(report["status_dimensions"].values()) == {"NOT_RUN"}

    def test_every_measurement_starts_null(self):
        report = blank_report("R1", "E2E-02")
        assert report["results"]["query_client_p95_ms"] is None
        assert report["proof"]["missing_ID_count"] is None

    def test_the_version_is_pinned(self):
        assert blank_report("R1", "E2E-02")["report_version"] == REPORT_VERSION == "p08.v1"

    def test_the_summary_says_null_may_not_be_guessed(self):
        assert "may not be replaced" in blank_report("R1", "E2E-02")["summary"]

    def test_the_exclusion_policy_defaults_to_not_pre_registered(self):
        # Section 4.4 wants the stop rule registered in advance; absent that,
        # the honest default is False.
        assert blank_report("R1", "E2E-02")["statistics"]["exclusion_policy_pre_registered"] is False

    def test_a_blank_report_validates(self):
        # Incomplete is allowed; overclaiming is not.
        assert validate_report(blank_report("R1", "E2E-02")) == []


class TestBuilder:
    def test_fields_can_be_set_section_by_section(self):
        builder = ReportBuilder("R1", "E2E-02")
        builder.set("workload", target_market_ticks_per_s=120_000)
        assert builder.build()["workload"]["target_market_ticks_per_s"] == 120_000

    def test_a_field_outside_the_template_is_refused(self):
        # Keeps the report diffable against the document; a typo would add a
        # field nobody reads and leave the real one null.
        builder = ReportBuilder("R1", "E2E-02")
        with pytest.raises(ConfigError, match="not part of the p08.v1 report template"):
            builder.set("workload", throughput=120_000)

    def test_an_unknown_section_is_refused(self):
        with pytest.raises(ConfigError, match="unknown report section"):
            ReportBuilder("R1", "E2E-02").set("tuning", x=1)

    def test_limitations_are_deduplicated(self):
        # The same limitation arrives from the config and from the verdict.
        builder = ReportBuilder("R1", "E2E-02")
        builder.add_limitations(["steady window too short"])
        builder.add_limitations(["steady window too short", "simulated SUT"])
        assert builder.build()["limitations"] == ["steady window too short", "simulated SUT"]

    def test_applying_a_verdict_sets_every_dimension(self):
        builder = ReportBuilder("R1", "E2E-02")
        builder.apply_verdict(passing_verdict())
        dimensions = builder.build_unchecked()["status_dimensions"]
        assert dimensions["overall"] == "PASS"
        assert dimensions["capacity"] == "PASS"

    def test_blocking_findings_become_blockers(self):
        verdict = RunVerdict(
            dimensions=(
                DimensionVerdict(Dimension.CORRECTNESS, Status.FAIL, ("missing identities",)),
            ),
            overall=Status.FAIL,
        )
        builder = ReportBuilder("R1", "E2E-02").apply_verdict(verdict)
        assert builder.build()["blockers"] == ["correctness: missing identities"]

    def test_build_refuses_an_overclaiming_report(self):
        builder = ReportBuilder("R1", "E2E-02").apply_verdict(passing_verdict())
        with pytest.raises(ConfigError, match="no evidence for"):
            builder.build()

    def test_build_unchecked_returns_the_bad_report_for_inspection(self):
        builder = ReportBuilder("R1", "E2E-02").apply_verdict(passing_verdict())
        assert builder.build_unchecked()["status_dimensions"]["capacity"] == "PASS"


class TestValidation:
    def _capacity_pass(self) -> dict:
        report = blank_report("R1", "E2E-02")
        report["status_dimensions"]["capacity"] = "PASS"
        return report

    def test_a_capacity_pass_without_evidence_is_refused(self):
        problems = validate_report(self._capacity_pass())
        assert any("capacity is PASS" in p for p in problems)

    def test_the_named_capacity_fields_are_all_required(self):
        problems = validate_report(self._capacity_pass())
        for _, field_name in CAPACITY_EVIDENCE_FIELDS:
            assert field_name in problems[0]

    def test_a_capacity_pass_with_full_evidence_validates(self):
        report = self._capacity_pass()
        report["provenance"].update(
            source_manifest_sha256="a" * 64, config_sha256="b" * 64,
            hardware_profile_sha256="c" * 64,
        )
        report["workload"].update(steady_seconds=1800, issued_ticks=1)
        report["results"].update(
            steady_verified_ticks_per_s=1.0, per_tp_backlog_slopes={"md.ticks.v1-0": 0.0}
        )
        report["proof"].update(expected_accepted_ID_count=1, ready_canonical_ID_count=1)
        assert validate_report(report) == []

    def test_an_empty_slope_map_does_not_count_as_evidence(self):
        # Case E2E-44: a missing series is not a measured zero.
        report = self._capacity_pass()
        report["provenance"].update(
            source_manifest_sha256="a" * 64, config_sha256="b" * 64,
            hardware_profile_sha256="c" * 64,
        )
        report["workload"].update(steady_seconds=1800, issued_ticks=1)
        report["results"].update(steady_verified_ticks_per_s=1.0, per_tp_backlog_slopes={})
        report["proof"].update(expected_accepted_ID_count=1, ready_canonical_ID_count=1)
        assert any("per_tp_backlog_slopes" in p for p in validate_report(report))

    def test_a_latency_pass_without_its_labels_is_refused(self):
        # INV-17: a p95 without family, concurrency, cache and N is unreadable.
        report = blank_report("R1", "E2E-02")
        report["status_dimensions"]["latency"] = "PASS"
        assert any("query_family" in p for p in validate_report(report))

    def test_a_correctness_pass_needs_the_oracle_diff(self):
        report = blank_report("R1", "E2E-02")
        report["status_dimensions"]["correctness"] = "PASS"
        assert any("oracle_diff_path" in p for p in validate_report(report))

    def test_an_overall_pass_needs_an_artifact_manifest(self):
        # FR-15: the artifacts must be enough to rerun.
        report = blank_report("R1", "E2E-02")
        report["status_dimensions"]["overall"] = "PASS"
        assert any("artifact manifest" in p for p in validate_report(report))

    def test_an_invalid_run_may_not_pass_capacity_or_latency(self):
        # A report that reads as partially successful when the experiment was
        # broken is the worst of both.
        report = blank_report("R1", "E2E-02")
        report["status_dimensions"]["validity"] = "INVALID"
        report["status_dimensions"]["capacity"] = "PASS"
        report["status_dimensions"]["latency"] = "PASS"
        assert any("validity is INVALID" in p for p in validate_report(report))

    def test_a_wrong_report_version_is_a_problem(self):
        report = blank_report("R1", "E2E-02")
        report["report_version"] = "p07.v9"
        assert any("report_version" in p for p in validate_report(report))

    def test_missing_status_dimensions_is_a_problem(self):
        assert any(
            "status_dimensions" in p
            for p in validate_report({"report_version": REPORT_VERSION})
        )


class TestTextSummary:
    def test_unmeasured_fields_print_as_not_measured(self):
        # Printing the shape of what is missing beats omitting the line.
        text = render_text_summary(blank_report("R1", "E2E-02"))
        assert "[not measured]" in text
        assert "Run: R1 / scenario E2E-02" in text

    def test_the_verdict_line_lists_every_dimension(self):
        text = render_text_summary(blank_report("R1", "E2E-02"))
        for dimension in ("validity", "correctness", "capacity", "latency", "safety"):
            assert dimension in text

    def test_blockers_and_limitations_are_rendered(self):
        report = blank_report("R1", "E2E-02")
        report["blockers"] = ["correctness: missing identities"]
        report["limitations"] = ["simulated SUT"]
        text = render_text_summary(report)
        assert "- correctness: missing identities" in text
        assert "- simulated SUT" in text

    def test_measured_values_replace_the_placeholder(self):
        report = blank_report("R1", "E2E-02")
        report["results"]["query_client_p95_ms"] = 271.0
        assert "271.0 ms" in render_text_summary(report)
