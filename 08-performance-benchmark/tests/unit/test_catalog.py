"""Catalog inventory and the release coverage gate (sections 16, 17, 24; case E2E-48).

The inventory tests assert the figures section 17 declares -- 512 what-if cases
in 32 groups, split 242/206/64, plus 48 end-to-end tests -- against the CSV
generated from the document. If the document changes, these fail, which is the
intent: the catalog is derived, not retyped.

:class:`TestCoverageGate` is case E2E-48: a plan with one P0 case quietly
dropped must come out NO_GO and must never print an all-checked status.
"""

from __future__ import annotations

import pytest

from p08_bench.catalog import (
    EXPECTED_E2E,
    EXPECTED_PRIORITY_COUNTS,
    EXPECTED_WHAT_IF,
    CaseResult,
    CaseStatus,
    CatalogCase,
    CoverageGate,
    Gate,
    Kind,
    Priority,
    CaseCatalog,
    Waiver,
    statuses_from_mapping,
)
from p08_bench.errors import ConfigError


@pytest.fixture(scope="module")
def catalog() -> CaseCatalog:
    return CaseCatalog.load()


def tiny_catalog() -> CaseCatalog:
    """A three-case catalog so gate tests do not depend on the real inventory."""
    return CaseCatalog(
        [
            CatalogCase("C-P0-A", Kind.WHAT_IF, "G01", "group", Priority.P0, "w", "s", "e"),
            CatalogCase("C-P0-B", Kind.WHAT_IF, "G01", "group", Priority.P0, "w", "s", "e"),
            CatalogCase("C-P1-A", Kind.WHAT_IF, "G02", "group", Priority.P1, "w", "s", "e"),
        ]
    )


class TestInventory:
    def test_the_catalog_has_512_what_if_cases(self, catalog: CaseCatalog):
        assert len(catalog.by_kind(Kind.WHAT_IF)) == EXPECTED_WHAT_IF == 512

    def test_the_catalog_has_48_e2e_tests(self, catalog: CaseCatalog):
        assert len(catalog.by_kind(Kind.E2E)) == EXPECTED_E2E == 48

    def test_the_priority_split_matches_section_17(self, catalog: CaseCatalog):
        assert EXPECTED_PRIORITY_COUNTS == {"P0": 242, "P1": 206, "P2": 64}
        counts = {
            p.value: len([c for c in catalog.by_priority(p) if c.kind is Kind.WHAT_IF])
            for p in (Priority.P0, Priority.P1, Priority.P2)
        }
        assert counts == EXPECTED_PRIORITY_COUNTS

    def test_there_are_32_what_if_groups_of_16(self, catalog: CaseCatalog):
        groups = [g for g in catalog.groups if g.startswith("G")]
        assert len(groups) == 32
        for group in groups:
            assert len(catalog.by_group(group)) == 16

    def test_the_four_e2e_sections_hold_twelve_tests_each(self, catalog: CaseCatalog):
        for section in ("E2E-A", "E2E-B", "E2E-C", "E2E-D"):
            assert len(catalog.by_group(section)) == 12

    def test_e2e_tests_carry_no_invented_priority(self, catalog: CaseCatalog):
        # Section 18 assigns no P-levels; inventing them would put a priority in
        # the catalog that the document never set.
        assert all(c.priority is Priority.UNSET for c in catalog.by_kind(Kind.E2E))

    def test_every_case_keeps_its_stimulus_and_oracle(self, catalog: CaseCatalog):
        for case in catalog:
            assert case.stimulus.strip()
            assert case.expected_evidence.strip()

    def test_known_cases_are_present_with_their_text(self, catalog: CaseCatalog):
        assert "P08-G01-01" in catalog
        assert "E2E-46" in catalog
        assert "nearest-rank p95=1000ms" in catalog.get("E2E-46").expected_evidence

    def test_an_unknown_id_is_refused(self, catalog: CaseCatalog):
        with pytest.raises(ConfigError, match="unknown case id"):
            catalog.get("P08-G99-01")

    def test_duplicate_ids_are_refused_at_construction(self):
        case = CatalogCase("X", Kind.WHAT_IF, "G01", "g", Priority.P0, "w", "s", "e")
        with pytest.raises(ConfigError, match="duplicate case ids"):
            CaseCatalog([case, case])

    def test_inventory_verification_rejects_a_truncated_catalog(self):
        with pytest.raises(ConfigError, match="inventory mismatch"):
            tiny_catalog().verify_inventory()


class TestResultsAndWaivers:
    def test_a_pass_must_cite_evidence(self):
        # Section 16: a PASS without an artifact reference is not a PASS.
        with pytest.raises(ConfigError, match="evidence reference"):
            CaseResult("C-P0-A", CaseStatus.PASS)

    def test_a_fail_needs_no_evidence_reference(self):
        assert CaseResult("C-P0-A", CaseStatus.FAIL, reason="missing ids").status is CaseStatus.FAIL

    def test_a_waiver_needs_an_owner(self):
        with pytest.raises(ConfigError, match="named owner"):
            Waiver("C-P1-A", owner="  ", reason="deferred")

    def test_a_waiver_needs_a_reason(self):
        with pytest.raises(ConfigError, match="reason"):
            Waiver("C-P1-A", owner="ops", reason="")

    def test_statuses_from_mapping_rejects_an_unknown_status(self):
        with pytest.raises(ConfigError, match="unknown case status"):
            statuses_from_mapping({"C-P0-A": "PROBABLY_FINE"})

    def test_statuses_from_mapping_parses_known_statuses(self):
        parsed = statuses_from_mapping({"C-P0-A": "FAIL", "C-P0-B": "NOT_RUN"})
        assert parsed["C-P0-A"] is CaseStatus.FAIL


class TestCoverageGate:
    def _gate(self) -> CoverageGate:
        return CoverageGate(catalog=tiny_catalog())

    def test_a_complete_passing_plan_is_go(self):
        gate = self._gate()
        gate.plan(["C-P0-A", "C-P0-B"])
        gate.record(CaseResult("C-P0-A", CaseStatus.PASS, evidence_ref="runs/a"))
        gate.record(CaseResult("C-P0-B", CaseStatus.PASS, evidence_ref="runs/b"))
        assert gate.evaluate()["verdict"] == Gate.GO.value

    def test_dropping_a_p0_case_from_the_plan_is_no_go(self):
        # Case E2E-48 exactly: the plan runs everything it contains and still
        # must not ship.
        gate = self._gate()
        gate.plan(["C-P0-A"])
        gate.record(CaseResult("C-P0-A", CaseStatus.PASS, evidence_ref="runs/a"))
        result = gate.evaluate()
        assert result["verdict"] == Gate.NO_GO.value
        assert result["unplanned_p0"] == ["C-P0-B"]
        assert any(f["kind"] == "coverage" for f in result["blocking_findings"])

    def test_the_coverage_statement_never_claims_all_checked(self):
        gate = self._gate()
        gate.plan(["C-P0-A"])
        gate.record(CaseResult("C-P0-A", CaseStatus.PASS, evidence_ref="runs/a"))
        statement = gate.evaluate()["coverage_statement"]
        assert "1/2 applicable P0 cases satisfied" in statement
        assert "1 never planned" in statement

    def test_a_planned_but_unrun_p0_case_is_no_go(self):
        gate = self._gate()
        gate.plan(["C-P0-A", "C-P0-B"])
        gate.record(CaseResult("C-P0-A", CaseStatus.PASS, evidence_ref="runs/a"))
        result = gate.evaluate()
        assert result["verdict"] == Gate.NO_GO.value
        assert result["not_run_p0"] == ["C-P0-B"]

    def test_a_failing_p0_case_is_no_go(self):
        gate = self._gate()
        gate.plan(["C-P0-A", "C-P0-B"])
        gate.record(CaseResult("C-P0-A", CaseStatus.FAIL, reason="missing identities"))
        gate.record(CaseResult("C-P0-B", CaseStatus.PASS, evidence_ref="runs/b"))
        assert gate.evaluate()["verdict"] == Gate.NO_GO.value

    def test_an_invalid_p0_case_outranks_a_failure(self):
        # Section 24: a FAIL produced by a broken measurement is not a fact
        # about the system, so INVALID takes precedence.
        gate = self._gate()
        gate.plan(["C-P0-A", "C-P0-B"])
        gate.record(CaseResult("C-P0-A", CaseStatus.FAIL, reason="slow"))
        gate.record(CaseResult("C-P0-B", CaseStatus.INVALID, reason="fault never triggered"))
        assert gate.evaluate()["verdict"] == Gate.INVALID.value

    def test_an_inconclusive_p0_case_is_inconclusive_not_go(self):
        gate = self._gate()
        gate.plan(["C-P0-A", "C-P0-B"])
        gate.record(CaseResult("C-P0-A", CaseStatus.PASS, evidence_ref="runs/a"))
        gate.record(CaseResult("C-P0-B", CaseStatus.INCONCLUSIVE, reason="clock unsynced"))
        assert gate.evaluate()["verdict"] == Gate.INCONCLUSIVE.value

    def test_a_case_blocked_by_an_unimplemented_dependency_is_no_go(self):
        # Distinct from NOT_RUN: nobody can run it here yet, and that still
        # cannot be shipped as covered.
        gate = self._gate()
        gate.plan(["C-P0-A", "C-P0-B"])
        gate.record(CaseResult("C-P0-A", CaseStatus.PASS, evidence_ref="runs/a"))
        gate.record(CaseResult("C-P0-B", CaseStatus.BLOCKED_BY_UPSTREAM, reason="no Kafka"))
        result = gate.evaluate()
        assert result["verdict"] == Gate.NO_GO.value
        assert result["blocked_p0"] == ["C-P0-B"]

    def test_a_waived_case_satisfies_the_gate_and_is_recorded(self):
        gate = self._gate()
        gate.plan(["C-P0-A", "C-P0-B"])
        gate.record(CaseResult("C-P0-A", CaseStatus.PASS, evidence_ref="runs/a"))
        gate.waive(Waiver("C-P0-B", owner="ops-lead", reason="needs lab hardware"))
        result = gate.evaluate()
        assert result["verdict"] == Gate.GO.value
        assert result["waivers"][0]["owner"] == "ops-lead"

    def test_a_waiver_overrides_a_recorded_failure_visibly(self):
        gate = self._gate()
        gate.plan(["C-P0-A", "C-P0-B"])
        gate.record(CaseResult("C-P0-A", CaseStatus.PASS, evidence_ref="runs/a"))
        gate.record(CaseResult("C-P0-B", CaseStatus.FAIL, reason="slow"))
        gate.waive(Waiver("C-P0-B", owner="ops-lead", reason="accepted risk"))
        assert gate.status("C-P0-B") is CaseStatus.WAIVED
        assert gate.evaluate()["waivers"][0]["reason"] == "accepted risk"

    def test_profile_scope_narrows_the_applicable_set_visibly(self):
        # Section 24 scopes P0 to the profile; a QUERY_ONLY run cannot be held
        # to the ingestion cases, and the narrowing must be visible.
        gate = CoverageGate(catalog=tiny_catalog(), profile_scope={"C-P0-A"})
        gate.plan(["C-P0-A"])
        gate.record(CaseResult("C-P0-A", CaseStatus.PASS, evidence_ref="runs/a"))
        result = gate.evaluate()
        assert result["verdict"] == Gate.GO.value
        assert result["profile_scoped"] is True
        assert result["applicable_p0"] == 1
        assert result["catalog_p0_total"] == 2

    def test_p1_and_p2_cases_do_not_block_the_gate(self):
        gate = self._gate()
        gate.plan(["C-P0-A", "C-P0-B"])
        gate.record(CaseResult("C-P0-A", CaseStatus.PASS, evidence_ref="runs/a"))
        gate.record(CaseResult("C-P0-B", CaseStatus.PASS, evidence_ref="runs/b"))
        assert gate.status("C-P1-A") is CaseStatus.NOT_RUN
        assert gate.evaluate()["verdict"] == Gate.GO.value

    def test_planning_an_unknown_case_is_refused(self):
        with pytest.raises(ConfigError, match="unknown case id"):
            self._gate().plan(["C-NOPE"])

    def test_recording_an_unplanned_result_is_refused(self):
        # Otherwise coverage accounting could be inflated by results for cases
        # nobody scheduled.
        gate = self._gate()
        with pytest.raises(ConfigError, match="never planned"):
            gate.record(CaseResult("C-P0-A", CaseStatus.PASS, evidence_ref="runs/a"))

    def test_the_default_status_is_not_run(self):
        assert self._gate().status("C-P0-A") is CaseStatus.NOT_RUN

    def test_summary_counts_every_case_by_status(self):
        gate = self._gate()
        gate.plan(["C-P0-A"])
        gate.record(CaseResult("C-P0-A", CaseStatus.PASS, evidence_ref="runs/a"))
        summary = gate.summary_by_status()
        assert summary["PASS"] == 1
        assert summary["NOT_RUN"] == 2


class TestRealCatalogStartsNotRun:
    def test_every_real_case_starts_not_run(self, catalog: CaseCatalog):
        # Section 17: all 512 cases are NOT_RUN at document time, and nothing in
        # this repository may upgrade a status without evidence.
        gate = CoverageGate(catalog=catalog)
        summary = gate.summary_by_status()
        assert summary["NOT_RUN"] == len(catalog) == 560
        assert summary["PASS"] == 0

    def test_the_real_gate_is_no_go_before_anything_runs(self, catalog: CaseCatalog):
        result = CoverageGate(catalog=catalog).evaluate()
        assert result["verdict"] == Gate.NO_GO.value
        assert result["satisfied_p0"] == 0
        assert len(result["unplanned_p0"]) == 242
