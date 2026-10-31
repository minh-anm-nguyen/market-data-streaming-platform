"""Three-dimension verdicts (sections 7, 15, 24; ADR-05).

The two tests that carry the design are
:meth:`TestAggregation.test_invalid_outranks_fail` -- a failure produced by a
broken measurement is not a fact about the system -- and
:meth:`TestOverloadSpecialCase.test_safe_overload_passes_safety_and_fails_capacity`,
which is section 7's special case: one run, two opposite and simultaneously
correct verdicts.
"""

from __future__ import annotations

import pytest

from p08_bench.reconciliation import Reconciliation
from p08_bench.verdict import (
    Dimension,
    RunEvidence,
    Status,
    Tri,
    aggregate,
    decide,
)


def clean_evidence(**overrides) -> RunEvidence:
    """A run where every gate was measured and passed."""
    evidence = RunEvidence(
        executed=True,
        manifest_valid=Tri.TRUE,
        workload_calibrated=Tri.TRUE,
        steady_window_matches_manifest=Tri.TRUE,
        clock_adequate_for_claims=Tri.TRUE,
        sufficient_samples=Tri.TRUE,
        histogram_resolution_adequate=Tri.TRUE,
        evidence_complete=Tri.TRUE,
        source_sealed=Tri.TRUE,
        reconciliation=Reconciliation.PASS,
        false_ready=Tri.FALSE,
        early_offset_commit=Tri.FALSE,
        query_results_correct=Tri.TRUE,
        backlog_stable=Tri.TRUE,
        drain_reconciled=Tri.TRUE,
        steady_protocol_met=Tri.TRUE,
        achieved_rate_meets_claim=Tri.TRUE,
        latency_within_budget=Tri.TRUE,
        recovery_within_budget=Tri.TRUE,
        recovery_preserved_correctness=Tri.TRUE,
        resources_bounded=Tri.TRUE,
        no_oom=Tri.TRUE,
        no_silent_loss=Tri.TRUE,
        safety_allowlist_respected=Tri.TRUE,
        degraded_safely=Tri.TRUE,
        fault_receipt_valid=Tri.TRUE,
    )
    for key, value in overrides.items():
        setattr(evidence, key, value)
    return evidence


class TestTri:
    def test_of_maps_none_to_unknown(self):
        assert Tri.of(None) is Tri.UNKNOWN
        assert Tri.of(True) is Tri.TRUE
        assert Tri.of(False) is Tri.FALSE

    def test_a_tri_cannot_be_used_as_a_boolean(self):
        # UNKNOWN in a boolean context would silently become False, which is
        # exactly the default section 15 forbids.
        with pytest.raises(TypeError, match="boolean context"):
            bool(Tri.UNKNOWN)

    def test_even_true_refuses_boolean_coercion(self):
        # If TRUE were truthy, `if tri:` would read correctly in tests and
        # wrongly in production for UNKNOWN.
        with pytest.raises(TypeError):
            bool(Tri.TRUE)


class TestAggregation:
    def test_all_pass_aggregates_to_pass(self):
        verdict = decide(clean_evidence())
        assert verdict.overall is Status.PASS

    def test_not_applicable_does_not_lower_a_pass(self):
        verdict = decide(clean_evidence())
        assert verdict.by_dimension(Dimension.CAPACITY).status is Status.NOT_APPLICABLE
        assert verdict.overall is Status.PASS

    def test_invalid_outranks_fail(self):
        # Section 24: a FAIL from a broken measurement is not a result.
        assert aggregate(
            [
                _dim(Dimension.VALIDITY, Status.INVALID),
                _dim(Dimension.CORRECTNESS, Status.FAIL),
            ]
        ) is Status.INVALID

    def test_fail_outranks_inconclusive(self):
        assert aggregate(
            [
                _dim(Dimension.CORRECTNESS, Status.FAIL),
                _dim(Dimension.CAPACITY, Status.INCONCLUSIVE),
            ]
        ) is Status.FAIL

    def test_inconclusive_outranks_pass(self):
        assert aggregate(
            [
                _dim(Dimension.CORRECTNESS, Status.PASS),
                _dim(Dimension.CAPACITY, Status.INCONCLUSIVE),
            ]
        ) is Status.INCONCLUSIVE

    def test_no_dimensions_is_not_run(self):
        assert aggregate([]) is Status.NOT_RUN


def _dim(dimension, status):
    from p08_bench.verdict import DimensionVerdict

    return DimensionVerdict(dimension, status)


class TestValidity:
    def test_an_unrun_case_is_not_run_everywhere(self):
        verdict = decide(RunEvidence(executed=False))
        assert verdict.overall is Status.NOT_RUN

    def test_an_unmeasured_gate_is_inconclusive_not_a_pass(self):
        # The central rule of section 15.
        verdict = decide(clean_evidence(evidence_complete=Tri.UNKNOWN))
        validity = verdict.by_dimension(Dimension.VALIDITY)
        assert validity.status is Status.INCONCLUSIVE
        assert any("unknown" in reason for reason in validity.reasons)

    def test_an_uncalibrated_driver_makes_the_run_invalid(self):
        # Case E2E-37: the pipeline may have been fine; nothing was learned.
        verdict = decide(clean_evidence(workload_calibrated=Tri.FALSE))
        assert verdict.by_dimension(Dimension.VALIDITY).status is Status.INVALID
        assert verdict.overall is Status.INVALID

    def test_a_mislabelled_steady_window_makes_the_run_invalid(self):
        # Case E2E-43: one minute of steady state labelled as thirty.
        verdict = decide(clean_evidence(steady_window_matches_manifest=Tri.FALSE))
        assert verdict.by_dimension(Dimension.VALIDITY).status is Status.INVALID

    def test_an_unsealed_source_makes_the_run_invalid(self):
        verdict = decide(clean_evidence(source_sealed=Tri.FALSE))
        assert verdict.by_dimension(Dimension.VALIDITY).status is Status.INVALID

    def test_a_fault_case_without_a_receipt_is_invalid(self):
        # Section 14: a script exiting 0 is not proof the fault landed.
        verdict = decide(
            clean_evidence(is_fault_case=True, fault_receipt_valid=Tri.FALSE)
        )
        assert verdict.by_dimension(Dimension.VALIDITY).status is Status.INVALID

    def test_a_normal_run_needs_no_fault_receipt(self):
        # Section 15's note: NOT_APPLICABLE is accepted, not a hard failure.
        verdict = decide(clean_evidence(is_fault_case=False, fault_receipt_valid=Tri.UNKNOWN))
        assert verdict.by_dimension(Dimension.VALIDITY).status is Status.PASS

    def test_clock_and_sample_gates_only_apply_to_latency_claims(self):
        verdict = decide(
            clean_evidence(
                is_latency_claim=False,
                clock_adequate_for_claims=Tri.UNKNOWN,
                sufficient_samples=Tri.UNKNOWN,
                histogram_resolution_adequate=Tri.UNKNOWN,
            )
        )
        assert verdict.by_dimension(Dimension.VALIDITY).status is Status.PASS

    def test_an_inadequate_clock_invalidates_a_latency_claim(self):
        # Case E2E-42.
        verdict = decide(
            clean_evidence(is_latency_claim=True, clock_adequate_for_claims=Tri.FALSE)
        )
        assert verdict.by_dimension(Dimension.VALIDITY).status is Status.INVALID

    def test_a_coarse_histogram_invalidates_a_threshold_claim(self):
        # Case E2E-40.
        verdict = decide(
            clean_evidence(is_latency_claim=True, histogram_resolution_adequate=Tri.FALSE)
        )
        assert verdict.by_dimension(Dimension.VALIDITY).status is Status.INVALID


class TestSimulationCannotEvidenceClaims:
    def test_a_simulated_capacity_claim_is_invalid(self):
        verdict = decide(clean_evidence(sut_is_simulation=True, is_capacity_claim=True))
        validity = verdict.by_dimension(Dimension.VALIDITY)
        assert validity.status is Status.INVALID
        assert any("simulation" in reason for reason in validity.reasons)

    def test_a_simulated_latency_claim_is_invalid(self):
        verdict = decide(clean_evidence(sut_is_simulation=True, is_latency_claim=True))
        assert verdict.by_dimension(Dimension.VALIDITY).status is Status.INVALID

    def test_a_simulated_run_making_no_claim_can_still_pass(self):
        # This is how the measurement tools get validated: the simulation proves
        # the instruments, not the pipeline.
        verdict = decide(clean_evidence(sut_is_simulation=True))
        assert verdict.overall is Status.PASS
        assert any("validate the measurement tools" in note for note in verdict.notes)


class TestCorrectness:
    def test_a_failed_reconciliation_fails_correctness(self):
        verdict = decide(clean_evidence(reconciliation=Reconciliation.FAIL))
        assert verdict.by_dimension(Dimension.CORRECTNESS).status is Status.FAIL

    def test_no_reconciliation_at_all_is_inconclusive(self):
        # Section 7: without the oracle, app metrics do not certify correctness.
        verdict = decide(clean_evidence(reconciliation=None))
        correctness = verdict.by_dimension(Dimension.CORRECTNESS)
        assert correctness.status is Status.INCONCLUSIVE
        assert any("app counters" in reason for reason in correctness.reasons)

    def test_an_inconclusive_oracle_propagates(self):
        verdict = decide(clean_evidence(reconciliation=Reconciliation.INCONCLUSIVE))
        assert verdict.by_dimension(Dimension.CORRECTNESS).status is Status.INCONCLUSIVE

    def test_a_false_ready_fails_correctness(self):
        # INV-18 / NFR-10.
        verdict = decide(clean_evidence(false_ready=Tri.TRUE))
        assert verdict.by_dimension(Dimension.CORRECTNESS).status is Status.FAIL

    def test_an_early_offset_commit_fails_correctness(self):
        # INV-07.
        verdict = decide(clean_evidence(early_offset_commit=Tri.TRUE))
        assert verdict.by_dimension(Dimension.CORRECTNESS).status is Status.FAIL

    def test_wrong_query_results_fail_a_latency_lane(self):
        # Case E2E-38 reaching the verdict.
        verdict = decide(
            clean_evidence(is_latency_claim=True, query_results_correct=Tri.FALSE)
        )
        assert verdict.by_dimension(Dimension.CORRECTNESS).status is Status.FAIL


class TestCapacityAndLatency:
    def test_capacity_is_not_applicable_without_a_claim(self):
        verdict = decide(clean_evidence(is_capacity_claim=False))
        assert verdict.by_dimension(Dimension.CAPACITY).status is Status.NOT_APPLICABLE

    def test_a_growing_backlog_fails_a_capacity_claim(self):
        verdict = decide(clean_evidence(is_capacity_claim=True, backlog_stable=Tri.FALSE))
        capacity = verdict.by_dimension(Dimension.CAPACITY)
        assert capacity.status is Status.FAIL
        assert any("backlog grew" in reason for reason in capacity.reasons)

    def test_an_unreconciled_drain_fails_a_capacity_claim(self):
        verdict = decide(clean_evidence(is_capacity_claim=True, drain_reconciled=Tri.FALSE))
        assert verdict.by_dimension(Dimension.CAPACITY).status is Status.FAIL

    def test_a_short_steady_window_fails_a_capacity_claim(self):
        verdict = decide(clean_evidence(is_capacity_claim=True, steady_protocol_met=Tri.FALSE))
        assert verdict.by_dimension(Dimension.CAPACITY).status is Status.FAIL

    def test_a_clean_capacity_claim_passes(self):
        verdict = decide(clean_evidence(is_capacity_claim=True))
        assert verdict.by_dimension(Dimension.CAPACITY).status is Status.PASS

    def test_latency_is_not_applicable_without_a_claim(self):
        verdict = decide(clean_evidence(is_latency_claim=False))
        assert verdict.by_dimension(Dimension.LATENCY).status is Status.NOT_APPLICABLE

    def test_a_missed_budget_fails_latency(self):
        verdict = decide(clean_evidence(is_latency_claim=True, latency_within_budget=Tri.FALSE))
        assert verdict.by_dimension(Dimension.LATENCY).status is Status.FAIL

    def test_an_unmeasurable_budget_is_inconclusive(self):
        # A censored percentile that cannot settle the comparison.
        verdict = decide(clean_evidence(is_latency_claim=True, latency_within_budget=Tri.UNKNOWN))
        assert verdict.by_dimension(Dimension.LATENCY).status is Status.INCONCLUSIVE


class TestRecoverabilityAndSafety:
    def test_recoverability_is_not_applicable_without_a_fault(self):
        verdict = decide(clean_evidence(is_recovery_case=False))
        assert verdict.by_dimension(Dimension.RECOVERABILITY).status is Status.NOT_APPLICABLE

    def test_recovery_that_loses_data_fails_even_if_it_was_fast(self):
        verdict = decide(
            clean_evidence(
                is_recovery_case=True,
                recovery_within_budget=Tri.TRUE,
                recovery_preserved_correctness=Tri.FALSE,
            )
        )
        assert verdict.by_dimension(Dimension.RECOVERABILITY).status is Status.FAIL

    def test_an_oom_fails_safety(self):
        verdict = decide(clean_evidence(no_oom=Tri.FALSE))
        assert verdict.by_dimension(Dimension.SAFETY).status is Status.FAIL

    def test_silent_loss_fails_safety(self):
        verdict = decide(clean_evidence(no_silent_loss=Tri.FALSE))
        assert verdict.by_dimension(Dimension.SAFETY).status is Status.FAIL

    def test_an_action_outside_the_allowlist_fails_safety(self):
        verdict = decide(clean_evidence(safety_allowlist_respected=Tri.FALSE))
        assert verdict.by_dimension(Dimension.SAFETY).status is Status.FAIL

    def test_unknown_resource_bounds_are_inconclusive(self):
        # NFR-07 unverified is not NFR-07 met.
        verdict = decide(clean_evidence(resources_bounded=Tri.UNKNOWN))
        assert verdict.by_dimension(Dimension.SAFETY).status is Status.INCONCLUSIVE


class TestOverloadSpecialCase:
    def test_safe_overload_passes_safety_and_fails_capacity(self):
        # Section 7, verbatim: a run above capacity that stays bounded and stops
        # safely passes the overload-behaviour case and fails the rate claim.
        verdict = decide(
            clean_evidence(
                is_overload_case=True,
                is_capacity_claim=True,
                achieved_rate_meets_claim=Tri.FALSE,
                backlog_stable=Tri.FALSE,
                degraded_safely=Tri.TRUE,
            )
        )
        assert verdict.by_dimension(Dimension.SAFETY).status is Status.PASS
        assert verdict.by_dimension(Dimension.CAPACITY).status is Status.FAIL
        assert verdict.overall is Status.FAIL

    def test_an_overload_that_was_not_handled_safely_fails_safety_too(self):
        verdict = decide(
            clean_evidence(is_overload_case=True, degraded_safely=Tri.FALSE)
        )
        assert verdict.by_dimension(Dimension.SAFETY).status is Status.FAIL

    def test_a_non_overload_case_is_not_held_to_degradation(self):
        verdict = decide(clean_evidence(is_overload_case=False, degraded_safely=Tri.UNKNOWN))
        assert verdict.by_dimension(Dimension.SAFETY).status is Status.PASS


class TestBlockedByUpstream:
    def test_blocked_stages_short_circuit_every_dimension(self):
        # Section 20: neither a pass nor a failure of the system under test.
        verdict = decide(
            clean_evidence(blocked_stages=("P04_BROKER", "P06_CANONICAL"))
        )
        assert verdict.overall is Status.BLOCKED_BY_UPSTREAM
        assert all(
            v.status is Status.BLOCKED_BY_UPSTREAM for v in verdict.dimensions
        )

    def test_the_blocked_stages_are_named(self):
        verdict = decide(clean_evidence(blocked_stages=("P04_BROKER",)))
        assert "P04_BROKER" in verdict.as_dict()["blocked_stages"]
        assert any("P04_BROKER" in r for r in verdict.dimensions[0].reasons)

    def test_blocked_outranks_a_clean_pass(self):
        verdict = decide(clean_evidence(blocked_stages=("P04_BROKER",)))
        assert verdict.overall is not Status.PASS


class TestVerdictSerialisation:
    def test_blocking_findings_list_every_reason(self):
        verdict = decide(clean_evidence(no_oom=Tri.FALSE, reconciliation=Reconciliation.FAIL))
        findings = verdict.blocking_findings
        assert {f["dimension"] for f in findings} == {"correctness", "safety"}

    def test_as_dict_has_both_the_summary_and_the_detail(self):
        payload = decide(clean_evidence()).as_dict()
        assert payload["dimensions"]["safety"] == "PASS"
        assert len(payload["dimension_detail"]) == 6

    def test_an_unknown_dimension_lookup_raises(self):
        with pytest.raises(KeyError):
            decide(clean_evidence()).by_dimension("not-a-dimension")  # type: ignore[arg-type]
