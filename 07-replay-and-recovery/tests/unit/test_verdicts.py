"""The gate's vocabulary and its precedence rules.

These are the tests that stop the harness from lying. Each one corresponds to a
way a reliability suite can report success it has not earned.
"""

from __future__ import annotations

import pytest

from p07.evidence.verdicts import (
    Finding,
    GateResult,
    Reason,
    Verdict,
    classify,
    expected_refusal,
)


def test_verdict_has_no_truth_value() -> None:
    """``if verdict:`` would make INCONCLUSIVE truthy. It must not compile away."""
    with pytest.raises(TypeError):
        bool(Verdict.INCONCLUSIVE)
    with pytest.raises(TypeError):
        _ = "yes" if Verdict.PASS else "no"


def test_only_pass_is_not_release_blocking() -> None:
    assert Verdict.PASS.is_release_blocking() is False
    for verdict in (Verdict.FAIL, Verdict.INCONCLUSIVE, Verdict.INVALID, Verdict.NOT_RUN):
        assert verdict.is_release_blocking() is True


def test_no_findings_and_a_confirmed_fault_is_a_pass() -> None:
    result = classify([], fault_confirmed=True)
    assert result.verdict is Verdict.PASS
    assert result.primary_reason == Reason.ALL_CHECKS_PROVEN


@pytest.mark.whatif("P07-G27-01")
def test_unconfirmed_fault_outranks_everything() -> None:
    """``INV-06``: a fault that never fired cannot produce a pass, however clean."""
    result = classify([], fault_confirmed=False)
    assert result.verdict is Verdict.INVALID
    assert result.primary_reason == Reason.FAULT_NOT_CONFIRMED
    # Even alongside a real contradiction, the test is invalid rather than
    # failed: it did not test what it claimed to test.
    result = classify([Finding(Reason.MISSING_IDENTITIES)], fault_confirmed=False)
    assert result.verdict is Verdict.INVALID


def test_a_plain_contradiction_is_a_fail() -> None:
    result = classify([Finding(Reason.IDENTITY_CONFLICT, {"count": 1})])
    assert result.verdict is Verdict.FAIL
    assert result.primary_reason == Reason.IDENTITY_CONFLICT


@pytest.mark.whatif("P07-G29-04")
def test_missing_proof_outranks_a_contradiction() -> None:
    """If the lab could not read what it needed, it does not know the answer.

    Reporting FAIL for data it merely could not see would be as much a
    fabrication as reporting PASS, so the contradiction is kept in the findings
    while the verdict says "cannot tell". Either way the case blocks release.
    """
    result = classify([Finding(Reason.MISSING_IDENTITIES), Finding(Reason.CUT_RANGE_EXPIRED)])
    assert result.verdict is Verdict.INCONCLUSIVE
    assert result.primary_reason == Reason.CUT_RANGE_EXPIRED
    assert {finding.reason for finding in result.findings} == {
        Reason.MISSING_IDENTITIES,
        Reason.CUT_RANGE_EXPIRED,
    }
    assert result.verdict.is_release_blocking()


def test_invalid_class_reason_outranks_inconclusive() -> None:
    result = classify([Finding(Reason.CUT_RANGE_EXPIRED), Finding(Reason.TARGET_NOT_ALLOWLISTED)])
    assert result.verdict is Verdict.INVALID
    assert result.primary_reason == Reason.TARGET_NOT_ALLOWLISTED


@pytest.mark.parametrize(
    "reason",
    [
        Reason.SINK_ACK_UNKNOWN,
        Reason.PRODUCER_ACK_UNKNOWN,
        Reason.SOURCE_UNAVAILABLE,
        Reason.RAW_ROWS_EXPIRED,
        Reason.EVIDENCE_INCOMPLETE,
        Reason.EVIDENCE_QUOTA_EXCEEDED,
        Reason.DEADLINE_EXCEEDED,
        Reason.SOURCE_ANCHOR_MISSING,
    ],
)
def test_unknowable_reasons_are_inconclusive(reason: str) -> None:
    """Each of these means "the lab could not tell", not "the system is wrong"."""
    assert classify([Finding(reason)]).verdict is Verdict.INCONCLUSIVE


@pytest.mark.parametrize(
    "reason",
    [
        Reason.FAULT_WRONG_TARGET,
        Reason.FAULT_CLEANUP_MISSING,
        Reason.PROFILE_NOT_VALIDATED,
        Reason.CONFIG_INVALID,
        Reason.SOURCE_SCHEMA_SKEW,
        Reason.SOURCE_FAULT_PLAN_UNHASHED,
    ],
)
def test_test_validity_reasons_are_invalid(reason: str) -> None:
    assert classify([Finding(reason)]).verdict is Verdict.INVALID


def test_timeout_is_never_silently_turned_into_a_result() -> None:
    """Design 1.1 FR-14: a timeout that cannot be resolved stays unresolved."""
    assert classify([Finding(Reason.DEADLINE_EXCEEDED)]).verdict is Verdict.INCONCLUSIVE


def test_expected_refusal_is_a_pass_that_records_what_was_refused() -> None:
    """Several matrix rows expect a refusal, which is a pass with content."""
    result = expected_refusal(Reason.SOURCE_PART_FILE_PRESENT, {"file": "events.jsonl.part"})
    assert result.verdict is Verdict.PASS
    assert result.primary_reason == Reason.EXPECTED_REFUSAL
    assert result.findings[0].reason == Reason.SOURCE_PART_FILE_PRESENT
    assert result.findings[0].detail["file"] == "events.jsonl.part"


def test_a_finding_needs_a_reason_code() -> None:
    with pytest.raises(ValueError):
        Finding("")


def test_gate_result_serialises_for_the_report() -> None:
    result = GateResult(
        Verdict.FAIL,
        Reason.BARS_MISMATCH,
        (Finding(Reason.BARS_MISMATCH, {"mismatched": 2}, ("analytics/bars_diff.json",)),),
    )
    payload = result.as_dict()
    assert payload["verdict"] == "FAIL"
    assert payload["findings"][0]["detail"] == {"mismatched": 2}
    assert payload["findings"][0]["evidence_refs"] == ["analytics/bars_diff.json"]


def test_reason_codes_are_unique() -> None:
    """A duplicated code would silently merge two different failure modes."""
    codes = [
        value
        for name, value in vars(Reason).items()
        if not name.startswith("_") and isinstance(value, str)
    ]
    assert len(codes) == len(set(codes))
