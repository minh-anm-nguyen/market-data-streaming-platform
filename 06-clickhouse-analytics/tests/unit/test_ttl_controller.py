"""The retention gate and reconstructability (group O, P)."""

from __future__ import annotations

from datetime import date

import pytest

from _coverage import covers
from md_analytics.errors import Verdict
from md_analytics.ttl_controller import (
    ReconstructabilityCheck,
    RetentionPolicy,
    evaluate_retention_gate,
    ttl_cutoff,
    would_expire_on_arrival,
)

TODAY = date(2026, 10, 30)


def policy(**overrides) -> RetentionPolicy:
    kwargs = dict(
        raw_ttl_days=90,
        recovery_window_days=7,
        audit_window_days=30,
        rebuild_window_days=14,
        late_arrival_window_days=3,
        restore_drill_passed=True,
        backup_age_days=1,
        backup_max_age_days=2,
        enabled=False,
    )
    kwargs.update(overrides)
    return RetentionPolicy(**kwargs)


class TestGate:
    @covers("P06-O-05")
    def test_a_policy_covering_every_window_with_a_tested_restore_passes(self):
        gate = evaluate_retention_gate(policy())
        assert gate.allowed and gate.verdict() is Verdict.PASS
        assert gate.required_days == 30

    @covers("P06-O-02", "P06-O-05")
    def test_a_ttl_shorter_than_the_audit_window_is_refused(self):
        gate = evaluate_retention_gate(policy(raw_ttl_days=10))
        assert not gate.allowed
        assert gate.verdict() is Verdict.NO_GO
        assert any("shorter than the longest window" in r for r in gate.reasons)

    @covers("P06-P-01", "P06-O-05")
    def test_an_untested_restore_blocks_ttl(self):
        gate = evaluate_retention_gate(policy(restore_drill_passed=False))
        assert not gate.allowed
        assert any("never been restored" in r for r in gate.reasons)

    @covers("P06-P-05")
    def test_an_unknown_backup_age_blocks_ttl(self):
        gate = evaluate_retention_gate(policy(backup_age_days=None))
        assert not gate.allowed
        assert any("backup age is unknown" in r for r in gate.reasons)

    @covers("P06-P-05")
    def test_a_stale_backup_blocks_ttl(self):
        gate = evaluate_retention_gate(policy(backup_age_days=9))
        assert not gate.allowed
        assert any("over the 2-day limit" in r for r in gate.reasons)

    def test_every_failing_condition_is_reported_not_just_the_first(self):
        gate = evaluate_retention_gate(
            policy(raw_ttl_days=1, restore_drill_passed=False, backup_age_days=99)
        )
        assert len(gate.reasons) == 3

    def test_a_non_positive_ttl_is_refused(self):
        assert not evaluate_retention_gate(policy(raw_ttl_days=0)).allowed

    def test_the_gate_serialises_for_evidence(self):
        payload = evaluate_retention_gate(policy()).as_dict()
        assert payload["verdict"] == "PASS"
        assert payload["required_days"] == 30 and payload["raw_ttl_days"] == 90

    @covers("P06-O-05")
    def test_a_config_file_cannot_assert_a_passed_restore_drill(self, dev_config):
        # The config maps to a policy with restore_drill_passed=False by
        # construction: a drill is an event that happened, not a setting.
        gate = evaluate_retention_gate(dev_config.retention_policy())
        assert not gate.allowed
        assert any("restore" in r for r in gate.reasons)


class TestCutoff:
    @covers("P06-O-01")
    def test_the_cutoff_is_the_oldest_date_kept(self):
        assert ttl_cutoff(today=TODAY, ttl_days=90) == date(2026, 8, 1)

    def test_a_non_positive_ttl_has_no_cutoff(self):
        with pytest.raises(ValueError):
            ttl_cutoff(today=TODAY, ttl_days=0)

    @covers("P06-O-03")
    def test_a_late_record_already_past_the_ttl_is_detected(self):
        assert would_expire_on_arrival(
            event_date=date(2026, 7, 1), today=TODAY, ttl_days=90
        ), (
            "a 100-day-old replay into a 90-day TTL would be deleted at the next "
            "merge, which is indistinguishable from never arriving unless it is "
            "refused up front"
        )

    @covers("P06-O-03")
    def test_a_record_inside_the_window_does_not_expire_on_arrival(self):
        assert not would_expire_on_arrival(
            event_date=date(2026, 10, 1), today=TODAY, ttl_days=90
        )

    @covers("P06-O-01")
    def test_the_boundary_date_itself_is_kept(self):
        assert not would_expire_on_arrival(
            event_date=date(2026, 8, 1), today=TODAY, ttl_days=90
        )


class TestReconstructability:
    @covers("P06-P-04")
    @pytest.mark.parametrize(
        "raw,replay,backup",
        [(True, False, False), (False, True, False), (False, False, True), (True, True, True)],
    )
    def test_any_one_source_makes_a_rebuild_possible(self, raw, replay, backup):
        check = ReconstructabilityCheck(raw, replay, backup)
        assert check.verdict() is Verdict.PASS
        assert "reconstructable from" in check.reason()

    @covers("P06-O-02", "P06-P-04", "P06-AB-02")
    def test_nothing_left_is_not_reconstructable_and_says_so(self):
        check = ReconstructabilityCheck(False, False, False)
        assert check.verdict() is Verdict.NO_GO
        assert check.reason().startswith("NOT_RECONSTRUCTABLE")
