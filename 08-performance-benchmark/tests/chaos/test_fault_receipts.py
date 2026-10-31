"""Fault allowlist and receipts (sections 8, 14; group G30).

The rule under test is section 14's closing sentence: a script exiting 0 is not
evidence that the fault reached its target. Every test here is either about
refusing a target outside the lab, or about refusing a receipt that nobody
verified.

No test injects anything real. The actions are callables the test supplies, so
the injector logic is exercised without touching a container, a network or a
disk -- which is also what ``dry_run=True`` defaults exist for.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from p08_bench.chaos import (
    Allowlist,
    FaultInjector,
    FaultKind,
    FaultSpec,
    Landing,
    _forbidden_roots,
    default_lab_allowlist,
    exit_code_is_not_proof,
    platform_chaos_limitations,
)
from p08_bench.clock import ClockDomain, FakeClock
from p08_bench.errors import SafetyError

pytestmark = pytest.mark.chaos


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(ClockDomain("bench", "chaos"))


@pytest.fixture
def allowlist(tmp_path: Path) -> Allowlist:
    return Allowlist.for_lab(
        containers=("p08-lab-kafka",),
        networks=("p08-lab_default",),
        volumes=("p08-lab_clickhouse",),
        paths=(tmp_path / "volume",),
    )


def spec(**overrides) -> FaultSpec:
    base = dict(
        fault_id="F-001",
        kind=FaultKind.PROCESS_STOP,
        target="p08-lab-kafka",
        start_condition="steady window reached",
        end_condition="30 s elapsed",
        verification="broker reports the partition leader moved",
    )
    base.update(overrides)
    return FaultSpec(**base)


class TestAllowlist:
    def test_an_empty_allowlist_permits_nothing(self):
        # A fresh injector must not be able to touch the host.
        empty = Allowlist()
        with pytest.raises(SafetyError):
            empty.check_container("anything")
        with pytest.raises(SafetyError):
            empty.check_network("anything")
        with pytest.raises(SafetyError):
            empty.check_path("/tmp")

    def test_a_listed_container_is_permitted(self, allowlist):
        allowlist.check_container("p08-lab-kafka")

    def test_an_unlisted_container_is_refused_with_the_allowed_set(self, allowlist):
        with pytest.raises(SafetyError, match="not on the lab allowlist") as excinfo:
            allowlist.check_container("prod-kafka-1")
        assert excinfo.value.context["allowed"] == ["p08-lab-kafka"]

    def test_an_unlisted_network_is_refused(self, allowlist):
        with pytest.raises(SafetyError, match="network"):
            allowlist.check_network("bridge")

    def test_an_unlisted_volume_is_refused(self, allowlist):
        with pytest.raises(SafetyError, match="volume"):
            allowlist.check_volume("prod_data")

    def test_a_path_inside_the_allowlisted_root_is_permitted(self, allowlist, tmp_path):
        target = tmp_path / "volume" / "fill"
        assert allowlist.check_path(target) == target.resolve()

    def test_a_path_outside_every_root_is_refused(self, allowlist, tmp_path):
        with pytest.raises(SafetyError, match="outside every allowlisted root"):
            allowlist.check_path(tmp_path / "elsewhere")

    def test_a_filesystem_root_is_refused_on_every_platform(self):
        # Section 8: disk-fill must stay inside a test volume. Filling a drive
        # root is indistinguishable from filling the host.
        root = Path(Path.cwd().anchor)
        listed = Allowlist.for_lab(paths=(root,))
        with pytest.raises(SafetyError, match="filesystem root"):
            listed.check_path(root)

    @pytest.mark.parametrize("name", sorted(set(_forbidden_roots())))
    def test_host_system_directories_are_refused(self, name):
        listed = Allowlist.for_lab(paths=(name,))
        with pytest.raises(SafetyError, match="host system path"):
            listed.check_path(name)

    @pytest.mark.parametrize("name", sorted(set(_forbidden_roots())))
    def test_paths_inside_a_host_system_directory_are_also_refused(self, name):
        # The containment check is the point: forbidding one exact string would
        # leave every file under it reachable.
        listed = Allowlist.for_lab(paths=(name,))
        with pytest.raises(SafetyError, match="host system path"):
            listed.check_path(Path(name) / "deeper" / "target")

    def test_allowlisting_a_system_root_does_not_unlock_it(self):
        # The forbidden-root check is not overridable by listing the parent.
        roots = _forbidden_roots()
        if not roots:
            pytest.skip("no platform system roots to test")
        listed = Allowlist.for_lab(paths=(roots[0],))
        with pytest.raises(SafetyError, match="host system path"):
            listed.check_path(roots[0])

    def test_the_root_escape_hatch_must_be_set_explicitly(self, tmp_path):
        # It exists so a deliberate lab decision is possible and visible, and
        # it still has to be inside an allowlisted root.
        listed = Allowlist.for_lab(paths=(tmp_path,), allow_host_root_path=True)
        assert listed.allow_host_root_path
        assert listed.check_path(tmp_path / "x") == (tmp_path / "x").resolve()

    def test_the_default_lab_allowlist_is_narrow(self, tmp_path):
        listed = default_lab_allowlist(tmp_path)
        assert listed.containers == frozenset()
        assert not listed.allow_host_root_path
        assert len(listed.paths) == 1

    def test_the_default_lab_allowlist_refuses_a_filesystem_root(self):
        with pytest.raises(SafetyError, match="may not be a filesystem root"):
            default_lab_allowlist(Path(Path.cwd().anchor))


class TestFaultSpec:
    def test_every_required_field_must_be_present(self):
        for field in ("fault_id", "target", "start_condition", "end_condition", "verification"):
            with pytest.raises(SafetyError, match=field):
                spec(**{field: "  "})

    def test_the_spec_records_what_verification_means(self):
        # Section 14 wants the verification condition declared before the run,
        # not invented after it.
        assert "leader moved" in spec().as_dict()["verification"]


class TestDryRunDefault:
    def test_the_injector_defaults_to_dry_run(self, allowlist, clock):
        injector = FaultInjector(allowlist, clock)
        assert injector.dry_run

    def test_a_dry_run_validates_the_target_and_injects_nothing(self, allowlist, clock):
        injector = FaultInjector(allowlist, clock)
        calls: list[str] = []
        receipt = injector.inject(
            spec(), lambda s: calls.append("acted") or "acted", lambda s: True
        )
        assert calls == []
        assert receipt.landing is Landing.DRY_RUN
        assert not receipt.valid

    def test_a_dry_run_still_refuses_a_target_outside_the_allowlist(self, allowlist, clock):
        injector = FaultInjector(allowlist, clock)
        with pytest.raises(SafetyError):
            injector.inject(spec(target="prod-kafka-1"), lambda s: "", lambda s: True)

    def test_a_dry_run_receipt_cannot_support_a_chaos_verdict(self, allowlist, clock):
        injector = FaultInjector(allowlist, clock)
        receipt = injector.inject(spec(), lambda s: "", lambda s: True)
        assert not receipt.valid


class TestReceipts:
    def _armed(self, allowlist, clock) -> FaultInjector:
        return FaultInjector(allowlist, clock, dry_run=False)

    def test_a_verified_fault_with_verified_cleanup_is_valid(self, allowlist, clock):
        injector = self._armed(allowlist, clock)
        receipt = injector.inject(
            spec(),
            lambda s: "stopped the container",
            lambda s: True,
            cleanup=lambda s: "restarted the container",
            cleanup_verifier=lambda s: True,
        )
        assert receipt.landing is Landing.VERIFIED
        assert receipt.cleanup_verified
        assert receipt.valid

    def test_a_fault_that_did_not_land_is_not_triggered(self, allowlist, clock):
        # Section 14: no trigger means INVALID upstream, not a resilience pass.
        injector = self._armed(allowlist, clock)
        receipt = injector.inject(
            spec(), lambda s: "ran the command", lambda s: False,
            cleanup=lambda s: "cleaned", cleanup_verifier=lambda s: True,
        )
        assert receipt.landing is Landing.NOT_TRIGGERED
        assert not receipt.valid

    def test_an_unverifiable_observation_is_its_own_landing(self, allowlist, clock):
        injector = self._armed(allowlist, clock)
        receipt = injector.inject(
            spec(), lambda s: "ran", lambda s: None,
            cleanup=lambda s: "cleaned", cleanup_verifier=lambda s: True,
        )
        assert receipt.landing is Landing.UNVERIFIABLE
        assert not receipt.valid

    def test_an_unverified_cleanup_invalidates_the_receipt(self, allowlist, clock):
        # The next run would otherwise start from an unknown state.
        injector = self._armed(allowlist, clock)
        receipt = injector.inject(
            spec(), lambda s: "ran", lambda s: True, cleanup=lambda s: "maybe cleaned"
        )
        assert receipt.landing is Landing.VERIFIED
        assert not receipt.cleanup_verified
        assert not receipt.valid
        assert "cleanup not verified" in receipt.verification_detail

    def test_the_receipt_records_the_injection_interval(self, allowlist, clock):
        injector = self._armed(allowlist, clock)

        def act(s):
            clock.advance_s(30)
            return "held for 30 s"

        receipt = injector.inject(
            spec(), act, lambda s: True, cleanup=lambda s: "cleared",
            cleanup_verifier=lambda s: True,
        )
        assert receipt.duration_ns == 30_000_000_000

    def test_a_receipt_with_no_cleanup_has_no_clear_time(self, allowlist, clock):
        injector = self._armed(allowlist, clock)
        receipt = injector.inject(spec(), lambda s: "ran", lambda s: True)
        assert receipt.cleared_at_ns is None
        assert receipt.duration_ns is None

    def test_the_receipt_dict_states_the_exit_code_rule(self, allowlist, clock):
        receipt = FaultInjector(allowlist, clock).inject(spec(), lambda s: "", lambda s: True)
        assert "exit code of 0 is not evidence" in receipt.as_dict()["note"]

    def test_a_proof_cut_can_be_attached(self, allowlist, clock):
        injector = self._armed(allowlist, clock)
        receipt = injector.inject(
            spec(), lambda s: "ran", lambda s: True, proof_cut="cut_steady_end"
        )
        assert receipt.proof_cut == "cut_steady_end"


class TestTargetRouting:
    def test_a_disk_fill_is_checked_against_the_paths(self, allowlist, clock, tmp_path):
        injector = FaultInjector(allowlist, clock)
        injector.inject(
            spec(kind=FaultKind.DISK_FILL, target=str(tmp_path / "volume" / "big")),
            lambda s: "", lambda s: True,
        )
        with pytest.raises(SafetyError):
            injector.inject(
                spec(kind=FaultKind.DISK_FILL, target=str(tmp_path / "outside")),
                lambda s: "", lambda s: True,
            )

    def test_a_network_fault_is_checked_against_the_networks(self, allowlist, clock):
        injector = FaultInjector(allowlist, clock)
        injector.inject(
            spec(kind=FaultKind.NETWORK_LATENCY, target="p08-lab_default"),
            lambda s: "", lambda s: True,
        )
        with pytest.raises(SafetyError, match="network"):
            injector.inject(
                spec(kind=FaultKind.NETWORK_LOSS, target="bridge"),
                lambda s: "", lambda s: True,
            )

    def test_an_observer_crash_is_a_first_class_fault(self, allowlist, clock):
        # Section 8 reserves a fault class for breaking the instruments.
        injector = FaultInjector(allowlist, clock)
        receipt = injector.inject(
            spec(kind=FaultKind.OBSERVER_CRASH), lambda s: "", lambda s: True
        )
        assert receipt.spec.kind is FaultKind.OBSERVER_CRASH


class TestCampaignSummary:
    def test_the_summary_separates_landings(self, allowlist, clock):
        injector = FaultInjector(allowlist, clock, dry_run=False)
        injector.inject(spec(fault_id="F1"), lambda s: "", lambda s: True)
        injector.inject(spec(fault_id="F2"), lambda s: "", lambda s: False)
        injector.inject(spec(fault_id="F3"), lambda s: "", lambda s: None)
        summary = injector.campaign_summary()
        assert summary["requested"] == 3
        assert summary["verified"] == 1
        assert summary["not_triggered"] == 1
        assert summary["unverifiable"] == 1

    def test_a_campaign_is_only_valid_when_every_receipt_is(self, allowlist, clock):
        injector = FaultInjector(allowlist, clock, dry_run=False)
        injector.inject(
            spec(), lambda s: "", lambda s: True, cleanup=lambda s: "", cleanup_verifier=lambda s: True
        )
        assert injector.campaign_summary()["all_valid"]
        injector.inject(spec(fault_id="F2"), lambda s: "", lambda s: False)
        assert not injector.campaign_summary()["all_valid"]

    def test_an_empty_campaign_is_not_valid(self, allowlist, clock):
        # Nothing attempted is not "all faults verified".
        assert not FaultInjector(allowlist, clock).campaign_summary()["all_valid"]

    def test_the_summary_carries_the_allowlist_and_the_mode(self, allowlist, clock):
        summary = FaultInjector(allowlist, clock).campaign_summary()
        assert summary["dry_run_mode"] is True
        assert summary["allowlist"]["containers"] == ["p08-lab-kafka"]


class TestNamedRefusals:
    def test_an_exit_code_is_never_proof(self):
        assert exit_code_is_not_proof(0) is False
        assert exit_code_is_not_proof(1) is False

    def test_platform_limitations_are_stated_on_non_linux(self):
        import sys

        notes = platform_chaos_limitations()
        if sys.platform.startswith("linux"):
            assert notes == []
        else:
            assert any("netem" in note for note in notes)
