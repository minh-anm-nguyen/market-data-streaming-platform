"""Capacity gates and the frontier (sections 4.2, 6.3, 19.1).

The rule this file protects is section 6.3's: if 100k passes every gate and
120k does not, the verified bound is ``>= 100k`` and **nothing** may be said
about the levels in between. :meth:`TestFrontier.test_extrapolation_always_raises`
is that rule as code.
"""

from __future__ import annotations

import pytest

from p08_bench.capacity import (
    CapacityFrontier,
    CapacityGates,
    Gate,
    gates_from_observations,
    littles_law_population,
    payload_bandwidth,
)
from p08_bench.errors import MetrologyError
from p08_bench.timeseries import Stability, StabilityAssessment
from p08_bench.units import Unit, ticks_per_s
from p08_bench.verdict import Status, Tri


def stability(verdict: Stability, *, name: str = "md.ticks.v1-3") -> StabilityAssessment:
    return StabilityAssessment(
        series_name=name, verdict=verdict, full_slope_per_s=0.0, tail_slope_per_s=0.0,
        tolerance_per_s=1.0, n_windows=10, min_windows=5, tail_windows=5,
        reasons=["synthetic"],
    )


def all_good(**overrides):
    kwargs = dict(
        useful_verified=ticks_per_s(100_000),
        calibrated=True,
        steady_seconds=1800,
        required_steady_seconds=1800,
        backlog=stability(Stability.STABLE),
        drain_reconciled=True,
        correctness_clean=True,
        latency_within_budget=True,
        error_fraction=0.0,
        error_budget=0.01,
        resource_blockers=[],
    )
    kwargs.update(overrides)
    return gates_from_observations(ticks_per_s(100_000), **kwargs)


class TestGates:
    def test_all_gates_true_is_a_pass(self):
        assert all_good().status is Status.PASS

    def test_an_unevaluated_gate_is_inconclusive_not_a_pass(self):
        gates = CapacityGates(offered=ticks_per_s(100_000))
        assert gates.status is Status.INCONCLUSIVE
        assert len(gates.unknown_gates()) == len(list(Gate))

    def test_one_false_gate_fails_the_level(self):
        gates = all_good(drain_reconciled=False)
        assert gates.status is Status.FAIL
        assert Gate.DRAIN_RECONCILED in gates.failed_gates()

    def test_a_false_gate_outranks_an_unknown_one(self):
        gates = all_good(drain_reconciled=False, latency_within_budget=None)
        assert gates.status is Status.FAIL

    def test_an_uncalibrated_driver_fails_the_level(self):
        assert Gate.CALIBRATED in all_good(calibrated=False).failed_gates()

    def test_a_short_steady_window_fails_the_level(self):
        gates = all_good(steady_seconds=60)
        assert Gate.STEADY_DURATION in gates.failed_gates()
        assert "60s against a required 1800s" in gates.get(Gate.STEADY_DURATION).detail

    def test_a_growing_backlog_fails_the_level(self):
        gates = all_good(backlog=stability(Stability.GROWING))
        assert Gate.BACKLOG_STABLE in gates.failed_gates()

    def test_a_draining_backlog_does_not_fail_the_level(self):
        # Offered below what the sink can take is not a broken capacity claim.
        gates = all_good(backlog=stability(Stability.SHRINKING))
        assert gates.get(Gate.BACKLOG_STABLE).value is Tri.TRUE

    def test_an_invalidated_backlog_series_is_unknown_not_failed(self):
        # Retention truncation means the question was not answered.
        gates = all_good(backlog=stability(Stability.INVALIDATED))
        assert gates.get(Gate.BACKLOG_STABLE).value is Tri.UNKNOWN
        assert gates.status is Status.INCONCLUSIVE

    def test_no_backlog_series_is_unknown(self):
        assert all_good(backlog=None).get(Gate.BACKLOG_STABLE).value is Tri.UNKNOWN

    def test_an_error_rate_above_budget_fails(self):
        gates = all_good(error_fraction=0.05, error_budget=0.01)
        assert Gate.ERROR_BUDGET in gates.failed_gates()

    def test_an_unmeasured_error_rate_is_unknown(self):
        assert all_good(error_fraction=None).get(Gate.ERROR_BUDGET).value is Tri.UNKNOWN

    def test_unevidenced_resource_budgets_make_the_gate_unknown(self):
        # NFR-07 unverified is not NFR-07 met: the host could not read cgroup
        # counters, so the budget is unknown rather than satisfied.
        gates = all_good(resource_blockers=["cgroup_oom_kill_count is unknown"])
        assert gates.get(Gate.RESOURCE_BUDGET).value is Tri.UNKNOWN
        assert gates.status is Status.INCONCLUSIVE

    def test_the_gate_set_is_the_conjunction_of_section_4_2(self):
        assert {g.value for g in Gate} == {
            "driver_calibrated", "steady_duration", "backlog_stable_per_partition",
            "drain_reconciled", "correctness_clean", "latency_budget",
            "error_budget", "resource_budget",
        }

    def test_as_dict_names_the_failures(self):
        payload = all_good(correctness_clean=False).as_dict()
        assert payload["status"] == "FAIL"
        assert "correctness_clean" in payload["failed_gates"]


class TestFrontier:
    def _ladder(self) -> CapacityFrontier:
        frontier = CapacityFrontier(Unit.TICKS_PER_S)
        for rate in (50_000, 100_000):
            frontier.add(
                gates_from_observations(
                    ticks_per_s(rate), useful_verified=ticks_per_s(rate), calibrated=True,
                    steady_seconds=1800, required_steady_seconds=1800,
                    backlog=stability(Stability.STABLE), drain_reconciled=True,
                    correctness_clean=True, latency_within_budget=True,
                    error_fraction=0.0, error_budget=0.01, resource_blockers=[],
                )
            )
        frontier.add(
            gates_from_observations(
                ticks_per_s(120_000), useful_verified=ticks_per_s(101_000), calibrated=True,
                steady_seconds=1800, required_steady_seconds=1800,
                backlog=stability(Stability.GROWING), drain_reconciled=False,
                correctness_clean=True, latency_within_budget=True,
                error_fraction=0.0, error_budget=0.01, resource_blockers=[],
            )
        )
        return frontier

    def test_the_bound_is_the_highest_level_that_passed_every_gate(self):
        assert self._ladder().verified_bound().value == 100_000

    def test_the_bound_is_not_the_highest_level_that_ran(self):
        frontier = self._ladder()
        assert 120_000 in frontier.levels
        assert frontier.verified_bound().value == 100_000

    def test_the_bound_is_not_the_highest_throughput_observed(self):
        # The 120k level pushed 101k useful through before failing its gates;
        # that is not a capacity.
        frontier = self._ladder()
        assert frontier.result(120_000).useful_verified.value == 101_000
        assert frontier.verified_bound().value == 100_000

    def test_no_passing_level_means_no_capacity_is_established(self):
        frontier = CapacityFrontier()
        frontier.add(
            gates_from_observations(
                ticks_per_s(1000), useful_verified=None, calibrated=False,
                steady_seconds=10, required_steady_seconds=1800, backlog=None,
                drain_reconciled=None, correctness_clean=None,
                latency_within_budget=None, error_fraction=None, error_budget=0.01,
                resource_blockers=["unknown"],
            )
        )
        assert frontier.verified_bound() is None
        assert "no capacity is established" in frontier.as_dict()["statement"]

    def test_the_knee_names_the_unexplored_gap(self):
        knee = self._ladder().knee()
        assert knee["verified_bound"]["value"] == 100_000
        assert knee["first_failing_level"]["value"] == 120_000
        assert knee["unexplored_gap"]["between"] == [100_000, 120_000]

    def test_extrapolation_always_raises(self):
        # Section 6.3: "100k passed and 120k failed, so 110k is fine" is exactly
        # the inference that is forbidden -- nobody measured 110k.
        with pytest.raises(MetrologyError, match="may not be extrapolated"):
            self._ladder().extrapolate(110_000)

    def test_a_level_in_another_unit_is_refused(self):
        from p08_bench.units import records_per_s

        frontier = CapacityFrontier(Unit.TICKS_PER_S)
        gates = CapacityGates(offered=records_per_s(1000))
        with pytest.raises(MetrologyError, match="unit does not match"):
            frontier.add(gates)

    def test_a_non_rate_unit_frontier_is_refused(self):
        with pytest.raises(MetrologyError, match="needs a rate unit"):
            CapacityFrontier(Unit.TICKS)

    def test_an_unmeasured_level_cannot_be_read(self):
        with pytest.raises(MetrologyError, match="no result for that level"):
            self._ladder().result(110_000)

    def test_the_statement_is_a_lower_bound(self):
        assert ">= 100000 ticks/s" in self._ladder().as_dict()["statement"]


class TestLittlesLaw:
    def test_it_applies_in_a_stationary_system(self):
        assert littles_law_population(1000, 0.5, stationary=True) == 500

    def test_it_returns_nothing_in_overload(self):
        # Section 6.3: in non-stationary overload the formula produces a number
        # that means nothing.
        assert littles_law_population(1000, 0.5, stationary=False) is None

    def test_negative_inputs_are_refused(self):
        with pytest.raises(MetrologyError, match="non-negative"):
            littles_law_population(-1, 0.5, stationary=True)


class TestPayloadBandwidth:
    def test_the_section_4_2_illustration_reproduces(self):
        # 120k x 160 B is about 19.2 MB/s of payload.
        result = payload_bandwidth(120_000, 160)
        assert result["payload_mb_per_s"] == pytest.approx(19.2)

    def test_binary_and_decimal_are_both_reported(self):
        result = payload_bandwidth(120_000, 160)
        assert result["payload_mib_per_s"] != result["payload_mb_per_s"]

    def test_the_exclusions_travel_with_the_number(self):
        # The hazard is someone sizing a production network from 19.2 MB/s.
        result = payload_bandwidth(120_000, 160)
        assert "replication traffic" in result["excludes"]
        assert "not for" in " ".join(result).lower() or "not_for" in result

    def test_negative_inputs_are_refused(self):
        with pytest.raises(MetrologyError):
            payload_bandwidth(-1, 160)
