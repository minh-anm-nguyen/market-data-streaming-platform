"""A/B experiments (sections 4.4, 12, 19.2; NFR-11; cases E2E-45, E2E-47).

Four refusals are tested here, in the precedence the module applies them:
a durability-weakening arm is rejected outright (E2E-47), a second changed
variable makes the pair confounded (E2E-45), too few or badly ordered runs are
inconclusive, and a median gain with a wrecked tail is a regression (section
19.2).
"""

from __future__ import annotations

import pytest

from p08_bench.errors import MetrologyError
from p08_bench.experiment import (
    DEFAULT_MIN_MEDIAN_IMPROVEMENT,
    Arm,
    Hypothesis,
    Outcome,
    PairedExperiment,
    RunSample,
    is_paired_order,
    reversal_holds,
)

ABBA = (Arm.A, Arm.B, Arm.B, Arm.A)


def hypothesis(**overrides) -> Hypothesis:
    defaults = dict(
        statement="raising P05 max_rows increases verified throughput",
        single_variable="max_rows",
        expected_direction="higher",
        min_pairs=2,
    )
    defaults.update(overrides)
    return Hypothesis(**defaults)


def experiment(
    a_values: list[float],
    b_values: list[float],
    *,
    a_tails: list[float] | None = None,
    b_tails: list[float] | None = None,
    correctness: bool | None = True,
    **kwargs,
) -> PairedExperiment:
    exp = PairedExperiment(hypothesis=kwargs.pop("hypothesis", hypothesis()), **kwargs)
    order_index = 0
    for index in range(max(len(a_values), len(b_values))):
        # Alternate so the default order is paired.
        for arm, values, tails in (
            (Arm.A, a_values, a_tails),
            (Arm.B, b_values, b_tails),
        ):
            if index >= len(values):
                continue
            exp.add(
                RunSample(
                    arm=arm,
                    order_index=order_index,
                    primary=values[index],
                    tail=tails[index] if tails else None,
                    correctness_clean=correctness,
                )
            )
            order_index += 1
    return exp


class TestOrdering:
    def test_abba_is_paired(self):
        assert is_paired_order(list(ABBA))

    def test_abab_is_paired(self):
        assert is_paired_order([Arm.A, Arm.B, Arm.A, Arm.B])

    def test_aaabbb_is_not_paired(self):
        # Host drift across the session would land entirely on the last arm.
        assert not is_paired_order([Arm.A, Arm.A, Arm.A, Arm.B, Arm.B, Arm.B])

    def test_unequal_arms_are_not_paired(self):
        assert not is_paired_order([Arm.A, Arm.A, Arm.B])

    def test_an_empty_order_is_not_paired(self):
        assert not is_paired_order([])

    def test_two_in_a_row_is_still_acceptable(self):
        # ABBA has a BB block by construction, so only three-in-a-row is refused.
        assert is_paired_order([Arm.A, Arm.B, Arm.B, Arm.A])


class TestHypothesis:
    def test_a_hypothesis_must_name_its_variable(self):
        with pytest.raises(MetrologyError, match="single variable"):
            Hypothesis(statement="s", single_variable="  ", expected_direction="higher")

    def test_fewer_than_two_pairs_is_refused(self):
        with pytest.raises(MetrologyError, match="at least 2 pairs"):
            hypothesis(min_pairs=1)

    def test_the_nfr11_threshold_is_the_default(self):
        assert DEFAULT_MIN_MEDIAN_IMPROVEMENT == 0.05
        assert hypothesis().min_median_improvement == 0.05

    def test_the_threshold_is_recorded_in_the_output(self):
        # Section 16: the definition of a win may not move after the result.
        exp = experiment([100.0, 100.0], [110.0, 110.0])
        assert exp.as_dict()["hypothesis"]["min_median_improvement"] == 0.05


class TestEffect:
    def test_a_ten_percent_throughput_gain_is_an_optimization(self):
        exp = experiment([100.0, 100.0], [110.0, 110.0])
        outcome, reasons = exp.outcome()
        assert outcome is Outcome.OPTIMIZED
        assert any("median effect +0.1000" in r for r in reasons)

    def test_a_two_percent_gain_is_below_the_threshold(self):
        exp = experiment([100.0, 100.0], [102.0, 102.0])
        assert exp.outcome()[0] is Outcome.NO_EFFECT

    def test_a_loss_is_a_regression(self):
        exp = experiment([100.0, 100.0], [80.0, 80.0])
        assert exp.outcome()[0] is Outcome.REGRESSED

    def test_direction_is_respected_for_latency_experiments(self):
        # Lower is better: B at 80 ms against A at 100 ms is a 20% improvement.
        exp = experiment(
            [100.0, 100.0], [80.0, 80.0],
            hypothesis=hypothesis(higher_is_better=False, expected_direction="lower"),
        )
        assert exp.relative_effect() == pytest.approx(0.2)
        assert exp.outcome()[0] is Outcome.OPTIMIZED

    def test_a_zero_baseline_has_no_relative_effect(self):
        exp = experiment([0.0, 0.0], [10.0, 10.0])
        with pytest.raises(MetrologyError, match="baseline median is zero"):
            exp.relative_effect()

    def test_an_empty_arm_is_refused(self):
        exp = experiment([100.0], [])
        with pytest.raises(MetrologyError, match="both arms need"):
            exp.relative_effect()


class TestTailRegression:
    def test_a_median_gain_with_a_wrecked_tail_is_a_regression(self):
        # Section 19.2: +3% median with a 50% p99 regression is not an
        # improvement, whatever the statistics say.
        exp = experiment(
            [100.0, 100.0], [110.0, 110.0],
            a_tails=[200.0, 200.0], b_tails=[300.0, 300.0],
        )
        outcome, reasons = exp.outcome()
        assert outcome is Outcome.REGRESSED
        assert any("tail regressed" in r for r in reasons)

    def test_a_small_tail_change_is_tolerated(self):
        exp = experiment(
            [100.0, 100.0], [110.0, 110.0],
            a_tails=[200.0, 200.0], b_tails=[205.0, 205.0],
        )
        assert exp.outcome()[0] is Outcome.OPTIMIZED

    def test_an_improved_tail_is_not_a_regression(self):
        exp = experiment(
            [100.0, 100.0], [110.0, 110.0],
            a_tails=[200.0, 200.0], b_tails=[150.0, 150.0],
        )
        assert exp.tail_regression() == pytest.approx(-0.25)
        assert exp.outcome()[0] is Outcome.OPTIMIZED

    def test_an_unmeasured_tail_is_none_rather_than_zero(self):
        assert experiment([100.0, 100.0], [110.0, 110.0]).tail_regression() is None


class TestConfounds:
    def test_a_second_changed_variable_confounds_the_pair(self):
        # Case E2E-45: the CPU governor swapped for the B arm only.
        exp = experiment([100.0, 100.0], [130.0, 130.0])
        exp.declared_differences = ("max_rows",)
        exp.arm_metadata = {
            Arm.A: {"max_rows": 1000, "cpu_governor": "performance"},
            Arm.B: {"max_rows": 5000, "cpu_governor": "powersave"},
        }
        outcome, reasons = exp.outcome()
        assert outcome is Outcome.CONFOUNDED
        assert any("cpu_governor differs" in r for r in reasons)

    def test_the_declared_variable_is_not_a_confound(self):
        exp = experiment([100.0, 100.0], [110.0, 110.0])
        exp.declared_differences = ("max_rows",)
        exp.arm_metadata = {Arm.A: {"max_rows": 1000}, Arm.B: {"max_rows": 5000}}
        assert exp.confounds() == []
        assert exp.outcome()[0] is Outcome.OPTIMIZED

    def test_different_host_profiles_confound_the_pair(self):
        exp = PairedExperiment(hypothesis=hypothesis())
        exp.add(RunSample(Arm.A, 0, 100.0, correctness_clean=True, host_profile_digest="h1"))
        exp.add(RunSample(Arm.B, 1, 110.0, correctness_clean=True, host_profile_digest="h2"))
        exp.add(RunSample(Arm.B, 2, 110.0, correctness_clean=True, host_profile_digest="h2"))
        exp.add(RunSample(Arm.A, 3, 100.0, correctness_clean=True, host_profile_digest="h1"))
        assert exp.outcome()[0] is Outcome.CONFOUNDED

    def test_confounds_are_reported_before_the_effect(self):
        # A confounded experiment has no effect worth reporting.
        exp = experiment([100.0, 100.0], [130.0, 130.0])
        exp.arm_metadata = {Arm.A: {"smt": True}, Arm.B: {"smt": False}}
        outcome, reasons = exp.outcome()
        assert outcome is Outcome.CONFOUNDED
        assert not any("median effect" in r for r in reasons)


class TestSafetyRejections:
    def test_an_arm_that_weakens_durability_is_rejected(self):
        # Case E2E-47: acks=all swapped for acks=0. The speedup is real and the
        # claim is void.
        exp = experiment([100.0, 100.0], [200.0, 200.0])
        exp.arm_durability_intact = {Arm.A: True, Arm.B: False}
        outcome, reasons = exp.outcome()
        assert outcome is Outcome.REJECTED_UNSAFE
        assert any("durability contract" in r for r in reasons)

    def test_an_arm_that_failed_reconciliation_is_rejected(self):
        exp = experiment([100.0, 100.0], [200.0, 200.0], correctness=False)
        outcome, reasons = exp.outcome()
        assert outcome is Outcome.REJECTED_UNSAFE
        assert any("correctness is a precondition" in r for r in reasons)

    def test_safety_outranks_a_confound(self):
        exp = experiment([100.0, 100.0], [200.0, 200.0], correctness=False)
        exp.arm_metadata = {Arm.A: {"smt": True}, Arm.B: {"smt": False}}
        assert exp.outcome()[0] is Outcome.REJECTED_UNSAFE


class TestEvidenceGaps:
    def test_too_few_pairs_is_inconclusive(self):
        exp = experiment([100.0], [110.0], hypothesis=hypothesis(min_pairs=5))
        outcome, reasons = exp.outcome()
        assert outcome is Outcome.INCONCLUSIVE
        assert any("paired repeat" in r for r in reasons)

    def test_an_unpaired_order_is_inconclusive(self):
        exp = PairedExperiment(hypothesis=hypothesis())
        for index, (arm, value) in enumerate(
            [(Arm.A, 100.0), (Arm.A, 100.0), (Arm.A, 100.0), (Arm.B, 110.0), (Arm.B, 110.0), (Arm.B, 110.0)]
        ):
            exp.add(RunSample(arm, index, value, correctness_clean=True))
        outcome, reasons = exp.outcome()
        assert outcome is Outcome.INCONCLUSIVE
        assert any("ABBA" in r for r in reasons)

    def test_an_unverified_arm_cannot_be_an_optimization(self):
        exp = experiment([100.0, 100.0], [110.0, 110.0], correctness=None)
        outcome, reasons = exp.outcome()
        assert outcome is Outcome.INCONCLUSIVE
        assert any("no correctness result" in r for r in reasons)


class TestConfidenceInterval:
    def test_the_interval_is_over_run_level_effects(self):
        # Section 4.4: millions of requests inside one run are not independent.
        exp = experiment([100.0] * 5, [110.0] * 5, hypothesis=hypothesis(min_pairs=5))
        interval = exp.confidence_interval()
        assert interval.n_runs == 5
        assert interval.low == pytest.approx(0.1)

    def test_fewer_than_two_pairs_gives_no_interval(self):
        assert experiment([100.0], [110.0]).confidence_interval() is None

    def test_an_interval_including_zero_makes_the_win_inconclusive(self):
        # A large median gain that is indistinguishable from noise.
        exp = experiment(
            [100.0, 100.0, 100.0, 100.0], [180.0, 40.0, 180.0, 40.0],
            hypothesis=hypothesis(min_pairs=2),
        )
        outcome, reasons = exp.outcome()
        assert outcome is Outcome.INCONCLUSIVE
        assert any("includes zero" in r for r in reasons)

    def test_the_interval_is_deterministic(self):
        exp = experiment([100.0] * 4, [110.0, 112.0, 108.0, 111.0])
        assert exp.confidence_interval(seed=5).low == exp.confidence_interval(seed=5).low


class TestSerialisationAndReversal:
    def test_as_dict_carries_the_outcome_and_the_samples(self):
        payload = experiment([100.0, 100.0], [110.0, 110.0]).as_dict()
        assert payload["outcome"] == "OPTIMIZED"
        assert payload["order_is_paired"] is True
        assert len(payload["samples"]) == 4

    def test_an_undefined_effect_is_inconclusive_rather_than_an_exception(self):
        # A baseline arm that produced nothing is a real measurement; the ratio
        # against it is undefined, and the report must still render.
        exp = experiment([0.0, 0.0], [10.0, 10.0])
        outcome, reasons = exp.outcome()
        assert outcome is Outcome.INCONCLUSIVE
        assert any("undefined" in r for r in reasons)
        payload = exp.as_dict()
        assert payload["relative_effect"] is None
        assert payload["outcome"] == "INCONCLUSIVE"

    def test_a_reversal_that_restores_the_baseline_holds(self):
        # Case RB-09: an effect that does not reverse was probably drift.
        assert reversal_holds([100.0, 100.0], [110.0, 110.0], [101.0, 99.0])

    def test_a_reversal_that_does_not_come_back_fails(self):
        assert not reversal_holds([100.0, 100.0], [110.0, 110.0], [110.0, 110.0])

    def test_reversal_needs_all_three_phases(self):
        with pytest.raises(MetrologyError, match="all three phases"):
            reversal_holds([100.0], [110.0], [])

    def test_a_zero_baseline_makes_reversal_undefined(self):
        with pytest.raises(MetrologyError, match="reversal is undefined"):
            reversal_holds([0.0], [10.0], [0.0])
