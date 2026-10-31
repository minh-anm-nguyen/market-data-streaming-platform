"""Attempt registry: coordinated omission and the denominator (section 4.3, INV-13, INV-16).

The registry is the structure that makes three documented mistakes impossible:
dropping timeouts from the denominator (E2E-41), giving a timeout a latency of
zero (section 4.3), and counting a fast wrong answer as a success (E2E-38).
"""

from __future__ import annotations

import pytest

from p08_bench.errors import MetrologyError
from p08_bench.goldens import FIX002_LATENCIES_MS
from p08_bench.percentiles import PercentileBasis
from p08_bench.samples import Attempt, AttemptRegistry, Outcome

MS = 1_000_000


def completed(seq: int, *, scheduled_ns: int, issued_ns: int, latency_ms: float, correct: bool | None = True) -> Attempt:
    return Attempt(
        seq=seq,
        scheduled_ns=scheduled_ns,
        issued_ns=issued_ns,
        completed_ns=issued_ns + int(latency_ms * MS),
        outcome=Outcome.COMPLETED,
        correct=correct,
    )


def registry_with(latencies_ms, *, deadline_ms: float = 2000.0, correct: bool | None = True) -> AttemptRegistry:
    registry = AttemptRegistry("q02", deadline_ns=int(deadline_ms * MS))
    for i, latency in enumerate(latencies_ms):
        registry.add(completed(i, scheduled_ns=i * MS, issued_ns=i * MS, latency_ms=latency, correct=correct))
    return registry


class TestAttemptInvariants:
    def test_a_completed_attempt_needs_both_timestamps(self):
        with pytest.raises(MetrologyError, match="issue and completion"):
            Attempt(seq=1, scheduled_ns=0, issued_ns=0, outcome=Outcome.COMPLETED)

    def test_completion_cannot_precede_issue(self):
        with pytest.raises(MetrologyError, match="precedes issue"):
            Attempt(seq=1, scheduled_ns=0, issued_ns=100, completed_ns=50, outcome=Outcome.COMPLETED)

    def test_a_timeout_may_not_be_given_a_completion_time(self):
        # Section 4.3: assigning the deadline as if it were observed turns a
        # censored observation into a measurement.
        with pytest.raises(MetrologyError, match="censored observation"):
            Attempt(seq=1, scheduled_ns=0, issued_ns=0, completed_ns=99, outcome=Outcome.TIMEOUT)

    def test_an_error_may_not_be_given_a_completion_time(self):
        with pytest.raises(MetrologyError):
            Attempt(seq=1, scheduled_ns=0, issued_ns=0, completed_ns=99, outcome=Outcome.ERROR)

    def test_an_unissued_attempt_has_no_issue_timestamp(self):
        with pytest.raises(MetrologyError, match="cannot have an issue timestamp"):
            Attempt(seq=1, scheduled_ns=0, issued_ns=5, outcome=Outcome.NOT_ISSUED)

    def test_a_timeout_has_no_latency(self):
        attempt = Attempt(seq=1, scheduled_ns=0, issued_ns=0, outcome=Outcome.TIMEOUT)
        assert attempt.completion_latency_ns is None
        assert attempt.schedule_to_completion_ns is None

    def test_issue_lag_is_measured_from_the_schedule(self):
        attempt = Attempt(seq=1, scheduled_ns=1_000, issued_ns=3_000, outcome=Outcome.PENDING)
        assert attempt.issue_lag_ns == 2_000

    def test_an_unissued_attempt_has_no_issue_lag(self):
        assert Attempt(seq=1, scheduled_ns=0, outcome=Outcome.NOT_ISSUED).issue_lag_ns is None


class TestSuccessRequiresCorrectness:
    def test_a_completed_and_correct_attempt_is_a_success(self):
        assert completed(1, scheduled_ns=0, issued_ns=0, latency_ms=1, correct=True).is_success

    def test_a_fast_wrong_answer_is_not_a_success(self):
        # Case E2E-38: 1 ms latency with empty results fails regardless.
        assert not completed(1, scheduled_ns=0, issued_ns=0, latency_ms=1, correct=False).is_success

    def test_an_unchecked_result_is_not_a_success_either(self):
        # INV-13 makes an unchecked result an evidence gap, and a gap is not a pass.
        assert not completed(1, scheduled_ns=0, issued_ns=0, latency_ms=1, correct=None).is_success

    def test_the_registry_counts_the_three_states_separately(self):
        registry = AttemptRegistry("q", deadline_ns=1000 * MS)
        registry.add(completed(1, scheduled_ns=0, issued_ns=0, latency_ms=1, correct=True))
        registry.add(completed(2, scheduled_ns=0, issued_ns=0, latency_ms=1, correct=False))
        registry.add(completed(3, scheduled_ns=0, issued_ns=0, latency_ms=1, correct=None))
        assert (registry.correct_completed, registry.incorrect_completed, registry.unchecked_completed) == (1, 1, 1)
        assert registry.completed == 3


class TestDenominators:
    def _mixed(self) -> AttemptRegistry:
        registry = AttemptRegistry("q02", deadline_ns=2000 * MS)
        for i in range(10):
            registry.add(completed(i, scheduled_ns=i * MS, issued_ns=i * MS, latency_ms=100))
        for i in range(10, 13):
            registry.record(i, scheduled_ns=i * MS, issued_ns=i * MS, outcome=Outcome.TIMEOUT)
        for i in range(13, 15):
            registry.record(i, scheduled_ns=i * MS, issued_ns=i * MS, outcome=Outcome.ERROR)
        registry.record(15, scheduled_ns=15 * MS, outcome=Outcome.NOT_ISSUED)
        return registry

    def test_scheduled_is_the_total_number_of_slots(self):
        assert self._mixed().scheduled == 16

    def test_error_rate_divides_by_scheduled_not_by_completions(self):
        # Section 4.2: error_rate = (failures + timeouts) / scheduled_attempts.
        assert self._mixed().error_rate == pytest.approx(5 / 16)

    def test_deadline_miss_fraction_uses_the_same_denominator(self):
        assert self._mixed().deadline_miss_fraction == pytest.approx(3 / 16)

    def test_issue_shortfall_is_tracked_separately_from_errors(self):
        # A driver that failed to emit invalidates a capacity claim; a system
        # that rejected work is a result about the system.
        assert self._mixed().issue_shortfall_fraction == pytest.approx(1 / 16)

    def test_issued_counts_only_what_went_out(self):
        assert self._mixed().issued == 15

    def test_rates_are_none_rather_than_zero_with_no_attempts(self):
        empty = AttemptRegistry("q", deadline_ns=MS)
        assert empty.error_rate is None
        assert empty.deadline_miss_fraction is None
        assert empty.issue_shortfall_fraction is None

    def test_a_non_positive_deadline_is_refused(self):
        with pytest.raises(MetrologyError, match="deadline"):
            AttemptRegistry("q", deadline_ns=0)


class TestPercentileViews:
    def test_fix002_conditional_p95_over_completed_only(self):
        registry = registry_with(FIX002_LATENCIES_MS)
        result = registry.percentile(0.95, basis=PercentileBasis.COMPLETED_ONLY)
        assert result.value_ms == pytest.approx(1000.0)
        assert result.n_completed == 10

    def test_adding_ten_timeouts_pushes_the_rank_into_the_censored_tail(self):
        registry = registry_with(FIX002_LATENCIES_MS)
        for i in range(10, 20):
            registry.record(i, scheduled_ns=i * MS, issued_ns=i * MS, outcome=Outcome.TIMEOUT)
        assert registry.scheduled == 20
        assert registry.timeouts == 10
        # Default basis keeps the timeouts in the population.
        censored = registry.percentile(0.95)
        assert censored.is_lower_bound
        assert censored.value_ms is None
        # The conditional view still reports 1000 ms, labelled as conditional.
        conditional = registry.percentile(0.95, basis=PercentileBasis.COMPLETED_ONLY)
        assert conditional.value_ms == pytest.approx(1000.0)
        assert conditional.n_censored == 10

    def test_correct_only_is_the_default_population(self):
        # Ten fast wrong answers must not produce a flattering p95.
        registry = registry_with([1.0] * 10, correct=False)
        result = registry.percentile(0.95)
        assert result.value_ms is None
        assert result.n_completed == 0
        assert result.n_censored == 10

    def test_correct_only_can_be_switched_off_for_diagnosis(self):
        registry = registry_with([1.0] * 10, correct=False)
        result = registry.percentile(0.95, correct_only=False)
        assert result.value_ms == pytest.approx(1.0)

    def test_schedule_to_completion_includes_driver_lag(self):
        # The corrected view of section 4.3: a request issued 500 ms late and
        # served in 100 ms took 600 ms from the schedule's point of view.
        registry = AttemptRegistry("q", deadline_ns=5000 * MS)
        registry.add(
            Attempt(
                seq=1, scheduled_ns=0, issued_ns=500 * MS,
                completed_ns=600 * MS, outcome=Outcome.COMPLETED, correct=True,
            )
        )
        completion = registry.percentile(0.95, basis=PercentileBasis.COMPLETED_ONLY)
        corrected = registry.percentile(0.95, basis=PercentileBasis.SCHEDULE_TO_COMPLETION)
        assert completion.value_ms == pytest.approx(100.0)
        assert corrected.value_ms == pytest.approx(600.0)
        assert corrected.basis is PercentileBasis.SCHEDULE_TO_COMPLETION

    def test_issue_lag_distribution_is_available(self):
        registry = AttemptRegistry("q", deadline_ns=MS)
        registry.record(1, scheduled_ns=0, issued_ns=5 * MS, outcome=Outcome.PENDING)
        assert registry.issue_lags_ms() == [pytest.approx(5.0)]


class TestSummary:
    def test_summary_carries_every_population_and_the_three_percentiles(self):
        registry = registry_with(FIX002_LATENCIES_MS)
        registry.record(99, scheduled_ns=99 * MS, issued_ns=99 * MS, outcome=Outcome.TIMEOUT)
        summary = registry.summary()
        for key in (
            "scheduled", "issued", "completed", "correct_completed",
            "incorrect_completed", "unchecked_completed", "timeouts", "errors",
            "not_issued", "error_rate", "deadline_miss_fraction", "p50", "p95", "p99",
        ):
            assert key in summary
        assert summary["scheduled"] == 11
        assert summary["deadline_ms"] == pytest.approx(2000.0)

    def test_sampled_registries_say_so(self):
        # A 200M-event lane cannot keep one record per event, and a report must
        # not read a sampled registry as a complete census.
        registry = AttemptRegistry("events", deadline_ns=MS, sampled=True)
        assert registry.summary()["sampled"] is True

    def test_iteration_and_len(self):
        registry = registry_with([1.0, 2.0])
        assert len(registry) == 2
        assert [a.seq for a in registry] == [0, 1]
