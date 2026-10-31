"""Attempt registry: every scheduled request, including the ones that failed.

This is the structure that makes coordinated omission impossible to commit by
accident. Section 4.3: an open-loop driver must keep the timestamp of when a
request was *meant* to go out, and percentiles computed only over completed
requests are a conditional distribution that has to be labelled as one.

The registry therefore records one :class:`Attempt` per schedule slot and never
deletes any of them. From that single list come all four populations section 4.3
asks for:

* ``completion`` latency -- issue to response, the number a client-side timer
  sees;
* ``schedule_to_completion`` latency -- the corrected view, which includes the
  time a request spent waiting because the driver itself was behind;
* ``late issue`` distribution -- how far the driver drifted from its own
  schedule, which is what decides whether a target-rate claim is valid at all
  (case E2E-37);
* the censored set -- timeouts, which keep their place in the denominator and
  never acquire a latency (INV-16).

Correctness is tracked alongside, because INV-13 says a query that returned
quickly and wrongly is not a success. ``correct=None`` means nobody checked,
and that is different from ``correct=False``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterator

from .errors import MetrologyError
from .percentiles import (
    PercentileBasis,
    PercentileResult,
    percentile_completed_only,
    percentile_with_censoring,
)


class Outcome(str, Enum):
    """What became of one scheduled attempt.

    ``NOT_ISSUED`` is separate from ``ERROR``: the first is the driver failing
    to emit, which invalidates a capacity claim; the second is the system under
    test rejecting work, which is a result about the system.
    """

    COMPLETED = "COMPLETED"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"
    NOT_ISSUED = "NOT_ISSUED"
    PENDING = "PENDING"


@dataclass(frozen=True)
class Attempt:
    """One schedule slot and everything observed about it."""

    seq: int
    scheduled_ns: int
    issued_ns: int | None = None
    completed_ns: int | None = None
    outcome: Outcome = Outcome.PENDING
    correct: bool | None = None
    reason: str | None = None
    labels: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.outcome is Outcome.COMPLETED:
            if self.issued_ns is None or self.completed_ns is None:
                raise MetrologyError(
                    "a completed attempt needs both issue and completion timestamps",
                    seq=self.seq,
                )
            if self.completed_ns < self.issued_ns:
                raise MetrologyError(
                    "completion precedes issue", seq=self.seq,
                    issued_ns=self.issued_ns, completed_ns=self.completed_ns,
                )
        if self.outcome in (Outcome.TIMEOUT, Outcome.ERROR, Outcome.NOT_ISSUED):
            if self.completed_ns is not None:
                raise MetrologyError(
                    "a timeout, error or unissued attempt has no completion time; "
                    "assigning one turns a censored observation into a measurement",
                    seq=self.seq,
                    outcome=self.outcome.value,
                )
        if self.outcome is Outcome.NOT_ISSUED and self.issued_ns is not None:
            raise MetrologyError(
                "an unissued attempt cannot have an issue timestamp", seq=self.seq
            )

    @property
    def completion_latency_ns(self) -> int | None:
        if self.outcome is not Outcome.COMPLETED:
            return None
        return self.completed_ns - self.issued_ns

    @property
    def schedule_to_completion_ns(self) -> int | None:
        if self.outcome is not Outcome.COMPLETED:
            return None
        return self.completed_ns - self.scheduled_ns

    @property
    def issue_lag_ns(self) -> int | None:
        if self.issued_ns is None:
            return None
        return self.issued_ns - self.scheduled_ns

    @property
    def is_success(self) -> bool:
        """Completed *and* verified correct. ``correct=None`` is not a success."""
        return self.outcome is Outcome.COMPLETED and self.correct is True

    def as_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "scheduled_ns": self.scheduled_ns,
            "issued_ns": self.issued_ns,
            "completed_ns": self.completed_ns,
            "outcome": self.outcome.value,
            "correct": self.correct,
            "reason": self.reason,
            "completion_latency_ns": self.completion_latency_ns,
            "schedule_to_completion_ns": self.schedule_to_completion_ns,
            "issue_lag_ns": self.issue_lag_ns,
            "labels": dict(sorted(self.labels.items())),
        }


class AttemptRegistry:
    """Every attempt of one driver, with the derived populations.

    Memory is one small record per attempt. For a long query lane that is fine
    (thousands of queries). For a 200M-event lane the event driver aggregates
    instead and only samples attempts -- the registry is for request-shaped
    work, and :attr:`sampled` records whether what it holds is complete.
    """

    __slots__ = ("name", "deadline_ns", "sampled", "_attempts")

    def __init__(self, name: str, *, deadline_ns: int, sampled: bool = False) -> None:
        if deadline_ns <= 0:
            raise MetrologyError("deadline must be positive", deadline_ns=deadline_ns)
        self.name = name
        self.deadline_ns = deadline_ns
        self.sampled = sampled
        self._attempts: list[Attempt] = []

    # -- recording --------------------------------------------------------
    def add(self, attempt: Attempt) -> None:
        self._attempts.append(attempt)

    def record(
        self,
        seq: int,
        scheduled_ns: int,
        *,
        issued_ns: int | None = None,
        completed_ns: int | None = None,
        outcome: Outcome = Outcome.PENDING,
        correct: bool | None = None,
        reason: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> Attempt:
        attempt = Attempt(
            seq=seq,
            scheduled_ns=scheduled_ns,
            issued_ns=issued_ns,
            completed_ns=completed_ns,
            outcome=outcome,
            correct=correct,
            reason=reason,
            labels=labels or {},
        )
        self.add(attempt)
        return attempt

    def __len__(self) -> int:
        return len(self._attempts)

    def __iter__(self) -> Iterator[Attempt]:
        return iter(self._attempts)

    # -- populations ------------------------------------------------------
    @property
    def scheduled(self) -> int:
        return len(self._attempts)

    def count(self, outcome: Outcome) -> int:
        return sum(1 for a in self._attempts if a.outcome is outcome)

    @property
    def issued(self) -> int:
        return sum(1 for a in self._attempts if a.issued_ns is not None)

    @property
    def completed(self) -> int:
        return self.count(Outcome.COMPLETED)

    @property
    def timeouts(self) -> int:
        return self.count(Outcome.TIMEOUT)

    @property
    def errors(self) -> int:
        return self.count(Outcome.ERROR)

    @property
    def not_issued(self) -> int:
        return self.count(Outcome.NOT_ISSUED)

    @property
    def pending(self) -> int:
        return self.count(Outcome.PENDING)

    @property
    def correct_completed(self) -> int:
        return sum(1 for a in self._attempts if a.is_success)

    @property
    def incorrect_completed(self) -> int:
        return sum(
            1 for a in self._attempts
            if a.outcome is Outcome.COMPLETED and a.correct is False
        )

    @property
    def unchecked_completed(self) -> int:
        """Completed but never compared against the result oracle.

        Not folded into ``correct`` or ``incorrect``: INV-13 makes an unchecked
        result an evidence gap, and a gap is not a pass.
        """
        return sum(
            1 for a in self._attempts
            if a.outcome is Outcome.COMPLETED and a.correct is None
        )

    # -- rates ------------------------------------------------------------
    @property
    def error_rate(self) -> float | None:
        """``(failures + timeouts) / scheduled`` per section 4.2.

        The denominator is the schedule, not the completions. That is the whole
        point of case E2E-41.
        """
        if self.scheduled == 0:
            return None
        return (self.errors + self.timeouts) / self.scheduled

    @property
    def deadline_miss_fraction(self) -> float | None:
        if self.scheduled == 0:
            return None
        return self.timeouts / self.scheduled

    @property
    def issue_shortfall_fraction(self) -> float | None:
        """Fraction of the schedule the driver never emitted (case E2E-37)."""
        if self.scheduled == 0:
            return None
        return self.not_issued / self.scheduled

    # -- latency views ----------------------------------------------------
    def completion_latencies_ms(self) -> list[float]:
        return [
            a.completion_latency_ns / 1e6
            for a in self._attempts
            if a.completion_latency_ns is not None
        ]

    def schedule_to_completion_ms(self) -> list[float]:
        return [
            a.schedule_to_completion_ns / 1e6
            for a in self._attempts
            if a.schedule_to_completion_ns is not None
        ]

    def correct_completion_latencies_ms(self) -> list[float]:
        """Latencies of attempts that completed **and** returned correct results."""
        return [
            a.completion_latency_ns / 1e6 for a in self._attempts if a.is_success
        ]

    def issue_lags_ms(self) -> list[float]:
        return [
            a.issue_lag_ns / 1e6 for a in self._attempts if a.issue_lag_ns is not None
        ]

    # -- percentiles ------------------------------------------------------
    def percentile(
        self,
        q: float,
        *,
        basis: PercentileBasis = PercentileBasis.ATTEMPTED_WITH_CENSORING,
        correct_only: bool = True,
    ) -> PercentileResult:
        """Quantile over the chosen population.

        ``correct_only`` defaults to True because INV-13 defines query success
        as a correct full response; a p95 over responses that were merely fast
        is the case E2E-38 failure.
        """
        samples = (
            self.correct_completion_latencies_ms()
            if correct_only
            else self.completion_latencies_ms()
        )
        # Everything that is not a usable observation stays in the denominator.
        # Under ``correct_only`` a wrong or unchecked response is not a latency
        # sample either, so it is censored rather than dropped (INV-13, INV-16).
        censored = self.timeouts + self.errors
        if correct_only:
            censored += self.incorrect_completed + self.unchecked_completed

        if basis is PercentileBasis.COMPLETED_ONLY:
            return percentile_completed_only(samples, q, n_censored=censored)
        if basis is PercentileBasis.SCHEDULE_TO_COMPLETION:
            corrected = self.schedule_to_completion_ms()
            result = percentile_with_censoring(
                corrected, q, n_censored=censored, deadline_ms=self.deadline_ns / 1e6
            )
            return PercentileResult(
                q=result.q,
                value_ms=result.value_ms,
                basis=PercentileBasis.SCHEDULE_TO_COMPLETION,
                n_considered=result.n_considered,
                n_completed=result.n_completed,
                n_censored=result.n_censored,
                is_lower_bound=result.is_lower_bound,
                lower_bound_ms=result.lower_bound_ms,
                reason=result.reason,
            )
        return percentile_with_censoring(
            samples, q, n_censored=censored, deadline_ms=self.deadline_ns / 1e6
        )

    # -- output -----------------------------------------------------------
    def summary(self) -> dict[str, Any]:
        return {
            "registry": self.name,
            "sampled": self.sampled,
            "deadline_ms": self.deadline_ns / 1e6,
            "scheduled": self.scheduled,
            "issued": self.issued,
            "completed": self.completed,
            "correct_completed": self.correct_completed,
            "incorrect_completed": self.incorrect_completed,
            "unchecked_completed": self.unchecked_completed,
            "timeouts": self.timeouts,
            "errors": self.errors,
            "not_issued": self.not_issued,
            "pending": self.pending,
            "error_rate": self.error_rate,
            "deadline_miss_fraction": self.deadline_miss_fraction,
            "issue_shortfall_fraction": self.issue_shortfall_fraction,
            "p50": self.percentile(0.50).as_dict(),
            "p95": self.percentile(0.95).as_dict(),
            "p99": self.percentile(0.99).as_dict(),
        }
