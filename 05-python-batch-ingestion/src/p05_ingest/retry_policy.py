"""Bounded retry with jittered exponential backoff, and a circuit breaker.

Spec section 5.2. Design points that matter more than the arithmetic:

  * **bounded, always.** There is an attempt cap *and* an absolute deadline.
    Unbounded retry is not a safety feature here: the ClickHouse insert
    de-duplication window is finite, so a retry that succeeds an hour later can
    produce a physical duplicate that the window would have absorbed.
  * **jitter is seeded.** A deterministic ``Random`` instance per policy keeps
    a chaos test reproducible while still de-correlating retries across
    workers.
  * **the breaker counts consecutive failures across batches.** A single batch
    exhausting its own budget stops its partition; a sink that is down should
    stop the whole process from hammering it.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from .config import RetryConfig
from .model import SinkOutcome


@dataclass(frozen=True, slots=True)
class RetryDecision:
    should_retry: bool
    delay_s: float = 0.0
    reason: str = ""


class RetryPolicy:
    """Decides whether one batch gets another attempt, and after how long."""

    def __init__(self, cfg: RetryConfig) -> None:
        self._cfg = cfg
        self._rng = random.Random(cfg.jitter_seed)

    @property
    def max_attempts(self) -> int:
        return self._cfg.max_attempts

    def decide(
        self,
        *,
        outcome: SinkOutcome,
        attempts_made: int,
        elapsed_s: float,
    ) -> RetryDecision:
        """Should the identical batch be sent again?

        ``attempts_made`` counts attempts already completed, so the first call
        after one failed attempt passes 1.
        """
        if outcome is SinkOutcome.ACKED:
            return RetryDecision(False, reason="acked")
        if outcome is SinkOutcome.PERMANENT:
            # Spec 5.2: no blind retry. The request cannot succeed as formed,
            # so retrying only delays the operator finding out.
            return RetryDecision(False, reason="permanent error, retry would not help")
        if attempts_made >= self._cfg.max_attempts:
            return RetryDecision(
                False,
                reason=(
                    f"retry budget exhausted after {attempts_made} attempts "
                    f"(max_attempts={self._cfg.max_attempts})"
                ),
            )
        if elapsed_s >= self._cfg.overall_deadline_s:
            return RetryDecision(
                False,
                reason=(
                    f"overall deadline {self._cfg.overall_deadline_s}s exceeded "
                    f"({elapsed_s:.1f}s elapsed)"
                ),
            )

        delay = self._backoff_s(attempts_made)
        remaining = self._cfg.overall_deadline_s - elapsed_s
        if delay >= remaining:
            # Sleeping past the deadline would burn the whole budget on a wait
            # and then fail anyway; stop now and report honestly.
            return RetryDecision(
                False,
                reason=(
                    f"next backoff {delay:.2f}s would exceed the remaining "
                    f"{remaining:.2f}s of the retry deadline"
                ),
            )
        return RetryDecision(True, delay_s=delay, reason=f"{outcome} is retryable")

    def _backoff_s(self, attempts_made: int) -> float:
        """Exponential growth, capped, with full jitter in the upper half.

        Jitter spans [0.5, 1.0] of the computed delay rather than [0, 1]: a
        near-zero sleep under contention is a busy loop, which the spec
        forbids.
        """
        exponent = max(0, attempts_made - 1)
        raw_ms = self._cfg.initial_backoff_ms * (2**exponent)
        capped_ms = min(raw_ms, self._cfg.max_backoff_ms)
        jitter = 0.5 + (self._rng.random() * 0.5)
        return (capped_ms / 1000.0) * jitter


@dataclass(slots=True)
class CircuitBreaker:
    """Trips after N consecutive sink failures; closes on the next success.

    Deliberately simple: no half-open probing state. When it is open the owner
    pauses every partition, so the next attempt happens only after the operator
    or the cooldown lets it, and a half-open probe would add a failure mode
    without adding information.
    """

    threshold: int = 5
    cooldown_s: float = 30.0
    _consecutive_failures: int = 0
    _opened_at: float | None = field(default=None)

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._opened_at = None

    def record_failure(self, *, now: float) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.threshold and self._opened_at is None:
            self._opened_at = now

    def is_open(self, *, now: float) -> bool:
        if self._opened_at is None:
            return False
        if now - self._opened_at >= self.cooldown_s:
            # Cooldown elapsed: allow traffic again but keep the failure count,
            # so a still-broken sink re-opens on the very next failure.
            self._opened_at = None
            self._consecutive_failures = self.threshold - 1
            return False
        return True

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures
