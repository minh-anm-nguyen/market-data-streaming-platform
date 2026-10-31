"""Retry classification, backoff shape, bounds and the circuit breaker."""

from __future__ import annotations

from dataclasses import replace

import pytest

from p05_ingest.config import RetryConfig
from p05_ingest.model import SinkOutcome
from p05_ingest.retry_policy import CircuitBreaker, RetryPolicy


@pytest.fixture
def cfg() -> RetryConfig:
    return RetryConfig(
        max_attempts=5,
        initial_backoff_ms=100,
        max_backoff_ms=3000,
        overall_deadline_s=30.0,
        jitter_seed=1337,
    )


@pytest.fixture
def policy(cfg) -> RetryPolicy:
    return RetryPolicy(cfg)


# ------------------------------------------------------------ classification


def test_ack_is_never_retried(policy):
    decision = policy.decide(outcome=SinkOutcome.ACKED, attempts_made=1, elapsed_s=0.1)
    assert not decision.should_retry


def test_permanent_error_is_never_retried(policy):
    """Spec 5.2: blind retry only delays the operator finding out."""
    decision = policy.decide(
        outcome=SinkOutcome.PERMANENT, attempts_made=1, elapsed_s=0.1
    )
    assert not decision.should_retry
    assert "permanent" in decision.reason


@pytest.mark.parametrize(
    "outcome", [SinkOutcome.AMBIGUOUS, SinkOutcome.TRANSIENT]
)
def test_ambiguous_and_transient_are_retried(policy, outcome):
    decision = policy.decide(outcome=outcome, attempts_made=1, elapsed_s=0.1)
    assert decision.should_retry
    assert decision.delay_s > 0


# ------------------------------------------------------------------- bounds


def test_attempt_cap_stops_retrying(policy, cfg):
    decision = policy.decide(
        outcome=SinkOutcome.TRANSIENT, attempts_made=cfg.max_attempts, elapsed_s=0.1
    )
    assert not decision.should_retry
    assert "budget exhausted" in decision.reason


def test_overall_deadline_stops_retrying(policy, cfg):
    decision = policy.decide(
        outcome=SinkOutcome.TRANSIENT, attempts_made=1, elapsed_s=cfg.overall_deadline_s
    )
    assert not decision.should_retry
    assert "deadline" in decision.reason


def test_a_backoff_that_would_overrun_the_deadline_stops_now(cfg):
    """Sleeping past the deadline then failing wastes the whole budget."""
    policy = RetryPolicy(replace(cfg, initial_backoff_ms=5000, max_backoff_ms=5000))
    decision = policy.decide(
        outcome=SinkOutcome.TRANSIENT, attempts_made=1, elapsed_s=28.0
    )
    assert not decision.should_retry
    assert "remaining" in decision.reason


def test_unbounded_retry_is_not_configurable():
    """Spec 5.2: retry must stay inside the ClickHouse de-duplication window."""
    with pytest.raises(Exception, match="overall_deadline_s"):
        RetryConfig(overall_deadline_s=0)
    with pytest.raises(Exception, match="max_attempts"):
        RetryConfig(max_attempts=0)


# ------------------------------------------------------------------ backoff


def test_backoff_grows_exponentially_and_is_capped(cfg):
    policy = RetryPolicy(cfg)
    delays = [
        policy.decide(
            outcome=SinkOutcome.TRANSIENT, attempts_made=n, elapsed_s=0.0
        ).delay_s
        for n in range(1, 5)
    ]
    # Jitter spans [0.5, 1.0] of the nominal delay, so compare against bounds
    # rather than exact values.
    nominal = [0.1, 0.2, 0.4, 0.8]
    for delay, base in zip(delays, nominal, strict=True):
        assert base * 0.5 <= delay <= base, (delay, base)


def test_backoff_never_exceeds_the_cap(cfg):
    policy = RetryPolicy(replace(cfg, max_attempts=50, overall_deadline_s=1e6))
    for n in range(1, 30):
        decision = policy.decide(
            outcome=SinkOutcome.TRANSIENT, attempts_made=n, elapsed_s=0.0
        )
        if decision.should_retry:
            assert decision.delay_s <= cfg.max_backoff_ms / 1000.0


def test_backoff_is_never_near_zero(cfg):
    """A near-zero sleep under contention is a busy loop, which is forbidden."""
    policy = RetryPolicy(cfg)
    for n in range(1, 5):
        delay = policy.decide(
            outcome=SinkOutcome.TRANSIENT, attempts_made=n, elapsed_s=0.0
        ).delay_s
        assert delay >= (cfg.initial_backoff_ms / 1000.0) * 0.5


def test_jitter_is_seeded_and_therefore_reproducible(cfg):
    def delays():
        policy = RetryPolicy(cfg)
        return [
            policy.decide(
                outcome=SinkOutcome.TRANSIENT, attempts_made=n, elapsed_s=0.0
            ).delay_s
            for n in range(1, 5)
        ]

    assert delays() == delays()


def test_different_seeds_decorrelate_retries(cfg):
    a = RetryPolicy(replace(cfg, jitter_seed=1))
    b = RetryPolicy(replace(cfg, jitter_seed=2))
    da = [
        a.decide(outcome=SinkOutcome.TRANSIENT, attempts_made=n, elapsed_s=0.0).delay_s
        for n in range(1, 6)
    ]
    db = [
        b.decide(outcome=SinkOutcome.TRANSIENT, attempts_made=n, elapsed_s=0.0).delay_s
        for n in range(1, 6)
    ]
    assert da != db


# ---------------------------------------------------------- circuit breaker


def test_breaker_opens_after_consecutive_failures():
    breaker = CircuitBreaker(threshold=3, cooldown_s=10.0)
    assert not breaker.is_open(now=0.0)
    for i in range(2):
        breaker.record_failure(now=float(i))
        assert not breaker.is_open(now=float(i))
    breaker.record_failure(now=2.0)
    assert breaker.is_open(now=2.0)


def test_success_closes_the_breaker():
    breaker = CircuitBreaker(threshold=2, cooldown_s=10.0)
    breaker.record_failure(now=0.0)
    breaker.record_success()
    assert breaker.consecutive_failures == 0
    breaker.record_failure(now=1.0)
    assert not breaker.is_open(now=1.0), "the streak was broken"


def test_cooldown_lets_traffic_through_but_stays_sensitive():
    """After the cooldown one failure re-opens it, instead of five more."""
    breaker = CircuitBreaker(threshold=3, cooldown_s=10.0)
    for i in range(3):
        breaker.record_failure(now=float(i))
    assert breaker.is_open(now=5.0)
    assert not breaker.is_open(now=15.0), "cooldown elapsed"

    breaker.record_failure(now=16.0)
    assert breaker.is_open(now=16.0), "a still-broken sink re-opens at once"
