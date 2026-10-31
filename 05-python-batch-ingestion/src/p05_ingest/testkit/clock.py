"""A manual clock, so batch age and retry backoff are testable.

Spec 9.3 forbids real ``time.sleep`` in unit tests: a timer-based flush test
that sleeps is slow and flaky, and a backoff test that sleeps for real cannot
explore a 30-second retry deadline. ``FakeClock`` advances only when a test
says so, and ``sleep`` advances it instead of blocking, so the engine and the
retry policy see a consistent monotonic timeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class FakeClock:
    """Monotonic clock under test control."""

    now: float = 1000.0
    #: Every sleep duration requested, in order. Tests assert on backoff shape.
    sleeps: list[float] = field(default_factory=list)

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> float:
        if seconds < 0:
            raise ValueError("a monotonic clock must not go backwards")
        self.now += seconds
        return self.now

    def advance_ms(self, milliseconds: float) -> float:
        return self.advance(milliseconds / 1000.0)

    def sleep(self, seconds: float) -> None:
        """Stand-in for ``time.sleep``: advances instead of blocking."""
        self.sleeps.append(seconds)
        self.advance(seconds)

    @property
    def total_slept(self) -> float:
        return sum(self.sleeps)
