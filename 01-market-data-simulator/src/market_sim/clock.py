"""Two clocks, kept strictly apart (section 4 of SYSTEM_DESIGN).

======================  ==================================  =======================
kind                    source                              used for
======================  ==================================  =======================
logical event time      start_time_ns + slot * 1e9 // rate  the event_time_ns field
monotonic elapsed       time.monotonic_ns()                 pacing, timeouts, lag
wall clock              time.time_ns()                      manifest metadata only
======================  ==================================  =======================

Why it matters: pacing on the wall clock would let an NTP step, a DST change or a
laptop suspend reorder events or move sequence numbers backwards. Pacing on the
monotonic clock cannot. And because event_time_ns in the payload is pure integer
arithmetic over the slot index, the recorded bytes do not depend on how fast the
machine actually ran -- which is what makes NFR-01 (determinism) achievable.

The scheduler's overload policy is slowdown-no-silent-drop: if the machine cannot
keep up, throughput falls and the lag is reported. A slot is never dropped to
flatter the clock.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Final

NS_PER_SEC: Final = 1_000_000_000

MonotonicSource = Callable[[], int]


class LogicalClock:
    """Maps a logical slot index to the event timestamp written into a record.

    Pure integer arithmetic, so it is exact and reproducible. Two slots can land
    on the same nanosecond as the rate approaches 1e9; ordering then comes from
    ``sequence``, not from the timestamp (case T11).
    """

    __slots__ = ("_start_time_ns", "_rate_per_sec")

    def __init__(self, start_time_ns: int, rate_per_sec: int) -> None:
        if rate_per_sec <= 0:
            raise ValueError(f"rate_per_sec must be > 0, got {rate_per_sec}")
        if start_time_ns < 0:
            raise ValueError(f"start_time_ns must be >= 0, got {start_time_ns}")
        self._start_time_ns = start_time_ns
        self._rate_per_sec = rate_per_sec

    @property
    def start_time_ns(self) -> int:
        return self._start_time_ns

    @property
    def rate_per_sec(self) -> int:
        return self._rate_per_sec

    def event_time_ns(self, slot_index: int) -> int:
        """Logical timestamp of global slot ``slot_index`` (0-based)."""
        if slot_index < 0:
            raise ValueError(f"slot_index must be >= 0, got {slot_index}")
        return self._start_time_ns + (slot_index * NS_PER_SEC) // self._rate_per_sec

    def slots_in(self, duration_s: float) -> int:
        return int(duration_s * self._rate_per_sec)


@dataclass(frozen=True, slots=True)
class BatchPlan:
    """What the scheduler wants done in this iteration."""

    slots: int
    """Slots to produce now. May be 0 when we are ahead of schedule."""

    sleep_s: float
    """How long to sleep before the next iteration; 0 means go again now."""

    lag_ns: int
    """How far production is behind schedule, in nanoseconds of logical time."""

    capped_by_burst: bool
    """True when max_burst_slots clipped the batch, i.e. we are catching up."""


class RateScheduler:
    """Batched, monotonic rate control.

    Instead of sleeping per event (hopeless above a few thousand per second) the
    scheduler computes how many slots *should* exist by now and produces the
    difference, capped at ``max_burst_slots`` so a catch-up cannot become an
    unbounded burst that floods the queues (case T05)::

        scheduled_slots   = elapsed_ns * rate // 1e9
        slots_to_produce  = max(0, scheduled_slots - produced_slots)
        slots_this_round  = min(slots_to_produce, max_burst_slots)

    ``lag_ns`` is the backlog expressed as logical time: how long it would take to
    emit the outstanding slots at the target rate. It has to be reported rather
    than hidden -- reaching the target slot count is not the same as having
    reached the target *rate* (NFR-04).
    """

    __slots__ = ("_rate", "_batch_ns", "_max_burst", "_monotonic", "_t0", "_produced")

    def __init__(
        self,
        rate_per_sec: int,
        *,
        batch_ms: int,
        max_burst_slots: int,
        monotonic: MonotonicSource = time.monotonic_ns,
    ) -> None:
        if rate_per_sec <= 0:
            raise ValueError(f"rate_per_sec must be > 0, got {rate_per_sec}")
        if batch_ms <= 0:
            raise ValueError(f"batch_ms must be > 0, got {batch_ms}")
        if max_burst_slots <= 0:
            raise ValueError(f"max_burst_slots must be > 0, got {max_burst_slots}")
        self._rate = rate_per_sec
        self._batch_ns = batch_ms * 1_000_000
        self._max_burst = max_burst_slots
        self._monotonic = monotonic
        self._t0: int | None = None
        self._produced = 0

    def start(self) -> None:
        self._t0 = self._monotonic()
        self._produced = 0

    @property
    def started(self) -> bool:
        return self._t0 is not None

    @property
    def produced_slots(self) -> int:
        return self._produced

    def elapsed_ns(self) -> int:
        if self._t0 is None:
            raise RuntimeError("RateScheduler.start() was not called")
        return max(0, self._monotonic() - self._t0)

    def scheduled_slots(self) -> int:
        """How many slots the target rate says should exist by now."""
        return (self.elapsed_ns() * self._rate) // NS_PER_SEC

    def note_produced(self, slots: int) -> None:
        if slots < 0:
            raise ValueError(f"slots must be >= 0, got {slots}")
        self._produced += slots

    def plan(self, *, remaining: int | None = None) -> BatchPlan:
        """Decide the next batch.

        ``remaining`` caps the batch for a count-bounded run so the last
        iteration does not overshoot the requested slot total.
        """
        behind = max(0, self.scheduled_slots() - self._produced)
        lag_ns = (behind * NS_PER_SEC) // self._rate

        slots = min(behind, self._max_burst)
        capped = behind > self._max_burst
        if remaining is not None:
            slots = min(slots, max(0, remaining))

        if slots > 0:
            # More work is already due; do not sleep between catch-up batches.
            sleep_s = 0.0
        else:
            # Sleep until the next slot is due, but at most one batch window, so a
            # very low rate neither busy-loops nor delays a stop request (T10).
            next_slot_due_ns = ((self._produced + 1) * NS_PER_SEC) // self._rate
            wait_ns = max(0, next_slot_due_ns - self.elapsed_ns())
            sleep_s = min(wait_ns, self._batch_ns) / NS_PER_SEC
            if sleep_s <= 0:
                # Due now but nothing to do (e.g. remaining == 0): yield for one
                # batch window instead of spinning the event loop.
                sleep_s = self._batch_ns / NS_PER_SEC
        return BatchPlan(
            slots=slots, sleep_s=sleep_s, lag_ns=lag_ns, capped_by_burst=capped
        )

    def effective_rate(self) -> float:
        """Slots actually produced per second of monotonic time so far."""
        elapsed = self.elapsed_ns()
        if elapsed <= 0:
            return 0.0
        return self._produced * NS_PER_SEC / elapsed


class RateWindow:
    """Rolling rate over a fixed monotonic window.

    Section 9.3 asks for both a 1-second and a 60-second view: the 60-second
    average hides the jitter the 1-second buckets expose, and quoting only the
    smooth number would be the dishonest option.
    """

    __slots__ = ("_window_ns", "_monotonic", "_buckets", "_total")

    def __init__(
        self, window_s: float, *, monotonic: MonotonicSource = time.monotonic_ns
    ) -> None:
        if window_s <= 0:
            raise ValueError(f"window_s must be > 0, got {window_s}")
        self._window_ns = int(window_s * NS_PER_SEC)
        self._monotonic = monotonic
        self._buckets: list[tuple[int, int]] = []  # (monotonic_ns, count)
        self._total = 0

    def add(self, count: int, now_ns: int | None = None) -> None:
        if count <= 0:
            return
        now = self._monotonic() if now_ns is None else now_ns
        self._buckets.append((now, count))
        self._total += count
        self._evict(now)

    def _evict(self, now: int) -> None:
        cutoff = now - self._window_ns
        idx = 0
        for ts, count in self._buckets:
            if ts >= cutoff:
                break
            self._total -= count
            idx += 1
        if idx:
            del self._buckets[:idx]

    def rate(self, now_ns: int | None = None) -> float:
        now = self._monotonic() if now_ns is None else now_ns
        self._evict(now)
        if not self._buckets:
            return 0.0
        span_ns = max(1, min(self._window_ns, now - self._buckets[0][0] + 1))
        return self._total * NS_PER_SEC / span_ns


def wall_clock_ns() -> int:
    """Wall-clock time. **Metadata only** -- never pacing, never a payload field."""
    return time.time_ns()
