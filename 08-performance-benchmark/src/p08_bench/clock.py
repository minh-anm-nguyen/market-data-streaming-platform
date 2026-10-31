"""Clock domains, monotonic timers and cross-host sync accounting (section 4.3).

Section 4.3 splits latency metrics by *which clock* can legitimately measure
them. ``kafka_enqueue_to_ack`` is a duration inside one process, so a monotonic
counter is exact. ``end_to_end_event_latency`` spans two hosts, so the answer is
only as good as the synchronisation between them -- and if the sync uncertainty
is the same order as the latency being claimed, the honest verdict is
``INCONCLUSIVE``, not a number (INV-15, case E2E-42).

The type system does that bookkeeping. A :class:`Timestamp` belongs to a
:class:`ClockDomain`; subtracting two timestamps from different domains raises
instead of producing a plausible-looking difference. Cross-host intervals have
to go through :class:`ClockSyncAssessment`, which can refuse.

Wall-clock time is kept separately and only ever used as correlation metadata:
an NTP step can move it backwards mid-run, so it must never measure a duration.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from .errors import ClockError

#: Default ceiling on how much of a latency target may be clock uncertainty
#: before a cross-host one-way claim is refused. 10% is a lab guardrail chosen
#: so a 280 ms budget tolerates 28 ms of uncertainty; it is not a standard, and
#: every run records the value it used.
DEFAULT_MAX_UNCERTAINTY_FRACTION = 0.10


@dataclass(frozen=True)
class ClockDomain:
    """A named clock. Two timestamps are comparable only within one domain.

    ``host`` and ``process`` are both part of identity: two processes on one host
    share a monotonic *base* on Linux but not on every platform, and section 4.3
    scopes ``adapter_receive_to_emit`` to "the same process" for exactly that
    reason.
    """

    host: str
    process: str = "main"

    @property
    def name(self) -> str:
        return f"{self.host}/{self.process}"

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True, order=False)
class Timestamp:
    """A monotonic reading, tagged with the clock that produced it."""

    ns: int
    domain: ClockDomain

    def __sub__(self, other: "Timestamp") -> int:
        """Duration in nanoseconds, same domain only.

        A negative result means the clock went backwards, which on a monotonic
        source is a bug or a platform quirk rather than a fast event; either way
        it must not become a negative latency sample (case G07).
        """
        if not isinstance(other, Timestamp):
            raise TypeError(f"cannot subtract {type(other).__name__} from Timestamp")
        if self.domain != other.domain:
            raise ClockError(
                "refusing to subtract timestamps from different clock domains; "
                "use ClockSyncAssessment for a cross-host interval",
                left=self.domain.name,
                right=other.domain.name,
            )
        delta = self.ns - other.ns
        if delta < 0:
            raise ClockError(
                "monotonic clock went backwards",
                domain=self.domain.name,
                delta_ns=delta,
            )
        return delta

    def as_dict(self) -> dict[str, Any]:
        return {"ns": self.ns, "clock_domain": self.domain.name}


class MonotonicClock:
    """``perf_counter_ns`` scoped to a domain, with a backwards-step guard.

    The guard keeps state, so one clock instance per domain per process. It
    raises rather than clamping: a clamped reading would silently compress a
    latency sample toward zero.
    """

    __slots__ = ("domain", "_source", "_last")

    def __init__(
        self,
        domain: ClockDomain,
        source: Callable[[], int] = time.perf_counter_ns,
    ) -> None:
        self.domain = domain
        self._source = source
        self._last = -1

    def now(self) -> Timestamp:
        reading = int(self._source())
        if reading < self._last:
            raise ClockError(
                "monotonic source returned a lower value than the previous read",
                domain=self.domain.name,
                previous_ns=self._last,
                reading_ns=reading,
            )
        self._last = reading
        return Timestamp(reading, self.domain)

    def now_ns(self) -> int:
        return self.now().ns


class FakeClock:
    """A clock tests drive by hand.

    Every timing assertion in the suite uses this rather than sleeping: a test
    that measures "30 minutes of steady state" has to be able to do it in
    microseconds, and a test of the backwards-clock guard has to be able to
    actually step backwards.
    """

    __slots__ = ("domain", "_ns")

    def __init__(self, domain: ClockDomain, start_ns: int = 0) -> None:
        self.domain = domain
        self._ns = int(start_ns)

    def now(self) -> Timestamp:
        return Timestamp(self._ns, self.domain)

    def now_ns(self) -> int:
        return self._ns

    def advance_ns(self, delta_ns: int) -> None:
        if delta_ns < 0:
            raise ClockError("use set_ns to move a FakeClock backwards", delta_ns=delta_ns)
        self._ns += int(delta_ns)

    def advance_s(self, delta_s: float) -> None:
        self.advance_ns(int(round(delta_s * 1e9)))

    def set_ns(self, ns: int) -> None:
        """Move the clock anywhere, including backwards, to test the guards."""
        self._ns = int(ns)


class Stopwatch:
    """One interval in one domain. ``elapsed_ns`` is only valid after ``stop``."""

    __slots__ = ("_clock", "_start", "_end")

    def __init__(self, clock: MonotonicClock | FakeClock) -> None:
        self._clock = clock
        self._start: Timestamp | None = None
        self._end: Timestamp | None = None

    def start(self) -> "Stopwatch":
        self._start = self._clock.now()
        self._end = None
        return self

    def stop(self) -> int:
        if self._start is None:
            raise ClockError("stopwatch stopped before it was started")
        self._end = self._clock.now()
        return self._end - self._start

    @property
    def elapsed_ns(self) -> int:
        if self._start is None or self._end is None:
            raise ClockError("stopwatch has no completed interval")
        return self._end - self._start


class ClockSyncQuality(str, Enum):
    """How much a cross-host interval can be trusted."""

    ADEQUATE = "ADEQUATE"
    INSUFFICIENT = "INSUFFICIENT"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class ClockSyncAssessment:
    """Measured synchronisation between two clock domains.

    ``uncertainty_ns`` is the number that decides claims. It is not the offset:
    a known, stable 5 ms offset can be subtracted, while 5 ms of *unknown*
    wander cannot. Section 4.3 requires offset, jitter and uncertainty all to be
    logged, so all three are fields and none has a default that implies success.
    """

    left: ClockDomain
    right: ClockDomain
    offset_ns: int | None
    jitter_ns: int | None
    uncertainty_ns: int | None
    method: str = "unmeasured"
    measured_at_utc: str | None = None

    @classmethod
    def unmeasured(cls, left: ClockDomain, right: ClockDomain) -> "ClockSyncAssessment":
        """No sync measurement was taken.

        This is the default state, and it yields ``UNKNOWN`` rather than
        ``ADEQUATE``: section 15 forbids a missing measurement from defaulting
        to the passing value.
        """
        return cls(left, right, None, None, None, method="unmeasured")

    def quality_for(
        self,
        target_ns: int,
        *,
        max_fraction: float = DEFAULT_MAX_UNCERTAINTY_FRACTION,
    ) -> ClockSyncQuality:
        if self.uncertainty_ns is None:
            return ClockSyncQuality.UNKNOWN
        if target_ns <= 0:
            raise ClockError("target_ns must be positive", target_ns=target_ns)
        budget = target_ns * max_fraction
        return (
            ClockSyncQuality.ADEQUATE
            if self.uncertainty_ns <= budget
            else ClockSyncQuality.INSUFFICIENT
        )

    def supports_one_way_latency(
        self,
        target_ns: int,
        *,
        max_fraction: float = DEFAULT_MAX_UNCERTAINTY_FRACTION,
    ) -> bool:
        return self.quality_for(target_ns, max_fraction=max_fraction) is ClockSyncQuality.ADEQUATE

    def cross_host_interval_ns(self, start: Timestamp, end: Timestamp) -> int:
        """Offset-corrected interval between two domains.

        Raises when the offset was never measured. The correction is applied to
        ``start``, so the caller gets a number only if someone actually measured
        the relationship between the two clocks.
        """
        if self.offset_ns is None:
            raise ClockError(
                "cross-host interval needs a measured offset",
                left=self.left.name,
                right=self.right.name,
            )
        pair = {start.domain, end.domain}
        if pair != {self.left, self.right} and len(pair) != 1:
            raise ClockError(
                "assessment does not cover these domains",
                assessment=f"{self.left.name}<->{self.right.name}",
                start=start.domain.name,
                end=end.domain.name,
            )
        if start.domain == end.domain:
            return end - start
        if start.domain == self.left:
            return end.ns - (start.ns + self.offset_ns)
        return end.ns - (start.ns - self.offset_ns)

    def as_dict(self) -> dict[str, Any]:
        return {
            "left_domain": self.left.name,
            "right_domain": self.right.name,
            "offset_ns": self.offset_ns,
            "jitter_ns": self.jitter_ns,
            "uncertainty_ns": self.uncertainty_ns,
            "method": self.method,
            "measured_at_utc": self.measured_at_utc,
        }


@dataclass
class WallClockCorrelation:
    """UTC metadata for joining records across systems.

    Deliberately has no duration helper. Section 4.3: wall clock correlates, it
    never measures, because an NTP step mid-run would otherwise show up as a
    negative or inflated latency.
    """

    utc_iso: str
    monotonic: Timestamp
    notes: list[str] = field(default_factory=list)

    @classmethod
    def capture(cls, clock: MonotonicClock | FakeClock, utc_iso: str | None = None) -> "WallClockCorrelation":
        stamp = utc_iso or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return cls(utc_iso=stamp, monotonic=clock.now())

    def as_dict(self) -> dict[str, Any]:
        return {
            "utc_iso": self.utc_iso,
            "monotonic": self.monotonic.as_dict(),
            "notes": list(self.notes),
            "usable_for_durations": False,
        }
