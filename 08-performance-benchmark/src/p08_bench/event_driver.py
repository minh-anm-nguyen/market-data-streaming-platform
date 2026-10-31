"""The open-loop event driver and its own accounting (sections 3.1, 4.1; group G02).

The driver walks a schedule and offers each slot to the system under test at the
time that slot was due. Four numbers come out, and keeping them apart is the
whole job (FR-01, FR-02):

``scheduled``
    Slots the manifest asked for. Fixed before the run.
``issued``
    Slots the driver actually emitted. If this is below ``scheduled``, no claim
    about the target rate is valid, however the pipeline behaved (case E2E-37,
    G02-01).
``accepted``
    What the pipeline admitted. Below ``issued`` by rejections and backpressure,
    which are different things and counted separately.
``late``
    Issued, but after the due time by more than the manifest tolerance. A driver
    that is merely late still offered the work; a driver that is late by seconds
    is no longer running the workload it claims.

The driver bounds itself. ``max_pending_slots`` caps how far behind its own
schedule it will chase before recording slots as never issued (case G02-09);
without that cap the driver becomes a second unbounded queue inside the
measurement.

Nothing here reads the pipeline for pacing. A SUT that stops acknowledging
causes late and dropped issues to be *recorded*, not the schedule to slow down
(case G02-02).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterable, Mapping

from .clock import FakeClock, MonotonicClock
from .errors import ConfigError
from .scheduler import CompositeSchedule, RateSchedule, Slot, SlotKind
from .sut import Identity, SubmitStatus, SystemUnderTest
from .units import Unit, rate

NS_PER_S = 1_000_000_000

#: A payload factory turns a slot into the event body the SUT receives.
PayloadFactory = Callable[[Slot], tuple[Identity, Mapping[str, Any]]]


class IssueOutcome(str, Enum):
    """What happened to one schedule slot from the driver's point of view."""

    ON_TIME = "ON_TIME"
    LATE = "LATE"
    NOT_ISSUED = "NOT_ISSUED"


@dataclass
class DriverCounters:
    """Counts per outcome, with the pipeline's answer kept separate.

    The split between ``rejected`` and ``backpressured`` matters: a rejection is
    the pipeline applying its validation policy, which is a result; backpressure
    is the pipeline saying it cannot take more, which is a capacity signal.
    Folding them together would make a correct run look lossy.
    """

    scheduled: int = 0
    issued: int = 0
    late: int = 0
    not_issued: int = 0
    accepted: int = 0
    rejected: int = 0
    backpressured: int = 0
    errors: int = 0
    by_kind_scheduled: dict[str, int] = field(default_factory=dict)
    by_kind_issued: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "scheduled": self.scheduled,
            "issued": self.issued,
            "late": self.late,
            "not_issued": self.not_issued,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "backpressured": self.backpressured,
            "errors": self.errors,
            "by_kind_scheduled": dict(sorted(self.by_kind_scheduled.items())),
            "by_kind_issued": dict(sorted(self.by_kind_issued.items())),
        }


@dataclass(frozen=True)
class IssueFidelity:
    """Whether the driver ran the workload it was configured to run.

    This is a judgement about the *instrument*, not the system, which is why it
    is separate from every performance number. Section 19.2: when issued differs
    from scheduled, the capacity test at that target is INVALID even if the
    system under test was never slow.
    """

    scheduled: int
    issued: int
    late: int
    max_late_issue_ms: int
    worst_late_ms: float | None
    tolerance_fraction: float

    @property
    def issue_fraction(self) -> float | None:
        if self.scheduled == 0:
            return None
        return self.issued / self.scheduled

    @property
    def late_fraction(self) -> float | None:
        if self.issued == 0:
            return None
        return self.late / self.issued

    @property
    def calibrated(self) -> bool:
        """True when the driver issued enough, soon enough, to support a claim."""
        fraction = self.issue_fraction
        if fraction is None:
            return False
        if fraction < 1.0 - self.tolerance_fraction:
            return False
        late = self.late_fraction
        return late is not None and late <= self.tolerance_fraction

    def reasons(self) -> list[str]:
        out: list[str] = []
        fraction = self.issue_fraction
        if fraction is None:
            out.append("no slots were scheduled")
            return out
        if fraction < 1.0 - self.tolerance_fraction:
            out.append(
                f"driver issued {self.issued}/{self.scheduled} slots "
                f"({fraction:.4f}); a target-rate claim is INVALID"
            )
        late = self.late_fraction
        if late is not None and late > self.tolerance_fraction:
            out.append(
                f"{self.late}/{self.issued} slots were issued more than "
                f"{self.max_late_issue_ms} ms late ({late:.4f})"
            )
        if self.worst_late_ms is not None and self.worst_late_ms > self.max_late_issue_ms:
            out.append(f"worst issue lag was {self.worst_late_ms:.3f} ms")
        return out

    def as_dict(self) -> dict[str, Any]:
        return {
            "scheduled": self.scheduled,
            "issued": self.issued,
            "late": self.late,
            "issue_fraction": self.issue_fraction,
            "late_fraction": self.late_fraction,
            "max_late_issue_ms": self.max_late_issue_ms,
            "worst_late_ms": self.worst_late_ms,
            "tolerance_fraction": self.tolerance_fraction,
            "calibrated": self.calibrated,
            "reasons": self.reasons(),
        }


@dataclass
class DriveResult:
    """Everything one drive produced."""

    counters: DriverCounters
    fidelity: IssueFidelity
    first_due_ns: int | None
    last_due_ns: int | None
    issue_lags_ms: list[float]
    sut_is_simulation: bool

    @property
    def measured_seconds(self) -> float | None:
        """Elapsed schedule span, used as every rate denominator (section 4.2)."""
        if self.first_due_ns is None or self.last_due_ns is None:
            return None
        span = (self.last_due_ns - self.first_due_ns) / NS_PER_S
        return span if span > 0 else None

    def issued_rate(self, kind: SlotKind, unit: Unit) -> Any:
        """Issued rate for one kind, over measured seconds, in a named unit."""
        elapsed = self.measured_seconds
        if elapsed is None:
            return None
        count = self.counters.by_kind_issued.get(kind.value, 0)
        return rate(count, elapsed, unit)

    def as_dict(self) -> dict[str, Any]:
        elapsed = self.measured_seconds
        payload: dict[str, Any] = {
            "counters": self.counters.as_dict(),
            "fidelity": self.fidelity.as_dict(),
            "measured_seconds": elapsed,
            "sut_is_simulation": self.sut_is_simulation,
        }
        if elapsed:
            # Ticks and events are different units and are printed as such.
            ticks = self.counters.by_kind_issued.get(SlotKind.TICK.value, 0)
            total = self.counters.issued
            payload["issued_ticks_per_s"] = rate(ticks, elapsed, Unit.TICKS_PER_S).as_dict()
            payload["issued_events_per_s"] = rate(total, elapsed, Unit.EVENTS_PER_S).as_dict()
        return payload


def default_payload_factory(
    dataset_id: str, symbols: list[str], *, base_time_ns: int = 0, price_ticks: int = 100_000
) -> PayloadFactory:
    """Round-robin symbols with per-symbol sequences, like P01.

    The sequence is per symbol and shared across ticks and quotes, which is the
    P01 identity contract that section 0.1 requires P08 to preserve. Prices stay
    integers (INV-03).
    """
    if not symbols:
        raise ConfigError("payload factory needs at least one symbol")
    sequences: dict[str, int] = {symbol: 0 for symbol in symbols}

    def factory(slot: Slot) -> tuple[Identity, Mapping[str, Any]]:
        symbol = symbols[slot.seq % len(symbols)]
        sequences[symbol] += 1
        identity = Identity(dataset_id, symbol, sequences[symbol])
        payload: dict[str, Any] = {
            "dataset_id": dataset_id,
            "symbol": symbol,
            "sequence": sequences[symbol],
            "event_time_ns": base_time_ns + slot.due_ns,
            "kind": slot.kind.value,
        }
        if slot.kind is SlotKind.TICK:
            payload["price_ticks"] = price_ticks + slot.seq
        else:
            payload["bid_ticks"] = price_ticks + slot.seq - 1
            payload["ask_ticks"] = price_ticks + slot.seq + 1
        return identity, payload

    return factory


class EventDriver:
    """Walks a schedule against a SUT, in schedule time.

    Time comes from the clock, not from the SUT. With a
    :class:`~p08_bench.clock.FakeClock` the whole thing is a deterministic
    simulation that runs a thirty-minute schedule in milliseconds; with a
    monotonic clock it drives a real pipeline and the lag between due time and
    actual issue time is recorded rather than smoothed away.
    """

    __slots__ = ("sut", "clock", "max_pending_slots", "max_late_issue_ms", "tolerance_fraction", "sample_lag_every")

    def __init__(
        self,
        sut: SystemUnderTest,
        clock: MonotonicClock | FakeClock,
        *,
        max_pending_slots: int = 100_000,
        max_late_issue_ms: int = 10,
        tolerance_fraction: float = 0.01,
        sample_lag_every: int = 1,
    ) -> None:
        if max_pending_slots <= 0:
            raise ConfigError("max_pending_slots must be positive", value=max_pending_slots)
        if max_late_issue_ms <= 0:
            raise ConfigError("max_late_issue_ms must be positive", value=max_late_issue_ms)
        if not (0.0 <= tolerance_fraction < 1.0):
            raise ConfigError("tolerance_fraction must be in [0, 1)", value=tolerance_fraction)
        if sample_lag_every <= 0:
            raise ConfigError("sample_lag_every must be positive", value=sample_lag_every)
        self.sut = sut
        self.clock = clock
        self.max_pending_slots = max_pending_slots
        self.max_late_issue_ms = max_late_issue_ms
        self.tolerance_fraction = tolerance_fraction
        self.sample_lag_every = sample_lag_every

    def drive(
        self,
        schedule: CompositeSchedule | RateSchedule,
        payload_factory: PayloadFactory,
        *,
        on_slot: Callable[[Slot, int], None] | None = None,
    ) -> DriveResult:
        """Offer every slot of ``schedule`` and return the accounting.

        ``on_slot`` is called with each slot and its issue time, so an observer
        can sample windows without the driver knowing about metrics.
        """
        slots: Iterable[Slot] = (
            schedule.slots() if isinstance(schedule, CompositeSchedule) else schedule.slots()
        )
        counters = DriverCounters()
        lags: list[float] = []
        worst_late_ms: float | None = None
        first_due: int | None = None
        last_due: int | None = None
        pending = 0

        for slot in slots:
            counters.scheduled += 1
            counters.by_kind_scheduled[slot.kind.value] = (
                counters.by_kind_scheduled.get(slot.kind.value, 0) + 1
            )
            if first_due is None:
                first_due = slot.due_ns
            last_due = slot.due_ns

            now = self.clock.now_ns()
            # Open loop: if the clock is behind the due time, the slot is simply
            # not due yet and schedule time advances to it. The SUT is never
            # consulted about whether to proceed.
            if isinstance(self.clock, FakeClock) and now < slot.due_ns:
                self.clock.set_ns(slot.due_ns)
                now = slot.due_ns

            lag_ns = now - slot.due_ns
            if lag_ns > 0:
                pending += 1
            else:
                pending = 0

            if pending > self.max_pending_slots:
                # The driver refuses to chase its own backlog without limit. The
                # slot is recorded as never issued, which is an instrument
                # finding, not a pipeline one.
                counters.not_issued += 1
                continue

            identity, payload = payload_factory(slot)
            outcome = self.sut.submit(identity, payload, now)

            if outcome.status is SubmitStatus.DROPPED:
                counters.not_issued += 1
                continue

            counters.issued += 1
            counters.by_kind_issued[slot.kind.value] = (
                counters.by_kind_issued.get(slot.kind.value, 0) + 1
            )
            if outcome.status is SubmitStatus.ACCEPTED:
                counters.accepted += 1
            elif outcome.status is SubmitStatus.REJECTED:
                counters.rejected += 1
            elif outcome.status is SubmitStatus.BACKPRESSURED:
                counters.backpressured += 1
            elif outcome.status is SubmitStatus.ERROR:
                counters.errors += 1

            lag_ms = lag_ns / 1e6
            if lag_ns > self.max_late_issue_ms * 1_000_000:
                counters.late += 1
                if worst_late_ms is None or lag_ms > worst_late_ms:
                    worst_late_ms = lag_ms
            if counters.scheduled % self.sample_lag_every == 0:
                lags.append(lag_ms)

            if on_slot is not None:
                on_slot(slot, now)

        fidelity = IssueFidelity(
            scheduled=counters.scheduled,
            issued=counters.issued,
            late=counters.late,
            max_late_issue_ms=self.max_late_issue_ms,
            worst_late_ms=worst_late_ms,
            tolerance_fraction=self.tolerance_fraction,
        )
        return DriveResult(
            counters=counters,
            fidelity=fidelity,
            first_due_ns=first_due,
            last_due_ns=last_due,
            issue_lags_ms=lags,
            sut_is_simulation=self.sut.is_simulation,
        )
