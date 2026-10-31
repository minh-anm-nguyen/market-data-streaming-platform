"""Load calibration: proving the driver is not the bottleneck (lane L00; group G02).

Section 2.1 gives the calibrator one job and one prohibition. The job: show on a
null sink that the driver can issue the schedule it is about to claim. The
prohibition: never become the bottleneck and then keep using the *requested*
rate to compute throughput.

Case E2E-01 is the lane, case E2E-37 is the failure it catches -- a driver
configured for 120k that emits 70k -- and case G02-01 is the same thing caused
by CPU starvation. In all three the verdict about the pipeline is ``INVALID``,
not ``FAIL``: nothing was learned about the system, because the workload was
never applied.

:class:`NullSink` deliberately declares its stages ``NOT_CONFIGURED`` rather
than ``SIMULATED``. It is not a model of a pipeline, it is the absence of one,
and :meth:`~p08_bench.sut.SutBase.require_stages` must refuse any lane beyond
L00 that is pointed at it.

:attr:`CalibrationResult.sink_is_suspect` covers case G02-05: a null sink with
its own injected delay is slower than the real thing, so a baseline measured
against it understates what the driver can do.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

from .clock import FakeClock, MonotonicClock
from .event_driver import DriveResult, EventDriver, PayloadFactory
from .metrics import StageCounters
from .percentiles import describe_distribution, exact_nearest_rank
from .scheduler import CompositeSchedule, RateSchedule, SlotKind
from .sut import (
    Availability,
    Backlog,
    Identity,
    Observation,
    Stage,
    StageStatus,
    SubmitOutcome,
    SubmitStatus,
    SutBase,
)
from .units import Quantity, Unit, rate

NS_PER_S = 1_000_000_000


class NullSink(SutBase):
    """Accepts everything and keeps nothing but counts.

    ``delay_ns`` exists so a test can build the case G02-05 situation on
    purpose: a sink that is itself slow, producing a calibration baseline that
    is lower than the driver is really capable of.
    """

    def __init__(self, *, delay_ns: int = 0) -> None:
        self.delay_ns = int(delay_ns)
        self._offered = 0
        self._now_ns = 0
        self.STAGES = {
            Stage.SOURCE: StageStatus(
                Stage.SOURCE, Availability.SIMULATED, "null sink: counts offers, stores nothing"
            ),
            **{
                stage: StageStatus(
                    stage, Availability.NOT_CONFIGURED,
                    "null sink has no pipeline behind it; calibration lane only",
                )
                for stage in (
                    Stage.ADAPTER, Stage.PRODUCER, Stage.BROKER, Stage.CONSUMER,
                    Stage.RAW, Stage.CANONICAL, Stage.QUERY,
                )
            },
        }

    def submit(self, identity: Identity, payload: Mapping[str, Any], t_ns: int) -> SubmitOutcome:
        self._offered += 1
        self._now_ns = max(self._now_ns, t_ns) + self.delay_ns
        return SubmitOutcome(SubmitStatus.ACCEPTED, identity, "null sink")

    def advance_to(self, t_ns: int) -> None:
        self._now_ns = max(self._now_ns, t_ns)

    def observe(self, t_ns: int) -> Observation:
        self.advance_to(t_ns)
        return Observation(
            t_ns=t_ns,
            counters=StageCounters(offered_count=self._offered, issued_count=self._offered),
            backlog=Backlog(),
            notes=["null sink: no pipeline measured"],
        )

    def drain(self, deadline_ns: int) -> Observation:
        return self.observe(deadline_ns)

    def canonical_identities(self) -> set[tuple[str, str, int]]:
        return set()


class Calibration(str, Enum):
    """Whether the driver may be used to make a claim at this rate."""

    CALIBRATED = "CALIBRATED"
    #: The driver could not issue the schedule: the instrument is the limit.
    GENERATOR_BOUND = "GENERATOR_BOUND"
    #: Issued enough but too far behind schedule to call it this workload.
    SCHEDULE_DRIFT = "SCHEDULE_DRIFT"
    #: Nothing was measured.
    NOT_RUN = "NOT_RUN"


@dataclass(frozen=True)
class CalibrationResult:
    """What the calibration lane establishes, and what it refuses to establish."""

    target_ticks_per_s: float
    target_events_per_s: float
    drive: DriveResult
    verdict: Calibration
    sink_delay_ns: int
    reasons: tuple[str, ...] = ()

    @property
    def achieved_events_per_s(self) -> Quantity | None:
        elapsed = self.drive.measured_seconds
        if elapsed is None:
            return None
        return rate(self.drive.counters.issued, elapsed, Unit.EVENTS_PER_S)

    @property
    def achieved_ticks_per_s(self) -> Quantity | None:
        elapsed = self.drive.measured_seconds
        if elapsed is None:
            return None
        ticks = self.drive.counters.by_kind_issued.get(SlotKind.TICK.value, 0)
        return rate(ticks, elapsed, Unit.TICKS_PER_S)

    @property
    def sink_is_suspect(self) -> bool:
        """Case G02-05: a null sink with its own latency is not a null sink."""
        return self.sink_delay_ns > 0

    @property
    def supports_rate_claim(self) -> bool:
        """Whether a pipeline measurement at this target can be believed.

        This is the gate section 19.2 describes: with the driver short of its
        schedule, the capacity test at that target is INVALID even when the
        system under test was never the problem.
        """
        return self.verdict is Calibration.CALIBRATED and not self.sink_is_suspect

    def lag_summary(self) -> dict[str, Any]:
        return describe_distribution(self.drive.issue_lags_ms)

    def p99_lag_ms(self) -> float | None:
        lags = self.drive.issue_lags_ms
        return exact_nearest_rank(lags, 0.99) if lags else None

    def as_dict(self) -> dict[str, Any]:
        achieved_ticks = self.achieved_ticks_per_s
        achieved_events = self.achieved_events_per_s
        return {
            "verdict": self.verdict.value,
            "supports_rate_claim": self.supports_rate_claim,
            "sink_is_suspect": self.sink_is_suspect,
            "sink_delay_ns": self.sink_delay_ns,
            # Target and achieved, in both units, never collapsed into one
            # number (INV-12).
            "target_ticks_per_s": self.target_ticks_per_s,
            "target_events_per_s": self.target_events_per_s,
            "achieved_ticks_per_s": achieved_ticks.as_dict() if achieved_ticks else None,
            "achieved_events_per_s": achieved_events.as_dict() if achieved_events else None,
            "issue_lag_ms": self.lag_summary(),
            "p99_issue_lag_ms": self.p99_lag_ms(),
            "reasons": list(self.reasons),
            "drive": self.drive.as_dict(),
        }


def calibrate(
    schedule: CompositeSchedule | RateSchedule,
    payload_factory: PayloadFactory,
    clock: MonotonicClock | FakeClock,
    *,
    sink: NullSink | None = None,
    max_late_issue_ms: int = 10,
    tolerance_fraction: float = 0.01,
    max_pending_slots: int = 100_000,
) -> CalibrationResult:
    """Drive ``schedule`` against a null sink and judge the driver.

    The verdict distinguishes two failures that look similar in a chart and mean
    different things: too few slots issued (``GENERATOR_BOUND``) and enough
    slots issued too late (``SCHEDULE_DRIFT``). The first says the driver cannot
    produce the load; the second says it produced it as a different workload.
    """
    target_sink = sink or NullSink()
    driver = EventDriver(
        target_sink,
        clock,
        max_pending_slots=max_pending_slots,
        max_late_issue_ms=max_late_issue_ms,
        tolerance_fraction=tolerance_fraction,
    )
    result = driver.drive(schedule, payload_factory)

    if isinstance(schedule, CompositeSchedule):
        target_ticks = schedule.target_rate_per_s(SlotKind.TICK)
        target_events = sum(s.peak_rate_per_s for s in schedule.schedules)
    else:
        target_ticks = schedule.peak_rate_per_s if schedule.kind is SlotKind.TICK else 0.0
        target_events = schedule.peak_rate_per_s

    fidelity = result.fidelity
    reasons = list(fidelity.reasons())
    if result.counters.scheduled == 0:
        verdict = Calibration.NOT_RUN
    elif (fidelity.issue_fraction or 0.0) < 1.0 - tolerance_fraction:
        verdict = Calibration.GENERATOR_BOUND
    elif not fidelity.calibrated:
        verdict = Calibration.SCHEDULE_DRIFT
    else:
        verdict = Calibration.CALIBRATED

    if target_sink.delay_ns > 0:
        reasons.append(
            f"null sink has an injected delay of {target_sink.delay_ns} ns; this "
            "baseline understates the driver and may not be trusted (case G02-05)"
        )

    return CalibrationResult(
        target_ticks_per_s=target_ticks,
        target_events_per_s=target_events,
        drive=result,
        verdict=verdict,
        sink_delay_ns=target_sink.delay_ns,
        reasons=tuple(reasons),
    )
