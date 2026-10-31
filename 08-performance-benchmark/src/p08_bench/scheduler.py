"""Open-loop arrival schedules (sections 3.1, 4.3; ADR-01).

The defining property: a schedule is computed from the target rate *before the
run starts* and never consults the system under test. That is what makes the
latency numbers meaningful. A closed-loop driver that waits for a response
before sending the next request stops offering load exactly when the system
slows down, so the slow period contributes few samples and the p95 looks fine --
coordinated omission, section 4.3.

Concretely, :meth:`RateSchedule.slots` yields the timestamp each slot was *due*,
and the driver records that timestamp whether it managed to issue on time, issued
late, or never issued at all. Nothing downstream can move a due time.

Ticks and quotes are separate schedules merged by timestamp rather than one
schedule with a kind field. Case E2E-05 is the reason: 100k ticks/s plus 25k
quotes/s has to keep both rates exact and both labels distinct, and interleaving
by ratio inside one stream makes the tick rate depend on the quote rate.

``max_pending_slots`` bounds the driver itself (case G02-09). A driver whose
backlog grows without limit stops being an instrument and becomes a second queue
in the system it is measuring.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Iterator, Sequence

from .errors import ConfigError
from .units import Unit

NS_PER_S = 1_000_000_000

#: The rate levels of NFR-01 and lane L13. Planned levels, not results: only the
#: ones a host actually sustains get recorded, and nothing above the highest
#: verified level may be extrapolated (section 6.3).
RATE_LADDER_TICKS_PER_S: tuple[int, ...] = (
    1_000, 10_000, 25_000, 50_000, 75_000, 100_000, 120_000,
)


class SlotKind(str, Enum):
    TICK = "tick"
    QUOTE = "quote"


@dataclass(frozen=True)
class Slot:
    """One scheduled arrival: a sequence number, a due time and a kind."""

    seq: int
    due_ns: int
    kind: SlotKind

    def as_dict(self) -> dict[str, Any]:
        return {"seq": self.seq, "due_ns": self.due_ns, "kind": self.kind.value}


class SegmentShape(str, Enum):
    """The three open-loop shapes of section 3.1."""

    CONSTANT = "constant"
    RAMP = "ramp"
    BURST = "burst"


@dataclass(frozen=True)
class Segment:
    """A phase of the schedule with its own rate profile.

    ``RAMP`` moves linearly from ``rate_per_s`` to ``end_rate_per_s``. ``BURST``
    is a constant segment that exists to be labelled as one: section 3.1 is
    explicit that a burst is never a sustainable capacity, so the shape travels
    with the data instead of being recoverable only from a comment.
    """

    shape: SegmentShape
    duration_s: float
    rate_per_s: float
    end_rate_per_s: float | None = None
    label: str = ""

    def __post_init__(self) -> None:
        if self.duration_s <= 0:
            raise ConfigError("segment duration must be positive", duration_s=self.duration_s)
        if self.rate_per_s < 0:
            raise ConfigError("segment rate may not be negative", rate_per_s=self.rate_per_s)
        if self.shape is SegmentShape.RAMP:
            if self.end_rate_per_s is None:
                raise ConfigError("a ramp segment needs an end rate")
            if self.end_rate_per_s < 0:
                raise ConfigError("ramp end rate may not be negative")
        elif self.end_rate_per_s is not None:
            raise ConfigError(
                "only a ramp segment takes an end rate", shape=self.shape.value
            )

    @property
    def mean_rate_per_s(self) -> float:
        if self.shape is SegmentShape.RAMP:
            return (self.rate_per_s + float(self.end_rate_per_s)) / 2.0
        return self.rate_per_s

    @property
    def expected_slots(self) -> int:
        """Slots this segment contributes, by area under the rate curve."""
        return int(self.mean_rate_per_s * self.duration_s)

    def as_dict(self) -> dict[str, Any]:
        return {
            "shape": self.shape.value,
            "duration_s": self.duration_s,
            "rate_per_s": self.rate_per_s,
            "end_rate_per_s": self.end_rate_per_s,
            "label": self.label,
            "expected_slots": self.expected_slots,
            "sustainable_capacity_candidate": self.shape is SegmentShape.CONSTANT,
        }


def constant(rate_per_s: float, duration_s: float, label: str = "") -> Segment:
    return Segment(SegmentShape.CONSTANT, duration_s, rate_per_s, label=label)


def ramp(from_rate: float, to_rate: float, duration_s: float, label: str = "") -> Segment:
    return Segment(SegmentShape.RAMP, duration_s, from_rate, end_rate_per_s=to_rate, label=label)


def burst(rate_per_s: float, duration_s: float, label: str = "") -> Segment:
    return Segment(SegmentShape.BURST, duration_s, rate_per_s, label=label)


class RateSchedule:
    """A deterministic sequence of due times for one event kind.

    Slot ``i`` of a constant segment is due at ``segment_start + i/rate``. The
    arithmetic is in nanoseconds and the accumulated start of each segment is
    carried as an integer, so a long schedule does not drift the way repeated
    float addition would.
    """

    __slots__ = ("kind", "segments", "start_ns")

    def __init__(
        self,
        segments: Sequence[Segment],
        *,
        kind: SlotKind = SlotKind.TICK,
        start_ns: int = 0,
    ) -> None:
        if not segments:
            raise ConfigError("a schedule needs at least one segment")
        self.segments = tuple(segments)
        self.kind = kind
        self.start_ns = int(start_ns)

    @classmethod
    def constant_rate(
        cls,
        rate_per_s: float,
        duration_s: float,
        *,
        kind: SlotKind = SlotKind.TICK,
        start_ns: int = 0,
        label: str = "steady",
    ) -> "RateSchedule":
        return cls([constant(rate_per_s, duration_s, label)], kind=kind, start_ns=start_ns)

    # -- shape ------------------------------------------------------------
    @property
    def duration_s(self) -> float:
        return sum(segment.duration_s for segment in self.segments)

    @property
    def expected_slots(self) -> int:
        return sum(segment.expected_slots for segment in self.segments)

    @property
    def peak_rate_per_s(self) -> float:
        return max(
            max(s.rate_per_s, s.end_rate_per_s or 0.0) for s in self.segments
        )

    @property
    def has_burst(self) -> bool:
        return any(s.shape is SegmentShape.BURST for s in self.segments)

    # -- generation -------------------------------------------------------
    def slots(self, *, first_seq: int = 0) -> Iterator[Slot]:
        """Yield every scheduled slot in due order.

        A zero-rate segment contributes nothing but still advances time, so a
        gap in the offered load is expressible without a special case.
        """
        seq = first_seq
        segment_start = self.start_ns
        for segment in self.segments:
            duration_ns = int(segment.duration_s * NS_PER_S)
            count = segment.expected_slots
            if count > 0:
                # Offsets are generated, never collected: a 120k/s half-hour
                # segment is 216 million slots, and building a list of them
                # would exhaust memory before the first slot was issued.
                if segment.shape is SegmentShape.RAMP:
                    offsets = self._ramp_offsets(segment, count)
                else:
                    step = duration_ns / count
                    offsets = (int(i * step) for i in range(count))
                for offset in offsets:
                    yield Slot(seq, segment_start + offset, self.kind)
                    seq += 1
            segment_start += duration_ns

    @staticmethod
    def _ramp_offsets(segment: Segment, count: int) -> Iterator[int]:
        """Due-time offsets for a linear ramp.

        With rate ``r(t) = r0 + (r1-r0) t/T``, the cumulative count is
        ``N(t) = r0 t + (r1-r0) t^2/(2T)``. Inverting it for each slot keeps the
        instantaneous rate correct rather than spacing the slots evenly, which
        would make a ramp indistinguishable from a constant segment at its mean.
        """
        r0 = segment.rate_per_s
        r1 = float(segment.end_rate_per_s)
        total_s = segment.duration_s
        slope = (r1 - r0) / total_s if total_s else 0.0
        for i in range(count):
            target = i + 0.5
            if abs(slope) < 1e-12:
                t = target / r0 if r0 else 0.0
            else:
                # Solve slope/2 t^2 + r0 t - target = 0 for the positive root.
                disc = r0 * r0 + 2 * slope * target
                t = (-r0 + (disc if disc > 0 else 0.0) ** 0.5) / slope
            yield int(min(max(t, 0.0), total_s) * NS_PER_S)

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "start_ns": self.start_ns,
            "duration_s": self.duration_s,
            "expected_slots": self.expected_slots,
            "peak_rate_per_s": self.peak_rate_per_s,
            "has_burst": self.has_burst,
            "unit": Unit.EVENTS_PER_S.value,
            "segments": [segment.as_dict() for segment in self.segments],
        }


class CompositeSchedule:
    """Several per-kind schedules merged into one due-ordered stream.

    Merging rather than interleaving by ratio is what keeps ``ticks/s`` and
    ``quotes/s`` independent and exact (case E2E-05). Sequence numbers are
    assigned after the merge so the driver sees one monotonic stream.
    """

    __slots__ = ("_schedules",)

    def __init__(self, schedules: Iterable[RateSchedule]) -> None:
        ordered = list(schedules)
        if not ordered:
            raise ConfigError("a composite schedule needs at least one schedule")
        kinds = [s.kind for s in ordered]
        if len(set(kinds)) != len(kinds):
            raise ConfigError(
                "two schedules share a kind; merge them into one instead",
                kinds=[k.value for k in kinds],
            )
        self._schedules = tuple(ordered)

    @property
    def schedules(self) -> tuple[RateSchedule, ...]:
        return self._schedules

    def expected_slots(self, kind: SlotKind | None = None) -> int:
        return sum(
            s.expected_slots for s in self._schedules if kind is None or s.kind is kind
        )

    def target_rate_per_s(self, kind: SlotKind) -> float:
        """Peak declared rate for one kind, which is what the label refers to."""
        for schedule in self._schedules:
            if schedule.kind is kind:
                return schedule.peak_rate_per_s
        return 0.0

    def slots(self) -> Iterator[Slot]:
        """Yield slots in due order, renumbered across kinds.

        ``heapq.merge`` keeps this streaming: a 120k/s half-hour schedule is
        216 million slots and must never be materialised as a list.
        """
        merged = heapq.merge(
            *(s.slots() for s in self._schedules), key=lambda slot: slot.due_ns
        )
        for seq, slot in enumerate(merged):
            yield Slot(seq, slot.due_ns, slot.kind)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schedules": [s.as_dict() for s in self._schedules],
            "expected_slots_total": self.expected_slots(),
            "expected_slots_by_kind": {
                s.kind.value: s.expected_slots for s in self._schedules
            },
            # The two labels of INV-12, kept apart in the output as well.
            "tick_target_per_s": self.target_rate_per_s(SlotKind.TICK),
            "quote_target_per_s": self.target_rate_per_s(SlotKind.QUOTE),
        }


@dataclass
class PhasePlan:
    """Warmup, ramp, steady and drain as one labelled schedule (section 11).

    The phases are kept separate because the measurement rules differ: warmup is
    reported but excluded from the steady p95, a ramp finds the knee point and
    establishes no capacity, and only the steady window feeds a capacity claim.
    """

    warmup_s: int
    ramp_s: int
    steady_s: int
    drain_s: int
    tick_rate_per_s: float
    quote_rate_per_s: float = 0.0
    ramp_from_rate_per_s: float = 0.0

    def __post_init__(self) -> None:
        for name in ("warmup_s", "ramp_s", "steady_s", "drain_s"):
            if getattr(self, name) < 0:
                raise ConfigError(f"{name} may not be negative", value=getattr(self, name))
        if self.steady_s <= 0:
            raise ConfigError("steady_s must be positive")

    def _segments(self, rate: float) -> list[Segment]:
        segments: list[Segment] = []
        if self.warmup_s:
            segments.append(constant(rate, self.warmup_s, "warmup"))
        if self.ramp_s:
            segments.append(ramp(self.ramp_from_rate_per_s, rate, self.ramp_s, "ramp"))
        segments.append(constant(rate, self.steady_s, "steady"))
        return segments

    def schedule(self, *, start_ns: int = 0) -> CompositeSchedule:
        schedules = [
            RateSchedule(self._segments(self.tick_rate_per_s), kind=SlotKind.TICK, start_ns=start_ns)
        ]
        if self.quote_rate_per_s > 0:
            schedules.append(
                RateSchedule(
                    self._segments(self.quote_rate_per_s), kind=SlotKind.QUOTE, start_ns=start_ns
                )
            )
        return CompositeSchedule(schedules)

    @property
    def steady_window_ns(self) -> tuple[int, int]:
        """Absolute ``[start, end)`` of the steady phase, relative to t0.

        The window the primary numbers come from. Having it as data stops a
        report from labelling a one-minute steady phase as thirty (case E2E-43).
        """
        start = (self.warmup_s + self.ramp_s) * NS_PER_S
        return start, start + self.steady_s * NS_PER_S

    def as_dict(self) -> dict[str, Any]:
        start, end = self.steady_window_ns
        return {
            "warmup_s": self.warmup_s,
            "ramp_s": self.ramp_s,
            "steady_s": self.steady_s,
            "drain_s": self.drain_s,
            "tick_rate_per_s": self.tick_rate_per_s,
            "quote_rate_per_s": self.quote_rate_per_s,
            "steady_window_ns": [start, end],
        }


def rate_ladder(
    levels: Sequence[int] = RATE_LADDER_TICKS_PER_S,
    *,
    seconds_per_level: float = 120.0,
) -> RateSchedule:
    """The 1k-to-120k ladder as one schedule of constant segments.

    Each level is its own labelled segment so the analyzer can judge them
    independently. Section 6.3: a ladder establishes the highest level that
    passed every gate, and nothing above it.
    """
    if not levels:
        raise ConfigError("a ladder needs at least one level")
    if sorted(levels) != list(levels):
        raise ConfigError("ladder levels must ascend", levels=list(levels))
    return RateSchedule(
        [constant(rate, seconds_per_level, f"level-{rate}") for rate in levels]
    )
