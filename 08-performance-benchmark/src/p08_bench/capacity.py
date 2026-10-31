"""Capacity gates and the saturation frontier (sections 4.2, 6.3, 19.1).

Section 4.2 defines a candidate sustainable capacity as a conjunction, and the
conjunction is the whole content: a fixed workload, a steady window of the
declared length, no logical mismatch, per-partition backlog growth near zero
over the whole window *and* its tail, latency, error and resource budgets met,
and a drain plus reconcile afterwards proving no processing debt is left.

:class:`CapacityGates` holds that conjunction as named gates so a failing run
says which gate it failed rather than "did not reach 120k".

:class:`CapacityFrontier` adds the rule from section 6.3 that is easiest to
break by accident. If 100k passes every gate and 120k does not, the verified
bound is ``>= 100k`` and **nothing** may be said about 110k. So the frontier
exposes :meth:`CapacityFrontier.verified_bound` and
:meth:`CapacityFrontier.extrapolate`, and the second one always raises.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence

from .errors import MetrologyError
from .timeseries import Stability, StabilityAssessment
from .units import Quantity, Unit
from .verdict import Status, Tri


class Gate(str, Enum):
    """The named conditions of section 4.2."""

    CALIBRATED = "driver_calibrated"
    STEADY_DURATION = "steady_duration"
    BACKLOG_STABLE = "backlog_stable_per_partition"
    DRAIN_RECONCILED = "drain_reconciled"
    CORRECTNESS = "correctness_clean"
    LATENCY_BUDGET = "latency_budget"
    ERROR_BUDGET = "error_budget"
    RESOURCE_BUDGET = "resource_budget"


@dataclass(frozen=True)
class GateResult:
    gate: Gate
    value: Tri
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {"gate": self.gate.value, "value": self.value.value, "detail": self.detail}


@dataclass
class CapacityGates:
    """All gates for one offered rate level."""

    offered: Quantity
    useful_verified: Quantity | None = None
    results: dict[Gate, GateResult] = field(default_factory=dict)

    def set(self, gate: Gate, value: Tri, detail: str = "") -> None:
        self.results[gate] = GateResult(gate, value, detail)

    def get(self, gate: Gate) -> GateResult:
        return self.results.get(gate, GateResult(gate, Tri.UNKNOWN, "not evaluated"))

    @property
    def status(self) -> Status:
        """PASS only when every gate is TRUE; UNKNOWN anywhere is INCONCLUSIVE."""
        values = [self.get(gate).value for gate in Gate]
        if Tri.FALSE in values:
            return Status.FAIL
        if Tri.UNKNOWN in values:
            return Status.INCONCLUSIVE
        return Status.PASS

    @property
    def passed(self) -> bool:
        return self.status is Status.PASS

    def failed_gates(self) -> list[Gate]:
        return [gate for gate in Gate if self.get(gate).value is Tri.FALSE]

    def unknown_gates(self) -> list[Gate]:
        return [gate for gate in Gate if self.get(gate).value is Tri.UNKNOWN]

    def as_dict(self) -> dict[str, Any]:
        return {
            "offered": self.offered.as_dict(),
            "useful_verified": self.useful_verified.as_dict() if self.useful_verified else None,
            "status": self.status.value,
            "gates": [self.get(gate).as_dict() for gate in Gate],
            "failed_gates": [g.value for g in self.failed_gates()],
            "unknown_gates": [g.value for g in self.unknown_gates()],
        }


def gates_from_observations(
    offered: Quantity,
    *,
    useful_verified: Quantity | None,
    calibrated: bool | None,
    steady_seconds: int,
    required_steady_seconds: int,
    backlog: StabilityAssessment | None,
    drain_reconciled: bool | None,
    correctness_clean: bool | None,
    latency_within_budget: bool | None,
    error_fraction: float | None,
    error_budget: float,
    resource_blockers: Sequence[str],
) -> CapacityGates:
    """Assemble the gate set from measured values.

    Each argument that can be unmeasured is ``bool | None``, and ``None``
    becomes ``UNKNOWN`` rather than a failure or a pass. ``resource_blockers``
    is the list of budgets this host could not evidence (see
    :meth:`~p08_bench.collectors.ResourceObserver.budget_blockers`); a non-empty
    list means the resource gate is unknown, not met.
    """
    gates = CapacityGates(offered=offered, useful_verified=useful_verified)

    gates.set(
        Gate.CALIBRATED,
        Tri.of(calibrated),
        "driver issued the scheduled workload" if calibrated else "driver did not issue the schedule",
    )
    gates.set(
        Gate.STEADY_DURATION,
        Tri.of(steady_seconds >= required_steady_seconds),
        f"steady window {steady_seconds}s against a required {required_steady_seconds}s",
    )

    if backlog is None:
        gates.set(Gate.BACKLOG_STABLE, Tri.UNKNOWN, "no backlog series was collected")
    elif backlog.verdict is Stability.STABLE:
        gates.set(Gate.BACKLOG_STABLE, Tri.TRUE, f"worst partition {backlog.series_name} is stable")
    elif backlog.verdict in (Stability.GROWING,):
        gates.set(
            Gate.BACKLOG_STABLE, Tri.FALSE,
            f"{backlog.series_name}: " + "; ".join(backlog.reasons),
        )
    elif backlog.verdict is Stability.SHRINKING:
        # A draining backlog during steady state means the offered rate is
        # below what the sink can take; it does not break the capacity claim.
        gates.set(Gate.BACKLOG_STABLE, Tri.TRUE, f"{backlog.series_name} is draining")
    else:
        gates.set(
            Gate.BACKLOG_STABLE, Tri.UNKNOWN,
            f"{backlog.series_name}: " + "; ".join(backlog.reasons),
        )

    gates.set(Gate.DRAIN_RECONCILED, Tri.of(drain_reconciled))
    gates.set(Gate.CORRECTNESS, Tri.of(correctness_clean))
    gates.set(Gate.LATENCY_BUDGET, Tri.of(latency_within_budget))
    gates.set(
        Gate.ERROR_BUDGET,
        Tri.UNKNOWN if error_fraction is None else Tri.of(error_fraction <= error_budget),
        f"error fraction {error_fraction} against budget {error_budget}",
    )
    gates.set(
        Gate.RESOURCE_BUDGET,
        Tri.UNKNOWN if resource_blockers else Tri.TRUE,
        "; ".join(resource_blockers) if resource_blockers else "all budgets evidenced",
    )
    return gates


class CapacityFrontier:
    """Ladder levels and the single bound they justify (section 6.3)."""

    __slots__ = ("unit", "_levels")

    def __init__(self, unit: Unit = Unit.TICKS_PER_S) -> None:
        if unit not in (Unit.TICKS_PER_S, Unit.EVENTS_PER_S, Unit.RECORDS_PER_S, Unit.QUERIES_PER_S):
            raise MetrologyError("a capacity frontier needs a rate unit", unit=unit.value)
        self.unit = unit
        self._levels: dict[float, CapacityGates] = {}

    def add(self, gates: CapacityGates) -> None:
        if gates.offered.unit is not self.unit:
            raise MetrologyError(
                "level unit does not match the frontier unit",
                frontier=self.unit.value,
                level=gates.offered.unit.value,
            )
        self._levels[gates.offered.value] = gates

    @property
    def levels(self) -> list[float]:
        return sorted(self._levels)

    def result(self, level: float) -> CapacityGates:
        gates = self._levels.get(level)
        if gates is None:
            raise MetrologyError("no result for that level", level=level)
        return gates

    def verified_bound(self) -> Quantity | None:
        """The highest level that passed **every** gate.

        Not the highest level that ran, and not the highest throughput seen in
        any window. ``None`` when no level passed, which is a perfectly good
        answer and the one a report must print rather than rounding up to the
        nearest level that nearly worked.
        """
        passing = [value for value, gates in self._levels.items() if gates.passed]
        if not passing:
            return None
        return Quantity(max(passing), self.unit)

    def first_failing_level(self) -> Quantity | None:
        failing = [
            value for value, gates in sorted(self._levels.items())
            if gates.status is Status.FAIL
        ]
        return Quantity(min(failing), self.unit) if failing else None

    def knee(self) -> dict[str, Any]:
        """Where behaviour changed: the last pass and the first failure."""
        bound = self.verified_bound()
        first_fail = self.first_failing_level()
        return {
            "verified_bound": bound.as_dict() if bound else None,
            "first_failing_level": first_fail.as_dict() if first_fail else None,
            "unexplored_gap": (
                None
                if bound is None or first_fail is None
                else {
                    "between": [bound.value, first_fail.value],
                    "unit": self.unit.value,
                    "statement": (
                        "no level between these two was measured; nothing in the "
                        "gap may be claimed"
                    ),
                }
            ),
        }

    def extrapolate(self, level: float) -> None:
        """Always raises. Section 6.3 forbids extrapolating above the bound.

        Kept as a named refusal because the request is reasonable-sounding --
        "100k passed and 120k failed, so surely 110k is fine" -- and the answer
        is that nobody measured 110k.
        """
        raise MetrologyError(
            "capacity may not be extrapolated; only levels that were run and "
            "passed every gate may be claimed",
            requested_level=level,
            measured_levels=self.levels,
        )

    def as_dict(self) -> dict[str, Any]:
        bound = self.verified_bound()
        return {
            "unit": self.unit.value,
            "levels": {
                str(value): gates.as_dict() for value, gates in sorted(self._levels.items())
            },
            "verified_bound": bound.as_dict() if bound else None,
            "knee": self.knee(),
            "statement": (
                f"verified bound is >= {bound.value:g} {self.unit.value}"
                if bound
                else "no level passed every gate; no capacity is established"
            ),
        }


def littles_law_population(
    arrival_rate: float, mean_wait_s: float, *, stationary: bool
) -> float | None:
    """``L = lambda * W``, but only where the precondition holds (section 6.3).

    Little's Law needs a stable system with one consistent definition of
    arrivals and no unmeasured loss. In overload the system is non-stationary
    and the formula produces a number that means nothing, so ``stationary=False``
    returns ``None`` instead of a value.
    """
    if arrival_rate < 0 or mean_wait_s < 0:
        raise MetrologyError(
            "arrival rate and wait must be non-negative",
            arrival_rate=arrival_rate,
            mean_wait_s=mean_wait_s,
        )
    if not stationary:
        return None
    return arrival_rate * mean_wait_s


def payload_bandwidth(rate_per_s: float, payload_bytes: int) -> dict[str, Any]:
    """Payload-only bandwidth, with the exclusions spelled out.

    Section 4.2 uses 120k x 160 B as an illustration and then lists everything
    it leaves out. Those exclusions ship with the number here, because the whole
    hazard is someone using 19.2 MB/s to size a production network.
    """
    if rate_per_s < 0 or payload_bytes < 0:
        raise MetrologyError("rate and payload size must be non-negative")
    bytes_per_s = rate_per_s * payload_bytes
    return {
        "payload_bytes_per_s": Quantity(bytes_per_s, Unit.BYTES_PER_S).as_dict(),
        "payload_mib_per_s": bytes_per_s / (1024 * 1024),
        "payload_mb_per_s": bytes_per_s / 1_000_000,
        "excludes": [
            "Kafka protocol and batching overhead",
            "replication traffic",
            "compression effects",
            "quote stream",
            "producer ACK traffic",
            "ClickHouse index, parts and merge write amplification",
            "on-disk compression",
            "query response bytes",
        ],
        "not_for": "network or disk sizing; this is a payload floor, not a budget",
    }
