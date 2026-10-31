"""Unit-tagged quantities (INV-12, section 13).

Section 1.2 of PROJECT_08 exists because of one arithmetic mistake: reading
``120k events/s`` as ``120k ticks/s``. With a 4:1 tick:quote mix those are
120k and 150k, and a report that mixes them overstates capacity by 25% while
looking internally consistent. Section 13 adds two more traps: ``bit/s`` is not
``byte/s``, and ``MiB`` is not ``MB``.

So a rate in P08 is never a bare float. :class:`Quantity` carries its unit and
refuses to add, compare or convert across unit families. The cost is noise at
every call site; the benefit is that the 120k/150k mistake becomes a
:class:`~p08_bench.errors.UnitError` instead of a published number.

Conversion is allowed only where a *measured* ratio exists, which is why
:meth:`Quantity.to_events_per_s` demands the tick:quote composition instead of
assuming one.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from .errors import UnitError

#: Binary prefixes (section 13: report MiB/GiB separately from MB/GB).
KIB = 1024
MIB = 1024 * KIB
GIB = 1024 * MIB

#: Decimal prefixes. Deliberately different names, never interchangeable.
KB = 1000
MB = 1000 * KB
GB = 1000 * MB


class Unit(str, Enum):
    """Every unit P08 is allowed to print.

    The rate units are separate members rather than one RATE member with a
    dimension field: ``TICKS_PER_S`` and ``EVENTS_PER_S`` share a dimension and
    must still never be interchanged (INV-12).
    """

    TICKS_PER_S = "ticks/s"
    QUOTES_PER_S = "quotes/s"
    EVENTS_PER_S = "events/s"
    RECORDS_PER_S = "records/s"
    QUERIES_PER_S = "queries/s"
    BYTES_PER_S = "bytes/s"
    BITS_PER_S = "bits/s"

    TICKS = "ticks"
    EVENTS = "events"
    RECORDS = "records"
    QUERIES = "queries"
    BYTES = "bytes"

    SECONDS = "s"
    MILLISECONDS = "ms"
    NANOSECONDS = "ns"

    RATIO = "ratio"
    COUNT = "count"


#: Units that express "something per second". Used only to reject a slope in
#: records/s being compared against one in ticks/s; it never licenses a cast.
_RATE_UNITS = frozenset(
    {
        Unit.TICKS_PER_S,
        Unit.QUOTES_PER_S,
        Unit.EVENTS_PER_S,
        Unit.RECORDS_PER_S,
        Unit.QUERIES_PER_S,
        Unit.BYTES_PER_S,
        Unit.BITS_PER_S,
    }
)

#: Time units and their factor to seconds. Durations are the one family where
#: conversion is unambiguous, so it is permitted.
_TIME_TO_SECONDS = {
    Unit.SECONDS: 1.0,
    Unit.MILLISECONDS: 1e-3,
    Unit.NANOSECONDS: 1e-9,
}


@dataclass(frozen=True)
class Quantity:
    """A number that knows what it measures.

    Frozen on purpose: a quantity that could be mutated in place would let a
    caller relabel ``events/s`` as ``ticks/s`` after the fact, which is exactly
    the substitution section 1.2 forbids.
    """

    value: float
    unit: Unit

    def __post_init__(self) -> None:
        if self.value != self.value:  # NaN
            raise UnitError("quantity value may not be NaN", unit=self.unit.value)

    # -- combination ------------------------------------------------------
    def _require_same_unit(self, other: "Quantity", op: str) -> None:
        if self.unit is not other.unit:
            raise UnitError(
                f"cannot {op} {self.unit.value} and {other.unit.value}; "
                "these are different units and P08 never coerces them",
                left=self.unit.value,
                right=other.unit.value,
                operation=op,
            )

    def __add__(self, other: "Quantity") -> "Quantity":
        self._require_same_unit(other, "add")
        return Quantity(self.value + other.value, self.unit)

    def __sub__(self, other: "Quantity") -> "Quantity":
        self._require_same_unit(other, "subtract")
        return Quantity(self.value - other.value, self.unit)

    def ratio_to(self, other: "Quantity") -> float:
        """Dimensionless ratio of two quantities in the *same* unit."""
        self._require_same_unit(other, "divide")
        if other.value == 0:
            raise UnitError("ratio denominator is zero", unit=self.unit.value)
        return self.value / other.value

    def scaled(self, factor: float) -> "Quantity":
        """Multiply by a dimensionless factor; the unit is preserved."""
        return Quantity(self.value * factor, self.unit)

    # -- comparison -------------------------------------------------------
    def exceeds(self, other: "Quantity") -> bool:
        self._require_same_unit(other, "compare")
        return self.value > other.value

    def within(self, budget: "Quantity") -> bool:
        """True when this quantity is at or below ``budget`` (same unit)."""
        self._require_same_unit(budget, "compare")
        return self.value <= budget.value

    # -- the conversions that are allowed --------------------------------
    def to_seconds(self) -> float:
        factor = _TIME_TO_SECONDS.get(self.unit)
        if factor is None:
            raise UnitError(f"{self.unit.value} is not a duration", unit=self.unit.value)
        return self.value * factor

    def to_milliseconds(self) -> float:
        return self.to_seconds() * 1e3

    def to_events_per_s(self, *, quotes_per_tick: float) -> "Quantity":
        """Convert a tick rate to a total event rate using a **measured** mix.

        ``quotes_per_tick`` has to come from the run composition counters.
        The 4:1 example in section 1.2 illustrates the arithmetic; it is not a
        ratio anyone may assume, so there is no default value here.
        """
        if self.unit is not Unit.TICKS_PER_S:
            raise UnitError(
                "only a tick rate converts to an event rate", unit=self.unit.value
            )
        if quotes_per_tick < 0:
            raise UnitError(
                "quotes_per_tick may not be negative", quotes_per_tick=quotes_per_tick
            )
        return Quantity(self.value * (1.0 + quotes_per_tick), Unit.EVENTS_PER_S)

    def bits_to_bytes(self) -> "Quantity":
        if self.unit is not Unit.BITS_PER_S:
            raise UnitError("not a bit rate", unit=self.unit.value)
        return Quantity(self.value / 8.0, Unit.BYTES_PER_S)

    @property
    def is_rate(self) -> bool:
        return self.unit in _RATE_UNITS

    def as_dict(self) -> dict[str, Any]:
        return {"value": self.value, "unit": self.unit.value}

    def __str__(self) -> str:
        return f"{self.value:g} {self.unit.value}"


def ticks_per_s(value: float) -> Quantity:
    return Quantity(value, Unit.TICKS_PER_S)


def events_per_s(value: float) -> Quantity:
    return Quantity(value, Unit.EVENTS_PER_S)


def records_per_s(value: float) -> Quantity:
    return Quantity(value, Unit.RECORDS_PER_S)


def bytes_per_s(value: float) -> Quantity:
    return Quantity(value, Unit.BYTES_PER_S)


def milliseconds(value: float) -> Quantity:
    return Quantity(value, Unit.MILLISECONDS)


def seconds(value: float) -> Quantity:
    return Quantity(value, Unit.SECONDS)


def rate(count: float, elapsed_s: float, unit: Unit) -> Quantity:
    """Build a rate from a count and a **measured** elapsed time.

    Section 4.2 writes every rate as ``count / measured_seconds``. Passing a
    nominal window length instead of the measured one is the quiet way to turn a
    29-second window into a 30-second claim, so a non-positive elapsed time is
    an error rather than a guard that returns 0.
    """
    if unit not in _RATE_UNITS:
        raise UnitError(f"{unit.value} is not a rate unit", unit=unit.value)
    if elapsed_s <= 0:
        raise UnitError("elapsed seconds must be > 0 to form a rate", elapsed_s=elapsed_s)
    return Quantity(count / elapsed_s, unit)
