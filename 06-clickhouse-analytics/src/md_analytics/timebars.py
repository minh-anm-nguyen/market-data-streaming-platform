"""Minute-bucket arithmetic and query range validation.

Section 3.3 and 4.3/4.4. The bucket is computed from integer nanoseconds and
never from a millisecond or ``DateTime64(3)`` intermediate: downcasting first
would both mis-bucket an event one nanosecond before a minute edge and destroy
the ``(event_time_ns, sequence)`` tie-break for events inside the same
millisecond (case D-01..D-03, I-08).

Range semantics are ``[start, end)`` everywhere -- closed-open -- so two adjacent
windows partition their union exactly once (case D-12).
"""

from __future__ import annotations

from typing import Final, NamedTuple

from .contracts import NS_PER_MINUTE, require_event_time_ns, require_int64
from .errors import QueryRejected

BAR_INTERVAL_NS: Final = NS_PER_MINUTE


def bar_start_ns(event_time_ns: int, *, interval_ns: int = BAR_INTERVAL_NS) -> int:
    """Floor a nanosecond timestamp to its bucket start.

    Python's ``//`` floors toward negative infinity, which is the behaviour a
    bucket needs; C-style truncation would fold the instants just before the
    epoch into the bucket *after* them.
    """
    if interval_ns <= 0:
        raise ValueError(f"interval_ns must be positive, got {interval_ns}")
    return (event_time_ns // interval_ns) * interval_ns


def bar_end_ns(start_ns: int, *, interval_ns: int = BAR_INTERVAL_NS) -> int:
    return start_ns + interval_ns


def is_minute_aligned(ns: int, *, interval_ns: int = BAR_INTERVAL_NS) -> bool:
    return ns % interval_ns == 0


class TimeRange(NamedTuple):
    """A validated closed-open nanosecond range."""

    start_ns: int
    end_ns: int

    @property
    def duration_ns(self) -> int:
        return self.end_ns - self.start_ns

    def contains(self, ns: int) -> bool:
        return self.start_ns <= ns < self.end_ns

    @property
    def minutes(self) -> int:
        return self.duration_ns // BAR_INTERVAL_NS


def validate_range(
    start_ns: object,
    end_ns: object,
    *,
    require_alignment: bool,
    max_window_minutes: int | None,
    interval_ns: int = BAR_INTERVAL_NS,
) -> TimeRange:
    """Validate a query window, or raise :class:`QueryRejected`.

    ``require_alignment`` exists because a bar query whose bounds fall mid-minute
    has no single honest answer: the design (section 4.3) says the service must
    either demand aligned bounds or define the semantics, and silently comparing
    an unaligned bound against ``bar_start_ns`` would return whole bars whose
    ticks lie partly outside the window the caller asked for.
    """
    start = require_event_time_ns(_as_int(start_ns, "start_ns"))
    end = require_event_time_ns(_as_int(end_ns, "end_ns"))
    if start >= end:
        raise QueryRejected(
            f"empty or inverted range: start_ns={start} must be < end_ns={end}",
            code="RANGE_NOT_ASCENDING",
            start_ns=start,
            end_ns=end,
        )
    if require_alignment and not (
        is_minute_aligned(start, interval_ns=interval_ns)
        and is_minute_aligned(end, interval_ns=interval_ns)
    ):
        raise QueryRejected(
            "bar range bounds must be aligned to the bar interval",
            code="RANGE_NOT_ALIGNED",
            start_ns=start,
            end_ns=end,
            interval_ns=interval_ns,
        )
    if max_window_minutes is not None:
        window_minutes = (end - start + interval_ns - 1) // interval_ns
        if window_minutes > max_window_minutes:
            raise QueryRejected(
                f"range spans {window_minutes} minutes, limit is {max_window_minutes}",
                code="RANGE_TOO_WIDE",
                window_minutes=window_minutes,
                max_window_minutes=max_window_minutes,
            )
    return TimeRange(start, end)


def _as_int(value: object, field: str) -> int:
    """Accept an int, or a decimal *string* of one, and nothing else.

    A JavaScript client cannot carry ``1700000000000000001`` through a JSON
    number without losing the last digits, so the wire form for nanoseconds is a
    string and the parse is exact (case D-13, B-12). A float is refused outright
    rather than rounded, because by the time a float arrives the low digits are
    already gone.
    """
    if isinstance(value, bool):
        raise QueryRejected(f"{field} must be an integer", code="PARAM_NOT_INTEGER", field=field)
    if isinstance(value, int):
        return require_int64(value, field)
    if isinstance(value, str):
        text = value.strip()
        if not text or not (text.lstrip("-").isdigit() and text.count("-") <= 1):
            raise QueryRejected(
                f"{field}={value!r} is not a decimal integer string",
                code="PARAM_NOT_INTEGER",
                field=field,
            )
        return require_int64(int(text), field)
    raise QueryRejected(
        f"{field} must be an int or decimal string, got {type(value).__name__}",
        code="PARAM_NOT_INTEGER",
        field=field,
    )
