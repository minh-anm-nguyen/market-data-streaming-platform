"""Compare stored bars against the independent oracle, field by field.

Section 5.1 S4 and matrix groups H/I. This is the step that turns "the INSERT
succeeded" into evidence, and it is deliberately pedantic:

* Comparison is **per field, per bar key**, on integers. A bar with the right
  count and the wrong OPEN is a failure, and so is a bar that exists on one side
  only -- an extra bar is as wrong as a missing one (case H-01, E2E-21).
* The oracle is read from the **canonical rows**, through
  :mod:`md_analytics.oracle`, which shares no code with the materialized view.
* ``LOW <= OPEN/CLOSE <= HIGH`` is checked but never reported as sufficient:
  section 17.1 points out that a thoroughly wrong bar can satisfy it.
* The number of **physical state rows** is reported alongside the bar count. If
  states outnumber bars, the ``*Merge`` read path is load-bearing, and that fact
  belongs in the evidence rather than in a reviewer's memory (case I-01).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from .errors import Verdict
from .oracle import Bar, BarKey, reference_bars, total_tick_count
from .rows import CanonicalTick
from .store.base import AnalyticsStore

_FIELDS = ("open_ticks", "high_ticks", "low_ticks", "close_ticks", "tick_count")


@dataclass(frozen=True, slots=True)
class BarMismatch:
    bar_key: tuple[Any, ...]
    field_name: str
    expected: int
    actual: int


@dataclass(slots=True)
class BarVerificationReport:
    """Field-level comparison of stored bars against the reference bars."""

    build_id: str
    reference_bars: int = 0
    stored_bars: int = 0
    state_rows: int = 0
    canonical_rows: int = 0
    reference_tick_total: int = 0
    stored_tick_total: int = 0
    missing_bars: list[tuple[Any, ...]] = field(default_factory=list)
    extra_bars: list[tuple[Any, ...]] = field(default_factory=list)
    mismatches: list[BarMismatch] = field(default_factory=list)
    bounds_violations: list[tuple[Any, ...]] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)

    @property
    def mismatch_count(self) -> int:
        return len(self.mismatches) + len(self.missing_bars) + len(self.extra_bars)

    def verdict(self) -> Verdict:
        if self.mismatch_count or self.bounds_violations:
            return Verdict.FAIL
        if self.reference_tick_total != self.stored_tick_total:
            return Verdict.FAIL
        if self.canonical_rows != self.reference_tick_total:
            # INV-06 over the whole build: every canonical tick lands in exactly
            # one bar, so the per-bar counts must sum to the row count.
            return Verdict.FAIL
        return Verdict.PASS

    def counters(self) -> dict[str, int]:
        return {
            "bar_count": self.stored_bars,
            "reference_bar_count": self.reference_bars,
            "bar_state_rows": self.state_rows,
            "canonical_rows": self.canonical_rows,
            "bar_tick_count": self.stored_tick_total,
            "reference_tick_count": self.reference_tick_total,
            "bar_reference_mismatch_count": self.mismatch_count,
            "bars_missing": len(self.missing_bars),
            "bars_extra": len(self.extra_bars),
            "bar_bounds_violations": len(self.bounds_violations),
        }

    def as_dict(self, *, sample_limit: int = 20) -> dict[str, Any]:
        return {
            "build_id": self.build_id,
            "verdict": str(self.verdict()),
            "counters": self.counters(),
            "reasons": list(self.reasons),
            "samples": {
                "mismatches": [
                    {
                        "bar_key": list(m.bar_key),
                        "field": m.field_name,
                        "expected": m.expected,
                        "actual": m.actual,
                    }
                    for m in self.mismatches[:sample_limit]
                ],
                "missing_bars": [list(k) for k in self.missing_bars[:sample_limit]],
                "extra_bars": [list(k) for k in self.extra_bars[:sample_limit]],
                "bounds_violations": [
                    list(k) for k in self.bounds_violations[:sample_limit]
                ],
            },
        }


def compare_bars(
    reference: dict[BarKey, Bar],
    stored: dict[BarKey, Bar],
    *,
    build_id: str,
    canonical_rows: int,
    state_rows: int,
) -> BarVerificationReport:
    """Pure comparison of two bar sets. No store, no oracle, no I/O."""
    report = BarVerificationReport(
        build_id=build_id,
        reference_bars=len(reference),
        stored_bars=len(stored),
        state_rows=state_rows,
        canonical_rows=canonical_rows,
        reference_tick_total=total_tick_count(reference),
        stored_tick_total=total_tick_count(stored),
    )

    for key in sorted(set(reference) - set(stored)):
        report.missing_bars.append(tuple(key))
    for key in sorted(set(stored) - set(reference)):
        report.extra_bars.append(tuple(key))

    for key in sorted(set(reference) & set(stored)):
        expected, actual = reference[key], stored[key]
        for name in _FIELDS:
            exp, act = getattr(expected, name), getattr(actual, name)
            if exp != act:
                report.mismatches.append(BarMismatch(tuple(key), name, exp, act))

    for key, bar in sorted(stored.items()):
        if not (
            bar.low_ticks <= bar.open_ticks <= bar.high_ticks
            and bar.low_ticks <= bar.close_ticks <= bar.high_ticks
        ):
            report.bounds_violations.append(tuple(key))

    if report.missing_bars:
        report.reasons.append(f"{len(report.missing_bars)} reference bars absent from the store")
    if report.extra_bars:
        report.reasons.append(f"{len(report.extra_bars)} stored bars absent from the reference")
    if report.mismatches:
        report.reasons.append(f"{len(report.mismatches)} field mismatches")
    if report.bounds_violations:
        report.reasons.append(
            f"{len(report.bounds_violations)} bars violate LOW <= OPEN/CLOSE <= HIGH"
        )
    if report.reference_tick_total != report.stored_tick_total:
        report.reasons.append(
            f"tick totals differ: reference {report.reference_tick_total}, "
            f"stored {report.stored_tick_total}"
        )
    if report.canonical_rows != report.reference_tick_total:
        report.reasons.append(
            f"{report.canonical_rows} canonical rows but bars account for "
            f"{report.reference_tick_total} ticks"
        )
    if not report.reasons:
        report.reasons.append(
            f"{report.stored_bars} bars match the oracle exactly "
            f"({report.state_rows} physical state rows merged at read time)"
        )
    return report


def verify_build_bars(
    store: AnalyticsStore,
    *,
    build_id: str,
    canonical: Sequence[CanonicalTick] | None = None,
) -> BarVerificationReport:
    """Verify one build's bars against bars recomputed from its canonical rows.

    ``canonical`` may be supplied by the caller when it already holds the staged
    rows -- which is *stronger* evidence, not weaker: the oracle then runs on the
    rows the builder intended to write, so a row that failed to land shows up as
    a mismatch instead of disappearing from both sides of the comparison.
    """
    rows = list(canonical) if canonical is not None else store.all_canonical(build_id=build_id)
    reference = reference_bars(rows)
    stored = store.all_bars_1m(build_id=build_id)
    return compare_bars(
        reference,
        stored,
        build_id=build_id,
        canonical_rows=len(rows),
        state_rows=store.bar_state_row_count(build_id=build_id),
    )
