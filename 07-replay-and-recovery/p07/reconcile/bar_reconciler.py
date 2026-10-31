"""Reconcile materialised bars against the reference OHLC -- ``L5`` in design 3.4.

The comparison is between two independently computed results: the bars P06
materialised from its canonical rows, and the bars
:mod:`p07.oracle.reference_bars` computed from the *expected accepted tick set*.
Deriving the reference from the canonical rows instead would make the check
circular -- it would confirm the aggregation but not the data it aggregated.

``tick_count`` matters as much as the prices. An inflated count is the
fingerprint of a physical duplicate that was never reduced to one logical event
(``INV-09``), and because the reference counts the accepted set it is immune to
that by construction.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from ..dataplane.clickhouse_fake import BarRow
from ..evidence.verdicts import Finding, Reason
from ..oracle.acceptance_oracle import AcceptanceExpectation
from ..oracle.reference_bars import Bar, BarKey, PriceScaleConflict, diff_bars, reference_bars


@dataclass(frozen=True, slots=True)
class BarReconciliation:
    build_id: str
    expected_bars: int
    observed_bars: int
    diff: Mapping[str, list[dict[str, Any]]]
    expected_tick_total: int
    observed_tick_total: int
    findings: tuple[Finding, ...]

    @property
    def verified(self) -> bool:
        return not self.findings

    def as_dict(self, sample: int = 20) -> dict[str, Any]:
        return {
            "build_id": self.build_id,
            "expected_bars": self.expected_bars,
            "observed_bars": self.observed_bars,
            "verified": self.verified,
            "expected_tick_total": self.expected_tick_total,
            "observed_tick_total": self.observed_tick_total,
            "missing_count": len(self.diff["missing"]),
            "missing_sample": self.diff["missing"][:sample],
            "extra_count": len(self.diff["extra"]),
            "extra_sample": self.diff["extra"][:sample],
            "mismatched_count": len(self.diff["mismatched"]),
            "mismatched_sample": self.diff["mismatched"][:sample],
            "findings": [
                {"reason": f.reason, "detail": dict(f.detail)} for f in self.findings[:sample]
            ],
        }


def _observed_map(build_id: str, rows: Iterable[BarRow]) -> tuple[dict[BarKey, Bar], list[str]]:
    observed: dict[BarKey, Bar] = {}
    problems: list[str] = []
    for row in rows:
        if row.build_id != build_id:
            problems.append(f"row from build {row.build_id}")
            continue
        key = BarKey(row.dataset_id, row.symbol, row.price_scale, row.bar_start_ns)
        if key in observed:
            # Two rows for one bucket. In a real MergeTree this is what an
            # unsettled materialised view looks like; either way the bar is not
            # a single answer and must not be published.
            problems.append(f"duplicate bar row for {key}")
            continue
        observed[key] = Bar(
            key=key,
            open_ticks=row.open_ticks,
            high_ticks=row.high_ticks,
            low_ticks=row.low_ticks,
            close_ticks=row.close_ticks,
            tick_count=row.tick_count,
        )
    return observed, problems


def reconcile_bars(
    expectation: AcceptanceExpectation,
    build_id: str,
    rows: Iterable[BarRow],
) -> BarReconciliation:
    """Compare observed bars with the reference bars for the accepted ticks."""
    try:
        expected = reference_bars(expectation.accepted.values())
    except PriceScaleConflict as exc:
        return BarReconciliation(
            build_id=build_id,
            expected_bars=0,
            observed_bars=0,
            diff={"missing": [], "extra": [], "mismatched": []},
            expected_tick_total=0,
            observed_tick_total=0,
            findings=(
                Finding(
                    Reason.PRICE_SCALE_CHANGED,
                    {"dataset_id": exc.dataset_id, "symbol": exc.symbol, "scales": list(exc.scales)},
                ),
            ),
        )

    observed, problems = _observed_map(build_id, rows)
    delta = diff_bars(expected, observed)

    findings: list[Finding] = []
    if delta["missing"] or delta["extra"] or delta["mismatched"]:
        findings.append(
            Finding(
                Reason.BARS_MISMATCH,
                {
                    "missing": len(delta["missing"]),
                    "extra": len(delta["extra"]),
                    "mismatched": len(delta["mismatched"]),
                    "sample": (delta["mismatched"] or delta["missing"] or delta["extra"])[:5],
                },
            )
        )
    for problem in problems:
        findings.append(Finding(Reason.BUILD_DIRTY, {"kind": "bar_table", "detail": problem}))

    return BarReconciliation(
        build_id=build_id,
        expected_bars=len(expected),
        observed_bars=len(observed),
        diff=delta,
        expected_tick_total=sum(bar.tick_count for bar in expected.values()),
        observed_tick_total=sum(bar.tick_count for bar in observed.values()),
        findings=tuple(findings),
    )
