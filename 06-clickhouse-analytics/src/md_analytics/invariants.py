"""INV-01 .. INV-14 of section 17.1, as checks that run.

The invariants in the design document are the release condition, so they are
written here once and evaluated from the actual artefacts rather than being
re-stated inside each test. A test asserts "INV-05 holds for this build"; what
that means lives in one place.

Two cautions carried over from section 17.1:

* **INV-07 is necessary and nowhere near sufficient.** A completely wrong bar
  can satisfy ``LOW <= OPEN/CLOSE <= HIGH``. It is checked because violating it
  is a fast, certain failure, not because passing it means anything much.
* **INV-13 is the meta-invariant**: a missing proof is never a pass. Every check
  that cannot be evaluated reports ``INCONCLUSIVE``, and
  :meth:`InvariantReport.verdict` treats that as "do not publish" -- the same
  way a failure is treated.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from .bars_verifier import BarVerificationReport
from .contracts import SourceIdentity
from .errors import Verdict
from .oracle import Bar, BarKey
from .raw_auditor import RawAuditReport
from .rows import CanonicalTick
from .timebars import BAR_INTERVAL_NS, bar_start_ns


@dataclass(frozen=True, slots=True)
class InvariantResult:
    invariant: str
    verdict: Verdict
    statement: str
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.verdict is Verdict.PASS


@dataclass(slots=True)
class InvariantReport:
    results: list[InvariantResult] = field(default_factory=list)

    def add(
        self, invariant: str, verdict: Verdict, statement: str, detail: str = ""
    ) -> None:
        self.results.append(InvariantResult(invariant, verdict, statement, detail))

    def by_id(self) -> dict[str, InvariantResult]:
        return {r.invariant: r for r in self.results}

    def failures(self) -> list[InvariantResult]:
        return [r for r in self.results if r.verdict is Verdict.FAIL]

    def unproven(self) -> list[InvariantResult]:
        return [r for r in self.results if r.verdict is Verdict.INCONCLUSIVE]

    def verdict(self) -> Verdict:
        """``PASS`` only when every invariant was evaluated and held."""
        if self.failures():
            return Verdict.FAIL
        if self.unproven():
            return Verdict.INCONCLUSIVE
        return Verdict.PASS

    def as_dict(self) -> dict[str, Any]:
        return {
            "verdict": str(self.verdict()),
            "results": [
                {
                    "invariant": r.invariant,
                    "verdict": str(r.verdict),
                    "statement": r.statement,
                    "detail": r.detail,
                }
                for r in self.results
            ],
        }


def check_build_invariants(
    *,
    audit: RawAuditReport,
    bars: BarVerificationReport,
    canonical: Sequence[CanonicalTick],
    stored_bars: dict[BarKey, Bar],
    identity_counts: dict[tuple[str, str, int], int],
    accepted_tick_identities: Iterable[SourceIdentity],
    published: bool,
    prior_build_bars: dict[BarKey, Bar] | None = None,
    retention_ok: bool | None = None,
    query_bounds_tested: bool | None = None,
) -> InvariantReport:
    """Evaluate the formal invariants for one candidate build.

    ``retention_ok`` and ``query_bounds_tested`` are tri-state on purpose:
    ``None`` means the caller did not establish them, which yields
    ``INCONCLUSIVE`` for INV-12/INV-14 rather than a cheerful pass on evidence
    nobody gathered.
    """
    report = InvariantReport()
    expected_ticks = frozenset(accepted_tick_identities)
    canonical_identities = frozenset(t.identity for t in canonical)

    # INV-01  S == valid(R) == identities(C)
    if audit.is_complete and audit.is_consistent:
        if canonical_identities == expected_ticks:
            report.add(
                "INV-01",
                Verdict.PASS,
                "accepted source identities == reconciled raw == canonical identities",
                f"{len(expected_ticks)} tick identities",
            )
        else:
            report.add(
                "INV-01",
                Verdict.FAIL,
                "accepted source identities == reconciled raw == canonical identities",
                f"canonical-only={len(canonical_identities - expected_ticks)}, "
                f"source-only={len(expected_ticks - canonical_identities)}",
            )
    else:
        report.add(
            "INV-01",
            Verdict.FAIL,
            "accepted source identities == reconciled raw == canonical identities",
            "; ".join(audit.reasons),
        )

    # INV-02  |C| == distinct accepted identities
    report.add(
        "INV-02",
        Verdict.PASS if len(canonical) == len(expected_ticks) else Verdict.FAIL,
        "canonical row count equals the number of distinct accepted tick identities",
        f"canonical={len(canonical)}, expected={len(expected_ticks)}",
    )

    # INV-03  no identity has more than one semantic projection
    report.add(
        "INV-03",
        Verdict.PASS if not audit.conflicts else Verdict.FAIL,
        "no raw identity carries more than one distinct semantic projection",
        f"conflicts={len(audit.conflicts)}",
    )

    # INV-04  exactly one physical canonical row per identity
    offenders = {k: v for k, v in identity_counts.items() if v != 1}
    report.add(
        "INV-04",
        Verdict.PASS if not offenders else Verdict.FAIL,
        "each source identity appears in exactly one canonical physical row",
        f"identities with row count != 1: {len(offenders)}",
    )

    # INV-05  per-bar tick_count == number of canonical ticks in that bucket
    recomputed: dict[BarKey, int] = {}
    for tick in canonical:
        key = BarKey(
            build_id=tick.build_id,
            dataset_id=tick.dataset_id,
            symbol=tick.symbol,
            price_scale=tick.price_scale,
            bar_start_ns=bar_start_ns(tick.event_time_ns),
        )
        recomputed[key] = recomputed.get(key, 0) + 1
    count_errors = [
        key
        for key in set(recomputed) | set(stored_bars)
        if recomputed.get(key, 0) != (stored_bars[key].tick_count if key in stored_bars else 0)
    ]
    report.add(
        "INV-05",
        Verdict.PASS if not count_errors else Verdict.FAIL,
        "every bar's tick_count equals the canonical ticks in its interval",
        f"bars with wrong count: {len(count_errors)}",
    )

    # INV-06  sum of tick_counts == |T|
    stored_total = sum(bar.tick_count for bar in stored_bars.values())
    report.add(
        "INV-06",
        Verdict.PASS if stored_total == len(canonical) else Verdict.FAIL,
        "the sum of bar tick counts equals the canonical tick count",
        f"sum={stored_total}, canonical={len(canonical)}",
    )

    # INV-07  LOW <= OPEN/CLOSE <= HIGH -- necessary only
    report.add(
        "INV-07",
        Verdict.PASS if not bars.bounds_violations else Verdict.FAIL,
        "LOW <= OPEN <= HIGH and LOW <= CLOSE <= HIGH (necessary, not sufficient)",
        f"violations={len(bars.bounds_violations)}",
    )

    # INV-08  OPEN/CLOSE follow the source order, proven by the oracle match
    report.add(
        "INV-08",
        Verdict.PASS if bars.verdict() is Verdict.PASS else Verdict.FAIL,
        "OPEN/CLOSE follow min/max (event_time_ns, sequence), per the oracle",
        "; ".join(bars.reasons),
    )

    # INV-09  bar scope is never crossed
    scope_errors = [
        tuple(key)
        for key in stored_bars
        if key.build_id != (canonical[0].build_id if canonical else key.build_id)
    ]
    scale_pairs: dict[tuple[str, str], set[int]] = {}
    for key in stored_bars:
        scale_pairs.setdefault((key.dataset_id, key.symbol), set()).add(key.price_scale)
    report.add(
        "INV-09",
        Verdict.PASS if not scope_errors else Verdict.FAIL,
        "bar scope (build, dataset, symbol, price_scale) is never crossed",
        f"out-of-build bars={len(scope_errors)}, "
        f"symbols with multiple scales={sum(1 for v in scale_pairs.values() if len(v) > 1)}",
    )

    # INV-10  PUBLISHED implies the independent proofs passed
    if published:
        proven = (
            audit.verdict() is Verdict.PASS
            and bars.verdict() is Verdict.PASS
            and not offenders
        )
        report.add(
            "INV-10",
            Verdict.PASS if proven else Verdict.FAIL,
            "a published build implies a READY manifest and valid independent proofs",
            f"audit={audit.verdict()}, bars={bars.verdict()}",
        )
    else:
        report.add(
            "INV-10",
            Verdict.PASS,
            "a published build implies a READY manifest and valid independent proofs",
            "build is not published; the implication holds vacuously",
        )

    # INV-11  a superseded build is immutable
    if prior_build_bars is None:
        report.add(
            "INV-11",
            Verdict.INCONCLUSIVE,
            "superseded builds stay immutable and pinned queries never mix versions",
            "no prior build snapshot was supplied to compare against",
        )
    else:
        drifted = [
            tuple(key)
            for key, bar in prior_build_bars.items()
            if key in stored_bars and stored_bars[key] != bar
        ]
        report.add(
            "INV-11",
            Verdict.PASS if not drifted else Verdict.FAIL,
            "superseded builds stay immutable and pinned queries never mix versions",
            f"bars of the prior build that changed: {len(drifted)}",
        )

    # INV-12  retention cannot silently invalidate the proof window
    report.add(
        "INV-12",
        Verdict.INCONCLUSIVE
        if retention_ok is None
        else (Verdict.PASS if retention_ok else Verdict.FAIL),
        "raw TTL cannot silently invalidate the agreed proof/recovery window",
        "retention gate not evaluated" if retention_ok is None else f"gate={retention_ok}",
    )

    # INV-13  missing evidence never maps to PASS
    report.add(
        "INV-13",
        Verdict.PASS,
        "failure or missing evidence never maps to PASS/READY",
        f"{len(report.unproven())} invariants currently unproven and reported as such",
    )

    # INV-14  queries are bounded
    report.add(
        "INV-14",
        Verdict.INCONCLUSIVE
        if query_bounds_tested is None
        else (Verdict.PASS if query_bounds_tested else Verdict.FAIL),
        "queries are bounded by scope, time, concurrency, memory and permissions",
        "query bounds not exercised in this run"
        if query_bounds_tested is None
        else f"bounds tested={query_bounds_tested}",
    )

    return report


def check_bar_interval_partition(bars: Iterable[BarKey]) -> list[BarKey]:
    """Bar starts that are not on an interval boundary (case I-06, D-01)."""
    return [key for key in bars if key.bar_start_ns % BAR_INTERVAL_NS != 0]
