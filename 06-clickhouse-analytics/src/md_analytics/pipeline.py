"""The correctness-first MVP pipeline, start to finish.

Section 2: ingest a finite replay, cut the snapshot, reconcile raw, stage an
immutable canonical candidate, derive 1m bars, verify them against the
independent oracle, and publish a ``build_id`` only if every proof holds.

This module is the one place the order of those steps is written down, which
matters because the order *is* the design: verifying before publishing is what
makes a published build trustworthy, and no amount of care in a later step can
substitute for an audit that was skipped.

:func:`run_build` returns rather than raises for an expected refusal. A ``NO_GO``
is a legitimate outcome -- often the behaviour a test is asserting -- and it
carries the reports that explain it. It raises only when a caller has asked for
something incoherent, such as publishing without a catalog.
"""

from __future__ import annotations

import os
import platform
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from .bars_verifier import BarVerificationReport, verify_build_bars
from .build_catalog import NO_ACTIVE_BUILD, BuildCatalog, BuildManifest
from .canonical_builder import (
    BUILDER_VERSION,
    BuildState,
    CanonicalBuildResult,
    build_canonical,
    policy_digest,
)
from .errors import BuildDirtyError, PublishRejected, StoreError, Verdict
from .fixtures import Corpus
from .invariants import InvariantReport, check_build_invariants
from .oracle import Bar, BarKey
from .raw_auditor import RawAuditReport, audit_raw
from .rows import CanonicalTick
from .schema_preflight import expected_raw_digest
from .sqlbank import SqlBank
from .store.base import AnalyticsStore


def environment_fingerprint() -> dict[str, str]:
    """Host facts every evidence record needs (NFR-12).

    Deliberately excludes anything that would make two runs of the same fixture
    look different for no reason -- no timestamps, no process ids, no paths.
    """
    return {
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": str(os.cpu_count() or 0),
        "max_int_bits": str(sys.maxsize.bit_length() + 1),
    }


@dataclass(slots=True)
class PipelineResult:
    """Everything one build attempt produced, including why it stopped."""

    dataset_id: str
    state: BuildState
    verdict: Verdict
    audit: RawAuditReport
    build: CanonicalBuildResult | None = None
    bars: BarVerificationReport | None = None
    invariants: InvariantReport | None = None
    manifest: BuildManifest | None = None
    pointer: dict[str, Any] | None = None
    canonical: tuple[CanonicalTick, ...] = ()
    stored_bars: dict[BarKey, Bar] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)

    @property
    def build_id(self) -> str | None:
        return None if self.build is None else self.build.physical_build_id

    @property
    def published(self) -> bool:
        return self.pointer is not None

    def counters(self) -> dict[str, int]:
        out = dict(self.audit.counters())
        if self.bars is not None:
            out.update(self.bars.counters())
        return out

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "state": str(self.state),
            "verdict": str(self.verdict),
            "build_id": self.build_id,
            "published": self.published,
            "reasons": list(self.reasons),
            "counters": self.counters(),
            "audit": self.audit.as_dict(),
            "bars": None if self.bars is None else self.bars.as_dict(),
            "invariants": None if self.invariants is None else self.invariants.as_dict(),
            "pointer": self.pointer,
        }


def run_build(
    store: AnalyticsStore,
    corpus: Corpus,
    *,
    catalog: BuildCatalog | None = None,
    schema_sha256: str | None = None,
    sql_bank: SqlBank | None = None,
    block_size: int = 5_000,
    attempt: int = 1,
    publish: bool = True,
    expected_current: str | None = NO_ACTIVE_BUILD,
    prior_build_bars: dict[BarKey, Bar] | None = None,
    retention_ok: bool | None = None,
    query_bounds_tested: bool | None = None,
    now_ns: int | None = None,
) -> PipelineResult:
    """Run audit -> stage -> verify -> publish for one sealed corpus."""
    if publish and catalog is None:
        raise ValueError("publish=True requires a catalog")

    schema = schema_sha256 if schema_sha256 is not None else expected_raw_digest()
    bank = sql_bank if sql_bank is not None else SqlBank()
    audit = audit_raw(corpus.raw_rows, manifest=corpus.manifest, cut=corpus.cut)

    result = PipelineResult(
        dataset_id=corpus.dataset_id,
        state=BuildState.RAW_AUDITING,
        verdict=Verdict.NOT_RUN,
        audit=audit,
    )

    if audit.verdict() is not Verdict.PASS:
        result.state = BuildState.NO_GO
        result.verdict = Verdict.NO_GO
        result.reasons = ["raw audit failed", *audit.reasons]
        return result

    # ------------------------------------------------------------- stage rows
    try:
        build = build_canonical(
            store,
            corpus.raw_rows,
            audit=audit,
            cut=corpus.cut,
            schema_sha256=schema,
            attempt=attempt,
            block_size=block_size,
        )
    except BuildDirtyError as exc:
        result.state = BuildState.DIRTY
        result.verdict = Verdict.NO_GO
        result.reasons = [f"canonical staging left the build dirty: {exc}"]
        return result
    except StoreError as exc:
        result.state = BuildState.DIRTY
        result.verdict = Verdict.NO_GO
        result.reasons = [f"store error during staging: {exc}"]
        return result

    result.build = build
    result.state = BuildState.BAR_VERIFYING

    # ------------------------------------------------------------ verify bars
    physical = build.physical_build_id
    canonical = store.all_canonical(build_id=physical)
    result.canonical = tuple(canonical)
    bars_report = verify_build_bars(store, build_id=physical, canonical=canonical)
    result.bars = bars_report
    result.stored_bars = store.all_bars_1m(build_id=physical)

    invariants = check_build_invariants(
        audit=audit,
        bars=bars_report,
        canonical=canonical,
        stored_bars=result.stored_bars,
        identity_counts=store.canonical_identity_counts(build_id=physical),
        accepted_tick_identities=audit.clean_tick_identities,
        published=False,
        prior_build_bars=prior_build_bars,
        retention_ok=retention_ok,
        query_bounds_tested=query_bounds_tested,
    )
    result.invariants = invariants

    if bars_report.verdict() is not Verdict.PASS or invariants.failures():
        result.state = BuildState.DIRTY
        result.verdict = Verdict.FAIL
        result.reasons = [
            "bar verification or an invariant failed",
            *bars_report.reasons,
            *[f"{r.invariant}: {r.detail}" for r in invariants.failures()],
        ]
        return result

    # ---------------------------------------------------------------- publish
    info = store.info()
    manifest = BuildManifest(
        build_id=physical,
        dataset_id=corpus.dataset_id,
        state=BuildState.READY,
        manifest_sha256=corpus.cut.manifest_sha256,
        cut_digest=corpus.cut.cut_digest(),
        schema_sha256=schema,
        sql_bank_digest=bank.bank_digest(),
        canonical_policy_digest=policy_digest(),
        builder_version=BUILDER_VERSION,
        canonical_count=len(canonical),
        bar_count=bars_report.stored_bars,
        bar_tick_count=bars_report.stored_tick_total,
        raw_physical_rows=audit.physical_rows,
        raw_duplicate_row_count=audit.duplicate_row_count,
        audit_verdict=audit.verdict(),
        bars_verdict=bars_report.verdict(),
        evidence_class=info.evidence_class,
        server_version=info.server_version,
        published_at_ns=now_ns if now_ns is not None else time.time_ns(),
        environment=environment_fingerprint(),
        counters={**audit.counters(), **bars_report.counters()},
        notes=(
            f"logical_build_id={build.build_id}",
            f"attempt={build.attempt}",
            f"backend={info.backend}",
        ),
    )
    result.manifest = manifest

    if not publish:
        result.state = BuildState.BAR_VERIFYING
        result.verdict = Verdict.PASS
        result.reasons = ["verified; publication not requested"]
        return result

    assert catalog is not None  # guarded at entry
    try:
        result.pointer = catalog.publish(manifest, expected_current=expected_current)
    except PublishRejected as exc:
        result.state = BuildState.DIRTY
        result.verdict = Verdict.NO_GO
        result.reasons = [f"publication refused: {exc}"]
        return result

    result.state = BuildState.READY
    result.verdict = Verdict.PASS
    result.reasons = [
        f"published {physical}: {len(canonical)} canonical ticks, "
        f"{bars_report.stored_bars} bars, {bars_report.state_rows} state rows"
    ]

    # Re-check the invariants now that the build *is* published, so INV-10 is
    # evaluated against the published state rather than vacuously.
    result.invariants = check_build_invariants(
        audit=audit,
        bars=bars_report,
        canonical=canonical,
        stored_bars=result.stored_bars,
        identity_counts=store.canonical_identity_counts(build_id=physical),
        accepted_tick_identities=audit.clean_tick_identities,
        published=True,
        prior_build_bars=prior_build_bars,
        retention_ok=retention_ok,
        query_bounds_tested=query_bounds_tested,
    )
    return result


def run_and_require_ready(
    store: AnalyticsStore,
    corpus: Corpus,
    *,
    catalog: BuildCatalog,
    expected_current: str | None = NO_ACTIVE_BUILD,
    **kwargs: Any,
) -> PipelineResult:
    """:func:`run_build`, asserting it reached READY.

    For the many tests whose subject is something downstream of a good build:
    without this, a fixture mistake upstream would surface as a confusing
    assertion about bars instead of a clear one about the build.
    """
    result = run_build(
        store, corpus, catalog=catalog, expected_current=expected_current, **kwargs
    )
    if result.state is not BuildState.READY:
        raise AssertionError(
            f"expected a READY build, got {result.state}/{result.verdict}: "
            f"{'; '.join(result.reasons)}"
        )
    return result


def bars_for_symbol(
    result: PipelineResult, symbol: str, *, dataset_id: str | None = None
) -> dict[int, Bar]:
    """Bars of one symbol keyed by ``bar_start_ns``, for readable assertions."""
    dataset = dataset_id if dataset_id is not None else result.dataset_id
    return {
        key.bar_start_ns: bar
        for key, bar in result.stored_bars.items()
        if key.symbol == symbol and key.dataset_id == dataset
    }


def canonical_sequences(result: PipelineResult, symbol: str) -> list[int]:
    return sorted(t.sequence for t in result.canonical if t.symbol == symbol)


def assert_corpus_shape(corpus: Corpus, *, expected_accepted: int, expected_raw: int) -> None:
    """Guard a fixture's own arithmetic before it is used as an oracle.

    Group AD is about oracle integrity: if a fixture quietly contains 999 rows
    where the test believes 1000, every assertion downstream is measuring the
    wrong thing and passing.
    """
    accepted = len(corpus.manifest.accepted)
    raw = len(corpus.raw_rows)
    if accepted != expected_accepted or raw != expected_raw:
        raise AssertionError(
            f"fixture shape drifted: accepted={accepted} (expected "
            f"{expected_accepted}), raw_rows={raw} (expected {expected_raw})"
        )


def sealed_tick_count(corpus: Corpus) -> int:
    return len(corpus.ticks_in_scope())


def unique_tick_identities(rows: Sequence[Any]) -> int:
    return len({row.identity for row in rows})
