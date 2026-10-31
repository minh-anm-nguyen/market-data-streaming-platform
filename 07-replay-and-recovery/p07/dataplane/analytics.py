"""P06: raw audit, canonical candidate, materialised bars, READY publication.

The pipeline is deliberately one-way and gated::

    raw audit -> canonical candidate (immutable build_id) -> bars -> VERIFIED -> READY

Each arrow can refuse. An identity conflict in raw stops the build rather than
picking a winner (design 7.3). A bar mismatch stops publication. ``VERIFIED`` is
not something this module can award itself: :meth:`BuildPipeline.mark_verified`
takes an explicit proof token from the harness, because a component declaring
its own output verified is how a false ``READY`` happens (``INV-05``).

The bar aggregation here is a **second implementation**, separate from
:mod:`p07.oracle.reference_bars`. This one groups rows per key and sorts each
group by ``(event_time_ns, sequence)``; the oracle streams single-pass with
running extremes. Two different algorithms agreeing is evidence; one algorithm
checking itself is not.

What the simulation deliberately keeps simple: there is no incremental
materialised view with partial aggregate states, no merge scheduling, no
``AggregateFunction`` columns. Those are real sources of P06 bugs and they are
*not* covered here -- which is why the group ``G18`` cases about MV partial
states are reported ``NOT_RUN`` rather than passed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from ..contracts import EventKind, SemanticCore, SourceIdentity
from ..oracle.semantic_digest import digest_core
from .clickhouse_fake import BarRow, BuildStatus, CanonicalRow, FakeClickHouse


@dataclass(frozen=True, slots=True)
class RawAudit:
    """What the raw table contains, classified by logical identity."""

    dataset_id: str
    physical_rows: int
    identities: int
    duplicate_identities: tuple[SourceIdentity, ...]
    conflicts: tuple[tuple[SourceIdentity, tuple[str, ...]], ...]
    digest_mismatches: tuple[SourceIdentity, ...]
    cores: Mapping[SourceIdentity, SemanticCore]

    @property
    def complete(self) -> bool:
        """Whether the audit found nothing that blocks a canonical build."""
        return not self.conflicts and not self.digest_mismatches

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "physical_rows": self.physical_rows,
            "identities": self.identities,
            "duplicate_identity_count": len(self.duplicate_identities),
            "duplicate_identities_sample": [list(i) for i in self.duplicate_identities[:20]],
            "conflicts": [
                {"identity": list(identity), "fields": list(fields)}
                for identity, fields in self.conflicts[:20]
            ],
            "conflict_count": len(self.conflicts),
            "digest_mismatch_count": len(self.digest_mismatches),
        }


@dataclass(slots=True)
class PipelineCounters:
    audits: int = 0
    builds_opened: int = 0
    canonical_rows: int = 0
    bar_rows: int = 0
    builds_dirtied: int = 0
    publish_attempts: int = 0
    publish_races_lost: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "audits": self.audits,
            "builds_opened": self.builds_opened,
            "canonical_rows": self.canonical_rows,
            "bar_rows": self.bar_rows,
            "builds_dirtied": self.builds_dirtied,
            "publish_attempts": self.publish_attempts,
            "publish_races_lost": self.publish_races_lost,
        }


class BuildRefused(Exception):
    """The build cannot proceed. Carries a reason code."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


class BuildPipeline:
    """Drives one ClickHouse instance's audit/build/verify/publish cycle.

    ``crash_after`` mirrors :class:`~p07.dataplane.ingestion.BatchIngestor`: a
    callable invoked at named stage boundaries so a test can kill the builder at
    exactly the point design 5.3 cares about -- mid-canonical-staging, after
    bars but before verification, or between the catalog swap and the client's
    acknowledgement.
    """

    def __init__(
        self,
        sink: FakeClickHouse,
        *,
        crash_after: Callable[..., None] | None = None,
    ) -> None:
        self._sink = sink
        self._crash_after = crash_after
        self.counters = PipelineCounters()

    def _checkpoint(self, stage: str, **context: Any) -> None:
        if self._crash_after is not None:
            self._crash_after(stage, **context)

    # -- audit -------------------------------------------------------------

    def audit_raw(self, dataset_id: str) -> RawAudit:
        """Classify raw rows by identity, detecting duplicates and conflicts.

        A duplicate is the same identity with the same semantic core -- a
        physical retry, and harmless. A *conflict* is the same identity with a
        different core, and it is terminal: there is no rule by which one of two
        contradictory versions of an event is the right one.

        The stored ``payload_digest`` is recomputed from the row's columns and a
        disagreement is reported separately. A row whose digest does not match
        its own data cannot be trusted either way.
        """
        self.counters.audits += 1
        cores: dict[SourceIdentity, SemanticCore] = {}
        counts: dict[SourceIdentity, int] = {}
        conflicts: list[tuple[SourceIdentity, tuple[str, ...]]] = []
        digest_mismatches: list[SourceIdentity] = []
        physical = 0

        for row in self._sink.scan_raw(dataset_id=dataset_id):
            physical += 1
            core = row.semantic_core()
            if digest_core(core) != row.payload_digest:
                digest_mismatches.append(row.identity)
            identity = row.identity
            counts[identity] = counts.get(identity, 0) + 1
            known = cores.get(identity)
            if known is None:
                cores[identity] = core
            elif known != core:
                conflicts.append((identity, known.differing_fields(core)))

        duplicates = tuple(
            identity for identity, count in sorted(counts.items()) if count > 1
        )
        return RawAudit(
            dataset_id=dataset_id,
            physical_rows=physical,
            identities=len(cores),
            duplicate_identities=duplicates,
            conflicts=tuple(conflicts),
            digest_mismatches=tuple(dict.fromkeys(digest_mismatches)),
            cores=cores,
        )

    # -- build -------------------------------------------------------------

    def build_canonical(
        self,
        build_id: str,
        *,
        audit: RawAudit,
        source_manifest_sha256: str,
        expected_identity_count: int,
        batch_size: int = 1000,
    ) -> int:
        """Stage one canonical row per identity. Returns the row count.

        Refuses on a non-clean audit. A crash partway leaves the build in
        ``STAGING``, which is not publishable -- that, rather than any cleanup,
        is what makes a half-built candidate safe (design 5.3).
        """
        if audit.conflicts:
            raise BuildRefused("IDENTITY_CONFLICT", f"{len(audit.conflicts)} conflicting identities")
        if audit.digest_mismatches:
            raise BuildRefused("SEMANTIC_MISMATCH", f"{len(audit.digest_mismatches)} bad digests")

        self._sink.open_build(
            build_id,
            source_manifest_sha256=source_manifest_sha256,
            expected_identity_count=expected_identity_count,
        )
        self.counters.builds_opened += 1

        batch: list[CanonicalRow] = []
        written = 0
        for identity in sorted(audit.cores):
            core = audit.cores[identity]
            batch.append(CanonicalRow(build_id=build_id, core=core, payload_digest=digest_core(core)))
            if len(batch) >= batch_size:
                self._sink.insert_canonical(build_id, batch)
                written += len(batch)
                batch.clear()
                self._checkpoint("canonical_batch", build_id=build_id, written=written)
        if batch:
            self._sink.insert_canonical(build_id, batch)
            written += len(batch)
        self.counters.canonical_rows += written
        self._sink.set_build_status(build_id, BuildStatus.CANDIDATE)
        self._checkpoint("canonical_complete", build_id=build_id, written=written)
        return written

    def materialize_bars(self, build_id: str) -> int:
        """Aggregate canonical ticks into 1-minute bars. Returns the row count.

        Group-then-sort, independent of the oracle's streaming aggregation. Only
        ticks participate; quotes are skipped, and a bucket with no ticks
        produces no row.
        """
        status = self._sink.build_status(build_id)
        if status not in {BuildStatus.CANDIDATE, BuildStatus.STAGING}:
            raise BuildRefused("BUILD_NOT_VERIFIED", f"{build_id} is {status}")

        grouped: dict[tuple[str, str, int, int], list[tuple[int, int, int]]] = {}
        scales: dict[tuple[str, str], int] = {}
        for row in self._sink.scan_canonical(build_id):
            core = row.core
            if core.event_kind is not EventKind.TICK or core.price_ticks is None:
                continue
            scale_key = (core.dataset_id, core.symbol)
            known = scales.setdefault(scale_key, core.price_scale)
            if known != core.price_scale:
                raise BuildRefused(
                    "PRICE_SCALE_CHANGED", f"{core.dataset_id}/{core.symbol}: {known} vs {core.price_scale}"
                )
            key = (core.dataset_id, core.symbol, core.price_scale, core.bar_start_ns)
            grouped.setdefault(key, []).append((core.event_time_ns, core.sequence, core.price_ticks))

        rows: list[BarRow] = []
        for (dataset_id, symbol, scale, bar_start), points in grouped.items():
            points.sort()
            prices = [price for _, _, price in points]
            rows.append(
                BarRow(
                    build_id=build_id,
                    dataset_id=dataset_id,
                    symbol=symbol,
                    price_scale=scale,
                    bar_start_ns=bar_start,
                    open_ticks=prices[0],
                    high_ticks=max(prices),
                    low_ticks=min(prices),
                    close_ticks=prices[-1],
                    tick_count=len(prices),
                )
            )
        self._sink.insert_bars(build_id, rows)
        self.counters.bar_rows += len(rows)
        self._checkpoint("bars_complete", build_id=build_id, bars=len(rows))
        return len(rows)

    # -- verification and publication --------------------------------------

    def mark_verified(self, build_id: str, *, proof: Mapping[str, Any]) -> None:
        """Promote a candidate to ``VERIFIED`` given an external proof token.

        ``proof`` must carry ``canonical_verified``, ``bars_verified`` and
        ``source_cut_sealed``, all true, plus an ``evidence_ref``. The pipeline
        checks the shape and refuses otherwise; it cannot check the *content*,
        which is deliberate -- the content is :mod:`p07.reconcile`'s job, and
        this gate exists so the pipeline cannot skip it.
        """
        required = ("canonical_verified", "bars_verified", "source_cut_sealed")
        missing = [name for name in required if not proof.get(name)]
        if missing:
            raise BuildRefused("BUILD_NOT_VERIFIED", f"proof missing {missing}")
        if not proof.get("evidence_ref"):
            raise BuildRefused("EVIDENCE_INCOMPLETE", "proof has no evidence_ref")
        self._sink.set_build_status(build_id, BuildStatus.VERIFIED)

    def mark_dirty(self, build_id: str, reason: str) -> None:
        """Mark a build unusable. A dirty build is never publishable."""
        self._sink.set_build_status(build_id, BuildStatus.DIRTY)
        self.counters.builds_dirtied += 1

    def publish(
        self,
        build_id: str,
        *,
        expected_current: str | None,
        evidence_refs: tuple[str, ...] = (),
        now_ns: int = 0,
    ) -> bool:
        """Swap the READY pointer. ``False`` means another writer won the race.

        The crash hook fires *after* the swap and before this returns, which is
        the ambiguous window of case ``E2E-30``: the pointer has moved but the
        caller never learns it. Reading the pointer back resolves it, and
        re-publishing the same build is idempotent.
        """
        self.counters.publish_attempts += 1
        won = self._sink.publish_ready(
            build_id, expected_current=expected_current, evidence_refs=evidence_refs, now_ns=now_ns
        )
        if not won:
            self.counters.publish_races_lost += 1
            return False
        self._checkpoint("after_pointer_commit", build_id=build_id)
        return True

    def describe(self) -> dict[str, Any]:
        return {"counters": self.counters.as_dict(), "store": self._sink.describe()}
