"""Stage an immutable canonical candidate, or leave the build DIRTY.

Section 5.1 S3/S4 and matrix group G. The builder's contract is narrow and
unforgiving:

* It **only runs on a PASS audit**. A conflict or a missing identity upstream is
  not something a builder can fix by being careful (case G-08).
* It writes **one row per source identity**, ticks only. ``MergeTree`` will not
  enforce that, so the invariant is produced here and *proved afterwards* by an
  independent check -- never assumed because the INSERT returned success
  (ADR-003, case G-03).
* **An unknown acknowledgement is not a retry.** If the client loses the
  response, the rows may be durable, and re-inserting them would add a second
  set of aggregate states to a view that cannot subtract. The build goes DIRTY
  and only a fresh staging attempt may continue (section 5.2, case G-02, I-07).
* **A build id is content-addressed.** Same source, same schema, same policy,
  same id -- which is what makes a rebuild recognisable. But the *attempt* is
  separate, so a dirty attempt can never be silently continued (S3).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Final

from .contracts import SourceIdentity
from .errors import AckUnknownError, BuildDirtyError, ContractError, StoreError, Verdict
from .raw_auditor import RawAuditReport
from .rows import CanonicalTick, RawRow, canonical_from_raw
from .snapshot_cutter import SnapshotCut
from .store.base import AnalyticsStore

BUILDER_VERSION: Final = "canonical-builder.1"

#: The canonicalisation policy. Hashed into the ``build_id`` so that changing
#: any of it produces a different build rather than quietly different numbers
#: under the same name (case G-06).
CANONICAL_POLICY: Final = {
    "policy_version": "1",
    "identity": "(dataset_id,symbol,sequence)",
    "order": "(event_time_ns,sequence)",
    "duplicate_resolution": "collapse_identical_semantic_core_only",
    "conflict_resolution": "none_stop_build",
    "quote_handling": "excluded_from_tick_bars",
    "scale_policy": "single_scale_per_dataset_symbol_or_stop",
    "event_date": "rederived_utc_from_event_time_ns",
    "bar_interval_ns": "60000000000",
}


class BuildState(StrEnum):
    """The states of section 5.1's diagram, as a value the catalog can store."""

    CREATED = "CREATED"
    CUT_CAPTURED = "CUT_CAPTURED"
    RAW_AUDITING = "RAW_AUDITING"
    CANONICAL_BUILDING = "CANONICAL_BUILDING"
    BAR_VERIFYING = "BAR_VERIFYING"
    READY = "READY"
    DIRTY = "DIRTY"
    NO_GO = "NO_GO"
    SUPERSEDED = "SUPERSEDED"
    REBUILDING = "REBUILDING"


def policy_digest(policy: dict[str, str] | None = None) -> str:
    return hashlib.sha256(
        json.dumps(
            policy if policy is not None else CANONICAL_POLICY,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def compute_build_id(
    *,
    manifest_sha256: str,
    cut_digest: str,
    schema_sha256: str,
    builder_version: str = BUILDER_VERSION,
    canonical_policy_digest: str | None = None,
) -> str:
    """``SHA256(manifest + cut + schema + builder + policy)`` (section 5.1 S3).

    Content-addressed, so two runs over the same sealed source with the same code
    and policy agree on the name -- and any change to any input renames the
    build instead of overwriting its meaning.
    """
    parts = [
        manifest_sha256,
        cut_digest,
        schema_sha256,
        builder_version,
        canonical_policy_digest if canonical_policy_digest is not None else policy_digest(),
    ]
    h = hashlib.sha256()
    for part in parts:
        h.update(part.encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()


def staging_build_id(build_id: str, attempt: int) -> str:
    """Name one *attempt* at a build.

    A dirty attempt must never be continued, but the logical build keeps its
    identity across attempts. Keeping the attempt in the physical id means a
    half-written attempt's rows can be dropped wholesale without touching
    anything else (section 5.2, case G-04).
    """
    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")
    return f"{build_id}.a{attempt}"


@dataclass(slots=True)
class CanonicalBuildResult:
    """Outcome of one staging attempt."""

    build_id: str
    physical_build_id: str
    attempt: int
    state: BuildState
    rows_written: int = 0
    identities: int = 0
    block_size: int = 0
    reasons: list[str] = field(default_factory=list)

    @property
    def is_staged(self) -> bool:
        return self.state is BuildState.CANONICAL_BUILDING

    def as_dict(self) -> dict[str, Any]:
        return {
            "build_id": self.build_id,
            "physical_build_id": self.physical_build_id,
            "attempt": self.attempt,
            "state": str(self.state),
            "rows_written": self.rows_written,
            "identities": self.identities,
            "block_size": self.block_size,
            "reasons": list(self.reasons),
        }


def select_canonical_rows(
    rows: Iterable[RawRow],
    *,
    audit: RawAuditReport,
    physical_build_id: str,
) -> list[CanonicalTick]:
    """Collapse audited raw rows to one canonical tick per clean identity.

    Only identities the audit marked clean are admitted, and the *first* row seen
    for one is kept -- which is safe precisely because the audit already proved
    every row for that identity has an identical semantic core. Without that
    proof this would be ``any()``, the thing case G-08 exists to forbid.
    """
    clean: set[SourceIdentity] = set(audit.clean_tick_identities)
    taken: dict[SourceIdentity, CanonicalTick] = {}
    for row in rows:
        identity = row.identity
        if identity not in clean or identity in taken:
            continue
        taken[identity] = canonical_from_raw(row, build_id=physical_build_id)

    missing = clean - set(taken)
    if missing:
        raise ContractError(
            f"{len(missing)} audited identities were absent from the row stream; "
            f"the audit and the build read different inputs",
            code="BUILDER_INPUT_DISAGREEMENT",
            sample=[list(i) for i in sorted(missing)[:10]],
        )
    return sorted(
        taken.values(), key=lambda t: (t.dataset_id, t.symbol, t.event_time_ns, t.sequence)
    )


def build_canonical(
    store: AnalyticsStore,
    rows: Sequence[RawRow],
    *,
    audit: RawAuditReport,
    cut: SnapshotCut,
    schema_sha256: str,
    attempt: int = 1,
    block_size: int = 5_000,
    build_id: str | None = None,
) -> CanonicalBuildResult:
    """Stage one canonical attempt.

    Returns a result in ``CANONICAL_BUILDING`` on success. Raises
    :class:`BuildDirtyError` when the physical outcome is uncertain, because a
    caller that gets an exception cannot accidentally proceed to publish, while a
    caller that gets a ``DIRTY`` return value very much can.
    """
    resolved_build_id = build_id or compute_build_id(
        manifest_sha256=cut.manifest_sha256,
        cut_digest=cut.cut_digest(),
        schema_sha256=schema_sha256,
    )
    physical = staging_build_id(resolved_build_id, attempt)
    result = CanonicalBuildResult(
        build_id=resolved_build_id,
        physical_build_id=physical,
        attempt=attempt,
        state=BuildState.CANONICAL_BUILDING,
        block_size=block_size,
    )

    if audit.verdict() is not Verdict.PASS:
        result.state = BuildState.NO_GO
        result.reasons = ["raw audit did not pass", *audit.reasons]
        raise BuildDirtyError(
            f"refusing to build from a {audit.verdict()} audit: {'; '.join(audit.reasons)}",
            code="AUDIT_NOT_PASSED",
            build_id=resolved_build_id,
        )

    existing = store.canonical_identity_counts(build_id=physical)
    if existing:
        result.state = BuildState.DIRTY
        raise BuildDirtyError(
            f"staging id {physical} already holds {len(existing)} identities; a "
            f"fresh attempt is required rather than appending to it",
            code="STAGING_NOT_EMPTY",
            build_id=resolved_build_id,
            physical_build_id=physical,
        )

    canonical = select_canonical_rows(rows, audit=audit, physical_build_id=physical)
    result.identities = len(canonical)

    try:
        result.rows_written = store.insert_canonical(canonical, block_size=block_size)
    except AckUnknownError as exc:
        result.state = BuildState.DIRTY
        raise BuildDirtyError(
            f"canonical INSERT for {physical} ended with an unknown acknowledgement; "
            f"the rows may be durable, so this attempt is abandoned rather than "
            f"retried: {exc}",
            code="ACK_UNKNOWN_BUILD_DIRTY",
            build_id=resolved_build_id,
            physical_build_id=physical,
        ) from exc
    except StoreError as exc:
        result.state = BuildState.DIRTY
        raise BuildDirtyError(
            f"canonical INSERT for {physical} failed partway ({exc.code}); staging "
            f"content is unknown and must be discarded: {exc}",
            code="PARTIAL_INSERT_BUILD_DIRTY",
            build_id=resolved_build_id,
            physical_build_id=physical,
        ) from exc

    if result.rows_written != result.identities:
        result.state = BuildState.DIRTY
        raise BuildDirtyError(
            f"wrote {result.rows_written} rows for {result.identities} identities",
            code="ROW_COUNT_DISAGREEMENT",
            build_id=resolved_build_id,
        )

    result.reasons.append(
        f"staged {result.rows_written} canonical ticks in blocks of {block_size}"
    )
    return result


def rebuild_fresh(
    store: AnalyticsStore,
    rows: Sequence[RawRow],
    *,
    audit: RawAuditReport,
    cut: SnapshotCut,
    schema_sha256: str,
    dirty_attempt: int,
    block_size: int = 5_000,
) -> CanonicalBuildResult:
    """Discard a dirty attempt and stage the next one.

    The drop covers the state table as well as the canonical rows. A
    materialized view does not follow its source's deletes, so leaving the old
    states behind would leave a rebuilt bar double-counted -- the failure mode of
    section 7.1 item 2, arrived at by trying to be tidy.
    """
    resolved_build_id = compute_build_id(
        manifest_sha256=cut.manifest_sha256,
        cut_digest=cut.cut_digest(),
        schema_sha256=schema_sha256,
    )
    store.drop_build(build_id=staging_build_id(resolved_build_id, dirty_attempt))
    return build_canonical(
        store,
        rows,
        audit=audit,
        cut=cut,
        schema_sha256=schema_sha256,
        attempt=dirty_attempt + 1,
        block_size=block_size,
        build_id=resolved_build_id,
    )
