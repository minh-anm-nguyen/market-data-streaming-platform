"""Reconcile the sealed source against the P05 raw table.

Section 5.1 S2 and matrix group E. The audit answers one question -- *is there
exactly one logical event per accepted identity, and does it say what the source
said it said?* -- and it answers it by comparing typed fields, not by comparing
counts (section 0.2).

The distinctions it has to keep straight:

* **physical duplicate**: one identity, several rows, one semantic core. Normal
  under at-least-once delivery. Classified, counted, and collapsed to one
  canonical row.
* **conflict**: one identity, several *different* semantic cores. The build
  stops. There is no honest way to pick a winner, and ``argMax(ingested_at)``
  picks the most recently *written* row, which has nothing to do with which one
  is true (case E-02..E-04).
* **digest mismatch**: the stored digest does not match a recomputation of the
  row's own fields. The row is rejected rather than compared, because a forged
  or corrupted digest would otherwise launder a mismatched row -- and a digest
  that collides by construction still loses to the typed comparison (E-05, E-10).
* **missing / extra**: set difference against the P03 accepted ledger, in both
  directions. One direction alone would miss half the failures (case F-08).

A row whose Kafka offset lies at or past the cut is *out of scope*, not extra: it
arrived after the snapshot was sealed and belongs to the next build (case E-12).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from .contracts import EVENT_TICK, SourceIdentity
from .errors import ContractError, Verdict
from .rows import RawRow
from .snapshot_cutter import SnapshotCut, SourceManifest


@dataclass(frozen=True, slots=True)
class InvalidRow:
    identity: tuple[str, str, int]
    code: str
    detail: str


@dataclass(frozen=True, slots=True)
class Conflict:
    identity: tuple[str, str, int]
    digests: tuple[str, ...]
    physical_rows: int


@dataclass(frozen=True, slots=True)
class EventTimeWindow:
    """The event-time range a dataset's contract declares as plausible.

    Separate from the ``Int64`` domain check in :mod:`md_analytics.contracts`,
    which only asks whether a timestamp is *representable*. A tick dated a
    century from now is perfectly representable and still wrong, and the design
    requires it quarantined rather than published (case D-10) -- partly because
    TTL is evaluated on event time, so a future-dated row would otherwise evade
    retention indefinitely (case O-12, J-08).

    The window comes from the source contract, never from the data: deriving it
    from the observed minimum and maximum would make any corrupt timestamp
    define its own plausibility.
    """

    min_ns: int
    max_ns: int

    def __post_init__(self) -> None:
        if self.min_ns > self.max_ns:
            raise ValueError(f"inverted window: {self.min_ns} > {self.max_ns}")

    def contains(self, event_time_ns: int) -> bool:
        return self.min_ns <= event_time_ns <= self.max_ns


@dataclass(slots=True)
class RawAuditReport:
    """Everything the publication gate needs to decide, and why.

    Lists are capped when exported, never when counted: the gate decides on
    counts, humans read the samples.
    """

    dataset_id: str
    physical_rows: int = 0
    rows_in_scope: int = 0
    rows_out_of_cut: int = 0
    distinct_identities: int = 0
    physical_duplicates: dict[tuple[str, str, int], int] = field(default_factory=dict)
    conflicts: list[Conflict] = field(default_factory=list)
    digest_mismatches: list[tuple[str, str, int]] = field(default_factory=list)
    invalid_rows: list[InvalidRow] = field(default_factory=list)
    missing: list[tuple[str, str, int]] = field(default_factory=list)
    extra: list[tuple[str, str, int]] = field(default_factory=list)
    manifest_digest_disagreements: list[tuple[str, str, int]] = field(default_factory=list)
    event_date_drift: list[tuple[str, str, int]] = field(default_factory=list)
    #: Identities whose event time is representable but outside the contract's
    #: plausible window. Quarantined, never published (case D-10, J-08, O-12).
    out_of_window: list[tuple[str, str, int]] = field(default_factory=list)
    scale_conflicts: dict[tuple[str, str], tuple[int, ...]] = field(default_factory=dict)
    kind_mismatches: list[tuple[str, str, int]] = field(default_factory=list)
    #: Tick identities that passed every check. The only input the canonical
    #: builder is allowed to use.
    clean_tick_identities: frozenset[SourceIdentity] = frozenset()
    reasons: list[str] = field(default_factory=list)

    @property
    def duplicate_row_count(self) -> int:
        """Extra physical rows beyond one per identity."""
        return sum(n - 1 for n in self.physical_duplicates.values())

    @property
    def is_complete(self) -> bool:
        return not self.missing and not self.extra

    @property
    def is_consistent(self) -> bool:
        return not (
            self.conflicts
            or self.digest_mismatches
            or self.invalid_rows
            or self.manifest_digest_disagreements
            or self.event_date_drift
            or self.out_of_window
            or self.scale_conflicts
            or self.kind_mismatches
        )

    def verdict(self) -> Verdict:
        """``PASS`` only when the snapshot is both complete and consistent.

        Everything else is ``NO_GO``: the data contradicts itself or the source,
        and no amount of downstream care can repair a build started from it.
        """
        return Verdict.PASS if (self.is_complete and self.is_consistent) else Verdict.NO_GO

    def counters(self) -> dict[str, int]:
        """Flat counters for metrics and the benchmark report template."""
        return {
            "raw_physical_rows": self.physical_rows,
            "raw_rows_in_scope": self.rows_in_scope,
            "raw_rows_out_of_cut": self.rows_out_of_cut,
            "raw_distinct_identities": self.distinct_identities,
            "raw_duplicate_id_count": len(self.physical_duplicates),
            "raw_duplicate_row_count": self.duplicate_row_count,
            "raw_conflict_id_count": len(self.conflicts),
            "raw_digest_mismatch_count": len(self.digest_mismatches),
            "raw_invalid_row_count": len(self.invalid_rows),
            "missing_ids": len(self.missing),
            "extra_ids": len(self.extra),
            "manifest_digest_disagreements": len(self.manifest_digest_disagreements),
            "event_date_drift_count": len(self.event_date_drift),
            "out_of_window_count": len(self.out_of_window),
            "scale_conflict_count": len(self.scale_conflicts),
            "kind_mismatch_count": len(self.kind_mismatches),
            "clean_tick_identities": len(self.clean_tick_identities),
        }

    def as_dict(self, *, sample_limit: int = 20) -> dict[str, Any]:
        def sample(items: Sequence[Any]) -> list[Any]:
            return [
                item if not isinstance(item, tuple) else list(item)
                for item in list(items)[:sample_limit]
            ]

        return {
            "dataset_id": self.dataset_id,
            "verdict": str(self.verdict()),
            "counters": self.counters(),
            "reasons": list(self.reasons),
            "samples": {
                "conflicts": [
                    {
                        "identity": list(c.identity),
                        "digests": list(c.digests),
                        "physical_rows": c.physical_rows,
                    }
                    for c in self.conflicts[:sample_limit]
                ],
                "missing": sample(self.missing),
                "extra": sample(self.extra),
                "digest_mismatches": sample(self.digest_mismatches),
                "invalid_rows": [
                    {"identity": list(r.identity), "code": r.code, "detail": r.detail}
                    for r in self.invalid_rows[:sample_limit]
                ],
                "event_date_drift": sample(self.event_date_drift),
                "out_of_window": sample(self.out_of_window),
                "scale_conflicts": [
                    {"dataset_symbol": list(k), "scales": list(v)}
                    for k, v in list(self.scale_conflicts.items())[:sample_limit]
                ],
                "kind_mismatches": sample(self.kind_mismatches),
            },
        }


def _in_scope(row: RawRow, cut: SnapshotCut) -> bool:
    """A row belongs to the snapshot iff its offset is below its partition's cut.

    A row from a topic-partition the cut does not mention is *not* in scope: the
    cut defines the snapshot, and silently absorbing an unknown partition's rows
    would make the sealed set unprovable (case F-13).
    """
    bound = cut.offset_bound(row.topic_partition)
    return bound is not None and row.kafka_offset < bound


def audit_raw(
    rows: Iterable[RawRow],
    *,
    manifest: SourceManifest,
    cut: SnapshotCut,
    event_time_window: EventTimeWindow | None = None,
) -> RawAuditReport:
    """Reconcile raw rows against the sealed source. Pure; no I/O.

    Takes an iterable so a 200M-row audit can stream from the server, and keeps
    per-identity state rather than per-row state so memory grows with the number
    of identities rather than with the number of physical retries.
    """
    report = RawAuditReport(dataset_id=manifest.dataset_id)
    expected_digests = manifest.expected_digests()
    expected_identities = manifest.accepted_identities()
    expected_kinds = {e.identity: e.event_kind for e in manifest.accepted}

    # identity -> {digest: (core, physical_count, first_row)}
    observed: dict[SourceIdentity, dict[str, list[Any]]] = defaultdict(dict)
    invalid_identities: set[SourceIdentity] = set()
    scales: dict[tuple[str, str], set[int]] = defaultdict(set)

    for row in rows:
        report.physical_rows += 1
        if row.dataset_id != manifest.dataset_id:
            # Another dataset's rows are not this snapshot's business. They are
            # neither extra nor missing here; cross-dataset isolation is the
            # point of case E-06.
            continue
        if not _in_scope(row, cut):
            report.rows_out_of_cut += 1
            continue
        report.rows_in_scope += 1
        identity = row.identity

        try:
            core = row.semantic_core()
        except ContractError as exc:
            report.invalid_rows.append(
                InvalidRow(tuple(identity), exc.code, str(exc))
            )
            invalid_identities.add(identity)
            continue

        try:
            stored = row.stored_digest()
        except ContractError as exc:
            report.invalid_rows.append(InvalidRow(tuple(identity), exc.code, str(exc)))
            invalid_identities.add(identity)
            continue

        recomputed = row.recomputed_digest()
        if stored != recomputed:
            report.digest_mismatches.append(tuple(identity))
            invalid_identities.add(identity)
            continue

        if row.event_date != row.derived_event_date():
            report.event_date_drift.append(tuple(identity))
            invalid_identities.add(identity)
            continue

        if event_time_window is not None and not event_time_window.contains(
            row.event_time_ns
        ):
            report.out_of_window.append(tuple(identity))
            invalid_identities.add(identity)
            continue

        # Keyed by the *recomputed* digest, so two rows that merely claim the
        # same digest are still compared on their fields (case E-10).
        bucket = observed[identity]
        entry = bucket.get(recomputed)
        if entry is None:
            bucket[recomputed] = [core, 1]
        else:
            entry[1] += 1

        scales[(row.dataset_id, row.symbol)].add(row.price_scale)

    # ---------------------------------------------------------- per identity
    clean_ticks: set[SourceIdentity] = set()
    for identity, bucket in observed.items():
        report.distinct_identities += 1
        total_rows = sum(count for _, count in bucket.values())
        if len(bucket) > 1:
            report.conflicts.append(
                Conflict(tuple(identity), tuple(sorted(bucket)), total_rows)
            )
            continue
        if total_rows > 1:
            report.physical_duplicates[tuple(identity)] = total_rows

        digest = next(iter(bucket))
        core = bucket[digest][0]

        expected_digest = expected_digests.get(identity)
        if expected_digest is not None and expected_digest != digest:
            # The row is internally consistent but disagrees with what the
            # source says it published: a real discrepancy, not a duplicate.
            report.manifest_digest_disagreements.append(tuple(identity))
            continue
        expected_kind = expected_kinds.get(identity)
        if expected_kind is not None and expected_kind != core["event_kind"]:
            report.kind_mismatches.append(tuple(identity))
            continue

        if identity in invalid_identities:
            continue
        if core["event_kind"] == EVENT_TICK:
            clean_ticks.add(identity)

    observed_identities = frozenset(observed) - invalid_identities
    report.missing = sorted(tuple(i) for i in expected_identities - observed_identities)
    report.extra = sorted(tuple(i) for i in observed_identities - expected_identities)

    report.scale_conflicts = {
        key: tuple(sorted(found)) for key, found in scales.items() if len(found) > 1
    }
    # A symbol with conflicting scales contributes no clean ticks: an OHLC bar
    # mixing two scales has no defined unit, so MVP policy is STOP (case C-03).
    if report.scale_conflicts:
        conflicted = set(report.scale_conflicts)
        clean_ticks = {i for i in clean_ticks if (i.dataset_id, i.symbol) not in conflicted}

    report.clean_tick_identities = frozenset(clean_ticks)

    for label, count in (
        ("missing accepted identities", len(report.missing)),
        ("extra identities not in the accepted ledger", len(report.extra)),
        ("identity conflicts", len(report.conflicts)),
        ("digest mismatches", len(report.digest_mismatches)),
        ("contract-invalid rows", len(report.invalid_rows)),
        ("digests disagreeing with the manifest", len(report.manifest_digest_disagreements)),
        ("event_date drift", len(report.event_date_drift)),
        ("event times outside the contract window", len(report.out_of_window)),
        ("price_scale conflicts", len(report.scale_conflicts)),
        ("event kind mismatches", len(report.kind_mismatches)),
    ):
        if count:
            report.reasons.append(f"{count} {label}")
    if not report.reasons:
        report.reasons.append(
            f"complete and consistent: {report.distinct_identities} identities, "
            f"{report.duplicate_row_count} duplicate rows classified"
        )
    return report
