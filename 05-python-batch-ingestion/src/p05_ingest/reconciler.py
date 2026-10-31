"""The independent oracle: identity-level reconciliation and the verdict.

Spec section 9.2. This module decides whether a finite run may be called
complete, and it is deliberately built so that it cannot be talked into a PASS:

  * **counts never decide anything.** The canonical counter-example in the spec
    (E2E-21) removes one expected event and duplicates another, so ``count()``
    matches while the data is wrong. Everything here compares *sets of
    identities* and *digests per identity*.
  * **physical duplicates are reported, not failed.** At-least-once delivery
    makes them expected. What must be zero is unexplained missing, unexplained
    extra, and identity conflicts.
  * **a conflict is never resolved.** Two different payloads under one identity
    is a P0 report, not a "take the highest offset" rule.
  * **missing evidence yields INCONCLUSIVE, not PASS.** No source manifest, an
    un-frozen cut, a commit ahead of the sink proof, or retention loss all
    produce a verdict that blocks release.

The three inputs are kept separate on purpose. ``expected`` comes from the P01
manifest and the P04 publish ledger; ``kafka_observed`` comes from a read-only
scan to a frozen cut; ``sink_observed`` comes from reading the table back. The
engine's own counters are not an input, because a bug in the engine would then
be allowed to vouch for itself.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from .model import EventIdentity


class Verdict(StrEnum):
    GO = "GO"
    NO_GO = "NO-GO"
    INCONCLUSIVE = "INCONCLUSIVE"
    NOT_RUN = "NOT_RUN"


@dataclass(frozen=True, slots=True)
class ObservedEvent:
    """One identity with its semantic digest, from any of the three sources."""

    identity: EventIdentity
    payload_digest: str


@dataclass(slots=True)
class EvidenceGaps:
    """Everything that makes a confident verdict impossible (spec 9.2)."""

    missing_source_manifest: bool = False
    kafka_cut_not_frozen: bool = False
    sink_readback_incomplete: bool = False
    settle_window_not_elapsed: bool = False
    retention_loss_detected: bool = False
    commit_ahead_of_sink_proof: bool = False
    unknown_sink_outcomes: int = 0
    notes: list[str] = field(default_factory=list)

    @property
    def any(self) -> bool:
        return bool(
            self.missing_source_manifest
            or self.kafka_cut_not_frozen
            or self.sink_readback_incomplete
            or self.settle_window_not_elapsed
            or self.retention_loss_detected
            or self.commit_ahead_of_sink_proof
            or self.unknown_sink_outcomes
        )

    def describe(self) -> list[str]:
        out = list(self.notes)
        if self.missing_source_manifest:
            out.append(
                "no source manifest: completeness can only be observed, not claimed"
            )
        if self.kafka_cut_not_frozen:
            out.append("the Kafka cut was still advancing during the scan")
        if self.sink_readback_incomplete:
            out.append("the ClickHouse read-back did not complete")
        if self.settle_window_not_elapsed:
            out.append("the settle window had not elapsed when the table was read")
        if self.retention_loss_detected:
            out.append("Kafka retention removed offsets the group still needed")
        if self.commit_ahead_of_sink_proof:
            out.append(
                "a committed offset was ahead of the proven sink frontier (I-01)"
            )
        if self.unknown_sink_outcomes:
            out.append(
                f"{self.unknown_sink_outcomes} sink batch(es) ended in an unknown "
                "state, so their rows may or may not exist"
            )
        return out


@dataclass(slots=True)
class ReconciliationReport:
    """The full result. Serialised verbatim into the run artifact."""

    verdict: Verdict
    expected_count: int
    kafka_count: int
    sink_logical_count: int
    sink_physical_count: int

    missing_from_kafka: list[EventIdentity] = field(default_factory=list)
    missing_from_sink: list[EventIdentity] = field(default_factory=list)
    extra_in_sink: list[EventIdentity] = field(default_factory=list)
    duplicate_physical: dict[EventIdentity, int] = field(default_factory=dict)
    conflicts: dict[EventIdentity, list[str]] = field(default_factory=dict)
    digest_mismatches: list[EventIdentity] = field(default_factory=list)
    gaps: EvidenceGaps = field(default_factory=EvidenceGaps)
    reasons: list[str] = field(default_factory=list)

    @property
    def is_go(self) -> bool:
        return self.verdict is Verdict.GO

    @property
    def duplicate_rows(self) -> int:
        """Physical rows beyond one per identity."""
        return sum(n - 1 for n in self.duplicate_physical.values())

    def to_dict(self) -> dict[str, object]:
        return {
            "verdict": str(self.verdict),
            "counts": {
                "expected": self.expected_count,
                "kafka": self.kafka_count,
                "sink_logical": self.sink_logical_count,
                "sink_physical": self.sink_physical_count,
                "duplicate_rows": self.duplicate_rows,
            },
            "missing_from_kafka": [list(i) for i in self.missing_from_kafka],
            "missing_from_sink": [list(i) for i in self.missing_from_sink],
            "extra_in_sink": [list(i) for i in self.extra_in_sink],
            "duplicate_physical": {
                "|".join(map(str, k)): v for k, v in self.duplicate_physical.items()
            },
            "conflicts": {
                "|".join(map(str, k)): sorted(v) for k, v in self.conflicts.items()
            },
            "digest_mismatches": [list(i) for i in self.digest_mismatches],
            "evidence_gaps": self.gaps.describe(),
            "reasons": self.reasons,
        }

    def write_json(self, path: str | Path) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True), encoding="utf-8"
        )

    def summary(self) -> str:
        lines = [
            f"verdict: {self.verdict}",
            f"expected={self.expected_count} kafka={self.kafka_count} "
            f"sink_logical={self.sink_logical_count} "
            f"sink_physical={self.sink_physical_count} "
            f"duplicate_rows={self.duplicate_rows}",
        ]
        if self.missing_from_kafka:
            lines.append(f"MISSING FROM KAFKA: {len(self.missing_from_kafka)}")
        if self.missing_from_sink:
            lines.append(f"MISSING FROM CLICKHOUSE: {len(self.missing_from_sink)}")
        if self.extra_in_sink:
            lines.append(f"EXTRA IN CLICKHOUSE: {len(self.extra_in_sink)}")
        if self.conflicts:
            lines.append(f"IDENTITY CONFLICTS: {len(self.conflicts)}")
        if self.digest_mismatches:
            lines.append(f"DIGEST MISMATCH VS SOURCE: {len(self.digest_mismatches)}")
        for reason in self.reasons:
            lines.append(f"  - {reason}")
        return "\n".join(lines)


def _index(events: list[ObservedEvent]) -> dict[EventIdentity, list[str]]:
    out: dict[EventIdentity, list[str]] = {}
    for event in events:
        out.setdefault(event.identity, []).append(event.payload_digest)
    return out


def reconcile(
    *,
    expected: list[ObservedEvent] | None,
    kafka_observed: list[ObservedEvent],
    sink_observed: list[ObservedEvent],
    gaps: EvidenceGaps | None = None,
) -> ReconciliationReport:
    """Compare the three sources and return a verdict.

    ``expected=None`` means no source manifest was available. The comparison
    still runs against Kafka, but the verdict can never be GO: without the
    manifest we cannot distinguish "the source never produced it" from "it was
    lost before Kafka" (spec 9.2, low-confidence list).
    """
    gaps = gaps or EvidenceGaps()
    if expected is None:
        gaps.missing_source_manifest = True

    expected_index = _index(expected or [])
    kafka_index = _index(kafka_observed)
    sink_index = _index(sink_observed)

    expected_keys = set(expected_index)
    kafka_keys = set(kafka_index)
    sink_keys = set(sink_index)

    missing_from_kafka = sorted(expected_keys - kafka_keys) if expected else []
    missing_from_sink = sorted(kafka_keys - sink_keys)
    extra_in_sink = sorted(sink_keys - kafka_keys)

    # A conflict is more than one *distinct* digest for one identity. Repeated
    # identical digests are physical duplicates, which is a different thing.
    conflicts: dict[EventIdentity, list[str]] = {}
    duplicate_physical: dict[EventIdentity, int] = {}
    for identity, digests in sink_index.items():
        distinct = set(digests)
        if len(distinct) > 1:
            conflicts[identity] = sorted(distinct)
        if len(digests) > 1:
            duplicate_physical[identity] = len(digests)

    # Kafka-side conflicts matter too: the same identity published twice with
    # different payloads is an upstream fault, not an ingestion fault, and must
    # not be attributed to P05.
    for identity, digests in kafka_index.items():
        if len(set(digests)) > 1:
            conflicts.setdefault(identity, sorted(set(digests)))

    # Field-level equality, not just identity presence. This is what catches a
    # pipeline that preserved the key but mangled a price (spec NFR-01).
    digest_mismatches: list[EventIdentity] = []
    for identity in sorted(sink_keys & kafka_keys):
        if identity in conflicts:
            continue
        if set(sink_index[identity]) != set(kafka_index[identity]):
            digest_mismatches.append(identity)
    if expected:
        for identity in sorted(expected_keys & sink_keys):
            if identity in conflicts or identity in digest_mismatches:
                continue
            if set(sink_index[identity]) != set(expected_index[identity]):
                digest_mismatches.append(identity)

    reasons: list[str] = []
    if missing_from_kafka:
        reasons.append(
            f"{len(missing_from_kafka)} expected identity(ies) never reached Kafka"
        )
    if missing_from_sink:
        reasons.append(
            f"{len(missing_from_sink)} identity(ies) present in Kafka are absent "
            "from ClickHouse"
        )
    if extra_in_sink:
        reasons.append(
            f"{len(extra_in_sink)} identity(ies) in ClickHouse were not in the "
            "Kafka cut"
        )
    if conflicts:
        reasons.append(
            f"{len(conflicts)} identity conflict(s): one identity carries more "
            "than one semantic payload; no winner may be chosen automatically"
        )
    if digest_mismatches:
        reasons.append(
            f"{len(digest_mismatches)} identity(ies) differ in payload digest "
            "between sources, so a field was altered in transit"
        )
    if duplicate_physical:
        reasons.append(
            f"{len(duplicate_physical)} identity(ies) have more than one physical "
            "row; expected under at-least-once delivery, and logical "
            "de-duplication must count each as one event"
        )

    fatal = bool(
        missing_from_kafka
        or missing_from_sink
        or extra_in_sink
        or conflicts
        or digest_mismatches
    )

    if fatal:
        verdict = Verdict.NO_GO
    elif gaps.any:
        verdict = Verdict.INCONCLUSIVE
        reasons.extend(gaps.describe())
    else:
        verdict = Verdict.GO

    sorted_duplicates = dict(sorted(duplicate_physical.items()))
    return ReconciliationReport(
        verdict=verdict,
        expected_count=len(expected_keys),
        kafka_count=len(kafka_keys),
        sink_logical_count=len(sink_keys),
        sink_physical_count=len(sink_observed),
        missing_from_kafka=missing_from_kafka,
        missing_from_sink=missing_from_sink,
        extra_in_sink=extra_in_sink,
        duplicate_physical=sorted_duplicates,
        conflicts=dict(sorted(conflicts.items())),
        digest_mismatches=digest_mismatches,
        gaps=gaps,
        reasons=reasons,
    )


# --------------------------------------------------------------------------
# Source adapters. Each one turns a raw source into ObservedEvent lists.
# --------------------------------------------------------------------------


def observed_from_kafka_scan(
    scan: list[tuple[object, object]],
    *,
    validator,
    mapper_digest,
) -> tuple[list[ObservedEvent], list[str]]:
    """Decode a read-only Kafka scan into identities plus digests.

    Returns the observations and the reason codes of anything the scan could
    not interpret. Records the scan rejects are reported, never dropped: a
    record Kafka holds but nobody can parse is a finding.
    """
    from .model import ConsumedRecord

    observed: list[ObservedEvent] = []
    refused: list[str] = []
    for tp, stored in scan:
        record = ConsumedRecord(
            tp=tp,
            offset=stored.offset,
            key=stored.key,
            value=stored.value,
            headers=stored.headers,
        )
        result = validator.validate(record)
        projection = getattr(result, "projection", None)
        if projection is None:
            refused.append(getattr(result, "code", "UNKNOWN"))
            continue
        observed.append(
            ObservedEvent(
                identity=projection.identity, payload_digest=mapper_digest(projection)
            )
        )
    return observed, refused


def observed_from_sink_rows(rows) -> list[ObservedEvent]:
    """Turn ClickHouse rows into observations.

    Reads the stored ``payload_digest`` column rather than recomputing it. That
    is the point: if the ingestion path computed a digest from a mangled
    projection, recomputing from the same mangled row would agree with itself.
    A separate check (``verify_stored_digests``) recomputes and compares.
    """
    return [
        ObservedEvent(
            identity=EventIdentity(
                row["dataset_id"], row["symbol"], int(row["sequence"])
            ),
            payload_digest=row["payload_digest"],
        )
        for row in rows
    ]


def verify_stored_digests(rows) -> list[tuple[EventIdentity, str, str]]:
    """Recompute each row digest from its stored columns and compare.

    Catches a row whose ``payload_digest`` does not describe its own contents,
    which would make every digest-based comparison above vacuous.
    """
    from .canonical_digest import payload_digest as compute
    from .model import EventKind, LogicalProjection

    mismatches: list[tuple[EventIdentity, str, str]] = []
    for row in rows:
        kind = (
            EventKind.TICK if str(row["event_kind"]) == "tick" else EventKind.QUOTE
        )
        projection = LogicalProjection(
            dataset_id=row["dataset_id"],
            symbol=row["symbol"],
            sequence=int(row["sequence"]),
            event_time_ns=int(row["event_time_ns"]),
            price_scale=int(row["price_scale"]),
            kind=kind,
            source_schema_version=row["source_schema_version"],
            bid_ticks=None if row["bid_ticks"] is None else int(row["bid_ticks"]),
            ask_ticks=None if row["ask_ticks"] is None else int(row["ask_ticks"]),
            price_ticks=None if row["price_ticks"] is None else int(row["price_ticks"]),
        )
        recomputed = compute(projection)
        if recomputed != row["payload_digest"]:
            mismatches.append((projection.identity, row["payload_digest"], recomputed))
    return mismatches
