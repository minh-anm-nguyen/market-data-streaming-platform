"""Seal a finite source snapshot, or refuse to.

Section 5.1 S1 and matrix group F. A cut is what makes "complete" a provable
claim rather than a hope, and almost every way of taking one is wrong:

* **``consumer lag == 0`` is not a cut.** The consumer can have fetched records
  whose sink INSERT has not been acknowledged, so a lag of zero says nothing
  about what is durable in ClickHouse (case F-01). The sealer refuses that as a
  reason outright.
* **``now()`` and ``ingested_at`` are not cuts.** They are wall clocks on
  whichever host read them, unrelated to the source's event stream.
* **``SELECT count()`` is not a cut.** A matching count over a lossy pipeline is
  a coincidence; section 0.2 is blunt about this.
* **A cut is per topic-partition**, including the topic generation, because a
  recreated topic restarts offsets and there is no total order across topics
  (case F-04, F-11, F-13).

If any of the evidence is missing the result is ``INCONCLUSIVE`` and the build
does not start. That is a correct outcome, not a failure to try harder.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, Final

from .contracts import EVENT_TICK, SourceIdentity, require_digest_format
from .errors import SnapshotCutError
from .rows import TopicPartition

#: Reasons a caller may offer for sealing. ``consumer_lag_zero`` is listed so it
#: can be *named and rejected*: an unknown reason and a known-bad reason deserve
#: different error messages (case F-01).
VALID_SEAL_REASONS: Final = frozenset({"durable_sink_frontier"})
REJECTED_SEAL_REASONS: Final = frozenset(
    {"consumer_lag_zero", "row_count_match", "wall_clock", "operator_judgement"}
)

#: P05 states from which a cut may be taken. Anything else -- still consuming,
#: rebalancing, crashed -- leaves the frontier moving under the audit's feet.
TERMINAL_P05_STATES: Final = frozenset({"DRAINED", "STOPPED_AT_OFFSETS"})


@dataclass(frozen=True, slots=True)
class AcceptedEvent:
    """One event the P03 adapter published, per its accepted-event ledger.

    The expected set comes from this ledger, not from everything P01 generated:
    P03 may legitimately suppress an invalid event, and demanding it appear in
    raw would manufacture a missing-row report (case A-04, F-03).
    """

    identity: SourceIdentity
    event_kind: str
    payload_digest: str

    def __post_init__(self) -> None:
        require_digest_format(self.payload_digest)


@dataclass(frozen=True, slots=True)
class SourceManifest:
    """The P01 dataset manifest joined to the P03 accepted-event ledger."""

    dataset_id: str
    accepted: tuple[AcceptedEvent, ...]
    rejected: tuple[tuple[SourceIdentity, str], ...] = ()
    #: SHA declared *by the producer*. Compared against a recomputation, never
    #: trusted: that comparison is the whole of case F-14.
    declared_sha256: str | None = None
    generator_version: str = "unknown"

    def content_digest(self) -> str:
        """Recompute the manifest digest from its content, in a fixed order."""
        payload = {
            "dataset_id": self.dataset_id,
            "generator_version": self.generator_version,
            "accepted": sorted(
                [list(e.identity), e.event_kind, e.payload_digest] for e in self.accepted
            ),
            "rejected": sorted([list(i), reason] for i, reason in self.rejected),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    def verify_integrity(self) -> None:
        if self.declared_sha256 is None:
            raise SnapshotCutError(
                "source manifest has no declared SHA-256, so provenance cannot be "
                "checked; verdict is INCONCLUSIVE rather than a guess",
                code="MANIFEST_SHA_MISSING",
                dataset_id=self.dataset_id,
            )
        actual = self.content_digest()
        if actual != self.declared_sha256:
            raise SnapshotCutError(
                f"source manifest digest mismatch: declared {self.declared_sha256}, "
                f"recomputed {actual}",
                code="MANIFEST_TAMPERED",
                dataset_id=self.dataset_id,
            )

    def accepted_identities(self) -> frozenset[SourceIdentity]:
        return frozenset(e.identity for e in self.accepted)

    def accepted_tick_identities(self) -> frozenset[SourceIdentity]:
        return frozenset(e.identity for e in self.accepted if e.event_kind == EVENT_TICK)

    def expected_digests(self) -> dict[SourceIdentity, str]:
        return {e.identity: e.payload_digest for e in self.accepted}

    def symbols(self) -> frozenset[str]:
        return frozenset(e.identity.symbol for e in self.accepted)


@dataclass(frozen=True, slots=True)
class TopicPartitionCut:
    """End offset of one topic-partition, with the consumer frontier past it."""

    topic_partition: TopicPartition
    #: Kafka high-water mark at the moment of capture, exclusive.
    end_offset: int
    #: P05's durable next-offset: everything below it has been acknowledged by
    #: the sink, not merely fetched.
    consumer_frontier: int
    #: Oldest offset Kafka still holds. If retention has passed the cut, the
    #: source can no longer be re-read and completeness is unprovable (F-02).
    earliest_available_offset: int = 0

    def covers_cut(self) -> bool:
        return self.consumer_frontier >= self.end_offset

    def retention_lost(self) -> bool:
        return self.earliest_available_offset > 0 and self.end_offset > 0


@dataclass(frozen=True, slots=True)
class SnapshotCut:
    """A sealed, finite view of the source. Immutable once constructed."""

    dataset_id: str
    manifest_sha256: str
    cuts: tuple[TopicPartitionCut, ...]
    p05_state: str
    seal_reason: str
    captured_at_ns: int
    notes: tuple[str, ...] = field(default=())

    def cut_digest(self) -> str:
        """Digest of the transport coordinates, for the ``build_id`` inputs."""
        payload = sorted(
            [
                c.topic_partition.topic,
                c.topic_partition.partition,
                c.topic_partition.topic_generation,
                c.end_offset,
            ]
            for c in self.cuts
        )
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    def offset_bound(self, tp: TopicPartition) -> int | None:
        for cut in self.cuts:
            if cut.topic_partition == tp:
                return cut.end_offset
        return None

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "manifest_sha256": self.manifest_sha256,
            "p05_state": self.p05_state,
            "seal_reason": self.seal_reason,
            "captured_at_ns": self.captured_at_ns,
            "cut_digest": self.cut_digest(),
            "cuts": [
                {
                    "topic": c.topic_partition.topic,
                    "partition": c.topic_partition.partition,
                    "topic_generation": c.topic_partition.topic_generation,
                    "end_offset": c.end_offset,
                    "consumer_frontier": c.consumer_frontier,
                    "earliest_available_offset": c.earliest_available_offset,
                }
                for c in sorted(
                    self.cuts,
                    key=lambda c: (
                        c.topic_partition.topic,
                        c.topic_partition.partition,
                        c.topic_partition.topic_generation,
                    ),
                )
            ],
            "notes": list(self.notes),
        }


def seal_snapshot(
    *,
    manifest: SourceManifest,
    cuts: Sequence[TopicPartitionCut],
    p05_state: str,
    seal_reason: str,
    captured_at_ns: int,
    expected_topic_partitions: Iterable[TopicPartition] | None = None,
) -> SnapshotCut:
    """Validate every piece of cut evidence, or raise :class:`SnapshotCutError`.

    ``expected_topic_partitions`` lets the caller state the topology it believes
    exists. Without it a cut over 15 of 16 partitions looks complete, and the
    missing partition's events would be reported as source loss much later, with
    no way to tell the two apart (case F-13, A-14).
    """
    manifest.verify_integrity()

    if seal_reason in REJECTED_SEAL_REASONS:
        raise SnapshotCutError(
            f"{seal_reason!r} is not a valid basis for a cut: it does not "
            f"establish what the sink has durably acknowledged",
            code="SEAL_REASON_REJECTED",
            seal_reason=seal_reason,
        )
    if seal_reason not in VALID_SEAL_REASONS:
        raise SnapshotCutError(
            f"unknown seal reason {seal_reason!r}; expected one of "
            f"{sorted(VALID_SEAL_REASONS)}",
            code="SEAL_REASON_UNKNOWN",
            seal_reason=seal_reason,
        )
    if p05_state not in TERMINAL_P05_STATES:
        raise SnapshotCutError(
            f"P05 is in state {p05_state!r}, not a terminal state "
            f"{sorted(TERMINAL_P05_STATES)}; the frontier may still move",
            code="P05_NOT_TERMINAL",
            p05_state=p05_state,
        )
    if not cuts:
        raise SnapshotCutError(
            "a finite snapshot needs explicit per-topic-partition bounds; an "
            "unbounded live stream cannot be sealed",
            code="CUT_EMPTY",
            dataset_id=manifest.dataset_id,
        )

    seen: set[TopicPartition] = set()
    for cut in cuts:
        tp = cut.topic_partition
        if tp in seen:
            raise SnapshotCutError(
                f"duplicate cut for {tp}", code="CUT_DUPLICATE_TP", topic_partition=str(tp)
            )
        seen.add(tp)
        if tp.topic_generation <= 0:
            raise SnapshotCutError(
                f"{tp} has no topic generation; offsets are ambiguous after a "
                f"topic is recreated",
                code="CUT_MISSING_GENERATION",
                topic_partition=str(tp),
            )
        if cut.end_offset < 0 or cut.consumer_frontier < 0:
            raise SnapshotCutError(
                f"{tp} has negative offsets", code="CUT_NEGATIVE_OFFSET", topic_partition=str(tp)
            )
        if not cut.covers_cut():
            raise SnapshotCutError(
                f"{tp}: consumer frontier {cut.consumer_frontier} is behind the cut "
                f"{cut.end_offset}; records up to the cut are not durable yet",
                code="CUT_FRONTIER_BEHIND",
                topic_partition=str(tp),
                consumer_frontier=cut.consumer_frontier,
                end_offset=cut.end_offset,
            )
        if cut.retention_lost():
            raise SnapshotCutError(
                f"{tp}: Kafka retention has dropped offsets below "
                f"{cut.earliest_available_offset}, so the source inside the cut "
                f"can no longer be re-read; completeness is unprovable",
                code="CUT_RETENTION_LOST",
                topic_partition=str(tp),
                earliest_available_offset=cut.earliest_available_offset,
            )

    if expected_topic_partitions is not None:
        expected = set(expected_topic_partitions)
        missing = expected - seen
        unexpected = seen - expected
        if missing or unexpected:
            raise SnapshotCutError(
                f"cut topology mismatch: missing {sorted(map(str, missing))}, "
                f"unexpected {sorted(map(str, unexpected))}",
                code="CUT_TOPOLOGY_MISMATCH",
                missing=sorted(map(str, missing)),
                unexpected=sorted(map(str, unexpected)),
            )

    return SnapshotCut(
        dataset_id=manifest.dataset_id,
        manifest_sha256=manifest.content_digest(),
        cuts=tuple(cuts),
        p05_state=p05_state,
        seal_reason=seal_reason,
        captured_at_ns=captured_at_ns,
        notes=(
            f"accepted={len(manifest.accepted)}",
            f"rejected={len(manifest.rejected)}",
            f"topic_partitions={len(cuts)}",
        ),
    )
