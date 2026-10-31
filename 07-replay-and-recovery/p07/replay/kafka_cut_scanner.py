"""Read-only scan of a sealed Kafka cut, and the cut model itself.

A **cut** is what makes a finite claim possible: per ``(topic, partition)`` a
``[start_offset, end_offset_exclusive)`` range, stamped with the cluster id and
the topic generation. Design 3.3 is strict about it:

* It is captured **after** the producers are fenced, so no writer can extend the
  range underneath the scan.
* ``topic_generation`` is part of it. End offsets from two generations describe
  different logs and :meth:`KafkaCut.merge` refuses to combine them.
* An offset range whose start has fallen below ``log_start_offset`` is
  **unscannable**, and the scanner raises instead of returning a short read. A
  short read is how "nothing is missing" gets reported about a log that no
  longer exists (case ``E2E-20``).

The scanner also enforces the record-level contract that P04 owes P05, because
nothing downstream re-checks it: no MDP1 prefix in the value, a value within the
4 KiB cap, the key equal to the symbol, the topic matching the event kind, and
the partition matching the key's route. Each violation is a finding with the
offending coordinate, not a count.

What comes out is a *classified* inventory. Two records with one identity and
one digest are a physical retry and perfectly fine; two records with one
identity and different semantics are a conflict, which is terminal. The scanner
reports both and resolves neither -- picking a winner is exactly what design 7.3
forbids.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping

from ..contracts import MAX_EVENT_VALUE_BYTES, EventKind, SemanticCore, SourceIdentity, TransportCoord, partition_for_symbol, topic_for_kind
from ..dataplane.codec import Codec, CodecError, DEFAULT_CODEC, looks_like_mdp1
from ..dataplane.kafka_fake import FakeKafka, KafkaError, OffsetExpired
from ..evidence.verdicts import Finding, Reason
from ..oracle.semantic_digest import digest_core

TopicPartition = tuple[str, int]


@dataclass(frozen=True, slots=True)
class KafkaCut:
    """A sealed, finite window over one topic generation."""

    cluster_id: str
    topic_generation: str
    ranges: Mapping[TopicPartition, tuple[int, int]]
    fenced: bool
    captured_at_ns: int
    partition_counts: Mapping[str, int] = field(default_factory=dict)
    producer_unknown_identities: tuple[SourceIdentity, ...] = ()

    @property
    def record_count(self) -> int:
        """Physical records inside the cut, from the offsets alone."""
        return sum(end - start for start, end in self.ranges.values())

    def start_offsets(self) -> dict[TopicPartition, int]:
        return {tp: start for tp, (start, _) in self.ranges.items()}

    def end_offsets(self) -> dict[TopicPartition, int]:
        return {tp: end for tp, (_, end) in self.ranges.items()}

    def merge(self, other: "KafkaCut") -> "KafkaCut":
        """Combine two cuts of the same cluster and generation.

        Refuses across generations. After a republish the offsets restart, so a
        merged range would describe a log that never existed -- the mistake the
        ``topic_generation`` field exists to make impossible.
        """
        if self.cluster_id != other.cluster_id:
            raise ValueError(Reason.CUT_CLUSTER_MISMATCH)
        if self.topic_generation != other.topic_generation:
            raise ValueError(Reason.CUT_GENERATION_MIXED)
        merged = dict(self.ranges)
        for tp, (start, end) in other.ranges.items():
            if tp in merged:
                existing = merged[tp]
                merged[tp] = (min(existing[0], start), max(existing[1], end))
            else:
                merged[tp] = (start, end)
        return KafkaCut(
            cluster_id=self.cluster_id,
            topic_generation=self.topic_generation,
            ranges=merged,
            fenced=self.fenced and other.fenced,
            captured_at_ns=max(self.captured_at_ns, other.captured_at_ns),
            partition_counts=dict(self.partition_counts),
            producer_unknown_identities=self.producer_unknown_identities
            + other.producer_unknown_identities,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "topic_generation": self.topic_generation,
            "fenced": self.fenced,
            "captured_at_ns": self.captured_at_ns,
            "partition_counts": dict(self.partition_counts),
            "record_count": self.record_count,
            "producer_unknown_identities": [
                list(identity) for identity in self.producer_unknown_identities
            ],
            "ranges": {
                f"{topic}/{partition}": {"start": start, "end_exclusive": end}
                for (topic, partition), (start, end) in sorted(self.ranges.items())
            },
        }


@dataclass(frozen=True, slots=True)
class ScannedRecord:
    """One physical record, decoded and placed."""

    coord: TransportCoord
    core: SemanticCore
    digest: str
    key: str


@dataclass(frozen=True, slots=True)
class CutInventory:
    """Everything the scan observed, classified."""

    cut: KafkaCut
    physical_records: int
    logical: Mapping[SourceIdentity, SemanticCore]
    digests: Mapping[SourceIdentity, str]
    physical_counts: Mapping[SourceIdentity, int]
    duplicate_identities: tuple[SourceIdentity, ...]
    identity_conflicts: tuple[tuple[SourceIdentity, tuple[str, ...]], ...]
    findings: tuple[Finding, ...]
    coords: Mapping[SourceIdentity, tuple[TransportCoord, ...]]

    def as_dict(self, sample: int = 20) -> dict[str, Any]:
        return {
            "cut": self.cut.as_dict(),
            "physical_records": self.physical_records,
            "logical_identities": len(self.logical),
            "physical_duplicates": sum(
                count - 1 for count in self.physical_counts.values() if count > 1
            ),
            "duplicate_identity_count": len(self.duplicate_identities),
            "duplicate_identities_sample": [list(i) for i in self.duplicate_identities[:sample]],
            "identity_conflicts": [
                {"identity": list(identity), "fields": list(fields)}
                for identity, fields in self.identity_conflicts[:sample]
            ],
            "identity_conflict_count": len(self.identity_conflicts),
            "findings": [
                {"reason": finding.reason, "detail": dict(finding.detail)}
                for finding in self.findings[:sample]
            ],
            "finding_count": len(self.findings),
        }


class CutUnscannable(Exception):
    """The cut cannot be read, so no completeness claim is possible."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


def scan_cut(
    kafka: FakeKafka,
    cut: KafkaCut,
    *,
    codec: Codec = DEFAULT_CODEC,
    expect_dataset_id: str | None = None,
) -> CutInventory:
    """Scan every partition of ``cut`` exactly once and classify the result.

    Raises :class:`CutUnscannable` when the cut describes a different cluster or
    generation than the broker currently serves, or when a required offset has
    expired. Those are ``INCONCLUSIVE`` conditions: the lab cannot see what it
    needs to, and no amount of surviving services changes that.
    """
    if kafka.cluster_id != cut.cluster_id:
        raise CutUnscannable(Reason.CUT_CLUSTER_MISMATCH, f"{kafka.cluster_id} != {cut.cluster_id}")
    if kafka.topic_generation != cut.topic_generation:
        raise CutUnscannable(
            Reason.CUT_GENERATION_MIXED, f"{kafka.topic_generation} != {cut.topic_generation}"
        )
    if not cut.fenced:
        raise CutUnscannable(Reason.CUT_NOT_FENCED, "cut captured without fencing the producers")

    logical: dict[SourceIdentity, SemanticCore] = {}
    digests: dict[SourceIdentity, str] = {}
    counts: dict[SourceIdentity, int] = {}
    conflicts: list[tuple[SourceIdentity, tuple[str, ...]]] = []
    findings: list[Finding] = []
    coords: dict[SourceIdentity, list[TransportCoord]] = {}
    physical = 0

    for (topic, partition), (start, end) in sorted(cut.ranges.items()):
        try:
            records = list(kafka.scan(topic, partition, start, end))
        except OffsetExpired as exc:
            raise CutUnscannable(Reason.CUT_RANGE_EXPIRED, exc.detail) from exc
        except KafkaError as exc:
            raise CutUnscannable(Reason.CUT_OFFSET_HOLE, exc.detail) from exc
        if len(records) != end - start:
            # The broker returned a different number of records than the offset
            # arithmetic demands. Treated as a hole, never as "that is fine".
            raise CutUnscannable(
                Reason.CUT_OFFSET_HOLE,
                f"{topic}/{partition} expected {end - start} records, scanned {len(records)}",
            )

        for record in records:
            physical += 1
            coord = TransportCoord(cut.cluster_id, cut.topic_generation, topic, partition, record.offset)
            if looks_like_mdp1(record.value):
                findings.append(
                    Finding(Reason.KAFKA_VALUE_HAS_MDP1_PREFIX, {"coord": list(coord)})
                )
                continue
            if len(record.value) > MAX_EVENT_VALUE_BYTES:
                findings.append(
                    Finding(
                        Reason.KAFKA_VALUE_TOO_LARGE,
                        {"coord": list(coord), "bytes": len(record.value)},
                    )
                )
                continue
            try:
                core = codec.decode(record.value)
            except CodecError as exc:
                findings.append(
                    Finding(Reason.KAFKA_DECODE_ERROR, {"coord": list(coord), "reason": exc.reason})
                )
                continue

            key = record.key.decode("utf-8", errors="replace")
            if key != core.symbol:
                findings.append(
                    Finding(
                        Reason.KAFKA_WRONG_PARTITION_FOR_KEY,
                        {"coord": list(coord), "key": key, "symbol": core.symbol},
                    )
                )
            if topic != topic_for_kind(core.event_kind):
                findings.append(
                    Finding(
                        Reason.KAFKA_WRONG_TOPIC_FOR_KIND,
                        {"coord": list(coord), "kind": core.event_kind.wire_name},
                    )
                )
            expected_partition = partition_for_symbol(
                core.symbol, cut.partition_counts.get(topic, kafka.partition_count(topic))
            )
            if partition != expected_partition:
                findings.append(
                    Finding(
                        Reason.KAFKA_WRONG_PARTITION_FOR_KEY,
                        {
                            "coord": list(coord),
                            "expected_partition": expected_partition,
                            "note": "key affinity holds only within one topic generation",
                        },
                    )
                )
            if expect_dataset_id is not None and core.dataset_id != expect_dataset_id:
                findings.append(
                    Finding(
                        Reason.EXTRA_IDENTITIES,
                        {"coord": list(coord), "dataset_id": core.dataset_id},
                    )
                )
                continue

            identity = core.identity
            digest = digest_core(core)
            counts[identity] = counts.get(identity, 0) + 1
            coords.setdefault(identity, []).append(coord)
            known = logical.get(identity)
            if known is None:
                logical[identity] = core
                digests[identity] = digest
            elif known != core:
                conflicts.append((identity, known.differing_fields(core)))

    duplicates = tuple(identity for identity, count in sorted(counts.items()) if count > 1)
    for identity, fields in conflicts:
        findings.append(
            Finding(Reason.IDENTITY_CONFLICT, {"identity": list(identity), "fields": list(fields)})
        )

    return CutInventory(
        cut=cut,
        physical_records=physical,
        logical=logical,
        digests=digests,
        physical_counts=counts,
        duplicate_identities=duplicates,
        identity_conflicts=tuple(conflicts),
        findings=tuple(findings),
        coords={identity: tuple(items) for identity, items in coords.items()},
    )


def per_symbol_sequences(inventory: CutInventory) -> dict[str, tuple[int, ...]]:
    """Sequences per symbol from the whole cut, ticks and quotes together.

    Deliberately not per topic. P01's sequence runs across both kinds for a
    symbol, so scanning only ``md.ticks.v1`` and finding 1, 3, 5 shows no gap at
    all -- the even numbers are quotes. Case ``E2E-24`` is exactly this
    confusion, and this function is the shape that avoids it.
    """
    by_symbol: dict[str, list[int]] = {}
    for identity in inventory.logical:
        by_symbol.setdefault(identity.symbol, []).append(identity.sequence)
    return {symbol: tuple(sorted(seqs)) for symbol, seqs in sorted(by_symbol.items())}


def tick_only_sequences(inventory: CutInventory) -> dict[str, tuple[int, ...]]:
    """Sequences of tick events only -- the misleading view, for the test to compare against."""
    by_symbol: dict[str, list[int]] = {}
    for identity, core in inventory.logical.items():
        if core.event_kind is EventKind.TICK:
            by_symbol.setdefault(identity.symbol, []).append(identity.sequence)
    return {symbol: tuple(sorted(seqs)) for symbol, seqs in sorted(by_symbol.items())}


def iter_logical(inventory: CutInventory) -> Iterator[tuple[SourceIdentity, SemanticCore]]:
    yield from sorted(inventory.logical.items())
