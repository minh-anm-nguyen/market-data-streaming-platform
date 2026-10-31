"""Reconcile the transport cut and the raw table against the expected set.

Two boundaries live here because they share one question -- "is every expected
identity present exactly once logically, whatever the physical copies" -- and
because the raw table's provenance points straight back at the Kafka records
this module has already scanned.

The transport comparison, ``L2`` in design 3.4: the scanned Kafka cut's logical
projection must equal the accepted set ``A``. Suppressed events must be
**absent**, and that absence is correct, not missing data. :func:`reconcile_kafka_cut`
therefore checks two things: that every accepted identity is present, and that
no identity the policy suppressed or rejected slipped through. Conflating those
with transport loss is what ``INV-15`` forbids.

The raw comparison, ``L3``: every raw row must carry provenance that the Kafka
scan confirms, its stored ``payload_digest`` must match a digest recomputed from
its own columns, physical duplicates are allowed and counted, and the logical
projection must again equal ``A``. A row whose provenance names a coordinate the
cut does not contain is an ``extra`` with a transport coordinate attached, which
is far more useful to an operator than a count.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from ..contracts import SemanticCore, SourceIdentity, TransportCoord
from ..dataplane.clickhouse_fake import RawRow
from ..evidence.verdicts import Finding, Reason
from ..oracle.acceptance_oracle import AcceptanceExpectation
from ..oracle.semantic_digest import digest_core
from ..replay.kafka_cut_scanner import CutInventory
from .diff import IdentityDiff, diff_identity_maps


@dataclass(frozen=True, slots=True)
class TransportReconciliation:
    """Result of comparing the Kafka cut with the expected accepted set."""

    diff: IdentityDiff
    leaked_suppressed: tuple[SourceIdentity, ...]
    leaked_rejected: tuple[SourceIdentity, ...]
    physical_duplicate_rows: int
    findings: tuple[Finding, ...]

    def as_dict(self, sample: int = 20) -> dict[str, Any]:
        return {
            "diff": self.diff.as_dict(sample=sample),
            "leaked_suppressed_count": len(self.leaked_suppressed),
            "leaked_suppressed_sample": [list(i) for i in self.leaked_suppressed[:sample]],
            "leaked_rejected_count": len(self.leaked_rejected),
            "leaked_rejected_sample": [list(i) for i in self.leaked_rejected[:sample]],
            "physical_duplicate_rows": self.physical_duplicate_rows,
            "findings": [
                {"reason": f.reason, "detail": dict(f.detail)} for f in self.findings[:sample]
            ],
        }


def reconcile_kafka_cut(
    expectation: AcceptanceExpectation,
    inventory: CutInventory,
) -> TransportReconciliation:
    """Compare the scanned cut against the policy's accepted set.

    A suppressed or rejected identity found in Kafka is reported separately from
    an ``extra``: it means the adapter published something its policy said it
    would not, which localises the bug to P03's output stage rather than to a
    stray producer.
    """
    diff = diff_identity_maps(
        dict(expectation.accepted),
        dict(inventory.logical),
        physical_counts=inventory.physical_counts,
        cut_proven=inventory.cut.fenced,
    )
    observed = set(inventory.logical)
    suppressed_ids = {
        event.identity
        for event in expectation.suppressed
        if event.identity is not None and event.identity not in expectation.accepted
    }
    rejected_ids = {
        event.identity
        for event in expectation.rejected
        if event.identity is not None and event.identity not in expectation.accepted
    }
    leaked_suppressed = tuple(sorted(suppressed_ids & observed))
    leaked_rejected = tuple(sorted(rejected_ids & observed))

    findings = list(diff.findings())
    findings.extend(inventory.findings)
    if leaked_suppressed:
        findings.append(
            Finding(
                Reason.EXTRA_IDENTITIES,
                {
                    "kind": "policy_suppressed_but_published",
                    "count": len(leaked_suppressed),
                    "sample": [list(i) for i in leaked_suppressed[:10]],
                },
            )
        )
    if leaked_rejected:
        findings.append(
            Finding(
                Reason.EXTRA_IDENTITIES,
                {
                    "kind": "policy_rejected_but_published",
                    "count": len(leaked_rejected),
                    "sample": [list(i) for i in leaked_rejected[:10]],
                },
            )
        )
    duplicate_rows = sum(count - 1 for count in inventory.physical_counts.values() if count > 1)
    return TransportReconciliation(
        diff=diff,
        leaked_suppressed=leaked_suppressed,
        leaked_rejected=leaked_rejected,
        physical_duplicate_rows=duplicate_rows,
        findings=tuple(findings),
    )


@dataclass(frozen=True, slots=True)
class RawReconciliation:
    """Result of comparing the raw table with the expected accepted set."""

    diff: IdentityDiff
    physical_rows: int
    physical_duplicate_rows: int
    digest_mismatches: tuple[SourceIdentity, ...]
    unverified_provenance: tuple[dict[str, Any], ...]
    wrong_generation_rows: int
    findings: tuple[Finding, ...]

    def as_dict(self, sample: int = 20) -> dict[str, Any]:
        return {
            "diff": self.diff.as_dict(sample=sample),
            "physical_rows": self.physical_rows,
            "physical_duplicate_rows": self.physical_duplicate_rows,
            "digest_mismatch_count": len(self.digest_mismatches),
            "digest_mismatch_sample": [list(i) for i in self.digest_mismatches[:sample]],
            "unverified_provenance_count": len(self.unverified_provenance),
            "unverified_provenance_sample": list(self.unverified_provenance[:sample]),
            "wrong_generation_rows": self.wrong_generation_rows,
            "findings": [
                {"reason": f.reason, "detail": dict(f.detail)} for f in self.findings[:sample]
            ],
        }


def reconcile_raw(
    expectation: AcceptanceExpectation,
    rows: Iterable[RawRow],
    *,
    inventory: CutInventory | None = None,
    cut_proven: bool = True,
    rows_inserted: int | None = None,
) -> RawReconciliation:
    """Compare raw rows against ``A``, checking digests and provenance.

    ``inventory`` is the scanned cut. When supplied, each row's
    ``(topic, partition, offset)`` must appear in the cut for that identity;
    rows that do not are listed in ``unverified_provenance``. When omitted, the
    provenance check is skipped and that omission shows up in the report -- so a
    run without a scan cannot look like one with it.

    ``rows_inserted`` is what the ingestor says it wrote. It is used for exactly
    one discrimination, and an important one: if fewer physical rows are present
    than were inserted, the rows were **removed after the fact** -- a TTL or a
    drop -- and the finding is
    :data:`~p07.evidence.verdicts.Reason.RAW_ROWS_EXPIRED` (``INCONCLUSIVE``),
    not ``MISSING_IDENTITIES`` (``FAIL``). "P05 never wrote it" and "it was
    written and then deleted" are different incidents with different runbooks
    (design 7.3 versus case ``E2E-33``), and a harness that reports both as
    missing data sends the operator to the wrong one.
    """
    observed: dict[SourceIdentity, SemanticCore] = {}
    counts: dict[SourceIdentity, int] = {}
    conflicts: list[tuple[SourceIdentity, tuple[str, ...]]] = []
    digest_mismatches: list[SourceIdentity] = []
    unverified: list[dict[str, Any]] = []
    wrong_generation = 0
    physical = 0

    known_coords: dict[SourceIdentity, set[TransportCoord]] = {}
    expected_generation: str | None = None
    if inventory is not None:
        expected_generation = inventory.cut.topic_generation
        known_coords = {identity: set(coords) for identity, coords in inventory.coords.items()}

    for row in rows:
        physical += 1
        core = row.semantic_core()
        identity = row.identity
        counts[identity] = counts.get(identity, 0) + 1
        if digest_core(core) != row.payload_digest:
            digest_mismatches.append(identity)
        known = observed.get(identity)
        if known is None:
            observed[identity] = core
        elif known != core:
            conflicts.append((identity, known.differing_fields(core)))

        if inventory is not None:
            if expected_generation is not None and row.topic_generation != expected_generation:
                wrong_generation += 1
                unverified.append(
                    {
                        "identity": list(identity),
                        "reason": Reason.CUT_GENERATION_MIXED,
                        "row_generation": row.topic_generation,
                        "cut_generation": expected_generation,
                    }
                )
                continue
            coord = TransportCoord(
                inventory.cut.cluster_id,
                row.topic_generation,
                row.topic,
                row.partition,
                row.offset,
            )
            if coord not in known_coords.get(identity, set()):
                unverified.append(
                    {
                        "identity": list(identity),
                        "reason": "PROVENANCE_NOT_IN_CUT",
                        "coord": list(coord),
                    }
                )

    diff = diff_identity_maps(
        dict(expectation.accepted), observed, physical_counts=counts, cut_proven=cut_proven
    )
    rows_vanished = rows_inserted is not None and physical < rows_inserted
    findings: list[Finding] = []
    if rows_vanished:
        findings.append(
            Finding(
                Reason.RAW_ROWS_EXPIRED,
                {
                    "rows_inserted": rows_inserted,
                    "rows_present": physical,
                    "rows_removed": rows_inserted - physical,
                    "missing_identities": len(diff.missing),
                },
            )
        )
        # The missing-identity findings are suppressed in favour of the one
        # above: they are a consequence of the deletion, not independent
        # evidence of lost data, and reporting both would double-count it.
        findings.extend(
            finding for finding in diff.findings() if finding.reason != Reason.MISSING_IDENTITIES
        )
    else:
        findings.extend(diff.findings())
    for identity, fields in conflicts:
        findings.append(
            Finding(Reason.IDENTITY_CONFLICT, {"identity": list(identity), "fields": list(fields)})
        )
    if digest_mismatches:
        findings.append(
            Finding(
                Reason.SEMANTIC_MISMATCH,
                {
                    "kind": "stored_digest_disagrees_with_columns",
                    "count": len(digest_mismatches),
                    "sample": [list(i) for i in digest_mismatches[:10]],
                },
            )
        )
    if unverified:
        findings.append(
            Finding(
                Reason.EXTRA_IDENTITIES,
                {
                    "kind": "unverified_provenance",
                    "count": len(unverified),
                    "sample": unverified[:10],
                },
            )
        )

    return RawReconciliation(
        diff=diff,
        physical_rows=physical,
        physical_duplicate_rows=sum(count - 1 for count in counts.values() if count > 1),
        digest_mismatches=tuple(dict.fromkeys(digest_mismatches)),
        unverified_provenance=tuple(unverified),
        wrong_generation_rows=wrong_generation,
        findings=tuple(findings),
    )


def frontier_findings(
    frontier: Mapping[str, Any],
    *,
    cut_end: Mapping[str, int] | None = None,
) -> list[Finding]:
    """Check ``INV-03`` on a reported ingestion frontier.

    ``committed_next`` must never exceed ``completed_contiguous_next``. The
    report is the consumer's own account, so this check catches a consumer that
    *admits* committing too far; a consumer that lies about its frontier is
    caught by the raw diff instead. Both matter, and neither replaces the other.
    """
    findings: list[Finding] = []
    for name, progress in (frontier.get("partitions") or {}).items():
        committed = progress.get("committed_next")
        completed = progress.get("completed_contiguous_next")
        if committed is not None and completed is not None and committed > completed:
            findings.append(
                Finding(
                    Reason.COMMIT_AHEAD_OF_COMPLETION,
                    {"partition": name, "committed_next": committed, "completed_next": completed},
                )
            )
        if cut_end is not None and name in cut_end:
            if completed is not None and completed < cut_end[name]:
                findings.append(
                    Finding(
                        Reason.MISSING_IDENTITIES,
                        {
                            "kind": "frontier_short_of_cut",
                            "partition": name,
                            "completed_next": completed,
                            "cut_end": cut_end[name],
                        },
                    )
                )
        if progress.get("unknown_count"):
            findings.append(
                Finding(
                    Reason.SINK_ACK_UNKNOWN,
                    {"partition": name, "unknown_offsets": progress.get("unknown_offsets")},
                )
            )
    return findings
