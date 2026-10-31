"""Reconcile a canonical build against the expected set -- ``L4`` in design 3.4.

A canonical build earns three statements and no more until the bars are checked
too:

1. exactly one row per :class:`~p07.contracts.SourceIdentity`,
2. every row's semantic core equal to the expected core, field by field,
3. no identity present that the policy did not accept.

Uniqueness gets its own check rather than being assumed from the storage.
``MergeTree`` does not enforce it (P06 section 4.1), so "one row per identity"
is something the builder claims and an independent reader confirms -- here, by
counting rows per identity rather than by trusting the table's shape.

The digest is recomputed from each row's own core and compared with the stored
one. A row whose digest disagrees with its data is reported as a semantic
mismatch even if the core happens to equal the expectation, because one of the
two values was written wrong and there is no way to tell which.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from ..contracts import SemanticCore, SourceIdentity
from ..dataplane.clickhouse_fake import CanonicalRow
from ..evidence.verdicts import Finding, Reason
from ..oracle.acceptance_oracle import AcceptanceExpectation
from ..oracle.semantic_digest import digest_core
from .diff import IdentityDiff, diff_identity_maps


@dataclass(frozen=True, slots=True)
class CanonicalReconciliation:
    build_id: str
    diff: IdentityDiff
    rows: int
    non_unique_identities: tuple[tuple[SourceIdentity, int], ...]
    digest_mismatches: tuple[SourceIdentity, ...]
    findings: tuple[Finding, ...]

    @property
    def verified(self) -> bool:
        return not self.findings

    def as_dict(self, sample: int = 20) -> dict[str, Any]:
        return {
            "build_id": self.build_id,
            "diff": self.diff.as_dict(sample=sample),
            "rows": self.rows,
            "verified": self.verified,
            "non_unique_identity_count": len(self.non_unique_identities),
            "non_unique_sample": [
                {"identity": list(identity), "rows": count}
                for identity, count in self.non_unique_identities[:sample]
            ],
            "digest_mismatch_count": len(self.digest_mismatches),
            "digest_mismatch_sample": [list(i) for i in self.digest_mismatches[:sample]],
            "findings": [
                {"reason": f.reason, "detail": dict(f.detail)} for f in self.findings[:sample]
            ],
        }


def reconcile_canonical(
    expectation: AcceptanceExpectation,
    build_id: str,
    rows: Iterable[CanonicalRow],
    *,
    cut_proven: bool = True,
) -> CanonicalReconciliation:
    """Compare a canonical build with the accepted set.

    Rows belonging to another ``build_id`` are a finding, not something to filter
    out: mixing two builds is how a verified snapshot ends up containing
    unverified rows.
    """
    observed: dict[SourceIdentity, SemanticCore] = {}
    counts: dict[SourceIdentity, int] = {}
    digest_mismatches: list[SourceIdentity] = []
    wrong_build: list[SourceIdentity] = []
    total = 0

    for row in rows:
        total += 1
        if row.build_id != build_id:
            wrong_build.append(row.core.identity)
            continue
        identity = row.core.identity
        counts[identity] = counts.get(identity, 0) + 1
        if digest_core(row.core) != row.payload_digest:
            digest_mismatches.append(identity)
        observed.setdefault(identity, row.core)

    diff = diff_identity_maps(
        dict(expectation.accepted), observed, physical_counts=counts, cut_proven=cut_proven
    )
    non_unique = tuple(
        (identity, count) for identity, count in sorted(counts.items()) if count > 1
    )

    findings = list(diff.findings())
    if non_unique:
        findings.append(
            Finding(
                Reason.CANONICAL_NOT_UNIQUE,
                {
                    "count": len(non_unique),
                    "sample": [
                        {"identity": list(identity), "rows": count}
                        for identity, count in non_unique[:10]
                    ],
                },
            )
        )
    if digest_mismatches:
        findings.append(
            Finding(
                Reason.SEMANTIC_MISMATCH,
                {
                    "kind": "canonical_digest_disagrees_with_columns",
                    "count": len(digest_mismatches),
                    "sample": [list(i) for i in digest_mismatches[:10]],
                },
            )
        )
    if wrong_build:
        findings.append(
            Finding(
                Reason.BUILD_DIRTY,
                {
                    "kind": "rows_from_another_build",
                    "count": len(wrong_build),
                    "sample": [list(i) for i in wrong_build[:10]],
                },
            )
        )

    return CanonicalReconciliation(
        build_id=build_id,
        diff=diff,
        rows=total,
        non_unique_identities=non_unique,
        digest_mismatches=tuple(dict.fromkeys(digest_mismatches)),
        findings=tuple(findings),
    )
