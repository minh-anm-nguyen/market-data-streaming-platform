"""The exact identity diff every boundary comparison is built on.

One function, four outcomes, and the vocabulary matters because design 7.3 maps
each to a different decision:

``missing``
    an expected identity that is not present. ``FAIL`` *if the cut is proven
    complete*; ``INCONCLUSIVE`` if it is not, since an unreadable range cannot
    distinguish "lost" from "unknown". The caller supplies ``cut_proven`` and
    this module respects it rather than guessing.
``extra``
    an observed identity nobody expected. Always a finding -- a record that
    should not exist is as bad as one that is gone.
``mismatched``
    same identity, different semantics. This is the identity *conflict*, the
    only P0 that has no benign reading, and the diff names the differing fields
    so the report can show what disagreed.
``physical_duplicates``
    same identity, same semantics, more than one physical row. Allowed by
    contract; recorded, never reduced away silently.

The large-fixture path is :func:`sharded_diff`, which partitions both sides by a
hash of the identity so a 200M-row comparison runs in bounded memory. Design
13.3 asks for exactly that, and it is a different code path from the in-memory
one, so :mod:`tests.property` checks the two agree.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

from ..contracts import SemanticCore, SourceIdentity
from ..evidence.verdicts import Finding, Reason


@dataclass(frozen=True, slots=True)
class IdentityDiff:
    """Result of comparing an expected identity map with an observed one."""

    expected_count: int
    observed_count: int
    missing: tuple[SourceIdentity, ...]
    extra: tuple[SourceIdentity, ...]
    mismatched: tuple[tuple[SourceIdentity, tuple[str, ...]], ...]
    physical_duplicates: Mapping[SourceIdentity, int]
    cut_proven: bool

    @property
    def equal(self) -> bool:
        """Whether the logical sets are identical, duplicates aside."""
        return not self.missing and not self.extra and not self.mismatched

    def findings(self, *, sample: int = 20) -> tuple[Finding, ...]:
        """Turn the diff into gate findings.

        ``missing`` becomes ``INCONCLUSIVE`` rather than ``FAIL`` when the cut
        was not proven, which is the single most important branch in this file:
        a harness that reports ``FAIL`` for data it merely could not read is
        just as untrustworthy as one that reports ``PASS``.
        """
        out: list[Finding] = []
        if self.missing:
            reason = Reason.MISSING_IDENTITIES if self.cut_proven else Reason.CUT_NOT_FENCED
            out.append(
                Finding(
                    reason,
                    {
                        "count": len(self.missing),
                        "sample": [list(identity) for identity in self.missing[:sample]],
                        "cut_proven": self.cut_proven,
                    },
                )
            )
        if self.extra:
            out.append(
                Finding(
                    Reason.EXTRA_IDENTITIES,
                    {
                        "count": len(self.extra),
                        "sample": [list(identity) for identity in self.extra[:sample]],
                    },
                )
            )
        if self.mismatched:
            out.append(
                Finding(
                    Reason.IDENTITY_CONFLICT,
                    {
                        "count": len(self.mismatched),
                        "sample": [
                            {"identity": list(identity), "fields": list(fields)}
                            for identity, fields in self.mismatched[:sample]
                        ],
                    },
                )
            )
        return tuple(out)

    def as_dict(self, *, sample: int = 20) -> dict[str, Any]:
        duplicates = {
            identity: count for identity, count in self.physical_duplicates.items() if count > 1
        }
        return {
            "expected_count": self.expected_count,
            "observed_count": self.observed_count,
            "equal": self.equal,
            "cut_proven": self.cut_proven,
            "missing_count": len(self.missing),
            "missing_sample": [list(identity) for identity in self.missing[:sample]],
            "extra_count": len(self.extra),
            "extra_sample": [list(identity) for identity in self.extra[:sample]],
            "mismatched_count": len(self.mismatched),
            "mismatched_sample": [
                {"identity": list(identity), "fields": list(fields)}
                for identity, fields in self.mismatched[:sample]
            ],
            "physical_duplicate_identities": len(duplicates),
            "physical_duplicate_rows": sum(count - 1 for count in duplicates.values()),
            "physical_duplicate_sample": [
                {"identity": list(identity), "rows": count}
                for identity, count in sorted(duplicates.items())[:sample]
            ],
        }


def diff_identity_maps(
    expected: Mapping[SourceIdentity, SemanticCore],
    observed: Mapping[SourceIdentity, SemanticCore],
    *,
    physical_counts: Mapping[SourceIdentity, int] | None = None,
    cut_proven: bool = True,
) -> IdentityDiff:
    """Compare two identity maps field by field.

    Comparison is on the whole :class:`~p07.contracts.SemanticCore`, which
    includes the presence of every optional field. Comparing digests would be
    equivalent *if* both sides computed the digest the same way -- and checking
    that assumption is the point, so the fields are compared directly and the
    digest is used only as a second opinion in
    :func:`~p07.reconcile.raw_reconciler.reconcile_raw`.
    """
    expected_keys = set(expected)
    observed_keys = set(observed)
    missing = tuple(sorted(expected_keys - observed_keys))
    extra = tuple(sorted(observed_keys - expected_keys))
    mismatched = tuple(
        (identity, expected[identity].differing_fields(observed[identity]))
        for identity in sorted(expected_keys & observed_keys)
        if expected[identity] != observed[identity]
    )
    return IdentityDiff(
        expected_count=len(expected),
        observed_count=len(observed),
        missing=missing,
        extra=extra,
        mismatched=mismatched,
        physical_duplicates=dict(physical_counts or {}),
        cut_proven=cut_proven,
    )


def shard_of(identity: SourceIdentity, shards: int) -> int:
    """Stable shard index for ``identity``.

    SHA-256 of the printable identity rather than :func:`hash`, because
    ``PYTHONHASHSEED`` randomises ``str`` hashing per process and a shard map
    that changes between runs would make a resumed diff compare the wrong halves.
    """
    if shards <= 0:
        raise ValueError("shards must be positive")
    blob = f"{identity.dataset_id}\x00{identity.symbol}\x00{identity.sequence}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(blob).digest()[:8], "big") % shards


def sharded_diff(
    expected_rows: Callable[[], Iterable[tuple[SourceIdentity, SemanticCore]]],
    observed_rows: Callable[[], Iterable[tuple[SourceIdentity, SemanticCore]]],
    *,
    shards: int = 16,
    cut_proven: bool = True,
) -> IdentityDiff:
    """Exact diff in bounded memory, one shard at a time.

    Both arguments are *callables* returning fresh iterables, because the inputs
    are re-read once per shard. Peak memory is therefore the largest shard, not
    the whole dataset -- which is what makes an exact 200M-row comparison
    possible at all instead of falling back to sampling.
    """
    missing: list[SourceIdentity] = []
    extra: list[SourceIdentity] = []
    mismatched: list[tuple[SourceIdentity, tuple[str, ...]]] = []
    physical: dict[SourceIdentity, int] = {}
    expected_total = observed_total = 0

    for shard in range(shards):
        expected_shard: dict[SourceIdentity, SemanticCore] = {}
        for identity, core in expected_rows():
            if shard_of(identity, shards) == shard:
                expected_shard[identity] = core
        observed_shard: dict[SourceIdentity, SemanticCore] = {}
        for identity, core in observed_rows():
            if shard_of(identity, shards) != shard:
                continue
            physical[identity] = physical.get(identity, 0) + 1
            if identity not in observed_shard:
                observed_shard[identity] = core
            elif observed_shard[identity] != core:
                mismatched.append((identity, observed_shard[identity].differing_fields(core)))

        expected_total += len(expected_shard)
        observed_total += len(observed_shard)
        partial = diff_identity_maps(expected_shard, observed_shard, cut_proven=cut_proven)
        missing.extend(partial.missing)
        extra.extend(partial.extra)
        mismatched.extend(partial.mismatched)

    # Deduplicate conflicts: an identity can be reported both by the
    # within-shard duplicate scan and by the expected-vs-observed comparison.
    seen: dict[SourceIdentity, tuple[str, ...]] = {}
    for identity, fields in mismatched:
        seen.setdefault(identity, fields)
    return IdentityDiff(
        expected_count=expected_total,
        observed_count=observed_total,
        missing=tuple(sorted(missing)),
        extra=tuple(sorted(extra)),
        mismatched=tuple(sorted(seen.items())),
        physical_duplicates=physical,
        cut_proven=cut_proven,
    )


def count_physical(rows: Iterable[SourceIdentity]) -> dict[SourceIdentity, int]:
    counts: dict[SourceIdentity, int] = {}
    for identity in rows:
        counts[identity] = counts.get(identity, 0) + 1
    return counts
