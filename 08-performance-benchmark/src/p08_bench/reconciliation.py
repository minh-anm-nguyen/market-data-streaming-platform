"""The independent oracle: identity and semantic reconciliation (section 9.2; INV-04..06).

Section 4.1 rules out the comparison everyone reaches for first. There is no
``offered == raw_rows == canonical_rows`` identity, because rejections, physical
retries and partial reads all break it, and a ``COUNT(DISTINCT)`` that matches
does not prove the payloads agree. What section 9.2 requires instead is a
comparison over a finite sealed cut:

    expected_accepted_IDs == READY_canonical_IDs
    and payload semantics and field presence match
    and unexpected_extra == 0 and conflicts == 0

So this module compares three things and reports them separately:

``missing``
    Expected identities absent from the canonical build. INV-04.
``unexpected_extra``
    Canonical identities nobody sent. INV-05 -- and a count-only check cannot
    see these at all when they happen to balance a missing one.
``conflicts``
    Identities present on both sides whose payloads disagree. INV-05.

Three design decisions worth stating:

* **Identity is ``(dataset_id, symbol, sequence)``.** Never a Kafka offset
  (INV-02); an offset is a position in one incarnation of one partition.
* **Physical duplicates are not a finding.** At-least-once delivery makes them
  expected, and INV-06 requires them to leave the canonical unique set alone.
  They are counted as a separate phenomenon.
* **Comparison never loads 200M identities into RAM.** The default path is a
  streaming merge over two sorted iterators (section 9.2), with the in-memory
  set version kept for golden fixtures where it is obviously affordable.

Payload comparison refuses floats. INV-03 keeps prices and timestamps as
integers end to end, and a float round-trip is exactly how an exact comparison
silently becomes approximate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence

from .errors import MetrologyError

#: The business identity of INV-02.
IdentityKey = tuple[str, str, int]

#: Fields compared by default: the ones INV-03 says must survive exactly.
DEFAULT_SEMANTIC_FIELDS: tuple[str, ...] = (
    "event_time_ns", "price_ticks", "bid_ticks", "ask_ticks", "price_scale", "kind",
)

#: How many diffs of each kind to keep. A 200M-row mismatch must not be
#: materialised in a report; the counts are exact and the examples are capped.
DEFAULT_EXAMPLE_LIMIT = 50


class MismatchKind(str, Enum):
    MISSING = "MISSING"
    UNEXPECTED_EXTRA = "UNEXPECTED_EXTRA"
    VALUE_CONFLICT = "VALUE_CONFLICT"
    FIELD_ABSENT = "FIELD_ABSENT"
    FIELD_UNEXPECTED = "FIELD_UNEXPECTED"


@dataclass(frozen=True)
class FieldMismatch:
    """One field of one identity that does not agree.

    ``FIELD_ABSENT`` is separate from ``VALUE_CONFLICT`` because of the P02
    contract: ``oneof`` presence is part of the message semantics, so a field
    that vanished is a different defect from one whose value changed.
    """

    identity: IdentityKey
    field_name: str
    kind: MismatchKind
    expected: Any = None
    actual: Any = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "identity": list(self.identity),
            "field": self.field_name,
            "kind": self.kind.value,
            "expected": self.expected,
            "actual": self.actual,
        }


def _reject_floats(identity: IdentityKey, name: str, value: Any) -> None:
    """INV-03: a compared field may not be a float.

    Booleans pass through as ints by Python's type hierarchy, which is fine;
    what is refused is the lossy numeric type, because a value that arrived as
    a float has already lost the exactness the comparison depends on.
    """
    if isinstance(value, float):
        raise MetrologyError(
            "a compared field arrived as a float; prices and timestamps are "
            "integer fixed-point end to end (INV-03)",
            identity=list(identity),
            field=name,
            value=value,
        )


def compare_payloads(
    identity: IdentityKey,
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
    *,
    fields: Sequence[str] = DEFAULT_SEMANTIC_FIELDS,
) -> list[FieldMismatch]:
    """Field-by-field comparison of two payloads for one identity.

    Only the named ``fields`` are compared, and only where at least one side
    has them. Comparing every key would make an added diagnostic field look
    like a data defect; comparing none would let a price change through.
    """
    mismatches: list[FieldMismatch] = []
    for name in fields:
        in_expected = name in expected
        in_actual = name in actual
        if not in_expected and not in_actual:
            continue
        if in_expected and not in_actual:
            mismatches.append(
                FieldMismatch(identity, name, MismatchKind.FIELD_ABSENT, expected[name], None)
            )
            continue
        if in_actual and not in_expected:
            mismatches.append(
                FieldMismatch(identity, name, MismatchKind.FIELD_UNEXPECTED, None, actual[name])
            )
            continue
        left, right = expected[name], actual[name]
        _reject_floats(identity, name, left)
        _reject_floats(identity, name, right)
        if left != right:
            mismatches.append(
                FieldMismatch(identity, name, MismatchKind.VALUE_CONFLICT, left, right)
            )
    return mismatches


class Reconciliation(str, Enum):
    """The verdict of one reconciliation, in section 7 terms."""

    PASS = "PASS"
    FAIL = "FAIL"
    #: The sets could not be compared: an unsealed source, a missing build, or
    #: no payload access to decide conflicts.
    INCONCLUSIVE = "INCONCLUSIVE"


@dataclass
class ReconciliationResult:
    """Exact counts, capped examples, and what they add up to."""

    expected_count: int
    canonical_count: int
    missing_count: int = 0
    unexpected_extra_count: int = 0
    conflict_identity_count: int = 0
    semantic_mismatch_count: int = 0
    compared_identities: int = 0
    payloads_compared: int = 0
    physical_duplicate_count: int | None = None
    missing_examples: list[IdentityKey] = field(default_factory=list)
    extra_examples: list[IdentityKey] = field(default_factory=list)
    mismatch_examples: list[FieldMismatch] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    method: str = "streaming_merge"

    @property
    def clean(self) -> bool:
        """No missing, no extra, no conflict. The INV-04/05 conjunction."""
        return (
            self.missing_count == 0
            and self.unexpected_extra_count == 0
            and self.conflict_identity_count == 0
            and self.semantic_mismatch_count == 0
        )

    @property
    def verdict(self) -> Reconciliation:
        if self.blockers:
            return Reconciliation.INCONCLUSIVE
        return Reconciliation.PASS if self.clean else Reconciliation.FAIL

    def summary(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict.value,
            "method": self.method,
            "expected_accepted_ID_count": self.expected_count,
            "ready_canonical_ID_count": self.canonical_count,
            "missing_ID_count": self.missing_count,
            "unexpected_extra_ID_count": self.unexpected_extra_count,
            "conflict_ID_count": self.conflict_identity_count,
            "semantic_mismatch_count": self.semantic_mismatch_count,
            "compared_identities": self.compared_identities,
            "payloads_compared": self.payloads_compared,
            # A separate physical phenomenon, never folded into the findings
            # above (INV-06).
            "physical_duplicate_count": self.physical_duplicate_count,
            "missing_examples": [list(key) for key in self.missing_examples],
            "extra_examples": [list(key) for key in self.extra_examples],
            "mismatch_examples": [m.as_dict() for m in self.mismatch_examples],
            "blockers": list(self.blockers),
            "note": (
                "counts are exact; examples are capped so a large mismatch does "
                "not materialise in the report"
            ),
        }


def reconcile_sets(
    expected: Iterable[IdentityKey],
    canonical: Iterable[IdentityKey],
    *,
    expected_payload: Callable[[IdentityKey], Mapping[str, Any] | None] | None = None,
    canonical_payload: Callable[[IdentityKey], Mapping[str, Any] | None] | None = None,
    fields: Sequence[str] = DEFAULT_SEMANTIC_FIELDS,
    example_limit: int = DEFAULT_EXAMPLE_LIMIT,
    physical_duplicate_count: int | None = None,
) -> ReconciliationResult:
    """In-memory reconciliation, for fixtures and small cuts.

    Memory is one tuple per identity on each side. Fine for the ten events of
    FIX-001; for FIX-004 use :func:`reconcile_streams`, which is why the method
    is recorded in the result.
    """
    expected_set = set(expected)
    canonical_set = set(canonical)
    missing = sorted(expected_set - canonical_set)
    extra = sorted(canonical_set - expected_set)
    common = expected_set & canonical_set

    result = ReconciliationResult(
        expected_count=len(expected_set),
        canonical_count=len(canonical_set),
        missing_count=len(missing),
        unexpected_extra_count=len(extra),
        compared_identities=len(common),
        missing_examples=missing[:example_limit],
        extra_examples=extra[:example_limit],
        physical_duplicate_count=physical_duplicate_count,
        method="in_memory_sets",
    )

    if expected_payload is None or canonical_payload is None:
        # Section 9.2 compares identity *and* semantics. Without payload access
        # the identity half can pass while the other half is unknown, and an
        # unknown is not a pass.
        result.blockers.append(
            "payload access was not provided; semantic equality and field "
            "presence are unverified, so the comparison cannot be a PASS"
        )
        return result

    conflicting: set[IdentityKey] = set()
    for key in sorted(common):
        left = expected_payload(key)
        right = canonical_payload(key)
        if left is None or right is None:
            result.blockers.append(f"payload unavailable for identity {key}")
            continue
        result.payloads_compared += 1
        mismatches = compare_payloads(key, left, right, fields=fields)
        if mismatches:
            conflicting.add(key)
            result.semantic_mismatch_count += len(mismatches)
            room = example_limit - len(result.mismatch_examples)
            if room > 0:
                result.mismatch_examples.extend(mismatches[:room])
    result.conflict_identity_count = len(conflicting)
    return result


def reconcile_streams(
    expected: Iterator[IdentityKey],
    canonical: Iterator[IdentityKey],
    *,
    example_limit: int = DEFAULT_EXAMPLE_LIMIT,
    physical_duplicate_count: int | None = None,
) -> ReconciliationResult:
    """Streaming merge over two **sorted ascending** iterators.

    This is the 200M path of section 9.2: memory is O(1) in the number of
    identities, with only the capped examples retained. Both inputs must be
    sorted and deduplicated; an out-of-order input raises rather than silently
    producing a wrong diff, because an unsorted merge would report both sides as
    mutually missing.

    Payload comparison is not attempted here -- it needs random access to both
    sides -- so the result carries that as a blocker and the caller decides how
    to verify semantics for the chunks that matter.
    """
    result = ReconciliationResult(
        expected_count=0, canonical_count=0,
        physical_duplicate_count=physical_duplicate_count,
        method="streaming_merge",
    )
    result.blockers.append(
        "streaming identity merge does not compare payloads; semantic equality "
        "must be established separately (section 9.2)"
    )

    left = _checked_sorted(expected, "expected")
    right = _checked_sorted(canonical, "canonical")
    a = next(left, None)
    b = next(right, None)
    while a is not None or b is not None:
        if b is None or (a is not None and a < b):
            result.expected_count += 1
            result.missing_count += 1
            if len(result.missing_examples) < example_limit:
                result.missing_examples.append(a)
            a = next(left, None)
        elif a is None or b < a:
            result.canonical_count += 1
            result.unexpected_extra_count += 1
            if len(result.extra_examples) < example_limit:
                result.extra_examples.append(b)
            b = next(right, None)
        else:
            result.expected_count += 1
            result.canonical_count += 1
            result.compared_identities += 1
            a = next(left, None)
            b = next(right, None)
    return result


def _checked_sorted(source: Iterator[IdentityKey], label: str) -> Iterator[IdentityKey]:
    """Pass through, raising on a non-ascending or duplicated key."""
    previous: IdentityKey | None = None
    for key in source:
        if previous is not None:
            if key < previous:
                raise MetrologyError(
                    f"{label} stream is not sorted ascending; a merge over "
                    "unsorted input reports both sides as mutually missing",
                    previous=list(previous),
                    key=list(key),
                )
            if key == previous:
                raise MetrologyError(
                    f"{label} stream contains a duplicate identity; deduplicate "
                    "before merging, and count physical duplicates separately",
                    key=list(key),
                )
        previous = key
        yield key


@dataclass(frozen=True)
class ExpectedSet:
    """The expected accepted set, derived independently of the pipeline.

    Section 9.2 wants this built from the P01 manifest plus an *independent
    reading* of the P03 validation policy. The acceptance predicate is
    therefore injected rather than read from a component counter: if the oracle
    asked the pipeline which events it should have accepted, the comparison
    would be circular (case G01-12).
    """

    dataset_id: str
    sealed: bool
    accept: Callable[[Mapping[str, Any]], bool]

    def build(self, events: Iterable[Mapping[str, Any]]) -> tuple[set[IdentityKey], dict[IdentityKey, dict[str, Any]]]:
        """Return the expected identities and their payloads."""
        if not self.sealed:
            raise MetrologyError(
                "cannot derive an expected set from an unsealed source",
                dataset_id=self.dataset_id,
            )
        identities: set[IdentityKey] = set()
        payloads: dict[IdentityKey, dict[str, Any]] = {}
        for event in events:
            if not self.accept(event):
                continue
            key = (
                str(event["dataset_id"]),
                str(event["symbol"]),
                int(event["sequence"]),
            )
            identities.add(key)
            payloads.setdefault(key, dict(event))
        return identities, payloads


def accept_all(_: Mapping[str, Any]) -> bool:
    """The trivial acceptance policy: every sealed event is admissible.

    Correct for a lane with no injected invalid data. A lane that exercises P03
    rejection supplies its own predicate, so that an intentional rejection is
    not counted as loss (section 0.1).
    """
    return True
