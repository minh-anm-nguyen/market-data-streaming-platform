"""The five verdicts, the reason codes, and the gate that assigns them.

Design 9.2 and 16.1 are unusually strict about vocabulary, because most of the
ways a reliability lab lies to itself are vocabulary failures:

``PASS``
    The fault was actually applied, the preconditions held, every exact check
    completed, and the evidence is durable. True *for the tested scope only*.
``FAIL``
    A confirmed counter-example: missing, extra, conflicting or wrong data, or
    a safety property violated.
``INCONCLUSIVE``
    The lab could not tell. Source gone, Kafka range expired, an ACK whose
    outcome no independent scan resolved, evidence truncated. Not a pass.
``INVALID``
    The test did not test what it claimed: the injector never fired, it hit the
    wrong target, or the topology/version was not the one under test.
``NOT_RUN``
    No execution. Distinct from all four above and never counted as coverage.

:class:`Verdict` deliberately has no boolean conversion. ``if verdict:`` would
make ``INCONCLUSIVE`` truthy and that is exactly the bug class this file exists
to prevent; :meth:`Verdict.is_release_blocking` forces the question to be asked
explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Final, Mapping, Sequence


class Verdict(str, Enum):
    """Terminal outcome of one check or one test case."""

    PASS = "PASS"
    FAIL = "FAIL"
    INCONCLUSIVE = "INCONCLUSIVE"
    INVALID = "INVALID"
    NOT_RUN = "NOT_RUN"

    def __bool__(self) -> bool:  # noqa: D105 - see module docstring
        raise TypeError(
            "Verdict has no truth value; compare explicitly "
            "(verdict is Verdict.PASS) or call is_release_blocking()"
        )

    def is_release_blocking(self) -> bool:
        """Whether a P0 case ending in this verdict blocks release (design 9.3).

        ``INCONCLUSIVE``, ``INVALID`` and ``NOT_RUN`` are all NO-GO for a
        mandatory case: "not proven" is not "fine".
        """
        return self is not Verdict.PASS


#: Reason codes. Bounded on purpose: an open-ended reason string ends up as a
#: free-text field nobody can aggregate, and as a metrics-cardinality problem
#: (case ``P07-G29-*``). Grouped by the boundary that produces them.
class Reason:
    """Namespace of reason-code constants."""

    # -- source sealing -------------------------------------------------
    SOURCE_PART_FILE_PRESENT: Final = "SOURCE_PART_FILE_PRESENT"
    SOURCE_NOT_COMPLETED: Final = "SOURCE_NOT_COMPLETED"
    SOURCE_HASH_MISMATCH: Final = "SOURCE_HASH_MISMATCH"
    SOURCE_COUNT_MISMATCH: Final = "SOURCE_COUNT_MISMATCH"
    SOURCE_ANCHOR_MISSING: Final = "SOURCE_ANCHOR_MISSING"
    SOURCE_ANCHOR_MISMATCH: Final = "SOURCE_ANCHOR_MISMATCH"
    SOURCE_SCHEMA_SKEW: Final = "SOURCE_SCHEMA_SKEW"
    SOURCE_DATASET_ID_COLLISION: Final = "SOURCE_DATASET_ID_COLLISION"
    SOURCE_SYMBOL_MISSING: Final = "SOURCE_SYMBOL_MISSING"
    SOURCE_MANIFEST_INVALID: Final = "SOURCE_MANIFEST_INVALID"
    SOURCE_FAULT_PLAN_UNHASHED: Final = "SOURCE_FAULT_PLAN_UNHASHED"
    SOURCE_UNTRACKED_ANOMALY: Final = "SOURCE_UNTRACKED_ANOMALY"
    SOURCE_IDENTITY_CONFLICT: Final = "SOURCE_IDENTITY_CONFLICT"
    SOURCE_UNAVAILABLE: Final = "SOURCE_UNAVAILABLE"

    # -- transport / Kafka cut ------------------------------------------
    CUT_NOT_FENCED: Final = "CUT_NOT_FENCED"
    CUT_WRITER_ACTIVE: Final = "CUT_WRITER_ACTIVE"
    CUT_GENERATION_MIXED: Final = "CUT_GENERATION_MIXED"
    CUT_RANGE_EXPIRED: Final = "CUT_RANGE_EXPIRED"
    CUT_OFFSET_HOLE: Final = "CUT_OFFSET_HOLE"
    CUT_CLUSTER_MISMATCH: Final = "CUT_CLUSTER_MISMATCH"
    PRODUCER_ACK_UNKNOWN: Final = "PRODUCER_ACK_UNKNOWN"
    KAFKA_UNAVAILABLE: Final = "KAFKA_UNAVAILABLE"
    KAFKA_MIN_ISR_UNAVAILABLE: Final = "KAFKA_MIN_ISR_UNAVAILABLE"
    KAFKA_VALUE_HAS_MDP1_PREFIX: Final = "KAFKA_VALUE_HAS_MDP1_PREFIX"
    KAFKA_VALUE_TOO_LARGE: Final = "KAFKA_VALUE_TOO_LARGE"
    KAFKA_DECODE_ERROR: Final = "KAFKA_DECODE_ERROR"
    KAFKA_WRONG_TOPIC_FOR_KIND: Final = "KAFKA_WRONG_TOPIC_FOR_KIND"
    KAFKA_WRONG_PARTITION_FOR_KEY: Final = "KAFKA_WRONG_PARTITION_FOR_KEY"

    # -- ingestion / frontier -------------------------------------------
    COMMIT_AHEAD_OF_COMPLETION: Final = "COMMIT_AHEAD_OF_COMPLETION"
    COMMIT_BY_STALE_OWNER: Final = "COMMIT_BY_STALE_OWNER"
    OFFSET_RESET_FORBIDDEN: Final = "OFFSET_RESET_FORBIDDEN"
    SINK_ACK_UNKNOWN: Final = "SINK_ACK_UNKNOWN"
    SINK_UNAVAILABLE: Final = "SINK_UNAVAILABLE"
    SINK_DISK_FULL: Final = "SINK_DISK_FULL"

    # -- reconciliation --------------------------------------------------
    MISSING_IDENTITIES: Final = "MISSING_IDENTITIES"
    EXTRA_IDENTITIES: Final = "EXTRA_IDENTITIES"
    IDENTITY_CONFLICT: Final = "IDENTITY_CONFLICT"
    SEMANTIC_MISMATCH: Final = "SEMANTIC_MISMATCH"
    CANONICAL_NOT_UNIQUE: Final = "CANONICAL_NOT_UNIQUE"
    BARS_MISMATCH: Final = "BARS_MISMATCH"
    PRICE_SCALE_CHANGED: Final = "PRICE_SCALE_CHANGED"
    RAW_ROWS_EXPIRED: Final = "RAW_ROWS_EXPIRED"
    SAMPLE_ONLY_NOT_ACCEPTED: Final = "SAMPLE_ONLY_NOT_ACCEPTED"

    # -- analytics publication -------------------------------------------
    BUILD_NOT_VERIFIED: Final = "BUILD_NOT_VERIFIED"
    BUILD_DIRTY: Final = "BUILD_DIRTY"
    READY_POINTER_UNCHANGED: Final = "READY_POINTER_UNCHANGED"
    READY_PUBLISH_RACE_LOST: Final = "READY_PUBLISH_RACE_LOST"
    READY_IMMUTABILITY_VIOLATED: Final = "READY_IMMUTABILITY_VIOLATED"
    LATE_DATA_AFTER_CUT: Final = "LATE_DATA_AFTER_CUT"

    # -- lab validity / safety -------------------------------------------
    FAULT_NOT_CONFIRMED: Final = "FAULT_NOT_CONFIRMED"
    FAULT_WRONG_TARGET: Final = "FAULT_WRONG_TARGET"
    FAULT_CLEANUP_MISSING: Final = "FAULT_CLEANUP_MISSING"
    TARGET_NOT_ALLOWLISTED: Final = "TARGET_NOT_ALLOWLISTED"
    EVIDENCE_INCOMPLETE: Final = "EVIDENCE_INCOMPLETE"
    EVIDENCE_QUOTA_EXCEEDED: Final = "EVIDENCE_QUOTA_EXCEEDED"
    RESOURCE_BUDGET_EXCEEDED: Final = "RESOURCE_BUDGET_EXCEEDED"
    DEADLINE_EXCEEDED: Final = "DEADLINE_EXCEEDED"
    PROFILE_NOT_VALIDATED: Final = "PROFILE_NOT_VALIDATED"
    CONFIG_INVALID: Final = "CONFIG_INVALID"

    # -- clean outcome ----------------------------------------------------
    ALL_CHECKS_PROVEN: Final = "ALL_CHECKS_PROVEN"
    EXPECTED_REFUSAL: Final = "EXPECTED_REFUSAL"


#: Reasons whose meaning is "the lab could not tell", as opposed to "the system
#: is wrong". Mapping them to ``FAIL`` would manufacture failures; mapping them
#: to ``PASS`` would manufacture successes.
_INCONCLUSIVE_REASONS: Final[frozenset[str]] = frozenset(
    {
        Reason.SOURCE_ANCHOR_MISSING,
        Reason.SOURCE_UNAVAILABLE,
        Reason.CUT_RANGE_EXPIRED,
        Reason.CUT_NOT_FENCED,
        Reason.PRODUCER_ACK_UNKNOWN,
        Reason.SINK_ACK_UNKNOWN,
        Reason.RAW_ROWS_EXPIRED,
        Reason.EVIDENCE_INCOMPLETE,
        Reason.EVIDENCE_QUOTA_EXCEEDED,
        Reason.DEADLINE_EXCEEDED,
        Reason.KAFKA_UNAVAILABLE,
        Reason.SINK_UNAVAILABLE,
    }
)

#: Reasons that mean the *test* was not valid, so its result says nothing about
#: the system either way.
_INVALID_REASONS: Final[frozenset[str]] = frozenset(
    {
        Reason.FAULT_NOT_CONFIRMED,
        Reason.FAULT_WRONG_TARGET,
        Reason.FAULT_CLEANUP_MISSING,
        Reason.TARGET_NOT_ALLOWLISTED,
        Reason.PROFILE_NOT_VALIDATED,
        Reason.CONFIG_INVALID,
        Reason.SOURCE_SCHEMA_SKEW,
        Reason.SOURCE_FAULT_PLAN_UNHASHED,
    }
)


@dataclass(frozen=True, slots=True)
class Finding:
    """One thing the lab observed, with the evidence that supports it.

    ``detail`` is a small mapping, not a formatted sentence, so the reporter can
    render it and the release gate can aggregate it.
    """

    reason: str
    detail: Mapping[str, object] = field(default_factory=dict)
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.reason:
            raise ValueError("a finding needs a reason code")


@dataclass(frozen=True, slots=True)
class GateResult:
    """A verdict plus the findings that produced it."""

    verdict: Verdict
    primary_reason: str
    findings: tuple[Finding, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "verdict": self.verdict.value,
            "primary_reason": self.primary_reason,
            "findings": [
                {
                    "reason": finding.reason,
                    "detail": dict(finding.detail),
                    "evidence_refs": list(finding.evidence_refs),
                }
                for finding in self.findings
            ],
        }


def classify(findings: Sequence[Finding], *, fault_confirmed: bool = True) -> GateResult:
    """Collapse findings into one verdict, in the precedence design 6.3 sets.

    The order is not arbitrary and is the heart of the gate::

        fault not confirmed        -> INVALID      (never PASS, whatever else held)
        any invalid-class reason   -> INVALID
        any inconclusive reason    -> INCONCLUSIVE (missing proof outranks "looks fine")
        any remaining finding      -> FAIL
        no findings                -> PASS

    ``INCONCLUSIVE`` outranking ``FAIL`` is the one choice worth justifying: if
    the lab could not read the Kafka range it needed, it does not know whether
    the data was missing, so reporting ``FAIL`` would be as much a fabrication
    as reporting ``PASS``. A real contradiction *plus* a missing proof is
    reported as ``INCONCLUSIVE`` with the contradiction kept in ``findings``,
    and the mandatory-case gate blocks release either way.
    """
    if not fault_confirmed:
        return GateResult(
            Verdict.INVALID,
            Reason.FAULT_NOT_CONFIRMED,
            tuple(findings) or (Finding(Reason.FAULT_NOT_CONFIRMED),),
        )
    findings = tuple(findings)
    if not findings:
        return GateResult(Verdict.PASS, Reason.ALL_CHECKS_PROVEN, ())
    for finding in findings:
        if finding.reason in _INVALID_REASONS:
            return GateResult(Verdict.INVALID, finding.reason, findings)
    for finding in findings:
        if finding.reason in _INCONCLUSIVE_REASONS:
            return GateResult(Verdict.INCONCLUSIVE, finding.reason, findings)
    return GateResult(Verdict.FAIL, findings[0].reason, findings)


def expected_refusal(reason: str, detail: Mapping[str, object] | None = None) -> GateResult:
    """A ``PASS`` whose content is "the system correctly refused".

    Several matrix rows expect a refusal rather than data flowing through: a
    corrupt source must be quarantined, a poisoned record must stop the build.
    Those are passes, and this constructor records *what* was refused so the
    report cannot be mistaken for "nothing happened" (design 10 preamble).
    """
    return GateResult(
        Verdict.PASS,
        Reason.EXPECTED_REFUSAL,
        (Finding(reason, detail or {}),),
    )
