"""Three-dimension verdicts and their aggregation (sections 7, 15, 24).

Section 15 sketches the decision as a chain of ``if`` statements over booleans
and then says, in the note immediately after, that a real implementation needs
three-dimensional verdicts and error propagation rather than those booleans.
This module is that implementation.

Two ideas carry it.

**Three-valued logic.** :class:`Tri` has ``TRUE``, ``FALSE`` and ``UNKNOWN``,
and ``UNKNOWN`` never collapses into either. Section 15: "forbidden to default
a missing boolean to true; unknown is always unknown". A gate that cannot be
evaluated produces ``INCONCLUSIVE``, not ``PASS``.

**Independent dimensions.** ADR-05 makes validity, correctness, capacity,
latency, recoverability and safety separate decisions, aggregated by severity.
That is what lets section 7's special case work: a run driven past capacity
which stays bounded and stops cleanly **passes** the overload-safety case and
**fails** the 120k capacity claim, in the same report, without contradiction.

Severity order is ``PASS < INCONCLUSIVE < FAIL < INVALID``. INVALID outranks
FAIL deliberately: a failure produced by a broken measurement is not a fact
about the system under test.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Mapping

from .reconciliation import Reconciliation


class Tri(str, Enum):
    """Three-valued truth. ``UNKNOWN`` is a first-class answer."""

    TRUE = "TRUE"
    FALSE = "FALSE"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def of(cls, value: bool | None) -> "Tri":
        if value is None:
            return cls.UNKNOWN
        return cls.TRUE if value else cls.FALSE

    def __bool__(self) -> bool:  # pragma: no cover - deliberately unavailable
        raise TypeError(
            "a Tri may not be used in a boolean context; UNKNOWN would become "
            "False and an unmeasured gate would silently fail or pass"
        )


class Status(str, Enum):
    """Per-dimension and overall status (section 7)."""

    PASS = "PASS"
    FAIL = "FAIL"
    INCONCLUSIVE = "INCONCLUSIVE"
    INVALID = "INVALID"
    NOT_RUN = "NOT_RUN"
    #: The dimension is out of scope for this case: a normal run has no fault
    #: receipt to verify, and section 15 accepts NOT_APPLICABLE there rather
    #: than hard-failing.
    NOT_APPLICABLE = "NOT_APPLICABLE"
    BLOCKED_BY_UPSTREAM = "BLOCKED_BY_UPSTREAM_IMPLEMENTATION"


#: Aggregation order. INVALID above FAIL: a FAIL from a broken measurement is
#: not a result. NOT_RUN and BLOCKED sit at the top because they mean the
#: question was never put to the system.
_SEVERITY: Mapping[Status, int] = {
    Status.PASS: 0,
    Status.NOT_APPLICABLE: 0,
    Status.INCONCLUSIVE: 2,
    Status.FAIL: 3,
    Status.INVALID: 4,
    Status.BLOCKED_BY_UPSTREAM: 5,
    Status.NOT_RUN: 6,
}


class Dimension(str, Enum):
    VALIDITY = "validity"
    CORRECTNESS = "correctness"
    CAPACITY = "capacity"
    LATENCY = "latency"
    RECOVERABILITY = "recoverability"
    SAFETY = "safety"


@dataclass(frozen=True)
class DimensionVerdict:
    dimension: Dimension
    status: Status
    reasons: tuple[str, ...] = ()

    @property
    def blocking(self) -> bool:
        return _SEVERITY[self.status] >= _SEVERITY[Status.FAIL]

    def as_dict(self) -> dict[str, Any]:
        return {
            "dimension": self.dimension.value,
            "status": self.status.value,
            "reasons": list(self.reasons),
        }


def aggregate(verdicts: Iterable[DimensionVerdict]) -> Status:
    """The most severe status across dimensions."""
    items = list(verdicts)
    if not items:
        return Status.NOT_RUN
    return max((v.status for v in items), key=lambda s: _SEVERITY[s])


@dataclass
class RunEvidence:
    """Everything the verdict is computed from.

    Every field defaults to ``UNKNOWN`` or ``False``. That is the point: a
    caller that forgets to supply a gate gets ``INCONCLUSIVE``, never a pass.
    The flags named ``is_*_claim`` say which dimensions are in scope, so a
    write-only lane is not held to a query budget it never measured.
    """

    executed: bool = False

    # -- validity ---------------------------------------------------------
    manifest_valid: Tri = Tri.UNKNOWN
    workload_calibrated: Tri = Tri.UNKNOWN
    steady_window_matches_manifest: Tri = Tri.UNKNOWN
    clock_adequate_for_claims: Tri = Tri.UNKNOWN
    sufficient_samples: Tri = Tri.UNKNOWN
    histogram_resolution_adequate: Tri = Tri.UNKNOWN
    evidence_complete: Tri = Tri.UNKNOWN
    source_sealed: Tri = Tri.UNKNOWN
    #: A simulated SUT can validate the instruments and nothing else.
    sut_is_simulation: bool = False
    #: Chaos only. ``UNKNOWN`` with ``is_fault_case=False`` reads as NOT_APPLICABLE.
    is_fault_case: bool = False
    fault_receipt_valid: Tri = Tri.UNKNOWN

    # -- correctness ------------------------------------------------------
    reconciliation: Reconciliation | None = None
    false_ready: Tri = Tri.UNKNOWN
    early_offset_commit: Tri = Tri.UNKNOWN
    query_results_correct: Tri = Tri.UNKNOWN

    # -- capacity ---------------------------------------------------------
    is_capacity_claim: bool = False
    backlog_stable: Tri = Tri.UNKNOWN
    drain_reconciled: Tri = Tri.UNKNOWN
    steady_protocol_met: Tri = Tri.UNKNOWN
    achieved_rate_meets_claim: Tri = Tri.UNKNOWN

    # -- latency ----------------------------------------------------------
    is_latency_claim: bool = False
    latency_within_budget: Tri = Tri.UNKNOWN

    # -- recoverability ---------------------------------------------------
    is_recovery_case: bool = False
    recovery_within_budget: Tri = Tri.UNKNOWN
    recovery_preserved_correctness: Tri = Tri.UNKNOWN

    # -- safety -----------------------------------------------------------
    #: An overload case is judged on bounded degradation, not on throughput.
    is_overload_case: bool = False
    resources_bounded: Tri = Tri.UNKNOWN
    no_oom: Tri = Tri.UNKNOWN
    no_silent_loss: Tri = Tri.UNKNOWN
    safety_allowlist_respected: Tri = Tri.UNKNOWN
    degraded_safely: Tri = Tri.UNKNOWN

    # -- upstream ---------------------------------------------------------
    blocked_stages: tuple[str, ...] = ()

    notes: list[str] = field(default_factory=list)


def _gate(
    checks: list[tuple[str, Tri, str]],
) -> tuple[Status, list[str]]:
    """Evaluate a list of ``(name, value, failure_message)`` gates.

    ``FALSE`` anywhere is a FAIL; otherwise ``UNKNOWN`` anywhere is
    INCONCLUSIVE. Failures are reported ahead of unknowns, because a definite
    failure is more actionable than a gap.
    """
    failures = [message for _, value, message in checks if value is Tri.FALSE]
    unknowns = [
        message if name == "reconciliation" else f"{name} is unknown"
        for name, value, message in checks
        if value is Tri.UNKNOWN
    ]
    if failures:
        return Status.FAIL, failures + unknowns
    if unknowns:
        return Status.INCONCLUSIVE, unknowns
    return Status.PASS, []


def validity_verdict(evidence: RunEvidence) -> DimensionVerdict:
    """Was this a valid experiment at all? (section 7, FR-13)

    A failure here is reported as ``INVALID`` rather than ``FAIL``: the
    experiment was broken, so nothing was learned about the system.
    """
    if not evidence.executed:
        return DimensionVerdict(Dimension.VALIDITY, Status.NOT_RUN, ("the case was never run",))

    checks: list[tuple[str, Tri, str]] = [
        ("manifest_valid", evidence.manifest_valid, "run manifest is not valid or drifted"),
        ("source_sealed", evidence.source_sealed, "source dataset is not sealed"),
        (
            "workload_calibrated",
            evidence.workload_calibrated,
            "the driver did not issue the scheduled workload; the target-rate "
            "claim is invalid regardless of how the pipeline behaved",
        ),
        (
            "steady_window_matches_manifest",
            evidence.steady_window_matches_manifest,
            "the measured steady window does not match the one declared",
        ),
        ("evidence_complete", evidence.evidence_complete, "the evidence bundle is incomplete"),
    ]
    if evidence.is_fault_case:
        checks.append(
            (
                "fault_receipt_valid",
                evidence.fault_receipt_valid,
                "the injected fault has no receipt proving it reached its target",
            )
        )
    if evidence.is_latency_claim:
        checks.extend(
            [
                (
                    "clock_adequate_for_claims",
                    evidence.clock_adequate_for_claims,
                    "clock synchronisation is not adequate for the latency claimed",
                ),
                ("sufficient_samples", evidence.sufficient_samples, "too few samples for the claim"),
                (
                    "histogram_resolution_adequate",
                    evidence.histogram_resolution_adequate,
                    "histogram resolution cannot decide the threshold being claimed",
                ),
            ]
        )

    status, reasons = _gate(checks)
    if status is Status.FAIL:
        status = Status.INVALID
    if evidence.sut_is_simulation and (evidence.is_capacity_claim or evidence.is_latency_claim):
        # The simulation is a ruler being checked against a known length, not a
        # measurement of the building.
        status = Status.INVALID
        reasons = [
            "the system under test is a simulation; it can validate the "
            "measuring tools but cannot evidence a capacity or latency claim",
            *reasons,
        ]
    return DimensionVerdict(Dimension.VALIDITY, status, tuple(reasons))


def correctness_verdict(evidence: RunEvidence) -> DimensionVerdict:
    """Did the data survive? (INV-04, INV-05, INV-07, INV-18)"""
    if not evidence.executed:
        return DimensionVerdict(Dimension.CORRECTNESS, Status.NOT_RUN, ("the case was never run",))
    # The reconciliation result is folded in as one more gate rather than
    # short-circuiting, so that a *definite* failure elsewhere -- a wrong query
    # result, a false READY -- is reported as FAIL instead of being swallowed by
    # the INCONCLUSIVE a missing oracle would produce. _gate reports failures
    # ahead of unknowns for the same reason.
    if evidence.reconciliation is None:
        reconciled = Tri.UNKNOWN
        reconcile_message = (
            "no reconciliation was performed; app counters do not establish correctness"
        )
    elif evidence.reconciliation is Reconciliation.INCONCLUSIVE:
        reconciled = Tri.UNKNOWN
        reconcile_message = "the independent oracle could not complete its comparison"
    else:
        reconciled = Tri.of(evidence.reconciliation is Reconciliation.PASS)
        reconcile_message = (
            "the independent oracle found missing, extra or conflicting identities"
        )

    checks: list[tuple[str, Tri, str]] = [
        ("reconciliation", reconciled, reconcile_message),
        ("false_ready", _negate(evidence.false_ready), "a READY build was published without full verification"),
        (
            "early_offset_commit",
            _negate(evidence.early_offset_commit),
            "an offset was committed past the acknowledged terminal frontier",
        ),
    ]
    if evidence.is_latency_claim:
        checks.append(
            (
                "query_results_correct",
                evidence.query_results_correct,
                "query responses did not match the result oracle",
            )
        )
    status, reasons = _gate(checks)
    return DimensionVerdict(Dimension.CORRECTNESS, status, tuple(reasons))


def _negate(value: Tri) -> Tri:
    """Invert a Tri, keeping ``UNKNOWN`` unknown."""
    if value is Tri.UNKNOWN:
        return Tri.UNKNOWN
    return Tri.FALSE if value is Tri.TRUE else Tri.TRUE


def capacity_verdict(evidence: RunEvidence) -> DimensionVerdict:
    """Was the claimed sustainable rate actually sustained? (section 4.2, 6.3)"""
    if not evidence.executed:
        return DimensionVerdict(Dimension.CAPACITY, Status.NOT_RUN, ("the case was never run",))
    if not evidence.is_capacity_claim:
        return DimensionVerdict(
            Dimension.CAPACITY,
            Status.NOT_APPLICABLE,
            ("this case makes no capacity claim",),
        )
    status, reasons = _gate(
        [
            (
                "achieved_rate_meets_claim",
                evidence.achieved_rate_meets_claim,
                "the verified useful rate did not reach the published rate",
            ),
            (
                "backlog_stable",
                evidence.backlog_stable,
                "backlog grew during the steady window on at least one partition",
            ),
            (
                "drain_reconciled",
                evidence.drain_reconciled,
                "the drain did not complete and reconcile, so processing debt remains",
            ),
            (
                "steady_protocol_met",
                evidence.steady_protocol_met,
                "the steady window was shorter than the declared protocol",
            ),
        ]
    )
    return DimensionVerdict(Dimension.CAPACITY, status, tuple(reasons))


def latency_verdict(evidence: RunEvidence) -> DimensionVerdict:
    if not evidence.executed:
        return DimensionVerdict(Dimension.LATENCY, Status.NOT_RUN, ("the case was never run",))
    if not evidence.is_latency_claim:
        return DimensionVerdict(
            Dimension.LATENCY, Status.NOT_APPLICABLE, ("this case makes no latency claim",)
        )
    status, reasons = _gate(
        [
            (
                "latency_within_budget",
                evidence.latency_within_budget,
                "the pinned percentile exceeded its budget for this query family",
            )
        ]
    )
    return DimensionVerdict(Dimension.LATENCY, status, tuple(reasons))


def recoverability_verdict(evidence: RunEvidence) -> DimensionVerdict:
    if not evidence.executed:
        return DimensionVerdict(
            Dimension.RECOVERABILITY, Status.NOT_RUN, ("the case was never run",)
        )
    if not evidence.is_recovery_case:
        return DimensionVerdict(
            Dimension.RECOVERABILITY,
            Status.NOT_APPLICABLE,
            ("this case injects no fault and makes no recovery claim",),
        )
    status, reasons = _gate(
        [
            (
                "recovery_within_budget",
                evidence.recovery_within_budget,
                "recovery exceeded the RPO/RTO budget",
            ),
            (
                "recovery_preserved_correctness",
                evidence.recovery_preserved_correctness,
                "recovery completed but logical correctness was not preserved",
            ),
        ]
    )
    return DimensionVerdict(Dimension.RECOVERABILITY, status, tuple(reasons))


def safety_verdict(evidence: RunEvidence) -> DimensionVerdict:
    """Did the system stay bounded and honest under pressure? (NFR-07, NFR-09, ADR-08)

    For an overload case this is the dimension that decides PASS. Section 7:
    a run above capacity that keeps queues bounded and stops safely passes the
    overload-behaviour test while failing the capacity claim.
    """
    if not evidence.executed:
        return DimensionVerdict(Dimension.SAFETY, Status.NOT_RUN, ("the case was never run",))
    checks: list[tuple[str, Tri, str]] = [
        ("resources_bounded", evidence.resources_bounded, "resource use was not bounded"),
        ("no_oom", evidence.no_oom, "the run was OOM-killed"),
        ("no_silent_loss", evidence.no_silent_loss, "records were dropped without being recorded"),
        (
            "safety_allowlist_respected",
            evidence.safety_allowlist_respected,
            "an action targeted something outside the lab allowlist",
        ),
    ]
    if evidence.is_overload_case:
        checks.append(
            (
                "degraded_safely",
                evidence.degraded_safely,
                "the overload was not handled by bounded degradation or an "
                "explicit fail-stop",
            )
        )
    status, reasons = _gate(checks)
    return DimensionVerdict(Dimension.SAFETY, status, tuple(reasons))


@dataclass(frozen=True)
class RunVerdict:
    """The six dimensions plus the aggregate, with blocking findings."""

    dimensions: tuple[DimensionVerdict, ...]
    overall: Status
    blocked_stages: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def by_dimension(self, dimension: Dimension) -> DimensionVerdict:
        for verdict in self.dimensions:
            if verdict.dimension is dimension:
                return verdict
        raise KeyError(dimension)

    @property
    def blocking_findings(self) -> list[dict[str, str]]:
        return [
            {"dimension": v.dimension.value, "status": v.status.value, "detail": reason}
            for v in self.dimensions
            if v.blocking
            for reason in (v.reasons or ("no reason recorded",))
        ]

    def as_dict(self) -> dict[str, Any]:
        return {
            "overall": self.overall.value,
            "dimensions": {v.dimension.value: v.status.value for v in self.dimensions},
            "dimension_detail": [v.as_dict() for v in self.dimensions],
            "blocking_findings": self.blocking_findings,
            "blocked_stages": list(self.blocked_stages),
            "notes": list(self.notes),
        }


def decide(evidence: RunEvidence) -> RunVerdict:
    """Compute the six dimensions and aggregate them.

    A run whose required stages do not exist short-circuits to
    ``BLOCKED_BY_UPSTREAM_IMPLEMENTATION`` (section 20). That is neither a pass
    nor a failure of the system under test: there is nothing there to measure.
    """
    if evidence.blocked_stages:
        dimensions = tuple(
            DimensionVerdict(
                dimension,
                Status.BLOCKED_BY_UPSTREAM,
                (
                    "requires stages with no implementation in this repository: "
                    + ", ".join(evidence.blocked_stages),
                ),
            )
            for dimension in Dimension
        )
        return RunVerdict(
            dimensions=dimensions,
            overall=Status.BLOCKED_BY_UPSTREAM,
            blocked_stages=evidence.blocked_stages,
            notes=tuple(evidence.notes),
        )

    dimensions = (
        validity_verdict(evidence),
        correctness_verdict(evidence),
        capacity_verdict(evidence),
        latency_verdict(evidence),
        recoverability_verdict(evidence),
        safety_verdict(evidence),
    )
    notes = list(evidence.notes)
    if evidence.sut_is_simulation:
        notes.append(
            "system under test is a simulation: these results validate the "
            "measurement tools, not a real pipeline"
        )
    return RunVerdict(
        dimensions=dimensions,
        overall=aggregate(dimensions),
        notes=tuple(notes),
    )
