"""Decide whether recovery is even possible, then measure what it cost.

Design 7.1 step 4 is the gate this module implements: before replaying
anything, check that the sealed source still exists, that the Kafka range still
exists, that the generation and schema match, and that the retention window is
wide enough. If any required precondition is missing, the answer is
``INCONCLUSIVE/NO-GO`` and **not** an automatic offset reset. :func:`assess_replay`
returns the blocking reasons rather than a boolean, so the report can say which
precondition failed.

The metrics are kept strictly apart, because mixing them is how a 20-second
service restart gets published as a recovery time:

``RTO-SERVICE``
    fault confirmed to services answering again. Says nothing about data.
``RTO-CORRECT``
    fault confirmed to a reconciled cut and a published (or terminally
    refused) build. This is the recovery metric that matters.
``RPO``
    the count of expected accepted identities that cannot be reconstructed or
    proven. Counted from the source diff, never inferred from elapsed time, and
    ``0`` only when the source or an independently verified backup survives.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..contracts import SourceIdentity
from ..dataplane.kafka_fake import FakeKafka
from ..evidence.verdicts import Finding, Reason
from ..replay.kafka_cut_scanner import KafkaCut


@dataclass(frozen=True, slots=True)
class ReplayFeasibility:
    """Whether a replay can be attempted, and what blocks it if not."""

    source_sealed: bool
    source_present: bool
    kafka_range_available: bool
    generation_matches: bool
    schema_compatible: bool
    blocking: tuple[str, ...]

    @property
    def feasible(self) -> bool:
        return not self.blocking

    def findings(self) -> tuple[Finding, ...]:
        return tuple(Finding(reason, {"stage": "replay_feasibility"}) for reason in self.blocking)

    def as_dict(self) -> dict[str, Any]:
        return {
            "feasible": self.feasible,
            "source_sealed": self.source_sealed,
            "source_present": self.source_present,
            "kafka_range_available": self.kafka_range_available,
            "generation_matches": self.generation_matches,
            "schema_compatible": self.schema_compatible,
            "blocking": list(self.blocking),
        }


def assess_replay(
    kafka: FakeKafka,
    cut: KafkaCut | None,
    *,
    source_sealed: bool,
    source_present: bool,
    schema_compatible: bool = True,
) -> ReplayFeasibility:
    """Check every precondition for replaying from ``cut``.

    A ``None`` cut means there is no transport evidence at all; that is not
    fatal on its own, because an intact sealed source can be republished into a
    new generation. What *is* fatal is losing both -- design 1.3 lists that as a
    case where RPO=0 cannot be promised, and this returns
    ``SOURCE_UNAVAILABLE`` for it.
    """
    blocking: list[str] = []
    if not source_sealed and not source_present:
        blocking.append(Reason.SOURCE_UNAVAILABLE)
    if not schema_compatible:
        blocking.append(Reason.SOURCE_SCHEMA_SKEW)

    range_available = True
    generation_matches = True
    if cut is not None:
        generation_matches = kafka.topic_generation == cut.topic_generation
        if not generation_matches:
            # Not necessarily fatal: with a live source the right move is a new
            # generation and a new cut. It is fatal for *this* cut.
            if not source_present:
                blocking.append(Reason.CUT_GENERATION_MIXED)
        else:
            for (topic, partition), (start, _) in cut.ranges.items():
                if start < kafka.log_start_offset(topic, partition):
                    range_available = False
                    break
            if not range_available and not source_present:
                blocking.append(Reason.CUT_RANGE_EXPIRED)
            elif not range_available:
                # The old cut is gone but the source can be republished. Record
                # it so the report cannot imply the old offsets still exist.
                pass
    elif not source_present:
        blocking.append(Reason.SOURCE_UNAVAILABLE)

    return ReplayFeasibility(
        source_sealed=source_sealed,
        source_present=source_present,
        kafka_range_available=range_available,
        generation_matches=generation_matches,
        schema_compatible=schema_compatible,
        blocking=tuple(dict.fromkeys(blocking)),
    )


@dataclass(frozen=True, slots=True)
class RecoveryPlan:
    """The chosen recovery route and why."""

    method: str
    rationale: str
    requires_new_generation: bool
    requires_restore: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "rationale": self.rationale,
            "requires_new_generation": self.requires_new_generation,
            "requires_restore": self.requires_restore,
        }


def choose_recovery(
    feasibility: ReplayFeasibility,
    *,
    sink_data_lost: bool = False,
    backup_restorable: bool = False,
) -> RecoveryPlan:
    """Pick a recovery route from the feasibility assessment.

    The precedence follows design 7.1 step 6: prefer reading Kafka from explicit
    offsets, fall back to republishing the sealed source into a *new* generation,
    and only then to a restore. ``hold`` is the honest outcome when nothing is
    available -- not a retry loop, which NFR-10 forbids.
    """
    if not feasibility.feasible:
        if sink_data_lost and backup_restorable:
            return RecoveryPlan(
                "restore_backup",
                "source and transport evidence insufficient, but a restorable backup exists",
                requires_new_generation=False,
                requires_restore=True,
            )
        return RecoveryPlan(
            "hold",
            f"blocked by {list(feasibility.blocking)}; no automatic reset or unbounded retry",
            requires_new_generation=False,
            requires_restore=False,
        )
    if feasibility.kafka_range_available and feasibility.generation_matches:
        return RecoveryPlan(
            "resume_and_reconcile",
            "the sealed Kafka range is still readable, so resume from explicit offsets",
            requires_new_generation=False,
            requires_restore=False,
        )
    if feasibility.source_present:
        return RecoveryPlan(
            "republish_new_generation",
            "the Kafka range is gone but the sealed source survives; republish and re-cut",
            requires_new_generation=True,
            requires_restore=False,
        )
    return RecoveryPlan(
        "hold", "no readable transport range and no source", False, False
    )


@dataclass(slots=True)
class RecoveryMetrics:
    """RTO/RPO measurements, kept apart by construction.

    All timestamps are monotonic nanoseconds. ``fault_confirmed_at_ns`` is the
    origin, per design 13.2: recovery time is measured from the confirmed fault,
    not from when somebody noticed.
    """

    fault_confirmed_at_ns: int | None = None
    service_restored_at_ns: int | None = None
    reconciled_at_ns: int | None = None
    rpo_unrecoverable: tuple[SourceIdentity, ...] = ()
    source_available_for_rpo: bool = True

    @property
    def rto_service_ns(self) -> int | None:
        if self.fault_confirmed_at_ns is None or self.service_restored_at_ns is None:
            return None
        return self.service_restored_at_ns - self.fault_confirmed_at_ns

    @property
    def rto_correct_ns(self) -> int | None:
        if self.fault_confirmed_at_ns is None or self.reconciled_at_ns is None:
            return None
        return self.reconciled_at_ns - self.fault_confirmed_at_ns

    def rpo_claim(self) -> dict[str, Any]:
        """The RPO statement, with its precondition attached.

        ``rpo_events`` of 0 is only meaningful alongside
        ``source_available_for_rpo``. NFR-11 ties the two together, and reporting
        the number without the condition is how a conditional zero becomes an
        unconditional promise.
        """
        return {
            "rpo_events": len(self.rpo_unrecoverable),
            "rpo_zero_claimable": not self.rpo_unrecoverable and self.source_available_for_rpo,
            "source_available": self.source_available_for_rpo,
            "unrecoverable_sample": [list(i) for i in self.rpo_unrecoverable[:20]],
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "rto_service_ns": self.rto_service_ns,
            "rto_correct_ns": self.rto_correct_ns,
            "rto_note": (
                "RTO-SERVICE is time to services answering; RTO-CORRECT is time to a reconciled "
                "cut and a terminal build decision. They are never summed or substituted."
            ),
            **self.rpo_claim(),
        }


def catch_up_seconds(backlog_events: int, arrival_rate: float, service_rate: float) -> float | None:
    """``B / (mu - lambda)`` when ``mu > lambda``, else ``None``.

    ``None`` is the point: with ``mu <= lambda`` the backlog cannot drain and
    there is no finite catch-up time to report. Design 7.4 also flags that this
    ignores startup, merge and read overhead, so the number is a lower bound and
    the report says so.
    """
    if service_rate <= arrival_rate:
        return None
    return backlog_events / (service_rate - arrival_rate)


def retention_headroom_ok(
    *,
    retention_seconds: float,
    outage_seconds: float,
    catch_up_seconds_value: float | None,
    verify_seconds: float,
    safety_factor: float = 1.5,
) -> tuple[bool, str]:
    """Whether retention covers outage plus catch-up plus verification.

    Returns ``False`` when the catch-up time is unknown, because an
    indeterminate drain cannot be shown to fit inside any window. Design 7.4
    also notes retention is bounded by bytes as well as time; this covers the
    time dimension only and says so.
    """
    if catch_up_seconds_value is None:
        return False, "catch-up time is unbounded (service rate <= arrival rate)"
    needed = (outage_seconds + catch_up_seconds_value + verify_seconds) * safety_factor
    if retention_seconds >= needed:
        return True, f"retention {retention_seconds}s >= needed {needed:.1f}s (time dimension only)"
    return False, f"retention {retention_seconds}s < needed {needed:.1f}s"
