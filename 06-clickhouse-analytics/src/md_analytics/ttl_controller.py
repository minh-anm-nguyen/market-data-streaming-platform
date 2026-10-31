"""The retention gate: TTL stays off until keeping data is provably safe.

Section 9 and matrix group O. NFR-11 states the rule plainly -- retention must
be at least as long as the longest window that still needs the data, and a
restore drill must have actually run. Both halves matter: a 90-day TTL with no
tested restore is a 90-day countdown to an unrecoverable gap.

Two properties of ClickHouse TTL shape everything here:

* **It is tied to merges, so it is late.** Rows past their deadline can still be
  present. "Deleted by the deadline" is not an SLA and must not be alerted on as
  though it were (case O-01).
* **It is evaluated on event time.** A record that arrives already older than the
  TTL can vanish at the next merge, effectively on arrival -- which is the trap
  in case O-03: a late replay silently drops instead of landing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from .errors import Verdict


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """Candidate retention settings and the windows they have to cover."""

    raw_ttl_days: int
    recovery_window_days: int
    audit_window_days: int
    rebuild_window_days: int
    late_arrival_window_days: int
    restore_drill_passed: bool
    backup_age_days: int | None
    backup_max_age_days: int
    enabled: bool = False

    def required_days(self) -> int:
        return max(
            self.recovery_window_days,
            self.audit_window_days,
            self.rebuild_window_days,
            self.late_arrival_window_days,
        )


@dataclass(slots=True)
class RetentionGate:
    """Whether TTL may be enabled, and why not if it may not."""

    allowed: bool
    required_days: int
    policy: RetentionPolicy
    reasons: list[str] = field(default_factory=list)

    def verdict(self) -> Verdict:
        """``PASS`` means TTL may be switched on, not that it is on."""
        return Verdict.PASS if self.allowed else Verdict.NO_GO

    def as_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "verdict": str(self.verdict()),
            "required_days": self.required_days,
            "raw_ttl_days": self.policy.raw_ttl_days,
            "reasons": list(self.reasons),
        }


def evaluate_retention_gate(policy: RetentionPolicy) -> RetentionGate:
    """Decide whether TTL may be enabled (NFR-11).

    Every condition is checked and reported, rather than short-circuiting, so an
    operator sees the whole list of what is missing before planning the work.
    """
    required = policy.required_days()
    reasons: list[str] = []

    if policy.raw_ttl_days < required:
        reasons.append(
            f"raw_ttl_days={policy.raw_ttl_days} is shorter than the longest window "
            f"that still needs the data ({required} days: recovery="
            f"{policy.recovery_window_days}, audit={policy.audit_window_days}, "
            f"rebuild={policy.rebuild_window_days}, late="
            f"{policy.late_arrival_window_days})"
        )
    if not policy.restore_drill_passed:
        reasons.append(
            "no successful restore drill on record; a backup that has never been "
            "restored is not a recovery plan"
        )
    if policy.backup_age_days is None:
        reasons.append("backup age is unknown, so recoverability is unproven")
    elif policy.backup_age_days > policy.backup_max_age_days:
        reasons.append(
            f"newest backup is {policy.backup_age_days} days old, over the "
            f"{policy.backup_max_age_days}-day limit"
        )
    if policy.raw_ttl_days <= 0:
        reasons.append("raw_ttl_days must be positive")

    return RetentionGate(
        allowed=not reasons, required_days=required, policy=policy, reasons=reasons
    )


def ttl_cutoff(*, today: date, ttl_days: int) -> date:
    """The oldest ``event_date`` TTL would keep.

    Rows strictly before this date are expired. The boundary is inclusive of the
    cutoff so a run on day N and a run on day N with a different clock reading
    agree, which matters because TTL merges fire at arbitrary times.
    """
    if ttl_days <= 0:
        raise ValueError(f"ttl_days must be positive, got {ttl_days}")
    return today - timedelta(days=ttl_days)


def would_expire_on_arrival(*, event_date: date, today: date, ttl_days: int) -> bool:
    """Does a record arriving now already sit past its TTL? (case O-03)

    A late replay that answers ``True`` must be refused with an explicit
    "cannot be stored under this retention policy" rather than inserted and
    dropped at the next merge, which looks identical to never having arrived.
    """
    return event_date < ttl_cutoff(today=today, ttl_days=ttl_days)


@dataclass(slots=True)
class ReconstructabilityCheck:
    """Can a build still be rebuilt from something? (INV-12)"""

    raw_present: bool
    source_replayable: bool
    backup_restorable: bool

    def verdict(self) -> Verdict:
        if self.raw_present or self.source_replayable or self.backup_restorable:
            return Verdict.PASS
        # Nothing left to rebuild from. The honest answer is that the data is
        # gone, not that the bars derived from it are still trustworthy.
        return Verdict.NO_GO

    def reason(self) -> str:
        if self.verdict() is Verdict.PASS:
            available = [
                name
                for name, present in (
                    ("raw", self.raw_present),
                    ("source replay", self.source_replayable),
                    ("backup", self.backup_restorable),
                )
                if present
            ]
            return f"reconstructable from: {', '.join(available)}"
        return "NOT_RECONSTRUCTABLE: no raw rows, no source replay and no restorable backup"
