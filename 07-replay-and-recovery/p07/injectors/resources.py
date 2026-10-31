"""Resource pressure: queue caps, memory budgets, and a watcher that fails loudly.

Design 8's rule for every budget is the same: crossing it must cause a
*controlled stop with a reason code*, never a silent drop and never a success
that happens to have lost data. ``INV-16`` is that rule as an invariant, and
:class:`ResourceBudget` is how a scenario states the numbers it is holding
itself to.

:func:`shrink_producer_queue` is the only injector here that changes the system
under test; the rest observe. That asymmetry is deliberate -- a memory limit in
this model lab cannot be imposed by a cgroup, so claiming a cgroup test would be
false. What *can* be tested exactly is that the bounded queue refuses at its cap
instead of growing, which is the behaviour the cgroup would otherwise catch.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..evidence.proof_ledger import FaultReceipt
from ..evidence.verdicts import Finding, Reason
from .base import DEFAULT_ALLOWLIST, LabAllowlist, inject


@dataclass(frozen=True, slots=True)
class ResourceBudget:
    """The caps a scenario declares, and which the report publishes."""

    max_producer_queue_events: int = 50_000
    max_producer_queue_bytes: int = 64 * 1024 * 1024
    max_raw_rows: int = 5_000_000
    max_evidence_bytes: int = 1 << 30
    max_rss_bytes: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "max_producer_queue_events": self.max_producer_queue_events,
            "max_producer_queue_bytes": self.max_producer_queue_bytes,
            "max_raw_rows": self.max_raw_rows,
            "max_evidence_bytes": self.max_evidence_bytes,
            "max_rss_bytes": self.max_rss_bytes,
            "note": (
                "In-process model lab: these are application-level caps. No cgroup, so "
                "kernel-level CPU/memory pressure is NOT exercised (group G23/G24 remain NOT_RUN)."
            ),
        }


def check_budget(
    budget: ResourceBudget,
    *,
    producer_queue_events: int = 0,
    producer_queue_bytes: int = 0,
    raw_rows: int = 0,
    evidence_bytes: int = 0,
    rss_bytes: int | None = None,
) -> list[Finding]:
    """Findings for every budget that was exceeded.

    Returned as findings rather than raised, so a run that overran its budget is
    reported with a reason code and its data still reconciled -- which is what
    distinguishes "we stopped on purpose" from "we crashed".
    """
    findings: list[Finding] = []
    checks = (
        ("producer_queue_events", producer_queue_events, budget.max_producer_queue_events),
        ("producer_queue_bytes", producer_queue_bytes, budget.max_producer_queue_bytes),
        ("raw_rows", raw_rows, budget.max_raw_rows),
        ("evidence_bytes", evidence_bytes, budget.max_evidence_bytes),
    )
    for name, observed, cap in checks:
        if cap is not None and observed > cap:
            findings.append(
                Finding(Reason.RESOURCE_BUDGET_EXCEEDED, {"budget": name, "observed": observed, "cap": cap})
            )
    if budget.max_rss_bytes is not None and rss_bytes is not None and rss_bytes > budget.max_rss_bytes:
        findings.append(
            Finding(
                Reason.RESOURCE_BUDGET_EXCEEDED,
                {"budget": "rss_bytes", "observed": rss_bytes, "cap": budget.max_rss_bytes},
            )
        )
    return findings


def shrink_producer_queue(
    producer: object,
    *,
    max_events: int,
    fault_id: str = "resource-queue-shrink",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Shrink the producer's queue cap so backpressure is reached immediately.

    Exercises the refuse-at-the-cap path without having to generate tens of
    thousands of events, which keeps the backpressure case cheap enough to run
    on every commit.
    """
    attribute = "_max_queue_events"
    before = getattr(producer, attribute, None)
    return inject(
        fault_id=fault_id,
        intended_fault="queue_cap_shrink",
        target="resource_budget",
        allowlist=allowlist,
        precondition=lambda: before is not None and before > max_events,
        action=lambda: setattr(producer, attribute, max_events),
        observe=lambda: (
            getattr(producer, attribute, None) == max_events,
            f"queue cap {before} -> {max_events}",
        ),
        detail={"before": before, "after": max_events},
    )


def rss_bytes() -> int | None:
    """Current resident set size, or ``None`` when it cannot be measured.

    ``psutil`` is an optional dependency, so the absence of a number is reported
    as absence. A zero would look like a measurement.
    """
    try:
        import psutil  # noqa: PLC0415 - optional dependency, imported on use
    except ImportError:
        return None
    return int(psutil.Process().memory_info().rss)


def measure(callback: Callable[[], Any]) -> tuple[Any, int | None, int | None]:
    """Run ``callback`` and report RSS before and after it.

    Two samples, not a trend. Design 13.2 wants a memory *slope* over a soak,
    and this is the primitive those tests sample repeatedly -- it is not itself
    a leak check.
    """
    before = rss_bytes()
    result = callback()
    return result, before, rss_bytes()
