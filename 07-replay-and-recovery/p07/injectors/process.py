"""Process-level faults: kill a component at a named boundary.

A component here is an in-process object, so "kill" means raising
:class:`ProcessKilled` out of its next operation and discarding whatever
in-memory state it held. That is a faithful model of the part that matters for
recovery: the state is gone, the side effects already written are not, and
restart has to rebuild position from durable storage alone.

:func:`arm_crash_at` returns the hook that
:class:`~p07.dataplane.ingestion.BatchIngestor` and
:class:`~p07.dataplane.analytics.BuildPipeline` accept as ``crash_after``. The
stage names are design 5.3's boundaries, so a scenario kills at a boundary
rather than after an interval -- the distinction design 6.3 insists on.
"""

from __future__ import annotations

from typing import Any, Callable

from ..evidence.proof_ledger import FaultReceipt
from .base import DEFAULT_ALLOWLIST, LabAllowlist, inject


class ProcessKilled(Exception):
    """The component died at ``stage``. Carries the context for the receipt."""

    def __init__(self, stage: str, context: dict[str, Any]) -> None:
        super().__init__(f"process killed at {stage}")
        self.stage = stage
        self.context = context


class CrashSwitch:
    """A one-shot crash hook, armed for one stage.

    Keeping the fired state here, rather than in a closure, lets the injector
    read it back afterwards -- which is how the receipt gets its independent
    observation instead of assuming the kill happened.
    """

    def __init__(self, stage: str, *, after_calls: int = 0) -> None:
        self.stage = stage
        self.after_calls = after_calls
        self.calls_seen = 0
        self.fired = False
        self.fire_context: dict[str, Any] = {}

    def __call__(self, stage: str, **context: Any) -> None:
        if stage != self.stage or self.fired:
            return
        self.calls_seen += 1
        if self.calls_seen <= self.after_calls:
            return
        self.fired = True
        self.fire_context = {key: repr(value) for key, value in context.items()}
        raise ProcessKilled(stage, self.fire_context)


def arm_crash_at(stage: str, *, after_calls: int = 0) -> CrashSwitch:
    """Build a crash hook for ``stage``, skipping ``after_calls`` first hits."""
    return CrashSwitch(stage, after_calls=after_calls)


def kill_component(
    *,
    component_name: str,
    stop: Callable[[], Any],
    is_stopped: Callable[[], bool],
    fault_id: str = "process-kill",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Stop a component and confirm it is stopped by reading its state back.

    ``is_stopped`` must observe something real -- a closed stream, a cleared
    assignment, a flag the component itself sets. Design 1.1 FR-04 forbids
    concluding recovery "solely" from liveness, and the mirror of that applies
    here: a kill is not confirmed by the kill call returning.
    """
    return inject(
        fault_id=fault_id,
        intended_fault="process_kill",
        target=component_name,
        allowlist=allowlist,
        precondition=lambda: not is_stopped(),
        action=stop,
        observe=lambda: (is_stopped(), f"{component_name} reports stopped"),
        detail={"component": component_name},
    )


def confirm_crash_fired(
    switch: CrashSwitch,
    *,
    component_name: str,
    fault_id: str = "process-crash-at-stage",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Turn an already-fired :class:`CrashSwitch` into a receipt.

    Called *after* the work that was supposed to crash. If the switch never
    fired, the receipt is unconfirmed and the scenario is ``INVALID`` -- which
    is case ``E2E-37`` exactly: an injector that failed to apply its fault must
    not produce a pass.
    """
    return inject(
        fault_id=fault_id,
        intended_fault=f"crash_at:{switch.stage}",
        target=component_name,
        allowlist=allowlist,
        precondition=lambda: True,
        action=lambda: switch.fire_context,
        observe=lambda: (
            switch.fired,
            f"crash fired at {switch.stage}" if switch.fired else "crash never fired",
        ),
        detail={"stage": switch.stage, "calls_seen": switch.calls_seen},
    )
