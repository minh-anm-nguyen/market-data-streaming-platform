"""Target allowlist and the receipt-building helper every injector uses.

The helper exists so the six confirmation facts cannot be filled in by hand.
:func:`inject` takes three callables -- a precondition check, the action, and an
*independent* observation -- and records what each one actually returned. An
injector that forgets the observation gets ``independent_observation_of_effect
= False`` and therefore an unconfirmed receipt, which the gate turns into
``INVALID``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from ..evidence.proof_ledger import FaultReceipt
from ..evidence.verdicts import Reason


class TargetNotAllowed(Exception):
    """The target is not on the lab allowlist. Nothing was done."""

    def __init__(self, target: str, allowed: Sequence[str]) -> None:
        super().__init__(f"{Reason.TARGET_NOT_ALLOWLISTED}: {target!r} not in {sorted(allowed)}")
        self.reason = Reason.TARGET_NOT_ALLOWLISTED
        self.target = target


@dataclass(frozen=True, slots=True)
class LabAllowlist:
    """The only targets this lab may touch.

    ``destructive_requires_confirmation`` guards the operations that destroy
    data -- dropping a volume, expiring retention. Design 14.2 refuses a
    ``--force-delete`` outside an allowlist, and this is the same refusal one
    level down: the caller has to pass ``confirm=True`` as well as being on the
    list.
    """

    targets: frozenset[str]
    lab_name: str = "md-p07-model"
    protect_host: bool = True
    destructive_requires_confirmation: bool = True

    def check(self, target: str) -> None:
        if target not in self.targets:
            raise TargetNotAllowed(target, self.targets)

    def check_destructive(self, target: str, *, confirm: bool) -> None:
        self.check(target)
        if self.destructive_requires_confirmation and not confirm:
            raise TargetNotAllowed(f"{target} (destructive, unconfirmed)", self.targets)


#: The default allowlist for the in-process model lab. Every name is an object
#: in this process; none of them is reachable infrastructure.
DEFAULT_ALLOWLIST = LabAllowlist(
    targets=frozenset(
        {
            "p03_adapter",
            "p04_producer",
            "kafka_broker",
            "kafka_topic",
            "p05_ingestor",
            "clickhouse_ingest",
            "clickhouse_server",
            "p06_builder",
            "network_proxy",
            "resource_budget",
        }
    )
)


def inject(
    *,
    fault_id: str,
    intended_fault: str,
    target: str,
    allowlist: LabAllowlist,
    precondition: Callable[[], bool],
    action: Callable[[], Any],
    observe: Callable[[], tuple[bool, str | None]],
    cleanup: Callable[[], Any] | None = None,
    detail: Mapping[str, Any] | None = None,
    confirm_destructive: bool = False,
    destructive: bool = False,
) -> FaultReceipt:
    """Run one injection and return its receipt.

    Order: allowlist, precondition, action, independent observation, cleanup.
    The observation runs *after* the action and reads real state; its boolean is
    recorded as-is. A failed precondition short-circuits with an unconfirmed
    receipt rather than injecting anyway -- firing at the wrong moment produces
    a test that looks like it covered a boundary it never reached.
    """
    if destructive:
        allowlist.check_destructive(target, confirm=confirm_destructive)
    else:
        allowlist.check(target)

    started = time.time_ns()
    precondition_ok = bool(precondition())
    if not precondition_ok:
        return FaultReceipt(
            fault_id=fault_id,
            intended_fault=intended_fault,
            target=target,
            trigger_precondition_verified=False,
            intended_target_matched=True,
            injection_command_acknowledged=False,
            independent_observation_of_effect=False,
            observed_effect=None,
            started_at_ns=started,
            ended_at_ns=time.time_ns(),
            cleanup_recorded=True,
            detail={**dict(detail or {}), "aborted": "precondition_not_met"},
        )

    acknowledged = True
    action_detail: Any = None
    try:
        action_detail = action()
    except Exception as exc:  # noqa: BLE001 - the failure itself is the evidence
        acknowledged = False
        action_detail = f"{type(exc).__name__}: {exc}"

    observed, observed_effect = observe()

    cleanup_recorded = True
    if cleanup is not None:
        try:
            cleanup()
        except Exception as exc:  # noqa: BLE001
            # A missing cleanup receipt makes the lab unusable for later tests
            # (design 8: "missing cleanup receipt -> no-go lab"), so it is
            # recorded rather than swallowed.
            cleanup_recorded = False
            observed_effect = f"{observed_effect}; cleanup failed: {exc}"

    return FaultReceipt(
        fault_id=fault_id,
        intended_fault=intended_fault,
        target=target,
        trigger_precondition_verified=precondition_ok,
        intended_target_matched=True,
        injection_command_acknowledged=acknowledged,
        independent_observation_of_effect=bool(observed),
        observed_effect=observed_effect,
        started_at_ns=started,
        ended_at_ns=time.time_ns(),
        cleanup_recorded=cleanup_recorded,
        detail={**dict(detail or {}), "action_result": action_detail},
    )
