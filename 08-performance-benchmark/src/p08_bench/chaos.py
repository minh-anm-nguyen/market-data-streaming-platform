"""Fault injection: the allowlist, and the receipt that proves a fault landed.

Section 14 ends with the rule that shapes this module: a script exiting 0 is
**not** evidence that the fault hit its target. A fault without a receipt makes
the test ``INVALID``, because an uninjected fault tested nothing and a PASS from
it would be a false negative dressed as resilience.

So :class:`FaultInjector` cannot produce a receipt without a verifier -- a
callable that observes the target and confirms the condition actually holds.
``exit code 0`` is not accepted as that observation.

Section 8 supplies the other half: chaos touches only allowlisted compose
projects, networks and volumes, disk-fill never touches the root filesystem,
and ``--dry-run`` with exact target ids is the default. :class:`Allowlist`
enforces that, and the default for :attr:`FaultInjector.dry_run` is ``True`` so
a destructive action requires an explicit decision to arm it.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from .errors import SafetyError

#: System directories that must never be a fault target, nor contain one,
#: whatever the config says. Filling the root filesystem takes the host down,
#: not the test (section 8).
#:
#: The check is by containment, not equality: ``/etc`` being forbidden has to
#: forbid ``/etc/anything`` too, or the rule protects only one exact string.
#:
#: Home directories are deliberately **not** here. A lab volume legitimately
#: lives under a user profile -- that is where a temp directory is -- and the
#: allowlist is the mechanism that governs it. What this list protects is the
#: system itself: OS binaries, configuration and kernel interfaces, where a
#: disk-fill or a delete damages the host rather than the test. ``/root`` is
#: included because targeting it implies running as the superuser.
POSIX_FORBIDDEN_ROOTS: tuple[str, ...] = (
    "/boot", "/etc", "/usr", "/sys", "/proc", "/dev",
    "/bin", "/sbin", "/lib", "/lib64", "/root",
)


def _forbidden_roots() -> tuple[Path, ...]:
    """System roots for *this* platform.

    Resolved per-platform because the names do not transfer: ``Path("/etc")``
    on Windows resolves to a drive-relative path that has nothing to do with
    system configuration, and hardcoding ``C:\\Windows`` would miss a host whose
    Windows lives elsewhere.
    """
    roots: list[Path] = []
    if sys.platform.startswith("win"):
        # System-owned locations only. The user profile root is not included,
        # for the same reason ``/home`` is not: that is where lab volumes live.
        for variable in ("SystemRoot", "ProgramFiles", "ProgramFiles(x86)", "ProgramData"):
            value = os.environ.get(variable)
            if value:
                roots.append(Path(value))
    else:
        roots.extend(Path(name) for name in POSIX_FORBIDDEN_ROOTS)
    return tuple(roots)


class FaultKind(str, Enum):
    """Fault families from section 14's campaigns."""

    PROCESS_KILL = "PROCESS_KILL"
    PROCESS_STOP = "PROCESS_STOP"
    NETWORK_LATENCY = "NETWORK_LATENCY"
    NETWORK_LOSS = "NETWORK_LOSS"
    NETWORK_PARTITION = "NETWORK_PARTITION"
    DISK_FILL = "DISK_FILL"
    CPU_STRESS = "CPU_STRESS"
    MEMORY_STRESS = "MEMORY_STRESS"
    BROKER_LEADER_LOSS = "BROKER_LEADER_LOSS"
    CONSUMER_REBALANCE = "CONSUMER_REBALANCE"
    CLOCK_SKEW = "CLOCK_SKEW"
    #: Section 8's metrology fault class: break the instruments on purpose.
    OBSERVER_CRASH = "OBSERVER_CRASH"


@dataclass(frozen=True)
class Allowlist:
    """What chaos may touch. Everything else raises.

    Empty by default: a fresh injector can do nothing until a lab operator
    names the targets, which is the opposite of a tool that defaults to the
    whole host.
    """

    compose_projects: frozenset[str] = frozenset()
    networks: frozenset[str] = frozenset()
    volumes: frozenset[str] = frozenset()
    containers: frozenset[str] = frozenset()
    paths: tuple[str, ...] = ()
    allow_host_root_path: bool = False

    @classmethod
    def for_lab(
        cls,
        *,
        compose_projects: Iterable[str] = (),
        networks: Iterable[str] = (),
        volumes: Iterable[str] = (),
        containers: Iterable[str] = (),
        paths: Iterable[str | Path] = (),
        allow_host_root_path: bool = False,
    ) -> "Allowlist":
        return cls(
            compose_projects=frozenset(compose_projects),
            networks=frozenset(networks),
            volumes=frozenset(volumes),
            containers=frozenset(containers),
            paths=tuple(str(Path(p).resolve()) for p in paths),
            allow_host_root_path=allow_host_root_path,
        )

    def check_container(self, name: str) -> None:
        if name not in self.containers:
            raise SafetyError(
                "container is not on the lab allowlist", container=name,
                allowed=sorted(self.containers),
            )

    def check_network(self, name: str) -> None:
        if name not in self.networks:
            raise SafetyError(
                "network is not on the lab allowlist", network=name,
                allowed=sorted(self.networks),
            )

    def check_volume(self, name: str) -> None:
        if name not in self.volumes:
            raise SafetyError(
                "volume is not on the lab allowlist", volume=name,
                allowed=sorted(self.volumes),
            )

    def check_path(self, target: str | Path) -> Path:
        """Resolve a path and refuse anything outside the allowlisted roots.

        The forbidden-prefix check runs first and is not overridable by listing
        a parent: a disk-fill aimed at the root filesystem is refused even if
        someone allowlisted ``/``.
        """
        resolved = Path(target).resolve()
        as_text = str(resolved)
        if not self.allow_host_root_path:
            # A filesystem root is always refused, on every platform: filling a
            # drive root is indistinguishable from filling the host.
            if resolved == Path(resolved.anchor):
                raise SafetyError(
                    "refusing to target a filesystem root; disk-fill must stay "
                    "inside a test volume",
                    path=as_text,
                )
            for root in _forbidden_roots():
                if resolved == root or resolved.is_relative_to(root):
                    raise SafetyError(
                        "refusing to target a host system path; disk-fill must "
                        "stay inside a test volume",
                        path=as_text,
                        system_root=str(root),
                    )
        if not self.paths:
            raise SafetyError("no fault paths are allowlisted", path=as_text)
        for allowed in self.paths:
            try:
                resolved.relative_to(allowed)
            except ValueError:
                continue
            return resolved
        raise SafetyError(
            "path is outside every allowlisted root", path=as_text, allowed=list(self.paths)
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "compose_projects": sorted(self.compose_projects),
            "networks": sorted(self.networks),
            "volumes": sorted(self.volumes),
            "containers": sorted(self.containers),
            "paths": list(self.paths),
            "allow_host_root_path": self.allow_host_root_path,
        }


@dataclass(frozen=True)
class FaultSpec:
    """A fault with the seven fields section 14 requires of every injection."""

    fault_id: str
    kind: FaultKind
    target: str
    start_condition: str
    end_condition: str
    #: What observing the target must show for the fault to count as landed.
    verification: str
    parameters: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("fault_id", "target", "start_condition", "end_condition", "verification"):
            if not str(getattr(self, name)).strip():
                raise SafetyError(f"fault spec needs a {name}", fault_id=self.fault_id)

    def as_dict(self) -> dict[str, Any]:
        return {
            "fault_id": self.fault_id,
            "kind": self.kind.value,
            "target": self.target,
            "start_condition": self.start_condition,
            "end_condition": self.end_condition,
            "verification": self.verification,
            "parameters": dict(self.parameters),
        }


class Landing(str, Enum):
    """Whether the stimulus actually reached its target."""

    VERIFIED = "VERIFIED"
    #: The command ran and the target shows no sign of it.
    NOT_TRIGGERED = "NOT_TRIGGERED"
    #: Nothing was attempted; the injector was in dry-run mode.
    DRY_RUN = "DRY_RUN"
    #: The verifier itself could not tell.
    UNVERIFIABLE = "UNVERIFIABLE"


@dataclass(frozen=True)
class FaultReceipt:
    """Evidence about one injection attempt.

    ``landing`` is the field that decides a chaos case. Section 14: without a
    verified landing the test is ``INVALID``, not a pass and not a failure.
    """

    spec: FaultSpec
    landing: Landing
    injected_at_ns: int | None
    cleared_at_ns: int | None
    verification_detail: str
    cleanup_verified: bool
    proof_cut: str | None = None

    @property
    def valid(self) -> bool:
        """Whether this receipt can support a chaos verdict."""
        return self.landing is Landing.VERIFIED and self.cleanup_verified

    @property
    def duration_ns(self) -> int | None:
        if self.injected_at_ns is None or self.cleared_at_ns is None:
            return None
        return self.cleared_at_ns - self.injected_at_ns

    def as_dict(self) -> dict[str, Any]:
        return {
            "fault": self.spec.as_dict(),
            "landing": self.landing.value,
            "injected_at_ns": self.injected_at_ns,
            "cleared_at_ns": self.cleared_at_ns,
            "duration_ns": self.duration_ns,
            "verification_detail": self.verification_detail,
            "cleanup_verified": self.cleanup_verified,
            "proof_cut": self.proof_cut,
            "valid": self.valid,
            "note": "an exit code of 0 is not evidence that the fault landed",
        }


#: An action performs the injection and returns a free-form detail string.
#: A verifier observes the target and returns True only if the condition holds.
FaultAction = Callable[[FaultSpec], str]
FaultVerifier = Callable[[FaultSpec], bool | None]


class FaultInjector:
    """Runs faults inside the allowlist and refuses unverifiable receipts."""

    __slots__ = ("allowlist", "dry_run", "clock", "_receipts")

    def __init__(self, allowlist: Allowlist, clock, *, dry_run: bool = True) -> None:
        self.allowlist = allowlist
        self.clock = clock
        self.dry_run = dry_run
        self._receipts: list[FaultReceipt] = []

    @property
    def receipts(self) -> list[FaultReceipt]:
        return list(self._receipts)

    def _check_target(self, spec: FaultSpec) -> None:
        if spec.kind is FaultKind.DISK_FILL:
            self.allowlist.check_path(spec.target)
        elif spec.kind in (
            FaultKind.NETWORK_LATENCY, FaultKind.NETWORK_LOSS, FaultKind.NETWORK_PARTITION
        ):
            self.allowlist.check_network(spec.target)
        else:
            self.allowlist.check_container(spec.target)

    def inject(
        self,
        spec: FaultSpec,
        action: FaultAction,
        verifier: FaultVerifier,
        *,
        cleanup: FaultAction | None = None,
        cleanup_verifier: FaultVerifier | None = None,
        proof_cut: str | None = None,
    ) -> FaultReceipt:
        """Apply a fault, verify it landed, clear it, verify the cleanup.

        The verifier is mandatory. A fault whose effect nobody confirmed leaves
        a receipt with ``landing=NOT_TRIGGERED`` or ``UNVERIFIABLE``, and either
        of those makes the surrounding case ``INVALID``.
        """
        self._check_target(spec)

        if self.dry_run:
            receipt = FaultReceipt(
                spec=spec,
                landing=Landing.DRY_RUN,
                injected_at_ns=None,
                cleared_at_ns=None,
                verification_detail="dry run: the target was validated, nothing was injected",
                cleanup_verified=True,
            )
            self._receipts.append(receipt)
            return receipt

        injected_at = self.clock.now_ns()
        detail = action(spec)
        observed = verifier(spec)
        if observed is None:
            landing = Landing.UNVERIFIABLE
        elif observed:
            landing = Landing.VERIFIED
        else:
            landing = Landing.NOT_TRIGGERED

        cleared_at: int | None = None
        cleanup_ok = True
        cleanup_detail = ""
        if cleanup is not None:
            cleanup_detail = cleanup(spec)
            cleared_at = self.clock.now_ns()
            if cleanup_verifier is not None:
                verified = cleanup_verifier(spec)
                cleanup_ok = verified is True
            else:
                # Section 14 wants a cleanup receipt, and an unverified cleanup
                # is not one: the next run would start from an unknown state.
                cleanup_ok = False
                cleanup_detail += " (cleanup not verified)"

        receipt = FaultReceipt(
            spec=spec,
            landing=landing,
            injected_at_ns=injected_at,
            cleared_at_ns=cleared_at,
            verification_detail="; ".join(filter(None, [detail, cleanup_detail])),
            cleanup_verified=cleanup_ok,
            proof_cut=proof_cut,
        )
        self._receipts.append(receipt)
        return receipt

    def campaign_summary(self) -> dict[str, Any]:
        receipts = self._receipts
        return {
            "requested": len(receipts),
            "verified": sum(1 for r in receipts if r.landing is Landing.VERIFIED),
            "not_triggered": sum(1 for r in receipts if r.landing is Landing.NOT_TRIGGERED),
            "unverifiable": sum(1 for r in receipts if r.landing is Landing.UNVERIFIABLE),
            "dry_run": sum(1 for r in receipts if r.landing is Landing.DRY_RUN),
            "cleanup_failures": sum(1 for r in receipts if not r.cleanup_verified),
            "all_valid": all(r.valid for r in receipts) if receipts else False,
            "receipts": [r.as_dict() for r in receipts],
            "dry_run_mode": self.dry_run,
            "allowlist": self.allowlist.as_dict(),
        }


def exit_code_is_not_proof(exit_code: int) -> bool:
    """Always ``False``. A named refusal for section 14's rule.

    Exists so the temptation has somewhere to land: a reviewer seeing this
    called knows the author tried to use a return code as a fault receipt.
    """
    return False


def default_lab_allowlist(test_root: Path) -> Allowlist:
    """A minimal allowlist scoped to one test directory.

    Not a recommendation for any particular lab: the compose project, networks
    and containers have to be named by whoever owns the machine. This exists so
    a local run has something safe and obviously narrow to start from.
    """
    resolved = test_root.resolve()
    if resolved == Path(resolved.anchor) or any(
        resolved == root or resolved.is_relative_to(root) for root in _forbidden_roots()
    ):
        raise SafetyError(
            "test root may not be a filesystem root", path=str(resolved)
        )
    return Allowlist.for_lab(
        compose_projects=("p08-bench-lab",),
        networks=("p08-bench-lab_default",),
        volumes=("p08-bench-lab_clickhouse", "p08-bench-lab_kafka"),
        containers=(),
        paths=(resolved,),
        allow_host_root_path=False,
    )


def platform_chaos_limitations() -> list[str]:
    """What this host cannot inject, stated rather than attempted.

    ``netem``, cgroup stress and container signals are Linux facilities. On
    another platform the honest output is that the chaos campaigns cannot run
    here, not a silently empty campaign that looks like it passed.
    """
    if sys.platform.startswith("linux"):
        return []
    return [
        f"host platform is {sys.platform}: netem network shaping, cgroup CPU and "
        "memory stress, and container signal injection are unavailable, so the "
        "section 14 fault campaigns cannot be executed here"
    ]
