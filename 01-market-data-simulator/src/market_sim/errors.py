"""Error taxonomy with stable exit codes.

Exit code contract (CLI):
    0  success / COMPLETED
    2  configuration or fault-plan rejected (fail-fast, nothing was produced)
    3  output error (file write/flush/fsync/rename, disk full, permissions)
    4  verification failed (incomplete, corrupt, invariant or manifest mismatch)
    5  replay input rejected
    6  runtime failure after start (drain timeout, background task crash)
    7  internal error (bug) -- traceback is always printed
"""

from __future__ import annotations


class SimError(Exception):
    """Base class. ``code`` is a stable machine-readable string."""

    exit_code: int = 7
    code: str = "INTERNAL"

    def __init__(self, message: str, *, code: str | None = None, **context: object) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        self.context: dict[str, object] = dict(context)

    def as_dict(self) -> dict[str, object]:
        return {
            "error": type(self).__name__,
            "code": self.code,
            "message": self.message,
            "exit_code": self.exit_code,
            "context": self.context,
        }

    def __str__(self) -> str:  # pragma: no cover - trivial
        if not self.context:
            return f"[{self.code}] {self.message}"
        ctx = " ".join(f"{k}={v!r}" for k, v in sorted(self.context.items()))
        return f"[{self.code}] {self.message} ({ctx})"


class ConfigError(SimError):
    exit_code = 2
    code = "CONFIG_INVALID"


class FaultPlanError(ConfigError):
    code = "FAULT_PLAN_INVALID"


class OutputError(SimError):
    exit_code = 3
    code = "OUTPUT_IO_ERROR"


class DiskBudgetError(OutputError):
    code = "OUTPUT_BUDGET_EXCEEDED"


class VerifyError(SimError):
    exit_code = 4
    code = "VERIFY_FAILED"


class ReplayError(SimError):
    exit_code = 5
    code = "REPLAY_INPUT_REJECTED"


class RuntimeFailure(SimError):
    exit_code = 6
    code = "RUNTIME_FAILURE"


class InvariantViolation(RuntimeFailure):
    """A *normal mode* invariant broke. This is always a bug in the generator."""

    code = "INVARIANT_VIOLATION"


class SchedulerLagExceeded(RuntimeFailure):
    code = "SCHEDULER_LAG_EXCEEDED"


class DrainTimeout(RuntimeFailure):
    code = "DRAIN_TIMEOUT"
