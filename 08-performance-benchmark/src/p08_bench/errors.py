"""Structured failures and exit codes (section 15 of PROJECT_08).

Every failure path in P08 carries a stable ``code``, because a benchmark that
dies has to say *which* contract it broke: a bad manifest is not a bad clock,
and neither one is "the system under test was slow". Section 15 forbids the
analyzer from turning an unknown into a default, so the error taxonomy is the
mechanism that makes an unknown loud.
"""

from __future__ import annotations

from typing import Any


class BenchError(Exception):
    """Base class. ``code`` is stable; the message is for humans."""

    code = "bench_error"
    exit_code = 1

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context = context

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"code": self.code, "error": self.message}
        if self.context:
            payload["context"] = {k: v for k, v in sorted(self.context.items())}
        return payload


class ConfigError(BenchError):
    """A workload/profile config is missing keys, has unknown keys or bad values."""

    code = "config_error"
    exit_code = 2


class ManifestError(BenchError):
    """Provenance broke: digest mismatch, unsealed source, reused run_id (G01)."""

    code = "manifest_error"
    exit_code = 3


class UnitError(BenchError):
    """Two quantities with different units were combined (INV-12)."""

    code = "unit_error"
    exit_code = 4


class ClockError(BenchError):
    """A duration was taken across clock domains, or a clock went backwards (INV-15)."""

    code = "clock_error"
    exit_code = 5


class MetrologyError(BenchError):
    """The measurement itself is invalid: censored samples mishandled, a
    percentile of percentiles, a histogram too coarse for the threshold."""

    code = "metrology_error"
    exit_code = 6


class SafetyError(BenchError):
    """A destructive action targeted something outside the lab allowlist (section 8)."""

    code = "safety_error"
    exit_code = 7


class UpstreamUnavailable(BenchError):
    """A lane needs a P02-P07 implementation that does not exist yet.

    This is deliberately not a generic failure: section 20 requires such lanes
    to be reported as ``BLOCKED_BY_UPSTREAM_IMPLEMENTATION``, never as a FAIL of
    the system under test and never as a PASS by omission.
    """

    code = "upstream_unavailable"
    exit_code = 8


EXIT_CODES: dict[str, int] = {
    cls.code: cls.exit_code
    for cls in (
        BenchError,
        ConfigError,
        ManifestError,
        UnitError,
        ClockError,
        MetrologyError,
        SafetyError,
        UpstreamUnavailable,
    )
}
