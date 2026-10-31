"""Stable, machine-readable rejection reasons (section 4.2).

Every rejection in this project carries one of these codes. They are stable
identifiers: metrics label on them, the test suite asserts on them, and a report
that says only "failed" is treated as an incomplete report.

The split that matters is *structural* against *business*:

* A structural failure (framing, Protobuf parse, presence, size, UTF-8) is
  always a rejection. No mode and no manifest can turn it into an accepted
  event, because the bytes genuinely do not describe a well-formed event.
* A business violation (negative price, crossed quote, non-positive sequence) is
  a rejection under ``strict_normal`` and a *preserved* value under
  ``fault_passthrough`` -- but only when Project 01's fault manifest says that
  exact anomaly was injected on purpose. A real corruption and a deliberate
  fault must never be allowed to look alike.
"""

from __future__ import annotations

from enum import Enum
from typing import Final


class Reason(str, Enum):
    """Rejection codes. ``str`` based so they serialise directly into reports."""

    # --- framing: MDP1 prefix, BE32 header, body (section 4.2) ---------------
    BAD_MAGIC = "BAD_MAGIC"
    TRUNCATED_PREFIX = "TRUNCATED_PREFIX"
    TRUNCATED_HEADER = "TRUNCATED_HEADER"
    INVALID_LENGTH = "INVALID_LENGTH"
    TRUNCATED_BODY = "TRUNCATED_BODY"
    READ_DEADLINE_EXCEEDED = "READ_DEADLINE_EXCEEDED"

    # --- Protobuf wire ------------------------------------------------------
    DECODE_ERROR = "DECODE_ERROR"
    PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"

    # --- envelope / presence ------------------------------------------------
    MISSING_REQUIRED_SEMANTIC_FIELD = "MISSING_REQUIRED_SEMANTIC_FIELD"
    UNSUPPORTED_EVENT_KIND = "UNSUPPORTED_EVENT_KIND"
    STRING_TOO_LONG = "STRING_TOO_LONG"
    INVALID_UTF8 = "INVALID_UTF8"
    INVALID_SYMBOL = "INVALID_SYMBOL"
    INVALID_DATASET_ID = "INVALID_DATASET_ID"
    UNSUPPORTED_SOURCE_SCHEMA_VERSION = "UNSUPPORTED_SOURCE_SCHEMA_VERSION"

    # --- strict JSONL input (section 3.3 layer A, cases C01-C17) ------------
    JSON_LINE_TOO_LARGE = "JSON_LINE_TOO_LARGE"
    INVALID_JSON = "INVALID_JSON"
    NOT_AN_OBJECT = "NOT_AN_OBJECT"
    DUPLICATE_JSON_KEY = "DUPLICATE_JSON_KEY"
    FLOAT_NOT_ALLOWED = "FLOAT_NOT_ALLOWED"
    NON_FINITE_NUMBER = "NON_FINITE_NUMBER"
    FIELD_NOT_INTEGER = "FIELD_NOT_INTEGER"
    INT64_OUT_OF_RANGE = "INT64_OUT_OF_RANGE"
    NUMERIC_TOKEN_TOO_LONG = "NUMERIC_TOKEN_TOO_LONG"
    FIELD_SET_MISMATCH = "FIELD_SET_MISMATCH"
    UNKNOWN_EVENT_TYPE = "UNKNOWN_EVENT_TYPE"

    # --- business policy (section 3.3 layer B) ------------------------------
    BUSINESS_VIOLATION = "BUSINESS_VIOLATION"
    FAULT_NOT_IN_MANIFEST = "FAULT_NOT_IN_MANIFEST"

    # --- operational --------------------------------------------------------
    QUEUE_BUDGET_EXCEEDED = "QUEUE_BUDGET_EXCEEDED"
    VERSION_POLICY_REJECT = "VERSION_POLICY_REJECT"
    SOURCE_NOT_COMPLETED = "SOURCE_NOT_COMPLETED"
    CONFIG_INVALID = "CONFIG_INVALID"
    SCHEMA_GATE_REJECT = "SCHEMA_GATE_REJECT"


#: Codes that no mode may downgrade. ``fault_passthrough`` widens what counts as
#: an acceptable *value*; it never widens what counts as well-formed *bytes*.
STRUCTURAL_REASONS: Final[frozenset[Reason]] = frozenset(
    {
        Reason.BAD_MAGIC,
        Reason.TRUNCATED_PREFIX,
        Reason.TRUNCATED_HEADER,
        Reason.INVALID_LENGTH,
        Reason.TRUNCATED_BODY,
        Reason.READ_DEADLINE_EXCEEDED,
        Reason.DECODE_ERROR,
        Reason.PAYLOAD_TOO_LARGE,
        Reason.MISSING_REQUIRED_SEMANTIC_FIELD,
        Reason.UNSUPPORTED_EVENT_KIND,
        Reason.STRING_TOO_LONG,
        Reason.INVALID_UTF8,
        Reason.JSON_LINE_TOO_LARGE,
        Reason.INVALID_JSON,
        Reason.NOT_AN_OBJECT,
        Reason.DUPLICATE_JSON_KEY,
        Reason.FLOAT_NOT_ALLOWED,
        Reason.NON_FINITE_NUMBER,
        Reason.FIELD_NOT_INTEGER,
        Reason.INT64_OUT_OF_RANGE,
        Reason.NUMERIC_TOKEN_TOO_LONG,
        Reason.FIELD_SET_MISMATCH,
        Reason.UNKNOWN_EVENT_TYPE,
    }
)


class ContractError(Exception):
    """Base class. Always carries a :class:`Reason`."""

    reason: Reason

    def __init__(self, reason: Reason, detail: str = "", **context: object) -> None:
        super().__init__(f"{reason.value}: {detail}" if detail else reason.value)
        self.reason = reason
        self.detail = detail
        self.context = context

    @property
    def is_structural(self) -> bool:
        return self.reason in STRUCTURAL_REASONS

    def as_dict(self) -> dict[str, object]:
        return {
            "reason": self.reason.value,
            "detail": self.detail,
            "structural": self.is_structural,
            **({"context": self.context} if self.context else {}),
        }


class StrictJsonError(ContractError):
    """One input JSONL line could not be turned into a typed event."""


class FramingError(ContractError):
    """The MDP1 byte stream is not well-formed at this offset."""

    def __init__(
        self, reason: Reason, detail: str = "", *, offset: int | None = None, **context: object
    ) -> None:
        super().__init__(reason, detail, **context)
        self.offset = offset

    def as_dict(self) -> dict[str, object]:
        out = super().as_dict()
        if self.offset is not None:
            out["offset"] = self.offset
        return out


class CodecError(ContractError):
    """Protobuf serialise/parse failed, or the payload broke a byte budget."""


class EnvelopeError(ContractError):
    """The message parsed, but is not a well-formed ``marketdata.v1`` event."""


class PolicyError(ContractError):
    """The event is well-formed but its values are not acceptable in this mode."""


class ConfigError(ContractError):
    def __init__(self, detail: str, **context: object) -> None:
        super().__init__(Reason.CONFIG_INVALID, detail, **context)


class SourceArtifactError(ContractError):
    """The Project 01 dataset is not a verified, completed source."""

    def __init__(self, detail: str, **context: object) -> None:
        super().__init__(Reason.SOURCE_NOT_COMPLETED, detail, **context)
