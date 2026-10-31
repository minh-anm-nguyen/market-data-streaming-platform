"""Typed errors and the verdict vocabulary of section 14.2.

Every failure path in this package raises one of these rather than returning a
sentinel, because the one outcome the design forbids is a missing proof that
reads as success. ``ContractError`` and its siblings all carry a machine-readable
``code`` so evidence exports and tests can assert *why* a build stopped, not
just that it did.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class Verdict(StrEnum):
    """Section 14.2. ``PASS`` is the only value that may be reported as success.

    ``NO_GO`` is not a bug: for a stimulus whose correct outcome is refusing to
    publish, ``NO_GO`` from the system under test is the behaviour a test asserts
    (and the test itself then reports ``PASS``).
    """

    PASS = "PASS"
    FAIL = "FAIL"
    NO_GO = "NO_GO"
    INCONCLUSIVE = "INCONCLUSIVE"
    INVALID = "INVALID"
    NOT_RUN = "NOT_RUN"


class EvidenceClass(StrEnum):
    """What a result is evidence *of*.

    The design (§4 note, §22) is explicit that SQL semantics may change between
    ClickHouse versions, so a result produced against the in-process model of
    those semantics cannot support a claim about ClickHouse itself. Keeping the
    distinction in the evidence record -- rather than in a comment -- is what
    stops a model run from being quoted as a server run later.
    """

    MODEL = "MODEL"
    REAL_CLICKHOUSE = "REAL_CLICKHOUSE"


class MdAnalyticsError(Exception):
    """Base class. ``code`` is stable; the message is for humans."""

    code = "MD_ANALYTICS_ERROR"

    def __init__(self, message: str, *, code: str | None = None, **context: Any) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code
        self.context: dict[str, Any] = context

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), **self.context}


class ConfigError(MdAnalyticsError):
    code = "CONFIG_ERROR"


class ContractError(MdAnalyticsError):
    """A row, timestamp or price violates the inherited P01-P05 contract."""

    code = "CONTRACT_ERROR"


class SchemaDriftError(MdAnalyticsError):
    """Observed DDL does not match the pinned schema hash (group B)."""

    code = "SCHEMA_DRIFT"


class SnapshotCutError(MdAnalyticsError):
    """The source cut is not sealable, so completeness cannot be proven (group F)."""

    code = "SNAPSHOT_CUT_NOT_SEALABLE"


class BuildDirtyError(MdAnalyticsError):
    """Staging holds rows of unknown provenance; only a fresh build may proceed."""

    code = "BUILD_DIRTY"


class PublishRejected(MdAnalyticsError):
    """The publish gate refused: proofs missing, stale pointer, or conflict."""

    code = "PUBLISH_REJECTED"


class QueryRejected(MdAnalyticsError):
    """Query violates scope, alignment, limit or build-readiness rules (group R)."""

    code = "QUERY_REJECTED"


class StoreError(MdAnalyticsError):
    code = "STORE_ERROR"


class AckUnknownError(StoreError):
    """The write may or may not have landed. Never retried blindly (§5.2)."""

    code = "ACK_UNKNOWN"


class ResourceLimitExceeded(StoreError):
    """A bounded error: the correct response to resource exhaustion (NFR-07)."""

    code = "RESOURCE_LIMIT_EXCEEDED"
