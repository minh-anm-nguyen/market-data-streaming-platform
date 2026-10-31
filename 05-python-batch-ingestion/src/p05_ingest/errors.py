"""Exception hierarchy for P05.

The taxonomy mirrors the retry-classification table in the P05 design spec
section 5.2. The class of a failure decides two independent questions:

  * may the same immutable batch be re-sent?            -> ``retryable``
  * may the Kafka offset frontier advance past it?      -> never, except via a
    terminal durable result (sink ACK or durable-quarantine ACK)

Nothing in this module is allowed to imply that an ambiguous outcome means
"not written". ``AmbiguousSinkOutcome`` exists precisely because the client
cannot know.
"""

from __future__ import annotations


class P05Error(Exception):
    """Root of every error raised by this package."""


# --------------------------------------------------------------------------
# Configuration / startup. These are fail-closed: the process must not start.
# --------------------------------------------------------------------------


class ConfigError(P05Error):
    """Malformed, missing or internally inconsistent configuration."""


class PreflightError(P05Error):
    """A startup gate refused to let the run begin.

    Raised for unsafe-but-syntactically-valid configuration (spec I-11, NFR-09)
    and for environment mismatches such as an unpinned descriptor hash.
    """


# --------------------------------------------------------------------------
# Input contract violations (spec section 3.1).
# --------------------------------------------------------------------------


class ContractViolation(P05Error):
    """A Kafka record violated the P02/P04 input contract.

    Carries a stable ``code`` so that fixtures and the what-if matrix can assert
    on *why* a record was refused, not merely that it was.
    """

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


# --------------------------------------------------------------------------
# Sink outcomes. Only ACKED is a terminal success.
# --------------------------------------------------------------------------


class SinkError(P05Error):
    """Base for ClickHouse sink failures."""

    retryable = False
    #: True when the write may already have been applied server-side.
    ambiguous = False


class TransientSinkError(SinkError):
    """Definitive transient failure: the server refused before writing.

    Examples: HTTP 429/503, connect refused, temporary DNS failure.
    """

    retryable = True


class AmbiguousSinkOutcome(SinkError):
    """The outcome of the insert is unknown and may be a success.

    Examples: read timeout after the request was fully sent, TCP reset after
    send, truncated 200 body. Spec section 7: a timeout does NOT mean the row
    was not written. Retry is permitted only with the identical immutable
    batch, and the offset frontier must not advance until a definitive result
    arrives.
    """

    retryable = True
    ambiguous = True


class PermanentSinkError(SinkError):
    """The request can never succeed as formed.

    Examples: unknown column, type mismatch, permission denied. Blind retry is
    forbidden; the partition stops and the run is INCOMPLETE.
    """


# --------------------------------------------------------------------------
# Kafka-side faults.
# --------------------------------------------------------------------------


class OwnershipLostError(P05Error):
    """The partition was revoked or lost before an action could complete.

    A stale owner must never commit (spec I-02). Note that this fences the
    *commit*, never the ClickHouse write that may already be in flight.
    """


class RetentionLostError(P05Error):
    """The offset the group needs has fallen off the log.

    Unrecoverable from Kafka alone. Global NO-GO; recovery requires a verified
    upstream replay source (spec section 5.2 ``RETENTION_LOST``).
    """


class CommitNotConfirmedError(P05Error):
    """An explicit offset commit did not come back confirmed.

    The committed position is UNKNOWN, not "failed": it must be re-read from
    the broker rather than assumed (spec section 5.3).
    """


# --------------------------------------------------------------------------
# Invariant breaches. These indicate a bug in this program, not bad input.
# --------------------------------------------------------------------------


class InvariantViolation(P05Error):
    """An internal correctness invariant from spec section 4.2 was broken.

    Raised eagerly and never caught for control flow: a commit ahead of the
    proven sink frontier is a P0 data-loss bug and must abort the run rather
    than be repaired at runtime.
    """


class BudgetExceeded(P05Error):
    """A memory/byte budget reservation was refused (spec I-05)."""
