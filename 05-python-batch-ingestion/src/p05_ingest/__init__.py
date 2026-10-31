"""P05 -- Python Kafka batch ingestion: Kafka -> Protobuf -> ClickHouse.

Delivery semantics, stated once here so no caller has to guess:

    Kafka to ClickHouse is **conditionally at-least-once**. Physical rows may
    be duplicated. Logical idempotency is defined at the query and
    reconciliation layer by the event identity ``(dataset_id, symbol,
    sequence)`` together with the ``P05-CORE-V1`` payload digest. ClickHouse
    block de-duplication is a finite optimisation, not a proof of exactly-once.

The module layout follows spec section 2.2, and the ownership rule is absolute:
only the owner thread touches the Kafka consumer, and only
``commit_coordinator`` decides what may be committed.
"""

from __future__ import annotations

__version__ = "0.1.0"

#: The canonical encoding version stored alongside every digest.
CANONICAL_ENCODING_VERSION = "P05-CORE-V1"

#: Status of this build, in the spec's own vocabulary. Updated by evidence,
#: never by optimism.
BUILD_STATUS = (
    "IMPLEMENTED; unit/property/simulated-integration/chaos tests EXECUTED; "
    "live Kafka + ClickHouse integration NOT EXECUTED (no broker or server "
    "available in this environment); SLO/capacity NOT VERIFIED"
)

__all__ = ["BUILD_STATUS", "CANONICAL_ENCODING_VERSION", "__version__"]
