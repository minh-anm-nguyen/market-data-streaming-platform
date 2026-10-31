"""P05's ``payload_digest`` encoding, as published in its spec section 3.2.

This module exists because ``md_ingest.market_events_raw.payload_digest`` is
**P05's** column. P05 fills it; P06 only reads it. Verifying a stored digest
therefore means recomputing it the way its writer did, and P05-CORE-V1 is a
length-prefixed binary encoding under the domain separator ``P05-CORE-V1\\0``
-- not :func:`md_analytics.contracts.payload_digest`, which hashes canonical
JSON and governs P06's *own* canonical layer.

Both projects had a function called ``payload_digest``, each with its own
encoding, each with a green test suite, and nothing compared them until P06
read P05's real table: every one of 85,520 rows mismatched and the audit
reported 2,000 missing identities for a dataset that was fully present
(pipeline run 20261031T093335Z). The digests do not disagree about the data.
They disagree about what a digest is.

Why re-implement rather than import ``p05_ingest.canonical_digest``: a byte
format is a published contract, and P06 consuming P05's table is exactly the
case that contract exists for. Importing the writer's code would make P06
unrunnable without P05 installed and would still not prove the two agree --
it would only guarantee they fail together. Instead the format is restated
here against :data:`VERIFIED_VECTOR`, a row read out of a live ClickHouse
table with its digest as P05 actually wrote it, and ``p09 audit`` asserts that
P05's implementation and this one still agree on that vector. Drift then fails
a gate instead of a deployment.

Nothing here normalises or repairs input: the core it encodes has already been
validated by :func:`md_analytics.contracts.semantic_core`, and a field that
function omits is absent in P05's encoding too, which is what the presence
flags are for.
"""

from __future__ import annotations

import hashlib
import struct
from typing import Final

from .errors import ContractError

#: Identifier used wherever an encoding has to be named rather than assumed.
ENCODING: Final = "P05-CORE-V1"

#: Changing these bytes invalidates every stored digest, which is why P05
#: versions them: a new layout becomes P05-CORE-V2 and both must be supported
#: for the length of a migration.
DOMAIN_SEPARATOR: Final = b"P05-CORE-V1\x00"

#: ``1`` and ``2`` are P05's wire values and are not derived from anything --
#: not an enum ordinal, not a sorted position. They are written out so that
#: reordering ``EVENT_KINDS`` upstream cannot silently renumber them.
_KIND_BYTE: Final = {"tick": 1, "quote": 2}

#: Order is part of the contract and must never be sorted or regenerated.
_OPTIONAL_FIELD_ORDER: Final = ("bid_ticks", "ask_ticks", "price_ticks")

_INT64_MIN: Final = -(2**63)
_INT64_MAX: Final = 2**63 - 1

_U32 = struct.Struct(">I")
_I64 = struct.Struct(">q")
_U8 = struct.Struct(">B")


def _string(value: object, field: str) -> bytes:
    """Length-prefixed UTF-8, where the length counts bytes, not code points."""
    if not isinstance(value, str):
        raise ContractError(
            "%s must be str for %s, got %s" % (field, ENCODING, type(value).__name__),
            code="P05_CORE_FIELD_TYPE",
            field=field,
        )
    raw = value.encode("utf-8")
    return _U32.pack(len(raw)) + raw


def _int64(value: object, field: str) -> bytes:
    """Signed 64-bit big-endian, range-checked.

    Python integers are unbounded, so a value that would wrap in another
    runtime has to be refused here rather than encoded as something else.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(
            "%s must be int for %s, got %s" % (field, ENCODING, type(value).__name__),
            code="P05_CORE_FIELD_TYPE",
            field=field,
        )
    if not _INT64_MIN <= value <= _INT64_MAX:
        raise ContractError(
            "%s does not fit in int64: %d" % (field, value),
            code="P05_CORE_FIELD_RANGE",
            field=field,
        )
    return _I64.pack(value)


def canonical_bytes(core: dict[str, object]) -> bytes:
    """Encode a validated semantic core in P05's byte layout.

    ``core`` is the output of :func:`md_analytics.contracts.semantic_core`,
    which omits the fields a kind does not carry -- a tick has no ``bid_ticks``
    key at all. P05 writes a presence flag for each optional field in a fixed
    order, so an omitted key and an explicit ``None`` encode identically, and
    neither can be confused with a zero.
    """
    kind = core.get("event_kind")
    if kind not in _KIND_BYTE:
        raise ContractError(
            "cannot encode event_kind %r in %s" % (kind, ENCODING),
            code="P05_CORE_UNKNOWN_KIND",
            value=kind,
        )

    out = [
        DOMAIN_SEPARATOR,
        _string(core.get("dataset_id"), "dataset_id"),
        _string(core.get("symbol"), "symbol"),
        _int64(core.get("sequence"), "sequence"),
        _int64(core.get("event_time_ns"), "event_time_ns"),
        _int64(core.get("price_scale"), "price_scale"),
        _string(core.get("source_schema_version"), "source_schema_version"),
        _U8.pack(_KIND_BYTE[kind]),
    ]
    for field in _OPTIONAL_FIELD_ORDER:
        value = core.get(field)
        if value is None:
            out.append(_U8.pack(0))
        else:
            out.append(_U8.pack(1))
            out.append(_int64(value, field))
    return b"".join(out)


def payload_digest(core: dict[str, object]) -> str:
    """Lower-case hex SHA-256 of :func:`canonical_bytes`. Always 64 characters."""
    return hashlib.sha256(canonical_bytes(core)).hexdigest()


#: A row read from a live ``md_ingest.market_events_raw`` together with the
#: digest P05 stored for it. This is evidence, not a fixture: it was taken from
#: ClickHouse over HTTP while the P09 DEV deployment was up, and it is the
#: reason this module is believed to match P05 at all. Any change to the layout
#: above that leaves this digest intact has not been tested.
VERIFIED_VECTOR: Final = (
    {
        "dataset_id": "ds-556f96b184c834c2",
        "symbol": "EURUSD",
        "sequence": 2,
        "event_kind": "quote",
        "event_time_ns": 1793322000002000000,
        "price_scale": 100000,
        "source_schema_version": "sim.v1",
        "bid_ticks": 108342,
        "ask_ticks": 108347,
    },
    "767881c6efcf1849710f772d57b281c9e2b794b64d856a2353039e8c81582bf4",
)
