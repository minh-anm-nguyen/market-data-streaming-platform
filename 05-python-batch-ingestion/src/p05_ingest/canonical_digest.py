"""P05-CORE-V1 canonical encoding and semantic payload digest.

Implements the byte layout frozen in the design spec section 3.2::

    ASCII "P05-CORE-V1\\0"                   domain separator
    str(dataset_id)             uint32_BE(len) + exact UTF-8 bytes
    str(symbol)                 uint32_BE(len) + exact uppercase ASCII bytes
    int64_BE(sequence)                        signed, two's complement
    int64_BE(event_time_ns)
    int64_BE(price_scale)
    str(source_schema_version)  uint32_BE(len) + UTF-8 bytes
    uint8(kind)                               1 = tick, 2 = quote
    for f in [bid_ticks, ask_ticks, price_ticks]:
        uint8(1 if present else 0)
        int64_BE(value) if present

    payload_digest = lower_hex(SHA256(canonical_bytes))   -> 64 ASCII chars

Why hand-rolled rather than ``SerializeToString()``:

  * protobuf serialization is explicitly NOT canonical across runtimes and
    versions -- field order, unknown-field retention and packing may differ.
    The spec forbids using it as the byte oracle.
  * ``hash()`` is randomized per process by PYTHONHASHSEED and must never
    produce a stored identifier.

What is deliberately excluded: Kafka topic/partition/offset, ``ingested_at``,
run identifiers, headers, ClickHouse query settings and unknown protobuf
fields. A replayed event therefore digests identically to its original, which
is what makes logical de-duplication possible at all.
"""

from __future__ import annotations

import hashlib
import struct

from .errors import InvariantViolation
from .model import INT64_MAX, INT64_MIN, EventKind, LogicalProjection

#: Domain separator. Changing this invalidates every stored digest, so it is
#: versioned: a new encoding becomes P05-CORE-V2 and both must then be
#: supported for the duration of a migration.
DOMAIN_SEPARATOR = b"P05-CORE-V1\x00"

CANONICAL_ENCODING_VERSION = "P05-CORE-V1"

#: Order is part of the contract and must never be sorted or regenerated.
_OPTIONAL_FIELD_ORDER = ("bid_ticks", "ask_ticks", "price_ticks")

_U32 = struct.Struct(">I")
_I64 = struct.Struct(">q")
_U8 = struct.Struct(">B")


def _encode_string(value: str, *, field_name: str) -> bytes:
    """Length-prefixed UTF-8. The length counts *bytes*, not code points."""
    if not isinstance(value, str):
        raise InvariantViolation(
            f"{field_name} must be str for canonical encoding, got "
            f"{type(value).__name__}"
        )
    raw = value.encode("utf-8")
    if len(raw) > 0xFFFFFFFF:  # pragma: no cover - unreachable under byte caps
        raise InvariantViolation(f"{field_name} too long to length-prefix")
    return _U32.pack(len(raw)) + raw


def _encode_int64(value: int, *, field_name: str) -> bytes:
    """Signed 64-bit big-endian, with an explicit range check.

    Python ints are unbounded, so a value that would silently wrap in another
    language must be rejected here rather than encoded as something else.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvariantViolation(
            f"{field_name} must be int for canonical encoding, got "
            f"{type(value).__name__}"
        )
    if not INT64_MIN <= value <= INT64_MAX:
        raise InvariantViolation(f"{field_name}={value} outside int64 range")
    return _I64.pack(value)


def canonical_bytes(projection: LogicalProjection) -> bytes:
    """Serialize a projection to its canonical byte form.

    Deterministic across processes, machines and Python versions: no dict
    iteration, no locale, no float, no pickling.
    """
    kind = projection.kind
    if not isinstance(kind, EventKind):
        # Accept the raw int value so a decoder reading a stored row can
        # reproduce the digest without importing the enum.
        kind = EventKind(kind)

    out = bytearray(DOMAIN_SEPARATOR)
    out += _encode_string(projection.dataset_id, field_name="dataset_id")
    out += _encode_string(projection.symbol, field_name="symbol")
    out += _encode_int64(projection.sequence, field_name="sequence")
    out += _encode_int64(projection.event_time_ns, field_name="event_time_ns")
    out += _encode_int64(projection.price_scale, field_name="price_scale")
    out += _encode_string(
        projection.source_schema_version, field_name="source_schema_version"
    )
    out += _U8.pack(int(kind))

    for field_name in _OPTIONAL_FIELD_ORDER:
        value = getattr(projection, field_name)
        if value is None:
            out += _U8.pack(0)
        else:
            out += _U8.pack(1)
            out += _encode_int64(value, field_name=field_name)

    return bytes(out)


def payload_digest(projection: LogicalProjection) -> str:
    """Lower-case hex SHA-256 of the canonical bytes. Always 64 chars."""
    return hashlib.sha256(canonical_bytes(projection)).hexdigest()


def batch_insert_token(
    *,
    topic_generation: str,
    topic: str,
    partition: int,
    coordinates_and_digests: tuple[tuple[int, str], ...],
    table_fqn: str,
    schema_version: str,
    insert_mode: str,
) -> str:
    """Stable identity for one immutable insert request (spec section 5.1).

    Doubles as the ClickHouse ``insert_deduplication_token``, so it must depend
    on the *content* as well as the offsets: a batch covering the same offset
    range but carrying different payloads (possible after an upstream rewrite)
    must not collide with the earlier one and be silently discarded by
    ClickHouse block de-duplication.

    It also includes the destination table and the insert mode, because the
    same rows written to a different table or under different settings is a
    different request whose de-duplication must not be shared.
    """
    h = hashlib.sha256()
    h.update(b"P05-BATCH-TOKEN-V1\x00")
    for part in (topic_generation, topic, table_fqn, schema_version, insert_mode):
        h.update(_encode_string(part, field_name="token_part"))
    h.update(_I64.pack(partition))
    h.update(_U32.pack(len(coordinates_and_digests)))
    for offset, digest in coordinates_and_digests:
        h.update(_I64.pack(offset))
        # Digests are fixed-width lower hex, so they need no length prefix, but
        # validate rather than trust.
        raw = digest.encode("ascii")
        if len(raw) != 64:
            raise InvariantViolation(
                f"payload_digest must be 64 hex chars, got {len(raw)}"
            )
        h.update(raw)
    return h.hexdigest()


def batch_id(*, topic: str, partition: int, insert_token: str) -> str:
    """Short human-usable batch label; the token remains the real identity."""
    return f"{topic}:{partition}:{insert_token[:16]}"
