"""The ``P05-CORE-V1`` canonical encoding, implemented twice on purpose.

Design 3.1 specifies the encoding exactly: literal prefix ``P05-CORE-V1\\0``,
strings as ``uint32`` big-endian length followed by UTF-8 bytes, signed integers
as ``int64`` big-endian, the event kind as one ``uint8``, and one presence byte
per optional field followed by its fixed-point value when present.

Two things this module refuses to do, both of them stated in the design:

* **It does not hash Protobuf bytes.** Serialized Protobuf is not a canonical
  form -- field order, unknown-field retention and runtime differences all
  change the bytes without changing the meaning (cases ``P07-G03-04``,
  ``P07-G02-03``). The digest is computed from the decoded semantic core.
* **It does not trust a digest handed over by the component under test.** P07
  recomputes from its own decode. :func:`digest_core` is that recomputation.

``reference_digest_core`` is a second, deliberately slower implementation
written from the spec text rather than from :func:`digest_core`. The unit tests
compare the two over generated cores, so a typo in the fast path cannot quietly
redefine identity -- the same trick P01 uses for its canonical JSON.
"""

from __future__ import annotations

import hashlib
import struct

from ..contracts import EventKind, SemanticCore

#: Version prefix. Changing the encoding means changing this string, because a
#: digest is only comparable to another digest of the same version.
DIGEST_VERSION: str = "P05-CORE-V1"

_PREFIX: bytes = DIGEST_VERSION.encode("ascii") + b"\x00"

#: Order matters and is part of the contract: bid, ask, price.
_OPTIONAL_FIELDS: tuple[str, ...] = ("bid_ticks", "ask_ticks", "price_ticks")

_U32 = struct.Struct(">I")
_I64 = struct.Struct(">q")

#: Signed 64-bit range. A value outside it cannot be encoded as ``int64_BE``,
#: and silently wrapping it would make two different events share a digest.
_I64_MIN = -(2**63)
_I64_MAX = 2**63 - 1


class DigestError(ValueError):
    """Raised when a core cannot be encoded, with a machine-readable reason."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


def _encode_str(value: object, field: str) -> bytes:
    if not isinstance(value, str):
        raise DigestError("field_not_string", field)
    raw = value.encode("utf-8")
    if len(raw) > 0xFFFFFFFF:  # pragma: no cover - unreachable in a 4 KiB record
        raise DigestError("string_too_long", field)
    return _U32.pack(len(raw)) + raw


def _encode_i64(value: object, field: str) -> bytes:
    # bool is an int subclass in Python, so ``True`` would otherwise encode as 1
    # and make a malformed record look well-formed.
    if not isinstance(value, int) or isinstance(value, bool):
        raise DigestError("field_not_integer", field)
    if not _I64_MIN <= value <= _I64_MAX:
        raise DigestError("integer_out_of_range", f"{field}={value}")
    return _I64.pack(value)


def encode_core(core: SemanticCore) -> bytes:
    """Canonical bytes for ``core``. The input to the SHA-256 below."""
    parts: list[bytes] = [
        _PREFIX,
        _encode_str(core.dataset_id, "dataset_id"),
        _encode_str(core.symbol, "symbol"),
        _encode_i64(core.sequence, "sequence"),
        _encode_i64(core.event_time_ns, "event_time_ns"),
        _encode_i64(core.price_scale, "price_scale"),
    ]
    if not isinstance(core.event_kind, EventKind):
        raise DigestError("field_not_event_kind", repr(core.event_kind))
    parts.append(bytes((int(core.event_kind),)))
    parts.append(_encode_str(core.source_schema_version, "source_schema_version"))
    for field in _OPTIONAL_FIELDS:
        value = getattr(core, field)
        if value is None:
            parts.append(b"\x00")
        else:
            parts.append(b"\x01")
            parts.append(_encode_i64(value, field))
    return b"".join(parts)


def digest_core(core: SemanticCore) -> str:
    """Lowercase hex SHA-256 of the canonical encoding of ``core``."""
    return hashlib.sha256(encode_core(core)).hexdigest()


def reference_encode_core(core: SemanticCore) -> bytes:
    """Independent re-implementation of :func:`encode_core`, from the spec.

    Written against the prose in design 3.1 rather than against the function
    above, and built with ``bytearray`` appends instead of a joined list, so the
    two paths share no helper. ``tests/unit/test_semantic_digest.py`` asserts
    byte equality across every event shape including the negative and oversized
    values the P01 fault plan produces.
    """
    out = bytearray()
    out += b"P05-CORE-V1"
    out += b"\x00"
    for text in (core.dataset_id, core.symbol):
        if not isinstance(text, str):
            raise DigestError("field_not_string", "dataset_id/symbol")
        blob = text.encode("utf-8")
        out += len(blob).to_bytes(4, "big", signed=False)
        out += blob
    for number in (core.sequence, core.event_time_ns, core.price_scale):
        if not isinstance(number, int) or isinstance(number, bool):
            raise DigestError("field_not_integer", "sequence/event_time_ns/price_scale")
        if not _I64_MIN <= number <= _I64_MAX:
            raise DigestError("integer_out_of_range", str(number))
        out += number.to_bytes(8, "big", signed=True)
    if not isinstance(core.event_kind, EventKind):
        raise DigestError("field_not_event_kind", repr(core.event_kind))
    out += int(core.event_kind).to_bytes(1, "big", signed=False)
    version = core.source_schema_version
    if not isinstance(version, str):
        raise DigestError("field_not_string", "source_schema_version")
    version_blob = version.encode("utf-8")
    out += len(version_blob).to_bytes(4, "big", signed=False)
    out += version_blob
    for value in (core.bid_ticks, core.ask_ticks, core.price_ticks):
        if value is None:
            out += b"\x00"
            continue
        if not isinstance(value, int) or isinstance(value, bool):
            raise DigestError("field_not_integer", "optional tick field")
        if not _I64_MIN <= value <= _I64_MAX:
            raise DigestError("integer_out_of_range", str(value))
        out += b"\x01"
        out += value.to_bytes(8, "big", signed=True)
    return bytes(out)


def reference_digest_core(core: SemanticCore) -> str:
    return hashlib.sha256(reference_encode_core(core)).hexdigest()
