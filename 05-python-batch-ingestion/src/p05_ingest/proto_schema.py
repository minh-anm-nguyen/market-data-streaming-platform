"""Pinned access to the `marketdata.v1` schema and raw-value decoding.

Two responsibilities:

  * the **descriptor pin**. The run refuses to start unless the descriptor the
    running parser actually uses hashes to the value in
    ``[contracts].protobuf_descriptor_sha256``. Pinning the generated
    descriptor rather than the ``.proto`` text means a regenerated-but-changed
    schema cannot slip in behind identical-looking source.
  * **decoding a raw Kafka value**. The value is bare protobuf: spec section
    3.1 is explicit that there is no ``MDP1`` prefix and no 4-byte framing
    length, so nothing here may strip leading bytes.

Observed runtime behaviour this module relies on (protobuf 7.x, verified):

  * truncated input, an over-long length delimiter and invalid UTF-8 in a
    string field all raise ``DecodeError``;
  * a *wrong wire type* for a known field number does NOT raise. The field is
    moved to the unknown-field set and reads as absent. That is a silent
    corruption channel, so it is caught twice: by the presence checks in
    ``contract_validator`` and by ``unknown_field_numbers`` here.
"""

from __future__ import annotations

import hashlib

from google.protobuf import unknown_fields as _unknown_fields
from google.protobuf.message import DecodeError

from .errors import ContractViolation, PreflightError
from .generated import market_data_pb2 as _pb

MarketDataEvent = _pb.MarketDataEvent
Quote = _pb.Quote
Tick = _pb.Tick

#: The oneof that selects the payload variant. Name is part of the contract.
PAYLOAD_ONEOF = "payload"

_FILE_DESCRIPTOR = MarketDataEvent.DESCRIPTOR.file


def descriptor_sha256() -> str:
    """SHA-256 of the serialized ``FileDescriptorProto`` in use.

    These are the exact bytes ``protoc`` embedded in the generated module, so
    the hash pins the schema *as the running parser sees it* -- not the
    ``.proto`` text, and not a separately generated descriptor set that could
    drift from the code.
    """
    return hashlib.sha256(_FILE_DESCRIPTOR.serialized_pb).hexdigest()


def schema_package() -> str:
    return _FILE_DESCRIPTOR.package


def verify_descriptor_pin(expected_sha256: str, *, expected_package: str) -> None:
    """Fail-closed schema gate, called from preflight.

    Raised as ``PreflightError`` rather than logged: a schema that is not the
    pinned one makes every digest in the run unverifiable.
    """
    actual_package = schema_package()
    if actual_package != expected_package:
        raise PreflightError(
            f"protobuf package mismatch: config expects {expected_package!r}, "
            f"compiled schema provides {actual_package!r}"
        )
    actual = descriptor_sha256()
    if actual != expected_sha256.lower():
        raise PreflightError(
            "protobuf descriptor hash mismatch -- the compiled schema is not "
            f"the pinned one.\n  expected: {expected_sha256.lower()}\n"
            f"  actual:   {actual}\n"
            "Regenerate from the P02 artifact and re-pin, or restore the "
            "pinned schema. Do not relax this check to make a run proceed."
        )


def decode_value(raw: bytes) -> MarketDataEvent:
    """Parse a raw Kafka value into a message, or raise ``ContractViolation``.

    Byte caps are *not* applied here -- the caller enforces them before this
    function is reached, so that a hostile 10 MiB value is never handed to the
    parser at all (spec NFR-03).
    """
    event = MarketDataEvent()
    try:
        event.ParseFromString(raw)
    except DecodeError as exc:
        raise ContractViolation("PROTO_DECODE_ERROR", str(exc)) from exc
    except (ValueError, TypeError) as exc:
        # Defensive: some runtimes surface malformed varints as ValueError.
        raise ContractViolation("PROTO_DECODE_ERROR", str(exc)) from exc
    return event


def unknown_field_numbers(event: MarketDataEvent) -> tuple[int, ...]:
    """Field numbers the pinned schema does not know about.

    Non-empty means one of: the producer runs a newer schema, the value was
    corrupted, or a known field arrived with the wrong wire type. The spec
    forbids inferring a new message type from unknown fields, so the caller
    treats this as a compatibility gate rather than data to keep.
    """
    try:
        unknown = _unknown_fields.UnknownFieldSet(event)
    except Exception:  # pragma: no cover - runtime without unknown-field support
        return ()
    return tuple(sorted({f.field_number for f in unknown}))
