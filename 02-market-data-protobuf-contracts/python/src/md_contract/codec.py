"""Protobuf serialise/parse, and the envelope check that follows a parse.

The ordering here is the point. A successful ``ParseFromString`` means the bytes
were *syntactically* a ``marketdata.v1.MarketDataEvent`` -- nothing more. It does
not mean the event has a payload, does not mean the envelope scalars were
present, and does not mean the values are usable. Protobuf has no required
fields in proto3, so an empty byte string parses into a perfectly valid message
with every field absent. Treating "parsed" as "accepted" is exactly the false
success this project exists to prevent, so every parse here is followed by an
explicit presence check, and the two failures get different reasons.

Byte budgets are checked *before* the parser is handed anything (NFR-03): the
Protobuf runtime's own limits are an upper bound on what it will tolerate, not a
substitute for the application's 4096-byte rule.
"""

from __future__ import annotations

from typing import Final

from google.protobuf import unknown_fields
from google.protobuf.message import DecodeError

from .domain import (
    ENVELOPE_FIELDS,
    PAYLOAD_NONE,
    PAYLOAD_QUOTE,
    PAYLOAD_TICK,
    PAYLOAD_UNKNOWN,
    DomainEvent,
)
from .errors import CodecError, EnvelopeError, Reason
from .generated.v1.marketdata.v1 import market_data_pb2 as pb

#: Section 6.1. Applies to the serialised ``MarketDataEvent``, excluding framing.
MAX_PAYLOAD_BYTES: Final = 4096

#: Tags 10-19 are the payload oneof range per the schema contract. An unknown
#: field in this range is a payload variant from a newer writer, which must be
#: reported as an unsupported *kind* rather than as a missing payload.
PAYLOAD_TAG_RANGE: Final = range(10, 20)

MessageType = pb.MarketDataEvent


def domain_to_pb(event: DomainEvent) -> pb.MarketDataEvent:
    """Build a Protobuf message, setting only the fields that are present.

    ``None`` is skipped rather than written as zero, so an absent sequence stays
    absent on the wire. Setting it to 0 instead would make cases B01 and B02
    indistinguishable downstream.
    """
    msg = pb.MarketDataEvent()
    for name in ENVELOPE_FIELDS:
        value = getattr(event, name)
        if value is not None:
            setattr(msg, name, value)

    if event.payload_case == PAYLOAD_QUOTE:
        # SetInParent() is required, and is easy to get wrong. Merely *reading*
        # msg.quote in Python returns a lazily-attached submessage and does NOT
        # mark the oneof as set, so a quote with both prices absent would
        # serialise with no payload at all and come back as payload_case
        # "none". C++'s mutable_quote() sets the case on access, so the two
        # languages would disagree -- and the B07 fixture (a Quote{} present but
        # missing its prices) would be impossible to express in Python.
        # SetInParent() states the presence explicitly in both directions.
        quote = msg.quote
        quote.SetInParent()
        if event.bid_ticks is not None:
            quote.bid_ticks = event.bid_ticks
        if event.ask_ticks is not None:
            quote.ask_ticks = event.ask_ticks
    elif event.payload_case == PAYLOAD_TICK:
        tick = msg.tick
        tick.SetInParent()
        if event.price_ticks is not None:
            tick.price_ticks = event.price_ticks
    elif event.payload_case not in (PAYLOAD_NONE, PAYLOAD_UNKNOWN):
        raise CodecError(Reason.UNSUPPORTED_EVENT_KIND, event.payload_case)
    return msg


def pb_to_domain(msg: pb.MarketDataEvent) -> DomainEvent:
    """Read a parsed message into the domain type, preserving presence.

    Presence comes from ``HasField``, never from comparing against zero. The
    payload case comes from ``WhichOneof``; when that is ``None`` the unknown
    fields are inspected, because a v2 writer's new variant parses into a message
    whose oneof looks unset but which is *not* an event without a payload.
    """
    values: dict[str, object] = {}
    for name in ENVELOPE_FIELDS:
        if msg.HasField(name):
            values[name] = getattr(msg, name)

    case = msg.WhichOneof("payload")
    if case == PAYLOAD_QUOTE:
        quote = msg.quote
        return DomainEvent(
            **values,  # type: ignore[arg-type]
            payload_case=PAYLOAD_QUOTE,
            bid_ticks=quote.bid_ticks if quote.HasField("bid_ticks") else None,
            ask_ticks=quote.ask_ticks if quote.HasField("ask_ticks") else None,
        )
    if case == PAYLOAD_TICK:
        tick = msg.tick
        return DomainEvent(
            **values,  # type: ignore[arg-type]
            payload_case=PAYLOAD_TICK,
            price_ticks=tick.price_ticks if tick.HasField("price_ticks") else None,
        )

    payload_case = (
        PAYLOAD_UNKNOWN if _has_unknown_payload_variant(msg) else PAYLOAD_NONE
    )
    return DomainEvent(**values, payload_case=payload_case)  # type: ignore[arg-type]


def _has_unknown_payload_variant(msg: pb.MarketDataEvent) -> bool:
    """Whether an unknown field occupies the payload tag range 10-19."""
    return any(
        field.field_number in PAYLOAD_TAG_RANGE
        for field in unknown_fields.UnknownFieldSet(msg)
    )


def unknown_field_numbers(msg: pb.MarketDataEvent) -> tuple[int, ...]:
    """Tags this build did not recognise. Used by the forward-compat tests (G01-G04)."""
    return tuple(sorted(field.field_number for field in unknown_fields.UnknownFieldSet(msg)))


def encode_event(
    event: DomainEvent, *, max_payload_bytes: int = MAX_PAYLOAD_BYTES
) -> bytes:
    """Serialise one event, refusing to emit a payload over budget.

    The size check is after serialisation because the encoded length is not
    predictable from the field values alone; on the reading side the equivalent
    check happens *before* allocation, where it protects against a hostile
    advertised length (D08, D09).
    """
    payload = domain_to_pb(event).SerializeToString()
    if len(payload) > max_payload_bytes:
        raise CodecError(
            Reason.PAYLOAD_TOO_LARGE, f"{len(payload)} > {max_payload_bytes}"
        )
    if not payload:
        # An all-absent event serialises to zero bytes, and MDP1 forbids L=0.
        # Rejecting here keeps the framing layer from ever having to invent a
        # representation for "an event with nothing in it" (B06, D07).
        raise CodecError(Reason.UNSUPPORTED_EVENT_KIND, "event serialises to 0 bytes")
    return payload


def parse_payload(
    payload: bytes, *, max_payload_bytes: int = MAX_PAYLOAD_BYTES
) -> pb.MarketDataEvent:
    """Parse payload bytes into a message, or raise ``DECODE_ERROR``.

    A fresh message per call, never a reused-and-cleared one: a parse that fails
    part way leaves a partially populated message behind, and reusing that object
    is how fields from a previous event leak into the next one (cases E13, B12).
    The cost of the allocation is the price of not having that bug.
    """
    if len(payload) > max_payload_bytes:
        raise CodecError(Reason.PAYLOAD_TOO_LARGE, f"{len(payload)} > {max_payload_bytes}")
    msg = pb.MarketDataEvent()
    try:
        consumed = msg.ParseFromString(payload)
    except (DecodeError, Exception) as exc:  # noqa: BLE001 - see below
        # upb raises DecodeError, the pure-Python backend can raise ValueError or
        # IndexError on crafted varints, and the set differs between releases.
        # Anything thrown here is one thing to this codec -- undecodable bytes --
        # but it is converted to a typed rejection rather than swallowed, because
        # `except Exception: pass` around a parser is precisely how a benchmark
        # ends up reporting throughput for events it silently lost (section 10.3).
        if isinstance(exc, (DecodeError, ValueError, IndexError, TypeError)):
            raise CodecError(Reason.DECODE_ERROR, type(exc).__name__) from exc
        raise
    if consumed != len(payload):
        # ParseFromString consumes the whole buffer or fails, so this is a
        # defensive check against a runtime that one day returns short.
        raise CodecError(
            Reason.DECODE_ERROR, f"consumed {consumed} of {len(payload)} bytes"
        )
    return msg


def check_envelope(event: DomainEvent) -> None:
    """Enforce the structural contract on a parsed event (section 3.3, layer A).

    This is where "the bytes parsed" becomes "the event is well-formed". Absent
    core scalars are a rejection and are never defaulted to zero; a missing or
    unrecognised payload is a rejection and is never defaulted to a quote or a
    tick.
    """
    missing = [name for name in ENVELOPE_FIELDS if getattr(event, name) is None]
    if missing:
        raise EnvelopeError(
            Reason.MISSING_REQUIRED_SEMANTIC_FIELD, ",".join(missing)
        )

    if event.payload_case == PAYLOAD_UNKNOWN:
        raise EnvelopeError(
            Reason.UNSUPPORTED_EVENT_KIND,
            "payload variant in tag range 10-19 is not known to this build",
        )
    if event.payload_case == PAYLOAD_NONE:
        raise EnvelopeError(Reason.UNSUPPORTED_EVENT_KIND, "oneof payload not set")

    if event.payload_case == PAYLOAD_QUOTE:
        absent = [
            name
            for name in ("bid_ticks", "ask_ticks")
            if getattr(event, name) is None
        ]
        if absent:
            raise EnvelopeError(
                Reason.MISSING_REQUIRED_SEMANTIC_FIELD, f"quote.{','.join(absent)}"
            )
    elif event.payload_case == PAYLOAD_TICK:
        if event.price_ticks is None:
            raise EnvelopeError(Reason.MISSING_REQUIRED_SEMANTIC_FIELD, "tick.price_ticks")
    else:
        raise EnvelopeError(Reason.UNSUPPORTED_EVENT_KIND, event.payload_case)


def decode_payload(
    payload: bytes, *, max_payload_bytes: int = MAX_PAYLOAD_BYTES
) -> DomainEvent:
    """Parse and envelope-check in one step. Raises on either failure."""
    event = pb_to_domain(parse_payload(payload, max_payload_bytes=max_payload_bytes))
    check_envelope(event)
    return event
