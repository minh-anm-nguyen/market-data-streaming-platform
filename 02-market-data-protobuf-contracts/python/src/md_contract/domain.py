"""The typed domain event and ``core_projection_v1`` -- the semantic oracle.

Why a projection instead of comparing bytes (ADR-005): a Protobuf message has
more than one valid binary encoding, and Protobuf explicitly does not promise
canonical output, so two byte strings that differ can mean exactly the same
event. Comparing ``SHA-256(bytes)`` across C++ and Python would therefore fail
on correct implementations and -- worse -- could pass on two implementations
that share a bug. The oracle is instead a *typed* projection: every v1 field,
its presence flag, and the payload case, rendered as canonical JSON.

Absent fields are **omitted** from the projection rather than written as null,
and the sorted ``present`` list states which fields existed. That makes
"absent", "present and zero" and "present and non-zero" three distinguishable
outcomes in the oracle itself (cases B01-B05), which is the whole reason the
schema uses explicit ``optional``.

The C++ side (``cpp/src/projection.cpp``) produces byte-identical output from
the same event, so the two languages can be compared with ``==`` on lines.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Final, NamedTuple

#: The input JSONL contract this project maps from (Project 01).
SOURCE_SCHEMA_VERSION: Final = "sim.v1"

#: The Protobuf contract. Distinct from the above on purpose: one describes the
#: input format, the other the wire contract (section 5.1).
PROTO_PACKAGE: Final = "marketdata.v1"

PAYLOAD_QUOTE: Final = "quote"
PAYLOAD_TICK: Final = "tick"
PAYLOAD_NONE: Final = "none"
#: A payload case a v1 reader parsed but cannot interpret -- a v2 writer's new
#: oneof variant arriving at a v1 reader (cases A11, D20, G06).
PAYLOAD_UNKNOWN: Final = "unknown"

INT64_MIN: Final = -(2**63)
INT64_MAX: Final = 2**63 - 1

#: Scalar fields of ``MarketDataEvent`` that every event must carry, in tag order.
ENVELOPE_FIELDS: Final = (
    "dataset_id",
    "symbol",
    "sequence",
    "event_time_ns",
    "price_scale",
    "source_schema_version",
)

QUOTE_PRICE_FIELDS: Final = ("bid_ticks", "ask_ticks")
TICK_PRICE_FIELDS: Final = ("price_ticks",)


class DomainEvent(NamedTuple):
    """One decoded event with presence preserved.

    ``None`` means *the field was not present on the wire*; it never means zero.
    ``payload_case`` is taken from the oneof, which is the only source of truth
    for the event type (ADR-003) -- there is no second ``event_type`` string that
    could disagree with it.

    A ``NamedTuple`` keeps this immutable and cheap, which matters because the
    same event is handed to the encoder, the validator and the oracle. It also
    makes accidental mutation of a shared event impossible, which is the failure
    case E12/E13 guards against on the Protobuf-object side.
    """

    dataset_id: str | None = None
    symbol: str | None = None
    sequence: int | None = None
    event_time_ns: int | None = None
    price_scale: int | None = None
    source_schema_version: str | None = None
    payload_case: str = PAYLOAD_NONE
    bid_ticks: int | None = None
    ask_ticks: int | None = None
    price_ticks: int | None = None

    @property
    def event_id(self) -> tuple[str | None, str | None, int | None]:
        """Logical identity: ``(dataset_id, symbol, sequence)``.

        Deliberately *not* a hash of the serialised bytes (section 3.4): an
        intentional duplicate from Project 01 must keep the same id, and byte
        hashes are not stable across runtimes.
        """
        return (self.dataset_id, self.symbol, self.sequence)

    @property
    def spread_ticks(self) -> int | None:
        if self.bid_ticks is None or self.ask_ticks is None:
            return None
        return self.ask_ticks - self.bid_ticks

    def display_price(self) -> dict[str, Decimal]:
        """Human-readable decimals, via ``Decimal`` so no float ever touches a price."""
        if self.price_scale is None or self.price_scale == 0:
            return {}
        scale = Decimal(self.price_scale)
        out: dict[str, Decimal] = {}
        for name, value in (
            ("bid", self.bid_ticks),
            ("ask", self.ask_ticks),
            ("price", self.price_ticks),
        ):
            if value is not None:
                out[name] = Decimal(value) / scale
        return out

    def present_fields(self) -> tuple[str, ...]:
        """Sorted names of the fields that were present on the wire."""
        names = [name for name in ENVELOPE_FIELDS if getattr(self, name) is not None]
        names += [
            name
            for name in QUOTE_PRICE_FIELDS + TICK_PRICE_FIELDS
            if getattr(self, name) is not None
        ]
        return tuple(sorted(names))


def core_projection_v1(event: DomainEvent) -> dict[str, object]:
    """The v1 semantic projection as a plain dict.

    Only v1 fields appear here. A v1.1 writer's extra ``feed_label`` is
    deliberately **not** part of it: the compatibility matrix asserts that adding
    a field leaves this projection unchanged (A03, G01), and checks unknown-field
    retention as a *separate* property, because core equality and lossless
    forwarding are different guarantees (section 5.5).
    """
    out: dict[str, object] = {
        "payload_case": event.payload_case,
        "present": list(event.present_fields()),
    }
    for name in ENVELOPE_FIELDS:
        value = getattr(event, name)
        if value is not None:
            out[name] = value
    for name in QUOTE_PRICE_FIELDS + TICK_PRICE_FIELDS:
        value = getattr(event, name)
        if value is not None:
            out[name] = value
    return out


def projection_json(event: DomainEvent) -> str:
    """Canonical one-line JSON of the projection; the cross-language oracle.

    ``sort_keys`` plus the tight separators make it byte-stable, and
    ``allow_nan=False`` is belt-and-braces: every number here is an ``int``, so
    a float could only appear through a bug, and this turns that bug into an
    exception instead of a ``NaN`` in a report.
    """
    return json.dumps(
        core_projection_v1(event),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def projection_line(event: DomainEvent) -> bytes:
    return projection_json(event).encode("utf-8") + b"\n"


def parse_projection_line(line: str | bytes) -> dict[str, object]:
    """Read back a projection line, for comparing against the C++ output."""
    text = line.decode("utf-8") if isinstance(line, bytes) else line
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise ValueError(f"projection line is not an object: {text[:80]!r}")
    return obj
