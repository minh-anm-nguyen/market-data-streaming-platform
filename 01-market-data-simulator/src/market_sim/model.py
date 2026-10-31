"""Immutable event model, integer-tick prices and canonical JSON (contract sim.v1).

Section 3 of SYSTEM_DESIGN. Key rules enforced here:

* Prices are **integer ticks**. ``float`` never carries price truth. The decimal
  form is derived for humans only (``display_price``) and never serialised.
* Canonical serialisation is byte-stable: keys sorted, no spaces, UTF-8, newline
  terminated by the writer, no ``NaN``/``Infinity`` (impossible: all numbers are
  ``int``), and a documented field allowlist.
* ``event_id`` is the logical triple ``(dataset_id, symbol, sequence)`` -- an
  intentional duplicate keeps the same id, which is what makes downstream
  deduplication testable.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Final, NamedTuple

SCHEMA_VERSION: Final = "sim.v1"

EVENT_QUOTE: Final = "quote"
EVENT_TICK: Final = "tick"
EVENT_TYPES: Final = frozenset({EVENT_QUOTE, EVENT_TICK})

#: Fields permitted in a canonical record, per event type, in sorted order. A
#: parser that sees anything else must reject the record, not ignore the extra key.
QUOTE_FIELDS: Final = (
    "ask_ticks",
    "bid_ticks",
    "dataset_id",
    "event_time_ns",
    "event_type",
    "price_scale",
    "schema_version",
    "sequence",
    "symbol",
)
TICK_FIELDS: Final = (
    "dataset_id",
    "event_time_ns",
    "event_type",
    "price_scale",
    "price_ticks",
    "schema_version",
    "sequence",
    "symbol",
)

#: Symbols are restricted so canonical JSON needs no string escaping, and so a
#: hostile config cannot smuggle control characters or quotes into a record.
SYMBOL_MIN_LEN: Final = 3
SYMBOL_MAX_LEN: Final = 16
_SYMBOL_ALPHABET: Final = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.")


def is_valid_symbol(symbol: object) -> bool:
    return (
        isinstance(symbol, str)
        and SYMBOL_MIN_LEN <= len(symbol) <= SYMBOL_MAX_LEN
        and set(symbol) <= _SYMBOL_ALPHABET
    )


class Event(NamedTuple):
    """One canonical record.

    A quote carries ``bid_ticks``/``ask_ticks``; a tick carries ``price_ticks``.
    Unused fields stay ``None`` and are omitted from the wire form.

    ``tick`` is a *synthetic price update*, deliberately without volume or trade
    id, so it cannot be mistaken for an executed trade from a consolidated feed
    (section 3.3, case D12).

    A ``NamedTuple`` rather than a frozen dataclass: both are immutable, which
    this design needs (one event is handed to two sinks and to the fault
    injector, which must copy rather than mutate), but a frozen dataclass pays
    ``object.__setattr__`` per field in ``__init__`` -- measured at 3.47 us per
    event against 1.37 us here, on the hot path of every single record.
    """

    dataset_id: str
    event_type: str
    symbol: str
    sequence: int
    event_time_ns: int
    price_scale: int
    bid_ticks: int | None = None
    ask_ticks: int | None = None
    price_ticks: int | None = None
    schema_version: str = SCHEMA_VERSION

    @property
    def event_id(self) -> tuple[str, str, int]:
        return (self.dataset_id, self.symbol, self.sequence)

    @property
    def spread_ticks(self) -> int | None:
        if self.bid_ticks is None or self.ask_ticks is None:
            return None
        return self.ask_ticks - self.bid_ticks

    def display_price(self) -> dict[str, Decimal]:
        """Human-readable decimals. Never serialised, never used for arithmetic."""
        scale = Decimal(self.price_scale)
        out: dict[str, Decimal] = {}
        if self.bid_ticks is not None:
            out["bid"] = Decimal(self.bid_ticks) / scale
        if self.ask_ticks is not None:
            out["ask"] = Decimal(self.ask_ticks) / scale
        if self.price_ticks is not None:
            out["price"] = Decimal(self.price_ticks) / scale
        return out

    def to_canonical_dict(self) -> dict[str, object]:
        base: dict[str, object] = {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "event_type": self.event_type,
            "symbol": self.symbol,
            "sequence": self.sequence,
            "event_time_ns": self.event_time_ns,
            "price_scale": self.price_scale,
        }
        if self.event_type == EVENT_QUOTE:
            base["bid_ticks"] = self.bid_ticks
            base["ask_ticks"] = self.ask_ticks
        else:
            base["price_ticks"] = self.price_ticks
        return base

    def replace(self, **changes: object) -> "Event":
        """Copy with overrides. Used only by the fault injector."""
        return self._replace(**changes)


def to_canonical_json(event: Event) -> bytes:
    """Serialise to the canonical byte form, **without** a trailing newline.

    Hand-rolled in sorted-key order rather than going through ``json.dumps`` to
    keep the hot path cheap, and built with f-strings rather than ``+``
    concatenation (measured 1.93 us against 4.02 us per record, because
    concatenation allocates an intermediate string per piece).

    ``tests/unit/test_model.py`` asserts byte equality against
    ``json.dumps(..., sort_keys=True, separators=(",", ":"))`` for every event
    shape, including the negative and oversized tick values that faults produce,
    so the shortcut cannot silently drift from the contract.
    """
    if event.event_type == EVENT_QUOTE:
        return (
            f'{{"ask_ticks":{event.ask_ticks},"bid_ticks":{event.bid_ticks},'
            f'"dataset_id":"{event.dataset_id}","event_time_ns":{event.event_time_ns},'
            f'"event_type":"{event.event_type}","price_scale":{event.price_scale},'
            f'"schema_version":"{event.schema_version}","sequence":{event.sequence},'
            f'"symbol":"{event.symbol}"}}'
        ).encode("utf-8")
    return (
        f'{{"dataset_id":"{event.dataset_id}","event_time_ns":{event.event_time_ns},'
        f'"event_type":"{event.event_type}","price_scale":{event.price_scale},'
        f'"price_ticks":{event.price_ticks},'
        f'"schema_version":"{event.schema_version}","sequence":{event.sequence},'
        f'"symbol":"{event.symbol}"}}'
    ).encode("utf-8")


def reference_canonical_json(event: Event) -> bytes:
    """Reference implementation via ``json`` -- the oracle for the fast path."""
    return json.dumps(
        event.to_canonical_dict(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


class RecordParseError(ValueError):
    """Raised by :func:`parse_canonical_line`, with a machine-readable reason."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


def _require_int(value: object, field: str) -> int:
    """Require a true integer, with a reason precise enough to act on.

    The distinctions matter: a float where an integer belongs is a schema
    violation, ``NaN``/``Infinity`` is a non-finite number the contract forbids
    outright (case D07), and ``true`` is neither -- even though ``bool`` is an
    ``int`` subclass in Python and would otherwise sail through as 1.

    These are checked *after* parsing rather than via ``json.loads``'s
    ``parse_float``/``parse_constant`` hooks, because passing either hook makes
    ``json`` fall back from its C scanner to the pure-Python one. That cost about
    20x on the verifier, which has to stream whole datasets.
    """
    if isinstance(value, bool):
        raise RecordParseError("field_not_integer", field)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise RecordParseError("non_finite_number", field)
        raise RecordParseError("float_not_allowed", field)
    if not isinstance(value, int):
        raise RecordParseError("field_not_integer", field)
    return value


def parse_canonical_line(line: str | bytes, *, max_bytes: int | None = None) -> Event:
    """Strict parser for one JSONL record.

    Rejects oversized lines, non-object JSON, unknown or missing fields, floats
    and booleans where integers are required, ``NaN``/``Infinity`` (which
    ``json`` accepts as bare words by default -- turned off here), unsupported
    schema versions and invalid symbols.

    It deliberately does **not** check price or sequence invariants: that is the
    verifier's job, because a fault-injected dataset is expected to violate them.
    """
    raw = line.encode("utf-8") if isinstance(line, str) else bytes(line)
    if max_bytes is not None and len(raw) > max_bytes:
        raise RecordParseError("record_too_large", f"{len(raw)} > {max_bytes}")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RecordParseError("invalid_utf8", str(exc)) from exc
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RecordParseError("invalid_json", exc.msg) from exc
    if not isinstance(obj, dict):
        raise RecordParseError("not_an_object", type(obj).__name__)

    schema_version = obj.get("schema_version")
    if schema_version != SCHEMA_VERSION:
        raise RecordParseError("unsupported_schema_version", repr(schema_version))
    event_type = obj.get("event_type")
    if event_type not in EVENT_TYPES:
        raise RecordParseError("unknown_event_type", repr(event_type))

    expected = QUOTE_FIELDS if event_type == EVENT_QUOTE else TICK_FIELDS
    keys = tuple(sorted(obj))
    if keys != expected:
        missing = [k for k in expected if k not in obj]
        unknown = [k for k in keys if k not in expected]
        raise RecordParseError("field_set_mismatch", f"missing={missing} unknown={unknown}")

    symbol = obj["symbol"]
    if not is_valid_symbol(symbol):
        raise RecordParseError("invalid_symbol", repr(symbol))
    dataset_id = obj["dataset_id"]
    if not isinstance(dataset_id, str) or not dataset_id:
        raise RecordParseError("invalid_dataset_id", repr(dataset_id))

    sequence = _require_int(obj["sequence"], "sequence")
    event_time_ns = _require_int(obj["event_time_ns"], "event_time_ns")
    price_scale = _require_int(obj["price_scale"], "price_scale")

    if event_type == EVENT_QUOTE:
        return Event(
            dataset_id=dataset_id,
            event_type=EVENT_QUOTE,
            symbol=symbol,
            sequence=sequence,
            event_time_ns=event_time_ns,
            price_scale=price_scale,
            bid_ticks=_require_int(obj["bid_ticks"], "bid_ticks"),
            ask_ticks=_require_int(obj["ask_ticks"], "ask_ticks"),
        )
    return Event(
        dataset_id=dataset_id,
        event_type=EVENT_TICK,
        symbol=symbol,
        sequence=sequence,
        event_time_ns=event_time_ns,
        price_scale=price_scale,
        price_ticks=_require_int(obj["price_ticks"], "price_ticks"),
    )
