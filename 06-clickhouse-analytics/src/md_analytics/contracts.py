"""Source identity, semantic core, payload digest and the inherited contract.

Section 3 of the system design. Three rules decide almost everything else in
this package:

* **Identity is ``(dataset_id, symbol, sequence)``** and nothing else. Kafka
  topic/partition/offset describe *transport*, so a replay through a different
  partition topology must produce the same identities (case A-03, E-11).
* **Same identity + same semantic core** is a physical duplicate, which
  at-least-once delivery makes normal. **Same identity + different semantic
  core** is a ``CONFLICT`` and stops the build; picking ``argMax(ingested_at)``
  would hide a real disagreement about what the market did (case E-02..E-04).
* **A stored digest is never trusted on its own.** It is recomputed from the
  typed fields and compared, because a corrupted or forged digest must not be
  able to launder a mismatched row through the audit (case E-05, E-10).

Prices stay integer ticks with an explicit ``price_scale``; ``float`` never
touches a value that a verdict depends on (section 3.2).
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from typing import Final, NamedTuple

from .errors import ContractError

CONTRACT_VERSION: Final = "marketdata.v1"

EVENT_TICK: Final = "tick"
EVENT_QUOTE: Final = "quote"
EVENT_KINDS: Final = frozenset({EVENT_TICK, EVENT_QUOTE})

NS_PER_SECOND: Final = 1_000_000_000
NS_PER_MINUTE: Final = 60 * NS_PER_SECOND

INT64_MIN: Final = -(2**63)
INT64_MAX: Final = 2**63 - 1

#: Negative epoch nanoseconds are outside the P01/P05 domain (case D-11), and
#: the upper bound is where ``Int64`` nanoseconds stop being representable at
#: all -- 2262-04-11T23:47:16.854775807Z. Anything outside is rejected rather
#: than wrapped (case D-05).
MIN_EVENT_TIME_NS: Final = 0
MAX_EVENT_TIME_NS: Final = INT64_MAX

#: Symbol alphabet inherited from the P01 record model. Keeping it narrow is
#: what lets ``LowCardinality(String)`` stay small, and it makes ``DEMO`` and
#: ``DEMO.`` two distinct identities rather than a silent trim (case E-14).
SYMBOL_MIN_LEN: Final = 3
SYMBOL_MAX_LEN: Final = 16
_SYMBOL_ALPHABET: Final = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.")

#: Fields of the semantic core, per kind, in digest order. The core deliberately
#: excludes ``ingested_at``, ``kafka_*`` and ``topic_generation``: a replay of
#: the same event through a new topic generation is the *same* event, and a
#: non-deterministic ingest clock must not change its digest (case B-10, A-13).
_TICK_CORE_FIELDS: Final = (
    "dataset_id",
    "symbol",
    "sequence",
    "event_kind",
    "event_time_ns",
    "price_scale",
    "price_ticks",
    "source_schema_version",
)
_QUOTE_CORE_FIELDS: Final = (
    "dataset_id",
    "symbol",
    "sequence",
    "event_kind",
    "event_time_ns",
    "price_scale",
    "bid_ticks",
    "ask_ticks",
    "source_schema_version",
)

DIGEST_HEX_LEN: Final = 64
_HEX_CHARS: Final = frozenset("0123456789abcdef")


class SourceIdentity(NamedTuple):
    """The logical event id. Transport coordinates are not part of it."""

    dataset_id: str
    symbol: str
    sequence: int


class SourceOrder(NamedTuple):
    """Ordering key, valid **only** within one ``(dataset_id, symbol)``.

    ``event_time_ns`` decides OPEN/CLOSE and ``sequence`` is the tie-break
    (section 3.3). Because ``sequence`` is unique per identity, this key is a
    total order inside a bar, which is what makes OPEN/CLOSE deterministic
    instead of "whatever ``any()`` happened to return" (case H-11, I-08).
    """

    event_time_ns: int
    sequence: int


def is_valid_symbol(symbol: object) -> bool:
    return (
        isinstance(symbol, str)
        and SYMBOL_MIN_LEN <= len(symbol) <= SYMBOL_MAX_LEN
        and set(symbol) <= _SYMBOL_ALPHABET
    )


def _require_int(value: object, field: str) -> int:
    # bool is an int subclass in Python, so a JSON ``true`` would pass as 1.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(
            f"{field} must be an integer, got {type(value).__name__}",
            code="FIELD_NOT_INTEGER",
            field=field,
        )
    return value


def require_int64(value: object, field: str) -> int:
    v = _require_int(value, field)
    if not INT64_MIN <= v <= INT64_MAX:
        raise ContractError(
            f"{field}={v} does not fit Int64",
            code="INT64_OVERFLOW",
            field=field,
            value=v,
        )
    return v


def require_event_time_ns(value: object) -> int:
    """Nanosecond epoch inside the supported domain, or reject (case D-05, D-11)."""
    v = require_int64(value, "event_time_ns")
    if not MIN_EVENT_TIME_NS <= v <= MAX_EVENT_TIME_NS:
        raise ContractError(
            f"event_time_ns={v} outside supported domain "
            f"[{MIN_EVENT_TIME_NS}, {MAX_EVENT_TIME_NS}]",
            code="EVENT_TIME_OUT_OF_DOMAIN",
            value=v,
        )
    return v


def require_price_scale(value: object) -> int:
    """``price_scale`` must be strictly positive: never divide by zero (case C-02)."""
    v = require_int64(value, "price_scale")
    if v <= 0:
        raise ContractError(
            f"price_scale={v} must be > 0", code="PRICE_SCALE_INVALID", value=v
        )
    return v


def utc_date_from_ns(event_time_ns: int) -> date:
    """UTC calendar date of a nanosecond timestamp, independent of host TZ.

    Built from integer floor division and pinned to ``timezone.utc``, so a server
    running ``TZ=Asia/Ho_Chi_Minh`` derives the same partition and a sub-second
    remainder cannot push the date across midnight (case B-03, D-04).
    """
    seconds = event_time_ns // NS_PER_SECOND  # floor division: correct for any sign
    return datetime.fromtimestamp(seconds, tz=timezone.utc).date()


def semantic_core(
    *,
    dataset_id: str,
    symbol: str,
    sequence: int,
    event_kind: str,
    event_time_ns: int,
    price_scale: int,
    price_ticks: int | None,
    bid_ticks: int | None,
    ask_ticks: int | None,
    source_schema_version: str,
) -> dict[str, object]:
    """Validate and project the fields that define *what happened*.

    Field presence is part of the core: a tick carrying ``bid_ticks``, or a quote
    carrying ``price_ticks``, is a contract violation rather than something to
    normalise away (case A-01, E-04).
    """
    if event_kind not in EVENT_KINDS:
        raise ContractError(
            f"unknown event_kind {event_kind!r}",
            code="UNKNOWN_EVENT_KIND",
            value=event_kind,
        )
    if not is_valid_symbol(symbol):
        raise ContractError(f"invalid symbol {symbol!r}", code="INVALID_SYMBOL", value=symbol)
    if not isinstance(dataset_id, str) or not dataset_id or dataset_id.strip() != dataset_id:
        raise ContractError(
            f"invalid dataset_id {dataset_id!r}",
            code="INVALID_DATASET_ID",
            value=dataset_id,
        )
    if not isinstance(source_schema_version, str) or not source_schema_version:
        raise ContractError(
            "source_schema_version is required",
            code="MISSING_SCHEMA_VERSION",
            value=source_schema_version,
        )

    core: dict[str, object] = {
        "dataset_id": dataset_id,
        "symbol": symbol,
        "sequence": require_int64(sequence, "sequence"),
        "event_kind": event_kind,
        "event_time_ns": require_event_time_ns(event_time_ns),
        "price_scale": require_price_scale(price_scale),
        "source_schema_version": source_schema_version,
    }
    if event_kind == EVENT_TICK:
        if price_ticks is None:
            raise ContractError(
                "tick without price_ticks", code="TICK_MISSING_PRICE", field="price_ticks"
            )
        if bid_ticks is not None or ask_ticks is not None:
            raise ContractError("tick carries quote fields", code="TICK_HAS_QUOTE_FIELDS")
        core["price_ticks"] = require_int64(price_ticks, "price_ticks")
        fields = _TICK_CORE_FIELDS
    else:
        if bid_ticks is None or ask_ticks is None:
            raise ContractError("quote without bid/ask", code="QUOTE_MISSING_SIDES")
        if price_ticks is not None:
            raise ContractError("quote carries price_ticks", code="QUOTE_HAS_TICK_PRICE")
        core["bid_ticks"] = require_int64(bid_ticks, "bid_ticks")
        core["ask_ticks"] = require_int64(ask_ticks, "ask_ticks")
        fields = _QUOTE_CORE_FIELDS

    return {k: core[k] for k in fields}


def canonical_encoding(core: dict[str, object]) -> bytes:
    """Byte-stable encoding of a semantic core: sorted keys, no spaces, UTF-8.

    ``allow_nan=False`` is belt and braces -- every value is an ``int`` or ``str``
    by the time it arrives -- but it turns a future regression that admits a
    float into an exception instead of a ``NaN`` inside a digest.
    """
    return json.dumps(
        core, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def payload_digest(core: dict[str, object]) -> str:
    """SHA-256 hex of the canonical encoding: 64 lowercase hex characters."""
    return hashlib.sha256(canonical_encoding(core)).hexdigest()


def require_digest_format(digest: object) -> str:
    """Reject padded, truncated or non-hex digests before they are compared.

    ``FixedString(64)`` pads with NUL bytes rather than failing, so a
    63-character digest round-trips as a *different* 64-byte value; this check is
    what turns case B-05 into an explicit reject instead of a silent mismatch.
    """
    if not isinstance(digest, str):
        raise ContractError(
            f"payload_digest must be str, got {type(digest).__name__}",
            code="DIGEST_NOT_STRING",
        )
    if len(digest) != DIGEST_HEX_LEN:
        raise ContractError(
            f"payload_digest length {len(digest)} != {DIGEST_HEX_LEN}",
            code="DIGEST_BAD_LENGTH",
            value=digest,
        )
    if not set(digest) <= _HEX_CHARS:
        raise ContractError(
            "payload_digest is not lowercase hex", code="DIGEST_NOT_HEX", value=digest
        )
    return digest


def display_price(price_ticks: int, price_scale: int) -> str:
    """Decimal string for humans only, built by integer arithmetic.

    Returned as a ``str`` rather than a ``float`` or ``Decimal`` because case
    C-01/C-09 is precisely about nothing in the pipeline round-tripping a price
    through a binary float -- including the formatting helper that someone will
    eventually reuse somewhere it matters.
    """
    scale = require_price_scale(price_scale)
    width = len(str(scale)) - 1
    sign = "-" if price_ticks < 0 else ""
    whole, frac = divmod(abs(price_ticks), scale)
    if width <= 0:
        return f"{sign}{whole}"
    return f"{sign}{whole}.{frac:0{width}d}"
