"""Strict reader for Project 01 ``sim.v1`` JSONL (section 3.3 layer A).

``json.loads`` on its own is **not** validation for this contract, and most of
this module exists because of what it accepts by default:

* ``{"sequence":1,"sequence":2}`` -- duplicate keys, last one silently wins.
* ``1.0`` and ``1e3`` -- floats where the contract says integer literal.
* ``true`` where an integer is expected: ``bool`` is an ``int`` subclass in
  Python, so ``isinstance(True, int)`` is ``True`` and a naive check passes it.
* ``NaN`` / ``Infinity`` / ``-Infinity`` -- accepted as bare words by default.
* An integer with thousands of digits, which converts fine and then will not fit
  in an ``int64``.

Each of those becomes a typed rejection with a stable reason instead. The parser
also enforces byte budgets **in bytes**, not characters: a 4096-character line of
multi-byte UTF-8 is not a 4096-byte line, and the limit that protects the
decoder downstream is the byte one (NFR-03).

What this module deliberately does *not* do is judge business values. A negative
bid is well-formed ``sim.v1``; whether it is acceptable is
:mod:`md_contract.validation`'s decision, and it depends on the mode and on
Project 01's fault manifest. Mixing the two layers here would make a real
corruption indistinguishable from a deliberate fault.
"""

from __future__ import annotations

import json
import re
from typing import Any, Final, Iterator

from .domain import (
    INT64_MAX,
    INT64_MIN,
    PAYLOAD_QUOTE,
    PAYLOAD_TICK,
    SOURCE_SCHEMA_VERSION,
    DomainEvent,
)
from .errors import Reason, StrictJsonError

#: Default byte budgets (section 6.1). The newline terminator is not counted.
MAX_JSON_LINE_BYTES: Final = 4096
MAX_SYMBOL_BYTES: Final = 32
MAX_DATASET_ID_BYTES: Final = 128
MAX_SOURCE_VERSION_BYTES: Final = 32

#: An int64 needs at most 19 digits plus a sign. Anything longer is refused
#: before conversion, so a hostile 4000-digit token cannot buy CPU time in
#: bignum conversion (case C07) even though the line byte cap already bounds it.
MAX_NUMERIC_DIGITS: Final = 19

#: Exactly Project 01's field sets, per event type, sorted. Both "missing" and
#: "unknown" are rejections: a quote carrying ``price_ticks`` is a contradiction,
#: not an extra field to ignore (cases C13, C14).
QUOTE_KEYS: Final = (
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
TICK_KEYS: Final = (
    "dataset_id",
    "event_time_ns",
    "event_type",
    "price_scale",
    "price_ticks",
    "schema_version",
    "sequence",
    "symbol",
)

#: Symbol charset, matching Project 01's generator so a round trip through this
#: codec cannot be the thing that rejects a legitimately generated dataset.
#: Confusable Unicode that merely *looks* like ASCII is refused rather than
#: normalised, because silent NFKC folding would change the identity tuple
#: (case C16).
_SYMBOL_RE: Final = re.compile(r"\A[A-Z0-9_.]{3,16}\Z")

#: Control characters and anything that could forge a log line or a path. Checked
#: on dataset_id, which unlike symbol has no tight charset (cases J04, J06).
_FORBIDDEN_IN_DATASET_ID: Final = re.compile(r"[\x00-\x1f\x7f/\\]")

#: Digit runs, to catch an over-long numeric token before ``json`` converts it.
_LONG_DIGIT_RUN: Final = re.compile(rf"\d{{{MAX_NUMERIC_DIGITS + 1},}}")

#: A JSON string literal, including its escapes.
#:
#: String contents must be removed before scanning for long digit runs. A
#: dataset_id of "00000000000000000000" is twenty perfectly legal characters,
#: and scanning the raw line would reject that record as an over-long *number* --
#: a false rejection of valid input, which is worse than the attack the guard
#: exists to stop. Found by a property test generating digit-only identifiers.
_JSON_STRING: Final = re.compile(r'"(?:[^"\\]|\\.)*"')


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """``object_pairs_hook`` that refuses duplicates instead of last-wins (C01)."""
    seen: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen:
            raise StrictJsonError(Reason.DUPLICATE_JSON_KEY, key)
        seen[key] = value
    return seen


def _reject_float(text: str) -> Any:
    """Catches ``1.0``, ``1e3`` and ``1E+3``: ``json`` routes all of them here (C03, C04)."""
    raise StrictJsonError(Reason.FLOAT_NOT_ALLOWED, text)


def _reject_constant(name: str) -> Any:
    """Catches the bare words ``NaN``, ``Infinity``, ``-Infinity``."""
    raise StrictJsonError(Reason.NON_FINITE_NUMBER, name)


def _require_int(value: object, field: str) -> int:
    # The bool check must come first and must be explicit: bool subclasses int,
    # so a JSON `true` would otherwise sail through as 1 (case C02).
    if isinstance(value, bool) or not isinstance(value, int):
        raise StrictJsonError(Reason.FIELD_NOT_INTEGER, f"{field}={value!r}")
    if not INT64_MIN <= value <= INT64_MAX:
        # Rejected here, before the value reaches generated code, so there is no
        # chance of a wrap or a clamp producing a plausible-looking price
        # (cases C05, C06).
        raise StrictJsonError(Reason.INT64_OUT_OF_RANGE, f"{field}={value}")
    return value


def _require_str(value: object, field: str, max_bytes: int, reason: Reason) -> str:
    if not isinstance(value, str):
        raise StrictJsonError(reason, f"{field} is {type(value).__name__}")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        # A `"\ud800"` escape in the JSON source decodes into a Python str
        # holding an unpaired surrogate, which is not encodable as UTF-8. The
        # byte-level UTF-8 check earlier cannot catch it, because the escape
        # itself is plain ASCII on the wire -- the surrogate only appears after
        # `json` interprets it. Converted to a typed rejection here so no
        # unclassified UnicodeEncodeError can escape the parser; the C++ reader
        # refuses surrogate escapes outright for the same reason.
        raise StrictJsonError(
            Reason.INVALID_UTF8, f"{field} contains an unpaired surrogate"
        ) from exc
    if len(encoded) > max_bytes:
        raise StrictJsonError(
            Reason.STRING_TOO_LONG, f"{field} is {len(encoded)} bytes > {max_bytes}"
        )
    return value


def parse_sim_v1_line(
    line: str | bytes,
    *,
    max_line_bytes: int = MAX_JSON_LINE_BYTES,
    max_symbol_bytes: int = MAX_SYMBOL_BYTES,
    max_dataset_id_bytes: int = MAX_DATASET_ID_BYTES,
) -> DomainEvent:
    """Parse one ``sim.v1`` record into a :class:`DomainEvent`, or raise.

    Raises :class:`~md_contract.errors.StrictJsonError` with a stable reason. The
    returned event always has every envelope field present and a ``quote`` or
    ``tick`` payload: an input record that cannot fill those is a rejection, not
    an event with zeros in it.
    """
    raw = line.encode("utf-8") if isinstance(line, str) else bytes(line)
    # Tolerate the line terminator but do not count it against the budget, so
    # the limit means the same thing for a file reader and a socket reader.
    if raw.endswith(b"\r\n"):
        raw = raw[:-2]
    elif raw.endswith(b"\n"):
        raw = raw[:-1]

    if len(raw) > max_line_bytes:
        raise StrictJsonError(
            Reason.JSON_LINE_TOO_LARGE, f"{len(raw)} bytes > {max_line_bytes}"
        )

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        # Decoded strictly: a lone 0x80 byte is a rejection, never a U+FFFD
        # replacement character smuggled into a symbol (case C15).
        raise StrictJsonError(Reason.INVALID_UTF8, str(exc)) from exc

    # Scan only outside string literals, so digits in an identifier are not
    # mistaken for a numeric token (see _JSON_STRING).
    if _LONG_DIGIT_RUN.search(_JSON_STRING.sub('""', text)):
        raise StrictJsonError(
            Reason.NUMERIC_TOKEN_TOO_LONG, f"numeric token over {MAX_NUMERIC_DIGITS} digits"
        )

    try:
        obj = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except StrictJsonError:
        raise
    except json.JSONDecodeError as exc:
        raise StrictJsonError(Reason.INVALID_JSON, exc.msg) from exc
    except RecursionError as exc:
        # A deeply nested array is a parser-level resource attack; the byte cap
        # bounds it but the failure should still be a clean rejection.
        raise StrictJsonError(Reason.INVALID_JSON, "nesting too deep") from exc

    if not isinstance(obj, dict):
        raise StrictJsonError(Reason.NOT_AN_OBJECT, type(obj).__name__)

    schema_version = obj.get("schema_version")
    if schema_version != SOURCE_SCHEMA_VERSION:
        # No mapper exists for any other input version, and guessing one would
        # mean inventing semantics for fields we have not seen (case C17).
        raise StrictJsonError(
            Reason.UNSUPPORTED_SOURCE_SCHEMA_VERSION, repr(schema_version)
        )

    event_type = obj.get("event_type")
    if event_type not in (PAYLOAD_QUOTE, PAYLOAD_TICK):
        # `trade` is refused rather than mapped onto `tick`: they are not the
        # same thing, and a silent widening here would put fabricated trades
        # into the stream (case C12).
        raise StrictJsonError(Reason.UNKNOWN_EVENT_TYPE, repr(event_type))

    expected = QUOTE_KEYS if event_type == PAYLOAD_QUOTE else TICK_KEYS
    keys = tuple(sorted(obj))
    if keys != expected:
        missing = [k for k in expected if k not in obj]
        unknown = [k for k in keys if k not in expected]
        raise StrictJsonError(
            Reason.FIELD_SET_MISMATCH, f"missing={missing} unknown={unknown}"
        )

    symbol = _require_str(obj["symbol"], "symbol", max_symbol_bytes, Reason.INVALID_SYMBOL)
    if not _SYMBOL_RE.match(symbol):
        raise StrictJsonError(Reason.INVALID_SYMBOL, repr(symbol))

    dataset_id = _require_str(
        obj["dataset_id"], "dataset_id", max_dataset_id_bytes, Reason.INVALID_DATASET_ID
    )
    if not dataset_id or _FORBIDDEN_IN_DATASET_ID.search(dataset_id):
        raise StrictJsonError(Reason.INVALID_DATASET_ID, repr(dataset_id))

    common = {
        "dataset_id": dataset_id,
        "symbol": symbol,
        "sequence": _require_int(obj["sequence"], "sequence"),
        "event_time_ns": _require_int(obj["event_time_ns"], "event_time_ns"),
        "price_scale": _require_int(obj["price_scale"], "price_scale"),
        "source_schema_version": SOURCE_SCHEMA_VERSION,
    }

    if event_type == PAYLOAD_QUOTE:
        return DomainEvent(
            **common,
            payload_case=PAYLOAD_QUOTE,
            bid_ticks=_require_int(obj["bid_ticks"], "bid_ticks"),
            ask_ticks=_require_int(obj["ask_ticks"], "ask_ticks"),
        )
    return DomainEvent(
        **common,
        payload_case=PAYLOAD_TICK,
        price_ticks=_require_int(obj["price_ticks"], "price_ticks"),
    )


def iter_sim_v1_file(
    path: str,
    *,
    max_line_bytes: int = MAX_JSON_LINE_BYTES,
) -> Iterator[tuple[int, DomainEvent | StrictJsonError]]:
    """Stream a ``sim.v1`` file, yielding ``(line_number, event_or_error)``.

    Errors are yielded rather than raised so a corpus run can classify every
    line in one pass. Read in binary and split on ``\\n`` deliberately: text mode
    would hide a line's true byte length and would apply newline translation,
    both of which matter for the byte budget.
    """
    with open(path, "rb") as handle:
        for index, raw in enumerate(handle, start=1):
            try:
                yield index, parse_sim_v1_line(raw, max_line_bytes=max_line_bytes)
            except StrictJsonError as exc:
                yield index, exc
