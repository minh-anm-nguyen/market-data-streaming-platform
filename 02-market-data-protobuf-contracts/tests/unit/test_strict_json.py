"""Strict sim.v1 reader: what-if group C.

These are the rejections ``json.loads`` would not make on its own, which is the
reason the module exists. Each test names the case it stands for so a failure
points at a documented scenario rather than at a line number.
"""

from __future__ import annotations

import pytest

from md_contract.domain import INT64_MAX, INT64_MIN, PAYLOAD_QUOTE, PAYLOAD_TICK
from md_contract.errors import Reason, StrictJsonError
from md_contract.strict_json import MAX_JSON_LINE_BYTES, parse_sim_v1_line

from ..conftest import REMOVE, quote_line, tick_line


def reason_of(line: bytes, **kwargs: object) -> Reason:
    with pytest.raises(StrictJsonError) as excinfo:
        parse_sim_v1_line(line, **kwargs)  # type: ignore[arg-type]
    return excinfo.value.reason


@pytest.mark.cases("C01")
def test_duplicate_key_is_rejected_not_last_wins() -> None:
    # Hand-built: a dict literal could not express a duplicate key at all, which
    # is itself why this has to be checked at the JSON text level.
    line = (
        b'{"ask_ticks":108348,"bid_ticks":108345,"bid_ticks":999,"dataset_id":"ds-test",'
        b'"event_time_ns":1,"event_type":"quote","price_scale":100000,'
        b'"schema_version":"sim.v1","sequence":1,"symbol":"EURUSD"}'
    )
    assert reason_of(line) is Reason.DUPLICATE_JSON_KEY


@pytest.mark.cases("C02")
def test_boolean_is_not_an_integer() -> None:
    # bool subclasses int in Python, so `isinstance(True, int)` is True and a
    # naive guard would accept this as sequence == 1.
    assert reason_of(quote_line(sequence="true")) is Reason.FIELD_NOT_INTEGER
    assert reason_of(quote_line(bid_ticks="false")) is Reason.FIELD_NOT_INTEGER


@pytest.mark.cases("C03")
def test_float_literal_is_rejected_not_rounded() -> None:
    assert reason_of(quote_line(sequence="1.0")) is Reason.FLOAT_NOT_ALLOWED
    assert reason_of(quote_line(bid_ticks="108345.5")) is Reason.FLOAT_NOT_ALLOWED


@pytest.mark.cases("C04")
def test_exponent_notation_is_rejected() -> None:
    for literal in ("1e3", "1E3", "1e+3", "1.5e3"):
        assert reason_of(quote_line(sequence=literal)) is Reason.FLOAT_NOT_ALLOWED


@pytest.mark.cases("C05", "C06")
def test_int64_boundaries() -> None:
    # The boundary values themselves must be accepted exactly...
    event = parse_sim_v1_line(quote_line(bid_ticks=str(INT64_MIN), ask_ticks=str(INT64_MAX)))
    assert event.bid_ticks == INT64_MIN
    assert event.ask_ticks == INT64_MAX

    # ... and one step past either end must be refused, never wrapped or clamped.
    # A 20-digit token may trip the digit-run guard first; both are refusals and
    # the test accepts either, because the important property is that no value
    # gets through.
    assert reason_of(quote_line(ask_ticks=str(INT64_MAX + 1))) in (
        Reason.INT64_OUT_OF_RANGE,
        Reason.NUMERIC_TOKEN_TOO_LONG,
    )
    assert reason_of(quote_line(bid_ticks=str(INT64_MIN - 1))) in (
        Reason.INT64_OUT_OF_RANGE,
        Reason.NUMERIC_TOKEN_TOO_LONG,
    )


@pytest.mark.cases("C07")
def test_overlong_numeric_token_is_refused_before_conversion() -> None:
    assert reason_of(quote_line(sequence="9" * 400)) is Reason.NUMERIC_TOKEN_TOO_LONG


@pytest.mark.cases("C08")
def test_single_nanosecond_resolution_is_preserved() -> None:
    base = 1793322000008200000
    a = parse_sim_v1_line(quote_line(event_time_ns=str(base)))
    b = parse_sim_v1_line(quote_line(event_time_ns=str(base + 1)))
    assert b.event_time_ns - a.event_time_ns == 1


@pytest.mark.cases("C09")
def test_values_above_2_to_the_53_are_exact() -> None:
    # 2**53 + 1 is the smallest integer a float64 cannot represent. If any step
    # of parsing went through a float, this would come back as 9007199254740992.
    value = 2**53 + 1
    event = parse_sim_v1_line(tick_line(price_ticks=str(value)))
    assert event.price_ticks == value
    assert event.price_ticks != float(value)


@pytest.mark.cases("C10")
def test_null_price_is_rejected_not_zeroed() -> None:
    assert reason_of(quote_line(bid_ticks="null")) is Reason.FIELD_NOT_INTEGER
    assert reason_of(tick_line(price_ticks="null")) is Reason.FIELD_NOT_INTEGER


@pytest.mark.cases("C11")
def test_missing_event_type_is_rejected() -> None:
    assert reason_of(quote_line(event_type=REMOVE)) is Reason.UNKNOWN_EVENT_TYPE


@pytest.mark.cases("C12")
def test_unsupported_event_type_is_not_guessed() -> None:
    # 'trade' must not be mapped onto 'tick': a synthetic price update and an
    # executed trade are different things.
    assert reason_of(tick_line(event_type='"trade"')) is Reason.UNKNOWN_EVENT_TYPE
    assert reason_of(tick_line(event_type='"QUOTE"')) is Reason.UNKNOWN_EVENT_TYPE


@pytest.mark.cases("C13")
def test_quote_carrying_price_ticks_is_contradictory() -> None:
    assert reason_of(quote_line(price_ticks="5")) is Reason.FIELD_SET_MISMATCH


@pytest.mark.cases("C14")
def test_tick_carrying_bid_ask_is_contradictory() -> None:
    assert reason_of(tick_line(bid_ticks="1", ask_ticks="2")) is Reason.FIELD_SET_MISMATCH


@pytest.mark.cases("C15")
def test_invalid_utf8_is_rejected_not_replaced() -> None:
    line = bytearray(quote_line())
    line[line.index(b"EURUSD")] = 0x80  # a lone continuation byte
    assert reason_of(bytes(line)) is Reason.INVALID_UTF8


@pytest.mark.cases("C16")
def test_confusable_symbols_are_rejected_not_normalised() -> None:
    # Cyrillic 'Е' (U+0415) renders like Latin 'E'. Silent NFKC folding would
    # change the identity tuple, so it is refused instead.
    assert reason_of(quote_line(symbol='"ЕURUSD"')) is Reason.INVALID_SYMBOL
    assert reason_of(quote_line(symbol='"eurusd"')) is Reason.INVALID_SYMBOL
    assert reason_of(quote_line(symbol='"EU"')) is Reason.INVALID_SYMBOL
    assert reason_of(quote_line(symbol='"EUR USD"')) is Reason.INVALID_SYMBOL


@pytest.mark.cases("C17")
def test_unknown_source_schema_version_is_rejected() -> None:
    assert (
        reason_of(quote_line(schema_version='"sim.v2"'))
        is Reason.UNSUPPORTED_SOURCE_SCHEMA_VERSION
    )
    assert (
        reason_of(quote_line(schema_version=REMOVE))
        is Reason.UNSUPPORTED_SOURCE_SCHEMA_VERSION
    )


@pytest.mark.cases("C07", "J04", "J06")
def test_dataset_id_rejects_control_characters_and_paths() -> None:
    assert reason_of(quote_line(dataset_id='"../../etc/passwd"')) is Reason.INVALID_DATASET_ID
    assert reason_of(quote_line(dataset_id='"ds\\u0000x"')) is Reason.INVALID_DATASET_ID
    assert reason_of(quote_line(dataset_id='"ds\\nx"')) is Reason.INVALID_DATASET_ID
    assert reason_of(quote_line(dataset_id='""')) is Reason.INVALID_DATASET_ID
    assert reason_of(quote_line(dataset_id=f'"{"x" * 200}"')) is Reason.STRING_TOO_LONG


@pytest.mark.cases("C03")
def test_non_finite_bare_words_are_rejected() -> None:
    for literal in ("NaN", "Infinity", "-Infinity"):
        assert reason_of(quote_line(bid_ticks=literal)) is Reason.NON_FINITE_NUMBER


def test_byte_budget_is_measured_in_bytes_not_characters() -> None:
    # The budget protects the downstream decoder, which deals in bytes. A line
    # of multi-byte characters that is short in `len(str)` can still be over.
    assert reason_of(b"x" * (MAX_JSON_LINE_BYTES + 1)) is Reason.JSON_LINE_TOO_LARGE
    padded = quote_line(dataset_id='"' + "d" * 100 + '"')
    assert reason_of(padded, max_line_bytes=len(padded) - 1) is Reason.JSON_LINE_TOO_LARGE
    # Exactly at the budget is accepted: the limit is inclusive.
    parse_sim_v1_line(padded, max_line_bytes=len(padded))


def test_line_terminator_is_not_counted_against_the_budget() -> None:
    line = quote_line()
    for suffix in (b"", b"\n", b"\r\n"):
        event = parse_sim_v1_line(line + suffix, max_line_bytes=len(line))
        assert event.sequence == 1


def test_structural_rejections() -> None:
    assert reason_of(b"[1,2,3]") is Reason.NOT_AN_OBJECT
    assert reason_of(b'"a string"') is Reason.NOT_AN_OBJECT
    assert reason_of(b"{not json") is Reason.INVALID_JSON
    assert reason_of(b"") is Reason.INVALID_JSON


def test_valid_quote_and_tick_parse_fully() -> None:
    quote = parse_sim_v1_line(quote_line())
    assert quote.payload_case == PAYLOAD_QUOTE
    assert (quote.bid_ticks, quote.ask_ticks) == (108345, 108348)
    assert quote.price_ticks is None
    assert quote.source_schema_version == "sim.v1"
    # Every envelope field present: a parsed event never has an absent core
    # scalar, because the field-set check would have rejected it.
    assert quote.present_fields() == (
        "ask_ticks",
        "bid_ticks",
        "dataset_id",
        "event_time_ns",
        "price_scale",
        "sequence",
        "source_schema_version",
        "symbol",
    )

    tick = parse_sim_v1_line(tick_line())
    assert tick.payload_case == PAYLOAD_TICK
    assert tick.price_ticks == 108346
    assert tick.bid_ticks is None


@pytest.mark.cases("C18")
def test_business_anomalies_parse_cleanly_at_this_layer() -> None:
    # Layer A's job is structure, not values. A negative bid is well-formed
    # sim.v1; whether it is acceptable is the validator's decision, and deciding
    # it here would make a real corruption and a deliberate fault look alike.
    event = parse_sim_v1_line(quote_line(bid_ticks="-1", ask_ticks="-1"))
    assert event.bid_ticks == -1
    crossed = parse_sim_v1_line(quote_line(bid_ticks="200", ask_ticks="100"))
    assert crossed.spread_ticks == -100
