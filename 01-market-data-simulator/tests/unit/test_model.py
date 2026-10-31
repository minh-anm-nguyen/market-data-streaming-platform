"""Contract tests for the canonical record (section 3, cases D06-D12)."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from market_sim.model import (
    EVENT_QUOTE,
    EVENT_TICK,
    QUOTE_FIELDS,
    SCHEMA_VERSION,
    TICK_FIELDS,
    Event,
    RecordParseError,
    is_valid_symbol,
    parse_canonical_line,
    reference_canonical_json,
    to_canonical_json,
)


class _Remove:
    def __repr__(self) -> str:
        return "<REMOVE>"


REMOVE = _Remove()


def quote(**kw) -> Event:
    base = dict(
        dataset_id="ds-0123456789abcdef",
        event_type=EVENT_QUOTE,
        symbol="EURUSD",
        sequence=42,
        event_time_ns=1793322000008200000,
        price_scale=100000,
        bid_ticks=108346,
        ask_ticks=108348,
    )
    base.update(kw)
    return Event(**base)


def tick(**kw) -> Event:
    base = dict(
        dataset_id="ds-0123456789abcdef",
        event_type=EVENT_TICK,
        symbol="EURUSD",
        sequence=43,
        event_time_ns=1793322000010000000,
        price_scale=100000,
        price_ticks=108347,
    )
    base.update(kw)
    return Event(**base)


def mutate(event: Event, **changes) -> str:
    obj = json.loads(to_canonical_json(event))
    for key, value in changes.items():
        if value is REMOVE:
            obj.pop(key, None)
        else:
            obj[key] = value
    return json.dumps(obj)


class TestCanonicalSerialization:
    """The fast serialiser must never drift from the json.dumps contract."""

    @pytest.mark.parametrize(
        "event",
        [
            quote(),
            tick(),
            quote(bid_ticks=1, ask_ticks=1),
            # Fault injection legitimately produces these shapes, so the
            # serialiser has to stay byte-exact for them too.
            quote(bid_ticks=-5, ask_ticks=-1),
            quote(bid_ticks=108348, ask_ticks=108346),
            tick(price_ticks=0),
            tick(price_ticks=-1),
            tick(price_ticks=2**62),
            quote(sequence=2**53, event_time_ns=0),
            quote(symbol="USD_JPY.X", price_scale=1),
        ],
    )
    def test_matches_reference_oracle(self, event):
        assert to_canonical_json(event) == reference_canonical_json(event)

    def test_matches_the_payload_printed_in_the_design_doc(self):
        # Section 3.1 of SYSTEM_DESIGN prints these exact records.
        assert json.loads(to_canonical_json(quote(dataset_id="demo-a1b2c3"))) == {
            "schema_version": "sim.v1",
            "dataset_id": "demo-a1b2c3",
            "event_type": "quote",
            "symbol": "EURUSD",
            "sequence": 42,
            "event_time_ns": 1793322000008200000,
            "price_scale": 100000,
            "bid_ticks": 108346,
            "ask_ticks": 108348,
        }
        assert json.loads(to_canonical_json(tick(dataset_id="demo-a1b2c3"))) == {
            "schema_version": "sim.v1",
            "dataset_id": "demo-a1b2c3",
            "event_type": "tick",
            "symbol": "EURUSD",
            "sequence": 43,
            "event_time_ns": 1793322000010000000,
            "price_scale": 100000,
            "price_ticks": 108347,
        }

    def test_keys_are_sorted_and_separators_are_tight(self):
        raw = to_canonical_json(quote()).decode()
        assert raw.startswith('{"ask_ticks":')
        assert ", " not in raw and ": " not in raw
        assert tuple(json.loads(raw)) == QUOTE_FIELDS

    def test_tick_field_set(self):
        assert tuple(sorted(json.loads(to_canonical_json(tick())))) == TICK_FIELDS

    def test_no_trailing_newline(self):
        # The writer owns the newline; a serialiser that added one would make
        # double-newline bugs easy and silent.
        assert not to_canonical_json(quote()).endswith(b"\n")

    def test_quote_omits_price_ticks_and_tick_omits_bid_ask(self):
        assert b"price_ticks" not in to_canonical_json(quote())
        assert b"bid_ticks" not in to_canonical_json(tick())


class TestRoundTrip:
    @pytest.mark.parametrize(
        "event", [quote(), tick(), quote(bid_ticks=-5, ask_ticks=-1)]
    )
    def test_parse_inverts_serialize(self, event):
        assert parse_canonical_line(to_canonical_json(event)) == event

    def test_accepts_str_and_bytes(self):
        raw = to_canonical_json(quote())
        assert parse_canonical_line(raw) == parse_canonical_line(raw.decode())


class TestStrictParser:
    @pytest.mark.parametrize(
        "raw,reason",
        [
            ("not json", "invalid_json"),
            ("[]", "not_an_object"),
            ('"a string"', "not_an_object"),
            ("123", "not_an_object"),
            ("{}", "unsupported_schema_version"),
            ('{"schema_version":"sim.v2"}', "unsupported_schema_version"),
        ],
    )
    def test_structural_rejections(self, raw, reason):
        with pytest.raises(RecordParseError) as exc:
            parse_canonical_line(raw)
        assert exc.value.reason == reason

    def test_unknown_event_type(self):
        with pytest.raises(RecordParseError) as exc:
            parse_canonical_line(mutate(quote(), event_type="trade"))
        assert exc.value.reason == "unknown_event_type"

    def test_unknown_extra_field_is_rejected_not_ignored(self):
        # Case D06: a consumer that ignores unknown fields cannot tell a schema
        # change from a corrupt record.
        with pytest.raises(RecordParseError) as exc:
            parse_canonical_line(mutate(quote(), volume=100))
        assert exc.value.reason == "field_set_mismatch"
        assert "volume" in exc.value.detail

    def test_missing_required_field(self):
        with pytest.raises(RecordParseError) as exc:
            parse_canonical_line(mutate(quote(), ask_ticks=REMOVE))
        assert exc.value.reason == "field_set_mismatch"
        assert "ask_ticks" in exc.value.detail

    def test_quote_fields_on_a_tick_are_rejected(self):
        with pytest.raises(RecordParseError) as exc:
            parse_canonical_line(mutate(tick(), bid_ticks=1))
        assert exc.value.reason == "field_set_mismatch"

    @pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity"])
    def test_non_finite_numbers_rejected(self, bad):
        # json.loads accepts these bare words by default; case D07 says we must not.
        raw = to_canonical_json(quote()).decode().replace("108346", bad)
        with pytest.raises(RecordParseError) as exc:
            parse_canonical_line(raw)
        assert exc.value.reason == "non_finite_number"

    @pytest.mark.parametrize(
        "field", ["bid_ticks", "sequence", "event_time_ns", "price_scale"]
    )
    def test_float_where_integer_required(self, field):
        with pytest.raises(RecordParseError) as exc:
            parse_canonical_line(mutate(quote(), **{field: 1.5}))
        assert exc.value.reason == "float_not_allowed"

    @pytest.mark.parametrize("field", ["bid_ticks", "sequence", "price_scale"])
    def test_bool_is_not_an_integer(self, field):
        # bool subclasses int in Python, so a JSON true would otherwise pass as 1.
        with pytest.raises(RecordParseError) as exc:
            parse_canonical_line(mutate(quote(), **{field: True}))
        assert exc.value.reason == "field_not_integer"

    @pytest.mark.parametrize(
        "symbol", ["", "EU", "e" * 17, "eurusd", "EUR USD", "EUR/USD", "EUR\nUSD"]
    )
    def test_invalid_symbols_rejected(self, symbol):
        with pytest.raises(RecordParseError) as exc:
            parse_canonical_line(mutate(quote(), symbol=symbol))
        assert exc.value.reason == "invalid_symbol"

    def test_empty_dataset_id_rejected(self):
        with pytest.raises(RecordParseError) as exc:
            parse_canonical_line(mutate(quote(), dataset_id=""))
        assert exc.value.reason == "invalid_dataset_id"

    def test_oversized_record_rejected_before_parse(self):
        raw = to_canonical_json(quote())
        with pytest.raises(RecordParseError) as exc:
            parse_canonical_line(raw, max_bytes=len(raw) - 1)
        assert exc.value.reason == "record_too_large"

    def test_size_check_is_inclusive_at_the_limit(self):
        raw = to_canonical_json(quote())
        assert parse_canonical_line(raw, max_bytes=len(raw)) == quote()

    def test_invalid_utf8_rejected(self):
        with pytest.raises(RecordParseError) as exc:
            parse_canonical_line(b'{"schema_version":"sim.v1","symbol":"\xff\xfe"}')
        assert exc.value.reason == "invalid_utf8"


class TestSymbolAllowlist:
    @pytest.mark.parametrize(
        "symbol", ["EURUSD", "USDJPY", "XAU_USD", "US500.CFD", "ABC", "A" * 16]
    )
    def test_valid(self, symbol):
        assert is_valid_symbol(symbol)

    @pytest.mark.parametrize(
        "symbol", ["", "AB", "a" * 17, "eur", "EUR-USD", 'EUR"USD', None, 42, b"EURUSD"]
    )
    def test_invalid(self, symbol):
        assert not is_valid_symbol(symbol)

    def test_allowlist_needs_no_json_escaping(self):
        # This is why the allowlist exists: the fast serialiser interpolates the
        # symbol straight into a JSON string with no escaping.
        for symbol in ("EURUSD", "US500.CFD", "XAU_USD", "A" * 16):
            event = quote(symbol=symbol)
            assert to_canonical_json(event) == reference_canonical_json(event)


class TestEventSemantics:
    def test_event_id_is_the_logical_triple(self):
        event = quote()
        assert event.event_id == (event.dataset_id, "EURUSD", 42)

    def test_intentional_duplicate_shares_the_event_id(self):
        # This is what makes downstream deduplication testable (case S02).
        assert quote().event_id == quote().event_id

    def test_same_sequence_across_symbols_is_a_different_event(self):
        # Case S05: sequence is scoped to (dataset_id, symbol).
        assert quote(symbol="EURUSD").event_id != quote(symbol="GBPUSD").event_id

    def test_spread_ticks(self):
        assert quote().spread_ticks == 2
        assert quote(bid_ticks=108348, ask_ticks=108346).spread_ticks == -2
        assert tick().spread_ticks is None

    def test_display_price_is_decimal_not_float(self):
        prices = quote().display_price()
        assert prices["bid"] == Decimal("1.08346")
        assert prices["ask"] == Decimal("1.08348")
        assert all(isinstance(v, Decimal) for v in prices.values())

    def test_display_price_honours_per_symbol_scale(self):
        # Case D11: USDJPY uses scale 1000 in this simulation.
        jpy = tick(symbol="USDJPY", price_scale=1000, price_ticks=151230)
        assert jpy.display_price()["price"] == Decimal("151.23")

    def test_display_price_is_never_serialised(self):
        assert b"1.08346" not in to_canonical_json(quote())

    def test_event_is_immutable(self):
        event = quote()
        with pytest.raises(AttributeError):
            event.bid_ticks = 1  # type: ignore[misc]

    def test_replace_returns_a_copy(self):
        event = quote()
        other = event.replace(bid_ticks=1)
        assert event.bid_ticks == 108346
        assert other.bid_ticks == 1
        assert other.sequence == event.sequence

    def test_schema_version_defaults_to_the_frozen_contract(self):
        assert quote().schema_version == SCHEMA_VERSION == "sim.v1"

    def test_to_canonical_dict_omits_the_unused_price_fields(self):
        assert "price_ticks" not in quote().to_canonical_dict()
        assert "bid_ticks" not in tick().to_canonical_dict()
