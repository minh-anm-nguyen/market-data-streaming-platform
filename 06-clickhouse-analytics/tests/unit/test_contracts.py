"""Contract layer: identity, digests, prices, timestamps (groups B, C, D, E)."""

from __future__ import annotations

from datetime import date

import pytest

from _coverage import covers
from md_analytics.contracts import (
    EVENT_QUOTE,
    EVENT_TICK,
    INT64_MAX,
    INT64_MIN,
    SourceIdentity,
    SourceOrder,
    canonical_encoding,
    display_price,
    is_valid_symbol,
    payload_digest,
    require_digest_format,
    require_event_time_ns,
    require_int64,
    require_price_scale,
    semantic_core,
    utc_date_from_ns,
)
from md_analytics.errors import ContractError

BASE = 1_793_361_600_000_000_000  # 2026-10-30T12:00:00Z


def tick_core(**overrides):
    kwargs = dict(
        dataset_id="DS-1",
        symbol="EURUSD",
        sequence=1,
        event_kind=EVENT_TICK,
        event_time_ns=BASE,
        price_scale=100_000,
        price_ticks=100_000,
        bid_ticks=None,
        ask_ticks=None,
        source_schema_version="marketdata.v1",
    )
    kwargs.update(overrides)
    return semantic_core(**kwargs)


class TestIdentity:
    @covers("P06-E-06", "P06-E-07")
    def test_identity_is_scoped_by_dataset_and_symbol(self):
        a = SourceIdentity("DS-A", "EURUSD", 1)
        b = SourceIdentity("DS-B", "EURUSD", 1)
        c = SourceIdentity("DS-A", "USDJPY", 1)
        assert len({a, b, c}) == 3, "sequence 1 is reused per dataset and per symbol"

    @covers("P06-E-14")
    def test_dataset_id_whitespace_is_rejected_not_trimmed(self):
        with pytest.raises(ContractError) as exc:
            tick_core(dataset_id="demo ")
        assert exc.value.code == "INVALID_DATASET_ID"

    @covers("P06-H-11", "P06-I-08")
    def test_source_order_sorts_by_time_then_sequence(self):
        assert SourceOrder(BASE, 1) < SourceOrder(BASE, 2)
        assert SourceOrder(BASE, 99) < SourceOrder(BASE + 1, 1)


class TestDigest:
    @covers("P06-E-01")
    def test_same_core_gives_same_digest(self):
        assert payload_digest(tick_core()) == payload_digest(tick_core())

    @covers("P06-E-02", "P06-E-03", "P06-E-04")
    def test_any_core_field_change_changes_the_digest(self):
        baseline = payload_digest(tick_core())
        for change in (
            {"price_ticks": 100_001},
            {"event_time_ns": BASE + 1},
            {"sequence": 2},
            {"price_scale": 1_000},
            {"source_schema_version": "marketdata.v2"},
            {"symbol": "GBPUSD"},
            {"dataset_id": "DS-2"},
        ):
            assert payload_digest(tick_core(**change)) != baseline, change

    @covers("P06-B-10", "P06-A-13")
    def test_transport_and_ingest_fields_are_not_in_the_core(self):
        # The core projection takes no kafka_* or ingested_at argument at all,
        # which is the structural guarantee behind cases B-10 and A-13.
        assert "ingested_at" not in tick_core()
        assert not any(key.startswith("kafka") for key in tick_core())

    @covers("P06-B-05")
    @pytest.mark.parametrize(
        "digest,code",
        [
            ("a" * 63, "DIGEST_BAD_LENGTH"),
            ("a" * 65, "DIGEST_BAD_LENGTH"),
            ("a" * 63 + " ", "DIGEST_NOT_HEX"),
            ("a" * 63 + "\x00", "DIGEST_NOT_HEX"),
            ("A" * 64, "DIGEST_NOT_HEX"),
            ("g" * 64, "DIGEST_NOT_HEX"),
        ],
    )
    def test_padded_or_truncated_digests_are_rejected(self, digest, code):
        with pytest.raises(ContractError) as exc:
            require_digest_format(digest)
        assert exc.value.code == code

    def test_canonical_encoding_is_byte_stable_and_sorted(self):
        encoded = canonical_encoding(tick_core())
        assert encoded.startswith(b'{"dataset_id":')
        assert b" " not in encoded


class TestEventKindContract:
    @covers("P06-A-01")
    def test_tick_without_price_is_rejected(self):
        with pytest.raises(ContractError) as exc:
            tick_core(price_ticks=None)
        assert exc.value.code == "TICK_MISSING_PRICE"

    @covers("P06-A-01", "P06-E-04")
    def test_tick_carrying_quote_fields_is_rejected(self):
        with pytest.raises(ContractError) as exc:
            tick_core(bid_ticks=1, ask_ticks=2)
        assert exc.value.code == "TICK_HAS_QUOTE_FIELDS"

    @covers("P06-A-01")
    def test_quote_carrying_tick_price_is_rejected(self):
        with pytest.raises(ContractError) as exc:
            tick_core(event_kind=EVENT_QUOTE, bid_ticks=1, ask_ticks=2, price_ticks=5)
        assert exc.value.code == "QUOTE_HAS_TICK_PRICE"

    @covers("P06-A-01")
    def test_quote_core_has_no_price_ticks_key(self):
        core = tick_core(event_kind=EVENT_QUOTE, price_ticks=None, bid_ticks=10, ask_ticks=12)
        assert "price_ticks" not in core
        assert core["bid_ticks"] == 10 and core["ask_ticks"] == 12

    def test_unknown_kind_is_rejected(self):
        with pytest.raises(ContractError) as exc:
            tick_core(event_kind="trade")
        assert exc.value.code == "UNKNOWN_EVENT_KIND"


class TestPrices:
    @covers("P06-C-02")
    @pytest.mark.parametrize("scale", [0, -1, -100_000])
    def test_non_positive_scale_is_rejected(self, scale):
        with pytest.raises(ContractError) as exc:
            require_price_scale(scale)
        assert exc.value.code == "PRICE_SCALE_INVALID"

    @covers("P06-C-04")
    @pytest.mark.parametrize("price", [INT64_MAX, INT64_MAX - 1, INT64_MIN, 0, -1])
    def test_extreme_int64_prices_round_trip_exactly(self, price):
        core = tick_core(price_ticks=price)
        assert core["price_ticks"] == price
        assert payload_digest(core) == payload_digest(tick_core(price_ticks=price))

    @covers("P06-C-04")
    def test_price_beyond_int64_is_rejected_not_wrapped(self):
        with pytest.raises(ContractError) as exc:
            require_int64(INT64_MAX + 1, "price_ticks")
        assert exc.value.code == "INT64_OVERFLOW"

    @covers("P06-C-14")
    @pytest.mark.parametrize("value", [-0.0, 0.0, 1.5, 100_000.0])
    def test_floats_are_rejected_everywhere_a_price_is_expected(self, value):
        with pytest.raises(ContractError) as exc:
            require_int64(value, "price_ticks")
        assert exc.value.code == "FIELD_NOT_INTEGER"

    def test_booleans_are_not_integers(self):
        with pytest.raises(ContractError):
            require_int64(True, "sequence")

    @covers("P06-C-01", "P06-C-09")
    @pytest.mark.parametrize(
        "ticks,scale,expected",
        [
            (123_456, 100_000, "1.23456"),
            (100_000, 100_000, "1.00000"),
            (99_900, 100_000, "0.99900"),
            (-123_456, 100_000, "-1.23456"),
            (151_230, 1_000, "151.230"),
            (7, 10_000_000, "0.0000007"),
            (12_345, 1, "12345"),
        ],
    )
    def test_display_price_is_exact_integer_formatting(self, ticks, scale, expected):
        assert display_price(ticks, scale) == expected

    @covers("P06-C-01")
    def test_display_price_never_loses_a_digit_through_a_float(self):
        # 1.23456 is not representable in binary; formatting via float would
        # produce 1.2345599... for some scales. This asserts the integer path.
        for ticks in range(123_450, 123_460):
            rendered = display_price(ticks, 100_000)
            whole, frac = rendered.split(".")
            assert int(whole) * 100_000 + int(frac) == ticks


class TestTimestamps:
    @covers("P06-D-11")
    def test_negative_event_time_is_rejected(self):
        with pytest.raises(ContractError) as exc:
            require_event_time_ns(-1)
        assert exc.value.code == "EVENT_TIME_OUT_OF_DOMAIN"

    @covers("P06-D-05")
    def test_event_time_beyond_int64_is_rejected(self):
        with pytest.raises(ContractError) as exc:
            require_event_time_ns(INT64_MAX + 1)
        assert exc.value.code == "INT64_OVERFLOW"

    @covers("P06-D-05")
    def test_int64_max_nanoseconds_is_inside_the_domain(self):
        assert require_event_time_ns(INT64_MAX) == INT64_MAX

    @covers("P06-D-04", "P06-B-03")
    def test_utc_date_is_independent_of_host_timezone(self, monkeypatch):
        # 2026-10-30T23:59:59.999999999Z is the 30th in UTC and the 31st in
        # UTC+07. The derived date must be the 30th regardless.
        ns = 1_793_404_799_999_999_999
        monkeypatch.setenv("TZ", "Asia/Ho_Chi_Minh")
        assert utc_date_from_ns(ns) == date(2026, 10, 30)
        monkeypatch.setenv("TZ", "UTC")
        assert utc_date_from_ns(ns) == date(2026, 10, 30)

    @covers("P06-D-04")
    def test_one_nanosecond_later_crosses_the_utc_date(self):
        ns = 1_793_404_799_999_999_999
        assert utc_date_from_ns(ns) == date(2026, 10, 30)
        assert utc_date_from_ns(ns + 1) == date(2026, 10, 31)

    @covers("P06-D-06")
    @pytest.mark.parametrize(
        "iso,expected",
        [
            ("2028-02-28", date(2028, 2, 28)),
            ("2028-02-29", date(2028, 2, 29)),
            ("2028-03-01", date(2028, 3, 1)),
        ],
    )
    def test_leap_day_boundaries(self, iso, expected):
        from datetime import datetime, timezone

        midnight = datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)
        ns = int(midnight.timestamp()) * 1_000_000_000
        assert utc_date_from_ns(ns) == expected
        assert utc_date_from_ns(ns + 86_399_999_999_999) == expected


class TestSymbols:
    @covers("P06-E-14")
    @pytest.mark.parametrize(
        "symbol,valid",
        [
            ("EURUSD", True),
            ("EUR.USD", True),
            ("EUR_USD", True),
            ("AB", False),
            ("eurusd", False),
            ("EURUSD ", False),
            ("EUR USD", False),
            ("EUR;DROP", False),
            ("E" * 17, False),
            ("EUR\x00USD", False),
            (None, False),
            (123, False),
        ],
    )
    def test_symbol_allowlist(self, symbol, valid):
        assert is_valid_symbol(symbol) is valid

    def test_invalid_symbol_stops_the_core_projection(self):
        with pytest.raises(ContractError) as exc:
            tick_core(symbol="eurusd")
        assert exc.value.code == "INVALID_SYMBOL"


class TestSchemaVersion:
    @covers("P06-B-11")
    def test_missing_source_schema_version_is_not_invented(self):
        with pytest.raises(ContractError) as exc:
            tick_core(source_schema_version="")
        assert exc.value.code == "MISSING_SCHEMA_VERSION"
