"""Schema pinning and drift refusal (group B, Q)."""

from __future__ import annotations

import pytest

from _coverage import covers
from md_analytics.errors import SchemaDriftError
from md_analytics.schema_preflight import (
    EXPECTED_CANONICAL_COLUMNS,
    EXPECTED_RAW_COLUMNS,
    ColumnSpec,
    expected_raw_digest,
    preflight_columns,
    preflight_engine,
    preflight_raw,
    schema_digest,
)


def observed(**changes: str) -> list[ColumnSpec]:
    """The pinned raw columns with some types replaced."""
    return [
        ColumnSpec(c.name, changes.get(c.name, c.type), c.position)
        for c in EXPECTED_RAW_COLUMNS
    ]


class TestCleanSchema:
    def test_the_pinned_schema_matches_itself(self):
        assert preflight_columns(observed()) == schema_digest(EXPECTED_RAW_COLUMNS)

    def test_column_order_does_not_change_the_digest_result(self):
        shuffled = list(reversed(observed()))
        assert preflight_columns(shuffled) == preflight_columns(observed())

    def test_whitespace_in_a_type_is_normalised(self):
        assert preflight_columns(observed(ingested_at="DateTime64(3,'UTC')"))

    def test_the_expected_digest_is_stable_and_hex(self):
        digest = expected_raw_digest()
        assert len(digest) == 64 and digest == expected_raw_digest()


class TestCriticalTypeDrift:
    @covers("P06-B-01")
    def test_event_time_as_float64_is_refused_and_says_why(self):
        with pytest.raises(SchemaDriftError) as exc:
            preflight_columns(observed(event_time_ns="Float64"))
        assert "nanosecond precision and ordering" in str(exc.value)

    @covers("P06-B-02")
    def test_price_ticks_as_decimal64_is_refused(self):
        with pytest.raises(SchemaDriftError) as exc:
            preflight_columns(observed(price_ticks="Decimal(18, 5)"))
        assert "fixed-point price exactness" in str(exc.value)

    @covers("P06-B-07")
    def test_sequence_turning_unsigned_is_refused(self):
        with pytest.raises(SchemaDriftError) as exc:
            preflight_columns(observed(sequence="UInt64"))
        assert "tie-break ordering and overflow behaviour" in str(exc.value)

    @covers("P06-B-05")
    def test_a_digest_column_of_the_wrong_width_is_refused(self):
        with pytest.raises(SchemaDriftError):
            preflight_columns(observed(payload_digest="FixedString(32)"))

    @covers("P06-B-03")
    def test_event_date_as_datetime_is_refused(self):
        with pytest.raises(SchemaDriftError) as exc:
            preflight_columns(observed(event_date="DateTime"))
        assert "UTC partitioning" in str(exc.value)

    @covers("P06-B-06")
    def test_lowcardinality_dropping_to_string_is_reported_as_drift(self):
        with pytest.raises(SchemaDriftError) as exc:
            preflight_columns(observed(symbol="String"))
        assert "type changes" in str(exc.value)

    @covers("P06-B-01", "P06-B-02")
    def test_every_difference_is_reported_at_once(self):
        with pytest.raises(SchemaDriftError) as exc:
            preflight_columns(observed(event_time_ns="Float64", price_ticks="Float64"))
        assert exc.value.context["type_changes"], "the context lists them for a migration"
        assert len(exc.value.context["type_changes"]) == 2


class TestMissingAndExtraColumns:
    @covers("P06-B-11")
    def test_a_missing_required_column_is_refused(self):
        columns = [c for c in observed() if c.name != "source_schema_version"]
        with pytest.raises(SchemaDriftError) as exc:
            preflight_columns(columns, require_exact_positions=False)
        assert exc.value.context["missing"] == ["source_schema_version"]

    @covers("P06-A-11", "P06-B-10")
    def test_an_unknown_new_column_stops_rather_than_being_guessed(self):
        columns = [*observed(), ColumnSpec("trade_size", "Int64", 19)]
        with pytest.raises(SchemaDriftError) as exc:
            preflight_columns(columns)
        assert exc.value.context["unexpected"] == ["trade_size"]

    @covers("P06-B-09")
    def test_a_reordered_column_is_caught_when_positions_are_pinned(self):
        columns = observed()
        columns[0] = ColumnSpec(columns[0].name, columns[0].type, 99)
        with pytest.raises(SchemaDriftError) as exc:
            preflight_columns(columns)
        assert exc.value.context["position_changes"]


class TestEngine:
    @covers("P06-B-04")
    def test_a_replacing_merge_tree_raw_table_is_refused(self):
        with pytest.raises(SchemaDriftError) as exc:
            preflight_engine(
                "ReplacingMergeTree", expected="MergeTree", table="md_ingest.market_events_raw"
            )
        assert exc.value.code == "ENGINE_MISMATCH"
        assert "collapsed behind P06" in str(exc.value)

    @covers("P06-B-04", "P06-B-09")
    def test_the_full_preflight_checks_engine_and_columns(self):
        digest = preflight_raw(columns=observed(), engine="MergeTree")
        assert len(digest) == 64
        with pytest.raises(SchemaDriftError):
            preflight_raw(columns=observed(), engine="ReplacingMergeTree")

    @covers("P06-B-13")
    def test_a_pinned_digest_mismatch_blocks_the_build(self):
        with pytest.raises(SchemaDriftError) as exc:
            preflight_raw(columns=observed(), engine="MergeTree", pinned_digest="0" * 64)
        assert exc.value.code == "SCHEMA_DIGEST_MISMATCH"

    @covers("P06-B-13")
    def test_a_matching_pinned_digest_passes(self):
        digest = schema_digest(EXPECTED_RAW_COLUMNS, engine="MergeTree")
        assert preflight_raw(columns=observed(), engine="MergeTree", pinned_digest=digest)


class TestCanonicalSchema:
    @covers("P06-Q-01")
    def test_the_canonical_table_has_no_nullable_price(self):
        price = next(c for c in EXPECTED_CANONICAL_COLUMNS if c.name == "price_ticks")
        assert price.type == "Int64", (
            "a canonical tick always has a price; nullability belongs to the raw "
            "table where a quote shares the same columns"
        )

    @covers("P06-Q-01")
    def test_the_canonical_table_carries_no_quote_columns(self):
        names = {c.name for c in EXPECTED_CANONICAL_COLUMNS}
        assert not names & {"bid_ticks", "ask_ticks", "event_kind"}

    @covers("P06-Q-07")
    def test_canonical_and_raw_digests_differ(self):
        from md_analytics.schema_preflight import expected_canonical_digest

        assert expected_canonical_digest() != expected_raw_digest()
