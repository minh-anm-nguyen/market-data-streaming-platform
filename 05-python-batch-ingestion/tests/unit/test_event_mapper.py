"""Row mapping: column order, UTC dates, presence passthrough, byte estimate."""

from __future__ import annotations

import datetime as dt

import pytest

from p05_ingest.event_mapper import EventMapper, estimate_row_bytes
from p05_ingest.model import (
    CH_INSERT_COLUMNS,
    NS_PER_DAY,
    EventKind,
    LogicalProjection,
)


@pytest.fixture
def mapper() -> EventMapper:
    return EventMapper(
        ingest_run_id="run-7", kafka_cluster_id="cluster-a", topic_generation="gen-b"
    )


def projection(**kwargs) -> LogicalProjection:
    base = {
        "dataset_id": "demo",
        "symbol": "EURUSD",
        "sequence": 11,
        "event_time_ns": 1_704_171_845_123_456_789,
        "price_scale": 100_000,
        "kind": EventKind.TICK,
        "source_schema_version": "marketdata.v1",
        "price_ticks": 109_385,
    }
    base.update(kwargs)
    return LogicalProjection(**base)


def test_row_carries_run_and_transport_identity(mapper):
    row = mapper.to_row(
        projection(), topic="md.ticks.v1", partition=3, offset=99, raw_value_len=40
    )
    assert row.ingest_run_id == "run-7"
    assert row.coordinate.kafka_cluster_id == "cluster-a"
    assert row.coordinate.topic_generation == "gen-b"
    assert row.coordinate.topic == "md.ticks.v1"
    assert row.coordinate.partition == 3
    assert row.coordinate.offset == 99
    assert row.identity == projection().identity


def test_digest_is_attached_and_matches_the_projection(mapper):
    from p05_ingest.canonical_digest import payload_digest

    p = projection()
    row = mapper.to_row(
        p, topic="md.ticks.v1", partition=0, offset=0, raw_value_len=40
    )
    assert row.payload_digest == payload_digest(p)


def test_column_tuple_matches_the_declared_column_order(mapper):
    """Spec 6.1: never rely on table column order."""
    row = mapper.to_row(
        projection(), topic="md.ticks.v1", partition=2, offset=5, raw_value_len=40
    )
    values = dict(zip(CH_INSERT_COLUMNS, row.to_ch_tuple(), strict=True))

    assert values["dataset_id"] == "demo"
    assert values["symbol"] == "EURUSD"
    assert values["sequence"] == 11
    assert values["event_kind"] == "tick"
    assert values["event_time_ns"] == 1_704_171_845_123_456_789
    assert values["price_scale"] == 100_000
    assert values["price_ticks"] == 109_385
    assert values["bid_ticks"] is None
    assert values["ask_ticks"] is None
    assert values["payload_digest"] == row.payload_digest
    assert values["kafka_topic"] == "md.ticks.v1"
    assert values["kafka_partition"] == 2
    assert values["kafka_offset"] == 5
    assert values["topic_generation"] == "gen-b"
    assert values["ingest_run_id"] == "run-7"


def test_ingested_at_is_not_a_mapped_column():
    """It differs between retries of one immutable batch (spec 6.1)."""
    assert "ingested_at" not in CH_INSERT_COLUMNS


def test_absent_optionals_map_to_null_not_zero(mapper):
    """A quote must not acquire a price_ticks of 0."""
    row = mapper.to_row(
        projection(kind=EventKind.QUOTE, price_ticks=None, bid_ticks=1, ask_ticks=2),
        topic="md.quotes.v1",
        partition=0,
        offset=0,
        raw_value_len=40,
    )
    values = dict(zip(CH_INSERT_COLUMNS, row.to_ch_tuple(), strict=True))
    assert values["price_ticks"] is None
    assert values["bid_ticks"] == 1
    assert values["ask_ticks"] == 2


def test_explicit_zero_maps_to_zero_not_null(mapper):
    row = mapper.to_row(
        projection(kind=EventKind.QUOTE, price_ticks=None, bid_ticks=0, ask_ticks=0),
        topic="md.quotes.v1",
        partition=0,
        offset=0,
        raw_value_len=40,
    )
    values = dict(zip(CH_INSERT_COLUMNS, row.to_ch_tuple(), strict=True))
    assert values["bid_ticks"] == 0
    assert values["ask_ticks"] == 0


# -------------------------------------------------------------- event_date


@pytest.mark.parametrize(
    "event_time_ns,expected",
    [
        (0, dt.date(1970, 1, 1)),
        (NS_PER_DAY - 1, dt.date(1970, 1, 1)),
        (NS_PER_DAY, dt.date(1970, 1, 2)),
        # 2024-01-01T23:59:59.999999999Z -- the UTC day boundary (E2E-12).
        (1_704_153_599_999_999_999, dt.date(2024, 1, 1)),
        (1_704_153_600_000_000_000, dt.date(2024, 1, 2)),
    ],
)
def test_event_date_is_derived_in_utc(event_time_ns, expected):
    assert projection(event_time_ns=event_time_ns).event_date == expected


def test_event_date_ignores_the_host_timezone(monkeypatch):
    """Spec E2E-12: a UTC+07 host must not shift the partition key."""
    import os
    import time

    p = projection(event_time_ns=1_704_153_599_999_999_999)
    before = p.event_date
    monkeypatch.setenv("TZ", "Asia/Bangkok")
    if hasattr(time, "tzset"):
        time.tzset()
    try:
        assert p.event_date == before == dt.date(2024, 1, 1)
    finally:
        monkeypatch.delenv("TZ", raising=False)
        if hasattr(time, "tzset"):
            time.tzset()
    assert os.environ.get("TZ") is None


# ------------------------------------------------------------ byte estimate


def test_estimate_grows_with_the_payload_size():
    small = estimate_row_bytes(projection(), 40)
    large = estimate_row_bytes(projection(), 4096)
    assert large > small
    assert large - small == 4096 - 40


def test_estimate_includes_the_decoded_text():
    short_id = estimate_row_bytes(projection(dataset_id="a"), 40)
    long_id = estimate_row_bytes(projection(dataset_id="a" * 128), 40)
    assert long_id > short_id


def test_estimate_counts_utf8_bytes_for_multibyte_ids():
    ascii_id = estimate_row_bytes(projection(dataset_id="abc"), 40)
    wide_id = estimate_row_bytes(projection(dataset_id="中文字"), 40)
    assert wide_id > ascii_id, "three characters, nine bytes"


def test_estimate_is_positive_and_over_counts_the_wire_size():
    """Over-counting is the safe direction for a cap."""
    estimate = estimate_row_bytes(projection(), 40)
    assert estimate > 40


def test_row_records_the_charged_bytes(mapper):
    row = mapper.to_row(
        projection(), topic="md.ticks.v1", partition=0, offset=0, raw_value_len=40
    )
    assert row.accounted_bytes == estimate_row_bytes(projection(), 40)
