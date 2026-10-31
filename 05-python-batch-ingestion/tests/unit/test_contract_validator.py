"""Input contract validation: the golden corpus, the poison corpus, hostile bytes."""

from __future__ import annotations

import pytest

from p05_ingest.contract_validator import (
    MAX_EVENT_TIME_NS,
    ContractValidator,
    ValidationOk,
    ValidationReject,
)
from p05_ingest.model import ConsumedRecord, EventKind, TopicPartition
from p05_ingest.testkit import (
    QUOTES_TOPIC,
    TICKS_TOPIC,
    golden_corpus,
    make_quote,
    make_tick,
    poison_corpus,
)


def record(
    *,
    topic: str = TICKS_TOPIC,
    partition: int = 0,
    offset: int = 0,
    key: bytes | None = b"EURUSD",
    value: bytes | None = b"",
    headers=(),
) -> ConsumedRecord:
    return ConsumedRecord(
        tp=TopicPartition(topic, partition),
        offset=offset,
        key=key,
        value=value,
        headers=tuple(headers),
    )


def rec_for(event, *, topic=TICKS_TOPIC, key=None, **kwargs) -> ConsumedRecord:
    return record(
        topic=topic,
        key=key if key is not None else event.symbol.encode("ascii"),
        value=event.SerializeToString(),
        **kwargs,
    )


# ------------------------------------------------------------------- corpora


@pytest.mark.parametrize("entry", golden_corpus(), ids=lambda e: e.label)
def test_golden_corpus_is_accepted_field_for_field(validator, entry):
    """Every accepted entry must come back with its exact field values.

    Compares the whole projection, not just acceptance: a validator that
    accepted the record but zeroed a price would still pass a boolean check.
    """
    result = validator.validate(
        record(topic=entry.topic, key=entry.key, value=entry.value)
    )
    assert isinstance(result, ValidationOk), getattr(result, "code", result)
    assert result.projection == entry.expected, entry.note


@pytest.mark.parametrize("entry", poison_corpus(), ids=lambda e: e.label)
def test_poison_corpus_is_refused_with_the_expected_code(validator, entry):
    result = validator.validate(
        record(topic=entry.topic, key=entry.key, value=entry.value)
    )
    assert isinstance(result, ValidationReject), f"{entry.label} was accepted"
    assert result.code == entry.expect_reject, (
        f"{entry.label}: expected {entry.expect_reject}, got {result.code} "
        f"({result.detail})"
    )


# ---------------------------------------------------------------- transport


def test_unknown_topic_is_refused_before_any_decode(validator):
    result = validator.validate(record(topic="md.evil.v1", value=b"\xff\xff"))
    assert isinstance(result, ValidationReject)
    assert result.code == "TOPIC_NOT_ALLOWED"


def test_oversized_value_is_refused_without_decoding(validator, dev_config):
    """Spec NFR-03: the cap must trip before the parser allocates anything."""
    cap = dev_config.contracts.max_record_value_bytes
    result = validator.validate(record(value=b"\x00" * (cap + 1)))
    assert isinstance(result, ValidationReject)
    assert result.code == "VALUE_TOO_LARGE"
    assert str(cap) in result.detail


def test_value_exactly_at_the_cap_is_not_refused_for_size(validator, dev_config):
    """Off-by-one check: the cap is inclusive."""
    cap = dev_config.contracts.max_record_value_bytes
    result = validator.validate(record(value=b"\x00" * cap))
    assert isinstance(result, ValidationReject)
    assert result.code != "VALUE_TOO_LARGE"


@pytest.mark.parametrize(
    "value,code",
    [
        (None, "VALUE_NULL"),
        (b"", "VALUE_EMPTY"),
    ],
)
def test_missing_value_is_refused(validator, value, code):
    result = validator.validate(record(value=value))
    assert isinstance(result, ValidationReject)
    assert result.code == code


@pytest.mark.parametrize(
    "key,code",
    [
        (None, "KEY_NULL"),
        (b"", "KEY_EMPTY"),
        (b"X" * 33, "KEY_TOO_LARGE"),
    ],
)
def test_bad_key_is_refused(validator, key, code):
    result = validator.validate(
        record(key=key, value=make_tick().SerializeToString())
    )
    assert isinstance(result, ValidationReject)
    assert result.code == code


def test_key_must_equal_the_symbol_exactly(validator):
    """No case folding, no trimming -- the partition must match the symbol."""
    event = make_tick(symbol="EURUSD")
    for wrong_key in (b"eurusd", b" EURUSD", b"EURUSD ", b"EURUSD\x00", b"GBPUSD"):
        result = validator.validate(rec_for(event, key=wrong_key))
        assert isinstance(result, ValidationReject), wrong_key
        assert result.code == "KEY_SYMBOL_MISMATCH"


# ------------------------------------------------------------------ headers


def test_known_headers_are_accepted(validator, dev_config):
    event = make_tick()
    result = validator.validate(
        rec_for(
            event,
            headers=[
                ("md-schema", b"marketdata.v1"),
                ("md-dataset-id", event.dataset_id.encode()),
                ("md-run-id", b"p04-run-7"),
            ],
        )
    )
    assert isinstance(result, ValidationOk)


def test_unknown_header_is_refused_rather_than_ignored(validator):
    result = validator.validate(rec_for(make_tick(), headers=[("x-secret", b"1")]))
    assert isinstance(result, ValidationReject)
    assert result.code == "HEADER_UNKNOWN"


def test_duplicate_header_is_refused(validator):
    result = validator.validate(
        rec_for(make_tick(), headers=[("md-run-id", b"a"), ("md-run-id", b"b")])
    )
    assert isinstance(result, ValidationReject)
    assert result.code == "HEADER_DUPLICATE"


def test_oversized_headers_are_refused(validator, dev_config):
    big = b"x" * (dev_config.contracts.max_headers_total_bytes + 1)
    result = validator.validate(rec_for(make_tick(), headers=[("md-run-id", big)]))
    assert isinstance(result, ValidationReject)
    assert result.code == "HEADERS_TOO_LARGE"


def test_header_dataset_id_must_agree_with_the_payload(validator):
    event = make_tick(dataset_id="demo-a")
    result = validator.validate(
        rec_for(event, headers=[("md-dataset-id", b"demo-b")])
    )
    assert isinstance(result, ValidationReject)
    assert result.code == "HEADER_DATASET_ID_MISMATCH"


def test_header_descriptor_hash_mismatch_is_refused(validator):
    result = validator.validate(
        rec_for(make_tick(), headers=[("md-descriptor-sha256", b"0" * 64)])
    )
    assert isinstance(result, ValidationReject)
    assert result.code == "HEADER_DESCRIPTOR_MISMATCH"


def test_header_descriptor_hash_match_is_accepted(validator, dev_config):
    pinned = dev_config.contracts.protobuf_descriptor_sha256.encode("ascii")
    result = validator.validate(
        rec_for(make_tick(), headers=[("md-descriptor-sha256", pinned)])
    )
    assert isinstance(result, ValidationOk)


# -------------------------------------------------------------- hostile bytes


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        out.append(byte | (0x80 if n else 0))
        if not n:
            return bytes(out)


@pytest.mark.parametrize(
    "label,payload",
    [
        ("truncated-varint", b"\x08"),
        ("all-continuation-bytes", b"\xff" * 11),
        ("huge-length-delimiter", _varint((1 << 3) | 2) + _varint(10**9) + b"ab"),
        ("invalid-utf8-string", _varint((1 << 3) | 2) + _varint(2) + b"\xff\xfe"),
        ("group-wire-type", _varint((1 << 3) | 3)),
    ],
)
def test_malformed_protobuf_is_refused_without_raising(validator, label, payload):
    """Hostile bytes must produce a rejection, never an exception (spec 9.4)."""
    result = validator.validate(record(value=payload))
    assert isinstance(result, ValidationReject), label
    assert result.code in {"PROTO_DECODE_ERROR", "SCHEMA_UNKNOWN_FIELDS"}, label


def test_unknown_field_number_is_a_compatibility_stop(validator):
    """Spec 3.1: do not infer a new message type from unknown fields."""
    raw = make_tick().SerializeToString()
    raw += _varint((99 << 3) | 0) + _varint(42)
    result = validator.validate(record(value=raw))
    assert isinstance(result, ValidationReject)
    assert result.code == "SCHEMA_UNKNOWN_FIELDS"
    assert "99" in result.detail


def test_wrong_wire_type_for_a_known_field_is_caught(validator):
    """protobuf moves it to the unknown set and reports the field absent.

    Verified against the runtime: this does NOT raise. Without the
    unknown-field gate it would read as a silently missing sequence.
    """
    raw = _varint((3 << 3) | 2) + _varint(2) + b"ab"
    result = validator.validate(record(value=raw))
    assert isinstance(result, ValidationReject)
    assert result.code == "SCHEMA_UNKNOWN_FIELDS"


def test_fully_empty_message_is_caught_by_the_byte_check(validator):
    """An all-absent message serializes to zero bytes, so VALUE_EMPTY fires.

    Worth pinning: the zero-byte guard runs before the parser, so this case
    never reaches the presence checks at all.
    """
    from p05_ingest.proto_schema import MarketDataEvent

    assert MarketDataEvent().SerializeToString() == b""
    result = validator.validate(record(value=MarketDataEvent().SerializeToString()))
    assert isinstance(result, ValidationReject)
    assert result.code == "VALUE_EMPTY"


def test_message_with_bytes_but_no_required_fields_hits_presence_checks(validator):
    """Only ``source_schema_version`` set: parses, then fails on presence."""
    from p05_ingest.proto_schema import MarketDataEvent

    event = MarketDataEvent()
    event.source_schema_version = "marketdata.v1"
    raw = event.SerializeToString()
    assert raw, "the message must carry at least one byte to reach the parser"
    result = validator.validate(record(value=raw))
    assert isinstance(result, ValidationReject)
    assert result.code == "DATASET_ID_ABSENT"


# ----------------------------------------------------------- numeric domain


def test_event_time_at_the_clickhouse_date_ceiling_is_accepted(validator):
    result = validator.validate(rec_for(make_tick(event_time_ns=MAX_EVENT_TIME_NS)))
    assert isinstance(result, ValidationOk)


@pytest.mark.parametrize("event_time_ns", [-1, MAX_EVENT_TIME_NS + 1])
def test_event_time_outside_the_supported_range_is_refused(validator, event_time_ns):
    result = validator.validate(rec_for(make_tick(event_time_ns=event_time_ns)))
    assert isinstance(result, ValidationReject)
    assert result.code == "EVENT_TIME_OUT_OF_RANGE"


def test_dataset_id_byte_cap_counts_utf8_bytes(validator, dev_config):
    """A 100-character multi-byte id can exceed a 128-*byte* cap."""
    cap = dev_config.contracts.max_dataset_id_bytes
    over = "中" * ((cap // 3) + 1)  # 3 bytes each
    assert len(over) <= cap < len(over.encode("utf-8"))
    result = validator.validate(rec_for(make_tick(dataset_id=over)))
    assert isinstance(result, ValidationReject)
    assert result.code == "DATASET_ID_TOO_LARGE"


def test_no_float_appears_anywhere_in_an_accepted_projection(validator):
    """Spec FR-05/I-06: integers only, all the way through."""
    for entry in golden_corpus():
        result = validator.validate(
            record(topic=entry.topic, key=entry.key, value=entry.value)
        )
        assert isinstance(result, ValidationOk)
        p = result.projection
        for name in ("sequence", "event_time_ns", "price_scale"):
            assert isinstance(getattr(p, name), int)
            assert not isinstance(getattr(p, name), bool)
        for name in ("bid_ticks", "ask_ticks", "price_ticks"):
            value = getattr(p, name)
            assert value is None or isinstance(value, int)


# ------------------------------------------------------------ topic routing


@pytest.mark.parametrize(
    "topic,kind",
    [(TICKS_TOPIC, EventKind.TICK), (QUOTES_TOPIC, EventKind.QUOTE)],
)
def test_matching_topic_and_payload_is_accepted(validator, topic, kind):
    event = make_tick() if kind is EventKind.TICK else make_quote()
    result = validator.validate(rec_for(event, topic=topic))
    assert isinstance(result, ValidationOk)
    assert result.projection.kind is kind


def test_relaxed_validation_allows_anomalies_in_the_fault_lane(dev_config):
    """The fault lane keeps structure strict but relaxes market semantics.

    This is how the isolated anomaly corpus gets ingested without weakening the
    market-valid lane (spec I-12, section 3.1 fault manifest).
    """
    from dataclasses import replace

    contracts = replace(dev_config.contracts, strict_market_validation=False)
    project = replace(
        dev_config.project, mode="isolated_fault_test", run_id="fault-run"
    )
    relaxed = ContractValidator(contracts, project)

    crossed = make_quote(bid_ticks=109_400, ask_ticks=109_380)
    result = relaxed.validate(rec_for(crossed, topic=QUOTES_TOPIC))
    assert isinstance(result, ValidationOk), "crossed quote allowed in the fault lane"

    # Structure is still enforced: a mismatched topic is refused in both lanes.
    assert isinstance(
        relaxed.validate(rec_for(crossed, topic=TICKS_TOPIC)), ValidationReject
    )
    # And the strict lane still refuses the crossed quote.
    strict = ContractValidator(dev_config.contracts, dev_config.project)
    assert isinstance(
        strict.validate(rec_for(crossed, topic=QUOTES_TOPIC)), ValidationReject
    )


def test_schema_version_allowlist_is_enforced_when_configured(dev_config):
    from dataclasses import replace

    contracts = replace(
        dev_config.contracts, allowed_source_schema_versions=("marketdata.v1",)
    )
    v = ContractValidator(contracts, dev_config.project)
    assert isinstance(v.validate(rec_for(make_tick())), ValidationOk)
    result = v.validate(rec_for(make_tick(source_schema_version="marketdata.v2")))
    assert isinstance(result, ValidationReject)
    assert result.code == "SCHEMA_VERSION_NOT_ALLOWED"
