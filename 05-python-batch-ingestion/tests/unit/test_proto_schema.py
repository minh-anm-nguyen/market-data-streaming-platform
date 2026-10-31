"""The descriptor pin and raw-value decoding behaviour.

Several of these assert on *protobuf runtime behaviour* rather than on our own
code. That is deliberate: the validator design depends on facts like "a wrong
wire type does not raise", and if a runtime upgrade changed that, the silent
corruption channel would reopen. These tests fail loudly if it does.
"""

from __future__ import annotations

import pytest

from p05_ingest.errors import ContractViolation, PreflightError
from p05_ingest.proto_schema import (
    MarketDataEvent,
    decode_value,
    descriptor_sha256,
    schema_package,
    unknown_field_numbers,
    verify_descriptor_pin,
)
from p05_ingest.testkit import make_quote, make_tick


def varint(n: int) -> bytes:
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        out.append(byte | (0x80 if n else 0))
        if not n:
            return bytes(out)


# ------------------------------------------------------------------- the pin


def test_descriptor_hash_is_stable_within_a_build():
    assert descriptor_sha256() == descriptor_sha256()
    assert len(descriptor_sha256()) == 64


def test_shipped_config_pins_the_compiled_descriptor(dev_config):
    """The config and the compiled schema must agree, or startup must fail."""
    assert dev_config.contracts.protobuf_descriptor_sha256 == descriptor_sha256()
    verify_descriptor_pin(
        dev_config.contracts.protobuf_descriptor_sha256,
        expected_package=dev_config.contracts.protobuf_package,
    )


def test_wrong_hash_is_refused_with_both_values_shown():
    with pytest.raises(PreflightError) as excinfo:
        verify_descriptor_pin("0" * 64, expected_package="marketdata.v1")
    message = str(excinfo.value)
    assert "0" * 64 in message
    assert descriptor_sha256() in message
    assert "Do not relax this check" in message


def test_wrong_package_is_refused():
    with pytest.raises(PreflightError, match="package mismatch"):
        verify_descriptor_pin(descriptor_sha256(), expected_package="other.v1")


def test_pin_accepts_an_uppercase_hash():
    verify_descriptor_pin(
        descriptor_sha256().upper(), expected_package=schema_package()
    )


def test_schema_package_is_the_contract_package():
    assert schema_package() == "marketdata.v1"


# ------------------------------------------------------------ explicit presence


def test_proto3_optional_distinguishes_zero_from_absent():
    """The behaviour the whole validator depends on."""
    event = MarketDataEvent()
    assert not event.HasField("sequence")
    event.sequence = 0
    assert event.HasField("sequence")
    assert event.sequence == 0


def test_oneof_reports_the_selected_variant():
    assert make_tick().WhichOneof("payload") == "tick"
    assert make_quote().WhichOneof("payload") == "quote"
    assert MarketDataEvent().WhichOneof("payload") is None


def test_oneof_is_set_even_with_an_empty_submessage():
    """A tick with no price still routes as a tick, then fails presence."""
    from p05_ingest.model import EventKind
    from p05_ingest.testkit import make_event

    event = make_event(kind=EventKind.TICK)
    assert event.WhichOneof("payload") == "tick"
    assert not event.tick.HasField("price_ticks")


def test_int64_boundaries_round_trip():
    from p05_ingest.model import INT64_MAX, INT64_MIN

    for value in (INT64_MIN, -1, 0, INT64_MAX):
        event = MarketDataEvent()
        event.sequence = value
        decoded = decode_value(event.SerializeToString())
        assert decoded.sequence == value


# --------------------------------------------------------------- raw decoding


def test_no_framing_prefix_is_stripped():
    """Spec 3.1: the value is bare protobuf, no MDP1 and no length prefix.

    A decoder that skipped four leading bytes would misparse a valid record,
    so feed it a real one and check every field survives.
    """
    event = make_tick(sequence=123, price_ticks=456)
    decoded = decode_value(event.SerializeToString())
    assert decoded.sequence == 123
    assert decoded.tick.price_ticks == 456


def test_prefixed_value_is_rejected_rather_than_silently_trimmed():
    raw = b"MDP1" + make_tick().SerializeToString()
    try:
        decoded = decode_value(raw)
    except ContractViolation as exc:
        assert exc.code == "PROTO_DECODE_ERROR"
    else:
        # If it happens to parse, the prefix must have become unknown fields
        # rather than being quietly discarded.
        assert unknown_field_numbers(decoded), "a prefix must not vanish silently"


@pytest.mark.parametrize(
    "label,payload",
    [
        ("truncated", b"\x08"),
        ("continuation-run", b"\xff" * 11),
        ("huge-length", varint((1 << 3) | 2) + varint(10**9) + b"ab"),
        ("bad-utf8", varint((1 << 3) | 2) + varint(2) + b"\xff\xfe"),
    ],
)
def test_malformed_input_raises_contract_violation(label, payload):
    with pytest.raises(ContractViolation) as excinfo:
        decode_value(payload)
    assert excinfo.value.code == "PROTO_DECODE_ERROR", label


def test_empty_input_parses_to_an_all_absent_message():
    decoded = decode_value(b"")
    assert not decoded.HasField("dataset_id")
    assert decoded.WhichOneof("payload") is None


# ------------------------------------------------------------ unknown fields


def test_unknown_field_numbers_are_reported():
    raw = make_tick().SerializeToString() + varint((99 << 3) | 0) + varint(42)
    assert unknown_field_numbers(decode_value(raw)) == (99,)


def test_no_unknown_fields_for_a_matching_schema():
    assert unknown_field_numbers(decode_value(make_tick().SerializeToString())) == ()


def test_wrong_wire_type_becomes_an_unknown_field_and_reads_absent():
    """Pins the runtime behaviour the validator compensates for.

    protobuf does not raise here. The field moves to the unknown set and
    ``HasField`` reports absent, which without the unknown-field gate would
    look like an innocently missing value.
    """
    raw = varint((3 << 3) | 2) + varint(2) + b"ab"  # field 3 is a varint
    decoded = decode_value(raw)
    assert not decoded.HasField("sequence")
    assert 3 in unknown_field_numbers(decoded)


def test_multiple_unknown_fields_are_sorted_and_deduplicated():
    raw = make_tick().SerializeToString()
    raw += varint((99 << 3) | 0) + varint(1)
    raw += varint((50 << 3) | 0) + varint(2)
    raw += varint((99 << 3) | 0) + varint(3)
    assert unknown_field_numbers(decode_value(raw)) == (50, 99)


def test_repeated_tag_for_a_singular_field_takes_the_last_value():
    """Legal protobuf, and a plausible tamper. Last-wins is the runtime rule.

    Not an error, so the validator cannot reject it on structure alone -- which
    is exactly why the semantic digest is computed from the decoded projection
    rather than from the wire bytes.
    """
    raw = varint((3 << 3) | 0) + varint(1) + varint((3 << 3) | 0) + varint(2)
    assert decode_value(raw).sequence == 2
