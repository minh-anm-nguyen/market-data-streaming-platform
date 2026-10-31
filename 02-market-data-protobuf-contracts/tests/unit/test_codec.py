"""Protobuf codec, presence and the envelope check: what-if group B, plus D18-D20.

The theme throughout: a successful parse is not an accepted event. proto3 has no
required fields, so the empty byte string parses into a valid message with every
field absent -- and that message must be rejected, not read as an event full of
zeros.
"""

from __future__ import annotations

import pytest

from md_contract.codec import (
    MAX_PAYLOAD_BYTES,
    check_envelope,
    decode_payload,
    domain_to_pb,
    encode_event,
    parse_payload,
    pb_to_domain,
    unknown_field_numbers,
)
from md_contract.domain import (
    INT64_MAX,
    INT64_MIN,
    PAYLOAD_NONE,
    PAYLOAD_QUOTE,
    PAYLOAD_TICK,
    PAYLOAD_UNKNOWN,
    DomainEvent,
    core_projection_v1,
    projection_json,
)
from md_contract.errors import CodecError, EnvelopeError, Reason


def sample_quote(**overrides: object) -> DomainEvent:
    base = dict(
        dataset_id="ds-test",
        symbol="EURUSD",
        sequence=42,
        event_time_ns=1793322000008200000,
        price_scale=100000,
        source_schema_version="sim.v1",
        payload_case=PAYLOAD_QUOTE,
        bid_ticks=108346,
        ask_ticks=108348,
    )
    base.update(overrides)
    return DomainEvent(**base)  # type: ignore[arg-type]


def sample_tick(**overrides: object) -> DomainEvent:
    base = dict(
        dataset_id="ds-test",
        symbol="EURUSD",
        sequence=42,
        event_time_ns=1793322000008200000,
        price_scale=100000,
        source_schema_version="sim.v1",
        payload_case=PAYLOAD_TICK,
        price_ticks=108347,
    )
    base.update(overrides)
    return DomainEvent(**base)  # type: ignore[arg-type]


def envelope_reason(event: DomainEvent) -> Reason:
    with pytest.raises(EnvelopeError) as excinfo:
        check_envelope(event)
    return excinfo.value.reason


# ---------------------------------------------------------------------------
# round trip
# ---------------------------------------------------------------------------


def test_quote_and_tick_round_trip_unchanged() -> None:
    for event in (sample_quote(), sample_tick()):
        decoded = decode_payload(encode_event(event))
        assert decoded == event
        assert core_projection_v1(decoded) == core_projection_v1(event)


@pytest.mark.cases("E03", "F09", "F12")
def test_int64_extremes_survive_without_wrapping() -> None:
    event = sample_quote(
        sequence=INT64_MAX,
        event_time_ns=INT64_MAX,
        bid_ticks=INT64_MIN,
        ask_ticks=INT64_MAX,
    )
    decoded = decode_payload(encode_event(event))
    assert decoded.sequence == INT64_MAX
    assert decoded.bid_ticks == INT64_MIN
    assert decoded.ask_ticks == INT64_MAX
    # Specifically not wrapped into the unsigned range, which is what a uint64
    # field would have done to a negative price (case A07's rationale).
    assert decoded.bid_ticks < 0

    # INT64_MAX must not be incremented anywhere in the codec.
    assert decoded.sequence == event.sequence


@pytest.mark.cases("C09")
def test_prices_above_2_to_the_53_are_exact() -> None:
    value = 2**53 + 1
    decoded = decode_payload(encode_event(sample_tick(price_ticks=value)))
    assert decoded.price_ticks == value


# ---------------------------------------------------------------------------
# presence: B01-B08
# ---------------------------------------------------------------------------


@pytest.mark.cases("B01")
def test_missing_sequence_is_absent_then_rejected() -> None:
    event = sample_quote(sequence=None)
    decoded = pb_to_domain(parse_payload(encode_event(event)))
    assert decoded.sequence is None
    assert envelope_reason(decoded) is Reason.MISSING_REQUIRED_SEMANTIC_FIELD


@pytest.mark.cases("B02")
def test_explicit_zero_sequence_differs_from_absent_on_the_wire() -> None:
    zero = encode_event(sample_quote(sequence=0))
    absent = encode_event(sample_quote(sequence=None))
    # Different bytes, because explicit presence means a zero is written out.
    assert zero != absent
    decoded = decode_payload(zero)
    assert decoded.sequence == 0
    assert "sequence" in decoded.present_fields()
    # Present-and-zero passes the *structural* check; whether 0 is an acceptable
    # sequence is a business decision made by mode, not by the codec.
    check_envelope(decoded)


@pytest.mark.cases("B03")
def test_explicit_zero_event_time_is_not_conflated_with_absent() -> None:
    decoded = decode_payload(encode_event(sample_quote(event_time_ns=0)))
    assert decoded.event_time_ns == 0
    assert "event_time_ns" in decoded.present_fields()

    absent = pb_to_domain(parse_payload(encode_event(sample_quote(event_time_ns=None))))
    assert absent.event_time_ns is None
    assert "event_time_ns" not in absent.present_fields()


@pytest.mark.cases("B04")
def test_explicit_zero_price_scale_is_present() -> None:
    decoded = decode_payload(encode_event(sample_quote(price_scale=0)))
    assert decoded.price_scale == 0
    # A zero scale makes the decimal price undefined; display_price refuses to
    # divide rather than returning something made up.
    assert decoded.display_price() == {}


@pytest.mark.cases("B05")
def test_empty_dataset_id_is_present_but_empty() -> None:
    decoded = decode_payload(encode_event(sample_quote(dataset_id="")))
    assert decoded.dataset_id == ""
    assert "dataset_id" in decoded.present_fields()

    absent = pb_to_domain(parse_payload(encode_event(sample_quote(dataset_id=None))))
    assert absent.dataset_id is None


@pytest.mark.cases("B06", "D07")
def test_empty_message_is_not_an_event() -> None:
    # The empty byte string is a *valid* MarketDataEvent as far as Protobuf is
    # concerned. Everything that makes it not an event is checked afterwards.
    parsed = pb_to_domain(parse_payload(b""))
    assert parsed.payload_case == PAYLOAD_NONE
    assert parsed.present_fields() == ()
    assert envelope_reason(parsed) is Reason.MISSING_REQUIRED_SEMANTIC_FIELD

    # A full envelope with no payload fails on the payload, specifically.
    no_payload = DomainEvent(
        dataset_id="ds-test",
        symbol="EURUSD",
        sequence=1,
        event_time_ns=1,
        price_scale=100000,
        source_schema_version="sim.v1",
        payload_case=PAYLOAD_NONE,
    )
    assert envelope_reason(no_payload) is Reason.UNSUPPORTED_EVENT_KIND

    # And an all-absent event cannot even be framed: MDP1 forbids L=0.
    with pytest.raises(CodecError) as excinfo:
        encode_event(DomainEvent())
    assert excinfo.value.reason is Reason.UNSUPPORTED_EVENT_KIND


@pytest.mark.cases("B07")
def test_quote_without_prices_keeps_its_case_and_is_rejected() -> None:
    event = sample_quote(bid_ticks=None, ask_ticks=None)
    decoded = pb_to_domain(parse_payload(encode_event(event)))
    # Touching the submessage set the oneof, so the case survives even with no
    # prices -- which is why the envelope check has to look at the subfields.
    assert decoded.payload_case == PAYLOAD_QUOTE
    assert decoded.bid_ticks is None
    assert envelope_reason(decoded) is Reason.MISSING_REQUIRED_SEMANTIC_FIELD

    # One price present and one absent is still a rejection.
    half = pb_to_domain(parse_payload(encode_event(sample_quote(ask_ticks=None))))
    assert half.bid_ticks == 108346
    assert envelope_reason(half) is Reason.MISSING_REQUIRED_SEMANTIC_FIELD


@pytest.mark.cases("B08")
def test_tick_without_price_is_rejected() -> None:
    decoded = pb_to_domain(parse_payload(encode_event(sample_tick(price_ticks=None))))
    assert decoded.payload_case == PAYLOAD_TICK
    assert envelope_reason(decoded) is Reason.MISSING_REQUIRED_SEMANTIC_FIELD


# ---------------------------------------------------------------------------
# oneof and duplicate wire tags: B09-B12
# ---------------------------------------------------------------------------


@pytest.mark.cases("B09")
def test_wire_carrying_both_quote_and_tick_is_resolved_by_the_runtime() -> None:
    """A crafted payload with two oneof members present.

    Protobuf's documented behaviour is last-one-wins, and ``WhichOneof`` after
    parsing therefore does **not** prove the wire held exactly one member
    (section 5.5). This test pins down what this runtime actually does, so the
    behaviour is documented rather than assumed, and records that detecting the
    duplicate would need a raw-wire audit this MVP does not perform.
    """
    quote_only = encode_event(sample_quote())
    tick_payload = domain_to_pb(sample_tick()).tick.SerializeToString()
    # Append field 11 (tick), wire type 2: tag 11*8+2 == 90, one varint byte.
    crafted = quote_only + bytes([90, len(tick_payload)]) + tick_payload

    decoded = decode_payload(crafted)
    assert decoded.payload_case == PAYLOAD_TICK, "last member on the wire wins"
    assert decoded.bid_ticks is None, "the earlier quote member is discarded"
    # The reverse order yields the other winner, confirming it is position and
    # not a type preference deciding this.
    reversed_craft = (
        encode_event(sample_tick())
        + bytes([82, len(domain_to_pb(sample_quote()).quote.SerializeToString())])
        + domain_to_pb(sample_quote()).quote.SerializeToString()
    )
    assert decode_payload(reversed_craft).payload_case == PAYLOAD_QUOTE


@pytest.mark.cases("B10")
def test_duplicate_singular_field_on_the_wire_is_last_wins() -> None:
    # sequence (field 3, varint) twice: 0x18 <value>. Documented, not relied on.
    crafted = encode_event(sample_quote()) + bytes([0x18, 99])
    decoded = decode_payload(crafted)
    assert decoded.sequence == 99, "the later value replaces the earlier one"


@pytest.mark.cases("B12", "E13")
def test_parsing_never_reuses_a_message_so_fields_cannot_leak() -> None:
    # Decode a tick, then a quote, then a tick again. If the codec reused and
    # merely Clear()-ed a message -- or worse, did not clear it -- a stale oneof
    # or a stale price from the previous event could survive into the next.
    tick = decode_payload(encode_event(sample_tick()))
    quote = decode_payload(encode_event(sample_quote()))
    tick_again = decode_payload(encode_event(sample_tick()))

    assert tick.payload_case == PAYLOAD_TICK and tick.bid_ticks is None
    assert quote.payload_case == PAYLOAD_QUOTE and quote.price_ticks is None
    assert tick_again == tick

    # A failed parse must not leave a usable half-populated object behind.
    with pytest.raises(CodecError):
        decode_payload(b"\x00")
    assert decode_payload(encode_event(sample_quote())) == sample_quote()


# ---------------------------------------------------------------------------
# unknown fields and unsupported kinds: D18-D20, G01
# ---------------------------------------------------------------------------


@pytest.mark.cases("G01", "G09")
def test_unknown_field_outside_the_payload_range_is_retained_and_ignored() -> None:
    # field 20 (v1.1's feed_label), wire type 2. Tag 20*8+2 == 162, which needs
    # a two-byte varint: 0xa2 0x01.
    payload = encode_event(sample_quote()) + bytes([0xA2, 0x01, 3]) + b"abc"
    msg = parse_payload(payload)
    assert unknown_field_numbers(msg) == (20,)

    decoded = pb_to_domain(msg)
    check_envelope(decoded)
    # The v1 core projection is untouched by the added field, which is exactly
    # the promise an additive wire-safe change makes (case A03).
    assert projection_json(decoded) == projection_json(sample_quote())


@pytest.mark.cases("D20", "G06", "A11")
def test_unknown_payload_variant_fails_closed() -> None:
    # A v2 writer's depth_update is field 12: tag 12*8+2 == 98, one byte.
    envelope = sample_quote(payload_case=PAYLOAD_NONE, bid_ticks=None, ask_ticks=None)
    inner = bytes([0x08, 3])  # level = 3
    payload = encode_event(envelope) + bytes([98, len(inner)]) + inner

    decoded = pb_to_domain(parse_payload(payload))
    # Distinguished from "no payload at all": the field number falls in the
    # reserved payload range, so the reader knows it is a newer variant.
    assert decoded.payload_case == PAYLOAD_UNKNOWN
    assert envelope_reason(decoded) is Reason.UNSUPPORTED_EVENT_KIND
    assert decoded.payload_case != PAYLOAD_QUOTE
    assert decoded.payload_case != PAYLOAD_TICK


@pytest.mark.cases("D13", "D14", "D19")
def test_undecodable_bytes_are_decode_errors() -> None:
    corpus = {
        "field number 0": b"\x00",
        "truncated varint": b"\x18\x80",
        "wire type 6 (invalid)": b"\x1e\x01",
        "wire type 7 (invalid)": b"\x1f\x01",
        "length-delimited overrunning the buffer": b"\x52\x7f\x01",
        "garbage": b"\xff" * 32,
    }
    for label, payload in corpus.items():
        with pytest.raises(CodecError) as excinfo:
            decode_payload(payload)
        assert excinfo.value.reason is Reason.DECODE_ERROR, label


@pytest.mark.cases("D18", "G07")
def test_payload_over_the_budget_is_refused_before_parsing() -> None:
    with pytest.raises(CodecError) as excinfo:
        parse_payload(b"\x00" * (MAX_PAYLOAD_BYTES + 1))
    assert excinfo.value.reason is Reason.PAYLOAD_TOO_LARGE

    # Unknown-field padding counts toward the same budget: a newer writer cannot
    # buy extra room by putting the bulk in fields this build does not know.
    padding = bytes([0xA2, 0x01]) + (MAX_PAYLOAD_BYTES).to_bytes(2, "little")
    oversize = encode_event(sample_quote()) + padding + b"x" * MAX_PAYLOAD_BYTES
    with pytest.raises(CodecError) as excinfo:
        decode_payload(oversize)
    assert excinfo.value.reason is Reason.PAYLOAD_TOO_LARGE


def test_encoder_refuses_a_payload_over_the_budget() -> None:
    with pytest.raises(CodecError) as excinfo:
        encode_event(sample_quote(dataset_id="d" * 5000))
    assert excinfo.value.reason is Reason.PAYLOAD_TOO_LARGE


@pytest.mark.cases("E04")
def test_semantic_equality_does_not_require_identical_bytes() -> None:
    """Two different encodings of one event must both be accepted.

    Protobuf does not promise canonical output, so field order on the wire is
    not fixed. Appending the fields in a different order produces different
    bytes for the same event, and the oracle has to treat them as equal --
    which is why no test in this suite compares hashes (ADR-005, case K07).
    """
    canonical = encode_event(sample_quote())
    # Re-emit the same quote with the payload submessage written before the
    # envelope scalars, by concatenating in the other order.
    pb = domain_to_pb(sample_quote())
    quote_bytes = pb.quote.SerializeToString()
    envelope_only = encode_event(
        sample_quote(payload_case=PAYLOAD_NONE, bid_ticks=None, ask_ticks=None)
    )
    reordered = bytes([82, len(quote_bytes)]) + quote_bytes + envelope_only

    assert reordered != canonical, "the two encodings should differ byte-wise"
    assert decode_payload(reordered) == decode_payload(canonical)
    assert projection_json(decode_payload(reordered)) == projection_json(sample_quote())
