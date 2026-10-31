"""Property tests for the invariants of section 9.4.

Hypothesis is used for the integer and string domains specifically because the
interesting values are at the edges -- INT64_MIN, zero, 2^53, values whose varint
encoding changes length -- and an example-based test only covers the edges
someone thought of. These properties say what must hold for *every* value in
the domain.

The parse-never-invents property is the one worth stating plainly: for arbitrary
bytes, the decoder either produces an event whose fields all came from those
bytes, or it rejects them. It never returns a partially filled event.
"""

from __future__ import annotations

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import pytest

from md_contract.codec import (
    check_envelope,
    decode_payload,
    encode_event,
    parse_payload,
    pb_to_domain,
)
from md_contract.domain import (
    INT64_MAX,
    INT64_MIN,
    PAYLOAD_QUOTE,
    PAYLOAD_TICK,
    DomainEvent,
    core_projection_v1,
    projection_json,
)
from md_contract.errors import CodecError, ContractError, StrictJsonError
from md_contract.framing import BytesSource, FrameReader, encode_stream
from md_contract.strict_json import parse_sim_v1_line

# The full signed 64-bit domain, so boundary values are generated rather than
# hoped for.
int64 = st.integers(min_value=INT64_MIN, max_value=INT64_MAX)
symbols = st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.", min_size=3, max_size=16)
dataset_ids = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789-", min_size=1, max_size=64
)

SETTINGS = settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)


@st.composite
def events(draw: st.DrawFn) -> DomainEvent:
    is_quote = draw(st.booleans())
    common = dict(
        dataset_id=draw(dataset_ids),
        symbol=draw(symbols),
        sequence=draw(int64),
        event_time_ns=draw(int64),
        price_scale=draw(int64),
        source_schema_version="sim.v1",
    )
    if is_quote:
        return DomainEvent(
            **common,  # type: ignore[arg-type]
            payload_case=PAYLOAD_QUOTE,
            bid_ticks=draw(int64),
            ask_ticks=draw(int64),
        )
    return DomainEvent(
        **common,  # type: ignore[arg-type]
        payload_case=PAYLOAD_TICK,
        price_ticks=draw(int64),
    )


@SETTINGS
@given(events())
def test_round_trip_preserves_every_field_exactly(event: DomainEvent) -> None:
    decoded = decode_payload(encode_event(event))
    assert decoded == event
    assert core_projection_v1(decoded) == core_projection_v1(event)


@SETTINGS
@given(events())
def test_round_trip_is_idempotent(event: DomainEvent) -> None:
    once = encode_event(event)
    twice = encode_event(decode_payload(once))
    # Re-encoding a decoded event reproduces the same bytes *within one
    # runtime*. This is asserted here, where both sides are the same build; it
    # is deliberately NOT asserted across C++ and Python (ADR-005).
    assert once == twice


@SETTINGS
@given(events())
def test_presence_survives_the_round_trip(event: DomainEvent) -> None:
    decoded = decode_payload(encode_event(event))
    assert decoded.present_fields() == event.present_fields()


@SETTINGS
@given(
    st.lists(events(), min_size=1, max_size=20),
    st.integers(min_value=1, max_value=97),
)
def test_framing_reassembles_any_stream_at_any_chunk_size(
    event_list: list[DomainEvent], chunk: int
) -> None:
    payloads = [encode_event(event) for event in event_list]
    reader = FrameReader(
        BytesSource(encode_stream(payloads), chunk_size=chunk), deadline_s=None
    )
    assert list(reader) == payloads


@settings(max_examples=400, deadline=None)
@given(st.binary(min_size=0, max_size=256))
def test_decoding_arbitrary_bytes_either_works_or_rejects_cleanly(payload: bytes) -> None:
    """Never a crash, never a partially populated event (section 9.4).

    The guarantee being checked is that there is no third outcome. Either the
    bytes decode into an event every field of which came from them, or a typed
    rejection is raised. An unexpected exception type would mean some failure
    path is not classified, which is how a caller ends up with a half-built
    event.
    """
    try:
        msg = parse_payload(payload)
    except CodecError:
        return
    event = pb_to_domain(msg)
    try:
        check_envelope(event)
    except ContractError:
        return
    # It passed the envelope check, so it must be a complete event. Re-encoding
    # and decoding again must give the same thing back.
    assert event.payload_case in (PAYLOAD_QUOTE, PAYLOAD_TICK)
    assert None not in (
        event.dataset_id,
        event.symbol,
        event.sequence,
        event.event_time_ns,
        event.price_scale,
        event.source_schema_version,
    )
    assert decode_payload(encode_event(event)) == event


@settings(max_examples=300, deadline=None)
@given(st.binary(min_size=0, max_size=512))
def test_framing_arbitrary_bytes_never_raises_an_unclassified_error(data: bytes) -> None:
    reader = FrameReader(BytesSource(data), deadline_s=None)
    try:
        while reader.read_frame() is not None:
            pass
    except ContractError:
        pass  # a typed rejection is the expected outcome for garbage


@settings(max_examples=300, deadline=None)
@given(st.binary(min_size=0, max_size=512))
def test_strict_json_on_arbitrary_bytes_rejects_without_crashing(data: bytes) -> None:
    try:
        parse_sim_v1_line(data)
    except StrictJsonError:
        pass


@SETTINGS
@given(int64)
def test_every_int64_survives_a_price_field(value: int) -> None:
    event = DomainEvent(
        dataset_id="ds",
        symbol="EURUSD",
        sequence=1,
        event_time_ns=1,
        price_scale=100000,
        source_schema_version="sim.v1",
        payload_case=PAYLOAD_TICK,
        price_ticks=value,
    )
    assert decode_payload(encode_event(event)).price_ticks == value


@SETTINGS
@given(int64, int64)
def test_the_projection_is_byte_stable_for_equal_events(bid: int, ask: int) -> None:
    def make() -> DomainEvent:
        return DomainEvent(
            dataset_id="ds",
            symbol="EURUSD",
            sequence=1,
            event_time_ns=1,
            price_scale=100000,
            source_schema_version="sim.v1",
            payload_case=PAYLOAD_QUOTE,
            bid_ticks=bid,
            ask_ticks=ask,
        )

    assert projection_json(make()) == projection_json(make())


@SETTINGS
@given(events())
def test_a_source_jsonl_line_round_trips_through_the_strict_reader(
    event: DomainEvent,
) -> None:
    """Generate an event, render it as sim.v1, read it back, compare.

    This closes the loop on the mapping: the reader and the renderer have to
    agree for every value in the domain, including negative prices and the
    int64 extremes.
    """
    import json

    record: dict[str, object] = {
        "schema_version": "sim.v1",
        "dataset_id": event.dataset_id,
        "event_type": event.payload_case,
        "symbol": event.symbol,
        "sequence": event.sequence,
        "event_time_ns": event.event_time_ns,
        "price_scale": event.price_scale,
    }
    if event.payload_case == PAYLOAD_QUOTE:
        record["bid_ticks"] = event.bid_ticks
        record["ask_ticks"] = event.ask_ticks
    else:
        record["price_ticks"] = event.price_ticks

    line = json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(line) > 4096:
        pytest.skip("generated line exceeds the byte budget, which is its own test")
    assert parse_sim_v1_line(line) == event
