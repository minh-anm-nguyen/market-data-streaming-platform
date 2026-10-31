"""P02's codec and P03's framer/validator/tracker, as the components under test."""

from __future__ import annotations

from typing import Any

import pytest
from google.protobuf import descriptor_pb2 as dpb

from p07.contracts import MAX_EVENT_VALUE_BYTES, EventKind, SemanticCore
from p07.dataplane.adapter import (
    Adapter,
    AdapterOutcome,
    FrameState,
    SymbolLimits,
    chunked,
    limits_from_sealed,
)
from p07.dataplane.codec import (
    DEFAULT_CODEC,
    MDP1_MAGIC,
    Codec,
    CodecError,
    build_file_descriptor,
    frame_mdp1,
    looks_like_mdp1,
    strip_mdp1_if_present,
)

LIMITS = {
    "EURUSD": SymbolLimits(
        price_scale=100000,
        min_price=100000,
        max_price=120000,
        min_spread=1,
        max_spread=5,
        max_step=8,
    )
}


def tick(sequence: int = 1, *, price: int | None = 108000, time_ns: int = 1_000) -> SemanticCore:
    return SemanticCore(
        dataset_id="ds",
        symbol="EURUSD",
        sequence=sequence,
        event_time_ns=time_ns,
        price_scale=100000,
        event_kind=EventKind.TICK,
        source_schema_version="sim.v1",
        bid_ticks=None,
        ask_ticks=None,
        price_ticks=price,
    )


def quote(
    sequence: int = 1, *, bid: int | None = 108000, ask: int | None = 108003, time_ns: int = 1_000
) -> SemanticCore:
    return SemanticCore(
        dataset_id="ds",
        symbol="EURUSD",
        sequence=sequence,
        event_time_ns=time_ns,
        price_scale=100000,
        event_kind=EventKind.QUOTE,
        source_schema_version="sim.v1",
        bid_ticks=bid,
        ask_ticks=ask,
        price_ticks=None,
    )


def drive(
    events: list[SemanticCore], *, chunk_sizes: tuple[int, ...] = (4096,), **kwargs: Any
) -> tuple[Adapter, list[SemanticCore], list[AdapterOutcome]]:
    published: list[SemanticCore] = []
    outcomes: list[AdapterOutcome] = []
    adapter = Adapter(
        dataset_id="ds",
        limits=LIMITS,
        sink=published.append,
        on_outcome=outcomes.append,
        **kwargs,
    )
    for core in events:
        for chunk in chunked(frame_mdp1(DEFAULT_CODEC.encode(core)), chunk_sizes):
            adapter.feed(chunk)
    adapter.close()
    return adapter, published, outcomes


# -- codec ---------------------------------------------------------------


def test_round_trip_preserves_every_field_and_its_presence() -> None:
    for core in (tick(), quote(), tick(price=0), quote(ask=None)):
        assert DEFAULT_CODEC.decode(DEFAULT_CODEC.encode(core)) == core


@pytest.mark.whatif("P07-G03-01")
def test_a_zero_value_is_distinguishable_from_an_absent_field() -> None:
    present = DEFAULT_CODEC.encode(tick(price=0))
    absent = DEFAULT_CODEC.encode(tick(price=None))
    assert present != absent
    assert DEFAULT_CODEC.decode(present).price_ticks == 0
    assert DEFAULT_CODEC.decode(absent).price_ticks is None


@pytest.mark.whatif("P07-G03-02")
def test_a_quote_missing_ask_decodes_as_absent_not_zero() -> None:
    decoded = DEFAULT_CODEC.decode(DEFAULT_CODEC.encode(quote(ask=None)))
    assert decoded.ask_ticks is None
    assert decoded.bid_ticks == 108000


@pytest.mark.whatif("P07-G03-05")
def test_a_truncated_payload_decodes_to_nothing() -> None:
    payload = DEFAULT_CODEC.encode(quote())
    with pytest.raises(CodecError) as exc:
        DEFAULT_CODEC.decode(payload[: len(payload) // 2])
    assert exc.value.reason in {"PROTO_DECODE_ERROR", "STRUCTURAL_REJECT"}


def test_a_message_missing_an_identity_field_is_a_structural_reject() -> None:
    message = DEFAULT_CODEC.message_class()
    message.symbol = "EURUSD"
    with pytest.raises(CodecError) as exc:
        DEFAULT_CODEC.decode(message.SerializeToString())
    assert exc.value.reason == "STRUCTURAL_REJECT"


def test_a_message_with_no_payload_oneof_is_a_structural_reject() -> None:
    core = tick()
    message = DEFAULT_CODEC.message_class()
    for name in ("dataset_id", "symbol", "source_schema_version"):
        setattr(message, name, getattr(core, name))
    for name in ("sequence", "event_time_ns", "price_scale"):
        setattr(message, name, getattr(core, name))
    with pytest.raises(CodecError, match="no payload oneof"):
        DEFAULT_CODEC.decode(message.SerializeToString())


@pytest.mark.whatif("P07-G03-13")
def test_an_oversize_payload_is_refused_before_parsing() -> None:
    with pytest.raises(CodecError) as exc:
        DEFAULT_CODEC.decode(b"\x00" * (MAX_EVENT_VALUE_BYTES + 1))
    assert exc.value.reason == "PAYLOAD_TOO_LARGE"


@pytest.mark.whatif("P07-G03-04", "P07-G03-14")
def test_an_unknown_field_does_not_change_the_semantic_core() -> None:
    """Version skew must not look like a conflict.

    A newer encoder adds a field this decoder has never heard of. The decoded
    core must be identical, which is why the digest is computed from the core
    and never from the Protobuf bytes.
    """
    skewed = Codec(
        build_file_descriptor(
            extra_event_field=(99, "vendor_tag", dpb.FieldDescriptorProto.TYPE_STRING)
        )
    )
    core = quote()
    payload = skewed.encode(core, extra={"vendor_tag": "from-the-future"})
    assert DEFAULT_CODEC.decode(payload) == core
    assert skewed.descriptor_sha256 != DEFAULT_CODEC.descriptor_sha256


@pytest.mark.whatif("P07-G03-11")
def test_a_retyped_field_produces_a_different_descriptor_hash() -> None:
    retyped = Codec(build_file_descriptor(sequence_type=dpb.FieldDescriptorProto.TYPE_STRING))
    assert retyped.descriptor_sha256 != DEFAULT_CODEC.descriptor_sha256


@pytest.mark.whatif("P07-G03-12")
def test_mdp1_framing_is_detectable_and_never_silently_stripped() -> None:
    """A Kafka value with a TCP prefix must be *detected*, not parsed around."""
    payload = DEFAULT_CODEC.encode(tick())
    framed = frame_mdp1(payload)
    assert framed.startswith(MDP1_MAGIC)
    assert looks_like_mdp1(framed)
    assert not looks_like_mdp1(payload)
    stripped, had_prefix = strip_mdp1_if_present(framed)
    assert had_prefix and stripped == payload
    assert strip_mdp1_if_present(payload) == (payload, False)


def test_framing_refuses_an_out_of_range_length() -> None:
    with pytest.raises(CodecError, match="INVALID_LENGTH"):
        frame_mdp1(b"")
    with pytest.raises(CodecError, match="INVALID_LENGTH"):
        frame_mdp1(b"\x00" * (MAX_EVENT_VALUE_BYTES + 1))


# -- adapter framing ------------------------------------------------------


@pytest.mark.whatif("P07-G04-01", "P07-G04-02")
@pytest.mark.parametrize("chunk_sizes", [(1,), (1, 17), (3,), (7, 1, 64), (4096,)])
def test_framing_is_chunk_agnostic(chunk_sizes: tuple[int, ...]) -> None:
    """Any segmentation must yield the same published set (case ``E2E-04``)."""
    events = [quote(1), tick(2), quote(3), tick(4)]
    _, published, _ = drive(events, chunk_sizes=chunk_sizes)
    assert published == events


def test_the_framer_holds_a_partial_frame_and_emits_nothing() -> None:
    published: list[SemanticCore] = []
    adapter = Adapter(dataset_id="ds", limits=LIMITS, sink=published.append)
    frame = frame_mdp1(DEFAULT_CODEC.encode(quote()))
    adapter.feed(frame[:-1])
    assert published == []
    assert adapter.pending_bytes > 0
    adapter.feed(frame[-1:])
    assert len(published) == 1
    assert adapter.pending_bytes == 0


@pytest.mark.whatif("P07-G04-05")
def test_closing_mid_frame_reports_truncation_and_publishes_nothing() -> None:
    published: list[SemanticCore] = []
    adapter = Adapter(dataset_id="ds", limits=LIMITS, sink=published.append)
    frame = frame_mdp1(DEFAULT_CODEC.encode(quote()))
    adapter.feed(frame[:10])
    adapter.close()
    assert published == []
    assert adapter.counters.closed_reason == "TRUNCATED_BODY"
    assert adapter.state is FrameState.CLOSED


@pytest.mark.whatif("P07-G04-03")
def test_bad_magic_fails_closed_without_resynchronising() -> None:
    """P03 rule 1: no hunting for the next magic, because a payload can spoof it."""
    published: list[SemanticCore] = []
    adapter = Adapter(dataset_id="ds", limits=LIMITS, sink=published.append)
    good = frame_mdp1(DEFAULT_CODEC.encode(quote(1)))
    adapter.feed(b"XXXX" + good[4:] + frame_mdp1(DEFAULT_CODEC.encode(quote(2))))
    assert published == []
    assert adapter.counters.closed_reason == "BAD_MAGIC"
    # And further bytes are ignored: the stream is closed.
    adapter.feed(good)
    assert published == []


@pytest.mark.whatif("P07-G04-04")
def test_an_invalid_length_is_rejected_before_any_allocation() -> None:
    published: list[SemanticCore] = []
    adapter = Adapter(dataset_id="ds", limits=LIMITS, sink=published.append)
    adapter.feed(MDP1_MAGIC + (0xFFFFFFFF).to_bytes(4, "big"))
    assert adapter.counters.closed_reason == "INVALID_LENGTH"
    assert published == []


def test_a_malformed_body_closes_the_stream() -> None:
    published: list[SemanticCore] = []
    adapter = Adapter(dataset_id="ds", limits=LIMITS, sink=published.append)
    adapter.feed(MDP1_MAGIC + (6).to_bytes(4, "big") + b"\xff\xff\xff\xff\xff\xff")
    assert published == []
    assert adapter.counters.closed_reason == "PROTO_DECODE_ERROR"


# -- adapter policy -------------------------------------------------------


def reasons(outcomes: list[AdapterOutcome]) -> list[str | None]:
    return [outcome.reason for outcome in outcomes]


@pytest.mark.whatif("P07-G05-03")
def test_an_exact_duplicate_is_suppressed() -> None:
    _, published, outcomes = drive([quote(1), quote(1)])
    assert len(published) == 1
    assert "DUPLICATE_EXACT" in reasons(outcomes)


@pytest.mark.whatif("P07-G16-01")
def test_a_conflicting_duplicate_is_rejected_and_counted() -> None:
    adapter, published, outcomes = drive([quote(1), quote(1, bid=108001, ask=108004)])
    assert len(published) == 1
    assert "DUPLICATE_CONFLICT" in reasons(outcomes)
    assert adapter.counters.conflicts_detected == 1


@pytest.mark.whatif("P07-G05-02")
def test_a_gap_is_flagged_but_the_new_event_still_publishes() -> None:
    """P03's documented default: emit with a DEGRADED flag, do not drop the feed."""
    adapter, published, outcomes = drive([quote(1), quote(5)])
    assert [core.sequence for core in published] == [1, 5]
    assert adapter.counters.gaps_detected == 1
    assert any("SEQUENCE_GAP" in outcome.flags for outcome in outcomes)


@pytest.mark.whatif("P07-G05-05")
def test_a_late_arrival_inside_the_window_is_not_published() -> None:
    _, published, outcomes = drive([quote(1), quote(3), quote(2)])
    assert [core.sequence for core in published] == [1, 3]
    assert "REORDER_LATE" in reasons(outcomes)


def test_an_event_older_than_the_window_is_rejected_not_assumed_duplicate() -> None:
    _, published, outcomes = drive([quote(1), quote(100_000), quote(2)])
    assert "TOO_OLD_UNVERIFIABLE" in reasons(outcomes)
    assert 2 not in [core.sequence for core in published]


@pytest.mark.whatif("P07-G06-02")
def test_a_stale_quote_is_suppressed_and_the_watermark_does_not_regress() -> None:
    events = [
        quote(1, time_ns=20_000_000_000),
        quote(2, time_ns=1_000_000_000),  # 19s older than the watermark
        quote(3, time_ns=20_000_000_001),
    ]
    _, published, outcomes = drive(events)
    assert [core.sequence for core in published] == [1, 3]
    assert "EVENT_TIME_OLD" in reasons(outcomes)


@pytest.mark.whatif("P07-G06-03")
def test_a_tick_is_not_judged_by_the_quote_watermark() -> None:
    """P03 section 6: a tick neither refreshes nor is measured by it."""
    events = [quote(1, time_ns=20_000_000_000), tick(2, time_ns=1_000_000_000)]
    _, published, outcomes = drive(events)
    assert [core.sequence for core in published] == [1, 2]
    assert "EVENT_TIME_OLD" not in reasons(outcomes)


@pytest.mark.whatif("P07-G30-03")
@pytest.mark.parametrize(
    ("core", "expected"),
    [
        (quote(1, bid=0), "INVALID_BID"),
        (quote(1, bid=108005, ask=108000), "INVALID_ASK"),
        (quote(1, bid=108000, ask=108100), "SPREAD_OUT_OF_RANGE"),
        (tick(1, price=0), "INVALID_PRICE"),
        (tick(1, price=999999), "PRICE_OUT_OF_RANGE"),
    ],
)
def test_business_invalid_events_are_rejected_with_a_reason(
    core: SemanticCore, expected: str
) -> None:
    _, published, outcomes = drive([core])
    assert published == []
    assert reasons(outcomes) == [expected]


def test_a_business_reject_does_not_update_the_sequence_tracker() -> None:
    """So the next event reports the gap the rejected one left."""
    adapter, published, outcomes = drive([quote(1), quote(2, bid=0), quote(3)])
    assert [core.sequence for core in published] == [1, 3]
    assert adapter.counters.gaps_detected == 1


def test_a_bad_price_scale_is_rejected() -> None:
    wrong_scale = quote(1)._replace(price_scale=1000)
    _, published, outcomes = drive([wrong_scale])
    assert published == []
    assert reasons(outcomes) == ["BAD_SCALE"]


def test_an_unknown_symbol_is_rejected_without_growing_state() -> None:
    _, published, outcomes = drive([quote(1)._replace(symbol="ZZZZZZ")])
    assert published == []
    assert reasons(outcomes) == ["UNKNOWN_SYMBOL"]


def test_a_foreign_dataset_is_rejected() -> None:
    _, published, outcomes = drive([quote(1)._replace(dataset_id="other")])
    assert published == []
    assert reasons(outcomes) == ["WRONG_DATASET"]


@pytest.mark.whatif("P07-G24-02")
def test_tracker_state_stays_bounded_over_a_long_run() -> None:
    """The ring is capped, so a long stream cannot grow per-symbol state forever."""
    from p07.dataplane.adapter import SEQUENCE_WINDOW

    adapter, published, _ = drive([quote(sequence) for sequence in range(1, 600)])
    key = adapter._keys["EURUSD"]  # noqa: SLF001 - inspecting bounded state
    assert len(published) == 599
    assert len(key.ring) <= SEQUENCE_WINDOW
    assert len(key.index) <= SEQUENCE_WINDOW


def test_limits_are_read_from_the_sealed_manifest(sealed: Any) -> None:
    limits = limits_from_sealed(sealed.symbols)
    assert set(limits) == set(sealed.symbols)
    assert limits["USDJPY"].price_scale == sealed.symbols["USDJPY"].price_scale


def test_chunked_refuses_a_nonpositive_size() -> None:
    with pytest.raises(ValueError):
        list(chunked(b"abc", [0]))
    with pytest.raises(ValueError):
        list(chunked(b"abc", []))
