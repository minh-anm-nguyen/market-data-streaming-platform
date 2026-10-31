"""Identity, presence, bar bucketing and the key partitioner."""

from __future__ import annotations

import pytest

from p07.contracts import (
    BAR_WIDTH_NS,
    QUOTE_TOPIC,
    TICK_TOPIC,
    EventKind,
    SemanticCore,
    SourceIdentity,
    TransportCoord,
    partition_for_symbol,
    topic_for_kind,
)


def core(**overrides: object) -> SemanticCore:
    base = dict(
        dataset_id="ds",
        symbol="EURUSD",
        sequence=5,
        event_time_ns=0,
        price_scale=100000,
        event_kind=EventKind.TICK,
        source_schema_version="sim.v1",
        bid_ticks=None,
        ask_ticks=None,
        price_ticks=100,
    )
    base.update(overrides)
    return SemanticCore(**base)  # type: ignore[arg-type]


def test_identity_is_exactly_three_fields() -> None:
    assert SourceIdentity._fields == ("dataset_id", "symbol", "sequence")
    assert core().identity == SourceIdentity("ds", "EURUSD", 5)


def test_identity_is_hashable_and_structural() -> None:
    """Usable directly as a diff key, with equality by value."""
    a = SourceIdentity("ds", "EURUSD", 5)
    b = SourceIdentity("ds", "EURUSD", 5)
    assert a == b and hash(a) == hash(b)
    assert len({a, b}) == 1


def test_transport_coord_is_not_an_identity() -> None:
    """A Kafka offset must not be confusable with a source sequence."""
    coord = TransportCoord("c", "gen-1", TICK_TOPIC, 3, 500)
    assert not isinstance(coord, SourceIdentity)
    assert "offset" in TransportCoord._fields
    assert "sequence" not in TransportCoord._fields
    assert "topic_generation" in TransportCoord._fields


def test_presence_bits_are_in_digest_order() -> None:
    quote = core(event_kind=EventKind.QUOTE, bid_ticks=1, ask_ticks=2, price_ticks=None)
    assert quote.presence == (True, True, False)
    assert core(price_ticks=None).presence == (False, False, False)
    assert core(price_ticks=0).presence == (False, False, True)


def test_differing_fields_names_the_disagreement() -> None:
    left = core(price_ticks=100)
    right = core(price_ticks=101, event_time_ns=7)
    assert set(left.differing_fields(right)) == {"price_ticks", "event_time_ns"}
    assert left.differing_fields(left) == ()


@pytest.mark.whatif("P07-G30-01")
@pytest.mark.parametrize(
    ("event_time_ns", "expected"),
    [
        (0, 0),
        (1, 0),
        (BAR_WIDTH_NS - 1, 0),
        (BAR_WIDTH_NS, BAR_WIDTH_NS),
        (BAR_WIDTH_NS + 1, BAR_WIDTH_NS),
        (-1, -BAR_WIDTH_NS),
        (-BAR_WIDTH_NS, -BAR_WIDTH_NS),
        (-BAR_WIDTH_NS - 1, -2 * BAR_WIDTH_NS),
    ],
)
def test_bar_start_floors_towards_negative_infinity(event_time_ns: int, expected: int) -> None:
    """A pre-epoch timestamp must floor down, not truncate towards zero.

    Truncating would file a negative ``event_time_ns`` in the bucket *above* it,
    putting a tick in the wrong minute.
    """
    assert core(event_time_ns=event_time_ns).bar_start_ns == expected


def test_event_kind_round_trips_through_its_wire_name() -> None:
    for kind in EventKind:
        assert EventKind.from_wire_name(kind.wire_name) is kind
    with pytest.raises(ValueError):
        EventKind.from_wire_name("trade")
    with pytest.raises(ValueError):
        EventKind.from_wire_name(None)


def test_topic_routing_keeps_the_kinds_apart() -> None:
    assert topic_for_kind(EventKind.TICK) == TICK_TOPIC
    assert topic_for_kind(EventKind.QUOTE) == QUOTE_TOPIC
    assert TICK_TOPIC != QUOTE_TOPIC


@pytest.mark.whatif("P07-G08-01")
def test_partitioner_is_stable_and_in_range() -> None:
    """Key affinity: the same symbol always lands on the same partition.

    The *value* is not asserted -- this is not librdkafka's murmur2 scheme and
    pretending otherwise would be asserting a compatibility the harness does not
    have (see the function's own docstring).
    """
    for count in (1, 2, 4, 8, 16):
        for symbol in ("EURUSD", "GBPUSD", "USDJPY", "A.B-C"):
            first = partition_for_symbol(symbol, count)
            assert 0 <= first < count
            assert partition_for_symbol(symbol, count) == first


def test_partitioner_rejects_a_nonpositive_count() -> None:
    with pytest.raises(ValueError):
        partition_for_symbol("EURUSD", 0)
