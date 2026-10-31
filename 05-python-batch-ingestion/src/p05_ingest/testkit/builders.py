"""Fixture builders: valid events, the golden corpus and the poison corpus.

The golden corpus is specified rather than random. Spec section 3.2 lists what
it must contain, and each entry here exists to pin one behaviour that a naive
implementation gets wrong:

  * an explicitly-present zero, so zero is not confused with absent;
  * an absent optional, so absent is not materialised as zero;
  * values at the int64 boundaries, so nothing silently wraps;
  * a multi-byte UTF-8 ``dataset_id``, so byte length is not confused with
    character count;
  * a duplicate pair (same identity, same digest) and a conflict pair (same
    identity, different digest), so the two are not collapsed.

``make_event`` builds the protobuf message itself rather than going through the
application, so a test asserting that the pipeline preserved a field is not
asserting against the same code that produced it.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..generated import market_data_pb2 as pb
from ..model import INT64_MAX, EventKind, LogicalProjection

DEFAULT_DATASET = "demo-a1b2c3"
DEFAULT_SCHEMA_VERSION = "marketdata.v1"
TICKS_TOPIC = "md.ticks.v1"
QUOTES_TOPIC = "md.quotes.v1"

#: 2024-01-02T03:04:05.123456789Z, chosen to exercise sub-microsecond digits.
BASE_EVENT_TIME_NS = 1_704_171_845_123_456_789
DEFAULT_PRICE_SCALE = 100_000


def make_event(
    *,
    kind: EventKind,
    dataset_id: str = DEFAULT_DATASET,
    symbol: str = "EURUSD",
    sequence: int = 1,
    event_time_ns: int = BASE_EVENT_TIME_NS,
    price_scale: int = DEFAULT_PRICE_SCALE,
    source_schema_version: str = DEFAULT_SCHEMA_VERSION,
    bid_ticks: int | None = None,
    ask_ticks: int | None = None,
    price_ticks: int | None = None,
    omit: tuple[str, ...] = (),
) -> pb.MarketDataEvent:
    """Build a message. ``omit`` leaves the named top-level fields absent."""
    event = pb.MarketDataEvent()
    if "dataset_id" not in omit:
        event.dataset_id = dataset_id
    if "symbol" not in omit:
        event.symbol = symbol
    if "sequence" not in omit:
        event.sequence = sequence
    if "event_time_ns" not in omit:
        event.event_time_ns = event_time_ns
    if "price_scale" not in omit:
        event.price_scale = price_scale
    if "source_schema_version" not in omit:
        event.source_schema_version = source_schema_version

    if "payload" in omit:
        return event
    if kind is EventKind.QUOTE:
        quote = event.quote
        if bid_ticks is not None:
            quote.bid_ticks = bid_ticks
        if ask_ticks is not None:
            quote.ask_ticks = ask_ticks
        # Touch the submessage so the oneof is set even when both are absent.
        event.quote.SetInParent()
    else:
        tick = event.tick
        if price_ticks is not None:
            tick.price_ticks = price_ticks
        event.tick.SetInParent()
    return event


def make_tick(
    *, sequence: int = 1, price_ticks: int = 109_385, **kwargs
) -> pb.MarketDataEvent:
    return make_event(
        kind=EventKind.TICK, sequence=sequence, price_ticks=price_ticks, **kwargs
    )


def make_quote(
    *,
    sequence: int = 1,
    bid_ticks: int = 109_380,
    ask_ticks: int = 109_390,
    **kwargs,
) -> pb.MarketDataEvent:
    return make_event(
        kind=EventKind.QUOTE,
        sequence=sequence,
        bid_ticks=bid_ticks,
        ask_ticks=ask_ticks,
        **kwargs,
    )


def topic_for(kind: EventKind) -> str:
    return TICKS_TOPIC if kind is EventKind.TICK else QUOTES_TOPIC


@dataclass(frozen=True, slots=True)
class GoldenEntry:
    """One corpus entry with everything a test needs to route and assert."""

    label: str
    topic: str
    key: bytes
    value: bytes
    expected: LogicalProjection | None
    #: Reason code expected when the entry must be refused.
    expect_reject: str | None = None
    note: str = ""


def _entry(
    label: str,
    event: pb.MarketDataEvent,
    kind: EventKind,
    *,
    key: bytes | None = None,
    expect_reject: str | None = None,
    note: str = "",
    topic: str | None = None,
) -> GoldenEntry:
    expected = None
    if expect_reject is None:
        expected = LogicalProjection(
            dataset_id=event.dataset_id,
            symbol=event.symbol,
            sequence=event.sequence,
            event_time_ns=event.event_time_ns,
            price_scale=event.price_scale,
            kind=kind,
            source_schema_version=event.source_schema_version,
            bid_ticks=(
                event.quote.bid_ticks if event.quote.HasField("bid_ticks") else None
            )
            if kind is EventKind.QUOTE
            else None,
            ask_ticks=(
                event.quote.ask_ticks if event.quote.HasField("ask_ticks") else None
            )
            if kind is EventKind.QUOTE
            else None,
            price_ticks=(
                event.tick.price_ticks if event.tick.HasField("price_ticks") else None
            )
            if kind is EventKind.TICK
            else None,
        )
    return GoldenEntry(
        label=label,
        topic=topic or topic_for(kind),
        key=key if key is not None else event.symbol.encode("ascii"),
        value=event.SerializeToString(),
        expected=expected,
        expect_reject=expect_reject,
        note=note,
    )


def golden_corpus() -> list[GoldenEntry]:
    """Accepted entries that must survive the pipeline field-for-field."""
    entries: list[GoldenEntry] = [
        _entry(
            "quote-two-positive-prices",
            make_quote(sequence=1),
            EventKind.QUOTE,
            note="the ordinary case: both prices present and bid <= ask",
        ),
        _entry(
            "tick-price",
            make_tick(sequence=2),
            EventKind.TICK,
            note="the ordinary tick case",
        ),
        _entry(
            "quote-equal-bid-ask",
            make_quote(sequence=3, bid_ticks=109_385, ask_ticks=109_385),
            EventKind.QUOTE,
            note="a zero spread is tight, not invalid",
        ),
        _entry(
            "tick-sequence-zero-present",
            make_tick(sequence=0),
            EventKind.TICK,
            note="sequence 0 is explicitly present, not absent",
        ),
        _entry(
            "tick-event-time-zero",
            make_tick(sequence=4, event_time_ns=0),
            EventKind.TICK,
            note="the epoch instant; event_date must be 1970-01-01",
        ),
        _entry(
            "tick-max-int64-sequence",
            make_tick(sequence=INT64_MAX),
            EventKind.TICK,
            note="int64 ceiling must round-trip without wrapping",
        ),
        _entry(
            "tick-large-price",
            make_tick(sequence=5, price_ticks=INT64_MAX),
            EventKind.TICK,
            note="price at the int64 ceiling",
        ),
        _entry(
            "tick-utf8-dataset-id",
            make_tick(sequence=6, dataset_id="dataset-éà中文"),
            EventKind.TICK,
            note="multi-byte UTF-8: byte length differs from character count",
        ),
        _entry(
            "tick-symbol-with-punctuation",
            make_tick(sequence=7, symbol="XAU/USD"),
            EventKind.TICK,
            note="slash is a legal symbol character",
        ),
        _entry(
            "tick-high-price-scale",
            make_tick(sequence=8, price_scale=1_000_000_000),
            EventKind.TICK,
            note="a large scale must not be coerced to float",
        ),
        _entry(
            "tick-far-future-date",
            make_tick(sequence=9, event_time_ns=4_102_444_800_000_000_000),
            EventKind.TICK,
            note="2100-01-01, inside the ClickHouse Date range",
        ),
    ]
    return entries


def duplicate_pair() -> tuple[GoldenEntry, GoldenEntry]:
    """Two records, same identity and same semantics: one logical event."""
    event = make_tick(sequence=42)
    first = _entry("dup-first", event, EventKind.TICK)
    second = _entry("dup-second", event, EventKind.TICK)
    return first, second


def conflict_pair() -> tuple[GoldenEntry, GoldenEntry]:
    """Same identity, different price: an identity conflict, never a winner."""
    a = _entry("conflict-a", make_tick(sequence=77, price_ticks=100_000), EventKind.TICK)
    b = _entry("conflict-b", make_tick(sequence=77, price_ticks=200_000), EventKind.TICK)
    return a, b


def poison_corpus() -> list[GoldenEntry]:
    """Entries that must be refused, each with the exact expected code."""
    good_tick = make_tick(sequence=1)
    good_quote = make_quote(sequence=1)
    entries: list[GoldenEntry] = [
        _entry(
            "tick-on-quotes-topic",
            good_tick,
            EventKind.TICK,
            topic=QUOTES_TOPIC,
            expect_reject="TOPIC_PAYLOAD_MISMATCH",
            note="the oneof must match the topic it arrived on",
        ),
        _entry(
            "quote-on-ticks-topic",
            good_quote,
            EventKind.QUOTE,
            topic=TICKS_TOPIC,
            expect_reject="TOPIC_PAYLOAD_MISMATCH",
        ),
        _entry(
            "key-lowercase",
            make_tick(sequence=2, symbol="eurusd"),
            EventKind.TICK,
            expect_reject="SYMBOL_NOT_UPPERCASE",
            note="must not be silently upper-cased",
        ),
        _entry(
            "key-mismatch",
            make_tick(sequence=3),
            EventKind.TICK,
            key=b"GBPUSD",
            expect_reject="KEY_SYMBOL_MISMATCH",
        ),
        _entry(
            "missing-dataset-id",
            make_tick(sequence=4, omit=("dataset_id",)),
            EventKind.TICK,
            expect_reject="DATASET_ID_ABSENT",
        ),
        _entry(
            "empty-dataset-id",
            make_tick(sequence=5, dataset_id=""),
            EventKind.TICK,
            expect_reject="DATASET_ID_EMPTY",
            note="present but empty is still unusable as an identity",
        ),
        _entry(
            "missing-sequence",
            make_tick(sequence=6, omit=("sequence",)),
            EventKind.TICK,
            expect_reject="SEQUENCE_ABSENT",
        ),
        _entry(
            "missing-event-time",
            make_tick(sequence=7, omit=("event_time_ns",)),
            EventKind.TICK,
            expect_reject="EVENT_TIME_ABSENT",
        ),
        _entry(
            "missing-price-scale",
            make_tick(sequence=8, omit=("price_scale",)),
            EventKind.TICK,
            expect_reject="PRICE_SCALE_ABSENT",
        ),
        _entry(
            "zero-price-scale",
            make_tick(sequence=9, price_scale=0),
            EventKind.TICK,
            expect_reject="PRICE_SCALE_NOT_POSITIVE",
        ),
        _entry(
            "negative-price-scale",
            make_tick(sequence=10, price_scale=-100),
            EventKind.TICK,
            expect_reject="PRICE_SCALE_NOT_POSITIVE",
        ),
        _entry(
            "no-payload-oneof",
            make_event(kind=EventKind.TICK, sequence=11, omit=("payload",)),
            EventKind.TICK,
            expect_reject="PAYLOAD_ONEOF_ABSENT",
        ),
        _entry(
            "tick-without-price",
            make_event(kind=EventKind.TICK, sequence=12),
            EventKind.TICK,
            expect_reject="TICK_PRICE_ABSENT",
            note="oneof set but the inner field absent",
        ),
        _entry(
            "tick-zero-price",
            make_tick(sequence=13, price_ticks=0),
            EventKind.TICK,
            expect_reject="TICK_PRICE_NOT_POSITIVE",
        ),
        _entry(
            "tick-negative-price",
            make_tick(sequence=14, price_ticks=-5),
            EventKind.TICK,
            expect_reject="TICK_PRICE_NOT_POSITIVE",
        ),
        _entry(
            "quote-missing-bid",
            make_event(kind=EventKind.QUOTE, sequence=15, ask_ticks=109_390),
            EventKind.QUOTE,
            expect_reject="QUOTE_BID_ABSENT",
        ),
        _entry(
            "quote-missing-ask",
            make_event(kind=EventKind.QUOTE, sequence=16, bid_ticks=109_380),
            EventKind.QUOTE,
            expect_reject="QUOTE_ASK_ABSENT",
        ),
        _entry(
            "quote-crossed",
            make_quote(sequence=17, bid_ticks=109_400, ask_ticks=109_380),
            EventKind.QUOTE,
            expect_reject="QUOTE_CROSSED",
        ),
        _entry(
            "negative-sequence",
            make_tick(sequence=-1),
            EventKind.TICK,
            expect_reject="SEQUENCE_NEGATIVE",
        ),
        _entry(
            "symbol-too-long",
            make_tick(sequence=18, symbol="A" * 40),
            EventKind.TICK,
            expect_reject="KEY_TOO_LARGE",
            note="the key cap trips before the symbol cap, by design",
        ),
        _entry(
            "symbol-bad-charset",
            make_tick(sequence=19, symbol="EUR USD"),
            EventKind.TICK,
            expect_reject="SYMBOL_CHARSET",
        ),
    ]
    return entries
