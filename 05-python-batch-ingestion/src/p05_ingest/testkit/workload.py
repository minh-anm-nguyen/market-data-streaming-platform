"""A deterministic market workload and the manifest that describes it.

This stands in for the P01 simulator plus the P04 publish ledger. The manifest
is the *expected* set for reconciliation, and it is produced by this generator
rather than by the ingestion path, which is what makes it an independent oracle
(spec 9.2: the engine counters are not evidence).

Properties that matter for the tests built on it:

  * ``sequence`` is per symbol and spans **both** topics, exactly as the P01
    contract states. A test that checks ticks-topic sequence contiguity would
    therefore see gaps, and must not treat them as loss (spec 3.2, last row).
  * partitioning is by symbol, so all events for one symbol share a partition
    and their relative order is preserved. Across topics there is no order.
  * everything is seeded, so a failing case can be replayed exactly.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from ..canonical_digest import payload_digest
from ..model import EventIdentity, EventKind, LogicalProjection
from .builders import (
    BASE_EVENT_TIME_NS,
    DEFAULT_DATASET,
    DEFAULT_PRICE_SCALE,
    DEFAULT_SCHEMA_VERSION,
    QUOTES_TOPIC,
    TICKS_TOPIC,
    make_quote,
    make_tick,
)

DEFAULT_SYMBOLS = ("EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "XAUUSD")

#: Starting mid price in ticks for each symbol, at price_scale 100000.
_BASE_PRICE = {
    "EURUSD": 109_385,
    "GBPUSD": 126_410,
    "USDJPY": 15_712_000,
    "AUDUSD": 65_820,
    "XAUUSD": 203_145_000,
}


@dataclass(frozen=True, slots=True)
class ExpectedEvent:
    """One manifest entry: what the source says should exist."""

    identity: EventIdentity
    payload_digest: str
    kind: EventKind
    topic: str
    partition: int
    event_time_ns: int


def partition_for(symbol: str, partitions: int) -> int:
    """Stable symbol-to-partition mapping.

    Deliberately not Python ``hash()``: that is salted per process, so the
    same symbol would land in different partitions across runs and a replay
    test would not replay.
    """
    total = 0
    for byte in symbol.encode("ascii"):
        total = (total * 31 + byte) & 0xFFFFFFFF
    return total % partitions


def produce_market_workload(
    cluster,
    *,
    events: int,
    partitions: int,
    seed: int = 1337,
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS,
    dataset_id: str = DEFAULT_DATASET,
    quote_ratio: float = 0.5,
    start_time_ns: int = BASE_EVENT_TIME_NS,
    time_step_ns: int = 1_000_000,
) -> list[ExpectedEvent]:
    """Produce ``events`` market-valid records and return the manifest.

    Interleaves ticks and quotes for the same symbol so that cross-topic
    ordering is genuinely exercised: consecutive sequences for one symbol land
    in different topics, and therefore in different partitions of different
    logs with no ordering between them.
    """
    rng = random.Random(seed)
    next_sequence: dict[str, int] = {s: 0 for s in symbols}
    price: dict[str, int] = {s: _BASE_PRICE.get(s, 100_000) for s in symbols}
    manifest: list[ExpectedEvent] = []

    for i in range(events):
        symbol = symbols[i % len(symbols)]
        sequence = next_sequence[symbol]
        next_sequence[symbol] = sequence + 1
        event_time_ns = start_time_ns + i * time_step_ns

        # Random walk in integer ticks. No float arithmetic anywhere on the
        # price path, so nothing can be rounded.
        price[symbol] = max(1, price[symbol] + rng.randint(-5, 5))
        mid = price[symbol]

        is_quote = rng.random() < quote_ratio
        partition = partition_for(symbol, partitions)

        if is_quote:
            spread = rng.randint(1, 10)
            bid, ask = mid - spread, mid + spread
            if bid < 1:
                bid = 1
                ask = max(2, mid + spread)
            event = make_quote(
                dataset_id=dataset_id,
                symbol=symbol,
                sequence=sequence,
                event_time_ns=event_time_ns,
                bid_ticks=bid,
                ask_ticks=ask,
            )
            projection = LogicalProjection(
                dataset_id=dataset_id,
                symbol=symbol,
                sequence=sequence,
                event_time_ns=event_time_ns,
                price_scale=DEFAULT_PRICE_SCALE,
                kind=EventKind.QUOTE,
                source_schema_version=DEFAULT_SCHEMA_VERSION,
                bid_ticks=bid,
                ask_ticks=ask,
            )
            topic = QUOTES_TOPIC
        else:
            event = make_tick(
                dataset_id=dataset_id,
                symbol=symbol,
                sequence=sequence,
                event_time_ns=event_time_ns,
                price_ticks=mid,
            )
            projection = LogicalProjection(
                dataset_id=dataset_id,
                symbol=symbol,
                sequence=sequence,
                event_time_ns=event_time_ns,
                price_scale=DEFAULT_PRICE_SCALE,
                kind=EventKind.TICK,
                source_schema_version=DEFAULT_SCHEMA_VERSION,
                price_ticks=mid,
            )
            topic = TICKS_TOPIC

        cluster.produce(
            topic,
            partition,
            key=symbol.encode("ascii"),
            value=event.SerializeToString(),
        )
        manifest.append(
            ExpectedEvent(
                identity=projection.identity,
                payload_digest=payload_digest(projection),
                kind=projection.kind,
                topic=topic,
                partition=partition,
                event_time_ns=event_time_ns,
            )
        )

    return manifest


def manifest_as_expected(manifest: list[ExpectedEvent]):
    """Adapt a manifest to the reconciler input type."""
    from ..reconciler import ObservedEvent

    return [
        ObservedEvent(identity=e.identity, payload_digest=e.payload_digest)
        for e in manifest
    ]
