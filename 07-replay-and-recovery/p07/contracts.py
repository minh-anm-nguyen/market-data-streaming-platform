"""Identity, semantic core and the vocabulary every other module shares.

Section 3.1 of the design fixes the contract this module encodes::

    source_identity = (dataset_id, symbol, sequence)
    source_order    = sequence within (dataset_id, symbol) across BOTH tick and quote
    semantic_core   = dataset_id, symbol, sequence, event_time_ns, price_scale,
                      event_kind, source_schema_version,
                      presence(bid_ticks, ask_ticks, price_ticks) + integer values
    transport_coord = (kafka_cluster_id, topic_generation, topic, partition, offset)

Three rules drive the shapes below.

**Identity is three fields, never four.** ``run_id`` and ``build_id`` are replay
and snapshot versions; folding either into the identity would make a retry look
like a new event and hide duplicates. :class:`SourceIdentity` therefore carries
exactly the triple, and :class:`TransportCoord` is a separate type so a Kafka
offset can never be mistaken for a source sequence.

**Presence is part of the semantics.** A tick with ``price_ticks=0`` present and
a tick with the field absent are different events (case ``P07-G03-01``). Every
optional field is ``int | None`` and the ``None`` travels into the digest as a
presence byte.

**Prices are integers.** ``float`` never carries price or timestamp truth here,
for the same reason it does not in P01's model: a scaled integer compares and
hashes exactly, a float does not.
"""

from __future__ import annotations

from enum import IntEnum
from typing import Final, NamedTuple

#: The source schema P01 emits, and the only one the oracles accept without an
#: explicit, separately verified migration (case ``P07-G01-12``).
SOURCE_SCHEMA_VERSION: Final = "sim.v1"

#: Protobuf package pinned by P02. A record claiming another package is a
#: version-skew failure, not something to coerce (case ``P07-G03-03``).
PROTOBUF_PACKAGE: Final = "marketdata.v1"

#: Kafka topics from P04. Ticks and quotes live apart, so there is no
#: cross-topic total order to lean on (design 4.3, group ``G08``).
TICK_TOPIC: Final = "md.ticks.v1"
QUOTE_TOPIC: Final = "md.quotes.v1"

#: P05 caps a Kafka record value at 4 KiB. P03's TCP frame cap is a separate
#: budget and is deliberately not reused here (design 8).
MAX_EVENT_VALUE_BYTES: Final = 4096

#: One minute in nanoseconds: the OHLC bucket width from P06 section 4.
BAR_WIDTH_NS: Final = 60_000_000_000


class EventKind(IntEnum):
    """Payload kind, with the wire values the semantic digest encodes.

    ``IntEnum`` rather than ``StrEnum`` because the digest writes the kind as a
    single byte, and a stray string would silently widen that field.
    """

    TICK = 1
    QUOTE = 2

    @property
    def wire_name(self) -> str:
        """The name P01's JSONL and P02's ``oneof`` both use."""
        return "tick" if self is EventKind.TICK else "quote"

    @classmethod
    def from_wire_name(cls, name: object) -> "EventKind":
        if name == "tick":
            return cls.TICK
        if name == "quote":
            return cls.QUOTE
        raise ValueError(f"unsupported event kind: {name!r}")


class SourceIdentity(NamedTuple):
    """``(dataset_id, symbol, sequence)`` and nothing else.

    Equality and hashing are structural, so this is directly usable as the key
    of the full-identity diffs in :mod:`p07.reconcile`.
    """

    dataset_id: str
    symbol: str
    sequence: int

    def __str__(self) -> str:  # pragma: no cover - diagnostics only
        return f"{self.dataset_id}/{self.symbol}/{self.sequence}"


class SemanticCore(NamedTuple):
    """The fields that must survive the pipeline unchanged in meaning.

    Two cores being equal is the only admissible proof that an event came
    through intact. Two records sharing a :class:`SourceIdentity` but differing
    here is an identity *conflict* -- a P0 failure, never a case for picking a
    winner (design 3.2 and 7.3).
    """

    dataset_id: str
    symbol: str
    sequence: int
    event_time_ns: int
    price_scale: int
    event_kind: EventKind
    source_schema_version: str
    bid_ticks: int | None
    ask_ticks: int | None
    price_ticks: int | None

    @property
    def identity(self) -> SourceIdentity:
        return SourceIdentity(self.dataset_id, self.symbol, self.sequence)

    @property
    def presence(self) -> tuple[bool, bool, bool]:
        """``(bid, ask, price)`` presence bits, in digest order."""
        return (
            self.bid_ticks is not None,
            self.ask_ticks is not None,
            self.price_ticks is not None,
        )

    @property
    def bar_start_ns(self) -> int:
        """UTC-aligned 1-minute bucket start (P06 section 4).

        ``//`` floors towards negative infinity on Python ints, which is what a
        pre-epoch timestamp needs; truncating division would file a negative
        ``event_time_ns`` under the bucket above it.
        """
        return (self.event_time_ns // BAR_WIDTH_NS) * BAR_WIDTH_NS

    def differing_fields(self, other: "SemanticCore") -> tuple[str, ...]:
        """Field names where ``self`` and ``other`` disagree.

        Lets a conflict be described precisely in the diff report instead of
        logging "mismatch" and leaving the operator to guess.
        """
        return tuple(
            name
            for name, mine, theirs in zip(self._fields, self, other, strict=True)
            if mine != theirs
        )


class TransportCoord(NamedTuple):
    """Where a physical record sat in Kafka. Deliberately not an identity.

    ``topic_generation`` is part of the coordinate because a republish into a
    fresh generation reuses offset numbers that mean something else; mixing the
    end offsets of two generations is the mistake this type exists to prevent
    (design 3.3, group ``G10``).
    """

    cluster_id: str
    topic_generation: str
    topic: str
    partition: int
    offset: int


def topic_for_kind(kind: EventKind) -> str:
    return TICK_TOPIC if kind is EventKind.TICK else QUOTE_TOPIC


def partition_for_symbol(symbol: str, partition_count: int) -> int:
    """Stable key partitioner used by the simulated P04 producer.

    This is *a* deterministic partitioner, not a re-implementation of
    librdkafka's murmur2 ``consistent_random`` scheme. P07 only needs key
    affinity to be stable within one topic generation; asserting a particular
    partition index against a real broker would be asserting librdkafka's
    internals, which :mod:`p07.dataplane` does not model. Tests accordingly
    assert "same symbol, same partition, within one generation" and never a
    fixed index.
    """
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    digest = 0x811C9DC5
    for byte in symbol.encode("utf-8"):
        # FNV-1a, 32-bit: cheap, well defined, dependency free.
        digest = ((digest ^ byte) * 0x01000193) & 0xFFFFFFFF
    return digest % partition_count
