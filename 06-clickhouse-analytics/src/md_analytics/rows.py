"""Row shapes for the P05 raw table and the P06 canonical table.

``RawRow`` mirrors ``md_ingest.market_events_raw`` exactly as section 0.1 records
it, including the transport coordinates. P06 reads this table and never creates
or alters it (FR-01).

``CanonicalTick`` is the only thing the 1m materialized view is allowed to see:
one row per source identity, ticks only, already scoped to a ``build_id``.
Quotes stop at the audit -- a quote is not a trade and its ask price must never
reach a tick HIGH (case A-01, H-03).
"""

from __future__ import annotations

from datetime import date
from typing import Any, Final, NamedTuple

from .contracts import (
    EVENT_TICK,
    SourceIdentity,
    SourceOrder,
    payload_digest,
    require_digest_format,
    semantic_core,
    utc_date_from_ns,
)
from .errors import ContractError

#: Column order of ``md_ingest.market_events_raw`` (section 0.1). Pinned here so
#: a schema preflight can compare names *and* order against the live table
#: instead of trusting that ``SELECT *`` lines up (case B-09).
RAW_COLUMNS: Final = (
    "dataset_id",
    "symbol",
    "sequence",
    "event_kind",
    "event_time_ns",
    "event_date",
    "price_scale",
    "bid_ticks",
    "ask_ticks",
    "price_ticks",
    "source_schema_version",
    "payload_digest",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "topic_generation",
    "ingest_run_id",
    "ingested_at",
)

CANONICAL_COLUMNS: Final = (
    "build_id",
    "dataset_id",
    "symbol",
    "sequence",
    "event_time_ns",
    "event_date",
    "price_scale",
    "price_ticks",
    "payload_digest",
    "source_schema_version",
)


class TopicPartition(NamedTuple):
    """Kafka coordinate of a *transport* record, including the generation.

    ``topic_generation`` is part of the coordinate because a deleted and
    recreated topic restarts offsets: without it, two physically different
    records share ``(topic, partition, offset)`` and lineage becomes ambiguous
    (case E-11, F-11).
    """

    topic: str
    partition: int
    # P05 declares this column as LowCardinality(String) and carries a tag
    # like "md-v1-generation-a", so the annotation admits both rather than
    # inviting a coercion that would fabricate the lineage identity this
    # field exists to protect (P09 finding P09-M0-21).
    topic_generation: int | str

    def __str__(self) -> str:
        return f"{self.topic}/{self.partition}@gen{self.topic_generation}"


class RawRow(NamedTuple):
    """One physical row of the P05 raw table.

    Several rows may share one :attr:`identity`: that is at-least-once delivery
    working as designed, not corruption. What matters is whether they share a
    semantic core as well (section 3.1).
    """

    dataset_id: str
    symbol: str
    sequence: int
    event_kind: str
    event_time_ns: int
    event_date: date
    price_scale: int
    bid_ticks: int | None
    ask_ticks: int | None
    price_ticks: int | None
    source_schema_version: str
    payload_digest: str
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    topic_generation: int
    ingest_run_id: str
    #: Ingest wall clock. Useful for latency only -- never a market timestamp and
    #: never part of a digest or an ordering decision (section 0.1).
    ingested_at: int

    @property
    def identity(self) -> SourceIdentity:
        return SourceIdentity(self.dataset_id, self.symbol, self.sequence)

    @property
    def order(self) -> SourceOrder:
        return SourceOrder(self.event_time_ns, self.sequence)

    @property
    def topic_partition(self) -> TopicPartition:
        return TopicPartition(self.kafka_topic, self.kafka_partition, self.topic_generation)

    @property
    def is_tick(self) -> bool:
        return self.event_kind == EVENT_TICK

    def semantic_core(self) -> dict[str, object]:
        """Typed projection of what happened, or raise :class:`ContractError`."""
        return semantic_core(
            dataset_id=self.dataset_id,
            symbol=self.symbol,
            sequence=self.sequence,
            event_kind=self.event_kind,
            event_time_ns=self.event_time_ns,
            price_scale=self.price_scale,
            price_ticks=self.price_ticks,
            bid_ticks=self.bid_ticks,
            ask_ticks=self.ask_ticks,
            source_schema_version=self.source_schema_version,
        )

    def recomputed_digest(self) -> str:
        return payload_digest(self.semantic_core())

    def stored_digest(self) -> str:
        return require_digest_format(self.payload_digest)

    def derived_event_date(self) -> date:
        return utc_date_from_ns(self.event_time_ns)

    def as_dict(self) -> dict[str, Any]:
        d = self._asdict()
        d["event_date"] = self.event_date.isoformat()
        return d


class CanonicalTick(NamedTuple):
    """One canonical tick: exactly one row per source identity in a build."""

    build_id: str
    dataset_id: str
    symbol: str
    sequence: int
    event_time_ns: int
    event_date: date
    price_scale: int
    price_ticks: int
    payload_digest: str
    source_schema_version: str

    @property
    def identity(self) -> SourceIdentity:
        return SourceIdentity(self.dataset_id, self.symbol, self.sequence)

    @property
    def order(self) -> SourceOrder:
        return SourceOrder(self.event_time_ns, self.sequence)

    def as_dict(self) -> dict[str, Any]:
        d = self._asdict()
        d["event_date"] = self.event_date.isoformat()
        return d


def canonical_from_raw(row: RawRow, *, build_id: str) -> CanonicalTick:
    """Project an audited raw tick into a canonical row.

    The ``event_date`` is **re-derived** from ``event_time_ns`` rather than copied
    from raw: if P05 wrote a date computed in the host's local zone, copying it
    would carry a wrong partition into the canonical table, where the audit can
    no longer see it (case B-03, D-08). The audit flags the mismatch; this
    function refuses to launder it.
    """
    if not row.is_tick:
        raise ContractError(
            f"canonical table holds ticks only, got {row.event_kind!r}",
            code="NOT_A_TICK",
            identity=tuple(row.identity),
        )
    core = row.semantic_core()
    if row.event_date != row.derived_event_date():
        raise ContractError(
            f"event_date {row.event_date.isoformat()} disagrees with UTC date "
            f"{row.derived_event_date().isoformat()} derived from event_time_ns",
            code="EVENT_DATE_MISMATCH",
            identity=tuple(row.identity),
        )
    return CanonicalTick(
        build_id=build_id,
        dataset_id=row.dataset_id,
        symbol=row.symbol,
        sequence=row.sequence,
        event_time_ns=row.event_time_ns,
        event_date=row.derived_event_date(),
        price_scale=row.price_scale,
        price_ticks=int(core["price_ticks"]),  # type: ignore[arg-type]
        payload_digest=payload_digest(core),
        source_schema_version=row.source_schema_version,
    )


def make_raw_row(
    *,
    dataset_id: str,
    symbol: str,
    sequence: int,
    event_kind: str,
    event_time_ns: int,
    price_scale: int,
    price_ticks: int | None = None,
    bid_ticks: int | None = None,
    ask_ticks: int | None = None,
    source_schema_version: str = "marketdata.v1",
    kafka_topic: str = "md.ticks.v1",
    kafka_partition: int = 0,
    kafka_offset: int = 0,
    topic_generation: int = 1,
    ingest_run_id: str = "run-0",
    ingested_at: int = 0,
    event_date: date | None = None,
    payload_digest_override: str | None = None,
) -> RawRow:
    """Build a well-formed raw row, for fixtures and tests.

    ``event_date`` and ``payload_digest_override`` are escape hatches so a test
    can inject exactly the corruption it means to inject -- a date computed in
    the wrong zone, or a digest that does not match its own row -- without
    needing a second, laxer row type that could drift from this one.
    """
    core = semantic_core(
        dataset_id=dataset_id,
        symbol=symbol,
        sequence=sequence,
        event_kind=event_kind,
        event_time_ns=event_time_ns,
        price_scale=price_scale,
        price_ticks=price_ticks,
        bid_ticks=bid_ticks,
        ask_ticks=ask_ticks,
        source_schema_version=source_schema_version,
    )
    return RawRow(
        dataset_id=dataset_id,
        symbol=symbol,
        sequence=sequence,
        event_kind=event_kind,
        event_time_ns=event_time_ns,
        event_date=event_date if event_date is not None else utc_date_from_ns(event_time_ns),
        price_scale=price_scale,
        bid_ticks=bid_ticks,
        ask_ticks=ask_ticks,
        price_ticks=price_ticks,
        source_schema_version=source_schema_version,
        payload_digest=(
            payload_digest_override
            if payload_digest_override is not None
            else payload_digest(core)
        ),
        kafka_topic=kafka_topic,
        kafka_partition=kafka_partition,
        kafka_offset=kafka_offset,
        topic_generation=topic_generation,
        ingest_run_id=ingest_run_id,
        ingested_at=ingested_at,
    )
