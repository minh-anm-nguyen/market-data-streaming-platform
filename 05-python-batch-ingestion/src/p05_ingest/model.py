"""Domain model: identities, projections, rows and immutable batches.

Design rules enforced here:

  * prices and times are ``int`` only -- never float, never Decimal (spec FR-05,
    I-06). Python ints are arbitrary precision, so range checks are explicit.
  * field *presence* is modelled as ``None`` vs a value, and ``None`` is not
    interchangeable with ``0``: a zero explicitly set on the wire is
    semantically different from an absent field, and the canonical digest
    distinguishes them.
  * a sealed batch is frozen. Retries re-send the same object (spec I-04).
"""

from __future__ import annotations

import datetime as _dt
import enum
from dataclasses import dataclass, field
from typing import NamedTuple

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

#: ClickHouse ``Date`` supports 1970-01-01 .. 2149-06-06. event_time_ns is
#: additionally bounded so a nanosecond value always maps into that range.
CH_DATE_MIN = _dt.date(1970, 1, 1)
CH_DATE_MAX = _dt.date(2149, 6, 6)

NS_PER_DAY = 86_400_000_000_000

_EPOCH_DATE = _dt.date(1970, 1, 1)


class EventKind(enum.IntEnum):
    """Wire value of the ClickHouse ``Enum8('tick'=1,'quote'=2)`` column.

    The integer values are part of the canonical digest (spec section 3.2) and
    of the table DDL, so they are frozen.
    """

    TICK = 1
    QUOTE = 2

    @property
    def ch_name(self) -> str:
        return self.name.lower()


class EventIdentity(NamedTuple):
    """``(dataset_id, symbol, sequence)`` -- the logical source identity.

    Spec section 3.2: ``event_kind`` is deliberately NOT part of the identity.
    The same identity arriving once as a tick and once as a quote is an
    identity *conflict* to be reported, not two valid events.
    """

    dataset_id: str
    symbol: str
    sequence: int


class TransportCoordinate(NamedTuple):
    """Where a record physically sat in Kafka.

    Never part of the semantic digest: the same logical event replayed to a new
    offset must digest identically.
    """

    kafka_cluster_id: str
    topic_generation: str
    topic: str
    partition: int
    offset: int


class TopicPartition(NamedTuple):
    """A Kafka topic-partition. Hashable, so it keys all per-TP state."""

    topic: str
    partition: int

    def __str__(self) -> str:  # pragma: no cover - diagnostics only
        return f"{self.topic}[{self.partition}]"


@dataclass(frozen=True, slots=True)
class LogicalProjection:
    """The semantic content of one market event.

    This is exactly the tuple the canonical digest is computed over. It holds
    no Kafka coordinates, no ingestion timestamp and no run identifiers, so two
    physical copies of one logical event produce one digest.
    """

    dataset_id: str
    symbol: str
    sequence: int
    event_time_ns: int
    price_scale: int
    kind: EventKind
    source_schema_version: str
    bid_ticks: int | None = None
    ask_ticks: int | None = None
    price_ticks: int | None = None

    @property
    def identity(self) -> EventIdentity:
        return EventIdentity(self.dataset_id, self.symbol, self.sequence)

    @property
    def event_date(self) -> _dt.date:
        """UTC calendar date of the event, derived from the nanosecond stamp.

        Floor division, so a pre-epoch value (reachable only in the isolated
        fault lane) floors toward the earlier day instead of truncating toward
        zero.
        """
        return _EPOCH_DATE + _dt.timedelta(days=self.event_time_ns // NS_PER_DAY)


@dataclass(frozen=True, slots=True)
class EventRow:
    """One row destined for ``md_ingest.market_events_raw``.

    ``ingested_at`` is deliberately absent: it is filled by the ClickHouse
    column DEFAULT. Carrying it here would make it differ between retries of
    one immutable batch and break the digest (spec section 6.1).
    """

    projection: LogicalProjection
    coordinate: TransportCoordinate
    payload_digest: str
    ingest_run_id: str
    #: Size this row was charged at against the byte budget.
    accounted_bytes: int = 0

    @property
    def identity(self) -> EventIdentity:
        return self.projection.identity

    def to_ch_tuple(self) -> tuple[object, ...]:
        """Column values in the order declared by ``CH_INSERT_COLUMNS``."""
        p = self.projection
        c = self.coordinate
        return (
            p.dataset_id,
            p.symbol,
            p.sequence,
            p.kind.ch_name,
            p.event_time_ns,
            p.event_date,
            p.price_scale,
            p.bid_ticks,
            p.ask_ticks,
            p.price_ticks,
            p.source_schema_version,
            self.payload_digest,
            c.topic,
            c.partition,
            c.offset,
            c.topic_generation,
            self.ingest_run_id,
        )


#: Explicit insert column list. Never rely on table column order (spec 6.1).
CH_INSERT_COLUMNS: tuple[str, ...] = (
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
)


class SealReason(enum.StrEnum):
    """Why a batch stopped accepting rows. Exported as a metric label."""

    MAX_ROWS = "max_rows"
    MAX_BYTES = "max_bytes"
    MAX_WAIT = "max_wait"
    REVOKE = "revoke"
    SHUTDOWN = "shutdown"
    FORCED = "forced"


@dataclass(frozen=True, slots=True)
class SealedBatch:
    """An immutable unit of work handed to a sink worker.

    Spec I-04: every retry of this batch re-sends the identical rows, in the
    identical order, under the identical ``insert_token``. Mutating a sealed
    batch is impossible by construction; ``rows`` is a tuple.
    """

    tp: TopicPartition
    assignment_epoch: int
    rows: tuple[EventRow, ...]
    batch_id: str
    insert_token: str
    seal_reason: SealReason
    accounted_bytes: int
    sealed_at_monotonic: float

    def __post_init__(self) -> None:
        if not self.rows:
            raise ValueError("a sealed batch must contain at least one row")

    @property
    def first_offset(self) -> int:
        return self.rows[0].coordinate.offset

    @property
    def last_offset(self) -> int:
        return self.rows[-1].coordinate.offset

    @property
    def offsets(self) -> tuple[int, ...]:
        return tuple(r.coordinate.offset for r in self.rows)

    def __len__(self) -> int:
        return len(self.rows)


class SinkOutcome(enum.StrEnum):
    """Terminal classification of one insert attempt (spec section 5.2)."""

    ACKED = "acked"
    AMBIGUOUS = "ambiguous"
    TRANSIENT = "transient"
    PERMANENT = "permanent"

    @property
    def is_definitive_success(self) -> bool:
        return self is SinkOutcome.ACKED

    @property
    def may_retry_same_batch(self) -> bool:
        return self in (SinkOutcome.AMBIGUOUS, SinkOutcome.TRANSIENT)


@dataclass(frozen=True, slots=True)
class SinkResult:
    """What a sink worker reports back to the owner thread.

    The epoch travels with the result so the owner can fence a completion that
    arrives after the partition was revoked (spec section 4.3).
    """

    tp: TopicPartition
    assignment_epoch: int
    batch_id: str
    outcome: SinkOutcome
    attempts: int
    latency_s: float
    detail: str = ""
    rows_written_hint: int | None = None


class RecordDisposition(enum.StrEnum):
    """Terminal per-record result consumed by the commit frontier."""

    #: Written to the market table and ACKed by ClickHouse.
    SINK_ACKED = "sink_acked"
    #: Refused by validation and durably quarantined (DLQ ACK received).
    QUARANTINED = "quarantined"
    #: Refused under fail-closed policy: permanently blocks the frontier.
    BLOCKED = "blocked"


class PartitionState(enum.StrEnum):
    """Partition lifecycle (spec section 4.3)."""

    UNASSIGNED = "unassigned"
    ASSIGNED = "assigned"
    PAUSED = "paused"
    INFLIGHT = "inflight"
    REVOKING = "revoking"
    REVOKED = "revoked"
    LOST = "lost"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class ConsumedRecord:
    """A raw record as returned by ``poll()``, before any interpretation."""

    tp: TopicPartition
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[str, bytes], ...] = ()
    timestamp_ms: int | None = None


@dataclass(slots=True)
class RunStatus:
    """Terminal verdict of a run. Defaults are deliberately pessimistic."""

    complete: bool = False
    reason: str = "NOT_RUN"
    blocked_partitions: list[str] = field(default_factory=list)

    @property
    def verdict(self) -> str:
        return "COMPLETE" if self.complete else "INCOMPLETE"
