"""Build a corpus from the real P05 landing table, instead of synthesising one.

Until this module existed, every P06 build read a corpus this project generated
in-process -- ``golden_six_event`` or ``synthetic_corpus``. That is the right
input for testing the bar algebra, and it is the wrong input for a platform:
``--clickhouse-url`` only chose where the build *wrote*, so P06 had never read a
row P05 produced. P09's audit records that as a blocker, because an analytics
layer that cannot consume its own upstream is not joined to anything.

A build needs three things, and each comes from the component that owns it:

* **raw rows** -- read from ``md_ingest.market_events_raw``, scoped to one
  dataset, in the pinned column order. P05 owns the table; P06 only reads it,
  and :mod:`md_analytics.schema_preflight` already refuses a shape it does not
  recognise.
* **the accepted-event ledger** -- read from P03's handoff stream.
  :class:`AcceptedEvent` says in its own docstring that the expected set comes
  from the adapter's ledger rather than from everything P01 generated, because
  P03 may legitimately suppress an event and demanding it appear in raw would
  manufacture a missing-row report. P03's ``p04_stub`` NDJSON *is* that ledger.
* **the snapshot cut** -- read from P05's ``snapshot`` output, which records the
  log start and the high-water mark per topic-partition at a moment in time.
  Without it the source is unbounded and completeness is unprovable.

The expected set is therefore derived from two components *upstream of the table
being audited*, which is what makes the audit mean anything: if the expected set
were recomputed from the raw rows, every row would agree with itself and a
missing row would be invisible.

One seam is deliberately visible rather than smoothed over. P05 declares
``topic_generation`` as ``LowCardinality(String)`` and carries a tag like
``md-v1-generation-a``; P06's :class:`TopicPartition` annotates the same field
as ``int``. Nothing is coerced here: the string is carried through unchanged and
P09 records the disagreement. Inventing an integer for it would fabricate the
lineage identity that field exists to protect.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence

from .contracts import (
    EVENT_QUOTE,
    EVENT_TICK,
    SourceIdentity,
    semantic_core,
)
from .errors import ContractError, MdAnalyticsError
from .fixtures import Corpus
# Everything this module reads was written by P05, so every digest it computes
# or compares is P05's, never contracts.payload_digest. That function is
# deliberately not imported here: importing both is how they got mixed up.
from .p05_core_v1 import ENCODING as P05_CORE_V1
from .p05_core_v1 import payload_digest as p05_payload_digest
from .rows import RawRow, TopicPartition
from .snapshot_cutter import (
    TERMINAL_P05_STATES,
    AcceptedEvent,
    SnapshotCut,
    SourceManifest,
    TopicPartitionCut,
)

#: The table P05 owns. Overridable, but never guessed at.
DEFAULT_RAW_TABLE = "md_ingest.market_events_raw"

#: The pinned column order of the raw table (section 0.1). Selected explicitly
#: rather than with ``SELECT *``: a column added upstream would otherwise shift
#: every value after it and the rows would parse into the wrong fields.
RAW_COLUMNS: tuple[str, ...] = (
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
    # Ingest wall clock. Read because RawRow requires it, and used for latency
    # only: never a market timestamp, never part of a digest or an ordering
    # decision (P06 section 0.1).
    "ingested_at",
)

#: One SELECT expression per :data:`RAW_COLUMNS` entry, in the same order.
#:
#: Every column reads as itself except ``ingested_at``, which is
#: ``DateTime64(3, 'UTC')`` and therefore arrives as formatted text --
#: ``'2026-10-31 05:35:29.843'`` -- rather than a number. :class:`RawRow`
#: declares it an ``int``, so something has to convert, and the server is the
#: only party that knows the column's type and timezone: parsing that text in
#: Python would hard-code a format ClickHouse is free to change and would
#: guess at the unit. ``Milli`` matches the declared scale of 3 exactly;
#: ``Nano`` would widen it and fabricate precision the column never stored.
#:
#: The model backend fills the same field from a monotonic counter, so the two
#: backends do not agree on a unit. That is sound only because nothing compares
#: or arithmetises this value -- see the note above -- and it is why this field
#: stays out of :data:`~md_analytics.contracts.SEMANTIC_CORE_FIELDS`.
RAW_SELECT: tuple[str, ...] = tuple(
    "toUnixTimestamp64Milli(%s)" % name if name == "ingested_at" else name
    for name in RAW_COLUMNS
)

#: P03's spellings for an event it forwarded. ``none`` is its clean-forward
#: value, and the gap reasons are forwards too (its ADR-07): a discontinuity is
#: a property of the series, not a defect in the event.
FORWARDED_REASONS: frozenset[str] = frozenset(
    {
        "none",
        "NONE",
        "HEALTHY",
        "SEQUENCE_GAP",
        "HUGE_GAP",
        "INITIAL_GAP",
        "CONTINUITY_LOST",
        "SEQUENCE_ROLLBACK",
        "DEGRADED",
        "RECOVERED",
        "RECOVERING",
    }
)


class RawSourceError(MdAnalyticsError):
    """The real source could not be read, so no build may be attempted."""

    code = "RAW_SOURCE_UNREADABLE"


def _as_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise RawSourceError("%s is a boolean, expected an integer" % field)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError as exc:
            raise RawSourceError("%s is not an integer: %r" % (field, value)) from exc
    raise RawSourceError("%s is missing or not an integer: %r" % (field, value))


def _as_optional_int(value: Any, field: str) -> int | None:
    if value is None or value in ("", "\\N"):
        return None
    return _as_int(value, field)


def _as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise RawSourceError("event_date is not ISO-8601: %r" % value) from exc
    raise RawSourceError("event_date is missing: %r" % value)


def _row_from_values(values: Sequence[Any]) -> RawRow:
    if len(values) != len(RAW_COLUMNS):
        raise RawSourceError(
            "expected %d columns, got %d; the raw table shape has changed"
            % (len(RAW_COLUMNS), len(values))
        )
    record = dict(zip(RAW_COLUMNS, values))
    kind = str(record["event_kind"])
    if kind not in (EVENT_TICK, EVENT_QUOTE):
        raise RawSourceError("unknown event_kind %r in the raw table" % kind)
    return RawRow(
        dataset_id=str(record["dataset_id"]),
        symbol=str(record["symbol"]),
        sequence=_as_int(record["sequence"], "sequence"),
        event_kind=kind,
        event_time_ns=_as_int(record["event_time_ns"], "event_time_ns"),
        event_date=_as_date(record["event_date"]),
        price_scale=_as_int(record["price_scale"], "price_scale"),
        bid_ticks=_as_optional_int(record["bid_ticks"], "bid_ticks"),
        ask_ticks=_as_optional_int(record["ask_ticks"], "ask_ticks"),
        price_ticks=_as_optional_int(record["price_ticks"], "price_ticks"),
        source_schema_version=str(record["source_schema_version"]),
        payload_digest=str(record["payload_digest"]),
        kafka_topic=str(record["kafka_topic"]),
        kafka_partition=_as_int(record["kafka_partition"], "kafka_partition"),
        kafka_offset=_as_int(record["kafka_offset"], "kafka_offset"),
        # Carried as P05 stores it. See the module docstring: not coerced.
        topic_generation=record["topic_generation"],
        ingest_run_id=str(record["ingest_run_id"]),
        ingested_at=_as_int(record["ingested_at"], "ingested_at"),
    )


def load_raw_rows(
    client: Any, *, dataset_id: str, table: str = DEFAULT_RAW_TABLE
) -> list[RawRow]:
    """Read every physical row of one dataset, ordered deterministically.

    Ordered by the Kafka coordinate rather than by identity, so repeated runs
    read the same rows in the same order and a build is reproducible. Physical
    duplicates are returned as stored: deduplication is the build's job, and
    hiding them here would hide a conflict too.
    """
    if not dataset_id:
        raise RawSourceError(
            "a dataset_id is required; refusing to read the whole table"
        )

    sql = (
        "SELECT %s FROM %s WHERE dataset_id = {dataset_id:String} "
        "ORDER BY kafka_topic, kafka_partition, kafka_offset"
        % (", ".join(RAW_SELECT), table)
    )
    try:
        rows = client.query_rows(sql, params={"dataset_id": dataset_id})
    except Exception as exc:  # the driver raises its own types
        raise RawSourceError("cannot read %s: %s" % (table, exc)) from exc
    return [_row_from_values(values) for values in rows]


def accepted_from_p03_handoff(path: str | Path) -> tuple[AcceptedEvent, ...]:
    """Read P03's accepted-event ledger from its p04_stub handoff stream.

    One record per event the adapter forwarded. Events P03 suppressed never
    reach the sink and so are absent here, which is exactly right: they must not
    be expected in raw.
    """
    source = Path(path)
    try:
        text = source.read_text(encoding="utf-8")
    except OSError as exc:
        raise RawSourceError("cannot read the P03 handoff ledger: %s" % exc) from exc

    accepted: list[AcceptedEvent] = []
    for number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RawSourceError(
                "P03 handoff line %d is not JSON: %s" % (number, exc)
            ) from exc

        quality = record.get("quality") or {}
        if str(quality.get("primary_reason", "none")) not in FORWARDED_REASONS:
            continue

        event_id = quality.get("event_id") or {}
        missing = [k for k in ("dataset_id", "symbol", "sequence") if k not in event_id]
        if missing:
            raise RawSourceError(
                "P03 handoff line %d has no %s in quality.event_id"
                % (number, ", ".join(missing))
            )
        # P03 spells the payload arm as "kind"; "event_kind" is accepted too so
        # a future rename on either side does not read as a missing field.
        kind = str(event_id.get("kind") or event_id.get("event_kind") or "")
        if kind not in (EVENT_TICK, EVENT_QUOTE):
            raise RawSourceError(
                "P03 handoff line %d has no recognisable payload kind: %r"
                % (number, kind)
            )

        # The expected digest is computed from what P03 forwarded, not read back
        # from the rows being audited. That is what lets a corrupted row differ
        # from its expectation instead of defining it.
        #
        # In P05's encoding, because it is compared against the digest P05
        # stored. Computed in P06's canonical JSON instead, this matched
        # nothing: all 2,000 identities read as missing and all 85,520 rows as
        # digest mismatches, for a dataset that was completely present.
        try:
            core = semantic_core(
                dataset_id=str(event_id["dataset_id"]),
                symbol=str(event_id["symbol"]),
                sequence=_as_int(event_id["sequence"], "sequence"),
                event_kind=kind,
                event_time_ns=_as_int(event_id["event_time_ns"], "event_time_ns"),
                price_scale=_as_int(event_id["price_scale"], "price_scale"),
                price_ticks=_as_optional_int(
                    event_id.get("price_ticks"), "price_ticks"
                ),
                bid_ticks=_as_optional_int(event_id.get("bid_ticks"), "bid_ticks"),
                ask_ticks=_as_optional_int(event_id.get("ask_ticks"), "ask_ticks"),
                source_schema_version=str(event_id.get("source_schema_version", "")),
            )
        except (KeyError, ContractError) as exc:
            raise RawSourceError(
                "P03 handoff line %d does not carry a complete semantic core: %s"
                % (number, exc)
            ) from exc

        accepted.append(
            AcceptedEvent(
                identity=SourceIdentity(
                    dataset_id=str(event_id["dataset_id"]),
                    symbol=str(event_id["symbol"]),
                    sequence=_as_int(event_id["sequence"], "sequence"),
                ),
                event_kind=kind,
                payload_digest=p05_payload_digest(core),
            )
        )
    if not accepted:
        raise RawSourceError(
            "the P03 handoff ledger yielded no accepted events; refusing to "
            "build against an empty expectation, which would pass trivially"
        )
    return tuple(accepted)


def cut_from_p05_snapshot(
    path: str | Path, *, dataset_id: str, manifest_sha256: str
) -> SnapshotCut:
    """Read the sealed cut from P05's ``snapshot`` output.

    P05 records the log start and the high-water mark per topic-partition. The
    consumer frontier is taken from P05's committed offsets when it reported
    them; otherwise it defaults to the end offset, and
    :meth:`TopicPartitionCut.covers_cut` is what later decides whether that was
    actually covered.
    """
    source = Path(path)
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except OSError as exc:
        raise RawSourceError("cannot read the P05 cut: %s" % exc) from exc
    except json.JSONDecodeError as exc:
        raise RawSourceError("the P05 cut is not JSON: %s" % exc) from exc

    raw_cut = payload.get("cut")
    if not isinstance(raw_cut, dict) or not raw_cut:
        raise RawSourceError("the P05 cut carries no topic-partition entries")

    generation = payload.get("topic_generation", "unknown")
    frontiers = payload.get("committed") or {}

    cuts: list[TopicPartitionCut] = []
    for key, entry in sorted(raw_cut.items()):
        if ":" not in key:
            raise RawSourceError("malformed topic-partition key %r in the cut" % key)
        topic, _, partition_text = key.rpartition(":")
        end_offset = _as_int(entry.get("end"), "cut[%s].end" % key)
        log_start = _as_int(entry.get("log_start", 0), "cut[%s].log_start" % key)
        frontier = frontiers.get(key)
        cuts.append(
            TopicPartitionCut(
                topic_partition=TopicPartition(
                    topic=topic,
                    partition=_as_int(partition_text, "partition"),
                    topic_generation=generation,
                ),
                end_offset=end_offset,
                consumer_frontier=(
                    _as_int(frontier, "committed[%s]" % key)
                    if frontier is not None
                    else end_offset
                ),
                earliest_available_offset=log_start,
            )
        )

    # P05's terminal state decides whether this cut may be sealed at all. It is
    # read from the snapshot rather than assumed: a cut sealed while the
    # consumer was still running would bound nothing.
    p05_state = str(payload.get("p05_state") or payload.get("state") or "")
    if p05_state not in TERMINAL_P05_STATES:
        raise RawSourceError(
            "P05 reports state %r, which is not terminal (%s); a cut taken "
            "mid-run does not bound the source"
            % (p05_state or "unknown", ", ".join(sorted(TERMINAL_P05_STATES)))
        )

    return SnapshotCut(
        dataset_id=dataset_id,
        manifest_sha256=manifest_sha256,
        cuts=tuple(cuts),
        p05_state=p05_state,
        seal_reason=str(payload.get("seal_reason") or "p05-snapshot"),
        captured_at_ns=_as_int(
            payload.get("captured_at_ns", 0), "captured_at_ns"
        ),
    )


def corpus_from_raw(
    *,
    dataset_id: str,
    raw_rows: Iterable[RawRow],
    accepted: Sequence[AcceptedEvent],
    cut: SnapshotCut,
    declared_sha256: str | None = None,
    generator_version: str = "p05-raw-table",
) -> Corpus:
    """Assemble a :class:`Corpus` whose rows came from the real landing table."""
    rows = list(raw_rows)
    if not rows:
        raise RawSourceError(
            "no raw rows for dataset %s; a build over an empty table would "
            "publish an empty READY pointer" % dataset_id
        )
    off_dataset = {row.dataset_id for row in rows} - {dataset_id}
    if off_dataset:
        raise RawSourceError(
            "rows for other datasets leaked into the read: %s" % sorted(off_dataset)
        )
    return Corpus(
        dataset_id=dataset_id,
        manifest=SourceManifest(
            dataset_id=dataset_id,
            accepted=tuple(accepted),
            declared_sha256=declared_sha256,
            generator_version=generator_version,
        ),
        raw_rows=rows,
        cut=cut,
        # These rows came out of P05's table, so their stored digests are
        # P05's. Declared rather than defaulted: audit_raw recomputes under
        # whatever the corpus says, and the default is P06's own encoding.
        digest_encoding=P05_CORE_V1,
    )
