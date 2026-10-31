-- P05 raw ingestion table.
--
-- STATUS: this DDL has NOT been executed against a pinned ClickHouse server in
-- this environment (no server was available). The column list, the types and
-- the explicit insert column order are exercised by the test suite against the
-- in-memory sink; the engine-level settings below must be verified against the
-- pinned server version before any durability or capacity claim is made.
--
-- This program never creates or alters this table at runtime. A program that
-- can reshape its own sink can hide a schema mismatch from the operator, so
-- preflight only ever *checks* (see src/p05_ingest/preflight.py).

CREATE DATABASE IF NOT EXISTS md_ingest;

CREATE TABLE IF NOT EXISTS md_ingest.market_events_raw
(
    -- Logical source identity. The de-duplication key for analytics is
    -- (dataset_id, symbol, sequence). event_kind is deliberately NOT part of
    -- it, so the same identity arriving as both a tick and a quote shows up as
    -- a conflict rather than as two valid events (spec 3.2).
    dataset_id       String,
    symbol           LowCardinality(String),
    sequence         Int64,
    event_kind       Enum8('tick' = 1, 'quote' = 2),

    -- Event time stays an exact nanosecond integer. event_date is derived from
    -- it in UTC by the application, not by the server, so the partition key
    -- cannot drift with the server timezone.
    event_time_ns    Int64,
    event_date       Date,

    -- Prices are integer ticks plus a scale. No Float and no Decimal: a float
    -- here would quietly lose the low digits of a scaled price.
    price_scale      Int64,
    bid_ticks        Nullable(Int64),
    ask_ticks        Nullable(Int64),
    price_ticks      Nullable(Int64),

    source_schema_version LowCardinality(String),

    -- P05-CORE-V1 semantic digest, fixed width lower hex. Two physical rows of
    -- one logical event carry the same digest; two different payloads under one
    -- identity do not, which is what makes the conflict gate possible.
    payload_digest   FixedString(64),

    -- Provenance. Kafka coordinates are metadata and never part of the digest,
    -- so a replayed event keeps its digest while gaining a new coordinate.
    kafka_topic      LowCardinality(String),
    kafka_partition  Int32,
    kafka_offset     Int64,
    topic_generation LowCardinality(String),
    ingest_run_id    String,

    -- Server-assigned. Deliberately excluded from the inserted column list and
    -- from the digest: it differs between retries of one immutable batch.
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (symbol, event_time_ns, dataset_id, sequence, kafka_topic, kafka_partition, kafka_offset)
-- ORDER BY in MergeTree is NOT a uniqueness constraint. This table can and
-- will hold duplicate rows under at-least-once delivery. Logical uniqueness is
-- a query-layer property, not a storage guarantee.
--
-- The de-duplication window below suppresses retries of an identical insert
-- inside a bounded window only. It is an optimisation, not exactly-once: a
-- retry after the window has rotated, or a restart that regroups rows into a
-- different batch, still produces a physical duplicate (spec 6.2, E2E-15).
--
-- Daily partitioning follows the P06 description, but it can create many parts
-- when a batch spans several days. Compare day against month partitioning on
-- the pinned server before deciding (spec 6.1, 10.3).
SETTINGS non_replicated_deduplication_window = 1000;
