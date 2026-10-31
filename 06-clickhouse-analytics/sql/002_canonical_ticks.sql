-- 002_canonical_ticks.sql -- ticks only, one row per source identity per build.
--
-- MergeTree does NOT enforce uniqueness: ORDER BY is a sorting key and an index,
-- not a constraint (section 0.2). The "exactly one row per identity" invariant
-- (INV-04) is produced by canonical_builder.py and *proved afterwards* by
-- 006_audit_queries.sql; this DDL only makes the proof cheap to run.
--
-- build_id leads the sorting key so snapshots stay physically separated and a
-- reader pinned to one build never pays for the others. Queries must supply
-- build_id + dataset_id + symbol + a time range; dropping the prefix invites a
-- full scan (section 6, group N).
--
-- No TTL clause here on purpose. TTL is enabled only after the retention gate
-- in section 9 passes (NFR-11): retention must exceed the recovery, audit,
-- rebuild and late-arrival windows, and a restore drill must have run.

CREATE TABLE IF NOT EXISTS md_analytics.canonical_ticks
(
    build_id              String,
    dataset_id            String,
    symbol                LowCardinality(String),
    sequence              Int64,
    event_time_ns         Int64,
    -- UTC date re-derived from event_time_ns by the builder, never copied from
    -- raw: a date computed in the server's local zone would carry a wrong
    -- partition past the audit (case B-03, D-08).
    event_date            Date,
    price_scale           Int64,
    price_ticks           Int64,
    payload_digest        FixedString(64),
    source_schema_version LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (build_id, dataset_id, symbol, event_time_ns, sequence);
