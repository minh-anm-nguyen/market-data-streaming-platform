-- 006_audit_queries.sql -- read-only audit and operations SQL.
--
-- Everything here is offline/admin scope. uniqExact and GROUP BY over a source
-- identity are expensive on 200M rows (section 4.5), so these run under the
-- audit resource profile, bounded, and never behind a client-facing endpoint.
--
-- None of these queries *proves* correctness on its own. They are detectors:
-- the proof is the full-field comparison against the independent oracle in
-- src/md_analytics/bars_verifier.py. A COUNT that matches expectation says
-- nothing about whether the right rows are present (section 0.2).

-- name: raw_identity_conflicts
-- Same identity, more than one distinct semantic digest. Any row returned is a
-- CONFLICT and blocks publication: no last-write-wins, no argMax(ingested_at)
-- (case E-02..E-04).
SELECT
    dataset_id,
    symbol,
    sequence,
    count()                   AS physical_count,
    uniqExact(payload_digest) AS digest_variants
FROM md_ingest.market_events_raw
WHERE dataset_id = {dataset_id:String}
GROUP BY dataset_id, symbol, sequence
HAVING digest_variants > 1
ORDER BY symbol, sequence
LIMIT {row_limit:UInt32};

-- name: raw_physical_duplicates
-- Same identity, one digest, several physical rows. Expected under
-- at-least-once delivery; must be classified, not treated as corruption, and
-- must not reach the canonical table twice (NFR-02).
SELECT
    dataset_id,
    symbol,
    sequence,
    count() AS physical_count
FROM md_ingest.market_events_raw
WHERE dataset_id = {dataset_id:String}
GROUP BY dataset_id, symbol, sequence
HAVING physical_count > 1
ORDER BY symbol, sequence
LIMIT {row_limit:UInt32};

-- name: canonical_duplicate_identities
-- INV-04. Must return zero rows for a build to be publishable.
SELECT
    dataset_id,
    symbol,
    sequence,
    count() AS n
FROM md_analytics.canonical_ticks
WHERE build_id = {build_id:String}
GROUP BY dataset_id, symbol, sequence
HAVING n != 1
ORDER BY symbol, sequence
LIMIT {row_limit:UInt32};

-- name: canonical_event_date_drift
-- event_date must equal the UTC date of event_time_ns. A row here means a
-- partition was computed in the wrong zone (case B-03, D-08).
SELECT
    dataset_id,
    symbol,
    sequence,
    event_date,
    toDate(toDateTime(intDiv(event_time_ns, 1000000000), 'UTC')) AS derived_date
FROM md_analytics.canonical_ticks
WHERE build_id = {build_id:String}
  AND event_date != toDate(toDateTime(intDiv(event_time_ns, 1000000000), 'UTC'))
LIMIT {row_limit:UInt32};

-- name: canonical_scale_variants
-- More than one price_scale for one (dataset, symbol) inside a build. MVP
-- policy is STOP: an OHLC bar spanning two scales has no defined unit
-- (section 3.2, case C-03).
SELECT
    dataset_id,
    symbol,
    uniqExact(price_scale) AS scale_variants,
    groupUniqArray(price_scale) AS scales
FROM md_analytics.canonical_ticks
WHERE build_id = {build_id:String}
GROUP BY dataset_id, symbol
HAVING scale_variants > 1
LIMIT {row_limit:UInt32};

-- name: bar_state_row_count
-- Physical state rows before merging. Evidence that *Merge is load-bearing: if
-- this exceeds the finalized bar count, an un-merged read would be wrong
-- (case I-01, E2E-20).
SELECT count() AS state_rows
FROM md_analytics.bars_1m_state
WHERE build_id = {build_id:String};

-- name: active_parts_by_partition
-- Part pressure (NFR-08). A rising trend in steady state means batching or the
-- partition strategy needs changing -- not an OPTIMIZE ... FINAL in cron
-- (section 6, ADR-010).
SELECT
    database,
    table,
    partition,
    count()             AS active_parts,
    sum(rows)           AS rows,
    sum(bytes_on_disk)  AS bytes_on_disk
FROM system.parts
WHERE active = 1
  AND database IN ('md_ingest', 'md_analytics')
GROUP BY database, table, partition
ORDER BY active_parts DESC
LIMIT {row_limit:UInt32};

-- name: slow_queries
-- query_log is asynchronous and local to a node, and is empty if logging is
-- disabled -- absence of rows here is not evidence of absence of slow queries
-- (section 11).
SELECT
    query_duration_ms,
    read_rows,
    read_bytes,
    memory_usage,
    query_id
FROM system.query_log
WHERE type = 'QueryFinish'
  AND event_time >= now() - INTERVAL {lookback_hours:UInt32} HOUR
ORDER BY query_duration_ms DESC
LIMIT {row_limit:UInt32};

-- name: table_columns
-- Schema preflight input: names, types and positions of a table's columns,
-- read from system.columns rather than trusting that CREATE TABLE IF NOT
-- EXISTS created what this repo expects (case B-09).
SELECT
    name,
    type,
    position
FROM system.columns
WHERE database = {database:String}
  AND table = {table:String}
ORDER BY position;

-- name: table_engine
SELECT
    engine,
    partition_key,
    sorting_key
FROM system.tables
WHERE database = {database:String}
  AND name = {table:String};
