-- 005_read_queries.sql -- the only read paths exposed to clients.
--
-- Named blocks, delimited by "-- name: <id>", are loaded by
-- src/md_analytics/sqlbank.py so the service and the SHA in every evidence
-- record refer to the same bytes. A query that is not in this file is not a
-- supported read path.
--
-- Every statement is parameterised with ClickHouse typed placeholders
-- ({x:Int64}). No string interpolation of caller-controlled values anywhere:
-- section 10 and group Y. Note that Int64 parameters are what keep a
-- nanosecond bound exact -- an implicit Float64 cast would silently move
-- 1700000000000000001 (case B-12, D-13).
--
-- No SELECT *, and no path that returns a serialized AggregateFunction state to
-- a client (case I-02): states are only ever finalized server-side.

-- name: bars_1m
-- Finalized 1m OHLC. Correct whether or not background merges have run,
-- because every column is read through its *Merge combinator (section 4.3).
SELECT
    dataset_id,
    symbol,
    price_scale,
    bar_start_ns,
    argMinMerge(open_state)  AS open_ticks,
    maxMerge(high_state)     AS high_ticks,
    minMerge(low_state)      AS low_ticks,
    argMaxMerge(close_state) AS close_ticks,
    countMerge(count_state)  AS tick_count
FROM md_analytics.bars_1m_state
WHERE build_id = {build_id:String}
  AND dataset_id = {dataset_id:String}
  AND symbol = {symbol:String}
  AND bar_start_ns >= {start_ns:Int64}
  AND bar_start_ns < {end_ns:Int64}
GROUP BY dataset_id, symbol, price_scale, bar_start_ns
ORDER BY bar_start_ns, price_scale
LIMIT {row_limit:UInt32};

-- name: canonical_range
-- Canonical ticks in a half-open [start, end) window. The predicate is on
-- event_time_ns, so two adjacent windows partition their union exactly once
-- (case D-12).
SELECT
    event_time_ns,
    sequence,
    price_ticks,
    price_scale
FROM md_analytics.canonical_ticks
WHERE build_id = {build_id:String}
  AND dataset_id = {dataset_id:String}
  AND symbol = {symbol:String}
  AND event_time_ns >= {start_ns:Int64}
  AND event_time_ns < {end_ns:Int64}
ORDER BY event_time_ns, sequence
LIMIT {row_limit:UInt32};

-- name: bars_1m_all_for_build
-- Whole-build bar export for the independent verifier. An audit path, not a
-- serving path: it is unbounded by design and runs under the offline resource
-- profile (section 10).
SELECT
    dataset_id,
    symbol,
    price_scale,
    bar_start_ns,
    argMinMerge(open_state)  AS open_ticks,
    maxMerge(high_state)     AS high_ticks,
    minMerge(low_state)      AS low_ticks,
    argMaxMerge(close_state) AS close_ticks,
    countMerge(count_state)  AS tick_count
FROM md_analytics.bars_1m_state
WHERE build_id = {build_id:String}
GROUP BY dataset_id, symbol, price_scale, bar_start_ns
ORDER BY dataset_id, symbol, bar_start_ns, price_scale;

-- name: canonical_all_for_build
SELECT
    build_id,
    dataset_id,
    symbol,
    sequence,
    event_time_ns,
    event_date,
    price_scale,
    price_ticks,
    payload_digest,
    source_schema_version
FROM md_analytics.canonical_ticks
WHERE build_id = {build_id:String}
ORDER BY dataset_id, symbol, event_time_ns, sequence;
