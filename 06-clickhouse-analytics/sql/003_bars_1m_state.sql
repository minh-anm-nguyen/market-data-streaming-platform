-- 003_bars_1m_state.sql -- partial aggregate states for 1-minute tick bars.
--
-- Every column is a *state*, not a value. One INSERT block into
-- canonical_ticks produces one state row per bar key; a second block covering
-- the same minute produces another. Background merges combine them eventually,
-- on no schedule anyone may rely on, so readers must finalize with *Merge
-- (004/005 and section 4.3).
--
-- The ordering tuple of argMin/argMax is (event_time_ns, sequence) -- both
-- Int64, no downcast. Casting the nanosecond to DateTime64(3) would make two
-- ticks inside one millisecond tie, and OPEN/CLOSE would stop being
-- deterministic (case D-03, I-08).
--
-- There is no volume column. P02 ticks carry no trade size, so count_state
-- counts price updates; calling it volume would be inventing data (case H-06).
--
-- The partition expression is pinned to UTC. Without the explicit 'UTC', a
-- server in a local zone splits a UTC day across two partitions and the
-- boundary audit of case I-06 has nothing stable to compare.

CREATE TABLE IF NOT EXISTS md_analytics.bars_1m_state
(
    build_id     String,
    dataset_id   String,
    symbol       LowCardinality(String),
    price_scale  Int64,
    bar_start_ns Int64,
    open_state   AggregateFunction(argMin, Int64, Tuple(Int64, Int64)),
    high_state   AggregateFunction(max, Int64),
    low_state    AggregateFunction(min, Int64),
    close_state  AggregateFunction(argMax, Int64, Tuple(Int64, Int64)),
    count_state  AggregateFunction(count)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(toDateTime(intDiv(bar_start_ns, 1000000000), 'UTC'))
ORDER BY (build_id, dataset_id, symbol, price_scale, bar_start_ns);
