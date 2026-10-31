-- 004_mv_canonical_to_bars.sql -- incremental MV: canonical ticks -> 1m states.
--
-- Source is canonical_ticks, NEVER md_ingest.market_events_raw. A view reading
-- raw would double count_state on every at-least-once retry, and no later
-- ReplacingMergeTree merge or SELECT ... FINAL on the source can undo an
-- aggregate state the view has already appended (section 7.1 items 1 and 2).
--
-- Created WITHOUT POPULATE, and created before the first canonical insert.
-- POPULATE over a live source has version-dependent race behaviour, and a view
-- attached after rows already exist simply does not see them -- which is case
-- G-05: the missing bars are invisible until an independent audit looks.
--
-- GROUP BY carries dataset_id and price_scale as well as symbol. Dropping
-- dataset_id would blend two tenants' identical symbols (case H-07); dropping
-- price_scale would produce a bar whose unit is undefined (case C-03, H-08).

CREATE MATERIALIZED VIEW IF NOT EXISTS md_analytics.mv_canonical_ticks_to_bars_1m
TO md_analytics.bars_1m_state AS
SELECT
    build_id,
    dataset_id,
    symbol,
    price_scale,
    intDiv(event_time_ns, 60000000000) * 60000000000 AS bar_start_ns,
    argMinState(price_ticks, tuple(event_time_ns, sequence)) AS open_state,
    maxState(price_ticks)                                    AS high_state,
    minState(price_ticks)                                    AS low_state,
    argMaxState(price_ticks, tuple(event_time_ns, sequence)) AS close_state,
    countState()                                             AS count_state
FROM md_analytics.canonical_ticks
GROUP BY build_id, dataset_id, symbol, price_scale, bar_start_ns;
