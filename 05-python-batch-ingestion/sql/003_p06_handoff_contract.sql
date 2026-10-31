-- The P06 handoff contract (spec 17.6, 6.2).
--
-- What P05 hands over, and the one thing P06 must not do with it.
--
-- HANDED OVER
--   * md_ingest.market_events_raw, as defined in 001_create_raw_table.sql.
--   * The logical event identity (dataset_id, symbol, sequence).
--   * The P05-CORE-V1 payload_digest, which makes a duplicate distinguishable
--     from a conflict.
--   * event_time_ns as an exact integer, plus a UTC event_date.
--   * Kafka provenance, so any finding can be traced back to a record.
--
-- NOT HANDED OVER
--   * Any guarantee that the table holds one row per event. It does not.
--   * Any aggregate, bar or materialized view. P05 creates none, by design.
--
-- THE TRAP, STATED PLAINLY
--   A materialized view that computes COUNT, SUM, or anything derived from
--   them straight off market_events_raw double-counts every retry duplicate.
--   MIN and MAX happen to survive duplication; COUNT and SUM do not. An OHLC
--   bar mixes both, so the bar as a whole is unsafe.
--
--   tests/integration/test_rollup_trap.py demonstrates this with real
--   duplicated rows: the naive rollup reports twice the true count and twice
--   the true sum, while the mean survives -- which is why a sanity check on
--   averages does not catch it.

-- ---------------------------------------------------------------------------
-- STEP 1: the gate. Refuse to build anything while a conflict exists.
--         A conflict means one identity carries two different payloads, and no
--         aggregate over it has a defined value.
-- ---------------------------------------------------------------------------
SELECT countIf(variants > 1) AS conflict_count
FROM
(
    SELECT uniqExact(payload_digest) AS variants
    FROM md_ingest.market_events_raw
    WHERE event_date BETWEEN {from_date:Date} AND {to_date:Date}
    GROUP BY dataset_id, symbol, sequence
);
-- Proceed only when conflict_count = 0.

-- ---------------------------------------------------------------------------
-- STEP 2: the canonical staging layer. One row per logical event.
--         This, not the raw table, is what an aggregate may read.
--
--         Build it as a materialized view only if the de-duplication is part
--         of the view. A view over the raw table that groups by identity at
--         *query* time is correct; one that pre-aggregates at *insert* time
--         sees one block at a time and cannot de-duplicate across blocks.
-- ---------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS md_ingest.market_events_canonical AS
SELECT dataset_id,
       symbol,
       sequence,
       any(event_kind)     AS event_kind,
       any(event_time_ns)  AS event_time_ns,
       any(event_date)     AS event_date,
       any(price_scale)    AS price_scale,
       any(bid_ticks)      AS bid_ticks,
       any(ask_ticks)      AS ask_ticks,
       any(price_ticks)    AS price_ticks,
       any(payload_digest) AS payload_digest,
       count()             AS physical_rows
FROM md_ingest.market_events_raw
GROUP BY dataset_id, symbol, sequence;

-- ---------------------------------------------------------------------------
-- STEP 3: a safe one-minute bar, over the canonical layer.
--         Correct because every identity contributes exactly one row.
--         The same query against market_events_raw would be wrong.
-- ---------------------------------------------------------------------------
SELECT symbol,
       toStartOfMinute(toDateTime64(event_time_ns / 1000000000, 9, 'UTC')) AS bucket,
       argMin(price_ticks, event_time_ns) AS open_ticks,
       max(price_ticks)                   AS high_ticks,
       min(price_ticks)                   AS low_ticks,
       argMax(price_ticks, event_time_ns) AS close_ticks,
       count()                            AS tick_count,
       any(price_scale)                   AS price_scale
FROM md_ingest.market_events_canonical
WHERE event_kind = 'tick'
  AND price_ticks IS NOT NULL
  AND event_date BETWEEN {from_date:Date} AND {to_date:Date}
GROUP BY symbol, bucket
ORDER BY symbol, bucket;

-- ---------------------------------------------------------------------------
-- STEP 4: the proof that the naive version is unsafe. Run both and compare.
--         When duplicate_rows > 0 the two tick_count columns disagree, and the
--         raw one is wrong. Keep this query: it is the regression test for the
--         whole handoff.
-- ---------------------------------------------------------------------------
SELECT c.symbol                 AS symbol,
       c.bucket                 AS bucket,
       c.tick_count             AS canonical_count,
       r.tick_count             AS naive_count,
       r.tick_count - c.tick_count AS over_count
FROM
(
    SELECT symbol,
           toStartOfMinute(toDateTime64(event_time_ns / 1000000000, 9, 'UTC')) AS bucket,
           count() AS tick_count
    FROM md_ingest.market_events_canonical
    WHERE event_kind = 'tick' AND event_date BETWEEN {from_date:Date} AND {to_date:Date}
    GROUP BY symbol, bucket
) AS c
INNER JOIN
(
    SELECT symbol,
           toStartOfMinute(toDateTime64(event_time_ns / 1000000000, 9, 'UTC')) AS bucket,
           count() AS tick_count
    FROM md_ingest.market_events_raw
    WHERE event_kind = 'tick' AND event_date BETWEEN {from_date:Date} AND {to_date:Date}
    GROUP BY symbol, bucket
) AS r
ON c.symbol = r.symbol AND c.bucket = r.bucket
WHERE r.tick_count != c.tick_count
ORDER BY over_count DESC
LIMIT 100;

-- ---------------------------------------------------------------------------
-- NOTE ON ReplacingMergeTree
--   Moving the canonical layer to a ReplacingMergeTree is a reasonable
--   optimisation, but it de-duplicates only *eventually*, during merges.
--   Before a merge completes, a query without FINAL still sees both rows. So a
--   ReplacingMergeTree does not remove the need for either the conflict gate
--   or the grouping above; it changes when the work happens, not whether it is
--   needed (spec 6.2).
--
-- NOTE ON late data
--   Partitioning by event_date means a late event lands in an older partition
--   and invalidates any bar already computed for it. A dedup-aware rollup must
--   therefore be re-runnable per partition, not append-only.
