-- Data-quality audit queries (spec 6.3).
--
-- Read these in order. The later queries are only meaningful once the earlier
-- gates pass, and each one says which.
--
-- These are the SQL counterpart of src/p05_ingest/reconciler.py. The reconciler
-- is the authority for a GO decision because it also compares against the
-- source manifest and a frozen Kafka cut; these queries answer the narrower
-- question of what the table itself contains.

-- ---------------------------------------------------------------------------
-- 1. Physical duplicates: one logical identity, one semantic payload, several
--    rows. Expected under at-least-once delivery. Report, do not panic.
-- ---------------------------------------------------------------------------
SELECT dataset_id, symbol, sequence,
       count() AS physical_rows,
       uniqExact(payload_digest) AS variants
FROM md_ingest.market_events_raw
WHERE dataset_id = {dataset_id:String}
GROUP BY dataset_id, symbol, sequence
HAVING physical_rows > 1
ORDER BY physical_rows DESC
LIMIT 100;

-- ---------------------------------------------------------------------------
-- 2. Identity conflicts: one identity, more than one semantic payload. P0.
--    There is no correct automatic resolution. Picking the highest offset or
--    the latest ingested_at would be inventing an answer, so quarantine the
--    dataset and reconcile against the source instead.
-- ---------------------------------------------------------------------------
SELECT dataset_id, symbol, sequence,
       uniqExact(payload_digest) AS variants,
       groupUniqArray(10)(payload_digest) AS digests,
       groupUniqArray(10)(event_kind) AS kinds
FROM md_ingest.market_events_raw
WHERE dataset_id = {dataset_id:String}
GROUP BY dataset_id, symbol, sequence
HAVING variants > 1
ORDER BY variants DESC;

-- ---------------------------------------------------------------------------
-- 3. The conflict gate. Everything below is valid ONLY when this returns 0.
-- ---------------------------------------------------------------------------
SELECT countIf(variants > 1) AS conflict_count
FROM
(
    SELECT uniqExact(payload_digest) AS variants
    FROM md_ingest.market_events_raw
    WHERE dataset_id = {dataset_id:String}
    GROUP BY dataset_id, symbol, sequence
);

-- ---------------------------------------------------------------------------
-- 4. Logical event count. Not the same number as count(*), and the difference
--    is the duplicate count rather than an error.
--
--    Scope this by dataset. An unbounded uniqExact over billions of rows is an
--    offline reconciliation job, not an online query (spec 6.3).
-- ---------------------------------------------------------------------------
SELECT uniqExact(tuple(dataset_id, symbol, sequence)) AS logical_events,
       count() AS physical_rows,
       count() - uniqExact(tuple(dataset_id, symbol, sequence)) AS duplicate_rows
FROM md_ingest.market_events_raw
WHERE dataset_id = {dataset_id:String};

-- ---------------------------------------------------------------------------
-- 5. Canonical view: one row per logical event.
--    any() is valid only once query 3 returned 0. With a conflict present it
--    silently picks one of several different payloads.
-- ---------------------------------------------------------------------------
SELECT dataset_id, symbol, sequence,
       any(event_kind)     AS event_kind,
       any(event_time_ns)  AS event_time_ns,
       any(price_scale)    AS price_scale,
       any(bid_ticks)      AS bid_ticks,
       any(ask_ticks)      AS ask_ticks,
       any(price_ticks)    AS price_ticks,
       any(payload_digest) AS payload_digest
FROM md_ingest.market_events_raw
WHERE dataset_id = {dataset_id:String}
GROUP BY dataset_id, symbol, sequence
ORDER BY symbol, event_time_ns, sequence;

-- ---------------------------------------------------------------------------
-- 6. Part pressure. Watch this against the batching trade-off in spec 10.3:
--    small batches mean many parts and merge pressure, which can throttle the
--    whole table while the insert rate still looks healthy.
-- ---------------------------------------------------------------------------
SELECT partition, count() AS parts, sum(rows) AS rows, sum(bytes_on_disk) AS bytes
FROM system.parts
WHERE database = 'md_ingest' AND table = 'market_events_raw' AND active
GROUP BY partition
ORDER BY parts DESC
LIMIT 50;

-- ---------------------------------------------------------------------------
-- 7. Per-run provenance, for reconciling one ingestion run against Kafka.
-- ---------------------------------------------------------------------------
SELECT ingest_run_id, topic_generation, kafka_topic, kafka_partition,
       min(kafka_offset) AS first_offset,
       max(kafka_offset) AS last_offset,
       count() AS rows
FROM md_ingest.market_events_raw
WHERE ingest_run_id = {ingest_run_id:String}
GROUP BY ingest_run_id, topic_generation, kafka_topic, kafka_partition
ORDER BY kafka_topic, kafka_partition;

-- ---------------------------------------------------------------------------
-- 8. Event-time sanity. Spec 3.3: ingested_at is NOT market chronology, so a
--    large spread between the two is information about ingestion lag, not a
--    reordering of the market.
-- ---------------------------------------------------------------------------
SELECT symbol,
       min(event_time_ns) AS first_event_time_ns,
       max(event_time_ns) AS last_event_time_ns,
       min(ingested_at)   AS first_ingested_at,
       max(ingested_at)   AS last_ingested_at,
       count()            AS rows
FROM md_ingest.market_events_raw
WHERE dataset_id = {dataset_id:String}
GROUP BY symbol
ORDER BY symbol;
