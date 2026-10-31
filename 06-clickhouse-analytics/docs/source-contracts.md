# Inherited contracts (P01 → P05) and what P06 may assume

Section 0.1 of the system design, restated as the interface this implementation
is written against. **P06 does not change any of it.** Where a contract is
ambiguous, the implementation refuses rather than guesses.

## The raw table P06 reads

`md_ingest.market_events_raw`, owned by Project 05. P06 reads it and never
creates, alters or drops it (FR-01).

| Column | Type | Notes |
|---|---|---|
| `dataset_id` | `String` | part of the logical identity |
| `symbol` | `LowCardinality(String)` | part of the logical identity |
| `sequence` | `Int64` | part of the logical identity; increases **per symbol**, across both tick and quote |
| `event_kind` | `LowCardinality(String)` | `tick` or `quote` |
| `event_time_ns` | `Int64` | nanoseconds since the Unix epoch, **UTC** |
| `event_date` | `Date` | must equal the UTC date of `event_time_ns` |
| `price_scale` | `Int64` | strictly positive |
| `bid_ticks`, `ask_ticks` | `Nullable(Int64)` | quotes only |
| `price_ticks` | `Nullable(Int64)` | ticks only |
| `source_schema_version` | `LowCardinality(String)` | `marketdata.v1` |
| `payload_digest` | `FixedString(64)` | SHA-256 hex of the semantic core |
| `kafka_topic`, `kafka_partition`, `kafka_offset`, `topic_generation` | | **transport**, not identity |
| `ingest_run_id` | `String` | lineage |
| `ingested_at` | `DateTime64(3, 'UTC')` | ingest clock — latency only, **never a market timestamp** |

Engine `MergeTree`, `PARTITION BY toYYYYMMDD(event_date)`,
`ORDER BY (symbol, event_time_ns, dataset_id, sequence, kafka_topic, kafka_partition, kafka_offset)`.

The expected shape is pinned in `src/md_analytics/schema_preflight.py` and
compared against `system.columns` — not against the success of a
`CREATE TABLE IF NOT EXISTS`, which happily succeeds against a table with a
different definition (case B-09).

## Identity

```
source_identity = (dataset_id, symbol, sequence)
source_order    = (event_time_ns, sequence)     # within one (dataset_id, symbol)
physical_record = source_identity + kafka_topic + kafka_partition + kafka_offset + topic_generation
```

| Situation | Classification | Consequence |
|---|---|---|
| Same identity, same semantic core | physical duplicate | normal; collapses to one canonical row |
| Same identity, different semantic core | `CONFLICT` | build stops; no winner is picked |
| Same identity, stored digest ≠ recomputed | invalid row | rejected before comparison |
| Different `dataset_id`, same symbol/sequence | two different events | both retained, scoped separately |
| Offset at or past the sealed cut | out of scope | belongs to the next build |

## What the semantic core contains

The digest covers exactly what *happened*: dataset, symbol, sequence, kind,
event time, scale, the price fields appropriate to the kind, and the schema
version. Field **presence** is part of it — a tick carrying `bid_ticks` is a
contract violation, not something to normalise.

It deliberately excludes `ingested_at`, `kafka_*` and `topic_generation`, so a
replay of the same event through a new topic generation has the same digest
(cases A-13, B-10, E-11).

## Per-project consequences

| Source | Contract | What P06 must therefore do |
|---|---|---|
| P01 simulator | sequence increases per symbol across tick *and* quote; finite dataset with a manifest | never treat a gap in the tick-only subsequence as a missing row (case A-02) |
| P02 Protobuf | `oneof tick(price_ticks)` / `quote(bid_ticks, ask_ticks)` | a tick is a **price update, not a trade**; no volume may be derived |
| P03 adapter | publishes an accepted-event ledger; may suppress invalid events | the expected set is the ledger, not everything P01 generated (case A-04) |
| P04 Kafka | `md.ticks.v1`, `md.quotes.v1`, keyed by symbol, **8 partitions per topic**; no total order across topics | order by `(event_time_ns, sequence)`, cut per topic-partition (cases A-14, F-04) |
| P05 ingestion | at-least-once; physical duplicates and identity conflicts can exist | prove logical uniqueness *before* aggregation; never "clean up" raw |

## Interface limits

P06 does **not**: move Kafka offsets, commit on P05's behalf, treat
`ingested_at` as a market timestamp, modify raw to repair a defect, or describe
the Kafka → ClickHouse path as exactly-once. PostgreSQL appears in the CV but
its role here is unconfirmed, and the MVP does not use it.

## Price and time rules

- Prices are **integer ticks** with an explicit `price_scale`. `float` never
  carries a value a verdict depends on. The decimal form is produced by integer
  arithmetic for display only (`display_price`).
- `price_scale` must be positive. Two scales for one `(dataset, symbol)` inside a
  build is a **STOP**: a bar spanning both has no defined unit (case C-03).
- `event_time_ns` is `Int64` UTC nanoseconds in `[0, 2^63-1]`. Negative is
  outside the P01/P05 domain; beyond `Int64` is rejected, never wrapped.
- `event_date` is re-derived from `event_time_ns` by the builder rather than
  copied, so a date computed in a local zone cannot be carried past the audit.
- Bucketing is `(ns // 60_000_000_000) * 60_000_000_000`. No `DateTime64(3)`
  intermediate: a downcast both mis-buckets an event one nanosecond before a
  minute edge and destroys the tie-break inside a millisecond.

## Handed to P07 / P08 / P09

1. `build_catalog` manifests: source cut, `build_id`, DDL and SQL digests, audit
   verdicts, counters, environment fingerprint, tamper evidence.
2. `md_analytics.fixtures` — the golden corpus and a deterministic generator with
   injectors for duplicates, conflicts, missing, extra, late and out-of-cut rows.
3. `sql/` with per-file and whole-bank digests.
4. `tests/registry/` — the 420-case matrix, the 32 E2E table, and a generated
   coverage report naming every case id and its coverage kind.
5. The invariant checker (INV-01 … INV-14) and the evidence-verdict rules.
