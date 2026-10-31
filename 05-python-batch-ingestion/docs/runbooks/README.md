# Runbooks

Incident procedures for the failure modes this system actually produces. Each
one says what the alert means, what to do, and — just as important — what *not*
to do, because several of these failures have an obvious-looking action that
loses data.

**The rule that overrides everything below: never hand-edit a Kafka offset to
make a problem go away.** A committed offset is a claim that the data behind it
is in ClickHouse. Moving it forward to clear an alert converts a visible stall
into silent loss.

---

## RUNBOOK-01 — ClickHouse timeout, ACK ambiguous

**Alert.** `p05_clickhouse_batches_unknown_total` rising, or a partition blocked
with `ambiguous` in its reason.

**What it means.** A batch was sent and the response never arrived. The rows may
or may not be in the table. The client genuinely cannot tell.

1. Capture the evidence from the blocked-partition reason and the metrics:
   `topic`, `partition`, `first_offset`, `last_offset`, `batch_id`,
   `insert_token`, attempt count. Do **not** log the raw market payload.
2. Check whether the write landed:
   ```sql
   SELECT count(), uniqExact(payload_digest)
   FROM md_ingest.market_events_raw
   WHERE ingest_run_id = {run_id:String}
     AND kafka_topic = {topic:String}
     AND kafka_partition = {partition:Int32}
     AND kafka_offset BETWEEN {first:Int64} AND {last:Int64};
   ```
3. If the rows are there, the retry will be absorbed by block de-duplication
   (if still inside the window) or produce a physical duplicate (if not). Both
   are acceptable; restart the consumer.
4. If the rows are not there, investigate the sink before restarting. The
   partition is paused and the frontier is pinned, so nothing is being lost
   while you look.
5. After recovery, run `tools/verifier.py` and confirm `conflict_count = 0`.

**Do not** commit past the ambiguous batch. **Do not** assume the timeout means
the write failed.

---

## RUNBOOK-02 — Consumer rebalance or lost assignment

**Alert.** `p05_rebalances_total{kind="lost"}` above zero, or a run reporting
`lost ownership without commit`.

**What it means.** A partition was taken away. Any insert already dispatched for
it can still land in ClickHouse — that cannot be retracted — but this consumer
has no right to commit it.

1. Expect duplicates. The new owner re-reads from the last committed offset, so
   the window between the last commit and the loss is processed twice.
2. Confirm no conflicts were produced, using query 3 of
   `sql/002_audit_queries.sql`. Duplicates are fine; more than one digest per
   identity is not.
3. Check why it happened. Frequent `lost` events usually mean the poll loop
   stalled past `max.poll.interval.ms`. Look at
   `p05_clickhouse_insert_latency_seconds` and the retry deadline — `config.py`
   enforces a 3x margin at startup, so a breach means the sink became far slower
   than the configuration assumed.
4. The run is reported `INCOMPLETE` even if the data later came out right. That
   is deliberate: the process cannot prove it missed nothing while it did not own
   the partition. Clear it by reconciling, not by reinterpreting the verdict.

**Do not** raise `max.poll.interval.ms` as a first response. It hides the
latency problem and lengthens every future recovery.

---

## RUNBOOK-03 — Retention exhausted, offsets out of range

**Alert.** The run stops with `RETENTION_LOST`, or
`p05_estimated_lag_age_seconds` approaching the retention window.

**What it means.** Kafka has deleted records the group still needed. This is
**unrecoverable from Kafka alone**.

1. Record the exact gap. For each affected partition:
   ```
   lost = range(committed_next, log_start_offset)
   ```
   `tools/verifier.py` reports the same set as `MISSING_FROM_SINK`.
2. Declare the dataset `NO-GO`. Do not publish analytics derived from it.
3. Recover from upstream: re-publish the missing range from a verified P01
   manifest or a retained P04 source. If neither exists, the data is gone and
   the report must say so.
4. Before resuming, fix the cause: retention headroom must exceed
   recovery time + maximum outage + margin (spec NFR-12). `doctor --live`
   reports the current margin.

**Do not** set `auto.offset.reset` to `latest` or `earliest` to clear the error.
Startup refuses those values precisely because they turn this visible failure
into a silent skip.

---

## RUNBOOK-04 — Poison record or schema mismatch

**Alert.** A partition blocked with a validation reason code, or
`p05_records_validated_total{result="SCHEMA_UNKNOWN_FIELDS"}` rising.

**What it means.** A record violated the input contract. Under the default
fail-closed policy its partition has stopped and nothing after it is committed.

1. Read the reason code from the blocked-partition message. Codes are stable and
   name the rule that was broken: `PROTO_DECODE_ERROR`, `KEY_SYMBOL_MISMATCH`,
   `QUOTE_CROSSED`, `SCHEMA_UNKNOWN_FIELDS`, and so on.
2. For `SCHEMA_UNKNOWN_FIELDS`: a producer is running a newer schema. Do not
   work around it. Either roll the producer back, or pin the new descriptor
   after a compatibility review and re-run `descriptor-hash`.
3. For a genuine data fault: take it up with P03/P04. The record should not have
   been published as market-valid.
4. For a deliberately injected fault: it belongs in the isolated fault lane
   (`mode = "isolated_fault_test"` with its own table), never in the market lane.
5. If the record must be skipped, enable the durable DLQ in an isolated profile.
   The skip only takes effect after the quarantine write is acknowledged.

**Do not** relax `strict_market_validation` to get a run through. **Do not**
delete the record from Kafka and pretend it was never published.

---

## RUNBOOK-05 — Too many parts, or disk pressure in ClickHouse

**Alert.** `p05_ch_parts_active` rising over a steady window, inserts returning
`too many parts`, or a growing merge backlog.

**What it means.** Inserts are too small and too frequent for the merge
throughput of this table. Note that this arrives as a `TRANSIENT` sink error, so
the consumer pauses and retries rather than failing — the backlog moves to
Kafka, which is bounded by retention.

1. Check parts per partition (query 6 of `sql/002_audit_queries.sql`).
2. Increase `per_partition_max_rows` and `per_partition_max_serialized_bytes`,
   or `max_wait_ms`, so each insert carries more rows. `tools/benchmark.py`
   prints the inserts-versus-parts trade-off directly.
3. Check whether batches span many `event_date` partitions. A batch covering
   several days creates a part per day. Compare day against month partitioning
   on the pinned server.
4. Watch retention headroom while tuning: the backlog is now in Kafka, and
   RUNBOOK-03 is what happens if it outlives the retention window.

**Do not** run `OPTIMIZE ... FINAL` to clear parts under load. It competes with
the merges that are already behind.

---

## RUNBOOK-06 — `commit_next > proven_frontier_next` (P0)

**Alert.** `InvariantViolation: I-01 VIOLATION` in the log, or the alert on
`p05_committed_next_offset > p05_proven_frontier_next_offset`.

**What it means.** A bug in this program committed an offset whose data was
never proven to be in ClickHouse. This is the data-loss failure the whole design
exists to prevent.

1. Freeze the release. Do not restart the consumer: a restart resumes from the
   bad offset and completes the loss.
2. Capture everything — the full log, the metrics snapshot, the run report,
   `tools/verifier.py` output, and the committed offsets per partition.
3. Determine the loss range by reconciling Kafka against ClickHouse for each
   affected partition.
4. Recover the range from upstream, as in RUNBOOK-03.
5. Treat it as `OVERALL_GO = NO-GO` until a regression test reproduces the bug
   and the fix makes that test go from red to green. The engine raises rather
   than continuing here specifically so the evidence survives.
