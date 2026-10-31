# Benchmarks — method, and what has not been measured

Section 12 and 18. **No performance claim in this repository has been measured
against ClickHouse.** This document records the method and the rules for
reporting, so that when a run does happen the result means something.

## What the CV says, and what that is worth here

The programme description gives two numbers: **120,000 ticks/s on 8 partitions**
and **p95 range query 2.4 s → 280 ms on 200M rows**. It gives no SQL, no
hardware, no workload and no cache policy.

A run here is therefore a **new benchmark**, not a reproduction. `verdict_for_target`
labels every result `measured_on_new_profile` unless the caller explicitly asserts
the whole reference environment is in place, which on a developer machine it is
not.

Two further distinctions the design insists on:

- **120k events/s total ≠ 120k ticks/s.** The P04/P05 profiles in this programme
  use 120k *events*/s including quotes. Matching the CV figure needs a separate
  tick-only profile on the 8 partitions of `md.ticks.v1`, reporting the quote rate
  and the total separately.
- **8 partitions per topic ≠ 8 in total.** Two topics is sixteen partitions. A
  test asserts this (`test_eight_partitions_per_topic_is_sixteen_in_total`).

Arithmetic worth keeping in view: 120,000 events/s sustained is 10.368 billion
events/day. 200M rows is ~27.8 minutes at that rate *if* every event yields one
tick row.

## Reporting rules, in evaluation order

`verdict_for_target` applies these, and each is a test:

1. **Evidence class.** A latency result from the in-process model is
   `INCONCLUSIVE`. It measures Python dictionaries. The number is real; the claim
   would not be.
2. **Correctness outranks latency.** A fast p95 over a build whose bars disagree
   with the oracle is a **global NO-GO**, not a performance pass.
3. **Errors void the sample.** Any query error → `INCONCLUSIVE`. Excluding
   timeouts from a distribution is how a failing system reports a good p95.
4. **The dataset must be the agreed size.** 200M **unique tick** rows. Physical
   duplicates and quotes do not count towards it.
5. **Then the number.** `p95 <= target` → PASS, labelled `measured_on_new_profile`.
   `p95 > target` → `FAIL`, reported as **performance NOT_MET** and explicitly not
   a correctness failure.

Percentiles are **nearest-rank** on the sorted sample — with 200 samples, p95 is
the 190th value, an observation rather than an interpolation. Cold and warm
samples are kept as separate populations; pooling them would let the mixture
ratio set the result. Timing uses `perf_counter_ns`, because a wall clock can
step.

## Storage planning

Measure bytes per row from `system.parts`. Never assume a fixed row size.

```
raw_bytes_per_day = 86400 * events_per_second * compressed_bytes_per_event
storage_required >= raw + active canonical builds + bar states + merge scratch
                  + replicas + backup + free-space reserve + evidence
```

With the design's *illustrative* 60 bytes/event: ~622 GB/day at 120k events/s,
and ~12 GB for 200M rows — before merge headroom, replicas, backups or indexes.
`storage_estimate()` computes what the caller asserted and labels it an estimate;
it knows nothing about any real table.

## The ladder (section 18.1)

| Level | Target | Purpose | Status |
|---|---|---|---|
| C0 | 1k → 1M events | identity, exact OHLC, UTC | **done** (model) |
| C1 | 10M / 1k/s | part trends, query baseline | NOT_RUN |
| C2 | 50M / 10k/s | ingest + query isolation | NOT_RUN |
| C3 | 200M unique ticks | the range-query benchmark | NOT_RUN |
| C4 | 120k events/s total | sustained acknowledged rate | NOT_RUN |
| C5 | 200M + 1/4/16/32 readers | concurrency percentiles | NOT_RUN |
| C6 | 2× burst | bounded overload, recovery time | NOT_RUN |
| C7 | 6h, then 24h soak | FD/RSS drift, backlog, TTL | NOT_RUN |
| C8 | combined faults under load | crash / NO-GO / restore behaviour | partial (model, no load) |

A laptop is not expected to sustain 120k/s or hold 200M rows at a 280 ms p95, and
the design says so outright.

## A/B comparisons to run, one variable at a time

1. Raw `ORDER BY (symbol, time, dataset, …)` against canonical
   `(build, dataset, symbol, time, seq)`.
2. With and without the full key prefix in the predicate; track marks and
   granules via `EXPLAIN indexes = 1`.
3. Daily against monthly partitions on the same rows — parts, TTL cost, range p95.
4. Raw tick range against pre-aggregated bars, **after** confirming the two
   return semantically equivalent results.
5. Client batch 1k/5k/20k/50k × reader concurrency 1/4/16/32 × writer load
   0/10k/50k/120k per second.
6. Codecs, row width, `LowCardinality`, projections and skip indexes — only after
   an A/B proves a benefit.
7. Mixed workload: merge storms, cold and warm cache, nearly-full disk, peak
   Kafka lag, a rebuild running concurrently.

What to record per run: actual P05 acknowledged rows/s, source unique events/s,
physical rows/s inserted, query p50/p95/p99 **and errors**, `read_rows` and
`read_bytes`, part counts, merge time, disk IOPS and await, CPU utilisation and
steal, RSS peak, Kafka lag age and slope, MV verification lag, bytes per event.

Never report the producer's *offered* rate as throughput, and never assume
`LIMIT 100` reads only 100 rows.

## Report template

`benchmark.report_template()` returns the section 18.2 skeleton with every field
defaulting to `TO_FILL` and the status to `NOT_RUN`, so an unfinished report is
obviously unfinished rather than looking like a result with gaps. `make benchmark`
fills what it measured and leaves the rest.

## Current measurements

**None against ClickHouse.** `make benchmark` runs the full pipeline and times the
bar query against the model backend; it prints the measured latency and returns
`INCONCLUSIVE`, naming the evidence class as the reason. That is the honest
output, and it will stay the only output until `make dev-up && make
e2e-clickhouse` has run on a host with the capacity to matter.
