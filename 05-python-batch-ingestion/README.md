# P05 — Python Kafka Batch Ingestion (Kafka → Protobuf → ClickHouse)

Consumes `marketdata.v1` protobuf records from `md.ticks.v1` and `md.quotes.v1`,
validates them against the P02/P04 contract, batches them per topic-partition,
inserts them into ClickHouse MergeTree, and **commits the Kafka offset only
after ClickHouse has acknowledged the write**.

Implements the design in
[`PROJECT_05_Python_Kafka_Batch_Ingestion_System_Design.md`](../docs/PROJECT_05_Python_Kafka_Batch_Ingestion_System_Design.md).

---

## What this delivers, and what it does not

**Delivery semantics: conditionally at-least-once.** Kafka and ClickHouse do not
share a transaction, so physical rows can be duplicated. Logical idempotency is
a property of the *query layer*, defined by the event identity
`(dataset_id, symbol, sequence)` plus the `P05-CORE-V1` payload digest.
ClickHouse block de-duplication is a bounded optimisation, not exactly-once.

| Claim | Status | Why |
|---|---|---|
| Correctness invariants I-01 … I-12 | **Verified** | Unit, property, model-checking, simulated-integration and chaos tests, including real process kills |
| No commit ahead of the sink proof | **Verified** | Enforced in `commit_coordinator`, asserted after every step in the model checker and the chaos campaigns |
| Recovery from a crash at any point | **Verified** | 5 crash hooks, real process kill, file-backed durable state, restart-and-reconcile |
| Live Kafka / ClickHouse behaviour | **NOT RUN** | No broker or server available in this environment; `tests/integration/test_live_infra.py` skips and says so |
| Throughput / latency SLOs | **NOT VERIFIED** | Measured only against in-memory fakes, which have no network and no MergeTree |
| HA, replication, disaster survival | **NOT RUN** | Needs the reliability lab topology |

Every gate in the spec GO/NO-GO rubric starts at `NOT_RUN`, and
`tools/report_validator.py` rejects any artifact that claims otherwise.

---

## Quick start, with no infrastructure

The whole correctness surface is exercisable without a broker or a database:

```bash
python -m pip install -e ".[test]"

# Startup gates. Everything unverified is reported WARN, never PASS.
python -m p05_ingest doctor --config config/dev.toml

# The full pipeline against a deterministic in-memory Kafka + ClickHouse.
python -m p05_ingest simulate --config config/dev.toml --events 1000

# The same, with ClickHouse writing the rows and then losing the ACK.
python -m p05_ingest simulate --config config/dev.toml --events 500 \
    --sink-plan write_then_lose_ack,ack

# Named what-if scenarios, each with an explicit PASS oracle.
python tools/run_scenarios.py --all --out artifacts/scenarios.json

# Refuse a report that claims more than its evidence supports.
python tools/report_validator.py --run artifacts/sim-clean.json \
    --scenarios artifacts/scenarios.json
```

## Running against real infrastructure

```bash
docker compose -f deployment/docker-compose.dev.yml up -d
docker compose -f deployment/docker-compose.dev.yml exec -T clickhouse \
    clickhouse-client --multiquery < sql/001_create_raw_table.sql

python -m p05_ingest doctor --config config/dev.toml --live
python -m p05_ingest run --config config/dev.toml --report artifacts/run.json

# Freeze a cut, then reconcile independently.
python -m p05_ingest snapshot --config config/dev.toml --out artifacts/cut.json
python tools/verifier.py --config config/dev.toml --cut artifacts/cut.json \
    --dataset demo-a1b2c3 --settled --out artifacts/reconciliation.json

pytest -m kafka_clickhouse          # the live-only suite
```

---

## How correctness is achieved

### Offsets follow the sink, never lead it

```
poll → validate → buffer → seal immutable batch → insert → ClickHouse ACK
    → contiguous prefix proven → commit explicit next offset
```

`commit_coordinator.py` is the only place offsets are committed, and it enforces
three things by raising rather than by logging:

- **I-01** `commit_next(tp) <= proven_frontier_next(tp)`. The frontier advances
  only on a terminal durable result: a ClickHouse ACK, or a durable-quarantine
  ACK when the optional DLQ is on.
- **I-03** A consumed record with no terminal result blocks the frontier, even
  when later records are already acknowledged. Committing past a hole is the P0
  data-loss bug.
- **I-02** A completion whose assignment epoch is stale may not commit.

The frontier walks *consumed records in consumption order*, not integers.
Transaction and control records consume offset space without being delivered, so
a skipped offset is not a hole.

### Ambiguity is a first-class outcome

A read timeout after the request was sent does **not** mean the row was not
written. `clickhouse_sink.classify_exception` therefore has four outcomes, not
two: `ACKED`, `AMBIGUOUS`, `TRANSIENT`, `PERMANENT`. An unrecognised error is
classified `AMBIGUOUS` on purpose — being pessimistic costs a possible
duplicate, being optimistic costs a lost record.

### Retries re-send the identical batch

A `SealedBatch` is frozen, and its `insert_token` is derived from the content as
well as the offsets — so a retry is eligible for ClickHouse de-duplication,
while a differently-grouped batch covering the same offsets cannot collide with
it and be silently discarded.

### Backpressure is expressed as not-fetching

Once `poll()` has returned a record it is out of the client queue; the only way
to see it again is `seek()`. There is therefore no way to "refuse" a record that
has already been handed over. The loop stops polling while headroom for a
maximum-size record is unavailable and pauses the hot partitions, so admission
after validation cannot fail. Nothing is ever dropped to relieve pressure.

### Poison records fail closed

The default policy stops the partition and ends the run `INCOMPLETE`. The
optional durable DLQ permits a skip **only after the quarantine write is
acknowledged**; if the DLQ is unavailable the record stays blocked. A quarantine
without a durable ACK is a silent drop wearing a different name.

### Completeness is decided by a separate program

`tools/verifier.py` has its own consumer and its own ClickHouse client and never
reads the engine counters. It compares *sets of identities and digests*, because
counts cannot distinguish "correct" from "one missing and one duplicated" — the
case `tests/integration/test_reconciliation.py` pins explicitly.

---

## Layout

```
src/p05_ingest/
  engine.py              the owner loop; the only code that touches Kafka
  commit_coordinator.py  the contiguous proven frontier (I-01, I-03)
  assignment_manager.py  epochs and the ownership fence (I-02)
  contract_validator.py  strict P02/P04 input validation, never repairs
  canonical_digest.py    P05-CORE-V1 encoding and the semantic digest
  partition_batcher.py   per-partition FIFO, immutable sealed batches (I-04)
  budget_manager.py      byte budget with pause/resume hysteresis (I-05)
  sink_worker.py         retry loop; inline and threaded executors
  clickhouse_sink.py     the driver adapter and error classification
  quarantine.py          fail-closed policy, opt-in durable DLQ (I-07)
  reconciler.py          the independent oracle and the GO/NO-GO verdict
  crash_hooks.py         the five mandatory crash points
  preflight.py           startup gates; checks, never repairs
  testkit/               deterministic in-memory Kafka and ClickHouse
sql/                     DDL, audit queries, the P06 handoff contract
tools/                   scenarios, verifier, benchmark, report validator
tests/                   unit, property, integration, chaos, performance
```

`testkit/` lives under `src/` rather than under `tests/` so that `tools/` and
the test suite share the same fakes. A divergence between what the tests
exercise and what the scenario runner exercises would be a silent source of
false confidence.

---

## Tests

```bash
pytest tests/unit tests/integration        # fast, no infrastructure
pytest tests/property                      # hypothesis + model checking
pytest tests/chaos                         # real process kills, fuzz, fault storms
pytest -m perf -s                          # measurements, printed not asserted
pytest -m kafka_clickhouse                 # live-only; skips without infra
```

| Layer | What it proves | What it cannot |
|---|---|---|
| unit | digest layout, validation codes, frontier arithmetic, batching bounds, retry policy | anything about real clients |
| property | the invariants hold under generated schedules, including rebalance and crash interleavings | real ClickHouse de-duplication |
| integration | E2E-01 … E2E-22 against the deterministic cluster | HA, merge pressure, real timing |
| chaos | real process kills at 5 points, hostile input, combined fault storms | failure modes the fakes cannot produce |
| performance | bounded memory and handles; relative throughput | any absolute events/s claim |

A skipped live test is a **gap in the evidence**, not a pass.
`test_live_tests_are_reported_as_not_run_when_skipped` always runs and prints
exactly what remains unproven.

---

## Operating notes

- **Credentials** come from the environment (`P05_CH_USER`, `P05_CH_PASSWORD`)
  and never from the TOML file. The config fingerprint deliberately ignores the
  variable *names*, so two machines that name the same credential differently
  still produce the same fingerprint.
- **`auto.offset.reset` must be `error` or `none`.** `latest` and `earliest`
  silently skip or re-read data when the committed offset is gone, so startup
  refuses them.
- **`async_insert=1` with `wait_for_async_insert=0` is refused at startup.** That
  ACK would only prove server-side buffering, so committing on it can lose data.
- **This program never creates or alters the ClickHouse table.** Apply
  `sql/001_create_raw_table.sql` yourself; preflight only checks.
- **Unknown config keys are errors.** A typo in a budget or a timeout would
  otherwise fall back to a default and invalidate every number in a report.

Incident procedures are in [`docs/runbooks/`](docs/runbooks/); the decisions and
their trade-offs are in [`docs/adr/`](docs/adr/).

## Known gaps

1. **No live-infrastructure evidence.** Everything about librdkafka callback
   semantics, real rebalance timing, MergeTree parts and merges, genuine driver
   timeouts, replica lag and HA is unproven here.
2. **No capacity envelope.** The measured figures exclude the insert round trip,
   which dominates in production. `CAPACITY_GO` is `NOT_RUN`.
3. **`schemas/market_data.proto` is a transcription**, not a checkout of the P02
   artifact, which is not present in this workspace. It must be replaced by the
   real file and the descriptor hash re-pinned; any divergence is a
   `CONTRACT_GO` blocker.
4. **One in-flight batch per partition.** Simple and correct, but a per-partition
   bottleneck. Raising it invalidates the contiguous-frontier argument, so it is
   a phase-2 redesign rather than a config change.
5. **`reconcile` has no live mode.** It refuses to fall back to a simulated
   source; use `tools/verifier.py` against real infrastructure.
