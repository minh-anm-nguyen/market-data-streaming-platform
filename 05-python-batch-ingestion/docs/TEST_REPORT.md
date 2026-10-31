# P05 test report

Spec section 16 requires a report artifact. This is it, with the numbers from
the actual runs and the gaps stated as plainly as the passes.

| | |
|---|---|
| Date | 2026-10-30 |
| Build status | `IMPLEMENTED` · correctness tests `EXECUTED` · live-infrastructure tests `NOT_RUN` · SLO/capacity `NOT_VERIFIED` |
| Python | 3.14.4 (spec targets 3.12; `requires-python = ">=3.12"`) |
| Clients | `confluent-kafka` 2.16.1, `clickhouse-connect` 1.11.0, `protobuf` 7.36.2 |
| Protobuf package | `marketdata.v1` |
| Descriptor SHA-256 | `e17bf37b0384e521ee4a65392ab4778405aacfc06355215e6ac8d0c5adff0cbc` |
| Canonical encoding | `P05-CORE-V1` |
| Config fingerprint (`config/dev.toml`) | `56a03c5d5f8ff508d27d7c2f32e2dca616280a8bcface69de4bc5e6c7a618fe7` |
| Golden manifest SHA-256 | `6097e369c4cef50b…` (2000 events, seed 1337, 8+8 partitions) |

**Environment limitation, stated first because it bounds every claim below:**
no Docker, no Kafka broker and no ClickHouse server were available. The real
client adapters are implemented and importable, but were exercised only against
their failure paths (nothing listening on the configured ports). Everything
proven below was proven against a deterministic in-memory Kafka and ClickHouse.

---

## 1. Results

```
602 passed, 5 skipped in 227s
coverage: 92% of src/p05_ingest (2995 statements, 246 missed)
```

| Layer | Tests | What it established |
|---|---:|---|
| `tests/unit` | 425 | Digest byte layout against an independently written decoder; every validation reason code; frontier arithmetic including offset gaps; batch sealing bounds and immutability; retry/backoff bounds; budget hysteresis; driver-error classification; all 10 fail-closed config gates |
| `tests/property` | 18 | Hypothesis properties of the encoding (determinism, injectivity, self-delimiting, presence never collapsing); frontier invariants over generated completion orders; a `RuleBasedStateMachine` driving the real engine through arbitrary interleavings of produce/step/fault/rebalance; a shadow model of the budget |
| `tests/integration` | 126 | E2E-01…E2E-22 against the deterministic cluster, each with the spec PASS oracle |
| `tests/chaos` | 27 | Real process kills at all five crash hooks with durable state; ~1500-case hostile-input corpus; seeded combined fault storms |
| `tests/performance` | 11 | Bounded RSS and handles under a permanently stalled sink; bounded commit ledger; relative throughput and the batch/parts trade-off |
| `test_live_infra.py` | 5 **skipped** | **NOT_RUN** — see section 4 |

Named scenarios (`tools/run_scenarios.py --all`): **6 PASS, 0 FAIL, 0 NOT_RUN**,
every one with its injected fault confirmed to have occurred.

---

## 2. Invariants

Each is enforced in code and asserted by tests, not merely documented.

| ID | Invariant | Where enforced | Status |
|---|---|---|---|
| I-01 | `commit_next <= proven_frontier_next` | `commit_coordinator.assert_commit_safe` raises | **VERIFIED** — unit, property, model checker (after every step), chaos campaigns, and all 5 subprocess crash points |
| I-02 | Only the current assignment epoch may commit | `assignment_manager.may_commit` | **VERIFIED** — stale completions fenced; `on_lost` never commits |
| I-03 | No commit past a record without a terminal result | frontier walk in `commit_coordinator` | **VERIFIED** — including the subtle case where later records are already ACKed |
| I-04 | Retries re-send identical rows/order/token | frozen `SealedBatch`, content-derived token | **VERIFIED** — token identical across every retry; a regrouped batch gets a different token |
| I-05 | Buffered bytes stay inside the budget | `budget_manager` + pre-poll headroom check | **VERIFIED for accounted bytes**; RSS measured separately, not certified |
| I-06 | Prices/times/presence preserved exactly | `event_mapper`, no float anywhere | **VERIFIED** — int64 boundaries, explicit zero vs absent, UTC date at the day edge under TZ+07 |
| I-07 | No silent poison skip | `quarantine`, fail-closed default | **VERIFIED** — including the DLQ-unavailable branch |
| I-08 | No false end-of-dataset | `run()` caveat, verdict text, CLI output | **VERIFIED** — every COMPLETE verdict carries the reconciliation caveat; `report_validator` rejects one that does not |
| I-09 | Four distinct clocks kept distinct | injected monotonic clock; `ingested_at` is server-side | **VERIFIED** |
| I-10 | Reports distinguish physical from logical | `reconciler` counts both plus conflicts | **VERIFIED** — the E2E-21 count-collision case fails as it must |
| I-11 | No silent offset reset | `config` rejects `latest`/`earliest`; `RetentionLostError` is fatal | **VERIFIED** |
| I-12 | Fault lane isolated from the market table | `config._check_fault_lane_isolation` | **VERIFIED** |

---

## 3. Notable findings during development

Four real defects were found by the tests and fixed. Recorded because they are
the evidence that the suite does something.

1. **Batches could exceed `per_partition_max_rows`.** The fetch loop appended up
   to 512 records per step before the sealer ran, producing an 11-row batch
   under a cap of 7. Fixed by returning from the fetch loop as soon as a
   threshold is reached, plus pausing a partition whose full batch cannot be
   submitted. Found by `test_batch_boundaries_respect_the_configured_caps`.
2. **The I-12 isolation gate was a no-op.** It compared a string against
   `ClickHouseConfig.table`, which under `slots=True` is a slot descriptor
   rather than the default value — so the comparison never matched and the
   fault lane could have written to the market table. Fixed with an explicit
   `MARKET_TABLE` constant. Found by
   `test_fault_lane_must_not_write_to_the_market_table`.
3. **A blocked partition could hang the engine.** Blocking left an open batch
   that the sealer skips, so the loop never reached a quiescent state. Fixed by
   releasing the open batch on block; those rows were never sent, so a restart
   re-reads them. Found by the poison-policy tests failing to settle.
4. **The cooperative-revoke drain window was nominal.** `on_revoke` committed
   before draining worker results, so a batch ClickHouse had just acknowledged
   was discarded unrecorded, turning every clean handover into a guaranteed
   duplicate. Fixed by draining completions before committing on revoke. Found
   by `test_cooperative_revoke_commits_the_proven_prefix`.

A fifth issue was in the test harness rather than the product: the fake consumer
rescanned the whole partition log on every `poll`, making the harness quadratic
and the throughput figures meaningless (993 events/s). Indexing the log directly
raised it to ~6100 events/s and made the measurement near-linear.

---

## 4. What is NOT proven

Nothing below is a pass. Each is a gap in the evidence.

| Gap | Why it could not be closed here | What would close it |
|---|---|---|
| librdkafka callback semantics (`on_revoke`/`on_lost` timing, commit result shape, effective property names) | No broker | `pytest -m kafka_clickhouse` against the dev lab |
| Real cooperative-sticky rebalance behaviour | No broker | Multi-member group test in the reliability lab |
| ClickHouse insert de-duplication window, MergeTree parts and merge pressure, `async_insert` server defaults | No server | Live run plus query 6 of `sql/002_audit_queries.sql` |
| Genuine driver timeout and reset classification | No server | A fault proxy (toxiproxy/netem) in front of a live ClickHouse |
| Replication, ISR, quorum, replica lag, HA, disaster survival | Single-node lab only | `RELIABILITY-LAB` profile (3 brokers RF=3 minISR=2, replicated CH) |
| Any absolute throughput or latency figure, including the 120k events/s target | Measured only against in-memory fakes with no network and no MergeTree | `CAPACITY-LAB` ladder with the run manifest from `tools/benchmark.py` |
| 6-hour soak | Not run | `E2E-19` in the capacity lab |
| P02 schema provenance | The P02 artifact is absent from this workspace; `schemas/market_data.proto` is a transcription of spec section 3.1 | Replace with the real file and re-derive the descriptor pin |

Coverage reflects these gaps precisely: `kafka_source.py` at 34% and
`clickhouse_sink.py` at 65% are the live adapters, and they account for most
of the 246 uncovered statements. Everything else is 88-100%.

---

## 5. GO / NO-GO

Spec 14.1 defaults every gate to `NOT_RUN`.
`tools/report_validator.py` rejects any artifact that claims otherwise.

| Gate | State | Basis |
|---|---|---|
| `CONTRACT_GO` | **BLOCKED** | Validation, digest, caps and presence all verified, but the schema is a transcription rather than the P02 artifact. Blocked on provenance, not on behaviour. |
| `CORRECTNESS_GO` | **PASS (simulated)** | Zero unexplained missing/extra/conflict across every finite run; no early commit under any generated schedule; the count-collision trap fails as designed. Scoped to the simulated cluster. |
| `RELIABILITY_GO` | **PARTIAL** | Crash (5 points, real kills), lost ACK, ambiguous outcome, poison stop, retention loss and rebalance/loss all have evidence. Broker failover, replica loss and real network faults are `NOT_RUN`. |
| `SECURITY_GO` | **PARTIAL** | Hostile input bounded (~1500 cases: no crash, no unhandled exception, bounded CPU); credentials env-only and absent from artifacts; metric cardinality enforced. TLS, ACLs and least-privilege DB roles are `NOT_RUN`. |
| `CAPACITY_GO` | **NOT_RUN** | No live infrastructure. The recorded figures explicitly exclude the insert round trip. |
| `P06_HANDOFF_GO` | **PASS (with a required warning)** | Raw schema, identity, digest and conflict gate delivered; `sql/003_p06_handoff_contract.sql` plus `test_rollup_trap.py` demonstrate that a naive rollup double-counts and that the dedup-aware one does not. P06 must not enable materialized bars over the raw table. |
| `OVERALL_GO` | **NO-GO** | `CAPACITY_GO` is `NOT_RUN` and `CONTRACT_GO` is blocked on schema provenance. The correctness core is ready for a live lab; it is not ready for a release claim. |

---

## 6. Reproducing this

```bash
python -m pip install -e ".[test]"

python -m p05_ingest doctor --config config/dev.toml
python -m coverage run --source=src/p05_ingest -m pytest tests -q
python -m coverage report --omit="*/generated/*"

python tools/run_scenarios.py --all --out artifacts/scenarios.json
python -m p05_ingest simulate --config config/dev.toml --events 1000 \
    --report artifacts/sim-clean.json
python tools/report_validator.py --run artifacts/sim-clean.json \
    --scenarios artifacts/scenarios.json
python tools/benchmark.py --rates 1000,10000 --out artifacts/bench.json
python tools/make_golden_manifest.py     # must reproduce the hash above
```

Everything is seeded. The golden manifest, the fuzz corpus, the retry jitter and
the chaos campaigns all reproduce exactly, so a failure can be replayed rather
than chased.
