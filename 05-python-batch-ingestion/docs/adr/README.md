# Architecture decisions

What was decided, why, what it costs, and what would make us change our mind.
Each entry says where it is enforced in code, so a decision cannot quietly
drift from the implementation.

---

## ADR-001 — Python 3.12+ with `confluent-kafka`

**Decision.** Use the librdkafka binding rather than a pure-Python client.

**Why.** Manual offset commits with per-partition results, `pause`/`resume`, and
the `on_lost` callback are all required by the design, and librdkafka exposes
them with the semantics the spec assumes.

**Cost.** A native dependency, and property names that do not line up with the
TOML keys. `ConfluentKafkaSource.client_properties()` keeps the whole mapping in
one visible place so it stays auditable.

**Verified?** The adapter is written but **not exercised against a broker here**.
Callback semantics remain unproven (`tests/integration/test_live_infra.py`).

---

## ADR-002 — One owner thread owns the consumer; sinks run on workers

**Decision.** Exactly one thread calls `poll`, `commit`, `pause`, `resume`,
`subscribe` and `close`. Sink workers never touch the consumer.

**Why.** A blocking ClickHouse insert must not stall the poll loop, or the group
rebalances on mere slowness. Keeping the consumer single-owner removes a whole
class of races instead of trying to synchronise them.

**Cost.** Results must travel back through a queue, and a completion can arrive
after its partition has moved. That is what `assignment_manager` epochs fence.

**Enforced in.** `engine.py` (no worker imports `kafka_source`), `sink_worker.py`.

---

## ADR-003 — One in-flight batch per topic-partition

**Decision.** At most one sink request per partition is outstanding.

**Why.** It makes the contiguous-frontier argument simple enough to be checked:
the proven prefix is a single walk over consumed records. With two batches in
flight, a later one could be acknowledged first and the frontier logic would
need to reason about interleaved completions.

**Cost.** A per-partition throughput ceiling of one insert round trip at a time.
Scale is obtained by adding partitions, not by deepening the pipeline.

**Change our mind if.** A measured capacity run shows the per-partition ceiling
is the binding constraint. That is a phase-2 redesign of the frontier, not a
config change — which is why `config.py` *refuses* any other value rather than
letting someone raise it casually.

---

## ADR-004 — Commit only after a terminal durable result

**Decision.** `commit_next(tp) <= proven_frontier_next(tp)`, always, where
"proven" means a ClickHouse ACK or a durable-quarantine ACK.

**Why.** The alternative loses data. Nothing else in the design matters if this
is wrong.

**Cost.** Duplicates on replay, and a partition that stops when its sink result
is ambiguous. Both are preferable to silent loss.

**Enforced in.** `commit_coordinator.assert_commit_safe`, which raises
`InvariantViolation` rather than returning a boolean: there is no safe way to
continue from a state where the committed offset is already wrong.

---

## ADR-005 — A hand-rolled canonical encoding, not `SerializeToString()`

**Decision.** `P05-CORE-V1`: a domain separator, length-prefixed strings,
big-endian signed int64s, and an explicit presence byte per optional field.

**Why.** Protobuf serialization is not canonical across runtimes or versions —
field order, unknown-field retention and packing may all differ — so it cannot
be the byte oracle for a stored identifier. `hash()` is worse: it is salted per
process.

**Cost.** A format to version and migrate. The domain separator carries the
version so a future `P05-CORE-V2` can coexist during a migration.

**Enforced in.** `canonical_digest.py`. `test_canonical_digest.py` decodes the
bytes with an independently written decoder rather than comparing against a
golden blob produced by the same encoder.

---

## ADR-006 — `event_kind` is not part of the event identity

**Decision.** The identity is `(dataset_id, symbol, sequence)` only.

**Why.** P01 sequences are per symbol across *both* topics. Including the kind
would turn a genuine upstream fault — the same sequence published as both a tick
and a quote — into two apparently valid events.

**Cost.** That fault surfaces as a conflict that blocks analytics, which is the
intended behaviour, but it does block them.

**Enforced in.** `model.EventIdentity`, `reconciler.reconcile`, and
`test_cross_topic_order.py::test_same_sequence_as_both_tick_and_quote_is_a_conflict`.

---

## ADR-007 — An ambiguous sink outcome is its own category

**Decision.** Four outcomes: `ACKED`, `AMBIGUOUS`, `TRANSIENT`, `PERMANENT`. An
unrecognised driver error is classified `AMBIGUOUS`.

**Why.** A timeout after the request was sent does not mean the write did not
happen. Collapsing it into "failed" is the single most dangerous simplification
available in this codebase.

**Cost.** Pessimism: some batches are retried that had in fact already landed,
producing duplicates. A possible duplicate beats a possible loss.

**Enforced in.** `clickhouse_sink.classify_exception`, and the ordering in that
function — ambiguity is checked before the transient markers, because a message
can contain both.

---

## ADR-008 — Insert de-duplication is an optimisation, never a guarantee

**Decision.** Send a content-derived `insert_deduplication_token`, and treat the
de-duplication window as best-effort.

**Why.** The window is finite. A retry after it rotates, or a restart that
regroups rows into a different batch, still duplicates. Claiming exactly-once
from this feature would be false.

**Cost.** Analytics must be dedup-aware. `sql/003_p06_handoff_contract.sql`
hands P06 both the canonical layer and the worked proof that the naive version
is wrong.

**Enforced in.** `canonical_digest.batch_insert_token` (content, not just
offsets), and `test_ack_ambiguity.py::test_e2e_15_*`, which demonstrates a real
physical duplicate after window eviction.

---

## ADR-009 — Poison records fail closed; the DLQ is opt-in and must ACK

**Decision.** Default: stop the partition. Optional: a durable DLQ, and a skip
is permitted only after the quarantine write is acknowledged.

**Why.** Silently skipping a record the validator refused is indistinguishable
from losing it.

**Cost.** One malformed record halts a partition and fails the run. That is the
point: it demands an operator decision rather than making one.

**Enforced in.** `quarantine.QuarantinePolicy.handle`, including the branch
where the DLQ publish raises and the record stays `BLOCKED`.

---

## ADR-010 — Backpressure as not-fetching, with a pre-poll headroom check

**Decision.** Stop polling while headroom for a maximum-size record is
unavailable, and pause the hottest partitions. Never discard a polled record.

**Why.** Once `poll()` returns a record it is out of the client queue; the only
way to see it again is `seek()`. So there is no such thing as refusing a record
that has already been handed over — it must be buffered or the run must stop.

**Cost.** Throughput is left on the table near the watermark. Hysteresis (pause
at 80%, resume at 50%) is mandatory, because a single threshold produces
pause/resume flapping that looks like a rebalance storm.

**Enforced in.** `engine._has_budget_headroom`, `budget_manager`, and the
`InvariantViolation` raised if a post-validation reservation ever fails — which
would mean the bound itself is wrong.

---

## ADR-011 — The verifier is a separate program

**Decision.** Reconciliation lives in `tools/verifier.py` with its own consumer,
its own ClickHouse client, and no access to the engine counters.

**Why.** A bug in the ingester must not be able to vouch for itself. Spec 9.2 is
explicit that application counters are not independent evidence.

**Cost.** Duplicated read paths and a second Kafka scan.

**Enforced in.** `reconciler.py` takes only observation lists, never an engine,
and `tools/verifier.py` builds its own clients.

---

## ADR-012 — Counts never decide completeness

**Decision.** Every verdict compares sets of identities and digests per
identity.

**Why.** One event missing and another duplicated leaves `count(*)` exactly
right. A counts-based oracle passes that case, which is why the spec calls it
out as the false-PASS trap (E2E-21).

**Cost.** Reconciliation is memory-hungry and must be scoped per dataset rather
than run across the whole table online.

**Enforced in.** `reconciler.reconcile`, and
`test_reconciliation.py::test_e2e_21_counts_match_but_identity_level_oracle_fails`.

---

## ADR-013 — The fakes ship inside the package

**Decision.** `src/p05_ingest/testkit/` rather than `tests/fakes/`.

**Why.** `tools/run_scenarios.py` and `tools/benchmark.py` exercise the same
fakes as the test suite. If the scenario runner had its own copies, a scenario
could pass against behaviour the tests never check.

**Cost.** Test infrastructure ships with the package. Nothing on the production
import path touches it, and the package docstring says so.

---

## ADR-014 — The engine is a step function

**Decision.** `IngestEngine.step()` performs one iteration; `run()` is a loop
over it. The clock is injected.

**Why.** It is the only way to assert "after exactly this sequence of events the
committed offset is X". That makes the property and model-checking tests
possible, and makes a failing schedule shrink to a short replayable sequence
instead of a flaky race.

**Cost.** The loop structure is slightly unnatural to read, and the ordering
inside `step()` is load-bearing (drain, commit, fetch, seal, backpressure) in a
way a comment has to explain.

---

## ADR-015 — Preflight checks, never repairs

**Decision.** The program never creates or alters its ClickHouse table, and
never rewrites its own configuration.

**Why.** A program that can reshape its own sink can hide a schema mismatch from
the operator. Every unverified check is reported `WARN`, never `PASS`.

**Cost.** An extra manual step when setting up a lab.

**Enforced in.** `preflight.py` (no DDL anywhere), `check_retention_headroom`
returning a non-required WARN when no broker is reachable.
