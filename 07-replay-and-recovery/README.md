# P07 — Deterministic Replay & Failure Recovery

A working harness for the design in
[`../docs/PROJECT_07_Deterministic_Replay_and_Failure_Recovery_System_Design.md`](../docs/PROJECT_07_Deterministic_Replay_and_Failure_Recovery_System_Design.md):
it seals a P01 source, derives the expected sets from an independent policy
oracle, replays the data through a fault-injectable pipeline, captures a sealed
Kafka cut, reconciles every boundary exactly, and ends in one of five verdicts
with an evidence bundle behind it.

## Status — read this first

**Profile implemented: `UNIT/MODEL` (design section 2.2). Nothing else.**

| Layer | What runs here |
|---|---|
| P01 simulator | **real** — sealed run directories are read and verified as P01 commits them |
| P02 protobuf | **real** — the `protobuf` runtime, with a `marketdata.v1` descriptor built at import time |
| P03 adapter | **simulated** — an independent re-implementation of the MDP1 framer, validator and sequence tracker |
| P04 Kafka | **simulated** — a broker state machine: per-partition offsets, generations, retention, ISR/minISR, idempotent dedupe, lost ACKs |
| P05 ingestion | **simulated** — batches, a contiguous completed frontier, epoch fencing, ambiguous sink ACKs |
| P06 analytics | **simulated** — raw audit, canonical build, 1-minute bars, a compare-and-swap READY catalog |

### What a `PASS` from this harness means

That the **protocol and state-machine logic** held for the tested dataset,
faults and topology. Every verdict carries `profile: "UNIT/MODEL"` and a note
saying so, in every bundle and every report.

### What it does not mean

No real Kafka broker, ClickHouse server, cgroup or network stack is exercised,
so per design 16.1 integration is **NOT VALIDATED**. Specifically unproven:

- replication, leader election and the produce/fetch protocol (only the ISR
  *decision* is modelled),
- ClickHouse `MergeTree` merges and incremental materialised-view partial
  states (bars are computed directly),
- kernel-level CPU/memory pressure and real packet loss,
- **any throughput or latency figure**. Design 13's capacity profiles S1–S6 are
  `NOT_RUN`, and no report this harness writes contains a pipeline throughput
  claim. `tests/perf` measures Python and says so.

Those cases exist in the suite as skipped tests marked `needs_real_lab`, so they
are counted `NOT_RUN` in the release report rather than quietly dropped.

## Layout

```
07-replay-and-recovery/
├── p07/
│   ├── contracts.py              identity, semantic core, topics, bar width
│   ├── dataset_sealer.py         verify and seal a P01 run directory
│   ├── yamlite.py                strict YAML subset loader (see ADR-003)
│   ├── scenario_loader.py        fail-closed scenario validation
│   ├── cli.py                    python -m p07 …
│   ├── oracle/                   independent expected sets
│   │   ├── semantic_digest.py    P05-CORE-V1, implemented twice
│   │   ├── acceptance_oracle.py  independent P03 policy
│   │   └── reference_bars.py     integer 1m OHLC
│   ├── dataplane/                the simulated P02–P06 (the system under test)
│   ├── replay/                   source replayer + read-only Kafka cut scanner
│   ├── recovery/                 fencing, cut capture, replay feasibility, RPO/RTO
│   ├── reconcile/                exact identity diffs at four boundaries
│   ├── injectors/                faults that must produce a receipt
│   ├── evidence/                 verdicts, hash-chained journal, artifact store
│   ├── orchestration/            lifecycle state machine + supervisor
│   └── reporting/                run summary and release report
├── config/replay-lab.yaml        lab configuration (design 14.1)
├── scenarios/                    declarative fault scenarios
├── catalog/what_if_matrix.json   448 cases parsed from the design document
├── fixtures/                     generated datasets + ANCHORS.json
├── tools/                        catalog builder, fixture factory
└── tests/{unit,property,model,integration,end_to_end,chaos,security,perf}
```

## Quick start

```bash
cd 07-replay-and-recovery

# Regenerate the fixtures and their external anchor file.
python -m tools.make_fixtures

# Validate the lab config. Exits non-zero if any gate is disabled.
python -m p07 validate-config config/replay-lab.yaml

# Seal a source. Pass the anchor from fixtures/ANCHORS.json to prove integrity.
python -m p07 verify-source fixtures/demo-fx-001 \
    --anchor 76d4fe09bcda705baba62dce731ac982a603c867369d72e05f254b2dc0149880

# Validate a scenario without running it.
python -m p07 plan scenarios/kafka/ack-unknown.yaml

# Run it and write an evidence bundle.
python -m p07 run scenarios/kafka/ack-unknown.yaml --run-id demo-1

# Read the bundle, then re-verify its hashes and journal chain.
python -m p07 report runs/p07-kafka-ack-unknown-001/demo-1
python -m p07 reconcile runs/p07-kafka-ack-unknown-001/demo-1

# Rebuild the case inventory from the design document, then gate a release.
python tools/build_catalog.py ../docs/PROJECT_07_*.md
python -m p07 verify-release --catalog catalog/what_if_matrix.json \
    --coverage catalog/coverage.json --out reports/release.md
```

Exit codes: `0` success, `1` a release-blocking verdict, `2` the command could
not run. A release gate has to tell "the lab says no" from "the lab did not
run", so those are separate.

### Running the tests

```bash
python -m pytest                      # everything
python -m pytest -m "not slow"        # fast correctness pass
python -m pytest tests/unit tests/model
python -m pytest tests/perf -s        # prints measurements
```

`catalog/coverage.json` is written by the test session from the
`@pytest.mark.whatif(...)` tags, and is what `verify-release` joins against the
448-case catalog. A case nobody tagged is `NOT_RUN`; a case whose test failed
contributes its failure, so coverage cannot be inflated.

**`fsync` and speed.** The evidence store flushes every write by default,
because a bundle documenting a crash has to survive one. On the Windows volume
this was developed on, `FlushFileBuffers` costs over a second per call, so the
test suite passes `fsync=False` and `python -m p07 run` accepts `--no-fsync`.
Every bundle records which it was under `evidence.durability`, so a run can
never imply a durability it did not pay for.

## Architecture decisions

### ADR-001 — The data plane is simulated, and says so everywhere

**Context.** P02–P06 do not exist yet as running systems, and this machine has
no Docker, Kafka or ClickHouse.

**Decision.** Implement the design's `UNIT/MODEL` profile: in-process state
machines for the broker, sink and catalog, with the real `protobuf` runtime and
real P01 datasets. Carry `profile` and a `profile_note` through every artifact,
report and summary.

**Consequence.** The protocol logic — offset frontiers, generations, retention,
ACK ambiguity, CAS publication — is genuinely exercised. Real-infrastructure
behaviour is not, and the cases needing it stay visible as `NOT_RUN`. The
alternative, a harness that quietly tested less than it claimed, is the failure
mode the design spends most of its length warning about.

### ADR-002 — Every oracle is implemented twice, on purpose

**Context.** Design 2.1 forbids taking the component's own output as the
expected set, and an oracle that shares code with the thing it checks cannot
detect that code being wrong.

**Decision.** Three deliberate duplications:
`p07.oracle.acceptance_oracle` and `p07.dataplane.adapter` both implement the
P03 policy, in different shapes (batch pass over a file versus a byte-fed state
machine); `p07.oracle.reference_bars` and `p07.dataplane.analytics` both
aggregate OHLC, one streaming and one group-then-sort;
`semantic_digest.encode_core` has a second implementation written from the spec
text.

**Consequence.** Duplication that must not be refactored away. Every one of
these pairs is covered by a test asserting they agree, and by a mutation test
asserting the checker notices when one is broken.

### ADR-003 — A strict YAML subset loader instead of a YAML library

**Context.** Scenario files are YAML in the design, may be untrusted, and
control destructive operations. PyYAML is not installed here.

**Decision.** `p07.yamlite` supports block mappings, block sequences, inline
flow sequences and scalars, and **rejects with an error** anchors, aliases,
tags, multiple documents, block scalars, flow mappings, duplicate keys and tab
indentation. `.json` is accepted anywhere a `.yaml` is.

**Consequence.** Reimplementing part of a standard format is normally a mistake.
It is taken here because the alternative accepts constructs the harness has no
meaning for in a file that controls destructive actions — and because the subset
is small enough to cover exhaustively, including every rejection
(`tests/unit/test_yamlite_and_scenarios.py`).

### ADR-004 — `INCONCLUSIVE` outranks `FAIL`

**Context.** When the lab cannot read the Kafka range it needs, it does not know
whether data was lost.

**Decision.** `p07.evidence.verdicts.classify` resolves an unknowable reason to
`INCONCLUSIVE` even when a contradiction is also present, keeping the
contradiction in `findings`. Both block release for a mandatory case.

**Consequence.** The harness never manufactures a failure it cannot support, and
never hides one either. `Verdict` has no `__bool__`, so `if verdict:` is a
`TypeError` rather than a silent truthy `INCONCLUSIVE`.

### ADR-005 — Fault types are a closed table with fixed stages

**Context.** A scenario file decides what gets broken and when.

**Decision.** `p07.scenario_loader.FAULT_TYPES` maps each permitted fault to the
lifecycle stage at which the runner applies it. There is no field anywhere in
the schema that takes a command, path or host, unknown keys are errors, and
every target is checked against the lab allowlist at load time *and* again at
injection time.

**Consequence.** An untrusted scenario is useless rather than dangerous. It also
means a new fault type is a code change, which is the intended trade.

### ADR-006 — The frontier is counted from the resume point, never from the commit

**Context.** `INV-03` requires `committed_next <= completed_contiguous_next`.

**Decision.** `PartitionProgress.completed_contiguous_next()` starts its cursor
at `start_offset`, not at `committed_next`.

**Consequence.** Found by a mutation test. Seeding the cursor from the commit
made the check depend on the thing it exists to detect: a worker that committed
past a hole reported a frontier at its own commit, and `INV-03` could never
fire. Both halves are pinned —
`tests/model/test_mutations.py::test_the_frontier_check_catches_a_max_offset_commit`
and the control test beside it.

### ADR-007 — Deviation from the design's source tree

The design lists `oracle/`, `replay/`, `injectors/`, `recovery/`, `reconcile/`,
`evidence/`, `orchestration/` and `reporting/`; all are present. Two additions
and one rename:

- `p07/dataplane/` — the simulated P02–P06. The design assumes those systems
  exist, so it has no module for them.
- `p07/yamlite.py` — see ADR-003.
- the Kafka-cut-versus-expected comparison lives in
  `reconcile/raw_reconciler.py` as `reconcile_kafka_cut` rather than in its own
  module, because it shares the raw reconciler's question and its provenance
  data.

## Honest limitations

- **The hash chain is not a signature.** It detects an edit by someone who did
  not rewrite the rest of the file. Someone who can rewrite the whole bundle can
  recompute it, and `tests/unit/test_proof_ledger.py` asserts exactly that.
  `ProofLedger.anchor()` exists so the head can be stored elsewhere;
  storing it is the operator's job.
- **`SHA256SUMS` lives inside the bundle it describes.** Same caveat, same
  remedy.
- **Source integrity needs an external anchor.** Without one, sealing records
  `anchor_verified: false`, nothing publishes, and the verdict is
  `INCONCLUSIVE`. `fixtures/ANCHORS.json` sits beside the run directories rather
  than inside them for this reason.
- **The price-step rule flags rather than rejects by default.** P03 case E-09
  permits either. Rejecting would couple the oracle to P01's generator
  internals, and a false reject there would corrupt every downstream verdict.
  `price_step_policy: reject` is available per scenario.
- **The partitioner is not librdkafka's.** It is stable and deterministic; it is
  not murmur2 `consistent_random`. Tests assert "same symbol, same partition,
  within one generation" and never a fixed index.
- **A scenario run takes tens of seconds** for a few thousand events. The
  simulation does seven SHA-256 digests, a protobuf encode and decode, and four
  exact boundary diffs per event. That is the cost of checking everything
  exactly; it is not a throughput measurement.
