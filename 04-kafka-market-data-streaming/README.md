# Project 04 — Kafka Market Data Streaming

A C++20 publisher that turns Project 01 market-data events into keyed Kafka
records carrying raw `marketdata.v1.MarketDataEvent` protobuf, plus the
independent verifiers that check whether it actually did so.

Implemented against `docs/PROJECT_04_Kafka_Market_Data_Streaming_System_Design.md`.
Section references throughout the code point back at that document.

## Status

| | |
|---|---|
| Build | C++20, g++ 11.4, protobuf 3.21.12, librdkafka 2.6.1 |
| Unit and property suite | **146 tests, all passing**, 0 compiler warnings under `-Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion` |
| Real broker | Apache Kafka 4.2.0 in Docker, KRaft, single broker (profile `local-dev`) |
| E2E-01 | **PASS** — 20000 source events, 20000 observed unique identities, 0 missing, 0 duplicate, 0 conflict, 0 contract violation |
| Real-broker negative and fault suite | **13 of 13 checks pass** |
| 280-case what-if matrix | 43 PASS, 3 PASS_MODEL_ONLY, 234 NOT_RUN — see `tests/expected/what_if_verdicts.yaml` |

The matrix is deliberately mostly NOT_RUN. Section 14 of the design forbids
marking a case PASS because nothing crashed, so a case is PASS only when the
stimulus was applied, an independent oracle checked the result and an artifact
exists.

## What this proves

* A C++ publisher reads a completed Project 01 dataset, routes each event to
  `md.ticks.v1` or `md.quotes.v1` by its protobuf `oneof` arm, keys it by
  `symbol`, and writes the value as a bare serialized `MarketDataEvent` — no
  MDP1 magic, no length prefix.
* Every one of 20000 records was found again by a verifier that shares no code
  with the publisher: a pure-Python Kafka client, a protobuf decode through the
  pinned descriptor set, and an expected-set derivation written independently in
  Python. Core field equality held for all of them, including int64 prices.
* The key-to-partition mapping agrees across three independent murmur2
  implementations (C++ publisher, Python golden generator, Python verifier) and
  the broker placed every record where all three predicted.
* `produce()` returning success is never recorded as an acknowledgement. The
  ledger distinguishes `QUEUED_APP`, `QUEUED_LIB`, `ACKED`, `FAILED_DEFINITE`
  and `UNKNOWN`, and the conservation equations from section 10.4 balanced on
  every run.
* A broker that disappears mid-run produces `UNKNOWN` outcomes, not false
  failures and not false successes: 14452 acknowledged before the loss, 5548
  `UNKNOWN`, 0 definite failures, 0 left pending, counters balanced, exit code 1
  `INCOMPLETE`. The kill fires once records have demonstrably landed, so a slow
  startup cannot silently turn this into the broker-down-at-startup case.
* A broker that is already down stops the run at startup, because the topic
  contract cannot be verified and section 2.3 requires failing closed.
* Every pinned-policy violation is refused before publishing: idempotence off,
  `acks=1`, in-flight above 5, `retries=0`, a non-murmur2 partitioner, a missing
  topic, a 1-partition topic, a compacted topic, an HA profile on one broker,
  and a mismatched schema descriptor digest.

## What this does not prove

* **Nothing about ClickHouse.** A broker acknowledgement is not a consumer read
  and not a sink commit. Project 05 owns the idempotent sink.
* **Nothing about HA.** The lab ran one broker. RF=3, minISR=2, leader failover
  and ISR shrink (E2E-04, E2E-06, E2E-07) are `BLOCKED` in the matrix, not
  passed.
* **Nothing about 120k events/s.** The rate ladder ran on 8 CPUs against one
  broker with RF=1, 8 partitions per topic and lz4: unpaced 12789 acknowledged
  events/s; 1000/s requested and 998 sustained (`PASS`); 5000/s requested and
  4899 sustained (`PASS`); 20000/s requested and 11930 sustained, reported as
  `FAIL` with the achieved rate next to the request. Safety stayed `PASS` at
  every stage and the app queue never exceeded its 4096 cap. E2E-14 is still
  `INCOMPLETE`, not passed, because the case requires a steady window of at
  least 15 minutes and the longest stage here ran about 20 seconds. A single
  loopback broker is not a capacity result for the section 7.2 target.
* **Nothing about Project 03.** That adapter does not exist. The upstream here
  is `MockP03Classifier` in `src/source/p01_source.cpp`, and every run report
  carries `mock_upstream_p03: true`.
* **No exactly-once anything.** Producer idempotence covers internal retries
  within one producer session. An application-level resend creates a second
  record and the ledger says so.

## Layout

```
schemas/market_data.proto          the pinned P02 contract, verbatim
src/handoff/                       the P03 handoff types, presence-aware
src/policy/publish_gate.*          market-versus-fault decision (section 3.5)
src/routing/route_table.*          topic, key, headers, caps
src/routing/partition_policy.*     pinned murmur2 and the golden audit
src/serialization/                 protobuf encode/decode, framing guards
src/queue/bounded_handoff.*        dual-cap credit budget with hysteresis
src/producer/kafka_transport.h     the transport seam
src/producer/rdkafka_producer.*    the real producer, and the error mapping
src/producer/fake_transport.*      deterministic fault-injectable stand-in
src/ledger/publish_status.*        attempt ledger and conservation equations
src/ops/                           config, metrics, topic guard, run report
src/source/p01_source.*            P01 reader plus the mock P03 classifier
src/app/                           the pipeline and the CLI
verifier/python_snapshot_verifier/ the independent oracle
verifier/cpp_group_verifier/       consumer group and offset-frontier checks
tests/unit, tests/property         146 tests, no broker required
tests/integration/                 real-broker scripts
tests/expected/                    the 280-case verdict matrix
tests/stress/                      the rate ladder
deploy/                            dependency setup, compose, topic creation
config/                            local-dev and reliability-lab profiles, golden mapping
tools/                             golden generator, verdict matrix builder, report merge
```

## Running it

Everything runs on Linux. The lab used WSL2 Ubuntu 22.04 with Docker; the
scripts assume `/mnt/d/trading/prj-3` is the repository root and take overrides
through environment variables.

```bash
# 1. dependencies, pinned (protobuf 3.21.12, librdkafka 2.6.1, kafka-python,
#    the Kafka and Toxiproxy images). Run as root, once.
bash deploy/setup-wsl-deps.sh

# 2. build
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j"$(nproc)"

# 3. unit and property suites, no broker needed
./build/p04_unit_tests

# 4. a source dataset from Project 01 (run from that project)
PYTHONPATH=src python -m market_sim record \
  --config config/bench.toml --count 20000 --rate 500000 --run-id p04-smoke

# 5. end to end against a real broker, with independent verification
P04_BUILD_DIR=$PWD/build bash tests/integration/run_e2e_01.sh

# 6. the real-broker negative and fault suite
P04_BUILD_DIR=$PWD/build bash tests/integration/run_fault_suite.sh

# 7. the rate ladder (E2E-14)
P04_BUILD_DIR=$PWD/build bash tests/stress/run_rate_ladder.sh

# 8. regenerate the verdict matrix after adding evidence
python3 tools/build_verdict_matrix.py
```

Exit codes from `p04_publisher` are part of the contract:

| code | meaning |
|---|---|
| 0 | every attempt terminal, counters balance, no contract violation |
| 1 | `INCOMPLETE`: attempts pending or `UNKNOWN`, so completeness is unprovable |
| 2 | `FAIL`: a contract violation, or counters that do not balance |
| 3 | `NO_GO` before publishing: config, schema pin, golden mapping or topics |

Exit 0 is meant to be hard to reach. A run that merely did not crash gets 1.

## Artifacts a run leaves behind

In `<output_dir>/<run_id>/`:

| file | what it is |
|---|---|
| `report.json` | the section 14 report. Unmeasured fields are `null`, never `0` |
| `report.md` | the same, readable, including a "not proven by this run" section |
| `publish-ledger.jsonl` | one line per publish attempt, with terminal status, broker partition and offset, and the resend ordinal |
| `metrics.prom` | Prometheus exposition, bounded cardinality by construction |
| `snapshot-verifier-report.json` | the independent verifier verdict and findings |
| `merged-verdict.json` | the two documents merged by `tools/summarize_run.py`: the worst of the component verdicts, plus any disagreement between what the publisher believed and what the broker holds |

## Decisions made while building

The design document's ADR table (section 13) is the starting point. These were
settled during implementation and are recorded in `docs/adr/`:

* **Poll while waiting, never block and wait.** In a single-owner design the
  budget is only released by a delivery report, and reports are only served by
  `Poll`. Blocking on a condition variable when the queue is full therefore
  deadlocks until the stall deadline. The first integration run against a real
  broker stopped at exactly 4096 events because of this. The backpressure wait
  is now an explicit poll loop. `BoundedHandoff::ReserveOrWait` still exists for
  a deployment with a dedicated poll thread.
* **A terminal attempt owns no budget.** Finalising pending attempts as
  `UNKNOWN` at the drain deadline also releases their queue reservations;
  without that the ledger and the budget disagree and the final
  `app_queue_records` in the report is non-zero for records that are no longer
  pending. Found by the pipeline test, not by review.
* **Unmapped delivery errors default to `UNKNOWN`.** The mapping table is swept
  over the whole librdkafka error space by a test, so a future client release
  cannot quietly turn a new code into a false "not written".
* **The snapshot is bounded at both ends.** The verifier takes its lower bound
  from offsets captured before the publish run. Scanning from `earliest` made
  the first verification report 44096 records and 20000 duplicates against a
  reused cluster — all of it leftover data, none of it a defect.
* **A measurement across a filesystem boundary measures the boundary.** The
  first rate ladder reported a hard ceiling near 1130 events/s at every
  requested rate. That was the dataset being read across the 9p mount that
  exposes the Windows filesystem to WSL: the same 20000-event run took 32.1 s
  from `/mnt/d` and 1.66 s from ext4, about 19 times faster. The ladder now
  stages the dataset locally before measuring, and the ceiling turned out to be
  roughly 12800 events/s. A number from the wrong side of a stage boundary is
  worse than no number, because it looks like a result.
* **Toxiproxy needs its own advertised listener.** A client that bootstraps
  through a proxy and then receives direct-path metadata reconnects around the
  proxy, so the injected fault never happens. The compose file advertises the
  proxy address on a second listener.

## Known gaps

* `tests/property/` holds property and metamorphic tests; the wire-level fuzzing and model-based suites
  in section 10.5 are not written.
* There is no 3-broker compose profile yet, so the whole H group, E2E-04 and
  E2E-06 through E2E-08 are `BLOCKED` or `NOT_RUN`.
* No soak run, no rate ladder, no skew fixture, no retention-expiry run.
* `config/reliability-lab.example.toml` describes the HA profile and the config
  loader validates it, but no 3-broker cluster was stood up, so it has never
  been run.
