# Test coverage against the what-if matrix

The design document (section 9) catalogues **231** what-if cases across 16
groups. This page says which of them executable tests actually exercise, and
which they do not.

It is not a score. "Partial" means some cases in the group run and others do
not. "Not covered" means no test touches the group, and no claim should be
made about it.

**The 231 cases have not all been executed.** By the design document's own
release policy (section 17.1) an untested `P0` case is a `NO-GO` for the
claim it covers, so the gaps below are load-bearing rather than cosmetic.

For what the adapter *does* about each failure class, and what it cannot
detect at all, see [failure-catalog.md](failure-catalog.md). This page is
only about which tests run.

## Executed

| Group | Scope | Covered by | State |
|---|---|---|---|
| **A** | Protocol selection, address, connect | `test_config` (port 9009, non-loopback opt-in, unknown enums), `test_tcp_live` (refused connect with bounded backoff, silent peer, wrong magic), e2e `CFG-01..06`, `A-01`, `A-07` | partial — multi-address DNS and an IPv4/IPv6 bind mismatch are not exercised |
| **B** | Non-blocking TCP, stream faults | `test_framer` (chunk independence, randomised partitions), `test_tcp_live` (byte-at-a-time, slowloris, mid-frame close), e2e `E2E-02`, `B-*` | partial — `EINTR`/`EAGAIN` are handled in code but not fault-injected |
| **C** | MDP1 framing, length, corrupted bytes | `test_framer` (magic, every length bound, truncation at each stage, deadlines, pause/resume, reset), e2e `E2E-06` (11 framing fixtures), `fuzz_framer` | good |
| **D** | Protobuf decoding, presence, version | `test_decoder`, `test_validators`, e2e `E2E-06` (16 message fixtures), `fuzz_decoder` | good |
| **E** | Business semantics, fixed point, identity | `test_validators` (every reason code, `INT64_MIN`/`MAX` arithmetic), e2e `E-biz` | good |
| **F** | Sequence: gaps, duplicates, reorder | `test_sequence_tracker` (window edges, huge gap, rollback, all caps), e2e `E2E-03`, `F-huge gap`, `FAULT-*` | good |
| **G** | Event-time, inactivity, clocks | `test_freshness` (scripted virtual clock throughout), e2e `E2E-04` | partial — host suspend/resume is not testable here |
| **H** | Provenance, replay, deterministic oracle | `test_offline_e2e`, `test_oracle_parity`, e2e `H-*`, `E2E-08` | good |
| **I** | Sink, backpressure, loss reporting | `test_bounded_queue`, `test_sink`, `test_offline_e2e` (slow sink vs stalled sink), e2e `E2E-07`, `I-*` | partial — `ENOSPC` and `EIO` at fsync are not injected |
| **J** | Bounded memory and resources | `test_state_bounds` (cardinality attack, huge jumps, whole-pipeline bounds), `test_sequence_tracker` caps | partial — `EMFILE`, rlimits and cgroup OOM are not exercised |
| **K** | Concurrency, ownership, signals | ThreadSanitizer over the whole suite, `test_bounded_queue` SPSC, e2e `K-SIGTERM`, `K-SIGKILL` | partial |
| **N** | Metrics, evidence, oracles | `test_metrics` (conservation, histogram), `tools/verify_report.py`, `tools/fault_sink.py`, e2e report assertions | good |
| **O** | Integration, cross-component | e2e `E2E-09` (P04 handoff: key, raw value, side channel) | partial — Projects 01 and 02 do not exist yet, so the real chain is untested |
| **P** | CI, determinism, release | e2e `E2E-08` (artifact digest stable across read sizes), `scripts/generate_test_report.sh` | partial — there is no CI pipeline |

## Not covered

| Group | Why |
|---|---|
| **L** — throughput, latency, overload, fairness | Measured but not *verified*. This host cannot produce a trustworthy capacity figure; see [benchmarks.md](benchmarks.md). The load progression and the soak lane have not been run. |
| **M** — security, hostile data, supply chain | Partly reached by the fuzz targets and by the loopback and descriptor-pinning gates, but there is no dependency audit, no signed-artifact policy and no adversarial-traffic lane (benchmark lane F). |

## What the executed tests amount to

- **211** ctest cases: unit, property, integration.
- **83** end-to-end cases driving the real binary as a subprocess.
- AddressSanitizer + UndefinedBehaviorSanitizer: clean over the whole suite.
- ThreadSanitizer: clean over the whole suite.
- Four libFuzzer targets: clean within their budget (33k–959k executions
  each at 30 s).

Four defects were found and fixed during development, three of them by a
test failing for the right reason:

| Defect | Found by |
|---|---|
| Backpressure could never engage, because events that left the deque stopped counting against the caps | e2e `E2E-07` |
| Frame and stall deadlines could never expire during replay, because they were measured on a clock that does not tick | e2e `E2E-07` |
| The writer thread was left running while the sink it was writing to was closed | `OfflineE2E.StalledSink...` (segfault) |
| The documented CLI override path rejected itself as a duplicate key | inspection; now covered by e2e `CFG-05/06` |

ThreadSanitizer additionally found a real race in the test harness itself
(the fake server's listening descriptor closed while its own thread still
read it), and clang found dead state in `SinkWriter`.

## Honest summary

Correctness, framing safety, stream-state classification and provenance
rules are well covered and independently cross-checked. Resource
exhaustion, OS-level faults, capacity and security are covered thinly or not
at all. Nothing here supports a `GO` verdict on availability or throughput.
