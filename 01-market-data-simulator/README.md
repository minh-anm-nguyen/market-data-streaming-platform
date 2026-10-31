# Project 01 — FX/CFD Market Data Simulator

A deterministic generator of **synthetic** FX/CFD tick and quote data, built as a
controllable test feed for Projects 03–08 of the Market Data Streaming and
Analytics Platform plan.

> **This is not market data.** Prices come from a seeded random walk over integer
> ticks. Nothing here may be used for trading, pricing, or analysis of real
> markets.

Implements `docs/PROJECT_01_FX_CFD_Market_Data_Simulator_System_Design.md`. Read
that for the reasoning; this file covers what was built, what was measured, and
what is still unproven.

---

## 1. The one idea worth keeping

A test feed is only useful if you can tell a bug in the system under test from a
bug in the feed. Everything below follows from that:

* **Determinism.** The same `(config, seed, t0, fault plan)` produces the same
  bytes, so a failure can be replayed exactly.
* **Named boundaries.** `enqueued`, `written`, `fsynced` and `drained` are four
  counters because they are four different claims. There is no counter for "the
  client received it", because nothing in this design can prove that.
* **Anomalies are explained.** Every gap, duplicate and reordering has a row in
  `fault-manifest.jsonl` saying it was intended. An unexplained anomaly fails
  verification.
* **Fail closed.** A disk error stops the run. A killed run leaves a `.part` file
  and no commit marker. Nothing reports success it cannot evidence.

## 2. Quick start

```bash
# Windows
python -m venv .venv && .venv/Scripts/pip install -e ".[test]"
# Linux / macOS
python -m venv .venv && .venv/bin/pip install -e ".[test]"

# record a deterministic dataset
python -m market_sim record --config config/example.toml --count 10000

# verify it (recomputes the hash, re-derives every anomaly)
python -m market_sim verify --run-dir runs/<run-id>

# stream to a consumer on loopback and record at the same time
python -m market_sim both --config config/example.toml --count 100000

# replay a recorded dataset at ten times its original pace
python -m market_sim replay --config config/example.toml \
    --input runs/<run-id> --speed 10

# measure this machine
python scripts/benchmark_report.py --levels 1000,10000 --duration 5 --repeat 3
```

Exit codes: `0` success, `2` bad config, `3` output/IO error, `4` verification
failed, `5` replay input rejected, `6` runtime failure, `7` internal error.

## 3. The data contract (`sim.v1`)

One canonical JSON object per line, newline terminated, keys sorted, no spaces:

```json
{"ask_ticks":108348,"bid_ticks":108346,"dataset_id":"demo-a1b2c3","event_time_ns":1793322000008200000,"event_type":"quote","price_scale":100000,"schema_version":"sim.v1","sequence":42,"symbol":"EURUSD"}
{"dataset_id":"demo-a1b2c3","event_time_ns":1793322000010000000,"event_type":"tick","price_scale":100000,"price_ticks":108347,"schema_version":"sim.v1","sequence":43,"symbol":"EURUSD"}
```

* Prices are **integer ticks**. The decimal value is `price_ticks / price_scale`
  and is never serialised. No float touches a price anywhere in the pipeline.
* `sequence` starts at 1 and increments **per symbol**. The deduplication key is
  the triple `(dataset_id, symbol, sequence)` — two symbols sharing a sequence
  number is normal and must not be treated as a duplicate.
* `event_time_ns` is *logical* event time: `start_time_ns + slot * 1e9 // rate`.
  It does not depend on how fast the machine ran, which is what makes the output
  reproducible.
* A `tick` is a synthetic price update. It carries no volume and no trade id, so
  it cannot be mistaken for an executed trade.
* **TCP uses the same framing.** A client must buffer on `\n`: one `read()` may
  hold a hundred records, or half of one.

## 4. Run directory and the commit protocol

```
runs/<run_instance_id>/
    manifest.json          config hash, seed, t0, runtime -- written first
    events.jsonl.part      the open file; NEVER a finished dataset
    events.jsonl           after the atomic rename
    fault-manifest.jsonl   the audit trail, kept out of the event stream
    report.json            counters, hashes, environment
    COMPLETED              the commit marker, written last
```

The order matters. `events.jsonl` existing is **not** enough: a crash between
the rename and the report would leave a plausible-looking file with no provable
contents. So:

1. drain the queue, flush, `fsync(file)`, check the on-disk size matches,
2. atomic rename `.part` to `events.jsonl`, `fsync(directory)`,
3. `report.json.tmp` to `report.json`, `fsync(directory)`,
4. `COMPLETED`, `fsync(directory)`.

`verify` reports `COMPLETE` only when the marker, the report and a **recomputed**
SHA-256 all agree. The four verdicts are kept apart because they call for
different actions:

| verdict | meaning |
|---|---|
| `COMPLETE` | marker, report, hash and line count agree; every anomaly is explained |
| `INCOMPLETE` | the run never finished committing (a `.part` file, or no marker) |
| `CORRUPT` | the bytes disagree with the recorded hash, or a line will not parse |
| `INVALID` | the dataset parses but contradicts itself or its fault manifest |

## 5. What is *not* promised

* **No exactly-once, no crash-resume.** A killed run is regenerated from its
  seed, never appended to. The `.part` file is evidence, not a dataset.
* **No durability between fsyncs.** `flush()` is not durability. The RPO is
  whatever `fsync_every_events` / `fsync_every_seconds` were set to, and
  `report.json` states both. On Windows a directory handle cannot be fsynced at
  all, so `directory_fsync_supported` is reported as `false` there rather than
  quietly assumed.
* **No TCP delivery guarantee.** `drain()` means our send buffer emptied. It says
  nothing about the peer having parsed or stored anything, so every client row
  reports `client_acknowledged: "unknown"`. An at-least-once live channel would
  need an application ACK and a replay cursor; that is a later milestone.
* **One TCP client.** A second connection is rejected rather than quietly given
  its own fan-out queue.
* **120k events/s is not met.** See the measurements below.

## 6. Measured results

Measured on the development machine, not a quiet benchmark host:

| | |
|---|---|
| CPU | Intel64 Family 6 Model 165, 8 logical cores |
| OS | Windows 11 (10.0.26200) |
| Python | **3.14.4** (the design specifies 3.12 — see limitations) |
| Profile | `record`-only, 3 symbols, fsync disabled |

Ranges are across every run of the suite, not one lucky run.
`TEST_REPORT.yaml` holds the numbers for a single specific run.

| level | target | median achieved (range over runs) | verdict |
|---|---:|---:|---|
| L0 | 100/s | 99/s | rate tracked |
| L1 | 1,000/s | 950–996/s | rate tracked |
| L2 | 10,000/s | 6,700–9,100/s | **under capacity** — reported, not gated |
| L3 | 50,000/s | not reached | **NOT ACHIEVED** |
| L4 | 120,000/s | not reached | **NOT ACHIEVED** |

Record-only ceiling, measured by setting the target far above capacity:
**13,600–25,500 events/s**, nearly a 2x spread between repetitions of the *same*
workload on this machine. That spread is why L2 is measured and reported rather
than gated — a 10k/s pass-fail check on it would be a coin toss dressed up as a
requirement, and section 9.3 says not to force the pass.

Other measurements:

* **1,000,000 records** (E2E-13): 201 MB written, verified in 30.4 s, **peak RSS
  55 MB**. Memory is bounded by the queue budget, not the event count — a
  200,000-event run and a 1,000,000-event run share the same high-water mark.
* **Generation hot path**: ~28 µs/event, roughly a third of it the PRNG. The
  invariant checker costs ~7 µs/event and is deliberately left on: it is the
  reason a gap in a dataset can be called intentional rather than suspected.
* **`asyncio.sleep` granularity here is 5–18 ms**, which sets a floor on
  scheduling precision and is why NFR-04's 2% band (defined over 60 s) is not
  asserted over the 3-second windows the suite uses.

### Why 120k/s is not reachable here

The hot path is pure Python: a per-symbol SplitMix64 draw, an integer price
update, a bounds fold and a canonical serialisation — about 28 µs per event. At
120k/s the budget is 8 µs.

Two optimisations were applied and measured: a `NamedTuple` event instead of a
frozen dataclass (3.47 µs to 1.37 µs per construction) and f-string
serialisation instead of concatenation (4.02 µs to 1.93 µs). Together they moved
an end-to-end run from 4.2k/s to 18k/s, with the golden SHA-256 unchanged. The
remaining cost is spread across work with no cheap Python form.

Per section 11.1 the honest options are to move the generation hot path to C++
and keep Python as the orchestrator, or to partition the generator across
processes and document the resulting ordering. Neither is in scope for Project
01, and neither is pretended to be done.

## 7. Architecture decisions

ADR-001 to ADR-009 from the design document are implemented as written. Two
decisions were taken during implementation:

**ADR-010: SplitMix64, not `random.Random`.** Section 4.2 left the PRNG open and
flagged cross-runtime reproducibility as a future concern; it was brought
forward, because Project 03 has to reproduce a dataset in C++ and the number of
Mersenne Twister words consumed per `random` API call is a CPython
implementation detail. SplitMix64 is about thirty lines of integer arithmetic,
is specified exactly, and bounded draws use rejection sampling rather than
modulo so the mapping from stream to value is implementation-independent too.
`tests/unit/test_prng.py` pins the published reference vector for seed 0 **and**
cross-checks against an independently written transcription, so an optimisation
cannot quietly change the stream.

**ADR-011: `Event` is a `NamedTuple`, not a frozen dataclass.** Both are
immutable, which this design needs — one event goes to two sinks and to the
fault injector, which must copy rather than mutate. A frozen dataclass pays
`object.__setattr__` per field in `__init__`, measured at 3.47 µs against
1.37 µs, on the hot path of every single record.

## 8. Tests

```bash
pytest                       # everything
pytest -m "not slow"         # skip the perf measurements
pytest -m chaos              # kill, disk failure, backpressure, shutdown
pytest tests/property        # Hypothesis
pytest tests/perf -s         # measurements, printed
```

| directory | what it holds |
|---|---|
| `tests/unit` | contract, config, clock, generator, faults, queues, recorder, verifier, golden fixtures |
| `tests/property` | Hypothesis: invariants over randomly drawn valid configs and fault plans |
| `tests/integration` | the section 9.1 E2E scenarios, over the real engine, real sockets, real files |
| `tests/chaos` | `kill -9`, injected disk errors, drain deadlines, background-task failure |
| `tests/perf` | the load ladder and the capacity ceiling |

Golden fixtures live in `tests/fixtures/golden` — the three datasets milestone M0
asks for: clean, gaps + duplicates, stale + reorder. `tests/unit/test_golden.py`
regenerates them and compares bytes, so an unintended change to generated output
fails there rather than propagating into every downstream fixture. It also checks
the fixtures still *contain* the anomalies they advertise: a byte-stable
gap-detection fixture with no gap in it would let a broken adapter pass. To
change them on purpose, run `scripts/make_golden.py` and bump
`GENERATOR_VERSION` in the same commit.

### What-if coverage

The design's section 8 matrix has **105 cases** and section 9.1 lists **15 E2E
scenarios**. Each case id appears in a comment on the test that covers it, so
`grep -rn "T05" tests/` finds the test rather than the prose:

* **98 of 105** cases are covered, **15 of 15** E2E scenarios.
* **7 are not covered**, and here is why rather than a silent gap:

| case | why not |
|---|---|
| T13 | laptop suspend/resume — cannot be provoked in-process; the monotonic clock behaviour is documented instead |
| F16 | resume-after-crash is explicitly out of MVP (ADR-009); regeneration from seed is tested instead |
| R08 | GC pause — `gc.collect()` does not reproduce a real allocator stall; lag percentiles would surface one |
| R09 | cgroup/CPU throttling — needs a container runtime |
| R10 | OOM killer — needs a memory-constrained cgroup; the bounded-queue tests show what caps memory |
| R12 | few cores / little RAM — needs other hardware; the environment is recorded with every measurement |
| R14 | burst vs p99 — partially covered: p95 and p99 scheduler lag and the queue high-water mark are reported, but no deliberate burst-shaped workload is run |

### Bugs this test suite found

Worth recording, because these are the failures the design predicted:

1. **Shutdown deadlock.** `TcpSink.shutdown` awaited `Server.wait_closed()`
   before draining its client sessions. Since Python 3.12 that call waits for
   every connection handler to return, and ours returns only once its session
   closes — in the drain loop that had not run yet. Exactly the deadlock section
   5.4 rules out.
2. **A disconnected client was never noticed.** With a one-way protocol the
   server never read, so a peer that closed while the feed was quiet left a dead
   session holding the single client slot and its file descriptor; a new and
   perfectly good client was then refused as busy. Fixed with a per-session EOF
   watcher.
3. **The symptom outranked the cause.** When the writer died on a full disk, the
   producer saw `QueueClosed` and the run reported a generic internal error with
   exit 6 instead of `OUTPUT_DISK_FULL` with exit 3.
4. **A reorder was counted twice.** The verifier derived gaps at arrival time, so
   a reordered event was reported as both a gap and an out-of-order event. Gaps
   are now "sequences that never arrived".
5. **The verifier was too strict about short runs.** It treated any `COMPLETED`
   run that produced fewer slots than planned as self-contradictory, which
   condemned a legitimate drained `SIGTERM` (NFR-05). It now holds the report to
   its own `exit_reason`.
6. **`json.loads` was running without its C scanner.** Passing `parse_float` and
   `parse_constant` to get precise rejection reasons silently disables the
   accelerated parser, making the verifier about 20x slower than it needed to
   be. The checks now run after parsing and keep the same reasons.

## 9. Known limitations and what has not been run

* **Python version.** The design pins 3.12; this was developed and measured on
  **3.14.4**. The output does not depend on the Python version by construction
  (SplitMix64, integer arithmetic, hand-rolled canonical JSON) and the golden
  fixtures pin the bytes — but that equivalence has not been *measured* on 3.12.
* **A real full volume has not been tested.** `tests/chaos/test_disk.py` injects
  `ENOSPC`, `EDQUOT` and `EIO` at the syscall boundary, which exercises the
  classification and the fail-closed path. A genuinely full filesystem needs a
  loop device or a container quota and remains a manual scenario.
* **Power loss has not been tested.** The RPO is stated and the fsync accounting
  is tested; cutting power to a machine is not something this suite does.
* **No soak run.** Section 9.3 asks for 6–24 hours. The longest run here is
  1,000,000 events. RSS was flat across run sizes, which is suggestive, not a
  soak result.
* **L3 (50k/s) and L4 (120k/s) are not reached**, as above.
* **Multi-client fan-out, TLS and authentication are not implemented.** The
  listener refuses a non-loopback bind unless `allow_public_bind` is set, and
  there is no auth behind it if you do set it.
* **`platform.platform()` costs ~2.4 s on its first call on Windows.** It is
  cached per process and happens before the measured window, so it does not
  pollute a rate — but it does dominate the wall clock of a very short run.

## 10. Status

**Implemented and tested** against the design's section 9.1 scenarios, its
section 8 what-if matrix and its section 9.2 properties — on one machine, on
Python 3.14.4.

Last full run: **706 passed, 2 skipped, 0 failed** in 7m51s. The two skips are
POSIX-only permission tests. `TEST_REPORT.yaml` carries the counts, the
environment and a reference run's hashes, and is regenerated by
`scripts/make_test_report.py`.

The GO criteria of section 12.1 are met for items 1–5. Item 6 (a benchmark
covering throughput, CPU, RAM, disk and p95/p99 lag) is reported with L3 and L4
recorded as **NOT ACHIEVED** rather than quietly dropped.

Suitable as a test feed for Project 03 at up to roughly 10k events/s on hardware
like the above. Above that, measure first.
