# Project 02 — Market Data Protobuf Schema & Cross-Language Contracts

`marketdata.v1`: the Protobuf contract, the C++20 and Python codecs, the MDP1
framing, the schema-evolution gate, and the conformance suite that checks them
against each other.

> **Synthetic data only.** A `Tick` here is a simulated price update, not an
> executed trade; a `Quote` is not a real market quote. Nothing in this project
> is market data and nothing in it is usable for trading.

`SYSTEM_DESIGN.md` is the design this implements, including the 170-case what-if
matrix the tests are keyed to. This file covers what exists, what was measured,
and what is **not** established.

---

## Status

| | |
|---|---|
| Contract | `marketdata.v1`, frozen; field numbers 1–6, 10–11 |
| Framing | `MDP1` + `[uint32 BE length][payload]`, `1 ≤ L ≤ 4096` |
| Source format | Project 01 `sim.v1` JSONL |
| Toolchain | protoc 35.1 · libprotobuf C++ 7.35.1 · Python protobuf 7.35.1 (`upb`) · g++ 16.1.0 · Python 3.14.4 |
| Tests | **216 Python tests + 123 C++ checks, all passing** |
| What-if matrix | **128 of 170 cases covered**; 42 documented as unreachable here, 0 unexplained |
| Capacity | **`NOT_ESTABLISHED` at 50k and 120k events/s** — see [Measured](#measured-not-claimed) |
| Release verdict | **`NOT_EVALUATED`** — a `GO` requires the whole matrix to have run |

The release verdict is deliberately not `GO`. Section 14.1 requires the matrix
to have been executed, and 42 cases could not be run in this environment
(cgroup limits, a 60-minute soak, ENOSPC injection, a Kafka broker, a second
protobuf install). Those stay `NOT_RUN` and are listed with reasons in
`tests/test_matrix_coverage.py`. Reporting `GO` with 42 unknowns would be the
false success the design argues against.

---

## Layout

```
schemas/marketdata/v1/market_data.proto   the contract -- the single source of truth
schemas/evolution/                        v1.1, a v2 probe, and 7 deliberately breaking variants
toolchain/versions.lock                   pinned versions + SHA-256 of schema and descriptor
python/src/md_contract/                   strict reader, codec, framing, validation, gate, CLI
cpp/                                      an independent C++20 implementation of the same rules
tests/{unit,property,integration,compatibility,fuzz,performance}
scripts/generate_proto.sh                 codegen for both languages from one schema
scripts/compare_descriptor.py             the schema-evolution gate
scripts/run_matrix.sh                     every evidence-producing step, in dependency order
reports/                                  interop.json, benchmark.json, test-report.json
```

## Running it

```bash
# 1. codegen (writes toolchain/versions.lock) and the C++ build
bash scripts/generate_proto.sh
cmake -S cpp -B build/cpp -G Ninja -DMD_EXPECTED_PROTOBUF_VERSION=35.1.0
cmake --build build/cpp

# 2. everything: gates, both test suites, conformance, benchmarks, report
bash scripts/run_matrix.sh

# or individually
python -m md_contract toolchain-check
python -m md_contract source-check --run-dir ../01-market-data-simulator/runs/smoke-rec-1
python -m md_contract encode-jsonl --run-dir <p01-run> --output out.pbstream --mode strict_normal
python -m md_contract decode-pbstream --input out.pbstream --output core.jsonl
python -m md_contract verify-pbstream --input out.pbstream --expected-jsonl <p01-events.jsonl>
python -m md_contract cross-lang-conformance --source <events.jsonl> --include-versions
python -m md_contract schema-gate --candidate build/descriptors/marketdata-v1_1.desc
pytest tests -q                     # add -m "not perf and not slow" for a fast run
```

Exit codes: `0` success, `1` the work ran but something was rejected or
mismatched, `2` the request was refused (bad config, unverified source).

---

## The five things worth knowing

### 1. The oracle is a typed projection, never a byte hash

Protobuf's output is not canonical, so two different byte strings can carry the
same event. Comparing `SHA-256(bytes)` across C++ and Python would fail on
correct implementations and could pass on two that share a bug. So the oracle is
`core_projection_v1`: every v1 field, its presence flag and the payload case,
rendered as canonical JSON that both languages produce **byte-identically**.

Verified over 5000 events in both directions, plus a boundary corpus
(`INT64_MIN`, `INT64_MAX`, `0`, `-1`, `2^53+1`). Where byte stability genuinely
matters — replaying an exact frame — the raw frame is kept instead.

### 2. Parsing is not accepting

`ParseFromString` succeeding means the bytes were syntactically a
`MarketDataEvent`. Nothing more. proto3 has no required fields, so the **empty
byte string parses into a valid message with every field absent**. Every parse
is therefore followed by an explicit presence check, and absent is never
defaulted to zero:

| on the wire | `sequence` | envelope check |
|---|---|---|
| field omitted | absent | `MISSING_REQUIRED_SEMANTIC_FIELD` |
| `1800` (explicit 0) | `0`, present | passes; the value policy decides |

### 3. Two modes, and neither one repairs anything

`strict_normal` rejects a business violation with a reason.
`fault_passthrough` preserves it **exactly** — but only when Project 01's fault
manifest shows a fault that explains *that anomaly* at *that* `(symbol,
sequence)`. On the real fault dataset:

```
strict_normal      4997 encoded, 3 rejected  BUSINESS_VIOLATION
                   bid=-1 · bid=0 · bid 126567 > ask 126564
fault_passthrough  5000 encoded, 0 rejected  faults f0001, f0002, f0003 preserved
```

A `stale_timestamp` fault does not license a negative price. An unapplied fault
licenses nothing. A transport-channel fault licenses nothing in the payload. No
manifest at all means the mode refuses to run.

Sequence gaps, duplicates and reordering are **not** judged per event — they are
properties of a stream, and detecting them is Project 03's job. Both modes
forward them untouched, so counts and order are preserved.

### 4. `protoc` does not protect the contract

Seven deliberately breaking schema variants were compiled. **Six compiled
without complaint** — renumbering `sequence`, `int64`→`double` on a price,
`int64`→`uint64` on the sequence, deleting `price_scale`, renaming a field. Only
the one using a field number in the implementation-reserved 19000–19999 range
was refused.

That is what `scripts/compare_descriptor.py` exists for:

| change | protoc | the gate |
|---|---|---|
| add `optional feed_label = 20` | compiles | **ALLOWED** (A03) |
| add `oneof` variant `depth_update = 12` | compiles | **CONDITIONAL** (A11) — only if old readers fail closed |
| rename a field, same tag | compiles | **CONDITIONAL** (A09) — the wire survives, generated APIs and ProtoJSON do not |
| `sequence` 3 → 7 | compiles | **FORBIDDEN** (A01) |
| `bid_ticks` `int64` → `double` | compiles | **FORBIDDEN** (A06) |
| `sequence` `int64` → `uint64` | compiles | **FORBIDDEN** (A07) — wire-compatible, and would reinterpret the negative fault values |
| remove `price_scale` | compiles | **FORBIDDEN** (A05) |
| field number `19000` | **refused** | — |

The version matrix runs in **separate processes**, which is forced rather than
chosen: every generated module registers the same file name in the default
descriptor pool, so importing two variants in one interpreter raises
`duplicate file name`. A v2 writer's `depth_update` reaching a v1 reader gives
40 frames read, 40 `UNSUPPORTED_EVENT_KIND`, **0 events emitted** — it is never
read as a quote or a tick.

### 5. Protobuf cannot detect a bit flip that still parses

There is no checksum in the wire format. A single flipped bit inside a price
varint can yield a message that parses perfectly and carries a different price.
`tests/integration/test_cross_language.py` demonstrates this: it searches for
such a flip, finds one, and shows the decoder reporting zero errors while the
event has changed. Only the comparison against the source caught it.

That comparison exists in test and not in production, which is why the design
says a path outside a trusted environment needs authenticated transport. MDP1
has no authentication, no encryption and no application-level ACK, and there is
deliberately no `peer_accepted` counter anywhere — `frames_written` means bytes
handed to the kernel, nothing more.

---

## Measured, not claimed

Hardware: 8-core Intel, Windows 11, Python 3.14.4 (`upb`), g++ 16.1.0
RelWithDebInfo. Every figure is the **median of several trials with the range
beside it**, because a single run on this machine is not reproducible: repeating
one unchanged encode lane gave 65k, 88k, 91k, 134k, 139k and 149k events/s in
one sitting, and the final run's five B1 trials spanned 26k–140k (×5.32). The
spread is unaffected by disabling the garbage collector, so it is CPU frequency
scaling and background load rather than anything in the code. **A single-run
number from this machine should not be quoted.**

Payloads in this corpus are 64 bytes (p50 = p99 = max), which is the figure the
rates below belong to; a 4096-byte corpus would give different ones.

| lane | median | spread | p50 | p99 |
|---|---:|---|---:|---:|
| B1 encode (in memory) | 129,092 ev/s | ×5.32 | 5.8 µs | 9.7 µs |
| B2 decode + envelope (in memory) | 58,590 ev/s | ×1.48 | 11.2 µs | 57.3 µs |
| B5 JSONL → encode → file+fsync → decode | 3,838 ev/s | ×1.00 | | |
| B8 reject a malformed corpus | 67,159 ev/s | ×1.47 | | |

Encode+decode+validate round trip, open loop: **median 27,730 ev/s**
(range 25,633–29,859 over three trials).

| paced target | achieved | verdict |
|---|---:|---|
| 1,000/s | 998 | **PASS** |
| 10,000/s | 9,996 | **PASS** |
| 50,000/s | 34,448 | **ABOVE_MEASURED_CEILING** |
| 120,000/s | — | **NOT_RUN** (the climb stops at the first rung that fails) |

B8 rejecting malformed input at 67k/s against B2 accepting valid input at 59k/s
is the result that matters there: refusing bad bytes is not more expensive than
accepting good ones, so garbage is not an amplification vector.

**Max sustained: 10,000 events/s (`ESTABLISHED`). 50k is above this machine's
measured ceiling and 120k was not attempted, so both are `NOT_ESTABLISHED`.** The platform-level 120k figure belongs to the whole pipeline across
languages and machines; it is not this codec's measured throughput and is not
claimed as such.

Three measurement traps were hit and fixed while building this, each recorded in
the code:

- per-event `perf_counter` instrumentation made decode read 11k ev/s against 88k
  uninstrumented — an 8× understatement, with the instrumentation dominating the
  thing being measured. Latency is now sampled 1-in-16 while throughput is taken
  over the whole batch.
- per-event pacing could not hit a 100 µs interval through the OS timer, giving
  3.7k ev/s against a 10k target while reaching 25k unpaced — the harness was
  the bottleneck, not the codec. Pacing is now batched.
- a rung above the machine's ceiling was reported `DEGRADED`, which blames the
  pipeline for a limit that belongs to the machine. It now reports
  `ABOVE_MEASURED_CEILING`.

B3, B4, B6 and B7 are named in the report as not run rather than omitted.

---

## Bugs this project's own tests found

Worth listing, because each is a class of defect the design predicted:

1. **A cross-language divergence.** C++ `mutable_quote()` sets the oneof case on
   access; Python's `msg.quote` does not. An empty `Quote{}` was therefore
   unrepresentable in Python, so the two languages disagreed about an event with
   no prices. Fixed with an explicit `SetInParent()`.
2. **An unclassified exception.** A JSON `"\ud800"` escape decodes into a Python
   string holding an unpaired surrogate, and `.encode("utf-8")` then raised
   `UnicodeEncodeError` straight out of the parser, breaking the "every
   rejection is typed" guarantee. Found by fuzzing; now `INVALID_UTF8`.
3. **A false rejection.** The over-long-numeric-token guard scanned digits
   inside string literals, so a legal 20-digit `dataset_id` was rejected as an
   over-long number. Found by a property test; the guard now skips strings.
4. **An inverted gate verdict.** proto3 implements `optional` presence with a
   *synthetic* oneof per field, so `oneof_index` is set on every field in this
   contract. Reading it naively made the gate report an added optional field as
   an added oneof variant — inverting its verdict on the most common safe
   change. Fixed by checking `proto3_optional`.
5. **A misleading API.** `PipelineResult.accepted` read a decode-side counter and
   was therefore always 0 after an encode run. Split into `encoded` and
   `accepted`.

## Limits

- `MDP1` has no authentication, no encryption, no ACK, no replay cursor and no
  exactly-once semantics. Loopback only unless `allow_public_bind` is set
  explicitly.
- A corrupted stream is not resynchronised. The reader stops at the exact offset
  and reports it; scanning for the next plausible frame would silently skip an
  unknown amount of data.
- `fault_passthrough` is for test streams. It must not be enabled against a real
  price source.
- The schema gate cannot prove *semantic* compatibility. Changing the meaning of
  `price_scale` while keeping its type is invisible to a descriptor diff, which
  is why it is paired with ADR review.
- 42 of the 170 what-if cases were not run here. They are listed with reasons,
  not quietly dropped.

## Handoff

`HANDOFF.md` has what Projects 03 and 04 need: the descriptor hash, the framing
rules, and the one that is easiest to get wrong — a Kafka record value is a
**raw** `MarketDataEvent`, with no `MDP1` prefix and no length header.
