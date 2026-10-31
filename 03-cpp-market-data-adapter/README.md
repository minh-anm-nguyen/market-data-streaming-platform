# Project 03 — C++20 FX/CFD Market Data Adapter

Consumes the Project 02 `MDP1` / `marketdata.v1` wire contract, validates it
structurally and semantically, tracks per-symbol stream quality, and hands
classified events to a bounded sink. Offline `.pbstream` replay and live TCP
share one pipeline, so a result never depends on which transport delivered
the bytes.

Design document:
[`../docs/PROJECT_03_CPP_FX_CFD_Market_Data_Adapter_System_Design.md`](../docs/PROJECT_03_CPP_FX_CFD_Market_Data_Adapter_System_Design.md).
Where the implementation diverges from it, [SYSTEM_DESIGN.md](SYSTEM_DESIGN.md)
says so and points at the record.

## What this is, and what it is not

It **detects and classifies**; it does not repair. There is no clamping, no
rounding, no gap filling, no protocol sniffing and no resynchronisation
after corruption. A successful outcome can be a reasoned rejection, a
degraded-but-flagged forward, backpressure, or a clean refusal to start.

Three claims it will never make on its own:

| Claim | Why not |
|---|---|
| "No data was lost" | Only provable against a verified source manifest. Otherwise the report says `null` — unknown, not zero. |
| "Delivered to Kafka" | The P04 sink is a stub. A successful write means bytes reached an interface, never a broker ack. |
| "Sustains N events/s" | A single run measures a single run. `sustainable_throughput_verified` is always `false` here; that verdict belongs to a benchmark lane with a steady-state criterion. |

## Build

Requires a C++20 compiler, CMake 3.22+, Ninja, protobuf (protoc and
libprotobuf from one matched install), and GoogleTest. Linux: the transport
layer uses POSIX sockets and `/proc`.

```bash
export P03_PROTOBUF_ROOT=/path/to/protobuf/prefix   # expects bin/, include/, lib/
./scripts/build.sh release
```

Flavours: `release`, `perf`, `debug`, `asan`, `ubsan`, `tsan`, `fuzz`. Build
directories live outside the source tree (`$P03_BUILD_ROOT`, default
`~/.cache/p03-build`).

## Run

```bash
# Validate the configuration without processing anything.
./md_adapter --config config/example.toml --check-config

# Print the digest of the protobuf contract this binary was built against,
# to pin in identity.expected_descriptor_sha256.
./md_adapter --print-descriptor

# Offline replay of a verified .pbstream.
./md_adapter --config config/example.toml \
    --input-mode pbstream --input fixtures/normal/golden.pbstream \
    --report reports/offline.yaml

# Live MDP1 over loopback.
./md_adapter --config config/example.toml \
    --input-mode tcp --host 127.0.0.1 --port 9010 \
    --report reports/live.yaml
```

Command-line overrides rewrite the matching key in the TOML and the result
is re-parsed, so they pass through the same validator as the file. There is
no second, looser path for them.

### Exit codes

| Code | Meaning |
|---:|---|
| 0 | Completed (`COMPLETE_OFFLINE` or `PARTIAL_LIVE`) |
| 2 | Configuration rejected |
| 3 | Source completeness could not be established |
| 4 | Stream failed (bad magic, bad length, truncation, decode or structural reject) |
| 5 | Incomplete (shutdown requested, writer did not drain) |
| 6 | Backpressure timeout |
| 8 | Sink I/O error |

A harness asserts on these rather than grepping stderr.

## Testing

```bash
./scripts/run_e2e_matrix.sh         # build + ctest + the subprocess e2e matrix
./scripts/build_sanitizers.sh       # ASan/UBSan and TSan, suite under each
./scripts/run_fuzzers.sh 120        # libFuzzer targets, bounded budget
./scripts/run_soak.sh 6             # long-running live soak
./scripts/generate_test_report.sh   # one report from whatever actually ran
python3 tools/verify_report.py reports/offline.yaml
```

Layers:

- **unit** — framer, decoder, validators, sequence tracker, freshness
  engine, classifier, bounded queue, metrics, sinks, config.
- **property** — chunk-partition invariance, replay determinism, per-symbol
  independence, bounded state under adversarial input, counter
  monotonicity.
- **integration** — the real runner over files and over a real loopback
  socket with a scriptable hostile server.
- **e2e** — the real binary as a subprocess: exit codes, on-disk artifacts,
  signals, config rejection, the P04 handoff shape.
- **fuzz** — framer, decoder, whole pipeline, config parser.

Expectations are written by hand from the design document, or produced by
`tools/reference_oracle.py` — an independent model, in another language,
with a deliberately different data structure. `test_oracle_parity` runs both
over the same trace and fails if they disagree. Fixtures are encoded with
Google's *Python* protobuf runtime (`tools/make_fixtures.py`), never with
the C++ code under test, so a shared bug cannot make a golden file agree
with a broken decoder.

State on the development host: **211 ctest cases and 79 end-to-end cases
pass**; ASan+UBSan and ThreadSanitizer are clean across the whole suite; all
four fuzz targets ran clean (40k–625k executions each in a 45 s budget).
Capacity is deliberately **not** established — see
[docs/benchmarks.md](docs/benchmarks.md).

## Layout

```
include/md_adapter/   public headers
src/                  implementation (one file per stage)
schemas/              frozen copy of the P02 .proto (never forked here)
config/               example and benchmark configurations
tests/                unit, property, integration, e2e, fuzz
tools/                fixture generator, reference oracle, TCP fault server,
                      fault scorer, report verifier
scripts/              build and verification entry points
docs/adr/             accepted decisions and their trade-offs
```

## Contract notes

**Wire.** `MDP1` appears exactly once per file or connection, then repeated
`[BE32 length][payload]` with `length` in `1..4096`. The magic is a one-shot
prefix, not a sync word: a corrupted stream is closed, never
re-synchronised.

**Schema.** `schemas/market_data.proto` is a frozen copy of the Project 02
contract. Project 02 is not implemented yet, so this copy is the de-facto
source today — pin `identity.expected_descriptor_sha256` and replace the
file with the P02 artifact once it exists. An `UNPINNED` digest starts with
a loud warning.

**Manifest.** Before an offline replay may be called COMPLETE, a sibling
`<file>.manifest.json` must exist with `status: "COMPLETED"` and a `sha256`
and `bytes` that match the file on disk. See
[docs/protocol_handoff.md](docs/protocol_handoff.md).

## Known limitations

- **No app-level ACK or replay cursor.** Live completeness is unprovable
  without external source-side evidence, so a live run reports
  `PARTIAL_LIVE` even when every byte arrived.
- **A `SEQUENCE_ROLLBACK` is not self-healing.** When a sender restarts at
  the baseline inside one `dataset_id`, the adapter refuses the event,
  raises an integrity alert and marks continuity uncertain. Subsequent low
  sequences classify as duplicates or unverifiable, so the stream
  effectively stops being published until the operator starts a new dataset
  epoch. Deliberate fail-closed behaviour, not a transient.
- **Offline replay does not detect quote inactivity.** Inactivity is
  measured on receive time, which has no meaning when a file is read at
  disk speed. Offline runs still detect event-time staleness; the
  inactivity engine is covered by unit tests with a scripted virtual clock.
- **A structural reject closes the stream.** This is the MVP fail-closed
  policy, so a corpus of deliberately malformed messages needs one file per
  case.
- **`output.stub_delay_us` is a test-only throttle** on the P04 stub. It is
  echoed into every report, so a throttled run cannot be mistaken for an
  unthrottled one.
- **Capacity is unestablished.** See
  [docs/benchmarks.md](docs/benchmarks.md) for what has actually been
  measured and what has not.

## Where to look next

| Question | Start at |
|---|---|
| What does this error mean? | [docs/runbook.md](docs/runbook.md) |
| What can it not detect? | [docs/failure-catalog.md](docs/failure-catalog.md) |
| Which tests actually run? | [docs/test-coverage.md](docs/test-coverage.md) |
| What do the neighbours owe it? | [docs/protocol_handoff.md](docs/protocol_handoff.md) |
| What has actually been measured? | [docs/benchmarks.md](docs/benchmarks.md) |
| Why is it built this way? | [docs/adr/](docs/adr/) |
