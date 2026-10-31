# P09 architecture decisions

Three decisions that shaped the assembly. Each is here because it was not
obvious, cost real time to establish, and would be easy to undo by accident.

---

## ADR-0001 — What identifies the wire contract across projects

**Status:** accepted · 2026-10-31

### Context

Four projects keep their own copy of `marketdata.v1`, and three of them pin a
"descriptor sha256" that was clearly meant to be a shared identity. No two
values matched:

| project | pinned value | what it hashes |
|---|---|---|
| P02 | `0e6fc2b2…` | `protoc --descriptor_set_out` FileDescriptorSet |
| P03 | `3363eeed…` | FileDescriptorSet wrapping a runtime copy |
| P04 | *(was unset)* | serialized FileDescriptorProto |
| P05 | `e17bf37b…` | serialized FileDescriptorProto |

The first version of the audit called this unfixable and blamed protobuf's lack
of a canonical serialisation. That was the wrong diagnosis. Building P03 and P04
and reproducing both digests in Python established:

- protobuf **3.12.4 (C++)**, **3.21.12 (C++)** and **7.35.1 (Python)** serialize
  byte-identical `FileDescriptorProto` for this schema, `proto3_optional`
  included. The runtimes agree exactly.
- P04's built value is `e17bf37b…` — the same as P05's, across a 3.21 C++
  runtime and a 7.35 Python one.
- The differing values differ by **representation only**: the wrapper (set vs
  bare proto), the `name` field carrying the path `protoc` was invoked with, and
  `json_name`, which `--descriptor_set_out` populates and a runtime `CopyTo`
  omits because it is derivable.

Normalising those three things reconciles all of them to `e17bf37b…`.

### Decision

Two identities, with different jobs.

**The primary cross-project identity is the normalised schema digest** from
`protoparse.py`: the package, message names, and every field's number, type,
label and oneof membership, canonically ordered and hashed. It ignores
formatting, comments and file paths, and needs no protobuf runtime. The lock
records it as `cross_project_identity.wire_schema.normalized_sha256`.

**Each project keeps its descriptor pin as a per-build self-check**, and P09
*derives* what that pin must be from one canonical descriptor in that project's
own encoding (`descriptors.py`). The question asked of each project is the one it
can answer: *does your pin equal what this schema yields in your encoding?*

### Consequences

- A cross-project provenance check now exists and passes, where before it could
  not have passed for any schema.
- P04's pin was set from a real build, so its own case D-08 is live.
- The lock labels each descriptor value with the object it hashes, under
  `per_build_identity`, so a future reader cannot mistake the spread for a defect
  and "fix" it by copying one value over the others.
- `descriptors.py` carries a table of representations. A project that changes how
  it hashes must add one, and the audit fails until it does — the intended cost.

### Alternatives rejected

- **Make all four hash the same object.** Cleanest in principle, but it
  invalidates three projects' pinned self-checks and their tests for no gain: the
  normalised digest already provides a runtime-independent identity.
- **Pick one project's value as canonical.** Wrong for the other three by
  construction, and the audit would have to special-case them.
- **Drop the pins, keep only the normalised digest.** Loses the per-build check
  that catches stale generated code — a real failure mode the text digest cannot
  see.

---

## ADR-0002 — How P03 and P04 are joined

**Status:** accepted · 2026-10-31

### Context

P04 read a P01 run directory and classified it with `MockP03Classifier`, a
re-implementation of a policy that belongs to P03. Its own README said so, and
every run report carried `mock_upstream_p03: true`. So the `P01 → P02 → P03 →
P04` chain P09 M4/M5 requires did not exist, and nothing downstream proved
anything about the adapter (finding P09-M0-05).

P03 already writes the handoff: its `p04_stub` sink emits one NDJSON record per
forwarded event with `key`, `value_hex` (the untouched `marketdata.v1` wire
bytes), `value_bytes`, and a `quality` side channel carrying flags, feed state,
primary reason and arrival index.

### Decision

Add a second upstream to P04 rather than replacing the first:
`run.upstream = "p01_mock" | "p03_handoff"`, defaulting to `p01_mock`.

`p03_handoff` reads P03's stream through `src/source/p03_source.cpp`, which:

- decodes `value_hex`, then **re-encodes the decoded event and compares it to
  P03's original bytes**, failing the run on any difference. P04 serializes the
  value it publishes, so without that check "these are P03's bytes" would be an
  assumption; with it, byte preservation is a verified invariant.
- rejects a payload carrying `MDP1` magic or a length prefix instead of stripping
  it. A Kafka value carries its own boundary, so a framing prefix is the upstream
  packaging bug of case L07, and stripping it hides it.
- requires `record.key` to equal the decoded `symbol` (case L08): the key picks
  the partition and the payload carries the meaning, so a disagreement is a
  routing violation.
- maps P03's reason vocabulary to P04's `Classification`, mapping **an
  unrecognised reason to `kUnclassified`**, which P04 refuses to publish. A
  reason the table has not seen stops the run rather than entering the market
  topics as valid. This fired in practice: P03 prints `Reason::kNone` as
  lowercase `"none"`, alone among its reasons, and the run failed closed until
  the mapping was corrected.

`source_run_dir` stays required in both modes: P01's report is the independent
source manifest every conservation equation is stated against, and the handoff
stream is not a substitute for it.

### Consequences

- The real chain runs. Measured on this host: 2000 source events → P03
  `COMPLETE_OFFLINE` with 2000 business-ok → P04 `verdict=PASS`,
  `mock_upstream_p03: false`, 2000 upstream-accepted, 1997 broker-acked, 0
  definite failures, 0 unknown, 0 pending, conservation `ok`.
- P04's 146 existing tests still pass; the mock path is untouched.
- The reason table is a maintenance surface. It fails closed, so P03 adding a
  reason costs a failed run with a named cause rather than silent
  misclassification — the right direction for the error to point.
- `mock_upstream_p03` is computed from the config now, so a report cannot claim
  a real adapter while the mock produced the events.

### Alternatives rejected

- **Link P03's classifier into P04 directly.** Tighter, but it couples two build
  systems with different pinned protobuf versions (3.12.4 and 3.21.12) and makes
  P03's policy untestable from P04 without the whole adapter.
- **Convert P03's output into P01 JSONL and keep the mock.** Discards the
  classification, which is the entire point of having P03 in the path.
- **Publish `value_hex` directly without decoding.** Skips P04's contract
  validation, route table and partition policy — the parts that make it more than
  a byte forwarder.

---

## ADR-0003 — Where each component runs, and why it is not uniform

**Status:** accepted · 2026-10-31

### Context

The platform mixes runtimes: six Python projects declaring `requires-python >=
3.12`, and two C++ projects built against different pinned protobuf prefixes. The
Docker daemon lives inside WSL2 Ubuntu-22.04, whose system Python is 3.10 — no
`tomllib`, so P01's config loader cannot even import. Kafka and ClickHouse are
published on the WSL host's loopback, which Windows cannot reach at all.

Three facts constrained every option:

1. **Published ports are reachable from WSL-native processes and from
   `--network host` containers.** Not from Windows; and the in-network name
   `kafka:9094` was reachable from a bridge-network container once and not on a
   later attempt.
2. **This host drops published-port connectivity across idle gaps.** Kafka and
   ClickHouse's native port both became unreachable after pauses, on containers
   reporting zero restarts, recovering on restart.
3. **Renaming Kafka's host listener broke P04.** Named `HOST` rather than
   `PLAINTEXT`, P04's librdkafka 2.6.1 could not complete an ApiVersions
   exchange, though the same broker served a 2.16.1 client fine.

### Decision

- **Python components run in one pinned image**
  (`images/python-tools.Dockerfile`, `python:3.12.7-slim-bookworm`) carrying all
  six projects plus P09. One image, not six: they are a toolchain run as a
  sequence, not six daemons, and six images of one interpreter would cost build
  time and drift without buying isolation. A component that becomes a daemon
  earns its own image then.
- **Python stages run with `--network host`**, using the same `127.0.0.1:9092`
  and `127.0.0.1:8123` the native binaries use — the only addresses reachable
  from both halves of the system.
- **C++ components run as native WSL binaries**, built out of tree.
  Containerising them means rebuilding two different protobuf prefixes inside
  images, which buys nothing while they are invoked as pipeline steps.
- **Kafka keeps its host listener named `PLAINTEXT`**, the exact shape P04's own
  lab proved. A second `INTERNAL://kafka:9094` listener stays defined for future
  in-network services.
- **ClickHouse is reached over HTTP 8123**, never native 9000. P05 already speaks
  HTTP, so nothing in the data path needs the port that flaps.
- **The pipeline runs in one continuous pass and retries the broker probe**,
  rather than treating the first transport failure on this host as real.

### Consequences

- The pipeline runs end to end today, which no uniform choice achieved.
- This topology is **not** what a production deployment should use. It is
  recorded so the next reader does not take it as a recommendation: M1 still owes
  per-service images pinned by digest, and the `INTERNAL` listener is the seam
  they will use.
- Two listener names and two network modes are a real complexity cost, carried
  deliberately because removing either broke a component that works.
- The idle-gap connectivity loss is **not diagnosed**. Anything measured across a
  pause on this host should be re-run rather than trusted.
