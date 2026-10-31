# Handoff to Project 03 (C++ adapter) and Project 04 (Kafka)

Written for: engineers implementing Projects 03 and 04 against this contract.

What you are consuming, what you must not assume, and the parts that are easiest
to get wrong.

---

## 1. The contract

| | |
|---|---|
| Proto package | `marketdata.v1` |
| Schema | `schemas/marketdata/v1/market_data.proto` |
| Schema SHA-256 | `toolchain/versions.lock` → `proto_v1_sha256` |
| Descriptor baseline | `toolchain/descriptor-baselines/marketdata-v1.desc` |
| Descriptor SHA-256 | `toolchain/versions.lock` → `descriptor_v1_sha256` |
| Source JSONL format | `sim.v1` (Project 01), carried in `source_schema_version` |

Field numbers are frozen. 1–6 are envelope scalars, 10–11 are the payload oneof,
**10–19 is the payload range**, and 7–9 are held for future envelope scalars.

```proto
message MarketDataEvent {
  optional string dataset_id            = 1;
  optional string symbol                = 2;
  optional int64  sequence              = 3;
  optional int64  event_time_ns         = 4;   // UTC epoch nanoseconds
  optional int64  price_scale           = 5;
  optional string source_schema_version = 6;   // "sim.v1"
  oneof payload { Quote quote = 10; Tick tick = 11; }   // 10-19 reserved for payloads
}
message Quote { optional int64 bid_ticks = 1; optional int64 ask_ticks = 2; }
message Tick  { optional int64 price_ticks = 1; }
```

Verify before building against it:

```bash
python -m md_contract toolchain-check     # schema + descriptor hashes against the lock
```

---

## 2. Framing differs per channel. This is the one that bites.

| channel | payload |
|---|---|
| Project 01 TCP, port 9009 | UTF-8 canonical JSON + `\n` (unchanged) |
| Project 02 TCP, port 9010 | `MDP1` once, then repeated `[uint32 BE len][protobuf]` |
| `.pbstream` file | the same MDP1 format, plus a `.COMPLETED` marker |
| **Kafka record value (P04)** | **one raw `MarketDataEvent`. No `MDP1`. No length prefix.** |

A Kafka record already carries its own boundary, so putting the TCP framing
inside the value would corrupt it. A consumer that finds `MDP1` or a 4-byte
length at the start of a record value should **reject** the record as a
packaging violation rather than strip it — stripping it hides the producer's bug
(case L07). Project 04's record key is the `symbol`, and it must match the
`symbol` inside the decoded payload (case L08).

`MDP1` versions the *framing*, not the schema. An incompatible framing change
becomes `MDP2` on a different port, never a sniffed variant of this one.

---

## 3. What you must do, because this codec deliberately does not

**Project 03 owns all stream-level detection.** This codec validates and
preserves single events. It does not detect, and must not be assumed to detect:

- **sequence gaps** — Project 01 injects them on purpose and they are forwarded
  untouched. Nothing is inserted to fill one.
- **duplicates** — an intentional duplicate arrives twice with the same
  `(dataset_id, symbol, sequence)`. Deduplicate on that tuple, not on a byte
  hash: the identity is the typed triple, and byte hashes are not stable across
  runtimes.
- **out-of-order events** — order is preserved as received. Nothing is sorted.
- **staleness** — `event_time_ns` is never compared against a wall clock here,
  because a historical replay would then be rejected wholesale. Staleness is
  your policy, against a logical reference you choose (case L06).

**Presence is explicit, so check it.** `HasField` / `has_...`, never a
comparison against zero. An absent `sequence` and `sequence = 0` are different
events and both reach you distinguishable. An event that fails the presence
check must be rejected, not defaulted.

**Fail closed on an unknown payload.** If `WhichOneof("payload")` returns nothing
but an unknown field occupies tags 10–19, a newer writer sent a variant you do
not know. Report `UNSUPPORTED_EVENT_KIND`. Do **not** treat it as an event
without a payload, and never as a quote or a tick.

**Prices are integer ticks.** Display as `Decimal(ticks) / Decimal(price_scale)`,
never via `float`: values above 2^53 are not representable and the test corpus
contains them deliberately. `price_scale` can differ per symbol and between
events of the same symbol; carry it, never assume it.

**Signed means signed.** `sequence`, prices and timestamps are `int64` so that
Project 01's deliberate negative fault values survive. Do not widen to unsigned.

---

## 4. What this project does not give you

Stated plainly so none of it is assumed:

- **No delivery guarantee.** MDP1 has no application-level ACK. `frames_written`
  means bytes handed to the kernel. There is no `peer_accepted` counter, and one
  should not be added unless the protocol is built to justify it.
- **No integrity check.** Protobuf has no checksum. A flipped bit inside a price
  varint can produce a message that parses cleanly and carries a different price
  — demonstrated in this project's own test suite. If your path leaves a trusted
  environment you need authenticated transport; a checksum detects accidental
  corruption only and does not replace authentication.
- **No encryption and no authentication.** Loopback only by default.
- **No replay cursor and no exactly-once.** A reconnect is a new session.
- **No schema registry.** The descriptor hash in `versions.lock` is the contract
  identity for this MVP.

---

## 5. Version policy

Already verified in both directions, in separate processes:

| producer | consumer | result |
|---|---|---|
| Python v1 | C++ v1 | accepted; projections byte-identical |
| C++ v1 | Python v1 | accepted; projections byte-identical |
| v1.1 (adds `feed_label = 20`) | v1 | accepted; core projection unchanged, field 20 retained as unknown |
| v1 | v1.1 | accepted; `feed_label` absent, not fabricated |
| v2 probe (`depth_update = 12`) | v1 | **fails closed**: 0 accepted, all `UNSUPPORTED_EVENT_KIND` |

Before changing the schema, run the gate — `protoc` will not catch a breaking
change for you:

```bash
python scripts/compare_descriptor.py <candidate.desc>
```

Forbidden in v1: renumbering or reusing a tag, changing an integer's type
(including to `uint64`, which is wire-compatible and still forbidden), removing
a core field, changing presence semantics, moving existing fields into an
existing oneof, and routing binary through ProtoJSON where forwarding must be
lossless.

Rollout order for an additive change: deploy consumers that tolerate the new
optional field **first**, then the producer; watch the rejected and
unknown-field metrics; roll the producer back first if anything moves. A new
event type or a changed field meaning is not an additive change — it needs
`marketdata.v2` with its own migration.

---

## 6. Reading what this project produced

```
reports/test-report.json     the section 15 report, incl. all 170 case verdicts
reports/interop.json         the cross-language and version matrix
reports/benchmark.json       lanes and the load ladder, with trial spreads
reports/what-if-cases.json   case -> verdict -> the tests providing the evidence
```

Two things to read rather than skim:

- **Capacity is `NOT_ESTABLISHED` above 10,000 events/s** on the machine that
  produced these numbers. Do not plan on 120k from this report.
- **The release verdict is `NOT_EVALUATED`, not `GO`.** 42 of the 170 cases
  could not be run in this environment and are listed with reasons. Treat them
  as unknowns, not as passes.
