# Protocol handoff: P01 → P02 → P03 → P04

What this component may assume about its neighbours, and what it refuses to
assume. Everything here is a contract the adapter enforces at runtime, not a
description of code that already exists elsewhere.

## Upstream: Project 02

### Wire format (`MDP1`)

```
<"MDP1">                       exactly once, per file and per connection
[BE32 length][payload] ...     repeated; length in 1..4096
```

- `length` is big-endian and is validated **before** any buffer is sized.
- `payload` is a serialized `marketdata.v1.MarketDataEvent`, with no
  additional envelope.
- `MDP1` is a one-shot prefix, **not** a sync word. The adapter never scans
  for it to recover from corruption (ADR-11). A second `MDP1` mid-stream is
  read as a length header and rejected as `INVALID_LENGTH`, which is the
  correct and visible outcome.
- The JSONL port (9009, Project 01) and the protobuf port (9010) are
  distinct. The adapter rejects port 9009 in configuration and never sniffs
  the protocol on either.

### Schema

`schemas/market_data.proto` is a frozen copy of the Project 02 contract,
reproduced from the design document. It is **not** forked: when Project 02
ships its artifact, replace this file with it and re-pin the digest.

```bash
./md_adapter --print-descriptor   # paste into identity.expected_descriptor_sha256
```

The digest is of the serialized `FileDescriptorSet` as the *runtime* sees
it. Protobuf makes no canonical-bytes promise across runtimes, so this value
is a property of a build, not a universal constant — which is exactly why it
is pinned per deployment rather than hard-coded.

Every field is `optional`, so presence is explicit. The adapter relies on
that: a field that is absent and a field that is present with value `0` are
different, and treating them alike would silently accept a message that
never carried a sequence.

### Offline manifest

An offline replay may only be reported `COMPLETE_OFFLINE` when a sibling
manifest proves it. EOF proves nothing: a truncated file also ends.

`<artifact>.pbstream.manifest.json`:

```json
{
  "dataset_id": "ds-golden",
  "sha256": "<hex digest of the .pbstream bytes>",
  "status": "COMPLETED",
  "frames": 3000,
  "bytes": 184512,
  "schema_descriptor_sha256": "<optional>",
  "producer": "p02-encoder"
}
```

Checked at startup: the file exists and parses, `status == "COMPLETED"`,
`bytes` equals the real file size, and `sha256` equals the real digest. Any
mismatch is `SOURCE_UNVERIFIED` (exit 3) when
`input.require_completed_manifest` is set, and otherwise downgrades the
verdict to `INCOMPLETE` while still processing the data.

An input path ending in `.part` is never a verified source.

## Downstream: Project 04

The `p04_stub` sink records exactly what would be handed to a Kafka producer:

```json
{
  "key": "EURUSD",
  "value_hex": "0a0864732d...",
  "value_bytes": 46,
  "quality": {
    "flags": ["GAP", "DEGRADED"],
    "feed_state": "DEGRADED",
    "primary_reason": "SEQUENCE_GAP",
    "arrival_index": 17,
    "event_id": { "dataset_id": "...", "symbol": "...", "sequence": 4, ... }
  }
}
```

Three rules that the e2e matrix asserts (ADR-14):

1. `record.key` is the symbol.
2. `record.value` is the **untouched** `MarketDataEvent` wire bytes — no
   `MDP1` magic, no length prefix. Kafka's own record boundary replaces the
   framing.
3. Data quality travels on a **side channel** with its own shape. It is never
   mixed into `record.value`, because that would change the bytes a P02/P04
   consumer parses.

`fault_observe` mode may never feed this sink; the configuration validator
refuses the combination.

**What a successful write means here:** the event reached the handoff
interface. It does not mean a broker acknowledged it, and the report never
says otherwise.

## Quality signal consumers must read

An event forwarded with `DEGRADED` is real, newer data whose series is not
continuous. A consumer that ignores `quality_flags` and treats a gapped
series as clean has made that choice itself — the adapter made the gap
visible rather than silently dropping the feed (ADR-07).

Suppressed events never reach the sink at all: exact duplicates, conflicting
duplicates, late arrivals, unverifiable old events, and quotes older than the
accepted watermark. They are counted in `policy_suppressed_by_reason`, so a
consumer reconciling against a source manifest can see them.
