# Failure catalog

Failure classes this component recognises, what it does about each, and —
more usefully — what it still cannot detect. The last column is the honest
part: a catalog that lists only what we handle is a marketing document.

Grouped to match the what-if matrix in design section 9.

## A — Protocol selection, address, connect

| Condition | Behaviour | Residual risk |
|---|---|---|
| JSONL sent to the MDP1 port | `BAD_MAGIC`, stream closed, exit 4 | — |
| Port 9009 configured for protobuf input | Refused at startup, exit 2 | An operator can still point at any other wrong port that happens to speak MDP1 |
| Non-loopback endpoint | Refused unless `allow_non_loopback` | No TLS, no authentication: the opt-in trusts the operator |
| Peer accepts then stays silent | Prefix deadline, `FRAME_TIMEOUT` | — |
| Connection refused | Bounded exponential backoff with deterministic jitter, then give up | — |
| Protocol changed to a different magic | `BAD_MAGIC`, no auto-negotiation | — |
| DNS returning several addresses | Numeric-first resolution, each address tried once inside the connect budget | A pathological resolver can still consume the budget |

## B / C — Transport and framing

| Condition | Behaviour | Residual risk |
|---|---|---|
| Any `recv()` boundary, including 1 byte at a time | Identical classification trace | — |
| Many frames coalesced in one read | All parsed, order preserved, bounded per iteration | — |
| Length 0, > 4096, or `UINT32_MAX` | `INVALID_LENGTH` **before** any sizing | — |
| Truncated prefix / header / body | Classified distinctly; no partial event emitted | — |
| Slowloris body | Absolute body deadline, never renewed per byte | A sender just under the rate limit still holds a connection for the full budget |
| A second `MDP1` mid-stream | Read as a length, rejected | — |
| Corruption mid-stream | Stream closed; no resynchronisation | One bad frame costs the rest of that connection. Deliberate (ADR-011) |

## D / E — Decoding and semantics

| Condition | Behaviour | Residual risk |
|---|---|---|
| Malformed protobuf wire data | `PROTO_DECODE_ERROR`, stream closed, no state mutated | — |
| Message parses but is empty | Rejected by presence checks, not accepted as zeros | — |
| Any required field absent | `MISSING_*`, stream closed | — |
| Field present with value 0 | Accepted as present; judged by the business validator | — |
| Unknown fields | Counted and flagged; rejected only if configured | Meaning cannot be inferred, so forwarding is not lossless for a v2 consumer |
| Oversize or non-allowlisted symbol | `FIELD_TOO_LONG` / `SYMBOL_CHARSET`; never trimmed or case-folded | — |
| Wrong `source_schema_version` | `UNSUPPORTED_SCHEMA_VERSION` | — |
| Wrong `price_scale` | `SCALE_MISMATCH`; never rescaled | — |
| Crossed or non-positive quote | Rejected with a specific reason | — |
| `INT64_MIN` / `INT64_MAX` prices | Checked arithmetic; no UB, no wraparound | — |

## F — Sequence

| Condition | Behaviour | Residual risk |
|---|---|---|
| Gap | Flagged `GAP`+`DEGRADED`, event still forwarded | The *cause* is unknowable from here: producer, converter, injector or restart |
| Exact duplicate | Suppressed, counted | — |
| Conflicting duplicate | Refused, integrity alert, first value kept | — |
| Late arrival inside the window | Observed, not published | Published continuity stays holed, by design |
| Below the window | `TOO_OLD_UNVERIFIABLE`; never assumed to be a duplicate | Genuinely unknowable without a wider window |
| Restart at the baseline | Refused, rollback flag, continuity uncertain | **Does not self-heal**; needs a new dataset epoch (ADR-015) |
| Jump to `INT64_MAX` | O(1) work, saturating counters, state unchanged | — |
| Business-invalid event between valid ones | Does not become seen-valid, so the next event reports a gap across it | Correct, but a burst of invalid events looks like a burst of gaps |

## G — Time

| Condition | Behaviour | Residual risk |
|---|---|---|
| Replay of a year-old dataset | Not stale: compared against the feed watermark, never wall time | — |
| Quote older than the watermark | `EVENT_TIME_OLD`, not published as current | Threshold is per-deployment; a feed with legitimate out-of-order timestamps needs a different one |
| Ticks only, no quotes | `QUOTE_INACTIVE` after the threshold; ticks do not refresh it | — |
| Feed silent entirely | Timer runs independently of reads | — |
| Offline replay | Event-time staleness detected; **inactivity is not** | Receive-time has no meaning for a file (ADR-017) |
| Host suspend/resume | `CLOCK_MONOTONIC` does not count suspended time | Hibernation is invisible; `CLOCK_BOOTTIME` is out of MVP scope |

## H / I — Provenance and output

| Condition | Behaviour | Residual risk |
|---|---|---|
| No manifest | `SOURCE_UNVERIFIED` (exit 3), or processed as `INCOMPLETE` if not required | — |
| Manifest digest or size mismatch | `SOURCE_UNVERIFIED`, with the specific note | — |
| Manifest not `COMPLETED` | `SOURCE_UNVERIFIED` | — |
| `.part` input | Never a verified source | — |
| Slow sink | Reader pauses; everything still arrives | Throughput falls to the sink's rate |
| Stalled sink | `BACKPRESSURE_TIMEOUT`, pending reported, `.part` left behind | — |
| Unwritable sink | `SINK_IO_ERROR` (exit 8) | — |
| Live stream, everything arrived | `PARTIAL_LIVE` | Completeness is **unprovable** without source-side evidence |

## J / K — Resources and lifecycle

| Condition | Behaviour | Residual risk |
|---|---|---|
| Symbol-cardinality attack | New keys refused at the cap; active keys never evicted | A legitimate new symbol is refused once the cap is reached |
| State byte budget exceeded | `STATE_CAP_EXCEEDED`, event refused | — |
| SIGTERM | Drain within the deadline, then commit or `INCOMPLETE` | — |
| SIGKILL | No cleanup; `.part` and no marker | Correct evidence, but the in-memory cursor is gone |
| Process restart | `START_UNVERIFIED` for every key | No durable cursor exists (ADR-012) |
| OOM kill, cgroup limit | Not handled in-process | The kernel gives no chance to write a report |

## What this component cannot detect

Stated plainly, because every one of these is a way a report could look
clean while something is wrong:

1. **Events the source never sent.** A gap is only visible if the sequence
   numbering reveals it. A producer that silently skips a symbol, or stops
   early, looks identical to a quiet feed.
2. **Whether a gap was loss or intent.** `gap_detected` is not a TCP loss
   count and must not be reported as one.
3. **Live completeness.** Without an app-level ACK or a source-side
   manifest, "everything arrived" is unprovable. Hence `PARTIAL_LIVE`.
4. **Truth between two conflicting duplicates.** The first value is kept
   because something has to be; that is a tie-break, not a judgement.
5. **Wall-clock end-to-end age**, unless the producer and consumer clocks
   are known to be synchronised. Measured only as an optional metric,
   never as a rule.
6. **A compromised toolchain or schema.** Pinning the descriptor digest
   detects an accidental mismatch, not a deliberate substitution upstream
   of the pin.
7. **Anything downstream of the handoff.** A successful sink write says
   bytes reached an interface. Nothing about a broker, a topic, or a
   consumer.

This list is expected to grow from real incidents. Adding a row here when
one happens is part of closing it.
