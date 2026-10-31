# Runbook

Operating the adapter: what each failure means, what to check, and what not
to do about it.

## Reading a run

The report is the primary evidence. Start with four fields:

```yaml
status:            COMPLETE_OFFLINE | PARTIAL_LIVE | INCOMPLETE | FAILED
terminal_reason:   none | <error code>
source_completeness: VERIFIED_COMPLETE_FILE | VERIFIED_PARTIAL_FILE | LIVE_UNVERIFIED
all_conservation_equations_hold: true | false
```

Then re-check it independently:

```bash
python3 tools/verify_report.py reports/offline.yaml
```

That tool re-derives the conservation equations from the counters and
refuses several claims the adapter is not entitled to make, so a reviewer
does not have to trust the binary's own verdict.

### What the statuses mean

| Status | Meaning |
|---|---|
| `COMPLETE_OFFLINE` | A verified source was consumed to a clean frame boundary and the artifact committed. The strongest claim available. |
| `PARTIAL_LIVE` | A live session ran and ended without error. Completeness is **not** claimed: there is no app-level ACK and no source manifest. |
| `INCOMPLETE` | Processing happened but the run did not finish, or completeness could not be established. Never treat as success. |
| `FAILED` | The stream or the sink failed. `terminal_reason` says which. |

`all_conservation_equations_hold: false` is always a defect in the adapter,
not in the feed. Capture the report and the input and file it.

## Failure triage

### `BAD_MAGIC`

The peer is not speaking MDP1. Almost always one of:

- pointed at port 9009 (Project 01 JSONL) instead of 9010;
- a Project 02 bridge sending raw protobuf with no `MDP1` prefix;
- a second `MDP1` mid-stream, from a producer restarting its framing without
  closing the connection.

Do **not** add protocol detection. Fix the endpoint or the producer.

### `INVALID_LENGTH`

A length outside `1..4096` was read. Either the producer is framing
incorrectly or the stream is out of sync — commonly because two streams were
concatenated. Reconnecting gives a clean frame boundary; the adapter does
not resynchronise within a connection, by design.

### `TRUNCATED_PREFIX` / `TRUNCATED_HEADER` / `TRUNCATED_BODY`

The source ended mid-frame. Offline: the file is incomplete, whatever its
manifest says — check that the producer finished. Live: the peer closed
mid-write. No partial event was emitted.

### `FRAME_TIMEOUT`

A partial frame did not complete inside its budget. The stage deadlines are
absolute and are not renewed per byte, so a sender trickling one byte at a
time trips this deliberately. Check producer health before raising
`input.body_deadline_ms`. An idle feed sitting at a frame boundary does
**not** trip this — only a partial frame does.

### `PROTO_DECODE_ERROR`

Wire-level corruption. Check the protobuf runtime and descriptor digests on
both sides; a version skew between producer and consumer shows up here.

### `MISSING_*` / `FIELD_TOO_LONG` / `SYMBOL_CHARSET` / `UNSUPPORTED_SCHEMA_VERSION`

The message parsed but is not something this build can interpret, so the
stream is closed (MVP fail-closed policy). These indicate a producer or
schema problem, not a transport problem.

### `SCALE_MISMATCH` and other business rejects

One event is dropped with a reason; the stream continues. A burst of
`SCALE_MISMATCH` usually means a symbol's `price_scale` changed upstream
without `[prices.<SYMBOL>]` being updated. **Do not** "fix" it by changing
the configured scale to match the feed unless you have confirmed which side
is right — silently rescaling every price is worse than rejecting.

### `SEQUENCE_GAP`

Logical slots were never observed. This is **not** a TCP loss count: the
cause may be the producer, a converter, a fault injector, or a restart. The
event is still forwarded, flagged `DEGRADED`. Reconcile
`missing_slots_observed` against a source-side manifest before attributing
the loss to anything.

### `DUPLICATE_CONFLICT`

The same `(dataset, symbol, sequence)` arrived with different content. This
is an integrity problem upstream, not a transport artefact. The first value
observed is kept and the event is refused. Escalate: something is producing
two different truths for one logical event.

### `SEQUENCE_ROLLBACK`

A sender restarted at the baseline inside one `dataset_id`. The adapter
refuses the event and marks continuity uncertain. **The stream will not
recover on its own** — subsequent low sequences classify as duplicates or
unverifiable. The fix is an explicit new dataset epoch: a new `dataset_id`
from the producer, and a restart of the adapter. This is fail-closed by
design; see README "Known limitations".

### `EVENT_TIME_OLD`

A quote older than the accepted watermark for its key; it is not published
as a current quote. Note this is measured against the feed's own watermark,
never against wall-clock time — replaying a year-old dataset does not
trigger it. If it fires on a live feed, the producer's clock or ordering is
suspect.

### `QUOTE_INACTIVE`

No valid **quote** for a key inside the inactivity threshold. Ticks do not
refresh this timer: a feed can be busy with ticks and still have stopped
quoting. `DEGRADED` is sticky and does not clear on a timer; a feed returns
to `HEALTHY` only after recovery, and a prior quality problem stays visible.

### `BACKPRESSURE_TIMEOUT` (exit 6)

The sink did not drain inside `output.output_stall_deadline_ms`. Nothing was
discarded; `output_failed_or_pending` says how much did not land, and the
artifact is left as `.part`. Investigate the sink first — raising the queue
caps hides the symptom and increases the memory the process holds.

A *uniformly slow* sink does not trigger this: the reader pauses, the run
takes longer, and everything still arrives. `read_pause_events` and
`read_paused_ns` show how much time went to backpressure.

### `STATE_CAP_EXCEEDED`

Per-key state hit `identity.max_state_bytes`, `max_symbols` or
`max_datasets`. An active key is **never** evicted to make room, so new keys
are refused instead. One key costs roughly `sequence_window × 48` bytes
(≈196 KiB at the default 4096), so a 64 MiB budget holds a few hundred keys
— the per-symbol bitmap figure alone understates this by orders of
magnitude. Either raise the budget deliberately or reduce
`sequence_window`.

### `SOURCE_UNVERIFIED` (exit 3)

No manifest, a manifest that is not `COMPLETED`, or a digest/size mismatch.
`provenance_notes` in the report says exactly which. Do not work around it
by setting `require_completed_manifest = false` and calling the result
complete — that flag lets the data be *processed*, and the verdict stays
`INCOMPLETE` precisely so the distinction survives.

### `SINK_IO_ERROR` (exit 8)

The artifact path is unwritable, or a write or fsync failed mid-run. Check
disk space and permissions. The `.part` file is left in place on purpose.

## Shutdown

`SIGTERM` stops accepting input, drains the writer up to
`output.shutdown_drain_deadline_ms`, commits if the drain completed, and
otherwise reports `INCOMPLETE`. `SIGKILL` runs no cleanup at all: expect a
`.part` file and no completion marker, which is the correct evidence that
the run did not finish.

## Changing configuration

The validator refuses anything it cannot honour: unknown keys, floats, zero
deadlines, inverted watermarks, a queue too small for one frame, a timer
coarser than the inactivity threshold, a non-loopback host without an
explicit opt-in. A rejected configuration is a startup failure (exit 2) and
lists every problem at once — fix them together.

After changing the Project 02 schema, re-pin the digest:

```bash
./md_adapter --print-descriptor
# update identity.expected_descriptor_sha256
```

An `UNPINNED` digest runs, but prints a warning and records
`descriptor_pinned: false` in every report.
