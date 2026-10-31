# Readiness, publication and recovery

Sections 5, 9 and 13 as implemented. The question this document answers is:
**what has to be true before anyone may read a build, and what happens when it
is not?**

## The six data states

Only state 6 may be served. Each is a distinct thing, and conflating any two is
how a wrong number gets published.

| | State | What it means | What it does **not** mean |
|---|---|---|---|
| 1 | raw-ACKed | ClickHouse accepted the INSERT | that raw matches the source |
| 2 | snapshot sealed | manifest + per-TP end offsets + a terminal P05 state | that `lag == 0` |
| 3 | raw reconciled | an independent verifier compared identities and typed fields | that `COUNT(*)` matched |
| 4 | canonical candidate | one row per identity, staged under its own `build_id` | that it is correct |
| 5 | bars reconciled | OHLC and counts match the independent oracle | that the MV is transactional |
| 6 | published | the catalog points at a READY manifest | that it can be changed |

## Sealing a cut

`seal_snapshot` requires all of:

- a source manifest whose **declared SHA matches a recomputation** (case F-14);
- a non-empty set of per-topic-partition bounds, each with a positive
  `topic_generation` (cases F-07, F-11);
- `consumer_frontier >= end_offset` for every partition — the sink has
  acknowledged up to the bound, not merely fetched to it (cases F-01, A-12);
- Kafka retention still below the cut, or completeness is unprovable (case F-02);
- a terminal P05 state (`DRAINED` or `STOPPED_AT_OFFSETS`);
- a seal reason of `durable_sink_frontier`.

`consumer_lag_zero`, `row_count_match`, `wall_clock` and `operator_judgement` are
rejected **by name**, with an error explaining that they do not establish what is
durable. An unknown reason gets a different error from a known-bad one, so a typo
and a bad idea are distinguishable.

Optionally pass the topology you believe exists: without it, a cut over 15 of 16
partitions looks complete, and the missing partition's events surface much later
as apparent source loss.

## Publication

The pointer lives outside ClickHouse — a single `SELECT` is not a gate — and
moves only when:

1. a lock is held (`O_CREAT | O_EXCL`), so two builders cannot interleave;
2. the caller named the build it expects to replace, so a slow builder cannot
   overwrite a newer READY build with an older one.

The write is a temp file in the same directory plus `os.replace`, which is atomic
on POSIX and Windows, followed by `fsync` on the file and the directory. A crash
halfway through leaves a stray temp file and the **previous** pointer intact.

Manifests are write-once. Re-publishing identical content after a lost
acknowledgement is idempotent — the volatile fields (`published_at_ns`, the host
fingerprint) are excluded from the immutability comparison but still covered by
the content digest, so a retry succeeds while any later edit to the file is
detected on read.

`proof_gaps()` blocks publication when the state is not READY, either verdict is
not PASS, the canonical count is not positive, the bar tick count disagrees with
the reference, or any provenance digest is empty. That is INV-10 and INV-13: a
manifest claiming READY while carrying a failed verdict is a release-blocking
incident, not a warning.

## When a build fails

| Event | Outcome | Why |
|---|---|---|
| Unknown ACK on the canonical INSERT | `DIRTY`, fresh attempt only | the rows may be durable; a resubmit would add a second set of aggregate states that nothing can subtract |
| Crash after canonical, before bars verified | `NOT_READY` | an INSERT acknowledgement is not proof the bars are complete |
| Late record found after READY | old build unchanged, new build created | changing a customer's output silently is worse than being out of date |
| Duplicate with the same semantics in raw | allowed; one canonical row | at-least-once is normal and must be classified |
| Conflicting semantic core | `NO_GO` + report | picking a winner forfeits truthfulness |
| TTL removed the source and no backup exists | `NOT_RECONSTRUCTABLE` | completeness cannot be proven from data that is gone |
| Background merge in flight | read through `*Merge` | merges are asynchronous; `OPTIMIZE FINAL` is not a correctness tool |

A `DIRTY` attempt is never continued. `rebuild_fresh` drops the attempt **and the
aggregate states derived from it** — a view does not follow its source's deletes,
so tidying up only the canonical rows would leave the rebuilt bars double
counted.

## Retention

TTL ships **disabled** and `evaluate_retention_gate` keeps it that way until all
of these hold (NFR-11):

- `raw_ttl_days >= max(recovery, audit, rebuild, late-arrival)` windows;
- a **successful restore drill** is on record — a config file cannot assert one,
  because a drill is an event that happened, not a setting;
- a backup exists and its age is known and within limit.

Two properties of ClickHouse TTL shape the surrounding code: it is tied to
merges, so it is **late** — rows past their deadline can still be present, and
"deleted by the deadline" is not an SLA; and it is evaluated on **event time**, so
a record arriving already older than the TTL can vanish at the next merge.
`would_expire_on_arrival` exists so that is refused up front rather than looking
like a successful insert that later evaporates.

Future-dated rows are the mirror image: representable, implausible, and they
would evade retention indefinitely. `EventTimeWindow` quarantines them, and the
window comes from the source contract — deriving it from the observed data would
let any corrupt timestamp define its own plausibility.

## Incident severity

| Class | Severity | Response |
|---|---|---|
| One identity, several payloads | P0 | freeze, `NO_GO` |
| Duplicate canonical row | P0 | `DIRTY`, rebuild clean |
| TTL deleted data before audit | P0 | stop publishing, restore or replay |
| MV target missing or partial | P0 | not READY, rebuild staging |
| Query OOM / limit hit | P1 | bounded error, back off |
| Too many parts, merge backlog | P1 | throttle batch/build/query |
| Restart during build | P1 | candidate stays not READY |
| Index or query regression | P2 | roll back, re-run the A/B |

**An operator never**: runs `OPTIMIZE FINAL` in a hot path, `ALTER DELETE`s a
data conflict, commits offsets on P05's behalf, edits a price, shortens TTL to
free disk without a backup, marks a build READY because the INSERT succeeded, or
attaches a volume figure to a tick bar.
