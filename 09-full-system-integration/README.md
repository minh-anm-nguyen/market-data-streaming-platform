# Project 09 — Full System Integration, Deployment & Release Engineering

The eight projects, assembled into one platform. This is where they stop being
eight things that each passed their own tests and start being one system: the
cross-project contract audit, the lock that pins it, the DEV deployment, the
init jobs and the end-to-end pipeline that runs `P01 → P07` in order.

Implements [`../docs/PROJECT_09_Full_System_Integration_and_Deployment_System_Design.md`](../docs/PROJECT_09_Full_System_Integration_and_Deployment_System_Design.md).
That document reasons about all fifteen milestones; this file covers what was
built, what was measured, and what is still unproven.

> **The real data path runs.** `P01 → P02 → P03 → P04 → Kafka` is joined and
> verified on real components, twice, with `verdict=PASS` and balanced
> conservation counters. `P05 → ClickHouse` ingests. What is **not** established
> is any performance number, HA, DR, or a completeness proof from P07 against the
> real stack. See [Status](#status).

---

## The one idea worth keeping

Each of the eight projects validated itself, and each one passed. None of them
could see the seam it shares with its neighbour. Everything P09 found, it found
at a seam:

* A value three projects treated as a shared identity — the protobuf descriptor
  digest — had three different meanings.
* P02 wrote a completion marker its own consumer could not read.
* P04 had never been connected to P03 at all; it classified events with a
  re-implementation of P03's policy.
* Kafka ran with no data volume, so no restart test could mean anything.
* ClickHouse reported `healthy` while its native port was dead.

Nobody was wrong locally. The system was wrong between the parts.

---

## Status

| | |
|---|---|
| M0 contract audit | **implemented** — 21 checks, **20 agree**, 1 outstanding |
| M0 verdict | **`M0_BLOCKED`** — 1 BLOCKER (was 3 BLOCKER / 3 HIGH / 1 MEDIUM) |
| M1 images & provenance | **mostly done** — pinned Python image, both infra images pinned **by digest**, `p09 preflight` G0 = **GO**; no SBOM, no signature |
| M2 DEV compose | **implemented** — healthy from cold in ~12 s, restart-persistent |
| M3 init jobs + validator | **implemented** — idempotent, `G2_PASS` against the lock |
| M4 P01→P02→P03 | **implemented and run** — 2000 events, `COMPLETE_OFFLINE` |
| M5 P04 → Kafka | **implemented and run** — real P03 upstream, `verdict=PASS` |
| M6 P05 → ClickHouse | **runs** — rows land; completeness still owed to P07 |
| M7 P06 canonical/bars | **joined** — `build-from-raw` reads P05's table (new) |
| M8 P07 against the real stack | **wired** — `live_kafka.py` + `live_clickhouse.py`; the scan classifier is unchanged, only the records are real. Pending a run that produces the verdict |
| M9 CI | **implemented** — contract gate, schema-sync gate, preflight gate, two mutation controls |
| M10 release record | **implemented** — `p09 release-manifest`, every gate at its measured value |
| M11/M12 lab profiles | **implemented** — `compose.labs.yaml`, reliability (RF=3/minISR=2) + capacity, isolated ports and volumes; **nothing measured** on this host |
| M13/M14 DR, soak, release rehearsal | **not started** |
| Tests | **69 P09** · **161 P04** · **586 P06** · **423 P07** · **876 P08** · **201 P02** — no regressions |
| Capacity, HA, DR | **`NOT_RUN`**, and out of scope for the DEV profile |

---

## What the audit found, and what was done about it

`make audit`, or `p09 audit --verbose` for the evidence behind each line.

### Resolved during this work

**P09-M0-03 — the descriptor digest had three meanings.** Three projects pinned
a "descriptor sha256"; no two matched. The first diagnosis was that they never
could, blaming protobuf's lack of a canonical serialisation. **That was wrong,
and building the C++ projects proved it wrong.** protobuf 3.12.4 (C++), 3.21.12
(C++) and 7.35.1 (Python) serialize *byte-identical* `FileDescriptorProto` for
this schema — P04's built value is exactly P05's. The pins differed by
representation only: the wrapper, the `name` field carrying the path `protoc` was
invoked with, and `json_name`, which `--descriptor_set_out` populates and a
runtime `CopyTo` omits.

So P09 now **derives** each project's expected pin from one canonical
descriptor, in that project's own encoding, and all four reconcile:

| project | encoding | digest |
|---|---|---|
| P02 | `FileDescriptorSet`, protoc path, `json_name` kept | `0e6fc2b2…` |
| P03 | `FileDescriptorSet`, runtime copy | `3363eeed…` |
| P04 | bare `FileDescriptorProto`, runtime | `e17bf37b…` |
| P05 | bare `FileDescriptorProto`, runtime | `e17bf37b…` |

Reasoning in [ADR-0001](docs/ADR.md).

**P09-M0-04 — P04 had no descriptor pin.** Set from a real build, so P04's own
case D-08 is now live.

**P09-M0-05 — P04 did not consume P03.** It read a P01 run directory through
`MockP03Classifier`. P04 now has a second upstream, `run.upstream =
"p03_handoff"`, reading the adapter's own `p04_stub` stream so the
classification, gap detection and suppression set are P03's. The reader
**re-encodes each decoded event and compares it to P03's original bytes**,
failing the run on any difference — byte preservation is verified, not assumed.
Reasoning in [ADR-0002](docs/ADR.md).

**P09-M0-20 — P06 did not consume P05 either.** The same defect, one stage
further down, and better hidden: P06's config names the raw table and
`schema_preflight` checks that table's *shape*, so it reads as joined. But every
`build` assembled its corpus in-process via `synthetic_corpus` or
`golden_six_event`, and `--clickhouse-url` only chose where the build *wrote*.
Checking a shape is not reading rows — P06 had never seen a row P05 produced,
and an audit of a corpus against itself cannot fail.

New: [`md_analytics/raw_source.py`](../06-clickhouse-analytics/src/md_analytics/raw_source.py)
and a `build-from-raw` subcommand. Its three inputs each come from the component
that owns them, and crucially **two of them come from upstream of the table
being audited**:

| input | source | why that source |
|---|---|---|
| raw rows | `md_ingest.market_events_raw` | P05 owns it; P06 only reads it |
| accepted-event ledger | P03's `p04_stub` NDJSON | `AcceptedEvent`'s own docstring says the expected set is P03's ledger, not everything P01 generated — P03 may legitimately suppress an event, and demanding it appear in raw would manufacture a missing-row report |
| snapshot cut | P05's `snapshot` output | without a bound the source is unbounded and completeness is unprovable |

If the expected set were recomputed from the raw rows, every row would agree with
itself and a missing row would be invisible. That is the whole reason the inputs
are split this way.

**P09-M0-21 — `topic_generation` had two types.** P05 declares
`LowCardinality(String)` and stores a tag like `md-v1-generation-a`; P06
annotated the same field as `int`. It is part of a Kafka coordinate precisely so
a deleted-and-recreated topic cannot make two records share
`(topic, partition, offset)`, so coercing that tag to an integer would fabricate
the identity the field exists to protect. P06's annotation now admits the string;
nothing coerces.

### Still open

| id | severity | finding |
|---|---|---|
| P09-M0-06 | BLOCKER | P07 still runs `UNIT/MODEL` and simulates P03–P06, so G4 proves nothing about this deployment |
| P09-M0-19 | BLOCKER | P02's `.pbstream.COMPLETED` does not satisfy P03's `.pbstream.manifest.json` contract — different name *and* schema. The pipeline carries a **named shim**, not a fix |
| P09-M0-02 | HIGH | four forked copies of the schema; they agree, and now CI is what keeps them agreeing |
| P09-M0-07 | HIGH | three protobuf installs build one contract. Measured: this does **not** corrupt the schema; the risk is reproducibility, and nothing asserted the matrix until M9 |
| P09-M0-08 | MEDIUM | P08 carries a topic-generation tag that is not the issued one |

---

## What runs

```sh
make test            # 61 tests, no infrastructure
make audit           # the M0 audit
make lock            # regenerate contracts/contracts.lock.yaml + reports/audit.json
make verify          # check the tree against the lock
make render-init     # generate compose/init/* from the lock
make render-pipeline # generate the end-to-end runner from the lock
```

Where the Docker daemon is:

```sh
make images          # the pinned Python service image
make up              # DEV stack, wait for both healthchecks
make infra           # init jobs, then judge them against the lock
make pipeline        # the whole platform once, P01 through P07
make down
```

The C++ stages need their own builds first: `03-*/scripts/build.sh release` and
`04-*/` via its Makefile or CMake.

Exit codes are the interface: `0` clean, `1` findings outstanding, `2` refused,
`3` lock drift. A non-zero code never means the tool broke — an internal failure
raises and shows a traceback, because a crash reported as a clean FAIL is
indistinguishable from a real finding.

### Measured end to end

WSL2 Ubuntu-22.04, Docker 29.1.3, 8 CPU, 15 GiB. Two independent runs.

| stage | result |
|---|---|
| infra cold start | both healthy in ~12 s |
| init topics | `md.ticks.v1`, `md.quotes.v1` at 8 partitions, **created then described to decide** |
| init DDL | 5 files applied **by path** from P05 and P06 — nothing copied |
| idempotency | re-running both init jobs is a no-op, exit 0 |
| `verify-infra` | **`G2_PASS`** — 8 partitions, `cleanup.policy=delete`, 18/10/10 columns |
| restart persistence | **PASS** — Kafka offsets 4→4, CH rows 1→1, cluster id unchanged |
| P01 record + verify | 2000 events, `COMPLETED`, verdict `COMPLETE` |
| P02 encode + verify | 2000 → 2000 decoded, **0 semantic mismatches** |
| P03 adapt | `COMPLETE_OFFLINE`, 2000 frames, 2000 business-ok, 0 suppressed, `VERIFIED_COMPLETE_FILE`, descriptor pinned and matching |
| P04 publish | **`verdict=PASS`**, `mock_upstream_p03: false`, 2000 accepted, 1997 broker-acked, 0 definite failures, 0 unknown, 0 pending, conservation `ok` |
| P05 ingest | preflight 7/7 PASS incl. the descriptor pin; rows land in `md_ingest.market_events_raw` |

P05's terminal verdict was **`INCOMPLETE`**, and it named the reason rather than
reporting a smaller dataset:

```text
RUN VERDICT: INCOMPLETE
  reason: blocked partitions: md.ticks.v1[3] (KEY_NULL: fail-closed policy stops
          partition md.ticks.v1[3] at offset 0), md.ticks.v1[5] (KEY_NULL: ...)
  blocked: md.ticks.v1[3], md.ticks.v1[5]
```

The cause was **this project's own test pollution**. The restart-persistence
check earlier wrote four probe records with `kafka-console-producer.sh
--property parse.key=false`, i.e. with null keys, into `md.ticks.v1`. P05's
poison policy is fail-closed per partition, so it stopped exactly the two
partitions those records landed in, at offset 0, and refused to proceed past
them. That is the correct behaviour and the reason the tick side shows 201
unique identities rather than the full set: two of eight tick partitions were
never consumed.

Nothing was lost, and nothing was silently skipped — which is the whole point.
A pipeline that had "recovered" by stepping over a record it could not key would
have produced a clean-looking row count and a corrupt dataset.

Two consequences worth keeping. **A lab topic that has been hand-poked is not a
test fixture**: the pipeline should create a fresh topic generation per run
rather than reusing one an operator has produced into, which is finding
P09-M0-08 arriving from the other direction. And a row count is still not a
completeness claim: `--max-idle-steps` is explicitly not an end-of-dataset
detector, so only P07's reconciler against a frozen cut can close that question.
Completeness stays `NOT_RUN`.

The duplication — 12,164 physical rows over 1,811 unique identities — is
at-least-once working as designed: repeated runs plus one killed run that never
committed its offsets, re-read from the seeded low watermark. P05 documents
exactly this, the raw table permits it, and logical identity is what the query
layer dedupes on.

Capacity was not measured and cannot be from this profile. One broker at RF=1 is
not durability, one node is not HA, and the `120k ticks/s` / `280 ms p95` figures
belong to CAPACITY-LAB with P08 holding the ruler.

---

## Defects found by deploying, not by reading

**Kafka ran with no data volume.** `04-*/deploy/docker-compose.local.yml` starts
the broker with no volume, so every `down` discarded the log and no retention or
restart test could mean anything. P09 gives it `p09-dev-kafka-data` and pins the
cluster id, so a restart against that volume is a restart rather than a new
cluster wearing the old data.

**ClickHouse reported `healthy` while its native port was dead.** This kernel has
IPv6 disabled; the image declares `listen_host ::`, every `[::]` bind fails with
*"Address family for hostname not supported"*, and ClickHouse logs that at
**Warning** and carries on with some listeners bound and some not. The image's
healthcheck probes HTTP `/ping` only, so Compose called the container ready while
`clickhouse-client` on 9000 refused every connection — breaking P06's Makefile
targets and this project's own DDL job.
[`compose/clickhouse/zz-p09-listen.xml`](compose/clickhouse/zz-p09-listen.xml)
pins the listeners to IPv4 and the healthcheck now probes **both** protocols. A
healthcheck that cannot observe a failure mode is not a check — P09 section 2's
argument about `service_healthy`, arrived at the hard way.

**Renaming Kafka's host listener broke P04.** Named `HOST` rather than
`PLAINTEXT`, P04's librdkafka 2.6.1 could not complete an ApiVersions exchange
against a broker that served a 2.16.1 client fine. The name is back to the shape
P04's own lab proved, with a note not to tidy it again.

---

## The pattern behind every defect assembly found

Three bugs, all the same shape: **a component's self-test passed because the
thing it tested was not the real thing.**

| component | what it checked itself against | what that hid |
|---|---|---|
| P04 | `MockP03Classifier`, its own re-implementation of P03's policy | the adapter was never in the path (P09-M0-05) |
| P06 | `synthetic_corpus`, a corpus it generated in-process | it had never read a row P05 wrote (P09-M0-20) |
| P06 | a model backend that does not parse SQL | its DDL splitter produced a non-statement (below) |

The third is worth spelling out, because it is the sharpest. P06's
`_split_statements` split on `;` **before** stripping `--` comments. One of its
own DDL files contains a comment with a semicolon in it:

```sql
-- 006_audit_queries.sql; this DDL only makes the proof cheap to run.
```

Splitting first cut that comment in half, dropped the leading fragment because
it still began with `--`, and sent the trailing prose to the server:

```
Code: 62. DB::Exception: Syntax error: failed at position 1 (this)
```

Its 584 tests could not catch it: the model backend never parses SQL, and the 22
integration tests that would have are all skipped for want of a server. The
function's own docstring even asserted "no string literals containing
semicolons" — true, and it overlooked comments. Fixed by stripping comments
first, with two regression tests: one for that exact shape, one that splits
every shipped DDL file and rejects any fragment that is not a statement.

A self-test is only evidence about the thing it ran against. That is the whole
argument for an integration layer, and it is why these were found by assembling
the system rather than by reading it.

---

## This host is not a measurement environment

Worth stating plainly, because it invalidates any number taken casually here.

The Docker daemon P09 uses is **shared with an unrelated stack**: six
`broker-risk:py312` containers crash-looping with 13–26 restarts each, plus an
exited `br-kafka` (Kafka **3.9.0**) and `mdp-clickhouse` (ClickHouse **24.8**)
that previously claimed **the same published ports, 9092 and 8123**.

That is the cause of the connectivity loss that cost most of the debugging time
here: Kafka and ClickHouse's native port intermittently refused connections on
containers reporting zero restarts, recovering on restart. It read like an idle
timeout; it is port contention on a shared daemon.

The pipeline is built to survive it — one continuous pass, retries on the broker
probe, HTTP rather than ClickHouse's native port — but surviving is not
measuring. **Anything timed on this host should be re-run on an isolated one.**
P09 section 2.2's failure-domain note is exactly about this, and it turns out to
apply to the lab itself.

---

## Layout

```text
contracts/contracts.lock.yaml   generated. The cross-project contract, pinned.
compose/compose.dev.yaml        DEV profile: Kafka KRaft + ClickHouse, named volumes
compose/clickhouse/             the IPv4 listener override, and why it exists
compose/init/                   generated init jobs + the offset-seeding tool
compose/pipeline/               generated end-to-end runner + STAGES.md
images/python-tools.Dockerfile  one pinned image for all six Python projects
docs/ADR.md                     the three decisions that shaped the assembly
reports/                        audit.json, infra.json, pipeline.log, persistence.log
src/p09_integration/
  protoparse.py   reduce a .proto to its contract and hash that
  descriptors.py  derive each project's descriptor pin from one schema
  probes.py       read each project's declared facts, with provenance
  audit.py        compare them; findings carry evidence and a gate
  lockfile.py     build, write and drift-check contracts.lock.yaml
  deploy.py       render the init jobs from the lock; judge the result
  pipeline.py     render the end-to-end runner from the lock
  yamlsub.py      a restricted YAML reader/writer, so the lock needs no dependency
  cli.py          the `p09` command line
tests/            61 tests, mostly negative controls
../.github/workflows/p09-contracts.yml   the M9 contract gate
```

### Changes made inside other projects

P09 is mostly additive, but assembling the system required three edits elsewhere,
each recorded in an ADR:

* **P04** — `src/source/p03_source.{h,cpp}` (the real P03 upstream, 15 new
  tests), `run.upstream` / `run.p03_handoff_path` config with validation, and
  `mock_upstream_p03` computed rather than hard-coded.
* **P04** — `config/local-dev.example.toml` now pins `proto_descriptor_sha256`.
* **P06** — `src/md_analytics/raw_source.py` and a `build-from-raw` subcommand
  (the real P05 upstream, 30 new tests), plus `TopicPartition.topic_generation`
  widened to admit the string tag P05 stores.
* Nothing in P01, P02, P03, P05, P07 or P08 was modified. P02 **should** be
  changed to emit the manifest P03 specified (finding P09-M0-19); the pipeline
  shims it in the meantime rather than editing P02 silently.

### Why two things are reimplemented rather than imported

`yamlsub.py` duplicates P07's `yamlite.py`, and `protoparse.py` could have used a
runtime descriptor instead of parsing text. Both are deliberate: P09 audits P07,
and an auditor sharing a parser with its subject cannot find a parser bug; and
the runtime descriptors are the thing under audit, so deriving the comparison
from one would reproduce the divergence being measured.

---

## On the tests

61 P09 tests and 15 new P04 tests, most of them feeding deliberately wrong input.
P09 FR-13 asks for negative controls because a suite that cannot fail is not
evidence: the lock tests tamper with five cross-project identities, the parser
tests mutate a schema six ways and must reject all eight of P02's breaking
variants (including `int64 → uint64`, which `protoc` passes), the infra tests
feed wrong partition counts and a compacted topic, and the P03 reader tests feed
framed payloads, key/symbol mismatches and an unknown reason.

That last one is not hypothetical. P03 prints `Reason::kNone` as lowercase
`"none"`, alone among its reasons, and the first real run **failed closed** on
it rather than publishing unclassified events. The test now pins that behaviour.

The suites are mutation-checked, and [the CI workflow re-checks it on every
push](../.github/workflows/p09-contracts.yml): it breaks the verdict rule, then
the schema normal form, and fails if the tests stay green.

---

## What this does not establish

* **No completeness proof.** Rows land, but P07's oracle has not read the real
  Kafka cut and the real tables. Until it does, G3 is `NOT_RUN` and the row
  counts above are observations, not claims.
* **Nothing about P06 in the pipeline.** Canonical build and bars are not run
  end to end yet.
* **Nothing about recovery.** G4 needs faults that actually fired, observed by an
  independent oracle. P07 models both.
* **Nothing about capacity, HA or DR.** One host, one broker, one replica — and
  see the warning about this host above.
* **No provenance.** Images are pinned by tag, not digest; no SBOM, no release
  manifest. G0 cannot pass on tags.

## Next, in order

1. **P02 emits `.pbstream.manifest.json`** (P09-M0-19), and the pipeline's shim
   is deleted rather than kept.
2. **Point P07 at the real stack** (P09-M0-06) — its oracle is already
   independent of the data plane, so this is wiring, and it is what finally makes
   G3 and G4 answerable.
3. **Add the P06 stage** to the pipeline, so bars are built and verified from the
   canonical snapshot.
4. **M1 properly**: per-service images pinned by digest, with an SBOM and a
   release manifest, so G0 can pass.
5. **Move the lab off this Docker daemon** before measuring anything.
