# Runbooks

Operational procedures for this implementation. Each starts from an observable
symptom. Every one of them ends in either a proof or an explicit refusal — none
ends in "probably fine".

Exit codes from `md-analytics`: `0` PASS, `1` FAIL, `3` NO_GO, `4` INCONCLUSIVE.

---

## 1. Publish a snapshot

```bash
make build                       # synthetic corpus
make build-golden                # the section 16.1 corpus
```

The pipeline runs audit → stage → verify → publish and prints the whole report.
Check, in order:

1. `audit.verdict` is `PASS` — `missing`, `extra`, `conflicts` all zero.
2. `bars.verdict` is `PASS` — `bar_reference_mismatch_count` is zero.
3. `invariants` — any `FAIL` blocks; `INCONCLUSIVE` entries name what was not
   evaluated (retention and query bounds are not evaluated by a plain build).
4. `pointer.pointer_version` incremented.

If `state` is `NO_GO`, read `reasons`. The pipeline has already declined; there
is nothing to undo.

---

## 2. A build came back NO_GO with conflicts

**Symptom:** `raw_conflict_id_count > 0`.

One identity carries more than one semantic core. This is a real disagreement
about what the market did, upstream of P06.

```bash
# On a server: which identities, and how many variants each.
clickhouse-client --query "$(sed -n '/-- name: raw_identity_conflicts/,/LIMIT/p' sql/006_audit_queries.sql)" \
  --param_dataset_id=DEV-01 --param_row_limit=100
```

Do **not**: pick the newest row, pick the highest price, or delete one of them.
Each of those publishes a number nobody can defend.

Do: take the conflicting identities back to P03/P05 with both digests, establish
which is correct at the source, re-cut, and rebuild. Until then the previous
READY build keeps serving, which is the correct state.

---

## 3. A build came back NO_GO with missing or extra rows

**`missing`** — an identity in the accepted ledger is absent from raw. Check
whether P05 committed an offset past a failed sink batch (case F-05), whether
Kafka retention dropped it (case F-02), and whether the consumer group was reset.

**`extra`** — a raw identity is not in the ledger. Check whether the P03 ledger
lost a row that Kafka does hold (case F-08), and whether the row is actually past
the cut, in which case the audit reports it as out of scope rather than extra.

Both block publication. Neither is repaired by changing P06.

---

## 4. The canonical INSERT ended with an unknown acknowledgement

**Symptom:** `state: DIRTY`, reason mentions `ACK_UNKNOWN_BUILD_DIRTY`.

The rows may be durable. **Do not retry into the same attempt** — the
materialized view cannot subtract an aggregate state, so a resubmit doubles
every `tick_count` in the affected minutes, permanently.

```python
from md_analytics.canonical_builder import compute_build_id, staging_build_id, rebuild_fresh

# Drop the dirty attempt, including the bars derived from it, then stage attempt 2.
rebuild_fresh(store, rows, audit=audit, cut=cut,
              schema_sha256=schema, dirty_attempt=1)
```

Verify afterwards that `bar_tick_count` equals the canonical row count. If it is
double, the attempt was appended to rather than replaced.

---

## 5. Bars disagree with the oracle

**Symptom:** `bar_reference_mismatch_count > 0`, or missing/extra bars.

Read the `samples.mismatches` block: it names the bar key, the field, the
expected value and the actual one.

| Pattern | Likely cause |
|---|---|
| `open_ticks` and `close_ticks` swapped, count correct | the ordering tuple lost `sequence` or was downcast (cases I-08, Q-07) |
| `tick_count` exactly doubled | canonical rows inserted twice (case I-07) |
| Bars missing from the tail | the view was attached or dropped mid-build (cases G-05, K-02) |
| Bars missing throughout | the view's target was unavailable during the insert (case K-06) |
| `high_ticks` too high | a quote ask reached a tick bar (case H-03) |

A mismatch is never resolved by adjusting the oracle.

---

## 6. Part pressure is climbing

**Symptom:** `active_parts_per_partition` rising in steady state.

```bash
clickhouse-client --query "$(sed -n '/-- name: active_parts_by_partition/,/LIMIT/p' sql/006_audit_queries.sql)" \
  --param_row_limit=100
```

In order: increase the insert batch size, check whether a backfill is competing
with ingestion, compare daily against monthly partitioning on the same row set.

Do **not** put `OPTIMIZE TABLE ... FINAL` in cron. It is heavy, it competes with
the ingestion you are trying to protect, and it cannot repair an aggregate state
that was wrong when written (ADR-010). `make e2e-clickhouse` includes a test that
asserts no read or build path contains it.

---

## 7. Enabling TTL for the first time

```bash
make retention-gate     # exits 3 until every condition holds
```

It will refuse until:

1. `raw_ttl_days_candidate >= max(recovery, audit, rebuild, late-arrival)`;
2. a restore drill has actually run — restore a backup into a clean environment
   and compare identity sets and integer OHLC against the source oracle, not
   "the backup file exists";
3. a backup exists whose age is known and within `backup_max_age_days`.

Then set `[retention].enabled = true` and add the TTL clause. Remember that TTL
is evaluated on event time and applied by merges: it is late, and a late replay
older than the window will be refused by `would_expire_on_arrival` rather than
inserted and silently dropped.

---

## 8. Rolling back a bad release

The previous READY build is the rollback. A failed new build never moves the
pointer, so:

```bash
python -c "
from md_analytics.build_catalog import BuildCatalog
c = BuildCatalog('.artifacts/catalog')
print('active:', c.active_build_id())
print('known:', c.list_builds())
"
```

If a published build turns out to be wrong, revoke it rather than deleting it —
the manifest is the only record of what was served:

```python
catalog.revoke(build_id, reason="late correction found in minute 12:00")
```

Revocation clears the pointer and marks the manifest, keeping the audit trail.

---

## 9. Running the ClickHouse-backed tests

```bash
make dev-up                                  # pinned 25.3.3.42
make e2e-clickhouse                          # 22 integration tests
P06_CLICKHOUSE_URL=http://127.0.0.1:8123 python -m pytest tests -q   # everything
make dev-down
```

If you change the image tag, re-run these and record the new version in the
benchmark report. The behaviour this project depends on — insert de-duplication,
dependent views, async insert defaults — has changed between releases, which is
why the tag is pinned and `latest` is not used (case AA-06).

---

## 10. Producing a release report

```bash
python scripts/export_evidence.py --out .artifacts/evidence
# with a server:
python scripts/export_evidence.py --out .artifacts/evidence --clickhouse-url http://127.0.0.1:8123
```

Writes per-suite logs and a `summary.json` carrying the release verdict, the
coverage figures and the provenance digests. It reports `INCONCLUSIVE` while any
ClickHouse-semantics or scale case is unrun, because a gate that reads silence as
success is worse than no gate.
