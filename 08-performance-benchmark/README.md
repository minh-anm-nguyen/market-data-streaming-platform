# P08 — High-Throughput Benchmark & Optimization

Implementation of [`docs/PROJECT_08_High_Throughput_Benchmark_and_Optimization_System_Design.md`](../docs/PROJECT_08_High_Throughput_Benchmark_and_Optimization_System_Design.md).

> **Status.** The measurement system is built and tested: metrology kernel,
> open-loop drivers, independent oracle, capacity gates, A/B analyzer,
> three-dimension verdicts, evidence store, report and release gate.
>
> **No pipeline performance has been measured.** P02–P07 are design documents
> with no code, so every lane that needs the C++ adapter, Kafka or ClickHouse
> reports `BLOCKED_BY_UPSTREAM_IMPLEMENTATION`. All 512 what-if cases and all 48
> end-to-end tests in the catalog are `NOT_RUN`. Nothing here claims 120k
> ticks/s or a 280 ms p95.

## What this is for

The design document asks one question: at what offered rate does the pipeline
*simultaneously* process the data correctly, hold its backlog steady, stay
inside its resource budget and meet the query SLO? Answering it needs a
measuring apparatus that is itself trustworthy, and section 8 is explicit that
the apparatus must be validated against known inputs **before** it is pointed at
a real system.

That apparatus is what exists here. It is a ruler that has been checked against
known lengths, not a measurement of the building.

## Quick start

```bash
make p08-selftest    # validate the metrology kernel against the golden fixtures
make p08-test        # 876 tests
make p08-catalog     # the 512 + 48 case inventory, all NOT_RUN
make p08-gate        # release coverage gate: NO_GO, 0/242 P0 cases satisfied
```

`p08-selftest` is the one to run first on a new machine. It executes FIX-002
(exact nearest-rank percentiles and censored timeouts), FIX-003 (the queue
arithmetic), and the FIX-004 preconditions and histogram-resolution check, and
it prints what this host cannot evidence at all — on a non-Linux machine, cgroup
throttling, OOM events and `perf` counters are unavailable and the output says
so rather than leaving those report fields quietly null.

Running a lane:

```bash
# Against the real sealed P01 dataset. The source stage is measured; everything
# downstream is absent, so the lane is blocked and says which project it needs.
make p08-bench SUT=p01 SOURCE=../01-market-data-simulator/runs/smoke-rec-1

# Against the deterministic simulated pipeline. The whole orchestrator runs and
# the verdict refuses to read any of it as a capacity or latency claim.
make p08-bench SUT=mock SOURCE=../01-market-data-simulator/runs/smoke-rec-1
make p08-verify OUT=reports/runs/P08-LOCAL-SMALL
```

## Layout

```
specs/          generated: test-catalog.csv (from the design doc), 3 JSON schemas (from the code)
configs/        four workload profiles: local_small, lab_capacity, workload_120k, query_200m
scripts/        the two generators; run `make p08-specs` after changing either source
src/p08_bench/
  units.py          unit-tagged quantities: ticks/s is not events/s (INV-12)
  clock.py          clock domains, monotonic timers, cross-host sync uncertainty (INV-15)
  percentiles.py    exact nearest-rank, censoring, and the arithmetic P08 refuses
  histogram.py      bucketed estimates that judge their own fitness for a threshold
  samples.py        the attempt registry: every scheduled request, including failures
  metrics.py        the nine stage counters, cardinality control
  timeseries.py     backlog slopes and what counts as stable (FIX-003)
  manifest.py       seals, digests, run identity, drift classification (group G01)
  config.py         strict workload parsing, and what a lane may not claim
  catalog.py        the 560 cases and the release coverage gate
  scheduler.py      open-loop arrival schedules (ADR-01)
  event_driver.py   the driver, and its accounting of its own fidelity
  query_driver.py   query schedule, client RTT, result oracle (INV-13)
  calibrator.py     null-sink calibration: is the instrument the bottleneck?
  sut.py            the SUT port and the blocked-stage refusal
  sut_mock.py       deterministic simulated pipeline, for testing the tools
  sut_p01.py        the real P01 source; everything downstream NOT_IMPLEMENTED
  reconciliation.py the independent identity + semantic oracle
  collectors.py     host/cgroup probes that report unknown rather than zero
  evidence.py       append-only artifact store, Kafka cuts, recorded gaps
  capacity.py       the gate conjunction and the saturation frontier
  experiment.py     paired A/B with confound and durability rejection
  verdict.py        six independent dimensions, aggregated by severity
  report.py         the p08.v1 template, and the refusal to overclaim
  chaos.py          allowlist and fault receipts
  orchestrator.py   preflight → load → drain → reconcile
  cli.py            selftest, preflight, calibrate, bench, verify, catalog, gate
tests/            unit, fixtures, integration, statistical, chaos
```

## The rules the code enforces

These are the traps the design document spends most of its length on. Each one
is a structural property here, not a convention someone has to remember.

| Rule | Where it lives |
|---|---|
| `ticks/s`, `events/s` and `bytes/s` never mix; conversion needs a *measured* ratio | `units.Quantity` raises `UnitError` |
| `p95` is exact nearest-rank — `ceil(q·N)`, so FIX-002 gives 1000 ms, not the 226 ms mean | `percentiles.exact_nearest_rank` |
| Timeouts stay in the denominator and never get a latency | `samples.Attempt` refuses a completion time for a timeout |
| A censored quantile is a lower bound, printed as such — it can fail a budget, never pass one | `PercentileResult.meets_budget_ms` |
| Averaging percentiles is not a percentile | `percentiles.average_of_percentiles` exists only to raise |
| A p95 without family, concurrency, cache profile and N is not comparable | `PercentileClaim.require_comparable` |
| A histogram too coarse for the threshold yields `INCONCLUSIVE`, not a confident green | `histogram.resolution_for_threshold` |
| Stability needs ≥5 windows, both slopes, and no retention truncation | `timeseries.assess_stability` |
| The worst partition governs; the mean exists only as a labelled diagnostic | `PartitionedBacklog.worst` |
| An unmeasured gate is `UNKNOWN` forever; `Tri` cannot be used as a boolean | `verdict.Tri.__bool__` raises |
| `INVALID` outranks `FAIL`: a failure from a broken measurement is not a result | `verdict._SEVERITY` |
| Identity is `(dataset_id, symbol, sequence)`; a Kafka offset never is | `evidence.KafkaCut.identity()` raises |
| Prices and timestamps stay integers; a float in a comparison is refused | `reconciliation._reject_floats` |
| Physical duplicates are expected and must not change the canonical set | `reconcile_sets(physical_duplicate_count=…)` |
| A 200M comparison streams; it never loads the identity set into RAM | `reconciliation.reconcile_streams` |
| Capacity may not be extrapolated above the highest level that passed every gate | `capacity.CapacityFrontier.extrapolate` raises |
| An A/B with two changed variables is `CONFOUNDED`, not an effect | `experiment.PairedExperiment.confounds` |
| A speedup bought by weakening durability is `REJECTED_UNSAFE` | `experiment.safety_rejections` |
| A fault needs a verified receipt; an exit code of 0 is not one | `chaos.FaultInjector.inject` requires a verifier |
| Chaos touches only allowlisted targets; system paths and filesystem roots are refused | `chaos.Allowlist.check_path` |
| A report may be incomplete but may not pass a dimension without its evidence | `report.validate_report` |
| Dropping a P0 case from the plan is `NO_GO`, and "all checked" is never printed | `catalog.CoverageGate.evaluate` |
| A simulated SUT cannot evidence a capacity or latency claim | `verdict.validity_verdict` |

## Tests

876 tests, 96% statement coverage, no network and no sleeping — all timing runs
on `FakeClock`, which is what lets a "30 minute steady phase" assertion execute
in microseconds.

```
tests/unit/          the kernel, contracts, drivers, analysis, CLI and specs
tests/fixtures/      FIX-001 executed: 10 events, 3 symbols, 2 topics, 1 duplicate
tests/integration/   full lane runs against both SUTs, including the blocked case
tests/statistical/   property-based metrology, §9.3 metamorphics, and all of E2E-D
tests/chaos/         allowlist and fault-receipt refusals
```

Two test suites are worth knowing about:

- **`tests/statistical/test_adversarial_measurement.py`** runs all twelve cases
  of section 18.4 — E2E-37 through E2E-48. These test the benchmark rather than
  the pipeline: a driver that under-issues, empty results returned in 1 ms, a
  mixed-family p95, a 200–500 ms bucket against a 280 ms gate, omitted timeouts,
  300 ms clock skew, a one-minute window labelled thirty, an exporter that
  vanishes, a governor swapped for one arm, `acks=0` sold as a speedup, and a P0
  test quietly dropped from CI. Every one must come out as a refusal.
- **`tests/fixtures/test_fix001_reconciliation.py`** executes FIX-001 and
  includes the fixture's own refusal: `raw_physical` is **not** asserted to be
  11, because an insert-side dedupe may legitimately collapse the physical copy.

## What is blocked, and why

Milestones M0–M3 and the analysis halves of M10/M11 are implemented. M4–M9 need
upstream code:

| Milestone | Needs | State |
|---|---|---|
| M0 contracts, M1 metric kernel, M2 calibrator, M3 probes | — | implemented and tested |
| M4 Kafka lanes | P04 producer and broker | `BLOCKED_BY_UPSTREAM_IMPLEMENTATION` |
| M5 ingestion lanes | P05 consumer, P06 raw | `BLOCKED_BY_UPSTREAM_IMPLEMENTATION` |
| M6 query 200M | P06 canonical build + ClickHouse | `BLOCKED_BY_UPSTREAM_IMPLEMENTATION` |
| M7 full pipeline, M8 mixed load | all of the above | `BLOCKED_BY_UPSTREAM_IMPLEMENTATION` |
| M9 fault under load | P07 injectors; Linux host for netem/cgroup | `BLOCKED_BY_UPSTREAM_IMPLEMENTATION` |
| M10 A/B, M11 release gate | a valid baseline from M4–M9 | analyzer and gate built, no data |

The `p01` SUT names the owning project in its refusal, so a blocked lane says
what it is waiting on:

```
lane needs stages that cannot be exercised here: P03_ADAPTER (NOT_IMPLEMENTED,
owned by P03 C++ market data adapter), P04_BROKER (NOT_IMPLEMENTED, owned by
P04 Kafka broker), ...
```

### Host limitations

This machine is Windows, so cgroup v2, PSI and `perf` are unreadable. Resource
budgets (NFR-07) therefore come back `UNKNOWN`, which makes the safety dimension
`INCONCLUSIVE` rather than `PASS` — section 5.1 forbids lowering host security
to obtain a metric, and section 15 forbids an unknown from defaulting to the
passing value. The section 14 chaos campaigns cannot execute here either;
`chaos.platform_chaos_limitations()` says so instead of reporting an empty
campaign that looks like a pass.

## When P02–P07 land

1. Implement `SystemUnderTest` for the real pipeline and declare each stage
   `AVAILABLE` (see `sut.py`); the blocked lanes unblock themselves.
2. Supply the expected set from the sealed P01 manifest plus an **independent**
   reading of the P03 validation policy — not from a P05 or P06 counter, which
   would make the comparison circular (case G01-12).
3. Pin the query catalog: exact SQL, binds, settings and client timeout per QID,
   so `query_catalog_sha256` can detect case G01-13.
4. Run `p08-calibrate` at the target rate on the real host *first*. Until the
   driver demonstrably issues the schedule, no rate claim is valid however the
   pipeline behaves.
5. Keep the per-case status in the catalog and let `p08-gate` decide the release.

## Scope

Synthetic workloads only. Not market data, not for trading. Lab use: chaos is
allowlisted and dry-run by default, and `safety.allow_host_root_path` is `false`
in every shipped profile.
