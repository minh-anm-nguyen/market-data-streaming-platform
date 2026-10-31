"""Section 18.4 executed: the twelve adversarial measurement cases (E2E-37..48).

E2E-D is the part of the suite that tests the benchmark rather than the
pipeline. Each case is a way to produce a flattering number from a broken
experiment, and the required outcome is always a refusal: ``INVALID``,
``INCONCLUSIVE``, ``FAIL`` or ``NO_GO``, never a pass.

All twelve run here. They need no Kafka and no ClickHouse, because the subject
is the measuring apparatus -- which is exactly why section 8 puts metrology
failures in their own fault class and section 9.1 gives statistical validation
its own layer of the pyramid.

One test per case, named for the case, so a reader can check the mapping
against the document.
"""

from __future__ import annotations

import pytest

from p08_bench.calibrator import Calibration, NullSink, calibrate
from p08_bench.capacity import CapacityFrontier
from p08_bench.capacity import Gate as CapacityGate
from p08_bench.capacity import gates_from_observations
from p08_bench.catalog import (
    CaseCatalog,
    CaseResult,
    CaseStatus,
    CatalogCase,
    CoverageGate,
    Kind,
    Priority,
)
from p08_bench.clock import ClockDomain, ClockSyncAssessment, ClockSyncQuality, FakeClock
from p08_bench.config import build_config
from p08_bench.errors import MetrologyError
from p08_bench.event_driver import default_payload_factory
from p08_bench.evidence import EvidenceGap, EvidenceStore
from p08_bench.experiment import Arm, Hypothesis, Outcome, PairedExperiment, RunSample
from p08_bench.goldens import FIX002_EXPECTED_P95_MS, FIX002_LATENCIES_MS
from p08_bench.histogram import BucketHistogram, Resolution
from p08_bench.percentiles import (
    PercentileClaim,
    percentile_completed_only,
    percentile_with_censoring,
    pooled_claim,
)
from p08_bench.samples import AttemptRegistry, Outcome as AttemptOutcome
from p08_bench.scheduler import RateSchedule
from p08_bench.sut import SubmitOutcome, SubmitStatus
from p08_bench.timeseries import Stability, WindowedSeries, assess_stability
from p08_bench.units import Unit, ticks_per_s
from p08_bench.verdict import Dimension, RunEvidence, Status, Tri, decide

from ..conftest import workload_dict

pytestmark = pytest.mark.statistical

MS = 1_000_000


def clean_evidence(**overrides) -> RunEvidence:
    evidence = RunEvidence(
        executed=True,
        manifest_valid=Tri.TRUE,
        workload_calibrated=Tri.TRUE,
        steady_window_matches_manifest=Tri.TRUE,
        clock_adequate_for_claims=Tri.TRUE,
        sufficient_samples=Tri.TRUE,
        histogram_resolution_adequate=Tri.TRUE,
        evidence_complete=Tri.TRUE,
        source_sealed=Tri.TRUE,
        resources_bounded=Tri.TRUE,
        no_oom=Tri.TRUE,
        no_silent_loss=Tri.TRUE,
        safety_allowlist_respected=Tri.TRUE,
    )
    for key, value in overrides.items():
        setattr(evidence, key, value)
    return evidence


class TestE2E37DriverUnderIssues:
    """E2E-37: driver configured for 120k, emits 70k. INVALID, series kept."""

    def _short_sink(self, limit: int) -> NullSink:
        class Short(NullSink):
            def __init__(self) -> None:
                super().__init__()
                self.seen = 0

            def submit(self, identity, payload, t_ns):  # type: ignore[override]
                self.seen += 1
                if self.seen > limit:
                    return SubmitOutcome(SubmitStatus.DROPPED, identity, "refused")
                return NullSink.submit(self, identity, payload, t_ns)

        return Short()

    def test_the_capacity_claim_is_invalid(self):
        clock = FakeClock(ClockDomain("bench", "driver"))
        # 1200 slots scheduled, 700 accepted: the documented 120k/70k ratio.
        result = calibrate(
            RateSchedule.constant_rate(120_000, 0.01),
            default_payload_factory("ds", ["EURUSD"]),
            clock,
            sink=self._short_sink(700),
        )
        assert result.verdict is Calibration.GENERATOR_BOUND
        assert not result.supports_rate_claim
        verdict = decide(clean_evidence(workload_calibrated=Tri.FALSE, is_capacity_claim=True))
        assert verdict.overall is Status.INVALID

    def test_the_actual_issue_series_is_retained(self):
        clock = FakeClock(ClockDomain("bench", "driver"))
        result = calibrate(
            RateSchedule.constant_rate(120_000, 0.01),
            default_payload_factory("ds", ["EURUSD"]),
            clock,
            sink=self._short_sink(700),
        )
        payload = result.as_dict()
        assert payload["drive"]["counters"]["scheduled"] == 1200
        assert payload["drive"]["counters"]["issued"] == 700
        assert payload["drive"]["counters"]["not_issued"] == 500
        assert payload["target_ticks_per_s"] == 120_000


class TestE2E38EmptyResultsAreFast:
    """E2E-38: all-empty results at 1 ms. The oracle fails it regardless."""

    def test_a_wrong_result_yields_no_latency_claim(self):
        registry = AttemptRegistry("q02", deadline_ns=2000 * MS)
        for seq in range(100):
            registry.record(
                seq, seq * MS, issued_ns=seq * MS, completed_ns=seq * MS + MS,
                outcome=AttemptOutcome.COMPLETED, correct=False,
            )
        assert registry.incorrect_completed == 100
        assert registry.percentile(0.95).value_ms is None

    def test_correctness_fails_even_with_a_perfect_latency(self):
        verdict = decide(
            clean_evidence(
                is_latency_claim=True,
                query_results_correct=Tri.FALSE,
                latency_within_budget=Tri.TRUE,
                reconciliation=None,
            )
        )
        assert verdict.by_dimension(Dimension.CORRECTNESS).status is Status.FAIL
        assert verdict.by_dimension(Dimension.LATENCY).status is Status.PASS
        assert verdict.overall is Status.FAIL


class TestE2E39HeterogeneousPercentile:
    """E2E-39: a mixed Q01+Q06 p95 compared against Q02-280ms is refused."""

    def _claim(self, family: str) -> PercentileClaim:
        return PercentileClaim(
            result=percentile_completed_only([100.0], 0.95),
            query_family=family, concurrency=4, cache_profile="warm",
            dataset_build_id="BUILD-200M",
        )

    def test_the_analyzer_refuses_the_comparison(self):
        with pytest.raises(MetrologyError, match="different workloads"):
            self._claim("Q02").require_comparable(self._claim("Q06"))

    def test_pooling_across_families_is_refused(self):
        with pytest.raises(MetrologyError, match="different workloads"):
            pooled_claim([self._claim("Q01"), self._claim("Q06")], [100.0], 0.95)

    def test_pooling_within_one_family_is_allowed(self):
        pooled = pooled_claim(
            [self._claim("Q02"), self._claim("Q02")], list(FIX002_LATENCIES_MS), 0.95
        )
        assert pooled.result.value_ms == FIX002_EXPECTED_P95_MS


class TestE2E40CoarseHistogram:
    """E2E-40: a 200-500 ms bucket cannot decide a 280 ms threshold."""

    def test_the_resolution_is_insufficient(self):
        hist = BucketHistogram([50, 100, 200, 500, 1000])
        assert hist.resolution_for_threshold(280.0) is Resolution.INSUFFICIENT

    def test_the_bucketed_quantile_decides_nothing(self):
        hist = BucketHistogram([50, 100, 200, 500, 1000])
        hist.observe_many([250.0] * 100)
        assert hist.quantile_interval(0.95).decides_threshold(280.0) is None

    def test_the_run_is_inconclusive_not_green(self):
        verdict = decide(
            clean_evidence(
                is_latency_claim=True,
                histogram_resolution_adequate=Tri.FALSE,
                latency_within_budget=Tri.UNKNOWN,
            )
        )
        assert verdict.by_dimension(Dimension.VALIDITY).status is Status.INVALID
        assert verdict.by_dimension(Dimension.LATENCY).status is Status.INCONCLUSIVE


class TestE2E41OmittedTimeouts:
    """E2E-41: timeouts omitted from the samples. The registry prevents it."""

    def test_the_attempt_registry_keeps_the_denominator(self):
        registry = AttemptRegistry("q02", deadline_ns=2000 * MS)
        for seq in range(90):
            registry.record(
                seq, seq * MS, issued_ns=seq * MS, completed_ns=seq * MS + MS,
                outcome=AttemptOutcome.COMPLETED, correct=True,
            )
        for seq in range(90, 100):
            registry.record(seq, seq * MS, issued_ns=seq * MS, outcome=AttemptOutcome.TIMEOUT)
        assert registry.scheduled == 100
        assert registry.error_rate == pytest.approx(0.10)

    def test_the_omitted_view_would_have_looked_better(self):
        # The point of the case: a p95 over completions only is a different and
        # more flattering number, and it has to be labelled as conditional.
        completed = [float(i) for i in range(1, 91)]
        conditional = percentile_completed_only(completed, 0.95, n_censored=10)
        honest = percentile_with_censoring(
            completed, 0.95, n_censored=10, deadline_ms=5000.0
        )
        assert conditional.value_ms == 86.0
        assert honest.is_lower_bound
        # The honest view has no value to print, and it still settles the
        # budget: the quantile is known to be at or above the 5000 ms deadline,
        # so a 280 ms budget is definitively missed. A censored bound can fail a
        # budget; it can never pass one.
        assert honest.meets_budget_ms(280.0) is False
        assert honest.meets_budget_ms(9000.0) is None


class TestE2E42ClockSkew:
    """E2E-42: 300 ms skew makes one-way latency INCONCLUSIVE; RTT stays valid."""

    def _assessment(self) -> ClockSyncAssessment:
        return ClockSyncAssessment(
            ClockDomain("source"), ClockDomain("clickhouse"),
            offset_ns=300 * MS, jitter_ns=5 * MS, uncertainty_ns=300 * MS, method="ntp",
        )

    def test_one_way_latency_is_not_supportable(self):
        assert self._assessment().quality_for(280 * MS) is ClockSyncQuality.INSUFFICIENT

    def test_the_latency_claim_becomes_invalid(self):
        verdict = decide(
            clean_evidence(is_latency_claim=True, clock_adequate_for_claims=Tri.FALSE)
        )
        assert verdict.by_dimension(Dimension.VALIDITY).status is Status.INVALID

    def test_a_monotonic_round_trip_is_unaffected(self):
        # The skew is between hosts; a client-side RTT uses one clock.
        clock = FakeClock(ClockDomain("bench", "query-driver"))
        start = clock.now()
        clock.advance_ns(120 * MS)
        assert clock.now() - start == 120 * MS


class TestE2E43MislabelledWindow:
    """E2E-43: one minute of steady state labelled as thirty is INVALID."""

    def test_the_config_declares_the_shortfall(self):
        config = build_config(workload_dict(load={"steady_seconds": 60}))
        assert not config.load.meets_steady_protocol
        assert any("below the 1800s protocol" in text for text in config.claim_limitations())

    def test_a_window_mismatch_invalidates_the_run(self):
        verdict = decide(
            clean_evidence(steady_window_matches_manifest=Tri.FALSE, is_capacity_claim=True)
        )
        assert verdict.by_dimension(Dimension.VALIDITY).status is Status.INVALID

    def test_a_short_window_fails_the_capacity_gate(self):
        gates = gates_from_observations(
            ticks_per_s(120_000), useful_verified=ticks_per_s(120_000), calibrated=True,
            steady_seconds=60, required_steady_seconds=1800, backlog=None,
            drain_reconciled=True, correctness_clean=True, latency_within_budget=True,
            error_fraction=0.0, error_budget=0.01, resource_blockers=[],
        )
        assert CapacityGate.STEADY_DURATION in gates.failed_gates()


class TestE2E44ExporterGap:
    """E2E-44: the exporter disappears during the busiest five minutes."""

    def test_the_gap_is_recorded_and_not_interpolated(self, tmp_path):
        store = EvidenceStore(tmp_path / "ev")
        store.record_gap(EvidenceGap("query_p95", 0, 300 * 10**9, "exporter crashed"))
        completeness = store.completeness()
        assert completeness["evidence_interval_has_gaps"]
        assert completeness["gaps"][0]["interpolated"] is False

    def test_a_series_with_a_gap_cannot_establish_stability(self):
        series = WindowedSeries("backlog", Unit.RECORDS)
        for index in range(10):
            series.add(index * 60 * 10**9, 100.0)
        assessment = assess_stability(series, tolerance_per_s=0.5, gap_windows=5)
        assert assessment.verdict is Stability.INVALIDATED
        assert not assessment.stable


class TestE2E45ConfoundedAB:
    """E2E-45: the CPU governor swapped for the B arm only."""

    def _experiment(self) -> PairedExperiment:
        exp = PairedExperiment(
            hypothesis=Hypothesis(
                statement="larger batches raise throughput",
                single_variable="max_rows",
                expected_direction="higher",
                min_pairs=2,
            ),
            declared_differences=("max_rows",),
        )
        for index, (arm, value) in enumerate(
            [(Arm.A, 100.0), (Arm.B, 140.0), (Arm.B, 140.0), (Arm.A, 100.0)]
        ):
            exp.add(RunSample(arm, index, value, correctness_clean=True))
        exp.arm_metadata = {
            Arm.A: {"max_rows": 1000, "cpu_governor": "performance"},
            Arm.B: {"max_rows": 5000, "cpu_governor": "powersave"},
        }
        return exp

    def test_the_pair_is_rejected_as_confounded(self):
        outcome, reasons = self._experiment().outcome()
        assert outcome is Outcome.CONFOUNDED
        assert any("cpu_governor" in reason for reason in reasons)

    def test_a_clean_rerun_is_required_before_any_effect_is_reported(self):
        outcome, reasons = self._experiment().outcome()
        assert outcome is Outcome.CONFOUNDED
        assert not any("median effect" in reason for reason in reasons)

    def test_removing_the_confound_allows_the_effect(self):
        exp = self._experiment()
        exp.arm_metadata = {Arm.A: {"max_rows": 1000}, Arm.B: {"max_rows": 5000}}
        assert exp.outcome()[0] is Outcome.OPTIMIZED


class TestE2E46PercentileSelfTest:
    """E2E-46: the ten-sample fixture with a 1000 ms outlier."""

    def test_nearest_rank_p95_is_the_outlier(self):
        assert percentile_completed_only(FIX002_LATENCIES_MS, 0.95).value_ms == 1000.0

    def test_the_histogram_approximation_is_disclaimed(self):
        hist = BucketHistogram([100, 200, 500, 1000, 2000])
        hist.observe_many(FIX002_LATENCIES_MS)
        interval = hist.quantile_interval(0.95)
        assert interval.as_dict()["estimate"] is True
        assert interval.contains(1000.0)


class TestE2E47DurabilityDowngrade:
    """E2E-47: acks=all swapped for acks=0 and the speedup claimed."""

    def test_the_config_flags_the_weakened_contract(self):
        config = build_config(workload_dict(kafka={"producer_acks": "0"}))
        assert not config.kafka.durability_contract_intact
        assert any("durability contract" in text for text in config.claim_limitations())

    def test_the_optimization_is_rejected_however_large_the_gain(self):
        exp = PairedExperiment(
            hypothesis=Hypothesis(
                statement="acks=0 is faster",
                single_variable="producer_acks",
                expected_direction="higher",
                min_pairs=2,
            ),
            declared_differences=("producer_acks",),
            arm_durability_intact={Arm.A: True, Arm.B: False},
        )
        for index, (arm, value) in enumerate(
            [(Arm.A, 100.0), (Arm.B, 300.0), (Arm.B, 300.0), (Arm.A, 100.0)]
        ):
            exp.add(RunSample(arm, index, value, correctness_clean=True))
        outcome, reasons = exp.outcome()
        assert outcome is Outcome.REJECTED_UNSAFE
        assert any("durability contract" in reason for reason in reasons)


class TestE2E48MissingP0Test:
    """E2E-48: a P0 case removed from the CI plan must block the release."""

    def _catalog(self) -> CaseCatalog:
        return CaseCatalog(
            [
                CatalogCase("P0-A", Kind.WHAT_IF, "G01", "g", Priority.P0, "w", "s", "e"),
                CatalogCase("P0-B", Kind.WHAT_IF, "G01", "g", Priority.P0, "w", "s", "e"),
                CatalogCase("P0-C", Kind.WHAT_IF, "G02", "g", Priority.P0, "w", "s", "e"),
            ]
        )

    def test_the_gate_is_no_go_when_a_p0_case_is_dropped(self):
        gate = CoverageGate(catalog=self._catalog())
        gate.plan(["P0-A", "P0-B"])
        for case_id in ("P0-A", "P0-B"):
            gate.record(CaseResult(case_id, CaseStatus.PASS, evidence_ref=f"runs/{case_id}"))
        result = gate.evaluate()
        assert result["verdict"] == "NO_GO"
        assert result["unplanned_p0"] == ["P0-C"]

    def test_an_all_checked_status_is_never_printed(self):
        gate = CoverageGate(catalog=self._catalog())
        gate.plan(["P0-A", "P0-B"])
        for case_id in ("P0-A", "P0-B"):
            gate.record(CaseResult(case_id, CaseStatus.PASS, evidence_ref=f"runs/{case_id}"))
        statement = gate.evaluate()["coverage_statement"]
        assert "2/3 applicable P0 cases satisfied" in statement
        assert "1 never planned" in statement

    def test_the_real_catalog_blocks_before_anything_runs(self):
        result = CoverageGate(catalog=CaseCatalog.load()).evaluate()
        assert result["verdict"] == "NO_GO"
        assert len(result["unplanned_p0"]) == 242


class TestFrontierRefusesExtrapolation:
    """Not an E2E-D case, but the same class of error: inventing a number.

    Section 6.3 forbids extrapolating capacity above the highest level that
    passed every gate, and the request sounds reasonable enough to be worth a
    named refusal.
    """

    def test_the_gap_between_pass_and_fail_stays_unclaimed(self):
        frontier = CapacityFrontier(Unit.TICKS_PER_S)
        frontier.add(
            gates_from_observations(
                ticks_per_s(100_000), useful_verified=ticks_per_s(100_000), calibrated=True,
                steady_seconds=1800, required_steady_seconds=1800, backlog=None,
                drain_reconciled=True, correctness_clean=True, latency_within_budget=True,
                error_fraction=0.0, error_budget=0.01, resource_blockers=[],
            )
        )
        with pytest.raises(MetrologyError, match="may not be extrapolated"):
            frontier.extrapolate(110_000)
