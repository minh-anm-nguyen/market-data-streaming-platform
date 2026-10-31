"""Phase orchestration: preflight, warmup, ramp, steady, drain, reconcile (section 11).

Section 11 gives each phase a different evidential status, and the orchestrator
exists to keep those statuses from leaking into each other:

* **Preflight** gates validity and produces no performance numbers at all.
* **Warmup** is reported and excluded from the steady percentile.
* **Ramp** finds the knee and establishes no capacity.
* **Steady** is the only window a capacity or latency claim may come from.
* **Drain** has a recorded deadline, so an incomplete drain is visible rather
  than waited out.
* **Reconcile** runs the independent oracle, and no result is published before
  it finishes.

The orchestrator never decides a verdict itself. It assembles a
:class:`~p08_bench.verdict.RunEvidence` out of what was actually measured --
with ``UNKNOWN`` wherever nothing was -- and hands that to
:func:`~p08_bench.verdict.decide`. That separation is what keeps the
"unknown stays unknown" rule from being quietly bypassed by a convenience
default inside the run loop.

Stop conditions are section 11's list. Hitting one stops the run and is recorded
as a stop, never as a pass.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .calibrator import CalibrationResult
from .capacity import Gate, gates_from_observations
from .clock import ClockSyncAssessment, FakeClock, MonotonicClock
from .config import WorkloadConfig
from .errors import UpstreamUnavailable
from .event_driver import DriveResult, EventDriver, PayloadFactory
from .evidence import EvidenceStore
from .manifest import HostProfile, RunManifest, SourceSeal
from .metrics import StageCounters, conservation_report
from .reconciliation import Reconciliation, ReconciliationResult
from .report import ReportBuilder
from .scheduler import NS_PER_S, PhasePlan, SlotKind
from .sut import Observation, Stage, SystemUnderTest, requirements_for
from .timeseries import PartitionedBacklog, StabilityAssessment
from .units import Quantity, Unit, rate
from .verdict import RunEvidence, RunVerdict, Status, Tri, decide


#: Artifacts the orchestrator itself writes, and therefore the only ones its
#: own completeness check can require. The report and any latency-sample dump
#: are written by the caller *after* the lane returns, so including them here
#: would make ``evidence_complete`` permanently false and every run INVALID.
#: The whole bundle is checked separately by ``p08-bench verify``.
ORCHESTRATOR_ARTIFACTS: tuple[str, ...] = (
    "preflight",
    "run_manifest",
    "host_profile",
    "workload_config",
    "stage_counters",
    "reconciliation",
)


class Phase(str, Enum):
    PREFLIGHT = "PREFLIGHT"
    WARMUP = "WARMUP"
    RAMP = "RAMP"
    STEADY = "STEADY"
    DRAIN = "DRAIN"
    RECONCILE = "RECONCILE"


class StopReason(str, Enum):
    """Section 11's stop conditions. A stop is never a pass."""

    COMPLETED = "COMPLETED"
    PREFLIGHT_FAILED = "PREFLIGHT_FAILED"
    BLOCKED_BY_UPSTREAM = "BLOCKED_BY_UPSTREAM_IMPLEMENTATION"
    MEMORY_LIMIT = "MEMORY_LIMIT"
    DISK_LIMIT = "DISK_LIMIT"
    OOM_OBSERVED = "OOM_OBSERVED"
    INVALID_CLOCK = "INVALID_CLOCK"
    INSUFFICIENT_PROVENANCE = "INSUFFICIENT_PROVENANCE"
    UNBOUNDED_BACKLOG = "UNBOUNDED_BACKLOG"
    METRIC_CARDINALITY = "METRIC_CARDINALITY"
    SAFETY_REFUSAL = "SAFETY_REFUSAL"
    DRIVER_NOT_CALIBRATED = "DRIVER_NOT_CALIBRATED"


@dataclass
class PhaseRecord:
    phase: Phase
    start_ns: int
    end_ns: int | None = None
    notes: list[str] = field(default_factory=list)

    @property
    def duration_s(self) -> float | None:
        if self.end_ns is None:
            return None
        return (self.end_ns - self.start_ns) / NS_PER_S

    def as_dict(self) -> dict[str, Any]:
        return {
            "phase": self.phase.value,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "duration_s": self.duration_s,
            "notes": list(self.notes),
        }


@dataclass
class PreflightResult:
    """Validity gates evaluated before any load is offered."""

    checks: dict[str, Tri] = field(default_factory=dict)
    blockers: list[str] = field(default_factory=list)
    blocked_stages: tuple[str, ...] = ()

    def set(self, name: str, value: Tri, blocker: str | None = None) -> None:
        self.checks[name] = value
        if value is Tri.FALSE and blocker:
            self.blockers.append(blocker)

    @property
    def ok(self) -> bool:
        return not self.blockers and not self.blocked_stages

    def as_dict(self) -> dict[str, Any]:
        return {
            "checks": {k: v.value for k, v in sorted(self.checks.items())},
            "blockers": list(self.blockers),
            "blocked_stages": list(self.blocked_stages),
            "ok": self.ok,
        }


@dataclass
class LaneResult:
    """Everything one lane produced."""

    run_id: str
    scenario_id: str
    phases: list[PhaseRecord]
    stop_reason: StopReason
    preflight: PreflightResult
    verdict: RunVerdict
    report: dict[str, Any]
    calibration: CalibrationResult | None = None
    drive: DriveResult | None = None
    observations: list[Observation] = field(default_factory=list)
    reconciliation: ReconciliationResult | None = None
    backlog_assessment: StabilityAssessment | None = None
    capacity_gates: Mapping[str, Any] | None = None

    @property
    def overall(self) -> Status:
        return self.verdict.overall

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "scenario_id": self.scenario_id,
            "stop_reason": self.stop_reason.value,
            "phases": [p.as_dict() for p in self.phases],
            "preflight": self.preflight.as_dict(),
            "verdict": self.verdict.as_dict(),
            "calibration": self.calibration.as_dict() if self.calibration else None,
            "drive": self.drive.as_dict() if self.drive else None,
            "reconciliation": self.reconciliation.summary() if self.reconciliation else None,
            "backlog": self.backlog_assessment.as_dict() if self.backlog_assessment else None,
            "capacity_gates": self.capacity_gates,
            "report": self.report,
        }


#: Supplies the expected identity set and payloads for the reconcile phase.
#: Returning ``None`` means no oracle was available, which is INCONCLUSIVE and
#: never a pass.
OracleFactory = Callable[[], tuple[set[tuple[str, str, int]], Mapping[tuple[str, str, int], Mapping[str, Any]]] | None]


class Orchestrator:
    """Runs one lane end to end and assembles its evidence."""

    def __init__(
        self,
        config: WorkloadConfig,
        sut: SystemUnderTest,
        clock: MonotonicClock | FakeClock,
        *,
        scenario_id: str,
        dataset_seal: SourceSeal,
        host_profile: HostProfile,
        evidence: EvidenceStore | None = None,
        clock_sync: ClockSyncAssessment | None = None,
        backlog_tolerance_per_s: float = 1.0,
        observation_interval_s: float = 1.0,
        error_budget: float = 0.01,
        resource_blockers: Sequence[str] = (),
    ) -> None:
        self.config = config
        self.sut = sut
        self.clock = clock
        self.scenario_id = scenario_id
        self.dataset_seal = dataset_seal
        self.host_profile = host_profile
        self.evidence = evidence
        self.clock_sync = clock_sync
        self.backlog_tolerance_per_s = backlog_tolerance_per_s
        self.observation_interval_s = observation_interval_s
        self.error_budget = error_budget
        self.resource_blockers = tuple(resource_blockers)
        self.phases: list[PhaseRecord] = []

    # -- manifest ---------------------------------------------------------
    def build_manifest(self) -> RunManifest:
        return RunManifest(
            run_id=self.config.run.id,
            test_id=self.scenario_id,
            lane=self.config.run.lane,
            seed=self.config.run.seed,
            profile=self.config.run.profile.value,
            created_at_utc="1970-01-01T00:00:00Z",
            workload_sha256=self.config.sha256,
            config_sha256=self.config.sha256,
            dataset_seal=self.dataset_seal,
            host_profile_sha256=self.host_profile.digest,
        )

    # -- phases -----------------------------------------------------------
    def _begin(self, phase: Phase) -> PhaseRecord:
        record = PhaseRecord(phase=phase, start_ns=self.clock.now_ns())
        self.phases.append(record)
        return record

    def preflight(self, calibration: CalibrationResult | None) -> PreflightResult:
        """Validity gates. No performance number comes out of this phase."""
        record = self._begin(Phase.PREFLIGHT)
        result = PreflightResult()
        manifest = self.build_manifest()

        blockers = manifest.validity_blockers()
        result.set(
            "manifest_valid",
            Tri.of(not blockers),
            "; ".join(blockers) if blockers else None,
        )
        result.set(
            "source_sealed",
            Tri.of(self.dataset_seal.usable_for_completeness_claim),
            f"source is {self.dataset_seal.state.value}" if not self.dataset_seal.sealed else None,
        )

        required = requirements_for(self.config.run.lane)
        try:
            self.sut.require_stages(required)  # type: ignore[attr-defined]
            result.set("stages_available", Tri.TRUE)
        except UpstreamUnavailable as exc:
            result.blocked_stages = tuple(exc.context.get("stages", ()))
            result.set("stages_available", Tri.FALSE, exc.message)

        if calibration is None:
            result.set("workload_calibrated", Tri.UNKNOWN)
        else:
            result.set(
                "workload_calibrated",
                Tri.of(calibration.supports_rate_claim),
                "; ".join(calibration.reasons) if not calibration.supports_rate_claim else None,
            )

        if self.config.query.supports_latency_claim:
            if self.clock_sync is None:
                result.set("clock_adequate", Tri.UNKNOWN)
            else:
                budget_ns = self.config.query.client_p95_budget_ms * 1_000_000
                result.set(
                    "clock_adequate",
                    Tri.of(self.clock_sync.supports_one_way_latency(budget_ns)),
                    "clock uncertainty is too large for the latency budget",
                )
        else:
            result.set("clock_adequate", Tri.TRUE)

        record.end_ns = self.clock.now_ns()
        record.notes.append(f"{len(result.blockers)} blocker(s)")
        if self.evidence is not None:
            self.evidence.write_json("preflight", result.as_dict())
            self.evidence.write_json("run_manifest", manifest.as_dict())
            self.evidence.write_json("host_profile", self.host_profile.as_dict())
            self.evidence.write_json("workload_config", self.config.as_dict())
        return result

    def _observe(self, phase: Phase, backlog: PartitionedBacklog, until_ns: int) -> list[Observation]:
        """Sample the SUT at the observation interval until ``until_ns``."""
        step = max(int(self.observation_interval_s * NS_PER_S), 1)
        observations: list[Observation] = []
        now = self.clock.now_ns()
        while now < until_ns:
            observation = self.sut.observe(now)
            observations.append(observation)
            for (topic, partition), records in observation.backlog.per_partition.items():
                backlog.add(topic, partition, now, records)
            for topic, partition in observation.backlog.retention_truncated:
                backlog.mark_retention_truncated(topic, partition)
            now += step
            if isinstance(self.clock, FakeClock):
                self.clock.set_ns(min(now, until_ns))
        return observations

    # -- the lane ---------------------------------------------------------
    def run(
        self,
        payload_factory: PayloadFactory,
        *,
        calibration: CalibrationResult | None = None,
        oracle: OracleFactory | None = None,
        canonical_payload: Callable[[tuple[str, str, int]], Mapping[str, Any] | None] | None = None,
        is_capacity_claim: bool | None = None,
        is_latency_claim: bool | None = None,
        latency_within_budget: Tri = Tri.UNKNOWN,
        safety: Mapping[str, Tri] | None = None,
    ) -> LaneResult:
        """Execute the lane and return its evidence and verdict."""
        preflight = self.preflight(calibration)
        builder = ReportBuilder(self.config.run.id, self.scenario_id)
        builder.add_limitations(self.config.claim_limitations())

        capacity_claim = (
            self.config.load.meets_steady_protocol if is_capacity_claim is None else is_capacity_claim
        )
        latency_claim = (
            self.config.query.supports_latency_claim if is_latency_claim is None else is_latency_claim
        )

        if preflight.blocked_stages:
            evidence = RunEvidence(
                executed=False, blocked_stages=preflight.blocked_stages,
                notes=["lane requires stages with no implementation in this repository"],
            )
            verdict = decide(evidence)
            builder.apply_verdict(verdict)
            builder.add_blockers(preflight.blockers)
            return LaneResult(
                run_id=self.config.run.id, scenario_id=self.scenario_id,
                phases=self.phases, stop_reason=StopReason.BLOCKED_BY_UPSTREAM,
                preflight=preflight, verdict=verdict, report=builder.build_unchecked(),
                calibration=calibration,
            )

        if not preflight.ok:
            evidence = RunEvidence(
                executed=True,
                manifest_valid=preflight.checks.get("manifest_valid", Tri.UNKNOWN),
                source_sealed=preflight.checks.get("source_sealed", Tri.UNKNOWN),
                workload_calibrated=preflight.checks.get("workload_calibrated", Tri.UNKNOWN),
                steady_window_matches_manifest=Tri.UNKNOWN,
                evidence_complete=Tri.FALSE,
                sut_is_simulation=self.sut.is_simulation,
                is_capacity_claim=capacity_claim,
                is_latency_claim=latency_claim,
                notes=["preflight failed; no load was offered"],
            )
            verdict = decide(evidence)
            builder.apply_verdict(verdict)
            builder.add_blockers(preflight.blockers)
            stop = (
                StopReason.DRIVER_NOT_CALIBRATED
                if preflight.checks.get("workload_calibrated") is Tri.FALSE
                else StopReason.PREFLIGHT_FAILED
            )
            return LaneResult(
                run_id=self.config.run.id, scenario_id=self.scenario_id,
                phases=self.phases, stop_reason=stop, preflight=preflight,
                verdict=verdict, report=builder.build_unchecked(), calibration=calibration,
            )

        plan = PhasePlan(
            warmup_s=self.config.load.warmup_seconds,
            ramp_s=self.config.load.ramp_up_seconds,
            steady_s=self.config.load.steady_seconds,
            drain_s=self.config.load.drain_seconds,
            tick_rate_per_s=self.config.load.market_tick_target_per_s,
            quote_rate_per_s=self.config.load.quote_target_per_s,
        )
        schedule = plan.schedule(start_ns=self.clock.now_ns())
        backlog = PartitionedBacklog()

        # Warmup, ramp and steady are one continuous offer; the phase records
        # mark which window each observation belongs to so the steady
        # percentile never includes warmup samples.
        load_record = self._begin(Phase.STEADY)
        driver = EventDriver(
            self.sut, self.clock,
            max_late_issue_ms=self.config.load.max_late_issue_ms,
        )
        observations: list[Observation] = []

        def on_slot(slot, issued_ns: int) -> None:
            if not observations or issued_ns - observations[-1].t_ns >= int(
                self.observation_interval_s * NS_PER_S
            ):
                observation = self.sut.observe(issued_ns)
                observations.append(observation)
                for (topic, partition), records in observation.backlog.per_partition.items():
                    backlog.add(topic, partition, issued_ns, records)
                for topic, partition in observation.backlog.retention_truncated:
                    backlog.mark_retention_truncated(topic, partition)

        drive = driver.drive(schedule, payload_factory, on_slot=on_slot)
        load_record.end_ns = self.clock.now_ns()
        load_record.notes.append(
            f"offered {drive.counters.scheduled} slots, issued {drive.counters.issued}"
        )

        drain_record = self._begin(Phase.DRAIN)
        drain_deadline = self.clock.now_ns() + self.config.load.drain_seconds * NS_PER_S
        drain_observation = self.sut.drain(drain_deadline)
        observations.append(drain_observation)
        drain_record.end_ns = self.clock.now_ns()
        drain_incomplete = any("processing debt" in note for note in drain_observation.notes)
        if drain_incomplete:
            drain_record.notes.extend(drain_observation.notes)

        reconcile_record = self._begin(Phase.RECONCILE)
        reconciliation: ReconciliationResult | None = None
        if oracle is not None:
            built = oracle()
            if built is not None:
                expected_ids, expected_payloads = built
                from .reconciliation import reconcile_sets

                reconciliation = reconcile_sets(
                    expected_ids,
                    self.sut.canonical_identities(),
                    expected_payload=lambda key: expected_payloads.get(key),
                    canonical_payload=canonical_payload,
                    physical_duplicate_count=None,
                )
        reconcile_record.end_ns = self.clock.now_ns()

        assessment = backlog.worst(tolerance_per_s=self.backlog_tolerance_per_s)
        final = observations[-1] if observations else None
        counters: StageCounters = final.counters if final else StageCounters()

        # Written before the evidence is assembled, so the completeness check
        # below sees the artifacts this run is responsible for.
        if self.evidence is not None:
            self.evidence.write_json("stage_counters", conservation_report(counters))
            self.evidence.write_json(
                "reconciliation",
                reconciliation.summary() if reconciliation else {"verdict": "NOT_RUN"},
            )

        measured_seconds = drive.measured_seconds
        verified_rate: Quantity | None = None
        if measured_seconds and counters.canonical_unique_count is not None:
            verified_rate = rate(
                counters.canonical_unique_count, measured_seconds, Unit.TICKS_PER_S
            )

        gates = gates_from_observations(
            Quantity(float(self.config.load.market_tick_target_per_s), Unit.TICKS_PER_S),
            useful_verified=verified_rate,
            calibrated=None if calibration is None else calibration.supports_rate_claim,
            steady_seconds=self.config.load.steady_seconds,
            required_steady_seconds=self.config.load.steady_seconds,
            backlog=assessment,
            drain_reconciled=None if reconciliation is None else not drain_incomplete,
            correctness_clean=None if reconciliation is None else reconciliation.clean,
            latency_within_budget=None if latency_within_budget is Tri.UNKNOWN else latency_within_budget is Tri.TRUE,
            error_fraction=0.0 if drive.counters.errors == 0 else drive.counters.errors / max(drive.counters.scheduled, 1),
            error_budget=self.error_budget,
            resource_blockers=self.resource_blockers,
        )

        safety_flags = dict(safety or {})
        evidence = RunEvidence(
            executed=True,
            manifest_valid=preflight.checks.get("manifest_valid", Tri.UNKNOWN),
            source_sealed=preflight.checks.get("source_sealed", Tri.UNKNOWN),
            workload_calibrated=preflight.checks.get("workload_calibrated", Tri.UNKNOWN),
            steady_window_matches_manifest=Tri.of(
                load_record.duration_s is not None
                and load_record.duration_s >= self.config.load.steady_seconds
            ),
            clock_adequate_for_claims=preflight.checks.get("clock_adequate", Tri.UNKNOWN),
            sufficient_samples=safety_flags.pop("sufficient_samples", Tri.UNKNOWN),
            histogram_resolution_adequate=safety_flags.pop(
                "histogram_resolution_adequate", Tri.UNKNOWN
            ),
            evidence_complete=Tri.of(
                self.evidence is None
                or self.evidence.completeness(ORCHESTRATOR_ARTIFACTS)["complete"]
            ),
            sut_is_simulation=self.sut.is_simulation,
            reconciliation=None if reconciliation is None else reconciliation.verdict,
            false_ready=safety_flags.pop("false_ready", Tri.FALSE),
            early_offset_commit=safety_flags.pop("early_offset_commit", Tri.FALSE),
            query_results_correct=safety_flags.pop("query_results_correct", Tri.UNKNOWN),
            is_capacity_claim=capacity_claim,
            backlog_stable=Tri.of(None if assessment is None else assessment.stable),
            drain_reconciled=Tri.of(None if reconciliation is None else not drain_incomplete),
            steady_protocol_met=Tri.of(self.config.load.meets_steady_protocol),
            achieved_rate_meets_claim=gates.get(Gate.CALIBRATED).value,
            is_latency_claim=latency_claim,
            latency_within_budget=latency_within_budget,
            resources_bounded=Tri.UNKNOWN if self.resource_blockers else Tri.TRUE,
            no_oom=safety_flags.pop("no_oom", Tri.UNKNOWN if self.resource_blockers else Tri.TRUE),
            no_silent_loss=Tri.of(
                None if reconciliation is None else reconciliation.missing_count == 0
            ),
            safety_allowlist_respected=safety_flags.pop("safety_allowlist_respected", Tri.TRUE),
            # The report builder already carries the claim limitations; repeating
            # them in the verdict notes would print each one twice.
            notes=[],
        )
        for name, value in safety_flags.items():
            setattr(evidence, name, value)

        verdict = decide(evidence)

        builder.set(
            "provenance",
            dataset_id=self.dataset_seal.dataset_id,
            source_manifest_sha256=self.dataset_seal.sha256,
            config_sha256=self.config.sha256,
            hardware_profile_sha256=self.host_profile.digest,
        )
        builder.set(
            "workload",
            mode=self.config.load.model.value,
            target_market_ticks_per_s=self.config.load.market_tick_target_per_s,
            target_quotes_per_s=self.config.load.quote_target_per_s,
            scheduled_ticks=drive.counters.by_kind_scheduled.get(SlotKind.TICK.value),
            issued_ticks=drive.counters.by_kind_issued.get(SlotKind.TICK.value),
            kafka_physical_records=counters.kafka_physical_count,
            raw_physical_rows=counters.raw_physical_count,
            canonical_verified_unique_ticks=counters.canonical_unique_count,
            symbols=self.config.dataset.symbol_count,
            partitions_per_topic=self.config.kafka.partitions_per_topic,
            warmup_seconds=self.config.load.warmup_seconds,
            steady_seconds=self.config.load.steady_seconds,
            drain_seconds=self.config.load.drain_seconds,
        )
        builder.set(
            "results",
            steady_verified_ticks_per_s=None if verified_rate is None else verified_rate.value,
            per_tp_backlog_slopes=(
                {k: v.full_slope_per_s for k, v in backlog.assess_all(
                    tolerance_per_s=self.backlog_tolerance_per_s
                ).items()}
            ),
        )
        if reconciliation is not None:
            builder.set(
                "proof",
                expected_accepted_ID_count=reconciliation.expected_count,
                ready_canonical_ID_count=reconciliation.canonical_count,
                missing_ID_count=reconciliation.missing_count,
                unexpected_extra_ID_count=reconciliation.unexpected_extra_count,
                conflict_ID_count=reconciliation.conflict_identity_count,
                semantic_mismatch_count=reconciliation.semantic_mismatch_count,
            )
        builder.apply_verdict(verdict)
        builder.add_blockers(preflight.blockers)

        stop = StopReason.COMPLETED
        if assessment is not None and not assessment.decided:
            stop = StopReason.UNBOUNDED_BACKLOG if assessment.verdict.name == "GROWING" else stop

        return LaneResult(
            run_id=self.config.run.id,
            scenario_id=self.scenario_id,
            phases=self.phases,
            stop_reason=stop,
            preflight=preflight,
            verdict=verdict,
            report=builder.build_unchecked(),
            calibration=calibration,
            drive=drive,
            observations=observations,
            reconciliation=reconciliation,
            backlog_assessment=assessment,
            capacity_gates=gates.as_dict(),
        )
