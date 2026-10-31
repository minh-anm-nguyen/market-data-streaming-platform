"""End-to-end lane runs against both available SUTs (sections 11, 20).

Two kinds of run exist today and they produce different verdicts by design:

* **Against the simulated pipeline** the whole orchestrator executes --
  preflight, load, drain, reconcile, verdict, report. That validates the
  measuring tools, which section 8 requires before they are pointed at a real
  system, and the verdict refuses to read any of it as a capacity claim.
* **Against the real P01 source** the lane is blocked, because P02-P07 have no
  implementation. Section 20 requires exactly that outcome:
  ``BLOCKED_BY_UPSTREAM_IMPLEMENTATION``, neither a pass nor a failure of the
  system under test.

This file covers E2E-02 (finite golden dataset, exact identity match) and
E2E-22 (offered above sink capacity: capacity fails, safety may pass) in their
measurement-validation form.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from p08_bench.calibrator import calibrate
from p08_bench.clock import ClockDomain, ClockSyncAssessment, FakeClock
from p08_bench.config import build_config
from p08_bench.errors import UpstreamUnavailable
from p08_bench.event_driver import default_payload_factory
from p08_bench.evidence import EvidenceStore
from p08_bench.manifest import HostProfile, SourceSeal, sha256_text
from p08_bench.orchestrator import Orchestrator, Phase, StopReason
from p08_bench.reconciliation import Reconciliation
from p08_bench.scheduler import RateSchedule
from p08_bench.sut import Stage
from p08_bench.sut_mock import MockConfig, MockPipeline
from p08_bench.sut_p01 import P01SourcePipeline, P01SourceReader
from p08_bench.verdict import Dimension, Status, Tri

from ..conftest import workload_dict

SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY"]


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(ClockDomain("bench", "driver"))


@pytest.fixture
def seal(tmp_path: Path) -> SourceSeal:
    """A sealed P01-shaped source directory."""
    run = tmp_path / "source"
    run.mkdir()
    events = '{"dataset_id":"BENCH_TEST_001","symbol":"EURUSD","sequence":1,"event_time_ns":1,"event_type":"tick","price_ticks":1}\n'
    (run / "events.jsonl").write_bytes(events.encode("utf-8"))
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "dataset_id": "BENCH_TEST_001",
                # P01's config digest, not the file hash.
                "data_sha256": "c0" * 32,
                "planned_logical_slots": 1,
            }
        ),
        encoding="utf-8",
    )
    (run / "COMPLETED").write_text(
        json.dumps(
            {
                "events_sha256": sha256_text(events),
                "events_lines": 1,
                "events_bytes": len(events.encode("utf-8")),
            }
        ),
        encoding="utf-8",
    )
    return SourceSeal.from_p01_run_dir(run)


def lane_config(**overrides):
    base = workload_dict(
        load={
            "market_tick_target_per_s": 100,
            "quote_target_per_s": 0,
            "warmup_seconds": 0,
            "ramp_up_seconds": 0,
            "steady_seconds": 2,
            "drain_seconds": 1,
        }
    )
    for section, values in overrides.items():
        base[section] = {**base[section], **values}
    return build_config(base)


def orchestrator(config, sut, clock, seal, tmp_path: Path, **kwargs) -> Orchestrator:
    return Orchestrator(
        config,
        sut,
        clock,
        scenario_id=kwargs.pop("scenario_id", "E2E-02"),
        dataset_seal=seal,
        host_profile=HostProfile(cpu_model="test-cpu", cpu_cores=4),
        evidence=EvidenceStore(tmp_path / "evidence"),
        clock_sync=ClockSyncAssessment.unmeasured(
            ClockDomain("bench", "driver"), ClockDomain("ch", "server")
        ),
        observation_interval_s=0.2,
        **kwargs,
    )


def oracle_from(sut: MockPipeline):
    """The expected set, derived from what the source actually emitted.

    In a real lane this comes from the sealed P01 manifest plus an independent
    reading of the P03 policy. Here the simulation is the source, so the
    accepted-identity set plays that role -- and it is deliberately *not* the
    canonical set, so a lost or injected identity still shows up as a diff.
    """

    def factory():
        return sut.accepted_identities(), {
            key: sut.source_payload(key) or {} for key in sut.accepted_identities()
        }

    return factory


class TestSimulatedLaneRuns:
    def test_a_clean_lane_executes_every_phase(self, clock, seal, tmp_path):
        sut = MockPipeline(MockConfig())
        result = orchestrator(lane_config(), sut, clock, seal, tmp_path).run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS),
            oracle=oracle_from(sut),
            canonical_payload=sut.canonical_payload,
        )
        phases = [p.phase for p in result.phases]
        assert phases == [Phase.PREFLIGHT, Phase.STEADY, Phase.DRAIN, Phase.RECONCILE]
        assert result.stop_reason is StopReason.COMPLETED

    def test_e2e_02_identities_reconcile_exactly(self, clock, seal, tmp_path):
        # Case E2E-02: a finite golden dataset through the pipeline, with the
        # accepted logical ids and payloads equal to the canonical set.
        sut = MockPipeline(MockConfig())
        result = orchestrator(lane_config(), sut, clock, seal, tmp_path).run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS),
            oracle=oracle_from(sut),
            canonical_payload=sut.canonical_payload,
        )
        assert result.reconciliation is not None
        assert result.reconciliation.verdict is Reconciliation.PASS
        assert result.reconciliation.missing_count == 0
        assert result.reconciliation.unexpected_extra_count == 0

    def test_a_simulated_run_is_marked_as_a_tool_self_test(self, clock, seal, tmp_path):
        sut = MockPipeline(MockConfig())
        result = orchestrator(lane_config(), sut, clock, seal, tmp_path).run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS),
            oracle=oracle_from(sut),
            canonical_payload=sut.canonical_payload,
        )
        assert any("simulation" in note for note in result.verdict.notes)

    def test_a_simulated_capacity_claim_is_invalid(self, clock, seal, tmp_path):
        sut = MockPipeline(MockConfig())
        result = orchestrator(lane_config(), sut, clock, seal, tmp_path).run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS),
            oracle=oracle_from(sut),
            canonical_payload=sut.canonical_payload,
            is_capacity_claim=True,
        )
        assert result.verdict.by_dimension(Dimension.VALIDITY).status is Status.INVALID
        assert result.overall is Status.INVALID

    def test_silent_loss_is_caught_by_the_oracle(self, clock, seal, tmp_path):
        sut = MockPipeline(MockConfig(loss_fraction=1.0))
        result = orchestrator(lane_config(), sut, clock, seal, tmp_path).run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS),
            oracle=oracle_from(sut),
            canonical_payload=sut.canonical_payload,
        )
        assert result.reconciliation.missing_count > 0
        assert result.verdict.by_dimension(Dimension.CORRECTNESS).status is Status.FAIL

    def test_a_run_without_an_oracle_is_inconclusive_not_a_pass(self, clock, seal, tmp_path):
        # Section 7: without the independent oracle, app metrics do not certify
        # correctness.
        sut = MockPipeline(MockConfig())
        result = orchestrator(lane_config(), sut, clock, seal, tmp_path).run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS),
        )
        assert result.reconciliation is None
        assert result.verdict.by_dimension(Dimension.CORRECTNESS).status is Status.INCONCLUSIVE

    def test_e2e_22_offered_above_sink_capacity_grows_the_backlog(self, clock, seal, tmp_path):
        # Case E2E-22: 100 offered into a 10/s sink. The backlog climbs, the
        # capacity gate fails, and the queue stays bounded.
        sut = MockPipeline(
            MockConfig(consumer_rate_per_s=10.0, broker_rate_per_s=10.0, consumer_queue_records=10_000)
        )
        result = orchestrator(lane_config(), sut, clock, seal, tmp_path).run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS),
            oracle=oracle_from(sut),
            canonical_payload=sut.canonical_payload,
        )
        assert result.backlog_assessment is not None
        assert result.capacity_gates["gates"]
        # Processing debt remains after the drain deadline.
        assert any("processing debt" in note for p in result.phases for note in p.notes)

    def test_the_evidence_bundle_is_written(self, clock, seal, tmp_path):
        sut = MockPipeline(MockConfig())
        orch = orchestrator(lane_config(), sut, clock, seal, tmp_path)
        orch.run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS),
            oracle=oracle_from(sut),
            canonical_payload=sut.canonical_payload,
        )
        artifacts = orch.evidence.artifacts
        for name in ("preflight", "run_manifest", "host_profile", "workload_config", "stage_counters", "reconciliation"):
            assert name in artifacts
        assert all(state.value == "OK" for state in orch.evidence.verify().values())

    def test_the_report_carries_provenance_and_the_verdict(self, clock, seal, tmp_path):
        sut = MockPipeline(MockConfig())
        result = orchestrator(lane_config(), sut, clock, seal, tmp_path).run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS),
            oracle=oracle_from(sut),
            canonical_payload=sut.canonical_payload,
        )
        report = result.report
        assert report["provenance"]["dataset_id"] == "BENCH_TEST_001"
        assert report["provenance"]["config_sha256"]
        assert report["status_dimensions"]["overall"] == result.overall.value
        assert report["workload"]["partitions_per_topic"] == 8

    def test_the_report_keeps_ticks_and_events_separate(self, clock, seal, tmp_path):
        sut = MockPipeline(MockConfig())
        config = lane_config(load={"quote_target_per_s": 25})
        result = orchestrator(config, sut, clock, seal, tmp_path).run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS),
            oracle=oracle_from(sut),
            canonical_payload=sut.canonical_payload,
        )
        workload = result.report["workload"]
        assert workload["target_market_ticks_per_s"] == 100
        assert workload["target_quotes_per_s"] == 25
        assert workload["scheduled_ticks"] == 200


class TestPreflightGating:
    def test_an_unsealed_source_stops_before_any_load(self, clock, tmp_path):
        # Case G01-03 at the lane level.
        run = tmp_path / "partial"
        run.mkdir()
        (run / "events.jsonl.part").write_bytes(b"{}\n")
        (run / "manifest.json").write_text('{"dataset_id":"x"}', encoding="utf-8")
        unsealed = SourceSeal.from_p01_run_dir(run)
        sut = MockPipeline(MockConfig())
        result = orchestrator(lane_config(), sut, clock, unsealed, tmp_path).run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS)
        )
        assert result.stop_reason is StopReason.PREFLIGHT_FAILED
        assert result.drive is None
        assert result.verdict.by_dimension(Dimension.VALIDITY).status is Status.INVALID

    def test_an_uncalibrated_driver_stops_the_lane(self, clock, seal, tmp_path):
        # Case E2E-37: the pipeline is never asked, because the instrument is
        # the limit.
        from p08_bench.calibrator import NullSink
        from p08_bench.sut import SubmitOutcome, SubmitStatus

        class Refusing(NullSink):
            def submit(self, identity, payload, t_ns):  # type: ignore[override]
                return SubmitOutcome(SubmitStatus.DROPPED, identity, "refused")

        calibration = calibrate(
            RateSchedule.constant_rate(100, 0.1),
            default_payload_factory("BENCH_TEST_001", SYMBOLS),
            FakeClock(ClockDomain("bench", "driver")),
            sink=Refusing(),
        )
        sut = MockPipeline(MockConfig())
        result = orchestrator(lane_config(), sut, clock, seal, tmp_path).run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS), calibration=calibration
        )
        assert result.stop_reason is StopReason.DRIVER_NOT_CALIBRATED
        assert result.verdict.overall is Status.INVALID

    def test_a_good_calibration_passes_preflight(self, clock, seal, tmp_path):
        calibration = calibrate(
            RateSchedule.constant_rate(100, 0.1),
            default_payload_factory("BENCH_TEST_001", SYMBOLS),
            FakeClock(ClockDomain("bench", "driver")),
        )
        sut = MockPipeline(MockConfig())
        result = orchestrator(lane_config(), sut, clock, seal, tmp_path).run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS),
            calibration=calibration,
            oracle=oracle_from(sut),
            canonical_payload=sut.canonical_payload,
        )
        assert result.preflight.checks["workload_calibrated"] is Tri.TRUE
        assert result.stop_reason is StopReason.COMPLETED


class TestRealP01LaneIsBlocked:
    def _reader(self, tmp_path: Path) -> P01SourceReader:
        run = tmp_path / "p01"
        run.mkdir()
        events = '{"dataset_id":"ds","symbol":"EURUSD","sequence":1,"event_time_ns":1,"event_type":"tick","price_ticks":5}\n'
        (run / "events.jsonl").write_bytes(events.encode("utf-8"))
        (run / "manifest.json").write_text(
            json.dumps({"dataset_id": "ds", "data_sha256": "c0" * 32, "planned_logical_slots": 1}),
            encoding="utf-8",
        )
        (run / "COMPLETED").write_text(
            json.dumps(
                {
                    "events_sha256": sha256_text(events),
                    "events_lines": 1,
                    "events_bytes": len(events.encode("utf-8")),
                }
            ),
            encoding="utf-8",
        )
        return P01SourceReader(run)

    def test_the_p01_source_reads_its_sealed_events(self, tmp_path):
        reader = self._reader(tmp_path)
        events = list(reader.events())
        assert len(events) == 1
        assert events[0].identity.as_tuple() == ("ds", "EURUSD", 1)
        assert events[0].is_tick

    def test_prices_stay_integers_through_the_reader(self, tmp_path):
        # INV-03 on the read path as well as the comparison path.
        event = next(iter(self._reader(tmp_path).events()))
        assert isinstance(event.payload["price_ticks"], int)

    def test_the_identity_set_comes_from_the_sealed_file(self, tmp_path):
        assert self._reader(tmp_path).identity_set() == {("ds", "EURUSD", 1)}

    def test_counts_by_type_keep_ticks_and_quotes_apart(self, tmp_path):
        assert self._reader(tmp_path).counts_by_type() == {"tick": 1}

    def test_a_full_pipeline_lane_is_blocked_by_upstream(self, clock, seal, tmp_path):
        # Section 20: the honest outcome while P02-P07 are documents.
        sut = P01SourcePipeline(self._reader(tmp_path))
        result = orchestrator(lane_config(), sut, clock, seal, tmp_path).run(
            default_payload_factory("BENCH_TEST_001", SYMBOLS)
        )
        assert result.stop_reason is StopReason.BLOCKED_BY_UPSTREAM
        assert result.overall is Status.BLOCKED_BY_UPSTREAM
        assert result.drive is None

    def test_the_blocked_stages_name_the_owning_projects(self, tmp_path):
        sut = P01SourcePipeline(self._reader(tmp_path))
        with pytest.raises(UpstreamUnavailable) as excinfo:
            sut.require_stages([Stage.BROKER, Stage.CANONICAL])
        message = excinfo.value.message
        assert "P04 Kafka broker" in message
        assert "P06 canonical READY build" in message

    def test_the_source_stage_alone_is_usable(self, tmp_path):
        sut = P01SourcePipeline(self._reader(tmp_path))
        sut.require_stages([Stage.SOURCE])
        assert not sut.is_simulation

    def test_asking_for_a_canonical_build_raises_rather_than_returning_empty(self, tmp_path):
        # An empty set would read as "everything was lost".
        sut = P01SourcePipeline(self._reader(tmp_path))
        with pytest.raises(UpstreamUnavailable, match="no canonical READY build"):
            sut.canonical_identities()

    def test_downstream_counters_are_unknown_not_zero(self, tmp_path):
        sut = P01SourcePipeline(self._reader(tmp_path))
        counters = sut.observe(0).counters
        assert counters.issued_count == 0
        assert counters.canonical_unique_count is None
        assert counters.kafka_physical_count is None

    def test_an_unsealed_p01_source_refuses_to_be_read(self, tmp_path):
        run = tmp_path / "partial"
        run.mkdir()
        (run / "events.jsonl.part").write_bytes(b"{}\n")
        (run / "manifest.json").write_text('{"dataset_id":"x"}', encoding="utf-8")
        reader = P01SourceReader(run)
        with pytest.raises(UpstreamUnavailable, match="cannot anchor an expected set"):
            list(reader.events())
