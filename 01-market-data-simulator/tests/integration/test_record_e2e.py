"""End-to-end record runs (E2E-01, E2E-09, E2E-13 and the GO/NO-GO counters).

These drive the real engine with real asyncio, real files and real fsyncs. The
assertions are the ones section 12.1 makes release-blocking: identical bytes for
identical input, counters that reconcile, and a COMPLETED marker that only ever
appears on a committed run.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from conftest import read_events, sequences_by_symbol
from market_sim.engine import RunState, run_simulation
from market_sim.recorder import COMPLETED_NAME, EVENTS_NAME, EVENTS_PART_NAME
from market_sim.verify import Verdict, verify_run

pytestmark = pytest.mark.asyncio


async def record(make_config, **kwargs):
    kwargs.setdefault("mode", "record")
    kwargs.setdefault("rate", 1_000_000)
    cfg = make_config(**kwargs)
    return cfg, await run_simulation(cfg)


class TestE2E01GoldenDataset:
    """Three symbols, fixed seed, 10k slots, no injection, run twice."""

    async def test_two_equivalent_runs_produce_identical_bytes(self, make_config):
        cfg_a, first = await record(make_config, count=10_000)
        cfg_b, second = await record(make_config, count=10_000)
        assert first.ok and second.ok
        assert cfg_a.dataset_id == cfg_b.dataset_id
        a = (first.run_dir / EVENTS_NAME).read_bytes()
        b = (second.run_dir / EVENTS_NAME).read_bytes()
        assert hashlib.sha256(a).hexdigest() == hashlib.sha256(b).hexdigest()
        assert a == b

    async def test_the_run_report_hash_matches_the_file_on_disk(self, make_config):
        _, result = await record(make_config, count=5_000)
        on_disk = hashlib.sha256((result.run_dir / EVENTS_NAME).read_bytes()).hexdigest()
        assert result.report["file"]["sha256"] == on_disk
        assert result.report["file"]["commit"]["events_sha256"] == on_disk

    async def test_exactly_the_requested_slot_count_with_no_anomalies(self, make_config):
        _, result = await record(make_config, count=10_000)
        events = read_events(result.run_dir / EVENTS_NAME)
        assert len(events) == 10_000
        for symbol, seqs in sequences_by_symbol(events).items():
            assert seqs == list(range(1, len(seqs) + 1)), symbol

    async def test_verifier_agrees_it_is_complete(self, make_config):
        _, result = await record(make_config, count=10_000)
        verdict = verify_run(result.run_dir)
        assert verdict.verdict is Verdict.COMPLETE, verdict.reasons
        assert verdict.totals["gaps"] == 0
        assert verdict.totals["duplicates"] == 0
        assert verdict.totals["out_of_order"] == 0
        assert verdict.totals["event_time_regressions"] == 0

    async def test_symbol_weights_are_honoured_exactly(self, make_config):
        _, result = await record(make_config, count=60_000)
        counts = {s: len(v) for s, v in sequences_by_symbol(read_events(result.run_dir / EVENTS_NAME)).items()}
        assert counts == {"EURUSD": 30_000, "GBPUSD": 20_000, "USDJPY": 10_000}

    async def test_artifacts_and_commit_order(self, make_config):
        _, result = await record(make_config, count=100)
        run = result.run_dir
        assert (run / "manifest.json").is_file()
        assert (run / EVENTS_NAME).is_file()
        assert (run / "report.json").is_file()
        assert (run / COMPLETED_NAME).is_file()
        assert not (run / EVENTS_PART_NAME).exists()

    async def test_manifest_records_everything_needed_to_reproduce(self, make_config):
        # NFR-10 / case C02: seed, t0, config hash and runtime must be captured.
        cfg, result = await record(make_config, count=100)
        manifest = json.loads((result.run_dir / "manifest.json").read_text())
        assert manifest["seed"] == 20261030
        assert manifest["logical_start_time_ns"] == 1793322000000000000
        assert manifest["dataset_id"] == cfg.dataset_id
        assert manifest["config_sha256"] == cfg.config_sha256
        assert manifest["runtime"]["python_version"]
        assert manifest["runtime"]["generator_version"]
        assert any("Synthetic data" in note for note in manifest["notes"])


class TestCountsAndEdges:
    async def test_zero_count_produces_a_complete_empty_dataset(self, make_config):
        # Case T03: "complete and empty" is a real outcome, not a failure.
        _, result = await record(make_config, count=0)
        assert result.ok
        assert (result.run_dir / EVENTS_NAME).read_bytes() == b""
        assert result.report["counts"]["recorded_events"] == 0
        assert verify_run(result.run_dir).verdict is Verdict.COMPLETE

    @pytest.mark.parametrize("count", [1, 2, 3])
    async def test_tiny_runs(self, make_config, count):
        _, result = await record(make_config, count=count)
        assert result.ok
        assert len(read_events(result.run_dir / EVENTS_NAME)) == count

    async def test_odd_count_is_exact(self, make_config):
        # E2E-09 without faults: logical_slots must equal the request exactly.
        _, result = await record(make_config, count=1001)
        assert result.report["counts"]["logical_slots"] == 1001
        assert result.report["counts"]["emitted_events"] == 1001
        assert len(read_events(result.run_dir / EVENTS_NAME)) == 1001

    async def test_duration_bounded_run_is_also_deterministic(self, make_config):
        # duration_s is resolved to a slot count up front, so a duration run is
        # as reproducible as a count run.
        cfg_a = make_config(mode="record", count=None, duration_s=0.01, rate=100_000)
        a = await run_simulation(cfg_a)
        cfg_b = make_config(mode="record", count=None, duration_s=0.01, rate=100_000)
        b = await run_simulation(cfg_b)
        assert a.ok and b.ok
        assert (a.run_dir / EVENTS_NAME).read_bytes() == (b.run_dir / EVENTS_NAME).read_bytes()
        assert a.report["counts"]["logical_slots"] == 1000

    async def test_counters_reconcile_on_a_clean_run(self, make_config):
        # GO/NO-GO item 5: slots, emitted, faults, recorded must all tie up.
        _, result = await record(make_config, count=5_000)
        counts = result.report["counts"]
        assert counts["reconciles"] is True
        assert counts["logical_slots"] == counts["emitted_events"] == 5_000
        assert counts["recorded_events"] == 5_000
        assert counts["intentional_omissions"] == 0
        assert counts["intentional_duplicates"] == 0

    async def test_report_never_claims_tcp_delivery_in_record_mode(self, make_config):
        _, result = await record(make_config, count=100)
        assert result.report["tcp"] == {"enabled": False}
        assert result.report["metrics"]["counters"]["tcp_events_enqueued_total"] == 0

    async def test_report_discloses_the_durability_boundary(self, make_config):
        # NFR-08: flush is not durability, and the report has to say so.
        _, result = await record(make_config, count=100, fsync_every_events=0)
        note = result.report["file"]["durability_note"]
        assert "fsynced" in note and "power loss" in note
        assert result.report["known_limitations"]

    async def test_single_symbol_run(self, make_config):
        from conftest import symbol_dict

        cfg = make_config(
            mode="record",
            rate=1_000_000,
            count=500,
            order=["EURUSD"],
            symbols={"EURUSD": symbol_dict()},
        )
        result = await run_simulation(cfg)
        assert result.ok
        events = read_events(result.run_dir / EVENTS_NAME)
        assert [e["sequence"] for e in events] == list(range(1, 501))


class TestFailureModes:
    async def test_a_run_directory_collision_fails_before_producing(self, make_config):
        cfg = make_config(mode="record", count=100, rate=1_000_000)
        first = await run_simulation(cfg, run_instance_id="run-fixed")
        assert first.ok
        second = await run_simulation(cfg, run_instance_id="run-fixed")
        assert second.state is RunState.FAILED
        assert second.exit_code == 3
        assert second.error.code == "OUTPUT_RUN_DIR_EXISTS"

    async def test_total_byte_budget_stops_the_run_without_a_marker(self, make_config):
        # Case F11 end to end: abort before the volume fills, and leave no
        # COMPLETED marker behind.
        cfg = make_config(
            mode="record", count=100_000, rate=1_000_000, max_total_bytes=20_000
        )
        result = await run_simulation(cfg)
        assert result.state is RunState.FAILED
        assert result.exit_code == 3
        assert result.error.code == "OUTPUT_BUDGET_EXCEEDED"
        assert not (result.run_dir / COMPLETED_NAME).exists()
        assert (result.run_dir / EVENTS_PART_NAME).exists()
        assert not (result.run_dir / EVENTS_NAME).exists()

    async def test_a_failed_run_still_writes_a_report_with_the_error(self, make_config):
        cfg = make_config(
            mode="record", count=100_000, rate=1_000_000, max_total_bytes=20_000
        )
        result = await run_simulation(cfg)
        report = json.loads((result.run_dir / "report.json").read_text())
        assert report["status"] == "FAILED"
        assert report["error"]["code"] == "OUTPUT_BUDGET_EXCEEDED"
        assert "OUTPUT_BUDGET_EXCEEDED" in report["exit_reason"]

    async def test_the_verifier_calls_a_budget_abort_incomplete(self, make_config):
        cfg = make_config(
            mode="record", count=100_000, rate=1_000_000, max_total_bytes=20_000
        )
        result = await run_simulation(cfg)
        verdict = verify_run(result.run_dir)
        assert verdict.verdict is Verdict.INCOMPLETE
        assert any(".part" in reason for reason in verdict.reasons)

    async def test_scheduler_lag_fail_fast_policy(self, make_config):
        # Case T04: with fail-fast selected, an impossible rate must stop the run
        # rather than quietly under-produce and still report success.
        cfg = make_config(
            mode="record",
            count=2_000_000,
            rate=5_000_000,
            max_scheduler_lag_ms=1,
            on_scheduler_lag="fail-fast",
            max_burst_slots=100,
        )
        result = await run_simulation(cfg)
        assert result.state is RunState.FAILED
        assert result.error.code == "SCHEDULER_LAG_EXCEEDED"
        assert not (result.run_dir / COMPLETED_NAME).exists()

    async def test_scheduler_lag_continue_degraded_reports_the_shortfall(self, make_config):
        # The default: throughput falls, the lag is recorded, and no slot is
        # silently dropped to flatter the clock.
        cfg = make_config(
            mode="record",
            count=20_000,
            rate=5_000_000,
            max_scheduler_lag_ms=1,
            on_scheduler_lag="continue-degraded",
        )
        result = await run_simulation(cfg)
        assert result.ok
        assert result.report["counts"]["logical_slots"] == 20_000
        assert result.report["metrics"]["counters"]["scheduler_lag_breach_total"] > 0
        assert result.report["p95_scheduler_lag_ms"] > 0
        assert result.report["rates"]["actual_generated_rate_per_sec"] < 5_000_000
