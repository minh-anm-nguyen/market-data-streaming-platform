"""Backpressure, shutdown and background-task failure (E2E-15, cases R04-R11).

Section 5.4's claim is that "survives" means safe, bounded and observable -- not
"keeps serving". So these tests check that a slow disk slows the producer instead
of growing memory, that a stop request drains within its deadline or fails
loudly, and that no background exception is ever swallowed.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from conftest import free_port
from market_sim.engine import RunState, SimulatorEngine, run_simulation
from market_sim.recorder import COMPLETED_NAME, EVENTS_NAME

pytestmark = pytest.mark.chaos


@pytest.fixture
def slow_writes(monkeypatch):
    """Make every file write block, the way a slow or contended disk would."""

    def _apply(delay_s: float):
        import market_sim.recorder as recorder_module

        real = recorder_module.JsonlRecorder._write_batch_bytes

        def slow(self, lines):
            time.sleep(delay_s)
            return real(self, lines)

        monkeypatch.setattr(recorder_module.JsonlRecorder, "_write_batch_bytes", slow)

    return _apply


@pytest.fixture
def slow_sink(monkeypatch):
    """Make the file writer slow in a way that still yields to the event loop.

    Distinct from :func:`slow_writes`: a blocking write stalls the producer too,
    so no backlog ever forms. To test the *drain deadline* a backlog has to
    exist, which means the writer must be slow while the loop keeps running --
    which is what a genuinely slow device looks like through non-blocking IO.
    """

    def _apply(delay_s: float, batch: int):
        from market_sim.queues import BoundedQueue

        real = BoundedQueue.get_batch

        async def slow_get(self, max_items, max_bytes=None):
            if self.name == "file_queue":
                await asyncio.sleep(delay_s)
                return await real(self, min(max_items, batch), max_bytes)
            return await real(self, max_items, max_bytes)

        monkeypatch.setattr(BoundedQueue, "get_batch", slow_get)

    return _apply


class TestFileBackpressure:
    async def test_a_slow_disk_bounds_the_queue_instead_of_growing_it(
        self, make_config, slow_writes
    ):
        # Cases F13 / R01: the producer waits, memory does not grow.
        slow_writes(0.002)
        cfg = make_config(
            mode="record",
            count=4_000,
            rate=1_000_000,
            max_queue_events=20,
            max_queue_bytes=65_536,
        )
        result = await run_simulation(cfg)
        assert result.ok
        queue = result.report["file"]["queue"]
        assert queue["peak_depth"] <= 20
        assert queue["peak_bytes"] <= 65_536
        # Every slot still arrived: slowdown, never a silent drop.
        assert result.report["counts"]["recorded_events"] == 4_000

    async def test_the_shortfall_is_reported_not_hidden(self, make_config, slow_writes):
        # Case T04: the measured rate must reflect reality.
        slow_writes(0.002)
        cfg = make_config(
            mode="record", count=2_000, rate=1_000_000, max_queue_events=20
        )
        result = await run_simulation(cfg)
        assert result.ok
        assert result.report["rates"]["actual_generated_rate_per_sec"] < 1_000_000
        assert result.report["p95_scheduler_lag_ms"] >= 0

    async def test_a_drain_that_misses_its_deadline_fails_the_run(
        self, make_config, slow_sink
    ):
        # Case R04: DRAINING has a deadline. Missing it is FAILED with a pending
        # count, never a COMPLETED marker.
        slow_sink(0.05, batch=100)
        cfg = make_config(
            mode="record",
            count=3_000,
            rate=1_000_000,
            max_queue_events=50_000,
            max_queue_bytes=64 * 1024 * 1024,
            shutdown_drain_timeout_s=0.1,
        )
        result = await run_simulation(cfg)
        assert result.state is RunState.FAILED
        assert result.exit_code == 6
        assert result.error.code == "DRAIN_TIMEOUT"
        assert result.error.context["pending_events"] > 0
        assert not (result.run_dir / COMPLETED_NAME).exists()
        assert not (result.run_dir / EVENTS_NAME).exists()


class TestStopRequest:
    async def test_a_stop_request_drains_and_completes(self, make_config):
        # NFR-05: stop generating, drain within the deadline, report honestly.
        cfg = make_config(mode="record", count=2_000_000, rate=50_000)
        engine = SimulatorEngine(cfg)
        task = asyncio.create_task(engine.run())
        await asyncio.sleep(0.3)
        engine.request_stop("test:stop")
        result = await asyncio.wait_for(task, 60)

        assert result.ok
        assert result.state is RunState.COMPLETED
        assert result.report["exit_reason"] == "test:stop"
        # It stopped early, so it produced fewer slots than planned, and says so.
        slots = result.report["counts"]["logical_slots"]
        assert 0 < slots < 2_000_000
        assert result.report["planned_logical_slots"] == 2_000_000
        assert result.report["counts"]["recorded_events"] == slots

    async def test_a_stopped_run_is_still_internally_consistent(self, make_config):
        from market_sim.verify import Verdict, verify_run

        cfg = make_config(mode="record", count=2_000_000, rate=50_000)
        engine = SimulatorEngine(cfg)
        task = asyncio.create_task(engine.run())
        await asyncio.sleep(0.3)
        engine.request_stop("test:stop")
        result = await asyncio.wait_for(task, 60)
        verdict = verify_run(result.run_dir)
        # Falling short of the plan is a warning, not a contradiction.
        assert verdict.verdict is Verdict.COMPLETE, verdict.reasons
        assert verdict.lines == result.report["counts"]["recorded_events"]
        assert any("planned slots" in w for w in verdict.warnings)

    async def test_request_stop_is_idempotent(self, make_config):
        cfg = make_config(mode="record", count=1_000_000, rate=50_000)
        engine = SimulatorEngine(cfg)
        task = asyncio.create_task(engine.run())
        await asyncio.sleep(0.2)
        engine.request_stop("first")
        engine.request_stop("second")
        result = await asyncio.wait_for(task, 60)
        assert result.report["exit_reason"] == "first"

    async def test_stopping_with_a_slow_client_attached_does_not_deadlock(
        self, make_config
    ):
        # E2E-15 proper: a client that never reads, a bounded TCP queue, and a
        # stop request arriving in the middle of it.
        engine = task = None
        for _ in range(6):
            cfg = make_config(
                mode="both",
                count=2_000_000,
                rate=200_000,
                tcp_port=free_port(),
                wait_for_client=True,
                startup_timeout_s=3.0,
                max_queue_events=500,
                tcp_max_queue_events=20,
                send_timeout_ms=100,
                shutdown_drain_timeout_s=20.0,
            )
            engine = SimulatorEngine(cfg)
            task = asyncio.create_task(engine.run())
            listening = asyncio.create_task(engine.listening.wait())
            done, _pending = await asyncio.wait(
                {task, listening}, timeout=20, return_when=asyncio.FIRST_COMPLETED
            )
            if listening in done:
                break
            listening.cancel()
            failed = task.result().error if task in done else None
            if failed is not None and failed.code == "OUTPUT_PORT_BIND_FAILED":
                continue
            raise AssertionError(f"engine did not start listening: {failed}")

        reader, writer = await asyncio.open_connection("127.0.0.1", engine.bound_port)
        await asyncio.sleep(0.3)
        engine.request_stop("test:stop-under-pressure")
        result = await asyncio.wait_for(task, 60)
        writer.close()

        assert result.ok, result.report.get("error")
        counts = result.report["counts"]
        assert counts["recorded_events"] == counts["logical_slots"] > 0
        assert result.report["metrics"]["counters"]["tcp_queue_overflow_total"] > 0


class TestBackgroundTaskFailure:
    async def test_an_unexpected_writer_exception_fails_the_run_loudly(
        self, make_config, monkeypatch
    ):
        # Case R05: no "Task exception was never retrieved", no bogus success.
        import market_sim.recorder as recorder_module

        def explode(self, lines):
            raise RuntimeError("simulated bug in the writer")

        monkeypatch.setattr(recorder_module.JsonlRecorder, "_write_batch_bytes", explode)
        cfg = make_config(mode="record", count=50_000, rate=1_000_000)
        result = await run_simulation(cfg)
        assert result.state is RunState.FAILED
        assert result.exit_code == 6
        assert result.error.code == "RUNTIME_RECORDER_FAILED"
        assert "simulated bug in the writer" in result.error.message
        assert not (result.run_dir / COMPLETED_NAME).exists()
        assert result.report["error"]["code"] == "RUNTIME_RECORDER_FAILED"

    async def test_an_oversized_record_is_refused_before_any_queue(self, make_config):
        # Cases D08 / R03: bound the record, never let it reach memory.
        from conftest import symbol_dict

        cfg = make_config(
            mode="record",
            count=100,
            rate=1_000_000,
            max_record_bytes=128,
            max_queue_bytes=65_536,
            order=["EURUSD"],
            symbols={"EURUSD": symbol_dict()},
        )
        result = await run_simulation(cfg)
        assert result.state is RunState.FAILED
        assert result.error.code == "RUNTIME_RECORD_TOO_LARGE"
        assert not (result.run_dir / COMPLETED_NAME).exists()
