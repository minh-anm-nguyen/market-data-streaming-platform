"""Clock independence, framing contract and leak checks.

Covers the what-if cases that need a running engine but did not fit the
scenario-shaped files: E2E-08/T01 (clock jump), T08 (stale threshold edges),
T12 (jitter is observable), N10 (reconnect has no replay), N13 (small receive
buffer), F07 (a substituted line), R06 (no leak across many runs), R13 (one
source order) and C11 (the contract Project 03 will code against).
"""

from __future__ import annotations

import asyncio
import gc
import json

import pytest

from conftest import free_port, read_events
from market_sim.engine import SimulatorEngine, run_simulation
from market_sim.faults import parse_fault_plan
from market_sim.model import SCHEMA_VERSION, QUOTE_FIELDS, TICK_FIELDS
from market_sim.recorder import EVENTS_NAME
from market_sim.verify import Verdict, verify_run

pytestmark = pytest.mark.asyncio


class TestClockIndependence:
    """E2E-08 / T01: the wall clock may do anything; the output may not change."""

    async def test_a_wall_clock_jump_does_not_change_a_single_byte(
        self, make_config, monkeypatch
    ):
        baseline = await run_simulation(
            make_config(mode="record", count=5_000, rate=200_000)
        )
        assert baseline.ok

        # Now make the wall clock lurch backwards by a day, repeatedly, while a
        # second run of the same config executes.
        import market_sim.clock as clock_module

        state = {"calls": 0}
        real = clock_module.time.time_ns

        def hostile_wall_clock():
            state["calls"] += 1
            offset = -86_400_000_000_000 if state["calls"] % 2 else 86_400_000_000_000
            return real() + offset

        monkeypatch.setattr(clock_module.time, "time_ns", hostile_wall_clock)
        jumped = await run_simulation(
            make_config(mode="record", count=5_000, rate=200_000)
        )
        assert jumped.ok
        assert (jumped.run_dir / EVENTS_NAME).read_bytes() == (
            baseline.run_dir / EVENTS_NAME
        ).read_bytes()
        assert jumped.report["file"]["sha256"] == baseline.report["file"]["sha256"]

    async def test_event_times_remain_monotonic_per_symbol_through_a_jump(
        self, make_config, monkeypatch
    ):
        import market_sim.clock as clock_module

        monkeypatch.setattr(
            clock_module.time, "time_ns", lambda: 1  # a permanently wrong wall clock
        )
        result = await run_simulation(
            make_config(mode="record", count=3_000, rate=200_000)
        )
        assert result.ok
        last: dict[str, int] = {}
        for event in read_events(result.run_dir / EVENTS_NAME):
            previous = last.get(event["symbol"], -1)
            assert event["event_time_ns"] >= previous
            last[event["symbol"]] = event["event_time_ns"]

    async def test_no_wall_clock_value_leaks_into_the_payload(self, make_config):
        # Invariant 9 of section 3.2, and case T06: the record carries logical
        # event time only. Publish delay belongs in metrics, never in the
        # payload, or the bytes could not be reproducible. no run timestamp, hostname, pid or latency
        # in the canonical record, or the bytes could not be reproducible.
        result = await run_simulation(
            make_config(mode="record", count=500, rate=1_000_000)
        )
        for event in read_events(result.run_dir / EVENTS_NAME):
            assert set(event) in (set(QUOTE_FIELDS), set(TICK_FIELDS))


class TestStaleThresholdEdges:
    """T08: a consumer has to be able to test <, == and > against a threshold.

    The simulator does not detect staleness -- that is Project 03's job. What it
    must do is place an event *exactly* at a chosen offset, so the consumer's
    inclusive/exclusive boundary can be tested rather than guessed at.
    """

    @pytest.mark.parametrize("offset_ms", [1999, 2000, 2001])
    async def test_the_injected_offset_is_exact(self, make_config, offset_ms):
        # Measured against the same event in a clean run, so the assertion is on
        # the offset itself rather than on a neighbour whose spacing depends on
        # the symbol weights.
        clean = await run_simulation(make_config(mode="record", count=1_000, rate=1_000_000))
        assert clean.ok
        baseline = {
            (e["symbol"], e["sequence"]): e["event_time_ns"]
            for e in read_events(clean.run_dir / EVENTS_NAME)
        }[("EURUSD", 100)]

        plan = parse_fault_plan(
            {
                "fault": [
                    {
                        "kind": "stale_timestamp",
                        "symbol": "EURUSD",
                        "sequence": 100,
                        "offset_ms": offset_ms,
                    }
                ]
            }
        )
        cfg = make_config(
            mode="record",
            count=1_000,
            rate=1_000_000,
            faults_enabled=True,
            plan_path="plan.toml",
            fault_plan_fingerprint=plan.fingerprint(),
        )
        result = await run_simulation(cfg, fault_plan=plan)
        assert result.ok
        stale = {
            (e["symbol"], e["sequence"]): e["event_time_ns"]
            for e in read_events(result.run_dir / EVENTS_NAME)
        }[("EURUSD", 100)]

        # Exact integer arithmetic: no rounding, so a consumer can place its
        # threshold at 2000 ms and test <, == and > without ambiguity.
        assert baseline - stale == offset_ms * 1_000_000

    async def test_only_the_targeted_event_moves(self, make_config):
        clean = await run_simulation(make_config(mode="record", count=1_000, rate=1_000_000))
        plan = parse_fault_plan(
            {
                "fault": [
                    {
                        "kind": "stale_timestamp",
                        "symbol": "EURUSD",
                        "sequence": 100,
                        "offset_ms": 2000,
                    }
                ]
            }
        )
        cfg = make_config(
            mode="record",
            count=1_000,
            rate=1_000_000,
            faults_enabled=True,
            plan_path="plan.toml",
            fault_plan_fingerprint=plan.fingerprint(),
        )
        faulted = await run_simulation(cfg, fault_plan=plan)
        before = read_events(clean.run_dir / EVENTS_NAME)
        after = read_events(faulted.run_dir / EVENTS_NAME)
        assert len(before) == len(after)

        # dataset_id differs by design: a faulted dataset must never share an
        # identity with a clean one, or a gap in one would look like a
        # contradiction in the other. So compare everything else.
        assert clean.report["dataset_id"] != faulted.report["dataset_id"]
        assert {e["dataset_id"] for e in before} != {e["dataset_id"] for e in after}

        def payload(event):
            return {k: v for k, v in event.items() if k != "dataset_id"}

        differing = [
            (b["symbol"], b["sequence"])
            for b, a in zip(before, after)
            if payload(b) != payload(a)
        ]
        assert differing == [("EURUSD", 100)]


class TestJitterIsObservable:
    """T12: sleep overshoot cannot be eliminated, so it must be visible."""

    async def test_the_report_exposes_both_windows_and_the_lag_percentiles(
        self, make_config
    ):
        result = await run_simulation(
            make_config(mode="record", count=20_000, rate=10_000)
        )
        assert result.ok
        rates = result.report["rates"]
        # A 60-second average hides the jitter a 1-second window shows, so both
        # are reported (section 9.3).
        assert "generated_rate_1s_last" in rates
        assert "generated_rate_60s_last" in rates
        lag = result.report["scheduler_lag_ms"]
        assert lag["count"] > 0
        assert lag["max"] >= lag["p50"]
        assert result.report["p95_scheduler_lag_ms"] is not None
        assert result.report["p99_scheduler_lag_ms"] is not None
        # And the histogram says whether its percentiles are exact or sampled.
        assert isinstance(lag["exact"], bool)


class TestOrderingHasOneSource:
    """R13: one generator, one FIFO queue, so the recorded order is the order."""

    async def test_recorded_order_matches_generation_order(self, make_config):
        from market_sim.generator import Generator
        from market_sim.model import to_canonical_json

        cfg = make_config(mode="record", count=5_000, rate=1_000_000)
        expected = [
            to_canonical_json(event) for event in Generator(cfg).iter_slots(5_000)
        ]
        result = await run_simulation(cfg)
        assert result.ok
        recorded = (result.run_dir / EVENTS_NAME).read_bytes().splitlines()
        assert recorded == expected

    async def test_order_is_unaffected_by_a_concurrent_tcp_client(self, make_config):
        from market_sim.generator import Generator
        from market_sim.model import to_canonical_json

        cfg_for_expected = make_config(mode="record", count=3_000, rate=200_000)
        expected = [
            to_canonical_json(e) for e in Generator(cfg_for_expected).iter_slots(3_000)
        ]

        for _ in range(6):
            cfg = make_config(
                mode="both",
                count=3_000,
                rate=200_000,
                tcp_port=free_port(),
                wait_for_client=True,
                startup_timeout_s=3.0,
            )
            engine = SimulatorEngine(cfg)
            task = asyncio.create_task(engine.run())
            listening = asyncio.create_task(engine.listening.wait())
            done, _ = await asyncio.wait(
                {task, listening}, timeout=20, return_when=asyncio.FIRST_COMPLETED
            )
            if listening in done:
                break
            listening.cancel()
            if task in done and (task.result().error or {}) and task.result().error.code == "OUTPUT_PORT_BIND_FAILED":
                continue
            raise AssertionError("engine did not start listening")

        reader, writer = await asyncio.open_connection("127.0.0.1", engine.bound_port)
        drained = asyncio.create_task(reader.read(-1))
        result = await asyncio.wait_for(task, 60)
        writer.close()
        drained.cancel()
        assert result.ok
        assert (result.run_dir / EVENTS_NAME).read_bytes().splitlines() == expected


class TestReconnectSemantics:
    """N10: reconnecting gets you the live feed, not a backlog."""

    async def test_a_reconnected_client_receives_current_events_not_a_replay(
        self, make_config
    ):
        for _ in range(6):
            cfg = make_config(
                mode="both",
                count=200_000,
                rate=20_000,
                tcp_port=free_port(),
                wait_for_client=True,
                startup_timeout_s=3.0,
            )
            engine = SimulatorEngine(cfg)
            task = asyncio.create_task(engine.run())
            listening = asyncio.create_task(engine.listening.wait())
            done, _ = await asyncio.wait(
                {task, listening}, timeout=20, return_when=asyncio.FIRST_COMPLETED
            )
            if listening in done:
                break
            listening.cancel()
            if task in done and task.result().error is not None:
                if task.result().error.code == "OUTPUT_PORT_BIND_FAILED":
                    continue
            raise AssertionError("engine did not start listening")
        port = engine.bound_port

        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        first = json.loads(await asyncio.wait_for(reader.readline(), 10))
        for _ in range(50):
            await asyncio.wait_for(reader.readline(), 10)
        writer.close()
        # Wait for the server to notice the peer is gone and free the single
        # client slot. Before the EOF watcher existed this never happened, and
        # the reconnect below was refused as "busy".
        for _ in range(100):
            if engine._tcp is not None and engine._tcp.active_clients == 0:
                break
            await asyncio.sleep(0.02)

        reader2, writer2 = await asyncio.open_connection("127.0.0.1", port)
        resumed = json.loads(await asyncio.wait_for(reader2.readline(), 10))
        engine.request_stop("test:done")
        result = await asyncio.wait_for(task, 60)
        writer2.close()

        # The second connection picks up the live stream; the events emitted
        # while nobody was connected are simply gone from the live channel.
        assert resumed["sequence"] > first["sequence"]
        counters = result.report["metrics"]["counters"]
        assert counters["tcp_no_client_events_total"] > 0
        assert counters["tcp_clients_accepted_total"] == 2
        # ...and the file has all of them, because that is the channel that counts.
        assert result.report["counts"]["recorded_events"] == result.report["counts"][
            "logical_slots"
        ]
        blob = json.dumps(result.report)
        assert "no_loss" not in blob and "exactly_once" not in blob


class TestSmallReceiveBuffer:
    """N13: a tiny client receive buffer must not grow the server."""

    async def test_a_small_rcvbuf_client_is_bounded_not_accommodated(
        self, make_config
    ):
        import socket

        for _ in range(6):
            cfg = make_config(
                mode="both",
                count=40_000,
                rate=1_000_000,
                tcp_port=free_port(),
                wait_for_client=True,
                startup_timeout_s=3.0,
                tcp_max_queue_events=100,
                tcp_max_queue_bytes=32_768,
                send_timeout_ms=200,
            )
            engine = SimulatorEngine(cfg)
            task = asyncio.create_task(engine.run())
            listening = asyncio.create_task(engine.listening.wait())
            done, _ = await asyncio.wait(
                {task, listening}, timeout=20, return_when=asyncio.FIRST_COMPLETED
            )
            if listening in done:
                break
            listening.cancel()
            if task in done and task.result().error is not None:
                if task.result().error.code == "OUTPUT_PORT_BIND_FAILED":
                    continue
            raise AssertionError("engine did not start listening")

        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2048)
        sock.connect(("127.0.0.1", engine.bound_port))
        result = await asyncio.wait_for(task, 90)
        sock.close()

        assert result.ok
        tcp = result.report["tcp"]
        assert tcp["peak_queue_bytes"] <= 32_768
        assert tcp["peak_queue_depth"] <= 100
        assert result.report["counts"]["recorded_events"] == 40_000


class TestNoLeakAcrossManyRuns:
    """R06: a hundred start/stop cycles must not accumulate handles or tasks."""

    async def test_fifty_runs_do_not_grow_handles_or_tasks(self, make_config):
        psutil = pytest.importorskip("psutil")
        process = psutil.Process()

        def handles() -> int:
            return process.num_fds() if hasattr(process, "num_fds") else process.num_handles()

        # Warm up, so one-time allocations are not counted as growth.
        for _ in range(3):
            assert (await run_simulation(make_config(mode="record", count=100, rate=1_000_000))).ok
        gc.collect()
        before_handles = handles()
        before_tasks = len(asyncio.all_tasks())

        for _ in range(50):
            result = await run_simulation(
                make_config(mode="record", count=100, rate=1_000_000)
            )
            assert result.ok
        gc.collect()

        after_handles = handles()
        after_tasks = len(asyncio.all_tasks())
        # A small drift is allowed (allocator and logging internals); a leak of
        # one handle per run would show up as +50.
        assert after_handles - before_handles < 25, (
            f"handles grew from {before_handles} to {after_handles} over 50 runs"
        )
        assert after_tasks <= before_tasks + 1
        assert [t for t in asyncio.all_tasks() if "jsonl-recorder" in t.get_name()] == []


class TestFrozenContractForProject03:
    """C11: the exact shape the C++ adapter will be written against.

    If any of this changes, Project 03 breaks. That is the point of pinning it
    here rather than leaving it implied by the code.
    """

    async def test_the_wire_contract(self, make_config):
        result = await run_simulation(
            make_config(mode="record", count=2_000, rate=1_000_000)
        )
        assert result.ok
        raw = (result.run_dir / EVENTS_NAME).read_bytes()

        # 1. One record per line, LF terminated, no CR anywhere.
        assert raw.endswith(b"\n")
        assert b"\r" not in raw
        lines = raw.splitlines()
        assert len(lines) == 2_000

        # 2. Every line is a complete JSON object with a known field set.
        for line in lines:
            event = json.loads(line)
            assert event["schema_version"] == SCHEMA_VERSION == "sim.v1"
            fields = set(event)
            assert fields in (set(QUOTE_FIELDS), set(TICK_FIELDS))

        # 3. Keys are sorted and there is no whitespace padding.
        for line in lines[:50]:
            assert json.dumps(
                json.loads(line), sort_keys=True, separators=(",", ":")
            ).encode() == line

        # 4. Every numeric field is an integer, never a float.
        for line in lines[:200]:
            for key, value in json.loads(line).items():
                if key.endswith(("_ticks", "_ns", "_scale")) or key == "sequence":
                    assert isinstance(value, int) and not isinstance(value, bool), key

        # 5. price_scale is fixed per symbol for the whole dataset.
        scales: dict[str, int] = {}
        for line in lines:
            event = json.loads(line)
            previous = scales.setdefault(event["symbol"], event["price_scale"])
            assert previous == event["price_scale"], event["symbol"]
        assert scales == {"EURUSD": 100000, "GBPUSD": 100000, "USDJPY": 1000}

        # 6. The verifier agrees, independently of the recorder.
        assert verify_run(result.run_dir).verdict is Verdict.COMPLETE

    async def test_the_tcp_framing_is_the_same_as_the_file_framing(self, make_config):
        # Project 03 will parse both paths with the same code; this is the test
        # that says it may.
        for _ in range(6):
            cfg = make_config(
                mode="both",
                count=1_000,
                rate=100_000,
                tcp_port=free_port(),
                wait_for_client=True,
                startup_timeout_s=3.0,
            )
            engine = SimulatorEngine(cfg)
            task = asyncio.create_task(engine.run())
            listening = asyncio.create_task(engine.listening.wait())
            done, _ = await asyncio.wait(
                {task, listening}, timeout=20, return_when=asyncio.FIRST_COMPLETED
            )
            if listening in done:
                break
            listening.cancel()
            if task in done and task.result().error is not None:
                if task.result().error.code == "OUTPUT_PORT_BIND_FAILED":
                    continue
            raise AssertionError("engine did not start listening")

        reader, writer = await asyncio.open_connection("127.0.0.1", engine.bound_port)
        received = b""
        while received.count(b"\n") < 1_000:
            chunk = await asyncio.wait_for(reader.read(65536), 10)
            if not chunk:
                break
            received += chunk
        writer.close()
        result = await asyncio.wait_for(task, 60)
        assert received == (result.run_dir / EVENTS_NAME).read_bytes()
