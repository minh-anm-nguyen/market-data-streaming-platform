"""TCP sink end to end (E2E-04, E2E-05, E2E-12, E2E-14, cases N01-N18).

Everything here is about one claim from section 5.4: a badly behaved client can
degrade the live stream but can never cost the recorded dataset an event, and
nothing in the output ever implies a delivery the transport did not prove.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from conftest import free_port, read_events
from market_sim.engine import RunState, SimulatorEngine, run_simulation
from market_sim.recorder import COMPLETED_NAME, EVENTS_NAME
from market_sim.verify import Verdict, verify_run

pytestmark = pytest.mark.asyncio


async def connect(port, *, host="127.0.0.1", timeout=5.0, **kwargs):
    """Connect, retrying until the server is listening or the deadline passes."""
    deadline = asyncio.get_running_loop().time() + timeout
    last = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            return await asyncio.open_connection(host, port, **kwargs)
        except (ConnectionRefusedError, OSError) as exc:
            last = exc
            await asyncio.sleep(0.01)
    raise AssertionError(f"could not connect to {host}:{port}: {last}")


async def start_engine(make_config, **kwargs):
    """Start an engine and wait until its listener is actually bound.

    ``port = 0`` is rejected by the validator on purpose (case E2E-11 lists it as
    invalid), so a test has to name a port -- and naming one is inherently racy:
    another process can take it between free_port() and bind(). This retries on a
    bind failure rather than leaving a flaky test behind, and waits on the
    engine's own ``listening`` event instead of polling connect().
    """
    plan = kwargs.pop("fault_plan", None)
    last_error = None
    for _ in range(6):
        port = free_port()
        cfg = make_config(tcp_port=port, **kwargs)
        engine = SimulatorEngine(cfg, fault_plan=plan)
        task = asyncio.create_task(engine.run())
        listening = asyncio.create_task(engine.listening.wait())
        done, _pending = await asyncio.wait(
            {task, listening}, timeout=20, return_when=asyncio.FIRST_COMPLETED
        )
        if listening in done:
            # bound_port is the authoritative answer, not what we asked for.
            return engine, task, engine.bound_port or port
        listening.cancel()
        if task in done:
            result = task.result()
            last_error = result.error
            if last_error is not None and last_error.code == "OUTPUT_PORT_BIND_FAILED":
                continue  # someone took the port; try another
            raise AssertionError(f"engine stopped before listening: {last_error}")
        task.cancel()
        raise AssertionError("engine neither started listening nor finished")
    raise AssertionError(f"could not bind a free port after 6 attempts: {last_error}")


async def run_tcp_run(make_config, **kwargs):
    """Run an engine to completion on a free port, retrying a lost bind.

    Same port race as :func:`start_engine`, but for the tests where nobody
    connects and there is nothing to wait for.
    """
    plan = kwargs.pop("fault_plan", None)
    last_error = None
    for _ in range(6):
        cfg = make_config(tcp_port=free_port(), **kwargs)
        result = await asyncio.wait_for(run_simulation(cfg, fault_plan=plan), 90)
        if result.ok or (result.error and result.error.code != "OUTPUT_PORT_BIND_FAILED"):
            return result
        last_error = result.error
    raise AssertionError(f"could not bind a free port after 6 attempts: {last_error}")


class TestNoClient:
    """E2E-05: nobody connects."""

    async def test_both_mode_records_everything_and_counts_non_delivery(
        self, make_config
    ):
        result = await run_tcp_run(
            make_config, mode="both", count=2_000, rate=1_000_000
        )
        assert result.ok
        assert len(read_events(result.run_dir / EVENTS_NAME)) == 2_000
        counters = result.report["metrics"]["counters"]
        assert counters["tcp_no_client_events_total"] == 2_000
        assert counters["tcp_events_enqueued_total"] == 0
        assert verify_run(result.run_dir).verdict is Verdict.COMPLETE

    async def test_serve_mode_does_not_hang_without_a_client(self, make_config):
        result = await run_tcp_run(
            make_config, mode="serve", count=500, rate=1_000_000
        )
        assert result.ok
        assert result.report["metrics"]["counters"]["tcp_no_client_events_total"] == 500

    async def test_wait_for_client_times_out_rather_than_waiting_forever(
        self, make_config
    ):
        # Section 5.2: an unbounded wait is not an option.
        result = await run_tcp_run(
            make_config,
            mode="serve",
            count=10,
            rate=1_000_000,
            wait_for_client=True,
            startup_timeout_s=0.3,
        )
        assert result.ok
        assert result.report["metrics"]["gauges"]["tcp_startup_wait_timed_out"] == 1

    async def test_serve_mode_has_no_dataset_and_says_so(self, make_config):
        result = await run_tcp_run(
            make_config, mode="serve", count=100, rate=1_000_000
        )
        assert not (result.run_dir / EVENTS_NAME).exists()
        assert not (result.run_dir / COMPLETED_NAME).exists()
        verdict = verify_run(result.run_dir)
        assert any("no recorded dataset" in w for w in verdict.warnings)


class TestFastClient:
    async def test_a_reading_client_receives_well_framed_records(self, make_config):
        engine, task, port = await start_engine(
            make_config,
            mode="both", count=1_000, rate=200_000, wait_for_client=True
        )
        reader, writer = await connect(port)
        received = []
        try:
            while len(received) < 1_000:
                line = await asyncio.wait_for(reader.readline(), 10)
                if not line:
                    break
                received.append(line)
        finally:
            writer.close()
        result = await asyncio.wait_for(task, 30)
        assert result.ok
        # Every line is a complete, parseable record terminated by a newline.
        assert all(line.endswith(b"\n") for line in received)
        parsed = [json.loads(line) for line in received]
        assert all(p["schema_version"] == "sim.v1" for p in parsed)
        assert len(received) == 1_000

    async def test_the_live_stream_matches_the_recorded_file(self, make_config):
        engine, task, port = await start_engine(
            make_config,
            mode="both", count=500, rate=200_000, wait_for_client=True
        )
        reader, writer = await connect(port)
        received = b""
        try:
            while received.count(b"\n") < 500:
                chunk = await asyncio.wait_for(reader.read(65536), 10)
                if not chunk:
                    break
                received += chunk
        finally:
            writer.close()
        result = await asyncio.wait_for(task, 30)
        assert received == (result.run_dir / EVENTS_NAME).read_bytes()

    async def test_one_read_may_contain_many_records(self, make_config):
        # Cases N05: TCP coalesces, so a client must split on the delimiter and
        # never assume one read is one event.
        engine, task, port = await start_engine(
            make_config,
            mode="both", count=2_000, rate=1_000_000, wait_for_client=True
        )
        reader, writer = await connect(port)
        max_in_one_read = 0
        total = b""
        try:
            while total.count(b"\n") < 2_000:
                chunk = await asyncio.wait_for(reader.read(262144), 10)
                if not chunk:
                    break
                max_in_one_read = max(max_in_one_read, chunk.count(b"\n"))
                total += chunk
        finally:
            writer.close()
        await asyncio.wait_for(task, 30)
        assert max_in_one_read > 1, "expected coalescing; the framing test is moot without it"
        assert total.count(b"\n") == 2_000

    async def test_byte_at_a_time_reader_loses_nothing(self, make_config):
        # Cases N04/N06 and E2E-12: a record split across many reads must be
        # reassembled, and the record count must not change.
        engine, task, port = await start_engine(
            make_config,
            mode="both", count=200, rate=100_000, wait_for_client=True
        )
        reader, writer = await connect(port)
        buffer = bytearray()
        lines = []
        try:
            while len(lines) < 200:
                byte = await asyncio.wait_for(reader.read(1), 20)
                if not byte:
                    break
                buffer += byte
                while b"\n" in buffer:
                    index = buffer.index(b"\n")
                    lines.append(bytes(buffer[:index]))
                    del buffer[: index + 1]
        finally:
            writer.close()
        result = await asyncio.wait_for(task, 60)
        assert len(lines) == 200
        assert buffer == b""  # no partial record left over
        recorded = (result.run_dir / EVENTS_NAME).read_bytes().splitlines()
        assert lines == recorded


class TestSlowClient:
    """E2E-04: the client stops reading. The file must not notice."""

    async def test_overflow_is_counted_and_the_file_stays_complete(self, make_config):
        engine, task, port = await start_engine(
            make_config,
            mode="both",
            count=20_000,
            rate=1_000_000,
            wait_for_client=True,
            tcp_max_queue_events=50,
            tcp_max_queue_bytes=16_384,
            send_timeout_ms=200,
        )
        reader, writer = await connect(port)
        # Connect and then never read a byte.
        result = await asyncio.wait_for(task, 60)
        writer.close()

        assert result.ok, result.report.get("error")
        # The dataset is untouched by the client's behaviour.
        assert len(read_events(result.run_dir / EVENTS_NAME)) == 20_000
        assert verify_run(result.run_dir).verdict is Verdict.COMPLETE
        counters = result.report["metrics"]["counters"]
        assert counters["tcp_queue_overflow_total"] > 0
        assert counters["tcp_events_enqueued_total"] < 20_000

    async def test_the_queue_budget_is_respected_rather_than_grown(self, make_config):
        # Case R02: a client holding the socket open must not grow RSS without
        # bound; the queue high-water mark proves the budget held.
        engine, task, port = await start_engine(
            make_config,
            mode="both",
            count=30_000,
            rate=1_000_000,
            wait_for_client=True,
            tcp_max_queue_events=100,
            tcp_max_queue_bytes=32_768,
            send_timeout_ms=200,
        )
        reader, writer = await connect(port)
        result = await asyncio.wait_for(task, 60)
        writer.close()
        tcp = result.report["tcp"]
        assert tcp["peak_queue_depth"] <= 100
        assert tcp["peak_queue_bytes"] <= 32_768

    async def test_the_report_never_claims_the_client_received_anything(
        self, make_config
    ):
        # NFR-08 / case N09: drain() emptied our buffer; that is all it means.
        engine, task, port = await start_engine(
            make_config,
            mode="both",
            count=5_000,
            rate=1_000_000,
            wait_for_client=True,
            tcp_max_queue_events=50,
            send_timeout_ms=200,
        )
        reader, writer = await connect(port)
        result = await asyncio.wait_for(task, 60)
        writer.close()
        tcp = result.report["tcp"]
        assert "best-effort" in tcp["delivery_guarantee"]
        assert all(c["client_acknowledged"] == "unknown" for c in tcp["clients"])
        blob = json.dumps(result.report)
        assert "events_received" not in blob
        assert "client_persisted" not in blob

    async def test_a_client_that_disconnects_immediately_does_not_break_the_run(
        self, make_config
    ):
        # Case N02.
        engine, task, port = await start_engine(
            make_config,
            mode="both", count=5_000, rate=1_000_000, wait_for_client=True
        )
        reader, writer = await connect(port)
        writer.close()
        await asyncio.sleep(0.05)
        result = await asyncio.wait_for(task, 60)
        assert result.ok
        assert len(read_events(result.run_dir / EVENTS_NAME)) == 5_000


class TestConnectionPolicy:
    async def test_a_second_client_is_rejected_not_silently_fanned_out(
        self, make_config
    ):
        # Case N11: MVP policy is one client. Accepting a second would mean a
        # second unbounded-by-default queue and a fairness problem nobody asked
        # for; it is refused instead.
        engine, task, port = await start_engine(
            make_config,
            mode="both", count=20_000, rate=1_000_000, wait_for_client=True
        )
        reader1, writer1 = await connect(port)
        reader2, writer2 = await connect(port)
        # The rejected connection is closed by the server, so it reads EOF.
        assert await asyncio.wait_for(reader2.read(100), 10) == b""
        result = await asyncio.wait_for(task, 60)
        writer1.close()
        writer2.close()
        counters = result.report["metrics"]["counters"]
        assert counters["tcp_clients_accepted_total"] == 1
        assert counters["tcp_clients_rejected_total"] >= 1
        assert len(read_events(result.run_dir / EVENTS_NAME)) == 20_000

    async def test_a_busy_port_fails_the_run_before_anything_is_produced(
        self, make_config
    ):
        # Case N12: no manifest may claim a successful serve on a port we never got.
        import socket

        blocker = socket.socket()
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        port = blocker.getsockname()[1]
        try:
            cfg = make_config(mode="both", count=100, rate=1_000_000, tcp_port=port)
            result = await run_simulation(cfg)
            assert result.state is RunState.FAILED
            assert result.exit_code == 3
            assert result.error.code == "OUTPUT_PORT_BIND_FAILED"
            assert not (result.run_dir / COMPLETED_NAME).exists()
            assert not (result.run_dir / EVENTS_NAME).exists()
        finally:
            blocker.close()

    async def test_many_connect_disconnect_cycles_do_not_leak(self, make_config):
        """E2E-14 / case N17, driven against the sink directly.

        Deliberately not run against a live engine: hammering connects while the
        engine is also shutting down races CPython's own accept path (a
        connection completing after Server.close() trips an assertion inside
        asyncio.base_events). That race says nothing about whether *we* leak, so
        the sink is exercised on its own and closed once, at the end.
        """
        import gc

        from market_sim.metrics import Metrics
        from market_sim.tcp_server import TcpSink

        metrics = Metrics()
        cfg = make_config(mode="both", count=10, rate=1000, tcp_port=free_port())
        sink = TcpSink(cfg.tcp, metrics=metrics)
        await sink.start()
        port = sink.bound_port

        try:
            for _ in range(100):
                reader, writer = await connect(port, timeout=5.0)
                sink.publish(b'{"probe":1}\n')
                writer.close()
                with __import__("contextlib").suppress(Exception):
                    await asyncio.wait_for(writer.wait_closed(), 2.0)
                # Let the server notice the peer is gone before the next cycle.
                for _ in range(50):
                    if sink.active_clients == 0:
                        break
                    await asyncio.sleep(0.01)
                assert sink.active_clients == 0
        finally:
            await sink.shutdown(5.0)
        gc.collect()

        # metrics.counter() rather than the dict: an untouched counter is absent
        # by design, and "absent" and "zero" are different claims.
        assert metrics.counter("tcp_clients_accepted_total") == 100
        assert metrics.counter("tcp_clients_disconnected_total") == 100
        assert metrics.counter("tcp_clients_rejected_total") == 0
        # Nothing left running and nothing accumulated per connection.
        assert [t for t in asyncio.all_tasks() if "tcp-writer" in t.get_name()] == []
        assert len(sink.client_reports()) == 100

    async def test_client_stats_are_kept_after_disconnect(self, make_config):
        engine, task, port = await start_engine(
            make_config,
            mode="both", count=2_000, rate=500_000, wait_for_client=True
        )
        reader, writer = await connect(port)
        await asyncio.wait_for(reader.readline(), 10)
        result = await asyncio.wait_for(task, 60)
        writer.close()
        clients = result.report["tcp"]["clients"]
        assert len(clients) == 1
        assert clients[0]["events_enqueued"] > 0
        assert clients[0]["client_acknowledged"] == "unknown"


class TestTransportFaults:
    """Corrupt bytes on the wire, with the recorded file left intact.

    Invariant 8 of section 3.2: raw malformed JSON and chopped lines are injected
    on the TCP channel only. Mixing them into the recorder would destroy the
    golden dataset every other test compares against.
    """

    async def _run(self, make_config, fault, *, count=3_000):
        from market_sim.faults import parse_fault_plan

        plan = parse_fault_plan({"fault": [fault]})
        engine, task, port = await start_engine(
            make_config,
            mode="both",
            count=count,
            rate=200_000,
            wait_for_client=True,
            faults_enabled=True,
            plan_path="plan.toml",
            fault_plan_fingerprint=plan.fingerprint(),
            fault_plan=plan,
        )
        reader, writer = await connect(port)
        received = b""
        try:
            while True:
                chunk = await asyncio.wait_for(reader.read(65536), 10)
                if not chunk:
                    break
                received += chunk
        except asyncio.TimeoutError:
            pass
        finally:
            writer.close()
        result = await asyncio.wait_for(task, 60)
        return result, received

    # Case N14: malformed bytes go out on the TCP channel only, so the
    # adapter's parser is exercised while the golden recorder stays intact.
    async def test_malformed_json_reaches_the_client_but_not_the_file(
        self, make_config
    ):
        result, received = await self._run(
            make_config,
            {"kind": "malformed_json", "symbol": "EURUSD", "sequence": 50},
        )
        assert result.ok
        # The client saw at least one line it cannot parse.
        unparseable = 0
        for line in received.splitlines():
            if not line.strip():
                continue
            try:
                json.loads(line)
            except ValueError:
                unparseable += 1
        assert unparseable >= 1

        # The golden file is still perfectly well formed.
        events = read_events(result.run_dir / EVENTS_NAME)
        assert len(events) == 3_000
        verdict = verify_run(result.run_dir)
        assert verdict.verdict is Verdict.COMPLETE, verdict.reasons
        assert result.report["metrics"]["counters"]["tcp_transport_faults_total"] >= 1

    async def test_truncated_line_then_close_leaves_the_file_complete(
        self, make_config
    ):
        result, received = await self._run(
            make_config,
            {
                "kind": "truncated_line",
                "symbol": "EURUSD",
                "sequence": 40,
                "truncate_bytes": 20,
            },
        )
        assert result.ok
        # The last thing the client got is a fragment with no newline, which a
        # correct consumer must discard rather than parse (case N07).
        assert not received.endswith(b"\n")
        assert len(read_events(result.run_dir / EVENTS_NAME)) == 3_000
        assert verify_run(result.run_dir).verdict is Verdict.COMPLETE

    async def test_close_connection_disconnects_without_touching_the_file(
        self, make_config
    ):
        result, received = await self._run(
            make_config, {"kind": "close_connection", "symbol": "EURUSD", "sequence": 30}
        )
        assert result.ok
        assert len(read_events(result.run_dir / EVENTS_NAME)) == 3_000
        assert verify_run(result.run_dir).verdict is Verdict.COMPLETE
        clients = result.report["tcp"]["clients"]
        assert any(c["transport_faults_applied"] for c in clients)

    async def test_the_transport_fault_is_audited_as_transport_channel(
        self, make_config
    ):
        result, _ = await self._run(
            make_config, {"kind": "close_connection", "symbol": "EURUSD", "sequence": 30}
        )
        rows = result.report["faults"]["records"]
        assert rows[0]["channel"] == "transport"
        assert rows[0]["applied"] is True

    async def test_a_transport_fault_with_no_client_is_reported_as_skipped(
        self, make_config
    ):
        # Over-claiming is the failure mode: with nobody connected there was no
        # stream to corrupt, and the manifest has to say so.
        from market_sim.faults import parse_fault_plan

        plan = parse_fault_plan(
            {"fault": [{"kind": "close_connection", "symbol": "EURUSD", "sequence": 10}]}
        )
        result = await run_tcp_run(
            make_config,
            mode="both",
            count=500,
            rate=1_000_000,
            faults_enabled=True,
            plan_path="plan.toml",
            fault_plan_fingerprint=plan.fingerprint(),
            fault_plan=plan,
        )
        assert result.ok
        row = result.report["faults"]["records"][0]
        assert row["applied"] is False
        assert "no client" in row["reason_if_not_applied"]
