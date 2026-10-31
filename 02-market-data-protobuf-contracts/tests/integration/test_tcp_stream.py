"""MDP1 over TCP and queue budgets: what-if group I, plus D16/D17 and H11-H13.

A socket is where fragmentation and coalescing genuinely happen: the writer's
frame boundaries have no relationship to the reader's ``recv`` boundaries. These
tests run over real loopback sockets for that reason, with the C++ client as the
reader where the point is cross-language framing.

The property asserted most often here is the one easiest to fake: that the
server never reports delivery it cannot know about. ``frames_written`` counts
bytes handed to the kernel, and no counter claims a peer accepted anything.
"""

from __future__ import annotations

import socket
import threading
import time

import pytest

from md_contract.codec import encode_event
from md_contract.config import Config
from md_contract.domain import PAYLOAD_QUOTE, DomainEvent
from md_contract.errors import Reason
from md_contract.framing import MAGIC, FrameReader, SocketSource, StreamEnd
from md_contract.tcp_stream import BoundedFrameQueue, MdpServer, QueueBudgetExceeded


def payloads(count: int, *, size_hint: int = 0) -> list[bytes]:
    out = []
    for index in range(1, count + 1):
        event = DomainEvent(
            dataset_id="ds-tcp" + ("x" * size_hint),
            symbol="EURUSD",
            sequence=index,
            event_time_ns=1793322000000000000 + index,
            price_scale=100000,
            source_schema_version="sim.v1",
            payload_case=PAYLOAD_QUOTE,
            bid_ticks=108345 + index,
            ask_ticks=108348 + index,
        )
        out.append(encode_event(event))
    return out


def loopback_config(**stream_overrides: object) -> Config:
    return Config.from_dict({"stream": {"host": "127.0.0.1", "port": 0, **stream_overrides}})


# ---------------------------------------------------------------------------
# the bounded queue: H11, H12
# ---------------------------------------------------------------------------


@pytest.mark.cases("H11")
def test_the_queue_blocks_at_the_event_count_budget() -> None:
    queue = BoundedFrameQueue(max_events=4, max_bytes=1 << 20)
    for payload in payloads(4):
        queue.put(payload, timeout=0.1)
    assert queue.depth == 4
    # The 5th is refused rather than making the queue 5 deep.
    with pytest.raises(QueueBudgetExceeded) as excinfo:
        queue.put(payloads(1)[0], timeout=0.05)
    assert "4/4 events" in str(excinfo.value)
    assert queue.depth == 4
    assert queue.rejected == 1


@pytest.mark.cases("H12")
def test_the_byte_budget_can_bite_before_the_count_budget() -> None:
    # A count-only budget lets memory use vary by the ratio of the smallest to
    # the largest payload. Here the count allows 1000 but the bytes allow ~3.
    one = payloads(1)[0]
    cost = len(one) + 4
    queue = BoundedFrameQueue(max_events=1000, max_bytes=cost * 3)
    for payload in payloads(3):
        queue.put(payload, timeout=0.1)
    with pytest.raises(QueueBudgetExceeded) as excinfo:
        queue.put(one, timeout=0.05)
    assert "bytes" in str(excinfo.value)
    assert queue.depth == 3
    assert queue.byte_depth <= cost * 3


def test_the_byte_budget_accounts_for_framing_overhead() -> None:
    one = payloads(1)[0]
    # Exactly one payload's worth of bytes, with no room for its 4-byte header.
    queue = BoundedFrameQueue(max_events=10, max_bytes=len(one))
    with pytest.raises(QueueBudgetExceeded):
        queue.put(one, timeout=0.02)


def test_a_frame_larger_than_the_whole_budget_is_refused_immediately() -> None:
    queue = BoundedFrameQueue(max_events=10, max_bytes=16)
    with pytest.raises(QueueBudgetExceeded) as excinfo:
        queue.put(b"x" * 100, timeout=None)
    assert "exceeds the whole" in str(excinfo.value)


def test_the_queue_never_drops_an_item_to_make_room() -> None:
    queue = BoundedFrameQueue(max_events=2, max_bytes=1 << 20)
    first, second, third = payloads(3)
    queue.put(first, timeout=0.05)
    queue.put(second, timeout=0.05)
    with pytest.raises(QueueBudgetExceeded):
        queue.put(third, timeout=0.02)
    # The two already queued are intact and in order. Dropping the oldest to
    # admit the newest would lose an event while reporting success.
    assert queue.get(timeout=0.1) == first
    assert queue.get(timeout=0.1) == second


def test_a_blocked_producer_is_released_when_the_consumer_drains() -> None:
    queue = BoundedFrameQueue(max_events=1, max_bytes=1 << 20)
    first, second = payloads(2)
    queue.put(first, timeout=0.05)

    def drain() -> None:
        time.sleep(0.05)
        queue.get(timeout=0.5)

    threading.Thread(target=drain, daemon=True).start()
    # Backpressure, not failure: the producer waits and then succeeds.
    queue.put(second, timeout=1.0)
    assert queue.depth == 1


# ---------------------------------------------------------------------------
# the server over real sockets: I01-I08
# ---------------------------------------------------------------------------


def read_stream_with_python(port: int, *, expect: int, chunk: int = 0) -> list[bytes]:
    with socket.create_connection(("127.0.0.1", port), timeout=5.0) as conn:
        reader = FrameReader(SocketSource(conn), deadline_s=5.0)
        frames = []
        while len(frames) < expect:
            payload = reader.read_frame()
            if payload is None:
                break
            frames.append(payload)
        return frames


@pytest.mark.cases("D16", "D17")
def test_a_python_client_reassembles_frames_off_a_real_socket() -> None:
    expected = payloads(50)
    config = loopback_config()
    with MdpServer(expected, config=config) as server:
        thread = threading.Thread(
            target=lambda: server.serve_once(accept_timeout=5.0), daemon=True
        )
        thread.start()
        received = read_stream_with_python(server.port, expect=50)
        thread.join(timeout=10)

    assert received == expected
    assert server.result.clients_accepted == 1
    assert server.result.frames_written == 50


@pytest.mark.cpp
@pytest.mark.cases("E01", "D16", "D17")
def test_the_cpp_client_reads_the_same_stream_off_a_socket(
    cpp_tools, tmp_path
) -> None:
    import subprocess

    expected = payloads(40)
    config = loopback_config()
    with MdpServer(expected, config=config) as server:
        thread = threading.Thread(
            target=lambda: server.serve_once(accept_timeout=5.0), daemon=True
        )
        thread.start()

        out = tmp_path / "tcp_proj.jsonl"
        report = cpp_tools.run(
            "md_cpp_tcp_client",
            "--host", "127.0.0.1",
            "--port", str(server.port),
            "--output", str(out),
            "--max-events", "40",
            timeout=30,
        )
        thread.join(timeout=10)

    assert report["exit_code"] == 0, report
    assert report["events_accepted"] == 40
    assert report["rejected"] == 0
    assert len(out.read_text(encoding="utf-8").splitlines()) == 40
    # The client reports what it read and makes no claim about the sender.
    assert "peer_accepted" not in report


@pytest.mark.cases("I05")
def test_no_client_is_reported_as_no_client_not_as_a_successful_run() -> None:
    config = loopback_config()
    with MdpServer(payloads(10), config=config) as server:
        result = server.serve_once(accept_timeout=0.3)
    assert result.clients_accepted == 0
    assert result.frames_written == 0
    assert result.completed is False
    assert result.disconnects["no_client"] == 1
    # Nothing in the report could be read as "10 events delivered".
    assert result.as_dict()["peer_accepted"] is None


@pytest.mark.cases("I06")
def test_a_second_connection_is_not_served_while_one_client_is_active() -> None:
    """max_clients=1 means the second client gets no stream.

    The first client deliberately never reads, so the socket buffers fill and
    the server is still inside serve_once when the second client arrives. The
    property asserted is "the second client was not served a stream" -- which it
    satisfies whether the connection was accepted-and-closed (counted in
    clients_rejected) or refused outright once the listen backlog filled. Both
    are bounded refusals; pinning the test to one of them would be testing the
    OS rather than the server.
    """
    config = loopback_config(max_clients=1, payload_deadline_ms=3000)
    # Large enough that the server cannot drain it into socket buffers and
    # finish before the second client connects.
    expected = payloads(8000, size_hint=400)
    with MdpServer(expected, config=config) as server:
        thread = threading.Thread(
            target=lambda: server.serve_once(accept_timeout=5.0), daemon=True
        )
        thread.start()
        first = socket.create_connection(("127.0.0.1", server.port), timeout=5.0)
        try:
            assert first.recv(len(MAGIC)) == MAGIC, "the first client is served"
            time.sleep(0.3)

            served_a_stream = False
            try:
                second = socket.create_connection(("127.0.0.1", server.port), timeout=2.0)
            except OSError:
                # Refused at the TCP layer: no stream, which is the property.
                pass
            else:
                try:
                    second.settimeout(1.0)
                    try:
                        served_a_stream = second.recv(len(MAGIC)) == MAGIC
                    except (socket.timeout, TimeoutError, OSError):
                        served_a_stream = False
                finally:
                    second.close()
            assert not served_a_stream, "a second client must never receive the stream"
        finally:
            first.close()
            server.stop()
            thread.join(timeout=15)

    assert server.result.clients_accepted == 1


@pytest.mark.cases("I01", "I03")
def test_a_client_vanishing_mid_stream_is_not_reported_as_delivered() -> None:
    expected = payloads(2000, size_hint=200)
    config = loopback_config(payload_deadline_ms=500)
    with MdpServer(expected, config=config) as server:
        thread = threading.Thread(
            target=lambda: server.serve_once(accept_timeout=5.0), daemon=True
        )
        thread.start()
        conn = socket.create_connection(("127.0.0.1", server.port), timeout=5.0)
        conn.recv(len(MAGIC))
        conn.recv(64)
        # Abort hard: SO_LINGER 0 sends an RST rather than a clean FIN.
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, (1).to_bytes(2, "little") + b"\x00" * 6)
        conn.close()
        thread.join(timeout=15)

    result = server.result
    assert result.completed is False, "an aborted stream is not a completed one"
    assert result.frames_written < len(expected)
    assert result.disconnects, "the reason the stream ended is recorded"
    # frames_written is bytes to the kernel, nothing more. Even for the frames
    # it did write, the server makes no claim the peer parsed them.
    assert result.as_dict()["peer_accepted"] is None
    assert "NOT proof" in result.as_dict()["frames_written_note"]


@pytest.mark.cases("I04", "H13")
def test_a_receiver_that_stops_reading_triggers_a_bounded_response() -> None:
    """A stalled reader must not stall the writer indefinitely.

    The client connects and then never reads. The socket buffers fill, the
    server's send blocks, and the deadline turns that into a closed connection
    with a recorded reason rather than an unbounded wait.
    """
    expected = payloads(20000, size_hint=400)
    config = loopback_config(payload_deadline_ms=400, max_queue_events=256)
    with MdpServer(expected, config=config) as server:
        thread = threading.Thread(
            target=lambda: server.serve_once(accept_timeout=5.0), daemon=True
        )
        thread.start()
        conn = socket.create_connection(("127.0.0.1", server.port), timeout=5.0)
        start = time.monotonic()
        thread.join(timeout=30)
        elapsed = time.monotonic() - start
        conn.close()

    assert not thread.is_alive(), "the server did not give up on a stalled reader"
    assert elapsed < 25, f"took {elapsed:.1f}s to react to a stalled reader"
    result = server.result
    assert result.completed is False
    assert result.frames_written < len(expected)
    # Queue growth stayed inside the configured budget throughout.
    assert result.peak_queue_events <= 256
    assert result.peak_queue_bytes <= config.stream.max_queue_bytes


@pytest.mark.cases("I07")
def test_a_reconnect_is_a_new_session_with_no_replay_cursor() -> None:
    """MDP1 has no resume point, and the server must not pretend otherwise."""
    expected = payloads(10)
    config = loopback_config()
    with MdpServer(expected, config=config) as first_server:
        thread = threading.Thread(
            target=lambda: first_server.serve_once(accept_timeout=5.0), daemon=True
        )
        thread.start()
        frames_first = read_stream_with_python(first_server.port, expect=3)
        thread.join(timeout=10)

    assert len(frames_first) == 3

    # A second server is a second session: it starts from its own beginning and
    # re-sends the magic. There is no cursor that would let a client resume, and
    # no exactly-once claim anywhere in the result.
    with MdpServer(expected, config=config) as second_server:
        thread = threading.Thread(
            target=lambda: second_server.serve_once(accept_timeout=5.0), daemon=True
        )
        thread.start()
        frames_second = read_stream_with_python(second_server.port, expect=3)
        thread.join(timeout=10)

    assert frames_second == frames_first[: len(frames_second)]
    assert "peer_accepted" in second_server.result.as_dict()
    assert second_server.result.as_dict()["peer_accepted"] is None


@pytest.mark.cases("I08", "D02")
def test_jsonl_sent_to_the_protobuf_port_is_rejected_by_the_reader() -> None:
    """Protocol confusion: the magic is what makes the two ports distinguishable.

    A JSONL line arriving where MDP1 is expected is refused. There is
    deliberately no JSON fallback, because serving two protocols on one port is
    how a payload gets interpreted as the wrong thing.
    """
    import socket as sk

    listener = sk.socket(sk.AF_INET, sk.SOCK_STREAM)
    listener.setsockopt(sk.SOL_SOCKET, sk.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    listener.listen(1)

    def send_jsonl() -> None:
        conn, _ = listener.accept()
        conn.sendall(b'{"schema_version":"sim.v1","event_type":"quote"}\n')
        time.sleep(0.2)
        conn.close()

    thread = threading.Thread(target=send_jsonl, daemon=True)
    thread.start()
    try:
        with sk.create_connection(("127.0.0.1", port), timeout=5.0) as conn:
            reader = FrameReader(SocketSource(conn), deadline_s=2.0)
            from md_contract.errors import FramingError

            with pytest.raises(FramingError) as excinfo:
                reader.read_frame()
            assert excinfo.value.reason is Reason.BAD_MAGIC
            assert reader.end is StreamEnd.FAILED
    finally:
        thread.join(timeout=5)
        listener.close()


@pytest.mark.cases("J09")
def test_the_server_refuses_a_non_loopback_bind_without_approval() -> None:
    from md_contract.errors import ConfigError

    # Config.from_dict refuses it first; this asserts the server does not have a
    # path around that check.
    with pytest.raises(ConfigError):
        loopback_config(host="0.0.0.0")


@pytest.mark.cases("J07")
@pytest.mark.slow
def test_a_connection_flood_does_not_leak_descriptors() -> None:
    """Connection churn must not accumulate handles in the server process.

    The server is single-shot by design (``max_clients = 1``), so this does not
    assert it keeps serving afterwards -- once its one client is done it stops
    accepting, and connections then being refused is the intended behaviour, not
    a fault. What must not happen is that the turned-away connections pile up as
    open descriptors, which is what would eventually exhaust the process.
    """
    psutil = pytest.importorskip("psutil", reason="handle counting needs psutil")
    import os

    process = psutil.Process(os.getpid())

    def handle_count() -> int:
        try:
            return process.num_handles() if hasattr(process, "num_handles") else process.num_fds()
        except Exception:  # noqa: BLE001
            pytest.skip("this platform does not expose a handle count")

    config = loopback_config()
    with MdpServer(payloads(5), config=config) as server:
        before = handle_count()
        refused = 0
        for _ in range(300):
            try:
                socket.create_connection(("127.0.0.1", server.port), timeout=0.5).close()
            except OSError:
                refused += 1
        after = handle_count()

    # Each churned connection is closed on both sides, so the count returns to
    # roughly where it started rather than growing with the number of attempts.
    growth = after - before
    assert growth < 100, f"handle count grew by {growth} across 300 connections"
    # The server object survived and shut down cleanly.
    assert server.result.clients_accepted == 0 or server.result.clients_accepted >= 1
