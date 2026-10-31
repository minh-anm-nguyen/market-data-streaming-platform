"""MDP1 over TCP: a bounded, loopback-only server (sections 4, 6.2).

The queue is bounded by **both** a frame count and a byte total, whichever is
reached first (NFR-04). One limit is not enough: 2048 frames of 60 bytes is
120 KiB, while 2048 frames of 4096 bytes is 8 MiB, so a count-only budget lets
memory use vary by a factor of seventy with the same configuration. The byte
total counts the 4-byte header too, because that is memory the process actually
holds.

What this server does not do, deliberately:

* It does not claim delivery. ``frames_written_total`` means bytes handed to the
  kernel for that socket. It is not proof the peer parsed anything, and there is
  no ``peer_accepted`` counter to suggest otherwise (FR-13).
* It does not drop frames to keep up. On a full queue it either applies
  backpressure with a timeout or closes the connection with
  ``QUEUE_BUDGET_EXCEEDED``. Dropping an event while reporting success is the
  failure this design rules out (section 6.2).
* It does not bind off loopback without an explicit decision in config, because
  MDP1 has neither authentication nor encryption (case J09).
"""

from __future__ import annotations

import socket
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Callable, Iterable

from .config import LOOPBACK_HOSTS, Config
from .errors import ConfigError, ContractError, Reason
from .framing import HEADER_LEN, MAGIC, frame_bytes
from .metrics import Metrics


class QueueBudgetExceeded(ContractError):
    def __init__(self, detail: str) -> None:
        super().__init__(Reason.QUEUE_BUDGET_EXCEEDED, detail)


@dataclass
class BoundedFrameQueue:
    """A frame queue bounded by count and by bytes.

    ``put`` blocks up to ``timeout`` so a producer feels backpressure, then
    raises. It never discards, and it never grows past either budget.
    """

    max_events: int = 2048
    max_bytes: int = 8 * 1024 * 1024
    _items: deque[bytes] = field(default_factory=deque, init=False)
    _bytes: int = field(default=0, init=False)
    _lock: threading.Condition = field(
        default_factory=lambda: threading.Condition(threading.Lock()), init=False
    )
    _closed: bool = field(default=False, init=False)
    peak_events: int = field(default=0, init=False)
    peak_bytes: int = field(default=0, init=False)
    rejected: int = field(default=0, init=False)

    def _cost(self, payload: bytes) -> int:
        # Framing overhead is part of what the queue holds, so it is part of the
        # budget rather than invisible headroom.
        return len(payload) + HEADER_LEN

    def put(self, payload: bytes, *, timeout: float | None = 1.0) -> None:
        cost = self._cost(payload)
        if cost > self.max_bytes:
            raise QueueBudgetExceeded(
                f"one frame of {cost} bytes exceeds the whole {self.max_bytes}-byte budget"
            )
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            while True:
                if self._closed:
                    raise QueueBudgetExceeded("queue closed")
                if (
                    len(self._items) < self.max_events
                    and self._bytes + cost <= self.max_bytes
                ):
                    self._items.append(payload)
                    self._bytes += cost
                    self.peak_events = max(self.peak_events, len(self._items))
                    self.peak_bytes = max(self.peak_bytes, self._bytes)
                    self._lock.notify()
                    return
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    self.rejected += 1
                    raise QueueBudgetExceeded(
                        f"queue full at {len(self._items)}/{self.max_events} events, "
                        f"{self._bytes}/{self.max_bytes} bytes"
                    )
                self._lock.wait(timeout=remaining if remaining is not None else 0.1)

    def get(self, *, timeout: float = 0.5) -> bytes | None:
        deadline = time.monotonic() + timeout
        with self._lock:
            while not self._items:
                if self._closed:
                    return None
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._lock.wait(timeout=remaining)
            payload = self._items.popleft()
            self._bytes -= self._cost(payload)
            self._lock.notify()
            return payload

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._lock.notify_all()

    @property
    def depth(self) -> int:
        with self._lock:
            return len(self._items)

    @property
    def byte_depth(self) -> int:
        with self._lock:
            return self._bytes


@dataclass
class ServeResult:
    """What the server actually did. Counters are per stage, never merged."""

    frames_enqueued: int = 0
    frames_written: int = 0
    bytes_written: int = 0
    clients_accepted: int = 0
    clients_rejected: int = 0
    queue_rejections: int = 0
    peak_queue_events: int = 0
    peak_queue_bytes: int = 0
    disconnects: Counter[str] = field(default_factory=Counter)
    completed: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "frames_enqueued": self.frames_enqueued,
            "frames_written": self.frames_written,
            "bytes_written": self.bytes_written,
            "clients_accepted": self.clients_accepted,
            "clients_rejected": self.clients_rejected,
            "queue_rejections": self.queue_rejections,
            "peak_queue_events": self.peak_queue_events,
            "peak_queue_bytes": self.peak_queue_bytes,
            "disconnects_by_reason": dict(self.disconnects),
            "completed": self.completed,
            "frames_written_note": (
                "bytes handed to the kernel for this socket; NOT proof the peer "
                "parsed or accepted any of them"
            ),
            "peer_accepted": None,
        }


class MdpServer:
    """Serves one MDP1 stream on loopback.

    ``max_clients`` defaults to 1 and a second connection is refused explicitly
    rather than queued, so a test can tell "rejected" from "accepted but idle"
    (case I06).
    """

    def __init__(
        self,
        payloads: Iterable[bytes],
        *,
        config: Config | None = None,
        metrics: Metrics | None = None,
        on_queue_full: str | None = None,
    ) -> None:
        self.config = config or Config()
        stream = self.config.stream
        if stream.host not in LOOPBACK_HOSTS and not stream.allow_public_bind:
            raise ConfigError(
                f"refusing to bind {stream.host}: MDP1 is unauthenticated and "
                "unencrypted, so a non-loopback bind needs allow_public_bind"
            )
        self.payloads = payloads
        self.metrics = metrics or Metrics()
        self.policy = on_queue_full or stream.on_queue_budget_exceeded
        self.queue = BoundedFrameQueue(
            max_events=stream.max_queue_events, max_bytes=stream.max_queue_bytes
        )
        self.result = ServeResult()
        self._sock: socket.socket | None = None
        self._stop = threading.Event()
        self.port: int = stream.port

    def __enter__(self) -> "MdpServer":
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.config.stream.host, self.config.stream.port))
        # Resolve port 0 to whatever the OS picked, so tests can run in
        # parallel without fighting over a fixed port.
        self.port = self._sock.getsockname()[1]
        self._sock.listen(8)
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()

    def stop(self) -> None:
        self._stop.set()
        self.queue.close()
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None

    def serve_once(self, *, accept_timeout: float = 10.0) -> ServeResult:
        """Accept one client, stream every payload, then close.

        Returns when the payloads are drained, the client goes away, or a budget
        is exceeded. The reason is recorded either way; a connection that ended
        early is never reported as a completed stream.
        """
        assert self._sock is not None, "serve_once outside the context manager"
        self._sock.settimeout(accept_timeout)
        try:
            conn, _ = self._sock.accept()
        except (socket.timeout, TimeoutError):
            # No consumer is a real outcome and is reported as such, not as a
            # successful run with zero events delivered (case I05).
            self.result.disconnects["no_client"] += 1
            return self.result
        self.result.clients_accepted += 1

        # Any further connection is refused while this one is served.
        def reject_extras() -> None:
            assert self._sock is not None
            while not self._stop.is_set():
                try:
                    self._sock.settimeout(0.2)
                    extra, _ = self._sock.accept()
                except (socket.timeout, TimeoutError):
                    continue
                except OSError:
                    return
                # Counted, then closed. A refused client has to be visible in the
                # result, otherwise "accepted but idle" and "turned away" would
                # look identical to a test (case I06).
                self.result.clients_rejected += 1
                try:
                    extra.close()
                except OSError:
                    pass

        rejecter = threading.Thread(target=reject_extras, daemon=True)
        rejecter.start()

        send_timeout = self.config.stream.payload_deadline_ms / 1000.0
        conn.settimeout(send_timeout)
        try:
            conn.sendall(MAGIC)
            self.result.bytes_written += len(MAGIC)
            for payload in self.payloads:
                if self._stop.is_set():
                    self.result.disconnects["stopped"] += 1
                    break
                try:
                    self.queue.put(
                        payload,
                        timeout=send_timeout if self.policy == "block_with_timeout" else 0.0,
                    )
                except QueueBudgetExceeded:
                    self.result.queue_rejections += 1
                    self.metrics.reject("queue", Reason.QUEUE_BUDGET_EXCEEDED.value)
                    self.result.disconnects["queue_budget_exceeded"] += 1
                    break
                self.result.frames_enqueued += 1

                drained = self.queue.get(timeout=0.5)
                if drained is None:
                    continue
                try:
                    conn.sendall(frame_bytes(drained))
                except (socket.timeout, TimeoutError):
                    # A receiver that stopped reading fills the socket buffer and
                    # this blocks. The deadline turns that into a closed
                    # connection rather than an unbounded stall (case I04).
                    self.result.disconnects["send_timeout"] += 1
                    break
                except OSError:
                    self.result.disconnects["peer_closed"] += 1
                    break
                self.result.frames_written += 1
                self.result.bytes_written += HEADER_LEN + len(drained)
            else:
                self.result.completed = True
        finally:
            self.result.peak_queue_events = self.queue.peak_events
            self.result.peak_queue_bytes = self.queue.peak_bytes
            self.metrics.gauge("queue_depth_events", self.queue.depth)
            self.metrics.gauge("queue_depth_bytes", self.queue.byte_depth)
            self.metrics.gauge("peak_queue_bytes", self.queue.peak_bytes)
            self._stop.set()
            try:
                conn.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            conn.close()
        return self.result


def serve_in_thread(
    server: MdpServer, *, accept_timeout: float = 10.0
) -> tuple[threading.Thread, Callable[[], ServeResult]]:
    """Run :meth:`MdpServer.serve_once` on a thread; returns the result getter."""
    holder: dict[str, ServeResult] = {}

    def run() -> None:
        holder["result"] = server.serve_once(accept_timeout=accept_timeout)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, lambda: holder.get("result", server.result)
