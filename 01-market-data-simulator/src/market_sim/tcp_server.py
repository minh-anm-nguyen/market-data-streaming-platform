"""Loopback TCP sink: live, best-effort, and honest about it (section 5.2).

The wire format is one canonical JSON record per line, terminated by ``\\n``.
TCP is a byte stream, so a client **must** buffer by delimiter: one ``read()``
may hold 100 records, or half of one (cases N04, N05, N06).

What this sink does and does not prove, in the exact words the metrics use:

``tcp_events_enqueued_total``
    accepted into this client's queue. Nothing has been sent.
``tcp_events_written_total``
    handed to ``StreamWriter.write()``. Still only in a buffer.
``tcp_bytes_drained_total``
    ``drain()`` returned, so the *local* send buffer emptied. This says nothing
    about the peer application having parsed or stored anything.

There is no ACK, so ``client_acknowledged`` is simply ``unknown`` -- see NFR-08
and case N09. Claiming delivery from ``drain()`` would be the single most
tempting lie available in this design, so no counter is named for it.

Backpressure policy: the queue is bounded by count and bytes, ``put_nowait``
refuses rather than waits, and a client that stops reading is disconnected once
its queue overflows or ``drain()`` exceeds ``send_timeout_ms``. A slow client
therefore cannot slow the recorder down or grow RSS without bound (cases N03,
N08, R02). In ``both`` mode the file keeps every event regardless.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from typing import Any

from .config import TcpOutputConfig
from .errors import OutputError
from .faults import FaultSpec
from .metrics import Metrics
from .queues import BoundedQueue, QueueClosed, QueueFull

#: One queue item: the bytes to send, plus an optional transport fault directive.
QueueItem = tuple[bytes, FaultSpec | None]

#: How many records the writer hands to one ``write()`` call.
WRITE_BATCH = 512

#: Default bytes for a ``malformed_json`` fault, when the plan does not name a
#: payload. Deliberately not valid JSON and deliberately newline-terminated, so
#: the receiver's framing stays intact while its parser has to reject the record.
DEFAULT_MALFORMED_PAYLOAD = b'{"schema_version":"sim.v1","event_type":"quo\n'


@dataclass
class ClientStats:
    """Per-connection accounting, kept after disconnect for the run report."""

    client_id: int
    peer: str
    enqueued: int = 0
    written: int = 0
    bytes_drained: int = 0
    overflow: int = 0
    bytes_from_client: int = 0
    disconnect_reason: str | None = None
    transport_faults_applied: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "client_id": self.client_id,
            "peer": self.peer,
            "events_enqueued": self.enqueued,
            "events_written": self.written,
            "bytes_drained": self.bytes_drained,
            "queue_overflow": self.overflow,
            "bytes_from_client": self.bytes_from_client,
            "disconnect_reason": self.disconnect_reason,
            "transport_faults_applied": self.transport_faults_applied,
            "client_acknowledged": "unknown",
        }


class ClientSession:
    """One connected client: its bounded queue and its writer task."""

    def __init__(
        self,
        client_id: int,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        config: TcpOutputConfig,
        metrics: Metrics,
    ) -> None:
        self.client_id = client_id
        self._reader = reader
        self._writer = writer
        self._config = config
        self._metrics = metrics
        peer = writer.get_extra_info("peername")
        self.stats = ClientStats(client_id=client_id, peer=str(peer))
        self.queue: BoundedQueue[QueueItem] = BoundedQueue(
            max_events=config.max_queue_events,
            max_bytes=config.max_queue_bytes,
            name=f"tcp_queue_c{client_id}",
            metrics=metrics,
        )
        self.closed = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._eof_task: asyncio.Task[None] | None = None
        self._should_close = False

    def start(self) -> None:
        self._task = asyncio.create_task(
            self._writer_loop(), name=f"tcp-writer-{self.client_id}"
        )
        self._eof_task = asyncio.create_task(
            self._watch_for_eof(), name=f"tcp-eof-{self.client_id}"
        )

    async def _watch_for_eof(self) -> None:
        """Notice a vanished peer while the writer is idle.

        The protocol is one-way, so nothing the client sends is interpreted --
        but without reading at all we would only discover a disconnect on the
        next write, which on a quiet feed may be never. A dead session would then
        keep holding the single client slot and its file descriptor, so a new and
        perfectly good client would be turned away as 'busy' (cases N02, N11).
        """
        try:
            while True:
                data = await self._reader.read(4096)
                if not data:
                    break
                # Unexpected, but worth counting: a client talking to a one-way
                # feed is a sign of a protocol misunderstanding.
                self.stats.bytes_from_client += len(data)
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            pass
        except asyncio.CancelledError:
            raise
        finally:
            if self.active:
                self.stats.disconnect_reason = (
                    self.stats.disconnect_reason or "peer_closed_connection"
                )
                # Wake the writer so it can drain what is queued and tear down.
                self.queue.close()

    @property
    def active(self) -> bool:
        return not self.closed.is_set()

    def enqueue(self, line: bytes, fault: FaultSpec | None) -> bool:
        """Try to queue one record. Returns False on overflow (never blocks)."""
        if not self.active:
            return False
        try:
            self.queue.put_nowait((line, fault), len(line))
        except (QueueFull, QueueClosed):
            self.stats.overflow += 1
            self._metrics.incr("tcp_queue_overflow_total")
            return False
        self.stats.enqueued += 1
        self._metrics.incr("tcp_events_enqueued_total")
        return True

    async def _writer_loop(self) -> None:
        timeout = self._config.send_timeout_ms / 1000.0
        try:
            while True:
                batch = await self.queue.get_batch(WRITE_BATCH)
                if not batch:
                    break
                chunks: list[bytes] = []
                close_after = False
                for line, fault in batch:
                    if fault is None:
                        chunks.append(line)
                        continue
                    payload, close_now, note = self._apply_transport_fault(line, fault)
                    if payload:
                        chunks.append(payload)
                    self.stats.transport_faults_applied.append(note)
                    self._metrics.incr("tcp_transport_faults_total")
                    if close_now:
                        close_after = True
                        break

                if chunks:
                    data = b"".join(chunks)
                    self._writer.write(data)
                    self.stats.written += len(chunks)
                    self._metrics.incr("tcp_events_written_total", len(chunks))
                    try:
                        await asyncio.wait_for(self._writer.drain(), timeout)
                    except asyncio.TimeoutError:
                        # The peer is not reading. Disconnect rather than let the
                        # transport buffer and this queue grow (case N03).
                        self.stats.disconnect_reason = (
                            f"slow_client: drain() exceeded {self._config.send_timeout_ms} ms"
                        )
                        self._metrics.incr("tcp_slow_disconnect_total")
                        break
                    except (ConnectionError, OSError) as exc:
                        self.stats.disconnect_reason = f"connection_error: {exc}"
                        self._metrics.incr("tcp_send_errors_total")
                        break
                    # drain() returned: the local buffer emptied. That is all.
                    self.stats.bytes_drained += len(data)
                    self._metrics.incr("tcp_bytes_drained_total", len(data))

                if close_after or self._should_close:
                    self.stats.disconnect_reason = (
                        self.stats.disconnect_reason or "fault_injected_close"
                    )
                    break
        except asyncio.CancelledError:
            self.stats.disconnect_reason = self.stats.disconnect_reason or "cancelled"
            raise
        finally:
            await self._teardown()

    def _apply_transport_fault(
        self, line: bytes, fault: FaultSpec
    ) -> tuple[bytes, bool, str]:
        """Return ``(bytes_to_send, close_after, audit_note)``."""
        if fault.kind == "truncated_line":
            cut = fault.truncate_bytes or max(1, len(line) // 2)
            return (
                line[:cut],
                True,
                f"{fault.fault_id}:truncated_line sent {cut}/{len(line)} bytes then closed",
            )
        if fault.kind == "malformed_json":
            payload = (
                fault.payload.encode("utf-8")
                if fault.payload is not None
                else DEFAULT_MALFORMED_PAYLOAD
            )
            if not payload.endswith(b"\n"):
                payload += b"\n"
            return (
                payload,
                False,
                f"{fault.fault_id}:malformed_json replaced one record with "
                f"{len(payload)} invalid bytes",
            )
        if fault.kind == "close_connection":
            return (
                line,
                True,
                f"{fault.fault_id}:close_connection sent the record then closed",
            )
        return line, False, f"{fault.fault_id}:{fault.kind} not applicable on transport"

    async def _teardown(self) -> None:
        self.queue.close()
        if self._eof_task is not None and not self._eof_task.done():
            self._eof_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._eof_task
        with contextlib.suppress(ConnectionError, OSError):
            self._writer.close()
        with contextlib.suppress(ConnectionError, OSError, asyncio.TimeoutError):
            await asyncio.wait_for(self._writer.wait_closed(), 5.0)
        self.closed.set()
        self._metrics.incr("tcp_clients_disconnected_total")

    async def request_close(self, reason: str) -> None:
        self.stats.disconnect_reason = self.stats.disconnect_reason or reason
        self._should_close = True
        self.queue.close()
        if self._task is not None:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(self._task), 5.0)
            if not self._task.done():
                self._task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._task

    async def drain_and_close(self, timeout: float) -> bool:
        """Let the writer finish its queue, then close. True if it drained in time."""
        self.queue.close()
        if self._task is None:
            return True
        try:
            await asyncio.wait_for(asyncio.shield(self._task), timeout)
            return True
        except asyncio.TimeoutError:
            self.stats.disconnect_reason = (
                self.stats.disconnect_reason or "drain_timeout_on_shutdown"
            )
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            return False


class TcpSink:
    """The listener plus the active client sessions.

    MVP policy is ``max_clients = 1``: a second connection is **rejected**
    immediately rather than silently given its own unbounded fan-out queue (case
    N11). Fan-out, per-client replay cursors and auth are a later milestone, not
    something to half-build here.
    """

    def __init__(
        self, config: TcpOutputConfig, *, metrics: Metrics, injector=None
    ) -> None:
        self._config = config
        self._metrics = metrics
        self._injector = injector
        self._server: asyncio.AbstractServer | None = None
        self._sessions: dict[int, ClientSession] = {}
        self._finished: list[ClientStats] = []
        self._next_client_id = 1
        self._client_connected = asyncio.Event()
        self.peak_queue_bytes = 0
        self.peak_queue_depth = 0

    @property
    def bound_host(self) -> str:
        return self._config.host

    @property
    def bound_port(self) -> int:
        if self._server is None:
            return self._config.port
        for sock in self._server.sockets or ():
            return int(sock.getsockname()[1])
        return self._config.port

    @property
    def active_clients(self) -> int:
        return sum(1 for s in self._sessions.values() if s.active)

    async def start(self) -> None:
        try:
            self._server = await asyncio.start_server(
                self._handle_client, self._config.host, self._config.port
            )
        except OSError as exc:
            # A busy port must fail the run before anything is produced, so no
            # manifest ever claims a successful serve (case N12).
            raise OutputError(
                f"cannot bind {self._config.host}:{self._config.port}: "
                f"{exc.strerror or exc}",
                code="OUTPUT_PORT_BIND_FAILED",
                host=self._config.host,
                port=self._config.port,
                errno=exc.errno,
            ) from exc

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        if self.active_clients >= self._config.max_clients:
            self._metrics.incr("tcp_clients_rejected_total")
            with contextlib.suppress(ConnectionError, OSError):
                writer.close()
            with contextlib.suppress(ConnectionError, OSError, asyncio.TimeoutError):
                await asyncio.wait_for(writer.wait_closed(), 5.0)
            return

        client_id = self._next_client_id
        self._next_client_id += 1
        session = ClientSession(
            client_id, reader, writer, config=self._config, metrics=self._metrics
        )
        self._sessions[client_id] = session
        self._metrics.incr("tcp_clients_accepted_total")
        session.start()
        self._client_connected.set()

        await session.closed.wait()
        self._collect(client_id)

    def _collect(self, client_id: int) -> None:
        session = self._sessions.pop(client_id, None)
        if session is None:
            return
        self.peak_queue_bytes = max(self.peak_queue_bytes, session.queue.peak_bytes)
        self.peak_queue_depth = max(self.peak_queue_depth, session.queue.peak_depth)
        self._finished.append(session.stats)

    async def wait_for_client(self, timeout: float) -> bool:
        """Wait for a first connection, bounded. Never waits forever (section 5.2)."""
        if self.active_clients:
            return True
        try:
            await asyncio.wait_for(self._client_connected.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def publish(self, line: bytes, fault: FaultSpec | None = None) -> None:
        """Offer one record to every active client. Never blocks, never raises.

        With no client this counts ``tcp_no_client_events_total`` and returns:
        the run keeps producing on schedule rather than hanging (case N01).
        """
        sessions = [s for s in self._sessions.values() if s.active]
        if not sessions:
            self._metrics.incr("tcp_no_client_events_total")
            if fault is not None and self._injector is not None:
                self._injector.note_transport_skipped(
                    fault, "no client was connected when the targeted event was emitted"
                )
            return
        accepted_any = False
        for session in sessions:
            if session.enqueue(line, fault):
                accepted_any = True
        if fault is not None and self._injector is not None:
            if accepted_any:
                self._injector.note_transport_applied(
                    fault,
                    f"{fault.kind} queued for the TCP client; see the client's "
                    "transport_faults_applied list for what was actually sent",
                )
            else:
                self._injector.note_transport_skipped(
                    fault, "targeted event was dropped by TCP queue overflow"
                )

    async def shutdown(self, drain_timeout: float) -> bool:
        """Stop accepting, drain each client within the deadline, then close.

        Returns whether every client drained in time. A False here is reported,
        not hidden: on the live channel some events may simply never have left.
        """
        if self._server is not None:
            # Stop accepting immediately, but do NOT await wait_closed() yet.
            # Since Python 3.12 it waits for every connection handler to return,
            # and ours returns only once its session closes -- which happens in
            # the drain loop below. Awaiting it first is a shutdown deadlock, the
            # exact outcome section 5.4 rules out.
            self._server.close()

        all_drained = True
        for client_id in list(self._sessions):
            session = self._sessions.get(client_id)
            if session is None:
                continue
            drained = await session.drain_and_close(drain_timeout)
            all_drained = all_drained and drained
            self._collect(client_id)

        if self._server is not None:
            # Bounded even now: a listener that will not close must not be able
            # to hold the whole run open.
            with contextlib.suppress(Exception, asyncio.TimeoutError):
                await asyncio.wait_for(self._server.wait_closed(), 5.0)
        return all_drained

    def client_reports(self) -> list[dict[str, Any]]:
        live = [s.stats.as_dict() for s in self._sessions.values()]
        return [s.as_dict() for s in self._finished] + live

    def stats(self) -> dict[str, Any]:
        queue_peak_bytes = self.peak_queue_bytes
        queue_peak_depth = self.peak_queue_depth
        for session in self._sessions.values():
            queue_peak_bytes = max(queue_peak_bytes, session.queue.peak_bytes)
            queue_peak_depth = max(queue_peak_depth, session.queue.peak_depth)
        return {
            "host": self._config.host,
            "port": self.bound_port,
            "max_clients": self._config.max_clients,
            "clients": self.client_reports(),
            "peak_queue_bytes": queue_peak_bytes,
            "peak_queue_depth": queue_peak_depth,
            "delivery_guarantee": (
                "best-effort live stream; no application ACK, so no event is "
                "proven to have been received or stored by any client"
            ),
        }
