"""A queue bounded by **both** item count and byte size.

``asyncio.Queue(maxsize=N)`` bounds items only, and ``maxsize=0`` means
unbounded -- the exact footgun case R01 warns about. A market-data queue needs
the byte bound too: 10,000 queued records is a very different amount of memory
at 250 bytes each than at 4 KiB each, and only the byte figure maps to RSS.

Two put disciplines, because the two sinks need opposite behaviour:

``put``
    Awaits space. This is **backpressure**: a slow disk slows the generator
    down, which is the honest outcome. Used for the file sink, where losing an
    event would corrupt the dataset everything else is compared against.

``put_nowait``
    Raises :class:`QueueFull` immediately. Used for the TCP sink, where a slow
    client must never be able to stall the recorder (section 5.4). The caller
    then applies the configured slow-client policy and counts the overflow.

Concurrency: one producer and one consumer per queue, which is what the engine
does. The waiter bookkeeping is deliberately simple on that basis, and
:meth:`close` wakes everyone so shutdown cannot deadlock.
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Generic, TypeVar

from .metrics import Metrics

T = TypeVar("T")


class QueueFull(Exception):
    """Raised by :meth:`BoundedQueue.put_nowait` when a bound would be exceeded."""

    def __init__(self, reason: str, depth: int, size_bytes: int) -> None:
        super().__init__(f"queue full ({reason}): depth={depth} bytes={size_bytes}")
        self.reason = reason
        self.depth = depth
        self.size_bytes = size_bytes


class QueueClosed(Exception):
    """Raised when putting into a closed queue."""


class BoundedQueue(Generic[T]):
    """FIFO queue with count and byte budgets, plus high-water tracking."""

    def __init__(
        self,
        *,
        max_events: int,
        max_bytes: int,
        name: str = "queue",
        metrics: Metrics | None = None,
    ) -> None:
        if max_events < 1:
            raise ValueError(f"max_events must be >= 1, got {max_events}")
        if max_bytes < 1:
            raise ValueError(f"max_bytes must be >= 1, got {max_bytes}")
        self._items: deque[tuple[T, int]] = deque()
        self._bytes = 0
        self._max_events = max_events
        self._max_bytes = max_bytes
        self._name = name
        self._metrics = metrics
        self._closed = False
        self._not_empty = asyncio.Event()
        self._not_full = asyncio.Event()
        self._not_full.set()
        self._peak_depth = 0
        self._peak_bytes = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def depth(self) -> int:
        return len(self._items)

    @property
    def size_bytes(self) -> int:
        return self._bytes

    @property
    def max_events(self) -> int:
        return self._max_events

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    @property
    def peak_depth(self) -> int:
        return self._peak_depth

    @property
    def peak_bytes(self) -> int:
        return self._peak_bytes

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def utilisation(self) -> float:
        """Fraction of the tighter of the two budgets that is in use."""
        return max(len(self._items) / self._max_events, self._bytes / self._max_bytes)

    def _would_exceed(self, size: int) -> str | None:
        if len(self._items) + 1 > self._max_events:
            return "events"
        if self._bytes + size > self._max_bytes:
            return "bytes"
        return None

    def _after_change(self) -> None:
        depth, size = len(self._items), self._bytes
        self._peak_depth = max(self._peak_depth, depth)
        self._peak_bytes = max(self._peak_bytes, size)
        if self._metrics is not None:
            self._metrics.set_gauge(f"{self._name}_depth", depth)
            self._metrics.set_gauge(f"{self._name}_bytes", size)
        if depth:
            self._not_empty.set()
        else:
            self._not_empty.clear()
        if self._would_exceed(1) is None:
            self._not_full.set()
        else:
            self._not_full.clear()

    def put_nowait(self, item: T, size: int) -> None:
        """Enqueue or raise :class:`QueueFull`. Never blocks, never drops silently."""
        if self._closed:
            raise QueueClosed(f"{self._name} is closed")
        if size > self._max_bytes:
            # A single record larger than the whole budget could never be queued;
            # say so explicitly instead of looping forever (case R03).
            raise QueueFull("record_larger_than_budget", len(self._items), self._bytes)
        reason = self._would_exceed(size)
        if reason is not None:
            raise QueueFull(reason, len(self._items), self._bytes)
        self._items.append((item, size))
        self._bytes += size
        self._after_change()

    async def put(self, item: T, size: int) -> None:
        """Enqueue, awaiting space. This is the backpressure path."""
        if size > self._max_bytes:
            raise QueueFull("record_larger_than_budget", len(self._items), self._bytes)
        while True:
            if self._closed:
                raise QueueClosed(f"{self._name} is closed")
            if self._would_exceed(size) is None:
                self._items.append((item, size))
                self._bytes += size
                self._after_change()
                return
            self._not_full.clear()
            await self._not_full.wait()

    async def get_batch(self, max_items: int, max_bytes: int | None = None) -> list[T]:
        """Await at least one item, then take up to ``max_items`` of them.

        Batching is what makes the writer cheap: one ``write()`` per batch rather
        than per event. Returns ``[]`` only when the queue is closed and drained,
        which is the consumer's signal to finish.
        """
        if max_items < 1:
            raise ValueError(f"max_items must be >= 1, got {max_items}")
        while not self._items:
            if self._closed:
                return []
            await self._not_empty.wait()
        taken: list[T] = []
        taken_bytes = 0
        while self._items and len(taken) < max_items:
            if max_bytes is not None and taken and taken_bytes >= max_bytes:
                break
            item, size = self._items.popleft()
            self._bytes -= size
            taken.append(item)
            taken_bytes += size
        self._after_change()
        return taken

    def drain_nowait(self) -> list[T]:
        """Take everything currently queued without awaiting."""
        taken = [item for item, _ in self._items]
        self._items.clear()
        self._bytes = 0
        self._after_change()
        return taken

    def close(self) -> None:
        """Close the queue and wake every waiter, so shutdown cannot deadlock."""
        self._closed = True
        self._not_empty.set()
        self._not_full.set()

    def stats(self) -> dict[str, int | float]:
        return {
            "depth": self.depth,
            "bytes": self.size_bytes,
            "max_events": self._max_events,
            "max_bytes": self._max_bytes,
            "peak_depth": self._peak_depth,
            "peak_bytes": self._peak_bytes,
            "utilisation": round(self.utilisation, 6),
        }


def time_to_queue_full_seconds(
    budget_events: int, arrival_rate: float, service_rate: float
) -> float | None:
    """How long until a queue of ``budget_events`` fills, given a rate mismatch.

    From section 9.4::

        time_to_full = Q / (arrival_rate - service_rate)

    Returns ``None`` when the service rate keeps up, i.e. the queue never fills.
    Having this in code lets the slow-consumer tests assert they ran for
    *several multiples* of the figure -- otherwise a test can pass simply by
    finishing before the backlog had a chance to matter.
    """
    deficit = arrival_rate - service_rate
    if deficit <= 0:
        return None
    return budget_events / deficit
