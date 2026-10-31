"""Bounded queue behaviour (cases R01, R02, R03, R11, N08, N15).

The queue is where "bounded memory" either is or is not true, so both bounds get
tested independently, and the two put disciplines get tested for the opposite
behaviours the two sinks rely on.
"""

from __future__ import annotations

import asyncio

import pytest

from market_sim.metrics import Metrics
from market_sim.queues import (
    BoundedQueue,
    QueueClosed,
    QueueFull,
    time_to_queue_full_seconds,
)


def q(max_events=10, max_bytes=1000, metrics=None):
    return BoundedQueue(
        max_events=max_events, max_bytes=max_bytes, name="t", metrics=metrics
    )


class TestConstruction:
    @pytest.mark.parametrize("override", [{"max_events": 0}, {"max_bytes": 0}])
    def test_zero_budget_rejected(self, override):
        # maxsize=0 meaning "unbounded" is the footgun this class exists to
        # avoid, so it must not even be constructible (case R01).
        args = {"max_events": 10, "max_bytes": 100}
        args.update(override)
        with pytest.raises(ValueError):
            BoundedQueue(**args)

    def test_starts_empty(self):
        queue = q()
        assert queue.depth == 0 and queue.size_bytes == 0
        assert queue.utilisation == 0.0
        assert not queue.closed


class TestBounds:
    def test_fills_to_the_event_limit(self):
        queue = q(max_events=3, max_bytes=10_000)
        for i in range(3):
            queue.put_nowait(f"e{i}", 1)
        assert queue.depth == 3
        with pytest.raises(QueueFull) as exc:
            queue.put_nowait("overflow", 1)
        assert exc.value.reason == "events"

    def test_byte_limit_bites_before_the_count_limit(self):
        # 10,000 queued records is a very different amount of memory at 250 bytes
        # each than at 4 KiB each, which is why the byte bound exists at all.
        queue = q(max_events=1000, max_bytes=100)
        queue.put_nowait("a", 60)
        with pytest.raises(QueueFull) as exc:
            queue.put_nowait("b", 60)
        assert exc.value.reason == "bytes"
        assert queue.depth == 1

    def test_exactly_at_the_byte_limit_is_accepted(self):
        queue = q(max_events=10, max_bytes=100)
        queue.put_nowait("a", 100)
        assert queue.size_bytes == 100

    def test_record_larger_than_the_whole_budget_is_rejected_immediately(self):
        # Case R03: waiting forever for space that can never exist would be a
        # hang, not backpressure.
        queue = q(max_events=10, max_bytes=100)
        with pytest.raises(QueueFull) as exc:
            queue.put_nowait("huge", 101)
        assert exc.value.reason == "record_larger_than_budget"

    async def test_awaiting_put_also_rejects_an_impossible_record(self):
        queue = q(max_events=10, max_bytes=100)
        with pytest.raises(QueueFull):
            await queue.put("huge", 101)

    def test_bytes_are_released_on_get(self):
        queue = q(max_events=10, max_bytes=100)
        queue.put_nowait("a", 40)
        queue.put_nowait("b", 40)
        assert queue.size_bytes == 80
        assert queue.drain_nowait() == ["a", "b"]
        assert queue.size_bytes == 0


class TestPutDisciplines:
    async def test_put_awaits_space_then_succeeds(self):
        # This is the file sink's backpressure: a slow consumer slows the
        # producer down rather than causing a loss.
        queue = q(max_events=1)
        queue.put_nowait("a", 1)
        task = asyncio.create_task(queue.put("b", 1))
        await asyncio.sleep(0)
        assert not task.done()
        assert await queue.get_batch(1) == ["a"]
        await asyncio.wait_for(task, 1.0)
        assert queue.depth == 1

    async def test_put_raises_when_the_queue_closes_while_waiting(self):
        # Without this, a failed writer would leave the producer blocked forever
        # instead of failing the run (case R11).
        queue = q(max_events=1)
        queue.put_nowait("a", 1)
        task = asyncio.create_task(queue.put("b", 1))
        await asyncio.sleep(0)
        queue.close()
        with pytest.raises(QueueClosed):
            await asyncio.wait_for(task, 1.0)

    def test_put_nowait_on_a_closed_queue_raises(self):
        queue = q()
        queue.close()
        with pytest.raises(QueueClosed):
            queue.put_nowait("a", 1)


class TestConsumer:
    async def test_get_batch_waits_for_an_item(self):
        queue = q()
        task = asyncio.create_task(queue.get_batch(10))
        await asyncio.sleep(0)
        assert not task.done()
        queue.put_nowait("a", 1)
        assert await asyncio.wait_for(task, 1.0) == ["a"]

    async def test_get_batch_takes_up_to_max_items_in_fifo_order(self):
        queue = q(max_events=100)
        for i in range(5):
            queue.put_nowait(i, 1)
        assert await queue.get_batch(3) == [0, 1, 2]
        assert await queue.get_batch(10) == [3, 4]

    async def test_get_batch_returns_empty_only_when_closed_and_drained(self):
        queue = q()
        queue.put_nowait("a", 1)
        queue.close()
        assert await queue.get_batch(10) == ["a"]
        assert await queue.get_batch(10) == []

    async def test_close_wakes_a_waiting_consumer(self):
        queue = q()
        task = asyncio.create_task(queue.get_batch(10))
        await asyncio.sleep(0)
        queue.close()
        assert await asyncio.wait_for(task, 1.0) == []

    async def test_byte_cap_on_a_batch(self):
        queue = q(max_events=100, max_bytes=10_000)
        for i in range(5):
            queue.put_nowait(i, 100)
        batch = await queue.get_batch(100, max_bytes=250)
        assert len(batch) == 3  # stops once the taken bytes reach the cap

    async def test_get_batch_rejects_a_non_positive_limit(self):
        with pytest.raises(ValueError):
            await q().get_batch(0)


class TestObservability:
    def test_high_water_marks_survive_draining(self):
        queue = q(max_events=10, max_bytes=1000)
        for i in range(5):
            queue.put_nowait(i, 100)
        queue.drain_nowait()
        assert queue.peak_depth == 5
        assert queue.peak_bytes == 500
        assert queue.stats()["peak_bytes"] == 500

    def test_gauges_are_published(self):
        metrics = Metrics()
        queue = q(metrics=metrics)
        queue.put_nowait("a", 42)
        assert metrics.gauge("t_depth") == 1
        assert metrics.gauge("t_bytes") == 42
        queue.drain_nowait()
        assert metrics.gauge("t_depth") == 0
        assert metrics.peak("t_bytes") == 42

    def test_utilisation_reports_the_tighter_bound(self):
        queue = q(max_events=10, max_bytes=100)
        queue.put_nowait("a", 50)  # 10% of events, 50% of bytes
        assert queue.utilisation == 0.5


class TestQueueFullMath:
    def test_matches_the_worked_example_from_the_design(self):
        # Section 9.4: lambda=120k, mu=80k, Q=10k -> 0.25 s.
        assert time_to_queue_full_seconds(10_000, 120_000, 80_000) == pytest.approx(0.25)

    @pytest.mark.parametrize("service", [120_000, 200_000])
    def test_none_when_the_consumer_keeps_up(self, service):
        assert time_to_queue_full_seconds(10_000, 120_000, service) is None

    def test_a_bigger_queue_only_buys_time(self):
        # The point of having the formula in code: a slow-consumer test has to
        # run for several multiples of this, or it passes by finishing early.
        small = time_to_queue_full_seconds(10_000, 120_000, 80_000)
        big = time_to_queue_full_seconds(1_000_000, 120_000, 80_000)
        assert big == pytest.approx(small * 100)
