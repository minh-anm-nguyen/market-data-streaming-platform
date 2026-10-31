"""The sink executors, including the threaded one used in production.

The integration suite drives ``InlineSinkExecutor`` so that scenarios stay
deterministic. That leaves ``ThreadedSinkExecutor`` -- the executor that
actually runs in production -- untested by those tests, which is the wrong gap
to leave open. It needs no live infrastructure: real threads against the
in-memory sink exercise the queueing, the in-flight accounting and the shutdown
path exactly as they run for real.

Both executors share ``run_batch_with_retries``, so a retry-semantics test
against either one covers the real policy.
"""

from __future__ import annotations

import threading
import time

import pytest

from p05_ingest.config import RetryConfig
from p05_ingest.metrics import (
    BATCHES_INSERT_ATTEMPTED,
    CH_BATCHES_ACKED,
    CH_BATCHES_UNKNOWN,
    CH_INSERT_LATENCY,
    MetricsRegistry,
)
from p05_ingest.model import (
    EventKind,
    EventRow,
    LogicalProjection,
    SealedBatch,
    SealReason,
    SinkOutcome,
    TopicPartition,
    TransportCoordinate,
)
from p05_ingest.retry_policy import RetryPolicy
from p05_ingest.sink_worker import (
    InlineSinkExecutor,
    ThreadedSinkExecutor,
    run_batch_with_retries,
)
from p05_ingest.testkit import Behaviour, FakeClickHouse, FakeClock

TP0 = TopicPartition("md.ticks.v1", 0)


def make_batch(*, offsets=(0, 1), epoch: int = 1, token: str = "t" * 64) -> SealedBatch:
    rows = []
    for offset in offsets:
        projection = LogicalProjection(
            dataset_id="demo",
            symbol="EURUSD",
            sequence=offset,
            event_time_ns=1_704_171_845_000_000_000 + offset,
            price_scale=100_000,
            kind=EventKind.TICK,
            source_schema_version="marketdata.v1",
            price_ticks=100_000 + offset,
        )
        rows.append(
            EventRow(
                projection=projection,
                coordinate=TransportCoordinate(
                    "c1", "gen-a", TP0.topic, TP0.partition, offset
                ),
                payload_digest=f"{offset:064d}",
                ingest_run_id="run-1",
                accounted_bytes=100,
            )
        )
    return SealedBatch(
        tp=TP0,
        assignment_epoch=epoch,
        rows=tuple(rows),
        batch_id=f"b-{offsets[0]}-{offsets[-1]}",
        insert_token=token,
        seal_reason=SealReason.MAX_ROWS,
        accounted_bytes=100 * len(rows),
        sealed_at_monotonic=0.0,
    )


@pytest.fixture
def policy() -> RetryPolicy:
    return RetryPolicy(
        RetryConfig(
            max_attempts=4,
            initial_backoff_ms=10,
            max_backoff_ms=40,
            overall_deadline_s=5.0,
        )
    )


# --------------------------------------------------------- the shared retry loop


def test_a_clean_insert_reports_acked_once(policy):
    clock = FakeClock()
    metrics = MetricsRegistry()
    sink = FakeClickHouse()

    result = run_batch_with_retries(
        make_batch(),
        sink=sink,
        policy=policy,
        metrics=metrics,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result.outcome is SinkOutcome.ACKED
    assert result.attempts == 1
    assert result.rows_written_hint == 2
    assert metrics.counter_total(CH_BATCHES_ACKED) == 1
    assert metrics.histogram(CH_INSERT_LATENCY).total == 1
    assert clock.sleeps == [], "no backoff on success"


def test_retries_send_the_identical_batch(policy):
    clock = FakeClock()
    sink = FakeClickHouse(
        script=[Behaviour.REFUSE_BEFORE_WRITE, Behaviour.REFUSE_BEFORE_WRITE],
        default_behaviour=Behaviour.ACK,
    )
    batch = make_batch(offsets=(5, 6, 7))

    result = run_batch_with_retries(
        batch,
        sink=sink,
        policy=policy,
        metrics=MetricsRegistry(),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result.outcome is SinkOutcome.ACKED
    assert result.attempts == 3
    assert {a.token for a in sink.attempts} == {batch.insert_token}
    assert {a.rows for a in sink.attempts} == {3}
    assert len(clock.sleeps) == 2, "one backoff between each attempt"


def test_an_exhausted_ambiguous_outcome_stays_ambiguous(policy):
    """It must not be downgraded to a failure: the rows may be in the table."""
    clock = FakeClock()
    metrics = MetricsRegistry()
    sink = FakeClickHouse(default_behaviour=Behaviour.WRITE_THEN_LOSE_ACK)

    result = run_batch_with_retries(
        make_batch(),
        sink=sink,
        policy=policy,
        metrics=metrics,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result.outcome is SinkOutcome.AMBIGUOUS
    assert result.attempts == 4
    assert "stopped:" in result.detail
    assert metrics.counter_total(CH_BATCHES_UNKNOWN) == 4
    assert sink.row_count == 2, "the first attempt really did write the rows"


def test_a_permanent_error_is_reported_after_one_attempt(policy):
    clock = FakeClock()
    metrics = MetricsRegistry()
    sink = FakeClickHouse(default_behaviour=Behaviour.PERMANENT_ERROR)

    result = run_batch_with_retries(
        make_batch(),
        sink=sink,
        policy=policy,
        metrics=metrics,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result.outcome is SinkOutcome.PERMANENT
    assert result.attempts == 1
    assert (
        metrics.counter(
            BATCHES_INSERT_ATTEMPTED, result_category=str(SinkOutcome.PERMANENT)
        )
        == 1
    )


def test_the_result_carries_the_epoch_for_fencing(policy):
    clock = FakeClock()
    result = run_batch_with_retries(
        make_batch(epoch=7),
        sink=FakeClickHouse(),
        policy=policy,
        metrics=MetricsRegistry(),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    assert result.assignment_epoch == 7
    assert result.tp == TP0


# ------------------------------------------------------------ inline executor


def test_inline_executor_completes_during_submit(policy):
    clock = FakeClock()
    sink = FakeClickHouse()
    executor = InlineSinkExecutor(
        sink=sink,
        policy=policy,
        metrics=MetricsRegistry(),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    executor.submit(make_batch())
    assert executor.inflight == 0, "the inline executor is synchronous"

    results = executor.poll_results()
    assert len(results) == 1
    assert executor.poll_results() == [], "results are drained once"

    assert executor.shutdown(1.0)
    assert sink.closed
    with pytest.raises(RuntimeError, match="shut down"):
        executor.submit(make_batch())


# ---------------------------------------------------------- threaded executor


def test_threaded_executor_completes_every_submitted_batch(policy):
    """The production path: real threads, one sink per thread."""
    sinks: list[FakeClickHouse] = []

    def factory() -> FakeClickHouse:
        sink = FakeClickHouse()
        sinks.append(sink)
        return sink

    executor = ThreadedSinkExecutor(
        sink_factory=factory,
        policy=policy,
        metrics=MetricsRegistry(),
        workers=3,
        monotonic=time.monotonic,
        sleep=time.sleep,
    )
    try:
        batches = [
            make_batch(offsets=(i * 2, i * 2 + 1), token=f"{i:064d}")
            for i in range(12)
        ]
        for batch in batches:
            executor.submit(batch)

        collected = []
        deadline = time.monotonic() + 30
        while len(collected) < len(batches) and time.monotonic() < deadline:
            collected.extend(executor.poll_results())
        assert len(collected) == len(batches), "every batch must report back"
        assert all(r.outcome is SinkOutcome.ACKED for r in collected)
        assert {r.batch_id for r in collected} == {b.batch_id for b in batches}
    finally:
        assert executor.shutdown(10.0), "all worker threads must exit"

    assert len(sinks) == 3, "one sink per worker thread, not one shared client"
    assert all(s.closed for s in sinks), "each worker closes its own client"
    assert sum(s.row_count for s in sinks) == 24


def test_threaded_executor_tracks_inflight_and_drains_to_zero(policy):
    release = threading.Event()

    class BlockingSink(FakeClickHouse):
        def insert_batch(self, batch):
            release.wait(timeout=20)
            return super().insert_batch(batch)

    executor = ThreadedSinkExecutor(
        sink_factory=BlockingSink,
        policy=policy,
        metrics=MetricsRegistry(),
        workers=2,
        monotonic=time.monotonic,
        sleep=time.sleep,
    )
    try:
        for i in range(4):
            executor.submit(make_batch(offsets=(i,), token=f"{i:064d}"))
        assert executor.inflight > 0, "work is outstanding while the sink blocks"

        release.set()
        deadline = time.monotonic() + 30
        collected = []
        while len(collected) < 4 and time.monotonic() < deadline:
            collected.extend(executor.poll_results())
        assert len(collected) == 4
        assert executor.inflight == 0, "in-flight must return to zero"
    finally:
        release.set()
        executor.shutdown(10.0)


def test_a_worker_exception_is_reported_as_ambiguous(policy):
    """A worker that died silently would stall its partition forever.

    The outcome is AMBIGUOUS rather than a failure: the request may have
    reached the server before the worker broke.
    """

    class ExplodingSink(FakeClickHouse):
        def insert_batch(self, batch):
            raise MemoryError("worker exploded mid-insert")

    executor = ThreadedSinkExecutor(
        sink_factory=ExplodingSink,
        policy=policy,
        metrics=MetricsRegistry(),
        workers=1,
        monotonic=time.monotonic,
        sleep=time.sleep,
    )
    try:
        executor.submit(make_batch())
        deadline = time.monotonic() + 30
        results = []
        while not results and time.monotonic() < deadline:
            results = executor.poll_results()
        assert len(results) == 1
        assert results[0].outcome is SinkOutcome.AMBIGUOUS
        assert "MemoryError" in results[0].detail
        assert executor.inflight == 0, "the slot must be released even on a crash"
    finally:
        executor.shutdown(10.0)


def test_threaded_executor_rejects_a_zero_worker_pool():
    with pytest.raises(ValueError, match="workers must be >= 1"):
        ThreadedSinkExecutor(
            sink_factory=FakeClickHouse,
            policy=RetryPolicy(RetryConfig()),
            metrics=MetricsRegistry(),
            workers=0,
            monotonic=time.monotonic,
            sleep=time.sleep,
        )


def test_metrics_are_safe_to_update_from_several_workers(policy):
    """Spec ADR-002: workers increment counters from their own threads."""
    metrics = MetricsRegistry()
    executor = ThreadedSinkExecutor(
        sink_factory=FakeClickHouse,
        policy=policy,
        metrics=metrics,
        workers=4,
        monotonic=time.monotonic,
        sleep=time.sleep,
    )
    total = 40
    try:
        for i in range(total):
            executor.submit(make_batch(offsets=(i,), token=f"{i:064d}"))
        collected = []
        deadline = time.monotonic() + 60
        while len(collected) < total and time.monotonic() < deadline:
            collected.extend(executor.poll_results())
        assert len(collected) == total
    finally:
        executor.shutdown(15.0)

    assert metrics.counter_total(CH_BATCHES_ACKED) == total
    assert metrics.histogram(CH_INSERT_LATENCY).total == total
