"""Sink execution: the retry loop, and two executors behind one protocol.

A sink worker owns exactly one job: send an immutable batch, retry *that same
batch* within the configured bound, and report one terminal result to the
owner. Spec section 4.4 is explicit that a worker never touches the Kafka
consumer, so nothing in this module imports it.

Two executors implement the same protocol:

  * ``ThreadedSinkExecutor`` -- production. A bounded pool, one ClickHouse
    client per thread (spec section 2.3: a worker owns its client rather than
    sharing one loosely), results delivered through a queue the owner drains.
  * ``InlineSinkExecutor`` -- tests. Runs the insert during ``submit`` and
    queues the result. This makes the whole engine single-threaded and
    therefore reproducible, which is what lets the property and model-checking
    tests explore schedules instead of racing them.

The retry loop is shared, so both executors have identical semantics and a test
that passes inline is testing the real policy.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from typing import Protocol, runtime_checkable

from .clickhouse_sink import ClickHouseSink
from .crash_hooks import (
    CRASH_03_AFTER_CH_REQUEST_SENT_BEFORE_RESPONSE,
    NO_CRASH,
    CrashHooks,
)
from .metrics import (
    BATCHES_INSERT_ATTEMPTED,
    CH_BATCHES_ACKED,
    CH_BATCHES_UNKNOWN,
    CH_INSERT_LATENCY,
    MetricsRegistry,
)
from .model import SealedBatch, SinkOutcome, SinkResult
from .retry_policy import RetryPolicy


@runtime_checkable
class SinkExecutor(Protocol):
    """How the owner hands work out and gets results back."""

    def submit(self, batch: SealedBatch) -> None: ...

    def poll_results(self) -> list[SinkResult]: ...

    @property
    def inflight(self) -> int: ...

    def shutdown(self, timeout_s: float) -> bool:
        """Stop accepting work and wait for in-flight batches to settle."""
        ...


def run_batch_with_retries(
    batch: SealedBatch,
    *,
    sink: ClickHouseSink,
    policy: RetryPolicy,
    metrics: MetricsRegistry,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
    crash_hooks: CrashHooks = NO_CRASH,
) -> SinkResult:
    """Send one batch, retrying the identical payload within the budget.

    Returns a single terminal ``SinkResult``. The last outcome is reported as
    it was: an exhausted ``AMBIGUOUS`` stays ambiguous rather than being
    downgraded to a failure, because the rows may well be in the table and the
    operator has to reconcile rather than assume (spec section 7).
    """
    started = monotonic()
    attempts = 0
    last_outcome = SinkOutcome.AMBIGUOUS
    last_detail = "no attempt was made"

    while True:
        attempts += 1
        crash_hooks.maybe_crash(
            CRASH_03_AFTER_CH_REQUEST_SENT_BEFORE_RESPONSE,
            detail=f"batch={batch.batch_id} attempt={attempts}",
        )
        result = sink.insert_batch(batch)
        metrics.inc(BATCHES_INSERT_ATTEMPTED, result_category=str(result.outcome))
        metrics.observe(CH_INSERT_LATENCY, result.latency_s)

        last_outcome = result.outcome
        last_detail = result.detail

        if result.outcome is SinkOutcome.ACKED:
            metrics.inc(CH_BATCHES_ACKED, table="market_events_raw")
            return SinkResult(
                tp=batch.tp,
                assignment_epoch=batch.assignment_epoch,
                batch_id=batch.batch_id,
                outcome=SinkOutcome.ACKED,
                attempts=attempts,
                latency_s=monotonic() - started,
                rows_written_hint=result.rows,
            )

        if result.outcome is SinkOutcome.AMBIGUOUS:
            metrics.inc(CH_BATCHES_UNKNOWN)

        decision = policy.decide(
            outcome=result.outcome,
            attempts_made=attempts,
            elapsed_s=monotonic() - started,
        )
        if not decision.should_retry:
            return SinkResult(
                tp=batch.tp,
                assignment_epoch=batch.assignment_epoch,
                batch_id=batch.batch_id,
                outcome=last_outcome,
                attempts=attempts,
                latency_s=monotonic() - started,
                detail=f"{last_detail} | stopped: {decision.reason}",
            )
        if decision.delay_s > 0:
            sleep(decision.delay_s)


class InlineSinkExecutor:
    """Runs each batch synchronously inside ``submit``. Tests only.

    ``inflight`` is always 0 after ``submit`` returns, which means a test can
    rely on the engine reaching a stable state after a bounded number of steps.
    """

    def __init__(
        self,
        *,
        sink: ClickHouseSink,
        policy: RetryPolicy,
        metrics: MetricsRegistry,
        monotonic: Callable[[], float],
        sleep: Callable[[float], None] = lambda _: None,
        crash_hooks: CrashHooks = NO_CRASH,
    ) -> None:
        self._sink = sink
        self._policy = policy
        self._metrics = metrics
        self._monotonic = monotonic
        self._sleep = sleep
        self._crash_hooks = crash_hooks
        self._results: list[SinkResult] = []
        self._closed = False

    def submit(self, batch: SealedBatch) -> None:
        if self._closed:
            raise RuntimeError("executor is shut down")
        self._results.append(
            run_batch_with_retries(
                batch,
                sink=self._sink,
                policy=self._policy,
                metrics=self._metrics,
                monotonic=self._monotonic,
                sleep=self._sleep,
                crash_hooks=self._crash_hooks,
            )
        )

    def poll_results(self) -> list[SinkResult]:
        out, self._results = self._results, []
        return out

    @property
    def inflight(self) -> int:
        return 0

    def shutdown(self, timeout_s: float) -> bool:
        self._closed = True
        self._sink.close()
        return True


class ThreadedSinkExecutor:
    """Bounded thread pool, one ClickHouse client per worker thread."""

    def __init__(
        self,
        *,
        sink_factory: Callable[[], ClickHouseSink],
        policy: RetryPolicy,
        metrics: MetricsRegistry,
        workers: int,
        monotonic: Callable[[], float],
        sleep: Callable[[float], None],
        crash_hooks: CrashHooks = NO_CRASH,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be >= 1")
        self._policy = policy
        self._metrics = metrics
        self._monotonic = monotonic
        self._sleep = sleep
        self._crash_hooks = crash_hooks
        # Bounded queue: the owner must not be able to pile up unbounded work
        # that is invisible to the byte budget.
        self._work: queue.Queue[SealedBatch | None] = queue.Queue(maxsize=workers * 2)
        self._results: queue.Queue[SinkResult] = queue.Queue()
        self._inflight = 0
        self._inflight_lock = threading.Lock()
        self._stopping = threading.Event()
        self._threads = [
            threading.Thread(
                target=self._worker_loop,
                args=(sink_factory,),
                name=f"p05-sink-{i}",
                daemon=True,
            )
            for i in range(workers)
        ]
        for thread in self._threads:
            thread.start()

    def _worker_loop(self, sink_factory: Callable[[], ClickHouseSink]) -> None:
        sink = sink_factory()
        try:
            while True:
                batch = self._work.get()
                if batch is None:
                    return
                try:
                    result = run_batch_with_retries(
                        batch,
                        sink=sink,
                        policy=self._policy,
                        metrics=self._metrics,
                        monotonic=self._monotonic,
                        sleep=self._sleep,
                        crash_hooks=self._crash_hooks,
                    )
                except BaseException as exc:  # noqa: BLE001
                    # A worker that dies silently would leave the partition
                    # in-flight forever and the frontier stuck. Report the
                    # failure as an ambiguous outcome instead.
                    result = SinkResult(
                        tp=batch.tp,
                        assignment_epoch=batch.assignment_epoch,
                        batch_id=batch.batch_id,
                        outcome=SinkOutcome.AMBIGUOUS,
                        attempts=0,
                        latency_s=0.0,
                        detail=f"sink worker raised {type(exc).__name__}: {exc}",
                    )
                finally:
                    with self._inflight_lock:
                        self._inflight -= 1
                self._results.put(result)
        finally:
            sink.close()

    def submit(self, batch: SealedBatch) -> None:
        with self._inflight_lock:
            self._inflight += 1
        try:
            self._work.put(batch, timeout=30.0)
        except queue.Full:  # pragma: no cover - the owner respects the slot cap
            with self._inflight_lock:
                self._inflight -= 1
            raise

    def poll_results(self) -> list[SinkResult]:
        out: list[SinkResult] = []
        while True:
            try:
                out.append(self._results.get_nowait())
            except queue.Empty:
                return out

    @property
    def inflight(self) -> int:
        with self._inflight_lock:
            return self._inflight

    def shutdown(self, timeout_s: float) -> bool:
        """Signal the workers and wait. Returns False if the deadline passed."""
        self._stopping.set()
        for _ in self._threads:
            self._work.put(None)
        deadline = self._monotonic() + timeout_s
        for thread in self._threads:
            remaining = max(0.0, deadline - self._monotonic())
            thread.join(timeout=remaining)
        return not any(t.is_alive() for t in self._threads)
