"""The run engine: state machine, routing, shutdown and the run report.

Sections 2, 5.3 and 10 of SYSTEM_DESIGN. The state machine is::

    INIT -> VALIDATING -> RUNNING -> DRAINING -> COMPLETED
                   |          |          |
                   +----------+----------+--> FAILED (non-zero exit, no marker)

Routing policy, which is the heart of the design:

* the **file** queue is awaited (``put``), so a slow disk slows generation down;
* the **TCP** queue is offered (``put_nowait``), so a slow client is disconnected
  instead of being allowed to stall the recorder.

That asymmetry is what makes the claim in section 5.4 true: in ``both`` mode a
badly behaved client cannot cost the dataset a single event, while in ``serve``
mode events genuinely can be missed and the counters say so.

The report reconciles five different counts -- slots, emitted, omissions,
duplicates, recorded -- because a single "events" number would hide exactly the
discrepancies these tests exist to find.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from .clock import RateScheduler, RateWindow, wall_clock_ns
from .config import SimConfig, runtime_fingerprint
from .errors import (
    ConfigError,
    DrainTimeout,
    OutputError,
    RuntimeFailure,
    SchedulerLagExceeded,
    SimError,
)
from .faults import FaultInjector, FaultPlan
from .generator import Generator
from .metrics import RUN_COUNTERS, Metrics
from .model import to_canonical_json
from .queues import BoundedQueue, QueueClosed, QueueFull
from .recorder import JsonlRecorder, RunDirectory, atomic_write_bytes, atomic_write_json
from .tcp_server import TcpSink

try:  # psutil is optional: without it RSS is reported as null, never as a guess.
    import psutil as _psutil
except ImportError:  # pragma: no cover - environment dependent
    _psutil = None


class RunState(str, Enum):
    INIT = "INIT"
    VALIDATING = "VALIDATING"
    RUNNING = "RUNNING"
    DRAINING = "DRAINING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass
class RunResult:
    """Outcome of a run. ``exit_code`` is what the CLI returns."""

    state: RunState
    exit_code: int
    report: dict[str, Any]
    run_dir: Path | None
    error: SimError | None = None

    @property
    def ok(self) -> bool:
        return self.state is RunState.COMPLETED and self.exit_code == 0


def new_run_instance_id() -> str:
    """Unique per run, and deliberately *not* part of ``dataset_id``.

    Two runs of the same config share a dataset id (same bytes) but never a run
    directory, so results can be compared without being overwritten (case C08).
    """
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return f"run-{stamp}-{uuid.uuid4().hex[:8]}"


def _memory_mb() -> dict[str, float | None]:
    """Current and peak process memory.

    ``peak`` is the peak working set for the **whole process lifetime**, not for
    this run: it never decreases, so in a long-lived process that performs many
    runs it describes the process, not the run. Both numbers are reported with
    that spelled out, because quoting the peak as "this run used N MB" would be
    wrong in exactly the case where it matters -- a soak or a test session.
    """
    if _psutil is None:
        return {"current_rss_mb": None, "peak_rss_mb": None, "peak_scope": None}
    try:
        info = _psutil.Process().memory_info()
        peak = getattr(info, "peak_wset", None)  # Windows only
        return {
            "current_rss_mb": round(info.rss / (1024 * 1024), 3),
            "peak_rss_mb": round((peak or info.rss) / (1024 * 1024), 3),
            "peak_scope": "process_lifetime" if peak else "current_rss_only",
        }
    except Exception:  # pragma: no cover - psutil platform differences
        return {"current_rss_mb": None, "peak_rss_mb": None, "peak_scope": None}


def _open_fds() -> int | None:
    if _psutil is None:
        return None
    try:
        proc = _psutil.Process()
        if hasattr(proc, "num_fds"):
            return proc.num_fds()
        return proc.num_handles()  # Windows
    except Exception:  # pragma: no cover
        return None


def _check_plan_matches_config(config: SimConfig, plan: FaultPlan) -> None:
    """Refuse a run whose config and fault plan disagree.

    ``dataset_id`` folds in the plan's fingerprint so that two runs differing
    only in their faults can never share an identity -- a gap in one would
    otherwise look like a contradiction in the other (cases S07, S08). That
    only holds if the fingerprint the config was resolved with is the
    fingerprint of the plan actually being applied, so it is checked here,
    where both are in hand.
    """
    if len(plan) and not config.faults.enabled:
        raise ConfigError(
            f"a fault plan with {len(plan)} fault(s) was supplied but "
            "[faults].enabled is false; the plan would be silently ignored and "
            "the dataset_id would claim the run was clean",
            code="CONFIG_FAULT_PLAN_IGNORED",
            planned=len(plan),
        )
    if not config.faults.enabled:
        return
    expected = plan.fingerprint()
    if config.fault_plan_fingerprint != expected:
        raise ConfigError(
            "the config was resolved with fault_plan_fingerprint="
            f"{config.fault_plan_fingerprint!r} but the supplied plan "
            f"fingerprints as {expected!r}; dataset_id would not identify the "
            "data that is about to be produced",
            code="CONFIG_FAULT_FINGERPRINT_MISMATCH",
            config_fingerprint=config.fault_plan_fingerprint,
            plan_fingerprint=expected,
        )


class SimulatorEngine:
    """Owns one run from validation to commit."""

    def __init__(
        self,
        config: SimConfig,
        *,
        fault_plan: FaultPlan | None = None,
        run_instance_id: str | None = None,
    ) -> None:
        self.config = config
        self.fault_plan = fault_plan or FaultPlan.empty()
        _check_plan_matches_config(config, self.fault_plan)
        self.run_instance_id = run_instance_id or new_run_instance_id()
        self.state = RunState.INIT
        self.metrics = Metrics()
        self.metrics.ensure_counters(RUN_COUNTERS)

        self.generator = Generator(config)
        self.injector = FaultInjector(self.fault_plan, enabled=config.faults.enabled)
        self.run_dir = RunDirectory(config.file.root_dir, self.run_instance_id)

        self._file_queue: BoundedQueue[bytes] | None = None
        self._recorder: JsonlRecorder | None = None
        self._recorder_task: asyncio.Task[None] | None = None
        self._tcp: TcpSink | None = None
        self._tcp_serve_task: asyncio.Task[None] | None = None

        #: Set once the TCP listener is bound, so an embedder (or a test) can
        #: connect without polling. Never set when the TCP sink is disabled.
        self.listening = asyncio.Event()
        self._stop_requested = False
        self._stop_reason: str | None = None
        self._slots_done = 0
        self._emitted = 0
        self._started_wall_ns = 0
        self._started_monotonic = 0.0
        self._finished_monotonic = 0.0
        self._rate_1s = RateWindow(1.0)
        self._rate_60s = RateWindow(60.0)
        self._signal_restore: list[tuple[int, Any]] = []
        self._exit_reason = "not_started"
        self.error: SimError | None = None

    # -- lifecycle --------------------------------------------------------

    @property
    def bound_port(self) -> int | None:
        """The port the TCP sink actually bound, once it is listening."""
        return self._tcp.bound_port if self._tcp is not None else None

    def request_stop(self, reason: str) -> None:
        """Ask the producer to stop and drain. Idempotent and signal-safe."""
        if not self._stop_requested:
            self._stop_requested = True
            self._stop_reason = reason

    async def run(self) -> RunResult:
        self.state = RunState.VALIDATING
        try:
            await self._setup()
            self.state = RunState.RUNNING
            self._install_signal_handlers()
            await self._produce()
            self.state = RunState.DRAINING
            await self._drain()
            self.state = RunState.COMPLETED
            self._exit_reason = self._stop_reason or "slot_count_reached"
            return self._finish(0)
        except SimError as exc:
            self.error = exc
            self.state = RunState.FAILED
            self._exit_reason = f"{exc.code}: {exc.message}"
            await self._abort()
            return self._finish(exc.exit_code, error=exc)
        except asyncio.CancelledError:
            self.error = RuntimeFailure("run cancelled", code="RUNTIME_CANCELLED")
            self.state = RunState.FAILED
            self._exit_reason = "cancelled"
            await self._abort()
            raise
        except Exception as exc:  # unexpected: report it, never swallow it
            wrapped = RuntimeFailure(
                f"unexpected {type(exc).__name__}: {exc}", code="RUNTIME_UNEXPECTED"
            )
            self.error = wrapped
            self.state = RunState.FAILED
            self._exit_reason = wrapped.message
            await self._abort()
            return self._finish(wrapped.exit_code, error=wrapped)
        finally:
            self._restore_signal_handlers()

    async def _setup(self) -> None:
        self._started_wall_ns = wall_clock_ns()
        self.run_dir.create()
        self.run_dir.write_manifest(self._manifest())

        if self.config.file.enabled:
            self._file_queue = BoundedQueue(
                max_events=self.config.file.max_queue_events,
                max_bytes=self.config.file.max_queue_bytes,
                name="file_queue",
                metrics=self.metrics,
            )
            self._recorder = JsonlRecorder(
                self.run_dir,
                self._file_queue,
                metrics=self.metrics,
                fsync_every_events=self.config.file.fsync_every_events,
                fsync_every_seconds=self.config.file.fsync_every_seconds,
                max_total_bytes=self.config.file.max_total_bytes,
            )
            self._recorder.open()
            self._recorder_task = asyncio.create_task(
                self._recorder.run(), name="jsonl-recorder"
            )

        if self.config.tcp.enabled:
            self._tcp = TcpSink(
                self.config.tcp, metrics=self.metrics, injector=self.injector
            )
            await self._tcp.start()
            self.listening.set()
            if self.config.tcp.wait_for_client:
                connected = await self._tcp.wait_for_client(
                    self.config.tcp.startup_timeout_s
                )
                if not connected:
                    # A bounded wait that expires is a result, not a hang.
                    self.metrics.set_gauge("tcp_startup_wait_timed_out", 1)

    async def _produce(self) -> None:
        cfg = self.config
        total = cfg.expected_slots()
        gen = self.generator
        injector = self.injector

        scheduler = RateScheduler(
            cfg.run.rate_per_sec,
            batch_ms=cfg.run.scheduler_batch_ms,
            max_burst_slots=cfg.run.max_burst_slots,
        )
        scheduler.start()
        self._started_monotonic = time.monotonic()
        max_lag_ns = cfg.run.max_scheduler_lag_ms * 1_000_000

        while True:
            if self._stop_requested:
                break
            remaining = None if total is None else total - self._slots_done
            if remaining is not None and remaining <= 0:
                break

            plan = scheduler.plan(remaining=remaining)
            self.metrics.observe("scheduler_lag_ms", plan.lag_ns / 1_000_000)
            if plan.capped_by_burst:
                self.metrics.incr("scheduler_burst_capped_total")
            if plan.lag_ns > max_lag_ns:
                self.metrics.incr("scheduler_lag_breach_total")
                if cfg.run.on_scheduler_lag == "fail-fast":
                    raise SchedulerLagExceeded(
                        f"scheduler lag {plan.lag_ns / 1e6:.1f} ms exceeded "
                        f"max_scheduler_lag_ms={cfg.run.max_scheduler_lag_ms}",
                        lag_ms=plan.lag_ns / 1e6,
                        limit_ms=cfg.run.max_scheduler_lag_ms,
                    )

            if plan.slots == 0:
                await asyncio.sleep(plan.sleep_s)
                continue

            for _ in range(plan.slots):
                slot_index = self._slots_done
                event = gen.generate(slot_index)
                self._slots_done += 1
                self.metrics.incr("logical_slots_total")
                for item in injector.process(event, slot_index):
                    await self._emit(item)
                if self._stop_requested:
                    break
            scheduler.note_produced(plan.slots)
            self._rate_1s.add(plan.slots)
            self._rate_60s.add(plan.slots)
            self.metrics.set_gauge("generated_rate_1s", self._rate_1s.rate())
            self.metrics.set_gauge("generated_rate_60s", self._rate_60s.rate())
            self.metrics.set_gauge("scheduler_effective_rate", scheduler.effective_rate())

            # Hand the loop to the recorder and TCP writer tasks. Without this a
            # CPU-bound generation batch would starve the sinks.
            await asyncio.sleep(0)

        # Release anything the reorder buffer still holds, then settle the audit.
        for item in injector.drain():
            await self._emit(item)
        if injector.held_count:
            raise RuntimeFailure(
                f"{injector.held_count} event(s) left in the reorder buffer after "
                "drain; that would be silent data loss",
                code="RUNTIME_REORDER_LEAK",
            )
        injector.finalize_unreached()
        self._finished_monotonic = time.monotonic()
        self._sync_fault_counters()

    def _sync_fault_counters(self) -> None:
        """Copy the injector's own tallies into the metric registry.

        Omissions in particular cannot be observed downstream -- a withheld event
        produces no item -- so the injector is the only place that knows.
        """
        counts = self.injector.counts()
        self.metrics.counters["intentional_omissions_total"] = self.injector.omissions
        self.metrics.counters["faults_planned_total"] = counts["planned"]
        self.metrics.counters["faults_applied_total"] = counts["applied"]
        self.metrics.counters["faults_not_applied_total"] = counts["not_applied"]

    def _recorder_error(self) -> SimError | None:
        """The writer task's own error, if it failed.

        Preferred over whatever the producer observed, so a run that stopped
        because the disk filled says exactly that.
        """
        if self._recorder is not None and self._recorder.error is not None:
            return self._recorder.error
        task = self._recorder_task
        if task is not None and task.done() and not task.cancelled():
            exc = task.exception()
            if isinstance(exc, SimError):
                return exc
            if exc is not None:
                return RuntimeFailure(
                    f"recorder task failed: {type(exc).__name__}: {exc}",
                    code="RUNTIME_RECORDER_FAILED",
                )
        return None

    async def _emit(self, item: Any) -> None:
        line = to_canonical_json(item.event) + b"\n"
        if len(line) > self.config.file.max_record_bytes:
            # Bounded before it ever reaches a queue (cases D08, R03). Failing the
            # run is deliberate: an oversized record means the contract is broken,
            # and writing it anyway would corrupt the dataset for Project 03.
            raise RuntimeFailure(
                f"record of {len(line)} bytes exceeds max_record_bytes="
                f"{self.config.file.max_record_bytes}",
                code="RUNTIME_RECORD_TOO_LARGE",
                symbol=item.event.symbol,
                sequence=item.event.sequence,
            )

        self._emitted += 1
        self.metrics.incr("events_emitted_total")
        if item.is_duplicate:
            self.metrics.incr("intentional_duplicates_total")
        if item.is_reordered:
            self.metrics.incr("intentional_reorders_total")
        if item.is_mutated:
            self.metrics.incr("intentional_mutations_total")

        if self._file_queue is not None:
            # Awaited: this is where disk backpressure reaches the generator.
            try:
                await self._file_queue.put(line, len(line))
            except (QueueClosed, QueueFull) as exc:
                # A closed queue is a *symptom*: the writer died and closed it to
                # free us. Report the writer's error, because "the queue closed"
                # would tell an operator nothing about the disk being full.
                cause = self._recorder_error()
                if cause is not None:
                    raise cause from exc
                raise RuntimeFailure(
                    f"the file queue rejected a record: {exc}",
                    code="RUNTIME_FILE_QUEUE_REJECTED",
                ) from exc
            self.metrics.incr("file_events_enqueued_total")
            cause = self._recorder_error()
            if cause is not None:
                # The writer died between batches; stop now rather than queue
                # into a dead sink and then report success.
                raise cause

        if self._tcp is not None:
            # Offered: never awaited, so the live channel cannot stall the file.
            self._tcp.publish(line, item.transport_fault)

    async def _drain(self) -> None:
        deadline = self.config.run.shutdown_drain_timeout_s

        if self._file_queue is not None and self._recorder_task is not None:
            self._file_queue.close()
            try:
                await asyncio.wait_for(asyncio.shield(self._recorder_task), deadline)
            except asyncio.TimeoutError:
                pending = self._file_queue.depth
                self._recorder_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._recorder_task
                raise DrainTimeout(
                    f"file queue did not drain within {deadline}s; "
                    f"{pending} event(s) were still pending",
                    pending_events=pending,
                    deadline_s=deadline,
                )

        if self._recorder is not None:
            self._recorder.finalize()

        if self._tcp is not None:
            drained = await self._tcp.shutdown(deadline)
            self.metrics.set_gauge("tcp_shutdown_all_drained", 1 if drained else 0)

        if self.injector.enabled or len(self.fault_plan):
            self.run_dir.write_fault_manifest(
                [record.to_json_line() for record in self.injector.records]
            )

    async def _abort(self) -> None:
        """Best-effort teardown on failure. Keeps evidence, writes no marker."""
        if self._file_queue is not None:
            self._file_queue.close()
        if self._recorder_task is not None:
            if not self._recorder_task.done():
                self._recorder_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._recorder_task
        if self._recorder is not None:
            self._recorder.abort()
        if self._tcp is not None:
            with contextlib.suppress(Exception):
                await self._tcp.shutdown(1.0)
        if len(self.fault_plan):
            with contextlib.suppress(OutputError):
                self.run_dir.write_fault_manifest(
                    [record.to_json_line() for record in self.injector.records]
                )

    # -- signals ----------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        """SIGINT/SIGTERM request a graceful drain; a second SIGINT gives up.

        ``loop.add_signal_handler`` is POSIX-only, so Windows falls back to
        ``signal.signal``. Either way the handler only sets a flag -- doing real
        work in a signal handler is how shutdown deadlocks are built.
        """
        loop = asyncio.get_running_loop()
        interrupts = {"count": 0}

        def on_signal(signum: int) -> None:
            name = signal.Signals(signum).name
            if signum == getattr(signal, "SIGINT", None):
                interrupts["count"] += 1
                if interrupts["count"] >= 2:
                    raise KeyboardInterrupt(
                        "second SIGINT: giving up on a graceful drain"
                    )
            self.request_stop(f"signal:{name}")

        for name in ("SIGTERM", "SIGINT"):
            signum = getattr(signal, name, None)
            if signum is None:
                continue
            try:
                loop.add_signal_handler(signum, on_signal, signum)
                self._signal_restore.append((signum, "loop"))
            except (NotImplementedError, RuntimeError, ValueError):
                try:
                    previous = signal.signal(signum, lambda s, _f: on_signal(s))
                    self._signal_restore.append((signum, previous))
                except (ValueError, OSError):  # pragma: no cover - not main thread
                    pass

    def _restore_signal_handlers(self) -> None:
        loop: asyncio.AbstractEventLoop | None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover
            loop = None
        for signum, previous in self._signal_restore:
            if previous == "loop":
                if loop is not None:
                    with contextlib.suppress(NotImplementedError, RuntimeError):
                        loop.remove_signal_handler(signum)
            else:
                with contextlib.suppress(ValueError, OSError, TypeError):
                    signal.signal(signum, previous)
        self._signal_restore.clear()

    # -- reporting --------------------------------------------------------

    def _manifest(self) -> dict[str, Any]:
        return {
            "run_instance_id": self.run_instance_id,
            "dataset_id": self.config.dataset_id,
            "schema_version": self.config.data_dict()["schema_version"],
            "mode": self.config.run.mode,
            "seed": self.config.run.seed,
            "logical_start_time_ns": self.config.run.start_time_ns,
            "rate_per_sec": self.config.run.rate_per_sec,
            "planned_logical_slots": self.config.expected_slots(),
            "config_sha256": self.config.config_sha256,
            "data_sha256": self.config.data_sha256,
            "config_source": str(self.config.source_path)
            if self.config.source_path
            else None,
            "config_resolved": self.config.full_dict(),
            "faults_enabled": self.config.faults.enabled,
            "fault_plan_fingerprint": self.config.fault_plan_fingerprint,
            "faults_planned": len(self.fault_plan),
            "runtime": runtime_fingerprint(),
            "run_started_at_wall_ns": self._started_wall_ns,
            "run_started_at_utc": time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime(self._started_wall_ns / 1e9)
            ),
            "notes": [
                "Synthetic data. Not market data, not for trading.",
                "A COMPLETED marker proves the file was committed; it never "
                "implies any TCP client received anything.",
            ],
        }

    def _report(self) -> dict[str, Any]:
        # A failed run never reached the end of _produce(), so sync here too:
        # the report must describe the faults that did fire before the failure.
        self._sync_fault_counters()
        fault_counts = self.injector.counts()
        duration_s = (
            max(0.0, (self._finished_monotonic or time.monotonic()) - self._started_monotonic)
            if self._started_monotonic
            else 0.0
        )
        lag = self.metrics.histogram("scheduler_lag_ms")
        recorder_stats = self._recorder.stats() if self._recorder else None
        commit = self.run_dir.state

        omissions = self.metrics.counter("intentional_omissions_total")
        duplicates = self.metrics.counter("intentional_duplicates_total")
        # The algebra from section 7: this is a cross-check, not a hope.
        reconciles = self._emitted == self._slots_done - omissions + duplicates

        report: dict[str, Any] = {
            "project": "fx-cfd-market-data-simulator",
            "schema_version": self.config.data_dict()["schema_version"],
            "status": self.state.value,
            "exit_reason": self._exit_reason,
            "run_instance_id": self.run_instance_id,
            "dataset_id": self.config.dataset_id,
            "config_sha256": self.config.config_sha256,
            "mode": self.config.run.mode,
            "seed": self.config.run.seed,
            "logical_start_time_ns": self.config.run.start_time_ns,
            "target_rate_per_sec": self.config.run.rate_per_sec,
            "planned_logical_slots": self.config.expected_slots(),
            "duration_s": round(duration_s, 6),
            "counts": {
                "logical_slots": self._slots_done,
                "emitted_events": self._emitted,
                "intentional_omissions": omissions,
                "intentional_duplicates": duplicates,
                "intentional_reorders": self.metrics.counter("intentional_reorders_total"),
                "intentional_mutations": self.metrics.counter("intentional_mutations_total"),
                "recorded_events": recorder_stats["events_written"] if recorder_stats else 0,
                "fsynced_events": recorder_stats["events_fsynced"] if recorder_stats else 0,
                "reconciles": reconciles,
                "reconcile_formula": "emitted == slots - omissions + duplicates",
            },
            "rates": {
                "actual_generated_rate_per_sec": round(
                    self._slots_done / duration_s, 3
                )
                if duration_s > 0
                else None,
                "actual_recorded_rate_per_sec": round(
                    (recorder_stats["events_written"] / duration_s), 3
                )
                if recorder_stats and duration_s > 0
                else None,
                "generated_rate_1s_last": round(self.metrics.gauge("generated_rate_1s"), 3),
                "generated_rate_60s_last": round(self.metrics.gauge("generated_rate_60s"), 3),
            },
            "scheduler_lag_ms": lag.as_dict(),
            "p95_scheduler_lag_ms": lag.percentile(95),
            "p99_scheduler_lag_ms": lag.percentile(99),
            "faults": {
                "enabled": self.config.faults.enabled,
                "plan_fingerprint": self.config.fault_plan_fingerprint,
                **fault_counts,
                "records": [record.as_dict() for record in self.injector.records],
            },
            "file": {
                "enabled": self.config.file.enabled,
                **(recorder_stats or {}),
                "events_path": str(self.run_dir.events_path)
                if commit.events_renamed
                else str(self.run_dir.events_part_path),
                "commit": commit.as_dict(),
                "queue": self._file_queue.stats() if self._file_queue else None,
                "durability_note": (
                    "Only fsynced events are durable against power loss. "
                    f"RPO is bounded by fsync_every_events="
                    f"{self.config.file.fsync_every_events} and "
                    f"fsync_every_seconds={self.config.file.fsync_every_seconds}."
                ),
            },
            "tcp": self._tcp.stats() if self._tcp else {"enabled": False},
            "resources": {
                **_memory_mb(),
                "open_fds": _open_fds(),
                "disk_free_bytes": self.run_dir.disk_free_bytes(),
            },
            "generator_state": self.generator.state_snapshot(),
            "metrics": self.metrics.snapshot(),
            "environment": runtime_fingerprint(),
            "known_limitations": [
                "No crash-resume: a killed run is regenerated from seed, never "
                "appended to (ADR-009).",
                "No application-level ACK on TCP, so no event is proven received.",
                "Synthetic prices; not market data and not usable for trading.",
            ],
        }
        if self.error is not None:
            report["error"] = self.error.as_dict()
        return report

    def _finish(self, exit_code: int, *, error: SimError | None = None) -> RunResult:
        report = self._report()
        try:
            if self.state is RunState.COMPLETED and self.config.file.enabled:
                # Only a clean, fully committed file run gets the marker.
                self.run_dir.commit(report)
                report["file"]["commit"] = self.run_dir.state.as_dict()
                # Rewrite the report so it records its own final commit state.
                atomic_write_json(self.run_dir.report_path, report)
            else:
                atomic_write_json(self.run_dir.report_path, report)
        except OutputError as exc:
            # Failing to write the report is itself a failure; we must not return
            # success for a run whose evidence could not be persisted.
            report["report_write_error"] = exc.as_dict()
            self.state = RunState.FAILED
            exit_code = exc.exit_code
            error = error or exc
            with contextlib.suppress(Exception):
                atomic_write_bytes(
                    self.run_dir.path / "report-error.json",
                    json.dumps(report["report_write_error"], indent=2).encode(),
                )
        report["status"] = self.state.value
        return RunResult(
            state=self.state,
            exit_code=exit_code,
            report=report,
            run_dir=self.run_dir.path,
            error=error,
        )


async def run_simulation(
    config: SimConfig,
    *,
    fault_plan: FaultPlan | None = None,
    run_instance_id: str | None = None,
) -> RunResult:
    engine = SimulatorEngine(
        config, fault_plan=fault_plan, run_instance_id=run_instance_id
    )
    return await engine.run()
