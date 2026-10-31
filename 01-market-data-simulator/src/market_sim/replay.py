"""Replay a recorded dataset without altering it (FR-12, cases S14, E2E-10).

The contract is narrow on purpose: **replay changes timing, never payload.**
Bytes, order, sequence numbers and ``event_time_ns`` all come straight from the
file. Re-deriving timestamps at replay time would destroy the one property that
makes the dataset useful as a fixture.

Pacing is derived from the *recorded* ``event_time_ns`` deltas divided by
``speed``, measured against the monotonic clock. ``speed=0`` means as fast as
possible. Because pacing is separate from content, the output of a ``speed=1``
and a ``speed=10`` replay are byte-identical files, which is exactly what
E2E-10 asserts.

The input is validated before anything is emitted, and the file's hash is
checked again at the end: a file edited while being read is rejected rather than
half-replayed (case F15).
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import SimConfig
from .errors import ReplayError
from .metrics import RUN_COUNTERS, Metrics
from .model import RecordParseError, parse_canonical_line
from .recorder import (
    COMPLETED_NAME,
    EVENTS_NAME,
    JsonlRecorder,
    RunDirectory,
    atomic_write_json,
)
from .tcp_server import TcpSink
from .verify import Verdict, hash_file, iter_lines, verify_run

NS_PER_SEC = 1_000_000_000


@dataclass
class ReplaySource:
    """A validated replay input."""

    path: Path
    sha256: str
    bytes_total: int
    lines: int
    dataset_id: str
    schema_version: str
    first_event_time_ns: int
    last_event_time_ns: int
    run_dir: Path | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "sha256": self.sha256,
            "bytes": self.bytes_total,
            "lines": self.lines,
            "dataset_id": self.dataset_id,
            "schema_version": self.schema_version,
            "first_event_time_ns": self.first_event_time_ns,
            "last_event_time_ns": self.last_event_time_ns,
            "source_run_dir": str(self.run_dir) if self.run_dir else None,
        }


def open_replay_source(
    input_path: str | Path,
    *,
    require_completed: bool = True,
    max_line_bytes: int = 1 << 20,
) -> ReplaySource:
    """Validate a replay input and summarise it. Raises :class:`ReplayError`.

    Accepts either a run directory or a path to ``events.jsonl``. When the input
    is a run directory the full verifier runs first, so a replay never silently
    propagates a corrupt or uncommitted dataset (case C03/C07).
    """
    path = Path(input_path).resolve()
    run_dir: Path | None = None

    if path.is_dir():
        run_dir = path
        result = verify_run(path, max_line_bytes=max_line_bytes)
        if result.verdict is not Verdict.COMPLETE and require_completed:
            raise ReplayError(
                f"refusing to replay run {path.name}: verdict {result.verdict.value}",
                code="REPLAY_SOURCE_NOT_COMPLETE",
                verdict=result.verdict.value,
                reasons=result.reasons[:5],
            )
        path = path / EVENTS_NAME

    if not path.is_file():
        raise ReplayError(
            f"replay input not found: {path}", code="REPLAY_INPUT_NOT_FOUND", path=str(path)
        )

    if require_completed and run_dir is None:
        marker = path.parent / COMPLETED_NAME
        if not marker.is_file():
            raise ReplayError(
                f"{path.name} has no {COMPLETED_NAME} marker beside it, so it is not "
                "a committed dataset; pass --allow-incomplete to replay it anyway",
                code="REPLAY_SOURCE_NOT_COMPLETE",
                path=str(path),
            )

    sha256, total = hash_file(path)

    lines = 0
    dataset_ids: set[str] = set()
    schema_versions: set[str] = set()
    first_ns: int | None = None
    last_ns = 0
    try:
        for line_number, raw, had_newline in iter_lines(path, max_line_bytes=max_line_bytes):
            if not had_newline:
                raise ReplayError(
                    f"line {line_number} is not newline-terminated: the file ends "
                    "mid-record and cannot be replayed faithfully",
                    code="REPLAY_TORN_FINAL_LINE",
                    path=str(path),
                    line=line_number,
                )
            if not raw.strip():
                continue
            try:
                event = parse_canonical_line(raw, max_bytes=max_line_bytes)
            except RecordParseError as exc:
                raise ReplayError(
                    f"line {line_number} is not a valid sim.v1 record: "
                    f"{exc.reason} {exc.detail}",
                    code="REPLAY_RECORD_INVALID",
                    path=str(path),
                    line=line_number,
                    reason=exc.reason,
                ) from exc
            lines += 1
            dataset_ids.add(event.dataset_id)
            schema_versions.add(event.schema_version)
            if first_ns is None:
                first_ns = event.event_time_ns
            last_ns = max(last_ns, event.event_time_ns)
    except RecordParseError as exc:
        raise ReplayError(
            f"cannot read {path}: {exc.reason} {exc.detail}",
            code="REPLAY_RECORD_INVALID",
            path=str(path),
        ) from exc
    except OSError as exc:
        raise ReplayError(
            f"cannot read {path}: {exc}", code="REPLAY_IO_ERROR", path=str(path)
        ) from exc

    if len(dataset_ids) > 1:
        raise ReplayError(
            f"file mixes {len(dataset_ids)} dataset ids; refusing to replay a "
            "mixture as one dataset",
            code="REPLAY_MIXED_DATASETS",
            dataset_ids=sorted(dataset_ids),
        )
    if len(schema_versions) > 1:
        raise ReplayError(
            f"file mixes schema versions {sorted(schema_versions)}",
            code="REPLAY_MIXED_SCHEMA",
        )

    return ReplaySource(
        path=path,
        sha256=sha256,
        bytes_total=total,
        lines=lines,
        dataset_id=next(iter(dataset_ids)) if dataset_ids else "",
        schema_version=next(iter(schema_versions)) if schema_versions else "",
        first_event_time_ns=first_ns or 0,
        last_event_time_ns=last_ns,
        run_dir=run_dir,
    )


@dataclass
class ReplayResult:
    status: str
    exit_code: int
    report: dict[str, Any]
    run_dir: Path | None
    error: ReplayError | None = None

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


class Replayer:
    """Re-emits a recorded dataset to a file and/or a TCP client."""

    def __init__(
        self,
        source: ReplaySource,
        config: SimConfig,
        *,
        speed: float = 1.0,
        run_instance_id: str | None = None,
    ) -> None:
        if speed < 0:
            raise ValueError(f"speed must be >= 0, got {speed}")
        self.source = source
        self.config = config
        self.speed = speed
        self.metrics = Metrics()
        self.metrics.ensure_counters(RUN_COUNTERS)
        from .engine import new_run_instance_id

        self.run_instance_id = run_instance_id or new_run_instance_id()
        self.run_dir = RunDirectory(config.file.root_dir, self.run_instance_id)
        self._emitted = 0
        self._stop = False

    async def run(self) -> ReplayResult:
        self.run_dir.create()
        self.run_dir.write_manifest(
            {
                "run_instance_id": self.run_instance_id,
                "mode": "replay",
                "dataset_id": self.source.dataset_id,
                "schema_version": self.source.schema_version,
                "replay_source": self.source.as_dict(),
                "speed": self.speed,
                "config_sha256": self.config.config_sha256,
                "notes": [
                    "Replay reproduces recorded payload bytes exactly; only the "
                    "emission timing differs.",
                ],
            }
        )

        recorder: JsonlRecorder | None = None
        tcp: TcpSink | None = None
        error: ReplayError | None = None
        started = time.monotonic()

        try:
            if self.config.file.enabled:
                from .queues import BoundedQueue

                queue: BoundedQueue[bytes] = BoundedQueue(
                    max_events=self.config.file.max_queue_events,
                    max_bytes=self.config.file.max_queue_bytes,
                    name="file_queue",
                    metrics=self.metrics,
                )
                recorder = JsonlRecorder(
                    self.run_dir,
                    queue,
                    metrics=self.metrics,
                    fsync_every_events=self.config.file.fsync_every_events,
                    fsync_every_seconds=self.config.file.fsync_every_seconds,
                    max_total_bytes=self.config.file.max_total_bytes,
                )
                recorder.open()
                writer_task = asyncio.create_task(recorder.run(), name="replay-recorder")
            else:
                queue = None  # type: ignore[assignment]
                writer_task = None

            if self.config.tcp.enabled:
                tcp = TcpSink(self.config.tcp, metrics=self.metrics)
                await tcp.start()
                if self.config.tcp.wait_for_client:
                    await tcp.wait_for_client(self.config.tcp.startup_timeout_s)

            await self._pump(queue, tcp)

            if queue is not None and writer_task is not None:
                queue.close()
                await asyncio.wait_for(
                    writer_task, self.config.run.shutdown_drain_timeout_s
                )
            if recorder is not None:
                recorder.finalize()
            if tcp is not None:
                await tcp.shutdown(self.config.run.shutdown_drain_timeout_s)

            # The source must still be the file we validated. Re-hashing catches
            # a file that was edited or truncated while we were reading it (F15).
            after_sha, _ = hash_file(self.source.path)
            if after_sha != self.source.sha256:
                raise ReplayError(
                    "replay source changed while it was being read: hash was "
                    f"{self.source.sha256}, now {after_sha}",
                    code="REPLAY_SOURCE_MUTATED",
                    path=str(self.source.path),
                )
            status, exit_code = "COMPLETED", 0
        except ReplayError as exc:
            error, status, exit_code = exc, "FAILED", exc.exit_code
            if recorder is not None:
                recorder.abort()
        except asyncio.TimeoutError:
            error = ReplayError(
                "replay drain timed out", code="REPLAY_DRAIN_TIMEOUT"
            )
            status, exit_code = "FAILED", error.exit_code
            if recorder is not None:
                recorder.abort()

        duration = max(0.0, time.monotonic() - started)
        report = {
            "project": "fx-cfd-market-data-simulator",
            "mode": "replay",
            "status": status,
            "run_instance_id": self.run_instance_id,
            "dataset_id": self.source.dataset_id,
            "speed": self.speed,
            "source": self.source.as_dict(),
            "counts": {
                "source_lines": self.source.lines,
                "emitted_events": self._emitted,
                "recorded_events": recorder.events_written if recorder else 0,
                "logical_slots": self.source.lines,
                "intentional_omissions": 0,
                "intentional_duplicates": 0,
                "reconciles": self._emitted == self.source.lines,
                "reconcile_formula": "emitted == source_lines (replay adds nothing)",
            },
            "duration_s": round(duration, 6),
            "file": {
                "enabled": self.config.file.enabled,
                **(recorder.stats() if recorder else {}),
                "commit": self.run_dir.state.as_dict(),
            },
            "tcp": tcp.stats() if tcp else {"enabled": False},
            "metrics": self.metrics.snapshot(),
            "known_limitations": [
                "Replay preserves payload bytes and order; it does not re-time "
                "event_time_ns, so transport-age metrics are meaningless on an "
                "accelerated replay.",
            ],
        }
        if error is not None:
            report["error"] = error.as_dict()

        if status == "COMPLETED" and self.config.file.enabled:
            self.run_dir.commit(report)
            report["file"]["commit"] = self.run_dir.state.as_dict()
        atomic_write_json(self.run_dir.report_path, report)
        return ReplayResult(
            status=status,
            exit_code=exit_code,
            report=report,
            run_dir=self.run_dir.path,
            error=error,
        )

    async def _pump(self, queue: Any, tcp: TcpSink | None) -> None:
        """Walk the source file, pacing against the recorded event times."""
        base_event_ns: int | None = None
        base_monotonic_ns = time.monotonic_ns()

        for _, raw, had_newline in iter_lines(
            self.source.path, max_line_bytes=self.config.file.max_record_bytes
        ):
            if self._stop:
                break
            if not had_newline or not raw.strip():
                continue
            event = parse_canonical_line(raw)
            if base_event_ns is None:
                base_event_ns = event.event_time_ns

            if self.speed > 0:
                target_offset_ns = int(
                    (event.event_time_ns - base_event_ns) / self.speed
                )
                due_ns = base_monotonic_ns + target_offset_ns
                wait_ns = due_ns - time.monotonic_ns()
                if wait_ns > 0:
                    await asyncio.sleep(wait_ns / NS_PER_SEC)

            # The recorded bytes go out unchanged -- this is the whole contract.
            line = raw + b"\n"
            self._emitted += 1
            self.metrics.incr("events_emitted_total")
            if queue is not None:
                await queue.put(line, len(line))
                self.metrics.incr("file_events_enqueued_total")
            if tcp is not None:
                tcp.publish(line)
            if self._emitted % 1000 == 0:
                await asyncio.sleep(0)

    def request_stop(self) -> None:
        self._stop = True


async def replay_dataset(
    input_path: str | Path,
    config: SimConfig,
    *,
    speed: float = 1.0,
    require_completed: bool = True,
    run_instance_id: str | None = None,
) -> ReplayResult:
    source = open_replay_source(input_path, require_completed=require_completed)
    replayer = Replayer(
        source, config, speed=speed, run_instance_id=run_instance_id
    )
    return await replayer.run()
