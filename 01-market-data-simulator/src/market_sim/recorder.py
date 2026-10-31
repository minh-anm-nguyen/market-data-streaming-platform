"""JSONL recorder, run directory layout and the commit protocol (section 5.1).

The recorded file is the **source of truth** for every other test in this
project, so the durability story has to be stated precisely rather than
optimistically.

Run directory::

    runs/<run_instance_id>/
        manifest.json          written first: config hash, seed, t0, runtime
        events.jsonl.part      the open file; NEVER a finished dataset
        events.jsonl           after atomic rename
        fault-manifest.jsonl   audit trail, deliberately outside the event stream
        report.json            counters, hashes, environment
        COMPLETED              the commit marker, written last

**Commit order matters.** ``events.jsonl`` existing is not enough: a crash
between the rename and the report would leave a plausible-looking file with no
provable contents. So the order is

1. drain the queue, flush, ``fsync(file)``, verify the on-disk size,
2. atomic rename ``.part`` to ``events.jsonl``, ``fsync(directory)``,
3. ``report.json.tmp`` to ``report.json``, ``fsync(directory)``,
4. ``COMPLETED`` marker, ``fsync(directory)``,

and :mod:`market_sim.verify` reports COMPLETE only when the marker, the report
and a recomputed hash all agree (cases F17, F10).

**What is not promised.** ``flush()`` is not durability. Between fsyncs a power
loss can lose the un-fsynced tail; the RPO is whatever ``fsync_every_events`` and
``fsync_every_seconds`` were set to, and the report states both. On Windows a
directory handle cannot be fsynced at all, so ``directory_fsync_supported`` is
reported as false there rather than quietly assumed (case F05).
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from .errors import DiskBudgetError, OutputError
from .metrics import Metrics
from .queues import BoundedQueue

EVENTS_PART_NAME = "events.jsonl.part"
EVENTS_NAME = "events.jsonl"
MANIFEST_NAME = "manifest.json"
REPORT_NAME = "report.json"
FAULT_MANIFEST_NAME = "fault-manifest.jsonl"
COMPLETED_NAME = "COMPLETED"

#: Writing one record per ``write()`` costs a syscall per event. Batching is what
#: keeps the file path from being the bottleneck at 10k+ events/s.
DEFAULT_WRITE_BATCH = 2048

#: Out-of-space conditions. Reported apart from a generic IO error because the
#: operator response differs, and because "disk full" is not always ENOSPC
#: (case F14: a quota shows up as EDQUOT, inode exhaustion as ENOSPC).
_DISK_FULL_ERRNOS = frozenset(
    code
    for code in (getattr(errno, name, None) for name in ("ENOSPC", "EDQUOT", "EFBIG"))
    if code is not None
)


def directory_fsync_supported() -> bool:
    """Whether this platform can fsync a directory handle.

    POSIX can; Windows cannot. Saying so explicitly is better than pretending a
    rename is equally durable everywhere.
    """
    return os.name == "posix"


def fsync_directory(path: Path) -> bool:
    """fsync a directory so a rename is durable. Returns False when unsupported."""
    if not directory_fsync_supported():
        return False
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return True


def wrap_os_error(exc: OSError, what: str, path: Path) -> OutputError:
    """Translate an ``OSError`` into a classified :class:`OutputError`."""
    if exc.errno in _DISK_FULL_ERRNOS:
        return OutputError(
            f"{what} failed: no space or quota left on device ({exc.strerror})",
            code="OUTPUT_DISK_FULL",
            path=str(path),
            errno=exc.errno,
        )
    if exc.errno == errno.EACCES or isinstance(exc, PermissionError):
        return OutputError(
            f"{what} failed: permission denied ({exc.strerror})",
            code="OUTPUT_PERMISSION_DENIED",
            path=str(path),
            errno=exc.errno,
        )
    return OutputError(
        f"{what} failed: {exc.strerror or exc}",
        code="OUTPUT_IO_ERROR",
        path=str(path),
        errno=exc.errno,
    )


def atomic_write_bytes(path: Path, payload: bytes, *, fsync_dir: bool = True) -> bool:
    """Write ``payload`` to ``path`` atomically.

    Via a ``.tmp`` sibling plus ``os.replace``, which is atomic on POSIX and on
    Windows. Returns whether the directory fsync happened, so a caller can report
    the real durability level instead of assuming one.
    """
    tmp = path.with_name(path.name + ".tmp")
    try:
        with open(tmp, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        return fsync_directory(path.parent) if fsync_dir else False
    except OSError as exc:
        tmp.unlink(missing_ok=True)
        raise wrap_os_error(exc, f"writing {path.name}", path) from exc


def atomic_write_json(path: Path, payload: dict[str, Any], *, fsync_dir: bool = True) -> bool:
    data = (json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode(
        "utf-8"
    )
    return atomic_write_bytes(path, data, fsync_dir=fsync_dir)


@dataclass(slots=True)
class CommitState:
    """What the commit protocol actually achieved, step by step."""

    events_renamed: bool = False
    report_written: bool = False
    completed_marker: bool = False
    directory_fsync: bool = False
    events_sha256: str | None = None
    events_bytes: int = 0
    events_lines: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "events_renamed": self.events_renamed,
            "report_written": self.report_written,
            "completed_marker": self.completed_marker,
            "directory_fsync": self.directory_fsync,
            "directory_fsync_supported": directory_fsync_supported(),
            "events_sha256": self.events_sha256,
            "events_bytes": self.events_bytes,
            "events_lines": self.events_lines,
        }


class RunDirectory:
    """Owns a run's output directory and its commit protocol."""

    def __init__(self, root_dir: Path, run_instance_id: str) -> None:
        self.root_dir = Path(root_dir)
        self.run_instance_id = run_instance_id
        self.path = self.root_dir / run_instance_id
        self.state = CommitState()

    @property
    def events_part_path(self) -> Path:
        return self.path / EVENTS_PART_NAME

    @property
    def events_path(self) -> Path:
        return self.path / EVENTS_NAME

    @property
    def manifest_path(self) -> Path:
        return self.path / MANIFEST_NAME

    @property
    def report_path(self) -> Path:
        return self.path / REPORT_NAME

    @property
    def fault_manifest_path(self) -> Path:
        return self.path / FAULT_MANIFEST_NAME

    @property
    def completed_path(self) -> Path:
        return self.path / COMPLETED_NAME

    def create(self) -> None:
        """Create the run directory. Refuses to reuse an existing one (case F09)."""
        try:
            self.root_dir.mkdir(parents=True, exist_ok=True)
            # exist_ok=False: a run directory is immutable, and silently mixing two
            # runs in one directory would make the dataset unexplainable (case C08).
            self.path.mkdir(exist_ok=False)
        except FileExistsError as exc:
            raise OutputError(
                f"run directory already exists: {self.path}; each run gets a unique "
                "run_instance_id and is never overwritten",
                code="OUTPUT_RUN_DIR_EXISTS",
                path=str(self.path),
            ) from exc
        except OSError as exc:
            raise wrap_os_error(exc, "creating run directory", self.path) from exc

    def disk_free_bytes(self) -> int | None:
        try:
            return shutil.disk_usage(str(self.path)).free
        except OSError:
            return None

    def write_manifest(self, payload: dict[str, Any]) -> None:
        atomic_write_json(self.manifest_path, payload)

    def write_fault_manifest(self, lines: Sequence[bytes]) -> None:
        atomic_write_bytes(self.fault_manifest_path, b"".join(lines))

    def commit(self, report: dict[str, Any]) -> CommitState:
        """Run steps 3 and 4 of the commit protocol.

        Step 1 and 2 belong to :meth:`JsonlRecorder.finalize`, which owns the open
        file handle. Splitting them this way keeps each step with the component
        that can actually fail at it.
        """
        fsynced = atomic_write_json(self.report_path, report)
        self.state.report_written = True
        marker = (
            json.dumps(
                {
                    "run_instance_id": self.run_instance_id,
                    "events_sha256": self.state.events_sha256,
                    "events_lines": self.state.events_lines,
                    "events_bytes": self.state.events_bytes,
                    "committed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                },
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        fsynced_marker = atomic_write_bytes(self.completed_path, marker)
        self.state.completed_marker = True
        self.state.directory_fsync = bool(fsynced or fsynced_marker)
        return self.state


class JsonlRecorder:
    """Async consumer that writes canonical JSONL and owns the file durability.

    One writer, one file. The rolling SHA-256 is computed while writing, so
    finalisation costs no extra read; :mod:`market_sim.verify` independently
    recomputes it from disk, which is what catches a bit-flip in a file of the
    right size (case F18).
    """

    def __init__(
        self,
        run_dir: RunDirectory,
        queue: BoundedQueue[bytes],
        *,
        metrics: Metrics,
        fsync_every_events: int,
        fsync_every_seconds: float,
        max_total_bytes: int,
        write_batch: int = DEFAULT_WRITE_BATCH,
        monotonic=time.monotonic,
    ) -> None:
        self._run_dir = run_dir
        self._queue = queue
        self._metrics = metrics
        self._fsync_every_events = fsync_every_events
        self._fsync_every_seconds = fsync_every_seconds
        self._max_total_bytes = max_total_bytes
        self._write_batch = write_batch
        self._monotonic = monotonic

        self._handle = None
        self._bytes_written = 0
        self._events_written = 0
        self._events_fsynced = 0
        self._events_since_fsync = 0
        self._last_fsync_at = 0.0
        self._hash = hashlib.sha256()
        self._closed = False
        self.error: OutputError | None = None

    @property
    def events_written(self) -> int:
        return self._events_written

    @property
    def events_fsynced(self) -> int:
        return self._events_fsynced

    @property
    def bytes_written(self) -> int:
        return self._bytes_written

    @property
    def sha256(self) -> str:
        return self._hash.hexdigest()

    def open(self) -> None:
        path = self._run_dir.events_part_path
        try:
            self._handle = open(path, "wb")
        except OSError as exc:
            raise wrap_os_error(exc, "opening events file", path) from exc
        self._last_fsync_at = self._monotonic()

    async def run(self) -> None:
        """Consume the queue until it is closed and drained.

        On any output error this records it, closes the queue (which releases the
        producer from backpressure) and re-raises. The engine turns that into a
        non-zero exit with no COMPLETED marker: fail closed, never a live-only
        downgrade (case F01).
        """
        if self._handle is None:
            raise OutputError(
                "recorder.run() called before open()", code="OUTPUT_NOT_OPEN"
            )
        try:
            while True:
                batch = await self._queue.get_batch(self._write_batch)
                if not batch:
                    break
                self._write_batch_bytes(batch)
                self._maybe_fsync()
        except OutputError as exc:
            self.error = exc
            self._metrics.incr("file_write_errors_total")
            # Unblock a producer that may be waiting on backpressure, otherwise
            # the run would deadlock instead of failing (case R11).
            self._queue.close()
            raise

    def _write_batch_bytes(self, lines: Sequence[bytes]) -> None:
        data = b"".join(lines)
        projected = self._bytes_written + len(data)
        if projected > self._max_total_bytes:
            raise DiskBudgetError(
                f"max_total_bytes budget exceeded: {projected} > "
                f"{self._max_total_bytes}; stopping before the disk fills",
                path=str(self._run_dir.events_part_path),
                limit=self._max_total_bytes,
                attempted=projected,
            )
        assert self._handle is not None
        try:
            self._handle.write(data)
        except OSError as exc:
            raise wrap_os_error(
                exc, "writing events", self._run_dir.events_part_path
            ) from exc
        self._hash.update(data)
        self._bytes_written += len(data)
        self._events_written += len(lines)
        self._events_since_fsync += len(lines)
        self._metrics.incr("file_events_written_total", len(lines))
        self._metrics.incr("file_bytes_written_total", len(data))

    def _maybe_fsync(self) -> None:
        by_events = (
            self._fsync_every_events > 0
            and self._events_since_fsync >= self._fsync_every_events
        )
        by_time = (
            self._fsync_every_seconds > 0
            and (self._monotonic() - self._last_fsync_at) >= self._fsync_every_seconds
        )
        if by_events or by_time:
            self.fsync()

    def fsync(self) -> None:
        """Flush and fsync. Only after this do we claim events are durable."""
        assert self._handle is not None
        try:
            self._handle.flush()
            os.fsync(self._handle.fileno())
        except OSError as exc:
            raise wrap_os_error(
                exc, "fsyncing events", self._run_dir.events_part_path
            ) from exc
        self._events_fsynced = self._events_written
        self._events_since_fsync = 0
        self._last_fsync_at = self._monotonic()
        self._metrics.incr("file_fsync_total")
        self._metrics.counters["file_events_fsynced_total"] = self._events_fsynced

    def finalize(self) -> CommitState:
        """Steps 1 and 2 of the commit protocol: durable file, then atomic rename."""
        if self._handle is None:
            raise OutputError("finalize() before open()", code="OUTPUT_NOT_OPEN")
        part = self._run_dir.events_part_path
        try:
            self._handle.flush()
            os.fsync(self._handle.fileno())
            # Cross-check what the filesystem actually holds against what we think
            # we wrote. A short file here means a partial write slipped through,
            # and finalising it would publish a truncated dataset (case F04).
            size = os.fstat(self._handle.fileno()).st_size
            if size != self._bytes_written:
                raise OutputError(
                    f"on-disk size {size} does not match {self._bytes_written} bytes "
                    "written; refusing to finalise a short file",
                    code="OUTPUT_SHORT_WRITE",
                    path=str(part),
                    expected=self._bytes_written,
                    actual=size,
                )
            self._handle.close()
        except OSError as exc:
            raise wrap_os_error(exc, "finalising events file", part) from exc
        finally:
            self._closed = True

        self._events_fsynced = self._events_written
        self._metrics.counters["file_events_fsynced_total"] = self._events_fsynced

        try:
            os.replace(part, self._run_dir.events_path)
        except OSError as exc:
            raise wrap_os_error(exc, "renaming events file", part) from exc

        state = self._run_dir.state
        state.events_renamed = True
        state.events_sha256 = self._hash.hexdigest()
        state.events_bytes = self._bytes_written
        state.events_lines = self._events_written
        state.directory_fsync = fsync_directory(self._run_dir.path)
        return state

    def abort(self) -> None:
        """Close the handle on a failed run, leaving ``.part`` in place.

        The partial file is kept on purpose: it is evidence, and
        :mod:`market_sim.verify` can describe exactly how far the run got. What
        must never happen is a COMPLETED marker next to it.
        """
        if self._handle is not None and not self._closed:
            try:
                self._handle.flush()
                os.fsync(self._handle.fileno())
            except OSError:
                # Already failing; the point of abort is to preserve what exists,
                # not to succeed at flushing.
                pass
            try:
                self._handle.close()
            except OSError:
                pass
            self._closed = True

    def stats(self) -> dict[str, Any]:
        return {
            "events_written": self._events_written,
            "events_fsynced": self._events_fsynced,
            "bytes_written": self._bytes_written,
            "sha256": self._hash.hexdigest(),
            "fsync_every_events": self._fsync_every_events,
            "fsync_every_seconds": self._fsync_every_seconds,
            "unfsynced_events": self._events_written - self._events_fsynced,
        }
