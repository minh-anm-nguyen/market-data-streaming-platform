"""Recorder, run directory and the commit protocol (section 5.1, cases F01-F18).

The claim being tested is narrow and important: a COMPLETED marker means the
file was committed, and nothing weaker ever produces one.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys

import pytest

from market_sim.errors import DiskBudgetError, OutputError
from market_sim.metrics import Metrics
from market_sim.queues import BoundedQueue
from market_sim.recorder import (
    COMPLETED_NAME,
    EVENTS_NAME,
    EVENTS_PART_NAME,
    JsonlRecorder,
    RunDirectory,
    atomic_write_json,
    directory_fsync_supported,
    fsync_directory,
    wrap_os_error,
)

LINES = [b'{"a":1}\n', b'{"a":2}\n', b'{"a":3}\n']


def make(tmp_path, *, max_total_bytes=10**9, fsync_events=0, fsync_seconds=0.0):
    run_dir = RunDirectory(tmp_path / "runs", "run-test-1")
    run_dir.create()
    metrics = Metrics()
    queue: BoundedQueue[bytes] = BoundedQueue(
        max_events=100, max_bytes=10**6, name="file_queue", metrics=metrics
    )
    recorder = JsonlRecorder(
        run_dir,
        queue,
        metrics=metrics,
        fsync_every_events=fsync_events,
        fsync_every_seconds=fsync_seconds,
        max_total_bytes=max_total_bytes,
    )
    return run_dir, queue, recorder, metrics


class TestRunDirectory:
    def test_creates_the_directory(self, tmp_path):
        run_dir = RunDirectory(tmp_path / "runs", "run-a")
        run_dir.create()
        assert run_dir.path.is_dir()

    def test_refuses_to_reuse_an_existing_run_directory(self, tmp_path):
        # Case F09/C08: mixing two runs in one directory would make the dataset
        # unexplainable, so a collision is an error rather than an overwrite.
        run_dir = RunDirectory(tmp_path / "runs", "run-a")
        run_dir.create()
        with pytest.raises(OutputError) as exc:
            RunDirectory(tmp_path / "runs", "run-a").create()
        assert exc.value.code == "OUTPUT_RUN_DIR_EXISTS"

    def test_manifest_is_written_atomically_and_parses(self, tmp_path):
        run_dir = RunDirectory(tmp_path / "runs", "run-a")
        run_dir.create()
        run_dir.write_manifest({"seed": 20261030})
        assert json.loads(run_dir.manifest_path.read_text())["seed"] == 20261030
        assert not (run_dir.manifest_path.with_name("manifest.json.tmp")).exists()

    def test_disk_free_is_reported(self, tmp_path):
        run_dir = RunDirectory(tmp_path / "runs", "run-a")
        run_dir.create()
        assert (run_dir.disk_free_bytes() or 0) > 0


class TestWriting:
    async def test_writes_batches_and_tracks_counters(self, tmp_path):
        run_dir, queue, recorder, metrics = make(tmp_path)
        recorder.open()
        for line in LINES:
            queue.put_nowait(line, len(line))
        queue.close()
        await recorder.run()
        assert recorder.events_written == 3
        assert recorder.bytes_written == sum(len(line) for line in LINES)
        assert metrics.counter("file_events_written_total") == 3
        assert metrics.counter("file_bytes_written_total") == recorder.bytes_written

    async def test_part_file_is_used_until_finalise(self, tmp_path):
        run_dir, queue, recorder, _ = make(tmp_path)
        recorder.open()
        queue.put_nowait(LINES[0], len(LINES[0]))
        queue.close()
        await recorder.run()
        assert run_dir.events_part_path.exists()
        assert not run_dir.events_path.exists()

    async def test_rolling_hash_matches_a_recomputed_one(self, tmp_path):
        run_dir, queue, recorder, _ = make(tmp_path)
        recorder.open()
        for line in LINES:
            queue.put_nowait(line, len(line))
        queue.close()
        await recorder.run()
        recorder.finalize()
        on_disk = hashlib.sha256(run_dir.events_path.read_bytes()).hexdigest()
        assert recorder.sha256 == on_disk

    async def test_empty_run_produces_an_empty_but_valid_file(self, tmp_path):
        # Case T03: count=0 is a complete dataset with zero events.
        run_dir, queue, recorder, _ = make(tmp_path)
        recorder.open()
        queue.close()
        await recorder.run()
        state = recorder.finalize()
        assert run_dir.events_path.read_bytes() == b""
        assert state.events_lines == 0
        assert state.events_sha256 == hashlib.sha256(b"").hexdigest()

    async def test_max_total_bytes_aborts_before_filling_the_disk(self, tmp_path):
        # Case F11: stop before the volume fills, not after.
        run_dir, queue, recorder, metrics = make(tmp_path, max_total_bytes=10)
        recorder.open()
        for line in LINES:
            queue.put_nowait(line, len(line))
        queue.close()
        with pytest.raises(DiskBudgetError) as exc:
            await recorder.run()
        assert exc.value.code == "OUTPUT_BUDGET_EXCEEDED"
        assert metrics.counter("file_write_errors_total") == 1
        assert recorder.error is not None

    async def test_a_write_failure_closes_the_queue_so_the_producer_is_freed(
        self, tmp_path
    ):
        # Case R11: a dead writer must fail the run, not deadlock it.
        run_dir, queue, recorder, _ = make(tmp_path, max_total_bytes=10)
        recorder.open()
        for line in LINES:
            queue.put_nowait(line, len(line))
        queue.close()
        with pytest.raises(DiskBudgetError):
            await recorder.run()
        assert queue.closed

    async def test_run_before_open_is_an_error(self, tmp_path):
        _, _, recorder, _ = make(tmp_path)
        with pytest.raises(OutputError) as exc:
            await recorder.run()
        assert exc.value.code == "OUTPUT_NOT_OPEN"

    def test_open_on_a_missing_directory_is_classified(self, tmp_path):
        run_dir = RunDirectory(tmp_path / "runs", "run-x")
        metrics = Metrics()
        queue: BoundedQueue[bytes] = BoundedQueue(
            max_events=10, max_bytes=1000, name="q", metrics=metrics
        )
        recorder = JsonlRecorder(
            run_dir,
            queue,
            metrics=metrics,
            fsync_every_events=0,
            fsync_every_seconds=0.0,
            max_total_bytes=1000,
        )
        with pytest.raises(OutputError):
            recorder.open()


class TestFsyncPolicy:
    async def test_fsynced_count_lags_the_written_count_without_a_policy(self, tmp_path):
        # The honest distinction: written means the API accepted it, fsynced
        # means it survives a power cut. With both policies off, nothing is
        # fsynced until finalise.
        _, queue, recorder, metrics = make(tmp_path, fsync_events=0, fsync_seconds=0.0)
        recorder.open()
        for line in LINES:
            queue.put_nowait(line, len(line))
        queue.close()
        await recorder.run()
        assert recorder.events_written == 3
        assert recorder.events_fsynced == 0
        assert recorder.stats()["unfsynced_events"] == 3

    async def test_count_based_fsync_fires(self, tmp_path):
        _, queue, recorder, metrics = make(tmp_path, fsync_events=2)
        recorder.open()
        for line in LINES:
            queue.put_nowait(line, len(line))
        queue.close()
        await recorder.run()
        assert metrics.counter("file_fsync_total") >= 1
        assert recorder.events_fsynced == 3

    async def test_finalise_fsyncs_everything(self, tmp_path):
        _, queue, recorder, _ = make(tmp_path)
        recorder.open()
        for line in LINES:
            queue.put_nowait(line, len(line))
        queue.close()
        await recorder.run()
        recorder.finalize()
        assert recorder.events_fsynced == recorder.events_written == 3
        assert recorder.stats()["unfsynced_events"] == 0

    async def test_stats_disclose_the_rpo_settings(self, tmp_path):
        _, queue, recorder, _ = make(tmp_path, fsync_events=100, fsync_seconds=5.0)
        recorder.open()
        queue.close()
        await recorder.run()
        stats = recorder.stats()
        assert stats["fsync_every_events"] == 100
        assert stats["fsync_every_seconds"] == 5.0


class TestCommitProtocol:
    async def _complete(self, tmp_path):
        run_dir, queue, recorder, _ = make(tmp_path)
        recorder.open()
        for line in LINES:
            queue.put_nowait(line, len(line))
        queue.close()
        await recorder.run()
        recorder.finalize()
        return run_dir, recorder

    async def test_finalise_renames_part_to_events(self, tmp_path):
        run_dir, _ = await self._complete(tmp_path)
        assert run_dir.events_path.is_file()
        assert not run_dir.events_part_path.exists()

    async def test_marker_is_written_last_and_carries_the_hash(self, tmp_path):
        run_dir, recorder = await self._complete(tmp_path)
        assert not run_dir.completed_path.exists()  # not until commit()
        state = run_dir.commit({"status": "COMPLETED"})
        marker = json.loads(run_dir.completed_path.read_text())
        assert marker["events_sha256"] == recorder.sha256
        assert marker["events_lines"] == 3
        assert marker["events_bytes"] == recorder.bytes_written
        assert state.completed_marker is True
        assert state.report_written is True

    async def test_commit_writes_the_report(self, tmp_path):
        run_dir, _ = await self._complete(tmp_path)
        run_dir.commit({"status": "COMPLETED", "counts": {"recorded_events": 3}})
        assert json.loads(run_dir.report_path.read_text())["status"] == "COMPLETED"

    async def test_commit_state_reports_the_real_durability_level(self, tmp_path):
        # Case F05: on Windows a directory handle cannot be fsynced, and the
        # report says so instead of implying the rename is equally durable.
        run_dir, _ = await self._complete(tmp_path)
        state = run_dir.commit({"status": "COMPLETED"})
        as_dict = state.as_dict()
        assert as_dict["directory_fsync_supported"] == directory_fsync_supported()
        if directory_fsync_supported():
            assert as_dict["directory_fsync"] is True
        else:
            assert as_dict["directory_fsync"] is False

    async def test_abort_leaves_the_part_file_and_no_marker(self, tmp_path):
        # Case F03: the partial file is evidence. What must never appear next to
        # it is a COMPLETED marker.
        run_dir, queue, recorder, _ = make(tmp_path)
        recorder.open()
        queue.put_nowait(LINES[0], len(LINES[0]))
        queue.close()
        await recorder.run()
        recorder.abort()
        assert run_dir.events_part_path.is_file()
        assert not run_dir.events_path.exists()
        assert not run_dir.completed_path.exists()

    async def test_abort_is_idempotent(self, tmp_path):
        _, queue, recorder, _ = make(tmp_path)
        recorder.open()
        queue.close()
        await recorder.run()
        recorder.abort()
        recorder.abort()

    def test_finalise_before_open_is_an_error(self, tmp_path):
        _, _, recorder, _ = make(tmp_path)
        with pytest.raises(OutputError) as exc:
            recorder.finalize()
        assert exc.value.code == "OUTPUT_NOT_OPEN"

    async def test_short_file_is_refused_rather_than_published(self, tmp_path):
        # Case F04: if the on-disk size disagrees with what we think we wrote,
        # finalising would publish a truncated dataset.
        run_dir, queue, recorder, _ = make(tmp_path)
        recorder.open()
        for line in LINES:
            queue.put_nowait(line, len(line))
        queue.close()
        await recorder.run()
        recorder._bytes_written += 100  # pretend a write was lost
        with pytest.raises(OutputError) as exc:
            recorder.finalize()
        assert exc.value.code == "OUTPUT_SHORT_WRITE"
        assert not run_dir.events_path.exists()


class TestErrorClassification:
    @pytest.mark.parametrize(
        "errno_name,expected",
        [
            ("ENOSPC", "OUTPUT_DISK_FULL"),
            ("EDQUOT", "OUTPUT_DISK_FULL"),
            ("EACCES", "OUTPUT_PERMISSION_DENIED"),
            ("EIO", "OUTPUT_IO_ERROR"),
        ],
    )
    def test_os_errors_are_classified(self, tmp_path, errno_name, expected):
        # Case F14: "disk full" is not always ENOSPC -- a quota is EDQUOT, and
        # the operator response differs from a generic IO error.
        import errno as errno_mod

        code = getattr(errno_mod, errno_name, None)
        if code is None:
            pytest.skip(f"{errno_name} not defined on this platform")
        exc = OSError(code, os.strerror(code))
        wrapped = wrap_os_error(exc, "writing", tmp_path / "f")
        assert wrapped.code == expected
        assert wrapped.exit_code == 3

    def test_permission_error_subclass_is_classified(self, tmp_path):
        wrapped = wrap_os_error(
            PermissionError(13, "Permission denied"), "writing", tmp_path / "f"
        )
        assert wrapped.code == "OUTPUT_PERMISSION_DENIED"

    def test_error_context_names_the_path(self, tmp_path):
        wrapped = wrap_os_error(OSError(5, "I/O error"), "writing", tmp_path / "f")
        assert wrapped.context["path"] == str(tmp_path / "f")

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="chmod on a directory does not block writes on Windows",
    )
    def test_unwritable_directory_fails_fast(self, tmp_path):
        # Case F02: fail before producing anything, not after a long run.
        root = tmp_path / "readonly"
        root.mkdir()
        os.chmod(root, 0o500)
        try:
            with pytest.raises(OutputError) as exc:
                RunDirectory(root, "run-a").create()
            assert exc.value.code in (
                "OUTPUT_PERMISSION_DENIED",
                "OUTPUT_IO_ERROR",
            )
        finally:
            os.chmod(root, 0o700)


class TestAtomicWrites:
    def test_no_temp_file_is_left_behind(self, tmp_path):
        target = tmp_path / "report.json"
        atomic_write_json(target, {"a": 1})
        assert target.is_file()
        assert list(tmp_path.iterdir()) == [target]

    def test_overwrite_is_atomic_and_complete(self, tmp_path):
        target = tmp_path / "report.json"
        atomic_write_json(target, {"a": 1})
        atomic_write_json(target, {"b": 2})
        assert json.loads(target.read_text()) == {"b": 2}

    def test_json_is_newline_terminated(self, tmp_path):
        target = tmp_path / "report.json"
        atomic_write_json(target, {"a": 1})
        assert target.read_bytes().endswith(b"\n")

    def test_failure_cleans_up_the_temp_file(self, tmp_path):
        missing = tmp_path / "nope" / "report.json"
        with pytest.raises(OutputError):
            atomic_write_json(missing, {"a": 1})
        assert not (tmp_path / "nope").exists()

    def test_fsync_directory_reports_support_honestly(self, tmp_path):
        assert fsync_directory(tmp_path) == directory_fsync_supported()


class TestFileNames:
    def test_the_names_the_verifier_depends_on(self):
        # These strings are a contract between the recorder and the verifier.
        assert EVENTS_NAME == "events.jsonl"
        assert EVENTS_PART_NAME == "events.jsonl.part"
        assert COMPLETED_NAME == "COMPLETED"
