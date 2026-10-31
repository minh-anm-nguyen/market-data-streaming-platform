"""Disk and filesystem failure (E2E-07, cases F01, F02, F04, F10, F14).

An honest note on coverage: a *real* ENOSPC needs a constrained filesystem -- a
small loop device, a container quota -- and is not something a portable test
suite can create. What is tested here is the handling path: the errors are
injected at the syscall boundary, so the classification, the fail-closed
behaviour and the absence of a COMPLETED marker are all genuinely exercised. The
README records "real full volume" as a manual scenario that has not been run.
"""

from __future__ import annotations

import errno
import os
import sys

import pytest

from market_sim.engine import RunState, run_simulation
from market_sim.recorder import COMPLETED_NAME, EVENTS_NAME, EVENTS_PART_NAME
from market_sim.verify import Verdict, verify_run

pytestmark = pytest.mark.chaos


class FailingAfterNBytes:
    """A file wrapper that raises a chosen OSError once it has passed N bytes."""

    def __init__(self, handle, after_bytes: int, error_no: int) -> None:
        self._handle = handle
        self._after = after_bytes
        self._errno = error_no
        self._written = 0

    def write(self, data: bytes) -> int:
        self._written += len(data)
        if self._written > self._after:
            raise OSError(self._errno, os.strerror(self._errno))
        return self._handle.write(data)

    def __getattr__(self, name):
        return getattr(self._handle, name)


@pytest.fixture
def inject_write_error(monkeypatch):
    """Make the recorder's event file start failing after a while."""

    def _inject(error_no: int, after_bytes: int = 8192):
        import market_sim.recorder as recorder_module

        real_open = open

        def fake_open(path, mode="rb", *args, **kwargs):
            handle = real_open(path, mode, *args, **kwargs)
            if str(path).endswith(EVENTS_PART_NAME):
                return FailingAfterNBytes(handle, after_bytes, error_no)
            return handle

        monkeypatch.setattr(recorder_module, "open", fake_open, raising=False)

    return _inject


class TestWriteFailures:
    @pytest.mark.parametrize(
        "error_name,expected_code",
        [
            ("ENOSPC", "OUTPUT_DISK_FULL"),
            ("EDQUOT", "OUTPUT_DISK_FULL"),
            ("EIO", "OUTPUT_IO_ERROR"),
        ],
    )
    async def test_the_run_fails_closed_with_the_right_classification(
        self, make_config, inject_write_error, error_name, expected_code
    ):
        error_no = getattr(errno, error_name, None)
        if error_no is None:
            pytest.skip(f"{error_name} is not defined on this platform")
        inject_write_error(error_no)
        cfg = make_config(mode="record", count=200_000, rate=1_000_000)
        result = await run_simulation(cfg)

        assert result.state is RunState.FAILED
        assert result.exit_code == 3, "a file error must exit 3, not 0"
        assert result.error.code == expected_code
        # Fail closed: no finished dataset, no marker, no silent downgrade to
        # live-only (case F01).
        assert not (result.run_dir / EVENTS_NAME).exists()
        assert not (result.run_dir / COMPLETED_NAME).exists()
        assert (result.run_dir / EVENTS_PART_NAME).exists()
        assert result.report["metrics"]["counters"]["file_write_errors_total"] >= 1

    async def test_the_verifier_does_not_accept_the_wreckage(
        self, make_config, inject_write_error
    ):
        inject_write_error(errno.ENOSPC)
        cfg = make_config(mode="record", count=200_000, rate=1_000_000)
        result = await run_simulation(cfg)
        assert verify_run(result.run_dir).verdict is Verdict.INCOMPLETE

    async def test_a_disk_failure_in_both_mode_is_not_a_live_only_success(
        self, make_config, inject_write_error
    ):
        # Section 2.3: a file error stops the run. Continuing on TCP alone and
        # reporting success would be the dishonest option.
        from conftest import free_port

        inject_write_error(errno.ENOSPC)
        cfg = make_config(
            mode="both", count=200_000, rate=1_000_000, tcp_port=free_port()
        )
        result = await run_simulation(cfg)
        assert result.state is RunState.FAILED
        assert result.exit_code == 3
        assert not (result.run_dir / COMPLETED_NAME).exists()

    async def test_the_cause_is_reported_not_a_generic_internal_failure(
        self, make_config, inject_write_error
    ):
        # The producer only sees "queue closed"; the report must name the disk.
        inject_write_error(errno.ENOSPC)
        cfg = make_config(mode="record", count=200_000, rate=1_000_000)
        result = await run_simulation(cfg)
        assert "OUTPUT_DISK_FULL" in result.report["exit_reason"]
        assert result.report["error"]["code"] == "OUTPUT_DISK_FULL"
        assert "no space" in result.report["error"]["message"]


class TestPermissions:
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="chmod on a directory does not prevent writes on Windows",
    )
    async def test_an_unwritable_output_root_fails_before_producing(self, make_config):
        # Case F02: fail fast, not after a long run.
        cfg = make_config(mode="record", count=1_000, rate=1_000_000)
        root = cfg.file.root_dir
        root.mkdir(parents=True, exist_ok=True)
        os.chmod(root, 0o500)
        try:
            result = await run_simulation(cfg)
            assert result.state is RunState.FAILED
            assert result.exit_code == 3
            assert result.error.code in ("OUTPUT_PERMISSION_DENIED", "OUTPUT_IO_ERROR")
        finally:
            os.chmod(root, 0o700)


class TestCommitFailures:
    async def test_a_failed_rename_is_reported_and_no_marker_appears(
        self, make_config, monkeypatch
    ):
        # Case F10: if the atomic rename fails the run is FAILED. It is never
        # reported as completed merely because the bytes were written.
        import market_sim.recorder as recorder_module

        real_replace = os.replace

        def failing_replace(src, dst, *args, **kwargs):
            if str(dst).endswith(EVENTS_NAME):
                raise OSError(errno.EACCES, "simulated rename failure")
            return real_replace(src, dst, *args, **kwargs)

        monkeypatch.setattr(recorder_module.os, "replace", failing_replace)
        cfg = make_config(mode="record", count=500, rate=1_000_000)
        result = await run_simulation(cfg)
        assert result.state is RunState.FAILED
        assert result.exit_code == 3
        assert result.error.code == "OUTPUT_PERMISSION_DENIED"
        assert not (result.run_dir / COMPLETED_NAME).exists()

    async def test_a_short_write_is_caught_before_the_rename(
        self, make_config, monkeypatch
    ):
        # Case F04: the on-disk size is cross-checked against what we believe we
        # wrote, so a lost write cannot be published as a complete dataset.
        import market_sim.recorder as recorder_module

        real_fstat = os.fstat

        class ShortStat:
            def __init__(self, real):
                self.st_size = max(0, real.st_size - 10)

        monkeypatch.setattr(
            recorder_module.os, "fstat", lambda fd: ShortStat(real_fstat(fd))
        )
        cfg = make_config(mode="record", count=500, rate=1_000_000)
        result = await run_simulation(cfg)
        assert result.state is RunState.FAILED
        assert result.error.code == "OUTPUT_SHORT_WRITE"
        assert not (result.run_dir / EVENTS_NAME).exists()
        assert not (result.run_dir / COMPLETED_NAME).exists()

    async def test_a_failed_report_write_fails_the_run(self, make_config, monkeypatch):
        # A run whose evidence could not be persisted is not a successful run.
        import market_sim.recorder as recorder_module

        real_replace = os.replace

        def failing_replace(src, dst, *args, **kwargs):
            if str(dst).endswith("report.json"):
                raise OSError(errno.EIO, "simulated report write failure")
            return real_replace(src, dst, *args, **kwargs)

        monkeypatch.setattr(recorder_module.os, "replace", failing_replace)
        cfg = make_config(mode="record", count=200, rate=1_000_000)
        result = await run_simulation(cfg)
        assert result.state is RunState.FAILED
        assert result.exit_code == 3
        assert not (result.run_dir / COMPLETED_NAME).exists()
