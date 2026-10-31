"""Hard kill and recovery (E2E-06, cases F03, F17, S09).

The whole point: an ungraceful death must leave evidence that is *obviously* not
a dataset, and the only recovery on offer is regenerating from the seed -- which
has to produce the same bytes as a clean run, or determinism was never real.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from conftest import _to_toml, config_dict
from market_sim.recorder import COMPLETED_NAME, EVENTS_NAME, EVENTS_PART_NAME
from market_sim.verify import Verdict, verify_run

pytestmark = pytest.mark.chaos

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC = PROJECT_ROOT / "src"


def write_config(tmp_path: Path, name="config.toml", **kwargs) -> Path:
    path = tmp_path / name
    path.write_text(_to_toml(config_dict(**kwargs)), encoding="utf-8")
    return path


def spawn(cfg_path: Path, *args: str) -> subprocess.Popen:
    env = {**os.environ, "PYTHONPATH": str(SRC), "PYTHONUNBUFFERED": "1"}
    return subprocess.Popen(
        [sys.executable, "-m", "market_sim", "record", "--config", str(cfg_path), *args],
        env=env,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def wait_for_part_file(root: Path, run_id: str, min_bytes=4096, timeout=60.0) -> Path:
    """Wait until the writer has actually put bytes on disk."""
    part = root / run_id / EVENTS_PART_NAME
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if part.is_file() and part.stat().st_size >= min_bytes:
            return part
        time.sleep(0.05)
    raise AssertionError(
        f"{part} did not reach {min_bytes} bytes in {timeout}s "
        f"(exists={part.is_file()})"
    )


class TestE2E06KillDuringWrite:
    def test_a_killed_run_leaves_no_completed_dataset(self, tmp_path):
        run_id = "run-killed"
        cfg = write_config(
            tmp_path,
            mode="record",
            count=4_000_000,
            rate=40_000,
            fsync_every_events=0,
            fsync_every_seconds=0.0,
        )
        proc = spawn(cfg, "--run-id", run_id)
        try:
            wait_for_part_file(tmp_path / "runs", run_id)
            # No graceful shutdown: this is TerminateProcess on Windows and
            # SIGKILL on POSIX. The process gets no chance to finalise.
            proc.kill()
            proc.wait(timeout=30)
        finally:
            if proc.poll() is None:  # pragma: no cover - defensive
                proc.kill()
                proc.wait(timeout=10)

        run_dir = tmp_path / "runs" / run_id
        assert (run_dir / EVENTS_PART_NAME).is_file()
        assert not (run_dir / EVENTS_NAME).exists()
        assert not (run_dir / COMPLETED_NAME).exists()

    def test_the_verifier_calls_it_incomplete(self, tmp_path):
        run_id = "run-killed-2"
        cfg = write_config(tmp_path, mode="record", count=4_000_000, rate=40_000)
        proc = spawn(cfg, "--run-id", run_id)
        try:
            wait_for_part_file(tmp_path / "runs", run_id)
            proc.kill()
            proc.wait(timeout=30)
        finally:
            if proc.poll() is None:  # pragma: no cover
                proc.kill()

        verdict = verify_run(tmp_path / "runs" / run_id)
        assert verdict.verdict is Verdict.INCOMPLETE
        assert any(".part" in reason for reason in verdict.reasons)
        # The partial file is still useful evidence: it says how far we got.
        assert verdict.lines > 0
        assert verdict.bytes_read > 0

    def test_a_partial_file_is_never_usable_as_a_replay_source(self, tmp_path):
        from market_sim.errors import ReplayError
        from market_sim.replay import open_replay_source

        run_id = "run-killed-3"
        cfg = write_config(tmp_path, mode="record", count=4_000_000, rate=40_000)
        proc = spawn(cfg, "--run-id", run_id)
        try:
            wait_for_part_file(tmp_path / "runs", run_id)
            proc.kill()
            proc.wait(timeout=30)
        finally:
            if proc.poll() is None:  # pragma: no cover
                proc.kill()

        with pytest.raises(ReplayError) as exc:
            open_replay_source(tmp_path / "runs" / run_id)
        assert exc.value.code == "REPLAY_SOURCE_NOT_COMPLETE"

    def test_regeneration_from_the_seed_reproduces_a_clean_run_exactly(self, tmp_path):
        # ADR-009 in practice: there is no crash-resume, and none is needed as
        # long as the same seed gives the same bytes (case S09).
        cfg = write_config(tmp_path, mode="record", count=20_000, rate=1_000_000)
        env = {**os.environ, "PYTHONPATH": str(SRC)}

        def clean_run(run_id: str) -> dict:
            proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "market_sim",
                    "record",
                    "--config",
                    str(cfg),
                    "--run-id",
                    run_id,
                    "--compact",
                ],
                env=env,
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                timeout=300,
            )
            assert proc.returncode == 0, proc.stderr[-2000:]
            return json.loads(proc.stdout)

        first = clean_run("run-clean-a")
        second = clean_run("run-clean-b")
        assert first["report"]["file"]["sha256"] == second["report"]["file"]["sha256"]
        assert first["report"]["dataset_id"] == second["report"]["dataset_id"]
        # And in a separate process from the one that wrote this test's fixtures.
        assert (
            tmp_path / "runs" / "run-clean-a" / EVENTS_NAME
        ).read_bytes() == (tmp_path / "runs" / "run-clean-b" / EVENTS_NAME).read_bytes()
