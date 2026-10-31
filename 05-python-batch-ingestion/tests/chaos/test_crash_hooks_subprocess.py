"""Real process kills at the five mandatory crash points (spec 9.6).

Each case does the full sequence the spec demands:

    arm the hook -> run a real subprocess -> verify the fault actually
    occurred (marker + exit status) -> inspect what survived on disk ->
    restart -> verify nothing was lost

A test that cannot show its marker is reported as not having run, never as a
pass. Exit status 137 on its own is not accepted as evidence either: the marker
names the hook that was reached, which is what distinguishes "died where we
aimed" from "died somewhere".
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from p05_ingest.crash_hooks import (
    ALL_HOOKS,
    CRASH_EXIT_STATUS,
    ENV_AFTER_N,
    ENV_HOOK,
    ENV_MARKER,
)

pytestmark = pytest.mark.chaos

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCENARIO = PROJECT_ROOT / "tools" / "crash_scenario.py"
EVENTS = 20


def run_scenario(
    state_dir: Path,
    *,
    hook: str | None = None,
    after_n: int = 2,
    extra: list[str] | None = None,
) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.pop(ENV_HOOK, None)
    env.pop(ENV_MARKER, None)
    env.pop(ENV_AFTER_N, None)
    if hook is not None:
        env[ENV_HOOK] = hook
        env[ENV_MARKER] = str(state_dir / "marker")
        env[ENV_AFTER_N] = str(after_n)

    cmd = [
        sys.executable,
        str(SCENARIO),
        "--state-dir",
        str(state_dir),
        "--config",
        str(PROJECT_ROOT / "config" / "dev.toml"),
        "--events",
        str(EVENTS),
        *(extra or []),
    ]
    return subprocess.run(
        cmd, capture_output=True, text=True, env=env, cwd=str(PROJECT_ROOT), timeout=180
    )


def durable_rows(state_dir: Path) -> list[dict]:
    path = state_dir / "rows.jsonl"
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def durable_offsets(state_dir: Path) -> dict[str, int]:
    path = state_dir / "offsets.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def identities(rows: list[dict]) -> set[tuple[str, str, int]]:
    return {(r["dataset_id"], r["symbol"], r["sequence"]) for r in rows}


# ------------------------------------------------------------------ baseline


def test_scenario_completes_cleanly_without_a_hook(tmp_path):
    """The control case. Without this, a crash test proves nothing."""
    result = run_scenario(tmp_path)
    assert result.returncode == 0, result.stderr
    summary = json.loads(result.stdout)
    assert summary["verdict"] == "COMPLETE"
    assert summary["logical_identities"] == EVENTS
    assert summary["physical_rows"] == EVENTS
    assert summary["crash_hook_armed"] is None


# --------------------------------------------------------------- every hook


@pytest.mark.parametrize("hook", ALL_HOOKS)
def test_crash_at_hook_is_proven_then_recovered(hook, tmp_path):
    """Kill at ``hook``, prove it happened, restart, lose nothing."""
    first = run_scenario(tmp_path, hook=hook)

    # 1. The fault really occurred, at the point we aimed for.
    marker = tmp_path / "marker"
    assert marker.exists(), (
        f"{hook}: no marker written, so the process did not reach the hook. "
        "This case is NOT_RUN, not a pass."
    )
    marker_text = marker.read_text(encoding="utf-8")
    assert f"hook={hook}" in marker_text
    # The marker identifies the process and which arrival fired, so a stale
    # marker from an earlier run cannot be mistaken for this one.
    assert "pid=" in marker_text
    assert "arrival=" in marker_text
    assert first.returncode == CRASH_EXIT_STATUS, (
        f"{hook}: expected exit {CRASH_EXIT_STATUS}, got {first.returncode}: "
        f"{first.stderr[-400:]}"
    )

    # 2. Whatever is on disk must satisfy I-01: everything below a committed
    # offset is already in the table.
    rows = durable_rows(tmp_path)
    offsets = durable_offsets(tmp_path)
    for key, committed_next in offsets.items():
        topic, partition = key.rsplit(":", 1)
        written = {
            r["kafka_offset"]
            for r in rows
            if r["kafka_topic"] == topic and r["kafka_partition"] == int(partition)
        }
        missing = {o for o in range(committed_next) if o not in written}
        # Offsets can legitimately be absent only if that offset was never
        # delivered; this scenario produces no control records, so every
        # offset below the committed one must be present.
        assert not missing, (
            f"{hook}: offsets {sorted(missing)} on {key} were committed but are "
            "not in the table -- that is data loss"
        )

    # 3. Restart and finish the job.
    second = run_scenario(tmp_path)
    assert second.returncode == 0, second.stderr
    summary = json.loads(second.stdout)

    assert summary["verdict"] == "COMPLETE", summary["reason"]
    assert summary["logical_identities"] == EVENTS, (
        f"{hook}: expected {EVENTS} logical events after recovery, got "
        f"{summary['logical_identities']}"
    )

    # 4. Physical duplicates are allowed; losing an event is not.
    final_rows = durable_rows(tmp_path)
    assert identities(final_rows) == identities(final_rows) | identities(rows)
    assert len(final_rows) >= EVENTS


@pytest.mark.parametrize(
    "hook",
    [
        "CRASH_03_AFTER_CH_REQUEST_SENT_BEFORE_RESPONSE",
        "CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT",
    ],
)
def test_crash_around_the_insert_leaves_rows_without_a_commit(hook, tmp_path):
    """The dangerous window, observed directly on disk.

    At these two points ClickHouse holds rows that Kafka has not committed. A
    design that committed first would show the opposite, and this assertion is
    what would catch it.
    """
    run_scenario(tmp_path, hook=hook, after_n=2)
    rows = durable_rows(tmp_path)
    offsets = durable_offsets(tmp_path)

    assert rows, f"{hook}: expected rows to be durable before the crash"
    committed_total = sum(offsets.values())
    assert len(rows) >= committed_total, (
        f"{hook}: {len(rows)} durable rows but {committed_total} committed "
        "offsets -- the commit ran ahead of the sink"
    )


def test_replay_after_an_uncommitted_ack_duplicates_rather_than_loses(tmp_path):
    """E2E-02 end to end, with de-duplication disabled.

    With the insert de-duplication window switched off, the replay writes the
    rows a second time. That is the correct at-least-once outcome: duplicated
    physical rows, one logical event each, nothing missing.
    """
    hook = "CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT"
    first = run_scenario(tmp_path, hook=hook, after_n=1, extra=["--no-dedup"])
    assert first.returncode == CRASH_EXIT_STATUS
    assert (tmp_path / "marker").exists()

    rows_before = durable_rows(tmp_path)
    assert rows_before, "the batch was acknowledged before the crash"
    assert durable_offsets(tmp_path) == {}, "and never committed"

    second = run_scenario(tmp_path, extra=["--no-dedup"])
    assert second.returncode == 0, second.stderr
    summary = json.loads(second.stdout)

    assert summary["logical_identities"] == EVENTS, "nothing lost"
    assert summary["physical_rows"] > EVENTS, (
        "with de-duplication off, the replay must produce physical duplicates"
    )

    # Duplicates, never conflicts: each identity has exactly one digest.
    by_identity: dict[tuple, set[str]] = {}
    for row in durable_rows(tmp_path):
        key = (row["dataset_id"], row["symbol"], row["sequence"])
        by_identity.setdefault(key, set()).add(row["payload_digest"])
    conflicted = {k: v for k, v in by_identity.items() if len(v) > 1}
    assert not conflicted, f"replay produced identity conflicts: {conflicted}"


def test_dedup_window_absorbs_the_replay_when_enabled(tmp_path):
    """The same crash with de-duplication on produces no duplicate."""
    hook = "CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT"
    assert run_scenario(tmp_path, hook=hook, after_n=1).returncode == CRASH_EXIT_STATUS

    second = run_scenario(tmp_path)
    assert second.returncode == 0, second.stderr
    summary = json.loads(second.stdout)
    assert summary["logical_identities"] == EVENTS
    assert summary["physical_rows"] == EVENTS, (
        "an identical retried batch should be suppressed inside the window"
    )


def test_unknown_hook_name_is_rejected_rather_than_ignored(tmp_path):
    """A typo in the hook name must fail loudly, or a test would silently
    run with no fault armed and then report a pass."""
    env = dict(os.environ)
    env[ENV_HOOK] = "CRASH_99_NOT_A_HOOK"
    env[ENV_MARKER] = str(tmp_path / "marker")
    result = subprocess.run(
        [
            sys.executable,
            str(SCENARIO),
            "--state-dir",
            str(tmp_path),
            "--config",
            str(PROJECT_ROOT / "config" / "dev.toml"),
            "--events",
            "5",
        ],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(PROJECT_ROOT),
        timeout=120,
    )
    assert result.returncode != 0
    assert "not a known crash hook" in result.stderr
    assert not (tmp_path / "marker").exists()
