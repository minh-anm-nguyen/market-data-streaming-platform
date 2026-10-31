"""Crash-hook arming, the marker file, and the arrival counter.

The subprocess tests in ``tests/chaos`` prove the hooks really kill a process.
These cover the logic around that: how a hook is armed, when it fires, and what
the marker records. They use the ``exit_fn`` seam so the test runner survives.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from p05_ingest.crash_hooks import (
    ALL_HOOKS,
    CRASH_01_AFTER_FETCH_BEFORE_DECODE,
    CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT,
    CRASH_EXIT_STATUS,
    ENV_AFTER_N,
    ENV_HOOK,
    ENV_MARKER,
    NO_CRASH,
    CrashHooks,
)


def recorder():
    """A stand-in for process death that records the exit status instead."""
    calls: list[int] = []
    return calls, calls.append


# --------------------------------------------------------------------- arming


def test_disarmed_by_default():
    hooks = CrashHooks.from_env({})
    assert not hooks.is_armed
    assert hooks.armed_hook is None


def test_the_shared_no_crash_instance_is_disarmed():
    assert not NO_CRASH.is_armed
    # Calling every hook on it must be a no-op.
    for hook in ALL_HOOKS:
        NO_CRASH.maybe_crash(hook, detail="should do nothing")


def test_arming_reads_the_environment(tmp_path):
    marker = tmp_path / "marker"
    hooks = CrashHooks.from_env(
        {
            ENV_HOOK: CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT,
            ENV_MARKER: str(marker),
            ENV_AFTER_N: "3",
        }
    )
    assert hooks.is_armed
    assert hooks.armed_hook == CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT
    assert hooks.after_n == 3


def test_an_unknown_hook_name_is_refused():
    """A typo must fail loudly, or a chaos test runs with nothing armed."""
    with pytest.raises(ValueError, match="not a known crash hook"):
        CrashHooks.from_env({ENV_HOOK: "CRASH_99_NOPE"})


@pytest.mark.parametrize("value", ["0", "-1", "abc", ""])
def test_a_bad_arrival_count_is_refused(value):
    with pytest.raises(ValueError):
        CrashHooks.from_env(
            {ENV_HOOK: CRASH_01_AFTER_FETCH_BEFORE_DECODE, ENV_AFTER_N: value}
        )


# --------------------------------------------------------------------- firing


def test_only_the_armed_hook_fires(tmp_path):
    calls, exit_fn = recorder()
    hooks = CrashHooks(
        armed_hook=CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT,
        marker_path=str(tmp_path / "marker"),
        exit_fn=exit_fn,
    )
    for hook in ALL_HOOKS:
        if hook != CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT:
            hooks.maybe_crash(hook)
    assert calls == [], "a hook that was not armed must not fire"

    hooks.maybe_crash(CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT)
    assert calls == [CRASH_EXIT_STATUS]


def test_after_n_skips_earlier_arrivals(tmp_path):
    """Lets a test crash at, say, the third batch rather than the first."""
    calls, exit_fn = recorder()
    hooks = CrashHooks(
        armed_hook=CRASH_01_AFTER_FETCH_BEFORE_DECODE,
        marker_path=str(tmp_path / "marker"),
        after_n=3,
        exit_fn=exit_fn,
    )
    hooks.maybe_crash(CRASH_01_AFTER_FETCH_BEFORE_DECODE)
    hooks.maybe_crash(CRASH_01_AFTER_FETCH_BEFORE_DECODE)
    assert calls == [], "the first two arrivals must pass through"
    assert not (tmp_path / "marker").exists()

    hooks.maybe_crash(CRASH_01_AFTER_FETCH_BEFORE_DECODE)
    assert calls == [CRASH_EXIT_STATUS]


# --------------------------------------------------------------------- marker


def test_marker_records_the_hook_pid_arrival_and_detail(tmp_path):
    calls, exit_fn = recorder()
    marker = tmp_path / "nested" / "marker"
    hooks = CrashHooks(
        armed_hook=CRASH_01_AFTER_FETCH_BEFORE_DECODE,
        marker_path=str(marker),
        exit_fn=exit_fn,
    )
    hooks.maybe_crash(
        CRASH_01_AFTER_FETCH_BEFORE_DECODE, detail="md.ticks.v1[0]@42"
    )

    assert marker.exists(), "the parent directory must be created"
    text = marker.read_text(encoding="utf-8")
    assert f"hook={CRASH_01_AFTER_FETCH_BEFORE_DECODE}" in text
    assert f"pid={os.getpid()}" in text
    assert "arrival=1" in text
    assert "detail=md.ticks.v1[0]@42" in text


def test_firing_without_a_marker_path_still_exits():
    """The marker is evidence, not a precondition for the crash."""
    calls, exit_fn = recorder()
    hooks = CrashHooks(
        armed_hook=CRASH_01_AFTER_FETCH_BEFORE_DECODE,
        marker_path=None,
        exit_fn=exit_fn,
    )
    hooks.maybe_crash(CRASH_01_AFTER_FETCH_BEFORE_DECODE)
    assert calls == [CRASH_EXIT_STATUS]


def test_marker_is_overwritten_not_appended(tmp_path):
    marker = tmp_path / "marker"
    marker.write_text("stale content from an earlier run", encoding="utf-8")
    calls, exit_fn = recorder()
    hooks = CrashHooks(
        armed_hook=CRASH_01_AFTER_FETCH_BEFORE_DECODE,
        marker_path=str(marker),
        exit_fn=exit_fn,
    )
    hooks.maybe_crash(CRASH_01_AFTER_FETCH_BEFORE_DECODE)
    assert "stale content" not in marker.read_text(encoding="utf-8")


# ------------------------------------------------------------------ the five


def test_all_five_mandatory_hooks_are_defined():
    """Spec 9.6 requires at least these five points."""
    assert ALL_HOOKS == (
        "CRASH_01_AFTER_FETCH_BEFORE_DECODE",
        "CRASH_02_AFTER_DECODE_BEFORE_BATCH_SEAL",
        "CRASH_03_AFTER_CH_REQUEST_SENT_BEFORE_RESPONSE",
        "CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT",
        "CRASH_05_AFTER_COMMIT_RESPONSE_BEFORE_REPORT_FSYNC",
    )


def test_each_hook_is_actually_invoked_somewhere():
    """Guards against a hook that exists but is never called.

    An unreferenced hook would silently make its crash scenario untestable, so
    each name must appear in the module that is supposed to fire it.
    """
    src = Path(__file__).resolve().parents[2] / "src" / "p05_ingest"
    combined = (src / "engine.py").read_text(encoding="utf-8") + (
        src / "sink_worker.py"
    ).read_text(encoding="utf-8")
    for hook in ALL_HOOKS:
        assert hook in combined, f"{hook} is defined but never invoked"
