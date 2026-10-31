"""Deterministic crash injection at the five mandatory points.

Spec section 9.6 requires at least these hooks, and requires that a crash test
*prove* the process reached the intended point before dying. A test that kills
the process somewhere else and then observes a clean recovery has proven
nothing.

So each hook does two things in order:

  1. write a marker file naming the hook and ``fsync`` it, plus ``fsync`` the
     containing directory so the entry itself is durable;
  2. terminate hard with ``os._exit(137)`` -- no atexit handlers, no buffered
     flush, no graceful shutdown path.

The marker is evidence that the point was reached, not evidence of durability
of anything else; the spec is explicit that an unflushed marker must not be
used as a durability proof, which is why the fsync is not optional here.

Hooks are armed by environment variable so that the crash happens in a real
child process rather than being simulated in-process::

    P05_CRASH_HOOK=CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT
    P05_CRASH_MARKER=artifacts/crash-04.marker
    P05_CRASH_AFTER_N=3        # optional: skip the first N-1 arrivals
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field

CRASH_01_AFTER_FETCH_BEFORE_DECODE = "CRASH_01_AFTER_FETCH_BEFORE_DECODE"
CRASH_02_AFTER_DECODE_BEFORE_BATCH_SEAL = "CRASH_02_AFTER_DECODE_BEFORE_BATCH_SEAL"
CRASH_03_AFTER_CH_REQUEST_SENT_BEFORE_RESPONSE = (
    "CRASH_03_AFTER_CH_REQUEST_SENT_BEFORE_RESPONSE"
)
CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT = "CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT"
CRASH_05_AFTER_COMMIT_RESPONSE_BEFORE_REPORT_FSYNC = (
    "CRASH_05_AFTER_COMMIT_RESPONSE_BEFORE_REPORT_FSYNC"
)

ALL_HOOKS: tuple[str, ...] = (
    CRASH_01_AFTER_FETCH_BEFORE_DECODE,
    CRASH_02_AFTER_DECODE_BEFORE_BATCH_SEAL,
    CRASH_03_AFTER_CH_REQUEST_SENT_BEFORE_RESPONSE,
    CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT,
    CRASH_05_AFTER_COMMIT_RESPONSE_BEFORE_REPORT_FSYNC,
)

ENV_HOOK = "P05_CRASH_HOOK"
ENV_MARKER = "P05_CRASH_MARKER"
ENV_AFTER_N = "P05_CRASH_AFTER_N"

#: Exit status used for an injected crash. 137 is what a SIGKILL looks like on
#: Linux; using it keeps the harness checks uniform across platforms. It is NOT
#: by itself a test pass (spec 9.6).
CRASH_EXIT_STATUS = 137


@dataclass(slots=True)
class CrashHooks:
    """Armed-hook state. One instance per process, read from the environment."""

    armed_hook: str | None = None
    marker_path: str | None = None
    after_n: int = 1
    _arrivals: dict[str, int] = field(default_factory=dict)
    #: Test seam: replaced with a recorder so in-process tests can assert the
    #: hook fired without actually killing the test runner.
    exit_fn: object = None

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> CrashHooks:
        env = os.environ if env is None else env
        hook = env.get(ENV_HOOK) or None
        if hook is not None and hook not in ALL_HOOKS:
            raise ValueError(
                f"{ENV_HOOK}={hook!r} is not a known crash hook; valid values: "
                + ", ".join(ALL_HOOKS)
            )
        try:
            after_n = int(env.get(ENV_AFTER_N, "1"))
        except ValueError as exc:
            raise ValueError(f"{ENV_AFTER_N} must be an integer") from exc
        if after_n < 1:
            raise ValueError(f"{ENV_AFTER_N} must be >= 1")
        return cls(
            armed_hook=hook,
            marker_path=env.get(ENV_MARKER) or None,
            after_n=after_n,
        )

    @property
    def is_armed(self) -> bool:
        return self.armed_hook is not None

    def maybe_crash(self, hook: str, *, detail: str = "") -> None:
        """Crash if ``hook`` is the armed one and its Nth arrival has come.

        Called from the engine at each of the five points. When no hook is
        armed this is a dict lookup against None and costs nothing measurable.
        """
        if self.armed_hook is None or hook != self.armed_hook:
            return
        count = self._arrivals.get(hook, 0) + 1
        self._arrivals[hook] = count
        if count < self.after_n:
            return
        self._write_marker(hook, detail=detail, arrival=count)
        self._die()

    # ---------------------------------------------------------------- details

    def _write_marker(self, hook: str, *, detail: str, arrival: int) -> None:
        if not self.marker_path:
            return
        path = os.path.abspath(self.marker_path)
        directory = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)
        payload = (
            f"hook={hook}\npid={os.getpid()}\narrival={arrival}\ndetail={detail}\n"
        ).encode("utf-8")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            os.write(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        # fsync the directory too, so the *entry* survives, not just the bytes.
        # Windows does not support opening a directory this way, so this is
        # best-effort there; the test harness compensates by reading the file
        # back before asserting.
        try:
            dir_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

    def _die(self) -> None:
        if self.exit_fn is not None:
            self.exit_fn(CRASH_EXIT_STATUS)
            return
        sys.stderr.write(f"[p05] injected crash at {self.armed_hook}\n")
        sys.stderr.flush()
        os._exit(CRASH_EXIT_STATUS)


#: A disarmed instance for ordinary runs and most tests.
NO_CRASH = CrashHooks()
