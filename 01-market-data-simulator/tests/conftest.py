"""Shared fixtures.

Every test builds its own config from :func:`config_dict` rather than reading
``config/example.toml``, so a test asserts against values it states itself and
cannot be broken by someone tuning the shipped example. Output always goes under
``tmp_path``; no test ever writes into the project's ``runs/``.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
from pathlib import Path
from typing import Any

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from market_sim.config import SimConfig, build_config  # noqa: E402
from market_sim.faults import FaultPlan, parse_fault_plan  # noqa: E402

#: A fixed logical t0 so every expected ``event_time_ns`` in the tests is a
#: number someone can check by hand. 2026-10-30T10:20:00Z, synthetic.
T0_NS = 1793322000000000000
SEED = 20261030


def symbol_dict(
    *,
    price_scale: int = 100000,
    initial: int = 108347,
    low: int = 100000,
    high: int = 120000,
    min_spread: int = 1,
    max_spread: int = 5,
    max_step: int = 8,
    weight: int = 1,
) -> dict[str, Any]:
    return {
        "price_scale": price_scale,
        "initial_price_ticks": initial,
        "min_price_ticks": low,
        "max_price_ticks": high,
        "min_spread_ticks": min_spread,
        "max_spread_ticks": max_spread,
        "max_step_ticks": max_step,
        "weight": weight,
    }


def config_dict(
    *,
    mode: str = "record",
    count: int | None = 100,
    duration_s: float | None = None,
    rate: int = 1000,
    seed: int = SEED,
    start_time_ns: int = T0_NS,
    quote_ratio_pct: int = 80,
    bound_policy: str = "reflect",
    scheduler_batch_ms: int = 10,
    max_burst_slots: int = 5000,
    max_scheduler_lag_ms: int = 2000,
    on_scheduler_lag: str = "continue-degraded",
    shutdown_drain_timeout_s: float = 30.0,
    symbols: dict[str, dict[str, Any]] | None = None,
    order: list[str] | None = None,
    file_enabled: bool | None = None,
    tcp_enabled: bool | None = None,
    root_dir: str = "runs",
    max_queue_events: int = 20000,
    max_queue_bytes: int = 16777216,
    fsync_every_events: int = 0,
    fsync_every_seconds: float = 0.0,
    max_record_bytes: int = 4096,
    max_total_bytes: int = 1073741824,
    tcp_host: str = "127.0.0.1",
    # Port 0 is deliberately *rejected* by the validator (case E2E-11 lists it as
    # invalid), so TCP tests pass an explicit free_port() instead of relying on
    # the OS to pick one.
    tcp_port: int = 9009,
    tcp_max_clients: int = 1,
    tcp_max_queue_events: int = 10000,
    tcp_max_queue_bytes: int = 8388608,
    send_timeout_ms: int = 2000,
    wait_for_client: bool = False,
    startup_timeout_s: float = 5.0,
    allow_public_bind: bool = False,
    faults_enabled: bool = False,
    plan_path: str | None = None,
) -> dict[str, Any]:
    """A complete, valid config as a plain mapping. Override exactly what matters."""
    if symbols is None:
        symbols = {
            "EURUSD": symbol_dict(weight=3),
            "GBPUSD": symbol_dict(
                initial=126540, low=115000, high=140000, max_spread=8, max_step=10, weight=2
            ),
            "USDJPY": symbol_dict(
                price_scale=1000,
                initial=151230,
                low=140000,
                high=165000,
                max_spread=6,
                max_step=9,
                weight=1,
            ),
        }
    if order is None:
        order = list(symbols)
    if file_enabled is None:
        file_enabled = mode in ("record", "both", "replay")
    if tcp_enabled is None:
        tcp_enabled = mode in ("serve", "both")

    run: dict[str, Any] = {
        "mode": mode,
        "seed": seed,
        "start_time_ns": start_time_ns,
        "rate_per_sec": rate,
        "quote_ratio_pct": quote_ratio_pct,
        "scheduler_batch_ms": scheduler_batch_ms,
        "max_burst_slots": max_burst_slots,
        "max_scheduler_lag_ms": max_scheduler_lag_ms,
        "on_scheduler_lag": on_scheduler_lag,
        "bound_policy": bound_policy,
        "shutdown_drain_timeout_s": shutdown_drain_timeout_s,
    }
    if count is not None:
        run["count"] = count
    if duration_s is not None:
        run["duration_s"] = duration_s

    faults: dict[str, Any] = {"enabled": faults_enabled}
    if plan_path is not None:
        faults["plan_path"] = plan_path

    return {
        "run": run,
        "symbols": {"order": order, **symbols},
        "output": {
            "file": {
                "enabled": file_enabled,
                "root_dir": root_dir,
                "max_queue_events": max_queue_events,
                "max_queue_bytes": max_queue_bytes,
                "fsync_every_events": fsync_every_events,
                "fsync_every_seconds": fsync_every_seconds,
                "max_record_bytes": max_record_bytes,
                "max_total_bytes": max_total_bytes,
            },
            "tcp": {
                "enabled": tcp_enabled,
                "host": tcp_host,
                "port": tcp_port,
                "max_clients": tcp_max_clients,
                "max_queue_events": tcp_max_queue_events,
                "max_queue_bytes": tcp_max_queue_bytes,
                "send_timeout_ms": send_timeout_ms,
                "slow_client_policy": "disconnect",
                "wait_for_client": wait_for_client,
                "startup_timeout_s": startup_timeout_s,
                "allow_public_bind": allow_public_bind,
            },
        },
        "faults": faults,
    }


@pytest.fixture
def make_config(tmp_path: Path):
    """Build a validated :class:`SimConfig` rooted at ``tmp_path``."""

    def _make(**kwargs: Any) -> SimConfig:
        fingerprint = kwargs.pop("fault_plan_fingerprint", "none")
        raw = config_dict(**kwargs)
        return build_config(
            raw, base_dir=tmp_path, fault_plan_fingerprint=fingerprint
        )

    return _make


@pytest.fixture
def make_config_file(tmp_path: Path):
    """Write a config TOML into ``tmp_path`` and return its path.

    Used by the CLI and subprocess tests, which must exercise the real file
    loader rather than an in-memory mapping.
    """

    def _make(name: str = "config.toml", **kwargs: Any) -> Path:
        raw = config_dict(**kwargs)
        path = tmp_path / name
        path.write_text(_to_toml(raw), encoding="utf-8")
        return path

    return _make


def _to_toml(data: dict[str, Any]) -> str:
    """Minimal TOML writer for the config shapes these tests build.

    Deliberately not a general serialiser: it handles exactly the nested-table,
    scalar and string-array structure of a sim config, and would rather be
    obviously wrong than silently approximate.
    """

    def scalar(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return repr(value)
        if isinstance(value, str):
            return json.dumps(value)
        if isinstance(value, list):
            return "[" + ", ".join(scalar(v) for v in value) + "]"
        raise TypeError(f"unsupported TOML value: {value!r}")

    lines: list[str] = []

    def emit(table: dict[str, Any], prefix: str) -> None:
        scalars = {k: v for k, v in table.items() if not isinstance(v, dict)}
        tables = {k: v for k, v in table.items() if isinstance(v, dict)}
        if prefix:
            lines.append(f"[{prefix}]")
        for key, value in scalars.items():
            lines.append(f"{key} = {scalar(value)}")
        lines.append("")
        for key, value in tables.items():
            emit(value, f"{prefix}.{key}" if prefix else key)

    emit(data, "")
    return "\n".join(lines)


@pytest.fixture
def fault_plan():
    """Build a :class:`FaultPlan` from a list of plain fault dicts."""

    def _make(*faults: dict[str, Any]) -> FaultPlan:
        return parse_fault_plan({"fault": list(faults)})

    return _make


@pytest.fixture
def write_fault_plan(tmp_path: Path):
    """Write a fault plan TOML and return its path."""

    def _make(*faults: dict[str, Any], name: str = "fault-plan.toml") -> Path:
        chunks: list[str] = []
        for entry in faults:
            chunks.append("[[fault]]")
            for key, value in entry.items():
                if isinstance(value, str):
                    chunks.append(f"{key} = {json.dumps(value)}")
                elif isinstance(value, bool):
                    chunks.append(f"{key} = {'true' if value else 'false'}")
                else:
                    chunks.append(f"{key} = {value}")
            chunks.append("")
        path = tmp_path / name
        path.write_text("\n".join(chunks), encoding="utf-8")
        return path

    return _make


def free_port() -> int:
    """Grab a port the OS says is free.

    There is an unavoidable race between closing this socket and the server
    binding it, so tests that must be certain use ``tcp_port = 0`` and read the
    bound port back from the sink instead.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def read_lines(
    reader: asyncio.StreamReader, count: int, timeout: float = 5.0
) -> list[bytes]:
    """Read exactly ``count`` newline-delimited records, or raise on timeout."""
    out: list[bytes] = []
    for _ in range(count):
        line = await asyncio.wait_for(reader.readline(), timeout)
        if not line:
            break
        out.append(line.rstrip(b"\n"))
    return out


def read_events(path: Path) -> list[dict[str, Any]]:
    """Parse a recorded JSONL file into dicts, asserting newline termination."""
    raw = path.read_bytes()
    assert raw == b"" or raw.endswith(b"\n"), "recorded file must end with a newline"
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def sequences_by_symbol(events: list[dict[str, Any]]) -> dict[str, list[int]]:
    out: dict[str, list[int]] = {}
    for event in events:
        out.setdefault(event["symbol"], []).append(event["sequence"])
    return out


@pytest.fixture
def no_windows_signal_noise():
    """Keep SIGINT/SIGTERM handlers from leaking between tests."""
    import signal

    saved = {}
    for name in ("SIGINT", "SIGTERM"):
        signum = getattr(signal, name, None)
        if signum is not None:
            try:
                saved[signum] = signal.getsignal(signum)
            except (ValueError, OSError):
                pass
    yield
    for signum, handler in saved.items():
        try:
            signal.signal(signum, handler)
        except (ValueError, OSError, TypeError):
            pass


IS_WINDOWS = os.name == "nt"
IS_POSIX = os.name == "posix"
