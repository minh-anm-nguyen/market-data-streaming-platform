"""Shared fixtures.

Two rules, inherited from the P01 suite and for the same reasons:

* **No test reads a shipped config.** Every test states the values it asserts
  against, so tuning ``configs/lab_capacity.toml`` can never turn a test green
  or red by accident.
* **No test sleeps.** Timing is driven by :class:`~p08_bench.clock.FakeClock`,
  which is what makes a "30 minute steady phase" assertion run in microseconds
  and makes a backwards-clock test possible at all.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from p08_bench.clock import ClockDomain, FakeClock  # noqa: E402

#: A fixed logical t0 so expected timestamps in tests are hand-checkable.
#: 2026-10-30T10:20:00Z, synthetic.
T0_NS = 1_793_322_000_000_000_000
SEED = 20261030


@pytest.fixture
def domain() -> ClockDomain:
    return ClockDomain(host="bench-host", process="driver")


@pytest.fixture
def fake_clock(domain: ClockDomain) -> FakeClock:
    return FakeClock(domain, start_ns=T0_NS)


@pytest.fixture
def other_domain() -> ClockDomain:
    return ClockDomain(host="clickhouse-host", process="server")


def ramp(values: list[float], *, start_ns: int = 0, step_s: float = 60.0) -> list[tuple[int, float]]:
    """Turn a list of values into evenly spaced ``(t_ns, value)`` samples."""
    step_ns = int(step_s * 1e9)
    return [(start_ns + i * step_ns, v) for i, v in enumerate(values)]


def workload_dict(**overrides: Any) -> dict[str, Any]:
    """A complete, valid workload manifest as a plain mapping.

    Mirrors section 21.1 but with the lane shrunk to something a test can run.
    Override exactly what matters.
    """
    base: dict[str, Any] = {
        "run": {
            "id": "P08-TEST-0001",
            "seed": SEED,
            "manifest_version": "p08.v1",
            "profile": "LOCAL_SMALL",
            "expected_oracle": "builtin-independent",
            "lane": "L01",
        },
        "dataset": {
            "dataset_id": "BENCH_TEST_001",
            "schema": "marketdata.v1.MarketDataEvent",
            "symbol_count": 3,
            "symbol_distribution": "uniform",
            "tick_to_quote_ratio": "4:1",
            "source_seal_required": True,
        },
        "kafka": {
            "tick_topic": "md.ticks.v1",
            "quote_topic": "md.quotes.v1",
            "partitions_per_topic": 8,
            "producer_acks": "all",
            "producer_idempotent": True,
            "topic_generation": "gen-1",
        },
        "load": {
            "model": "open_loop",
            "market_tick_target_per_s": 1000,
            "quote_target_per_s": 0,
            "ramp_up_seconds": 0,
            "warmup_seconds": 1,
            "steady_seconds": 10,
            "drain_seconds": 2,
            "max_late_issue_ms": 10,
        },
        "query": {
            "enabled": False,
            "family": "Q02",
            "concurrency": 4,
            "min_complete_samples": 10,
            "client_p95_budget_ms": 280,
            "cache_profile": "warm",
            "deadline_ms": 2000,
        },
        "evidence": {
            "require_ready_snapshot": True,
            "require_identity_diff": True,
            "require_semantic_match": True,
            "require_fault_receipt_if_chaos": True,
            "fail_on_missing_metrics": True,
        },
        "safety": {
            "lab_only": True,
            "dry_run_destructive_actions": True,
            "max_test_volume_fill_percent": 90,
            "allow_host_root_path": False,
        },
    }
    for section, values in overrides.items():
        if isinstance(values, dict) and section in base:
            base[section] = {**base[section], **values}
        else:
            base[section] = values
    return base
