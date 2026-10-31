"""Throughput and rate accuracy (NFR-04, NFR-07, section 9.3).

These are measurements, not pass/fail claims about the design. Where a target is
asserted it is one this machine can actually meet; where it cannot, the test
records the measurement and says so rather than being tuned until it passes.

Section 9.3 asks for at least three repetitions with the median and the worst
reported, which is what :func:`measure` does -- a single run on a shared laptop
is noise, not a benchmark.
"""

from __future__ import annotations

import json
import statistics
import time

import pytest

from market_sim.engine import run_simulation
from market_sim.recorder import EVENTS_NAME
from market_sim.verify import Verdict, verify_run

pytestmark = [pytest.mark.perf, pytest.mark.slow]


async def measure(make_config, *, rate, count, repeats=3, **kwargs):
    """Run the same workload several times and summarise honestly."""
    rates = []
    lags = []
    for _ in range(repeats):
        cfg = make_config(mode="record", rate=rate, count=count, **kwargs)
        result = await run_simulation(cfg)
        assert result.ok, result.report.get("error")
        assert result.report["counts"]["recorded_events"] == count
        rates.append(result.report["rates"]["actual_generated_rate_per_sec"])
        lags.append(result.report["p95_scheduler_lag_ms"] or 0.0)
    return {
        "target_rate": rate,
        "count": count,
        "median_rate": statistics.median(rates),
        "worst_rate": min(rates),
        "best_rate": max(rates),
        "median_p95_lag_ms": statistics.median(lags),
        "all_rates": rates,
    }


class TestRateAccuracy:
    """NFR-04: below the machine's capacity, the achieved rate tracks the target."""

    async def test_l0_100_events_per_second(self, make_config, capsys):
        summary = await measure(make_config, rate=100, count=300)
        with capsys.disabled():
            print("\nL0 " + json.dumps(summary))
        assert 0.90 <= summary["median_rate"] / 100 <= 1.10

    async def test_l1_1000_events_per_second(self, make_config, capsys):
        summary = await measure(make_config, rate=1_000, count=3_000)
        with capsys.disabled():
            print("\nL1 " + json.dumps(summary))
        # The tolerance is wider than NFR-04's 2% on purpose: that figure is
        # defined over a 60-second window, and this is a 3-second one on a
        # machine whose asyncio timer granularity is ~15 ms (see README).
        assert 0.90 <= summary["median_rate"] / 1_000 <= 1.10

    async def test_l2_10000_events_per_second_is_measured_not_asserted(
        self, make_config, capsys
    ):
        """L2 is reported, not gated.

        Measured on the development machine, this same workload came back
        anywhere between ~1.2k and ~25k events/s depending on load and on
        whether the OS was scanning a freshly created output directory. A
        10k/s pass-fail gate on that spread would be a coin toss dressed up
        as a requirement, so section 9.3's rule applies: record the number,
        do not force the pass.

        What *is* asserted holds on any machine: every slot was produced,
        every slot was recorded, and a shortfall against the target shows up
        in the lag figures instead of being hidden.
        """
        summary = await measure(make_config, rate=10_000, count=30_000)
        with capsys.disabled():
            print("\nL2 " + json.dumps(summary))
        assert summary["median_rate"] > 0
        if summary["median_rate"] < 9_000:
            # Falling short is an acceptable outcome; silently claiming the
            # target would not be.
            assert summary["median_p95_lag_ms"] > 0, (
                "the run fell short of the target rate but reported no "
                "scheduler lag, so the shortfall would have been invisible"
            )

    async def test_the_rate_is_measured_not_assumed(self, make_config):
        # Producing every slot does not mean the rate was met: reaching the
        # count and reaching the rate are different claims.
        cfg = make_config(
            mode="record", rate=5_000_000, count=20_000, max_scheduler_lag_ms=50
        )
        result = await run_simulation(cfg)
        assert result.ok
        # Every slot was produced...
        assert result.report["counts"]["logical_slots"] == 20_000
        assert result.report["counts"]["recorded_events"] == 20_000
        # ...and yet the rate was nowhere near the target, which the report says
        # plainly rather than inferring success from the slot count.
        assert result.report["rates"]["actual_generated_rate_per_sec"] < 5_000_000
        assert result.report["p95_scheduler_lag_ms"] > 0
        assert result.report["metrics"]["counters"]["scheduler_lag_breach_total"] > 0


class TestCapacityCeiling:
    """NFR-07 / section 11.1: measure the ceiling, do not assert a wish."""

    async def test_record_only_ceiling_is_measured_and_reported(
        self, make_config, capsys
    ):
        # The target is set far above what pure Python can do, so the measured
        # figure is this implementation's ceiling on this machine.
        cfg = make_config(mode="record", rate=5_000_000, count=100_000)
        started = time.monotonic()
        result = await run_simulation(cfg)
        elapsed = time.monotonic() - started
        assert result.ok
        ceiling = result.report["rates"]["actual_generated_rate_per_sec"]
        with capsys.disabled():
            print(
                "\nCEILING "
                + json.dumps(
                    {
                        "record_only_events_per_sec": ceiling,
                        "wall_clock_s": round(elapsed, 3),
                        "current_rss_mb": result.report["resources"]["current_rss_mb"],
                        "peak_rss_mb_process_lifetime": result.report["resources"][
                            "peak_rss_mb"
                        ],
                        "peak_file_queue_bytes": result.report["file"]["queue"][
                            "peak_bytes"
                        ],
                        "python": result.report["environment"]["python_version"],
                        "platform": result.report["environment"]["platform"],
                    }
                )
            )
        # A floor low enough to be defensible on a loaded machine, high enough
        # that a real regression (an accidental per-event fsync, say) trips it.
        assert ceiling > 1_000, (
            f"measured {ceiling:.0f} events/s; something has regressed badly"
        )

    async def test_memory_stays_bounded_at_the_ceiling(self, make_config):
        # Case R15 in miniature: the queue budget, not the event count, decides
        # how much memory a run uses.
        a = await run_simulation(
            make_config(
                mode="record", rate=5_000_000, count=20_000, max_queue_events=1_000
            )
        )
        b = await run_simulation(
            make_config(
                mode="record", rate=5_000_000, count=200_000, max_queue_events=1_000
            )
        )
        assert a.ok and b.ok
        assert a.report["file"]["queue"]["peak_depth"] <= 1_000
        assert b.report["file"]["queue"]["peak_depth"] <= 1_000
        # Ten times the events, and the queue high-water mark does not move.
        assert b.report["file"]["queue"]["peak_bytes"] <= a.report["file"]["queue"]["max_bytes"]


class TestE2E13LargeRun:
    async def test_one_million_records_verify_with_bounded_memory(
        self, make_config, capsys
    ):
        # E2E-13: the verifier streams, so it must handle a file far larger than
        # it could buffer. Anomaly tracking is off because it is O(sequences) by
        # design -- case C13 is exactly that trade-off.
        cfg = make_config(
            mode="record", rate=5_000_000, count=1_000_000, max_total_bytes=1 << 32
        )
        result = await run_simulation(cfg)
        assert result.ok
        size_mb = (result.run_dir / EVENTS_NAME).stat().st_size / (1024 * 1024)

        started = time.monotonic()
        verdict = verify_run(result.run_dir, check_anomalies=False)
        verify_s = time.monotonic() - started

        with capsys.disabled():
            print(
                "\nE2E-13 "
                + json.dumps(
                    {
                        "records": verdict.lines,
                        "file_mb": round(size_mb, 1),
                        "verify_seconds": round(verify_s, 2),
                        "generate_rate": result.report["rates"][
                            "actual_generated_rate_per_sec"
                        ],
                        "current_rss_mb": result.report["resources"]["current_rss_mb"],
                        "peak_rss_mb_process_lifetime": result.report["resources"][
                            "peak_rss_mb"
                        ],
                    }
                )
            )
        assert verdict.verdict is Verdict.COMPLETE, verdict.reasons
        assert verdict.lines == 1_000_000
        assert verdict.sha256 == result.report["file"]["sha256"]

    async def test_full_anomaly_verification_of_a_large_file(self, make_config):
        # With anomaly tracking on, every sequence is still accounted for.
        cfg = make_config(mode="record", rate=5_000_000, count=200_000)
        result = await run_simulation(cfg)
        verdict = verify_run(result.run_dir)
        assert verdict.verdict is Verdict.COMPLETE, verdict.reasons
        assert verdict.totals["gaps"] == 0
        assert verdict.totals["duplicates"] == 0
        assert sum(s["count"] for s in verdict.symbols.values()) == 200_000
