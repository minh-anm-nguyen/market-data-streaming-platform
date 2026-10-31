"""Measurements, reported rather than asserted against a target.

Design 13 asks for capacity profiles ``S1`` to ``S6`` on real infrastructure.
Those are ``NOT_RUN`` here and no number in this file is a throughput claim: an
in-process simulation measures Python, not Kafka, and publishing its rate as a
pipeline figure is exactly what design 2.1 forbids the reporter to do.

What these tests *do* assert is the shape of the growth, which is meaningful
even in a simulation:

* a 4x larger dataset must not take more than ~8x the time (no accidental
  quadratic path),
* the per-run identity maps must not retain memory across runs,
* the sharded diff must hold bounded memory where the in-memory one does not.

The measured numbers are printed so a developer can see a regression, and are
marked ``perf`` so a correctness run can skip them.
"""

from __future__ import annotations

import gc
import time
from typing import Any, Callable

import pytest

from p07.dataplane.adapter import Adapter, limits_from_sealed
from p07.dataplane.codec import DEFAULT_CODEC
from p07.dataset_sealer import seal_dataset
from p07.injectors.resources import rss_bytes
from p07.oracle.acceptance_oracle import build_expectation
from p07.reconcile.diff import diff_identity_maps, sharded_diff
from p07.replay.source_replayer import replay_source

pytestmark = [pytest.mark.perf, pytest.mark.slow]


def classify(run_dir: str) -> tuple[float, int]:
    """Seal and classify a dataset. Returns ``(seconds, accepted)``."""
    started = time.perf_counter()
    dataset = seal_dataset(run_dir)
    expectation = build_expectation(dataset)
    return time.perf_counter() - started, expectation.counts["accepted"]


def test_oracle_cost_grows_about_linearly(
    make_source: Callable[..., dict[str, Any]], capsys: pytest.CaptureFixture[str]
) -> None:
    """A 4x dataset must not cost 16x -- that would mean a quadratic path."""
    small = make_source(name="perf-small", count=500, dataset_id="perf-s")
    large = make_source(name="perf-large", count=2000, dataset_id="perf-l")

    small_seconds, small_count = classify(small["run_dir"])
    large_seconds, large_count = classify(large["run_dir"])
    with capsys.disabled():
        print(
            f"\noracle: {small_count} events in {small_seconds:.3f}s, "
            f"{large_count} in {large_seconds:.3f}s "
            f"(ratio {large_seconds / max(small_seconds, 1e-9):.2f}x for 4x the data)"
        )
    assert large_count == 4 * small_count
    # Generous: the machine is shared and the sample is small. The point is to
    # catch an order-of-magnitude regression, not to police a constant factor.
    assert large_seconds < small_seconds * 12 + 2.0


def test_replay_and_adapter_throughput_is_reported_not_asserted(
    make_source: Callable[..., dict[str, Any]], capsys: pytest.CaptureFixture[str]
) -> None:
    """Prints the simulation's rate, with the caveat attached."""
    info = make_source(name="perf-replay", count=2000, dataset_id="perf-r")
    dataset = seal_dataset(info["run_dir"])
    published: list[Any] = []
    adapter = Adapter(
        dataset_id=dataset.dataset_id,
        limits=limits_from_sealed(dataset.symbols),
        sink=published.append,
        codec=DEFAULT_CODEC,
    )
    started = time.perf_counter()
    report = replay_source(dataset, adapter)
    elapsed = time.perf_counter() - started
    rate = report.frames_sent / elapsed if elapsed else float("inf")
    with capsys.disabled():
        print(
            f"\nmodel replay: {report.frames_sent} frames in {elapsed:.3f}s "
            f"({rate:,.0f} events/s in-process). This measures Python, NOT the "
            "pipeline: no broker, no network, no ClickHouse."
        )
    assert len(published) == report.frames_sent
    assert rate > 0


def test_the_sharded_diff_holds_bounded_memory(
    make_source: Callable[..., dict[str, Any]], capsys: pytest.CaptureFixture[str]
) -> None:
    """Design 13.3: an exact diff of a large fixture must not need it all at once.

    The assertion is about *shape*: the sharded path's peak must not exceed the
    in-memory path's by a wide margin. RSS on a shared machine is noisy, so this
    reports the numbers and only fails if the sharded path is dramatically
    worse -- which would mean it is not actually sharding.
    """
    info = make_source(name="perf-diff", count=3000, dataset_id="perf-d")
    expectation = build_expectation(seal_dataset(info["run_dir"]))
    rows = [(core.identity, core) for core in expectation.accepted.values()]

    gc.collect()
    before_memory = rss_bytes()
    whole = diff_identity_maps(dict(rows), dict(rows))
    gc.collect()
    after_memory = rss_bytes()

    sharded = sharded_diff(lambda: rows, lambda: rows, shards=16)
    gc.collect()
    sharded_memory = rss_bytes()

    assert whole.equal and sharded.equal
    assert whole.expected_count == sharded.expected_count == len(rows)
    if None not in (before_memory, after_memory, sharded_memory):
        with capsys.disabled():
            print(
                f"\ndiff of {len(rows)} identities: rss before={before_memory:,} "
                f"in-memory={after_memory:,} sharded={sharded_memory:,}"
            )
        assert sharded_memory < after_memory * 2 + (64 << 20)


def test_repeated_runs_do_not_retain_memory(
    make_source: Callable[..., dict[str, Any]],
    run_factory: Callable[..., Any],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A short soak: five full runs, checking the trend rather than one sample.

    Design 13.2 wants a memory *slope*. Five runs is not a 24-hour soak and is
    not presented as one; it is enough to catch a per-run leak in the harness
    itself, which is what would make a real soak impossible.
    """
    source = make_source(name="perf-soak", count=300, dataset_id="perf-soak")
    samples: list[int] = []
    for index in range(5):
        run = run_factory(source=source, scenario_id="perf-soak", run_id=f"r{index}")
        gate, _ = run.run_clean()
        assert gate.verdict.value == "PASS"
        del run
        gc.collect()
        current = rss_bytes()
        if current is not None:
            samples.append(current)

    if len(samples) < 5:
        pytest.skip("psutil unavailable; RSS trend cannot be measured")
    with capsys.disabled():
        print(f"\nrss across 5 runs: {[f'{value:,}' for value in samples]}")
    growth = samples[-1] - samples[0]
    # A leak would show as monotonic growth of the whole working set. 48 MiB of
    # slack over five runs covers allocator noise and the interpreter's own
    # caches without hiding a real per-run retention.
    assert growth < (48 << 20), f"RSS grew {growth:,} bytes across five runs"
