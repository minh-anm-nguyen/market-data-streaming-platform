"""Resource bounds measured, not inferred from the budget.

Spec I-05 and section 5.4 are blunt about this: a 64 MiB queue budget does not
prove a 64 MiB RSS. The accounted-bytes checks live in the unit and integration
suites; these tests measure the *process* with ``psutil`` and check the one
property that matters for a long run -- that memory and handles do not grow
without bound while the sink is stalled.

What they do NOT do is certify an RSS ceiling. The figures depend on the
interpreter, the allocator and the platform, so they are printed for a report
rather than turned into a threshold that would pass or fail by accident. The
only hard assertions are about growth.
"""

from __future__ import annotations

import gc

import pytest

from p05_ingest.model import TopicPartition
from p05_ingest.testkit import TICKS_TOPIC, Behaviour, FakeClock, make_tick
from p05_ingest.testkit.workload import produce_market_workload
from tests.conftest import build_harness, make_config

pytestmark = [pytest.mark.perf, pytest.mark.slow]

psutil = pytest.importorskip("psutil")


def rss_bytes() -> int:
    gc.collect()
    return psutil.Process().memory_info().rss


def open_handles() -> int:
    process = psutil.Process()
    try:
        return process.num_fds()
    except AttributeError:
        # Windows has no num_fds; handles are the closest equivalent.
        return process.num_handles()


def test_rss_does_not_grow_without_bound_while_the_sink_is_stalled(
    dev_config, clock
):
    """The backpressure path must not leak while refusing to make progress."""
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={
            "per_partition_max_rows": 200,
            "per_partition_max_serialized_bytes": 1_048_576,
            "app_buffer_budget_bytes": 4_194_304,
        },
        retry={"max_attempts": 1, "overall_deadline_s": 1},
    )
    harness = build_harness(
        cfg, clock=clock, partitions=2, sink_default=Behaviour.REFUSE_BEFORE_WRITE
    )
    produce_market_workload(harness.cluster, events=20_000, partitions=2, seed=3)

    baseline = rss_bytes()
    samples = []
    for _ in range(20):
        for _ in range(100):
            harness.engine.step()
            harness.tick_timer()
        samples.append(rss_bytes())

    harness.engine.shutdown()
    peak = max(samples)
    tail_growth = samples[-1] - samples[len(samples) // 2]

    print(
        f"\nRSS baseline={baseline / 1e6:.1f} MB peak={peak / 1e6:.1f} MB "
        f"tail_growth={tail_growth / 1e6:.1f} MB"
    )
    print(
        "NOTE: a measurement, not a certified ceiling. It depends on the "
        "interpreter and allocator; only the growth bound below is asserted."
    )

    # The second half must not keep climbing: a steady state under a
    # permanently stalled sink is the property being tested.
    assert tail_growth < 32 * 1024 * 1024, (
        f"RSS grew {tail_growth / 1e6:.1f} MB during the second half of a "
        "stalled run, which suggests unbounded retention"
    )


def test_accounted_bytes_never_exceed_the_budget_during_a_long_run(
    dev_config, clock
):
    cfg = make_config(
        dev_config,
        batch={
            "per_partition_max_rows": 100,
            "per_partition_max_serialized_bytes": 262_144,
            "app_buffer_budget_bytes": 1_048_576,
        },
        retry={"max_attempts": 1, "overall_deadline_s": 1},
    )
    harness = build_harness(
        cfg, clock=clock, partitions=4, sink_default=Behaviour.REFUSE_BEFORE_WRITE
    )
    produce_market_workload(harness.cluster, events=20_000, partitions=4, seed=11)

    budget = harness.engine.budget
    peak = 0
    for _ in range(1500):
        harness.engine.step()
        peak = max(peak, budget.reserved_bytes)
        assert budget.reserved_bytes <= budget.budget_bytes
        harness.tick_timer()
    harness.engine.shutdown()

    print(f"\npeak accounted bytes={peak} of {budget.budget_bytes}")
    assert peak > 0, "the budget was never exercised"


def test_handles_do_not_leak_across_many_runs(dev_config):
    """Repeated engine construction must not accumulate handles."""
    before = open_handles()
    for _ in range(40):
        harness = build_harness(dev_config, clock=FakeClock(), partitions=2)
        produce_market_workload(harness.cluster, events=100, partitions=2, seed=1)
        harness.finish()
    gc.collect()
    after = open_handles()

    print(f"\nopen handles before={before} after={after}")
    assert after - before < 50, f"handle count grew by {after - before} across 40 runs"


def test_commit_ledger_memory_is_bounded_by_the_inflight_window(dev_config, clock):
    """The pending list must not grow with the topic.

    A ledger that kept every consumed offset would be a slow leak invisible to
    the byte budget, because it holds ints rather than payloads.
    """
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={"per_partition_max_rows": 10},
    )
    harness = build_harness(cfg, clock=clock, partitions=1)
    for sequence in range(5000):
        harness.cluster.produce(
            TICKS_TOPIC,
            0,
            key=b"EURUSD",
            value=make_tick(sequence=sequence).SerializeToString(),
        )

    tp0 = TopicPartition(TICKS_TOPIC, 0)
    worst_pending = 0
    for _ in range(3000):
        harness.engine.step()
        if tp0 in harness.engine.commits.tracked():
            worst_pending = max(
                worst_pending, harness.engine.commits.pending_count(tp0)
            )
        harness.tick_timer()
    harness.engine.shutdown()

    print(f"\nworst pending ledger entries={worst_pending}")
    # Bounded by one in-flight batch plus whatever a single step fetched.
    assert worst_pending <= 600, (
        f"the pending ledger reached {worst_pending} entries, which is not "
        "bounded by the in-flight window"
    )
    assert harness.engine.commits.pending_count(tp0) == 0, "fully drained"
