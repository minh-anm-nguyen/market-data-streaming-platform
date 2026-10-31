"""Throughput and latency measurement, with the claims kept honest.

Spec 10.4 and NFR-11 insist on two separations that are easy to blur:

  * a *requested* producer rate is not an achieved insert rate. Everything
    measured here is work the pipeline actually completed;
  * these numbers describe this machine with an in-memory sink. They say
    nothing about 120k events/s on real Kafka and real ClickHouse, because the
    sink here does no network I/O at all. The dominant cost in production --
    the insert round trip -- is entirely absent.

So these tests assert on *correctness under volume* and record throughput as
information. The only performance assertion is that validation and digesting
stay roughly linear, which would catch an accidental quadratic.
"""

from __future__ import annotations

import time

import pytest

from p05_ingest.canonical_digest import payload_digest
from p05_ingest.contract_validator import ValidationOk
from p05_ingest.metrics import CH_INSERT_LATENCY, KAFKA_RECORDS_FETCHED
from p05_ingest.model import ConsumedRecord, EventKind, LogicalProjection, TopicPartition
from p05_ingest.testkit import TICKS_TOPIC, make_tick
from p05_ingest.testkit.workload import produce_market_workload
from tests.conftest import build_harness, make_config

pytestmark = pytest.mark.perf

TP0 = TopicPartition(TICKS_TOPIC, 0)


def projection(sequence: int) -> LogicalProjection:
    return LogicalProjection(
        dataset_id="demo-a1b2c3",
        symbol="EURUSD",
        sequence=sequence,
        event_time_ns=1_704_171_845_000_000_000 + sequence,
        price_scale=100_000,
        kind=EventKind.TICK,
        source_schema_version="marketdata.v1",
        price_ticks=109_385 + (sequence % 100),
    )


def test_digest_throughput(capsys):
    """How fast the canonical encoding and SHA-256 run on this machine."""
    count = 50_000
    projections = [projection(i) for i in range(count)]

    started = time.perf_counter()
    for p in projections:
        payload_digest(p)
    elapsed = time.perf_counter() - started

    rate = count / elapsed
    print(f"\ndigest: {rate:,.0f} events/s ({elapsed:.2f}s for {count:,})")
    assert rate > 1000, "a pathologically slow digest would make any rate moot"


def test_validation_throughput(validator, capsys):
    count = 20_000
    value = make_tick(sequence=1).SerializeToString()
    record = ConsumedRecord(tp=TP0, offset=0, key=b"EURUSD", value=value)

    started = time.perf_counter()
    for _ in range(count):
        result = validator.validate(record)
    elapsed = time.perf_counter() - started
    assert isinstance(result, ValidationOk)

    rate = count / elapsed
    print(f"\nvalidate: {rate:,.0f} records/s ({elapsed:.2f}s for {count:,})")
    assert rate > 500


def test_digest_cost_is_linear_in_the_record_count():
    """A quadratic here would be invisible at small scale and fatal at large."""
    def elapsed_for(count: int) -> float:
        projections = [projection(i) for i in range(count)]
        started = time.perf_counter()
        for p in projections:
            payload_digest(p)
        return time.perf_counter() - started

    small = elapsed_for(5_000)
    large = elapsed_for(20_000)
    # Four times the work should cost roughly four times the time. A generous
    # bound, because this runs alongside other tests.
    assert large < small * 12, (
        f"4x the records cost {large / max(small, 1e-9):.1f}x the time, which "
        "suggests worse-than-linear scaling"
    )


def test_end_to_end_pipeline_rate_against_the_in_memory_sink(dev_config, clock, capsys):
    """Volume with exact correctness. The rate is recorded, not asserted."""
    events = 20_000
    cfg = make_config(
        dev_config,
        batch={"per_partition_max_rows": 500, "max_inflight_batches_global": 4},
    )
    harness = build_harness(cfg, clock=clock, partitions=8)
    manifest = produce_market_workload(
        harness.cluster, events=events, partitions=8, seed=17
    )

    started = time.perf_counter()
    status = harness.finish()
    elapsed = time.perf_counter() - started

    assert status.complete, status.reason
    assert len(harness.logical_identities()) == events
    assert harness.sink.row_count == events
    assert harness.metrics.counter_total(KAFKA_RECORDS_FETCHED) == events

    rate = events / elapsed
    print(
        f"\nend-to-end: {rate:,.0f} events/s ({elapsed:.2f}s for {events:,}) "
        f"over 8+8 partitions, {len(harness.sink.attempts)} inserts"
    )
    print(
        "NOTE: the sink is in-memory. This figure excludes the ClickHouse "
        "round trip, which dominates production throughput. It is NOT evidence "
        "for any events/s claim against real infrastructure (spec 10.4)."
    )
    assert len(manifest) == events


def test_batch_economy_improves_with_larger_batches(dev_config, capsys):
    """Spec 10.3: the batching-versus-parts trade-off, measured.

    Larger batches mean fewer inserts and fewer MergeTree parts. The test
    records both so the trade-off is visible rather than assumed.
    """
    from p05_ingest.testkit import FakeClock

    events = 4_000
    results = []
    for max_rows in (10, 100, 1000):
        cfg = make_config(dev_config, batch={"per_partition_max_rows": max_rows})
        harness = build_harness(cfg, clock=FakeClock(), partitions=2)
        produce_market_workload(harness.cluster, events=events, partitions=2, seed=19)
        harness.finish()
        results.append((max_rows, len(harness.sink.attempts), harness.sink.active_parts))
        assert harness.sink.row_count == events

    print("\nmax_rows  inserts  parts")
    for max_rows, inserts, parts in results:
        print(f"{max_rows:>8}  {inserts:>7}  {parts:>5}")

    insert_counts = [inserts for _, inserts, _ in results]
    assert insert_counts == sorted(insert_counts, reverse=True), (
        "larger batches must produce fewer inserts"
    )


def test_insert_latency_histogram_is_populated(dev_config, clock):
    """Spec 11.1: the latency histogram must actually record observations."""
    cfg = make_config(dev_config, batch={"per_partition_max_rows": 50})
    harness = build_harness(cfg, clock=clock, partitions=2)
    produce_market_workload(harness.cluster, events=500, partitions=2, seed=23)
    harness.finish()

    hist = harness.metrics.histogram(CH_INSERT_LATENCY)
    assert hist is not None
    assert hist.total == len(harness.sink.attempts)
    assert hist.quantile(0.95) >= 0.0

    snapshot = harness.metrics.as_dict()
    assert CH_INSERT_LATENCY in snapshot["histograms"]
    assert "p95" in snapshot["histograms"][CH_INSERT_LATENCY]


@pytest.mark.slow
def test_sustained_run_stays_correct_at_higher_volume(dev_config, clock, capsys):
    """A longer run, checked for exact correctness rather than speed."""
    events = 60_000
    cfg = make_config(
        dev_config,
        batch={"per_partition_max_rows": 1000, "max_inflight_batches_global": 4},
    )
    harness = build_harness(cfg, clock=clock, partitions=8)
    manifest = produce_market_workload(
        harness.cluster, events=events, partitions=8, seed=29
    )

    started = time.perf_counter()
    status = harness.finish()
    elapsed = time.perf_counter() - started

    assert status.complete, status.reason
    expected = {e.identity for e in manifest}
    assert harness.logical_identities() == expected
    assert harness.sink.row_count == events

    from p05_ingest.reconciler import verify_stored_digests

    assert verify_stored_digests(harness.sink.rows) == []
    print(f"\nsustained: {events / elapsed:,.0f} events/s over {elapsed:.1f}s")
