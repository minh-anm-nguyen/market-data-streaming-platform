"""E2E-01 golden pipeline, and E2E-12 the UTC date boundary.

PASS oracle for E2E-01 (spec 9.5): every expected market-valid identity
appears exactly once logically, with full core-field equality, and no commit
runs ahead of the sink.
"""

from __future__ import annotations

import datetime as dt

from p05_ingest.canonical_digest import payload_digest
from p05_ingest.metrics import (
    CH_BATCHES_ACKED,
    KAFKA_COMMIT_ATTEMPTS,
    KAFKA_RECORDS_FETCHED,
    RECORDS_VALIDATED,
)
from p05_ingest.model import TopicPartition
from p05_ingest.reconciler import Verdict, observed_from_sink_rows, reconcile
from p05_ingest.testkit import TICKS_TOPIC, golden_corpus, make_tick
from p05_ingest.testkit.workload import (
    manifest_as_expected,
    produce_market_workload,
)
from tests.conftest import build_harness, make_config


def test_e2e_01_golden_pipeline_preserves_every_event(dev_config, clock):
    """A multi-symbol, two-topic, 8+8-partition run with a clean sink."""
    cfg = make_config(dev_config, batch={"per_partition_max_rows": 250})
    harness = build_harness(cfg, clock=clock, partitions=8)
    manifest = produce_market_workload(
        harness.cluster, events=2000, partitions=8, seed=7
    )

    status = harness.finish()

    assert status.complete, status.reason
    assert harness.engine.fatal_reason is None
    assert harness.engine.blocked_partitions == {}

    expected = {e.identity for e in manifest}
    stored = harness.logical_identities()
    assert stored == expected, "every expected identity, and nothing else"

    # Physical count equals logical count: a clean run produces no duplicates.
    assert harness.sink.row_count == len(manifest)

    # Full field equality via the digest, recomputed from the stored columns
    # by the independent verifier rather than trusted.
    from p05_ingest.reconciler import verify_stored_digests

    assert verify_stored_digests(harness.sink.rows) == []

    expected_digests = {e.identity: e.payload_digest for e in manifest}
    for row in harness.sink.rows:
        identity = (row["dataset_id"], row["symbol"], row["sequence"])
        assert row["payload_digest"] == expected_digests[identity]

    report = reconcile(
        expected=manifest_as_expected(manifest),
        kafka_observed=manifest_as_expected(manifest),
        sink_observed=observed_from_sink_rows(harness.sink.rows),
    )
    assert report.verdict is Verdict.GO, report.summary()


def test_e2e_01_commits_never_lead_the_sink(dev_config, clock):
    """I-01 observed from the broker side, at every step of the run."""
    cfg = make_config(dev_config, batch={"per_partition_max_rows": 20})
    harness = build_harness(cfg, clock=clock, partitions=4)
    produce_market_workload(harness.cluster, events=400, partitions=4, seed=11)

    for _ in range(4000):
        report = harness.engine.step()
        # After every single iteration the broker offsets must be covered by
        # rows that are actually in the table.
        for tp, committed in harness.committed().items():
            written = {
                row["kafka_offset"]
                for row in harness.sink.rows
                if row["kafka_topic"] == tp.topic
                and row["kafka_partition"] == tp.partition
            }
            log = harness.cluster.log(tp)
            delivered = {r.offset for r in log.records if not r.is_control}
            unproven = {o for o in delivered if o < committed} - written
            assert not unproven, (
                f"{tp}: offsets {sorted(unproven)} are committed past but absent "
                "from ClickHouse"
            )
        if report.idle:
            if harness.engine.batcher.open_partitions:
                harness.tick_timer()
                continue
            break

    harness.engine.shutdown()


def test_e2e_01_golden_corpus_round_trips_field_for_field(dev_config, clock):
    """The hand-specified corpus: boundaries, presence, UTF-8, punctuation."""
    harness = build_harness(dev_config, clock=clock, partitions=1)
    entries = golden_corpus()
    for entry in entries:
        harness.cluster.produce(entry.topic, 0, key=entry.key, value=entry.value)

    status = harness.finish()
    assert status.complete, status.reason
    assert harness.sink.row_count == len(entries)

    by_identity = {
        (row["dataset_id"], row["symbol"], row["sequence"]): row
        for row in harness.sink.rows
    }
    for entry in entries:
        expected = entry.expected
        row = by_identity[expected.identity]
        assert row["event_time_ns"] == expected.event_time_ns, entry.label
        assert row["price_scale"] == expected.price_scale, entry.label
        assert row["event_kind"] == expected.kind.ch_name, entry.label
        assert row["bid_ticks"] == expected.bid_ticks, entry.label
        assert row["ask_ticks"] == expected.ask_ticks, entry.label
        assert row["price_ticks"] == expected.price_ticks, entry.label
        assert row["source_schema_version"] == expected.source_schema_version
        assert row["payload_digest"] == payload_digest(expected), entry.label
        assert row["event_date"] == expected.event_date, entry.label


def test_e2e_12_utc_date_boundary_is_not_shifted_by_the_host_timezone(
    dev_config, clock, monkeypatch
):
    """E2E-12: a nanosecond stamp at the UTC day edge on a UTC+07 host."""
    import time

    monkeypatch.setenv("TZ", "Asia/Bangkok")
    if hasattr(time, "tzset"):
        time.tzset()
    try:
        harness = build_harness(dev_config, clock=clock, partitions=1)
        # 2024-01-01T23:59:59.999999999Z and the nanosecond after it.
        last_ns_of_day = 1_704_153_599_999_999_999
        for i, event_time_ns in enumerate((last_ns_of_day, last_ns_of_day + 1)):
            event = make_tick(sequence=i, event_time_ns=event_time_ns)
            harness.cluster.produce(
                TICKS_TOPIC, 0, key=b"EURUSD", value=event.SerializeToString()
            )

        status = harness.finish()
        assert status.complete, status.reason

        rows = sorted(harness.sink.rows, key=lambda r: r["sequence"])
        assert rows[0]["event_time_ns"] == last_ns_of_day
        assert rows[0]["event_date"] == dt.date(2024, 1, 1)
        assert rows[1]["event_time_ns"] == last_ns_of_day + 1
        assert rows[1]["event_date"] == dt.date(2024, 1, 2)
    finally:
        monkeypatch.delenv("TZ", raising=False)
        if hasattr(time, "tzset"):
            time.tzset()


def test_e2e_01_metrics_distinguish_the_pipeline_stages(dev_config, clock):
    """Spec 11.1: fetched, validated and acknowledged are separate facts."""
    cfg = make_config(dev_config, batch={"per_partition_max_rows": 10})
    harness = build_harness(cfg, clock=clock, partitions=2)
    manifest = produce_market_workload(
        harness.cluster, events=100, partitions=2, seed=3
    )
    harness.finish()

    metrics = harness.metrics
    assert metrics.counter_total(KAFKA_RECORDS_FETCHED) == len(manifest)
    assert metrics.counter_total(RECORDS_VALIDATED) == len(manifest)
    assert metrics.counter(RECORDS_VALIDATED, kind="tick", result="accepted") > 0
    assert metrics.counter(RECORDS_VALIDATED, kind="quote", result="accepted") > 0
    assert metrics.counter_total(CH_BATCHES_ACKED) > 0
    assert metrics.counter(KAFKA_COMMIT_ATTEMPTS, status="confirmed") > 0
    assert metrics.counter(KAFKA_COMMIT_ATTEMPTS, status="unknown") == 0


def test_rows_carry_provenance_for_reconciliation(dev_config, clock):
    """FR-14: Kafka coordinates and the run id must reach the table."""
    harness = build_harness(dev_config, clock=clock, partitions=2)
    produce_market_workload(harness.cluster, events=50, partitions=2, seed=5)
    harness.finish()

    for row in harness.sink.rows:
        assert row["kafka_topic"] in (TICKS_TOPIC, "md.quotes.v1")
        assert isinstance(row["kafka_partition"], int)
        assert isinstance(row["kafka_offset"], int)
        assert row["topic_generation"] == dev_config.project.topic_generation
        assert row["ingest_run_id"] == dev_config.project.run_id

    # The coordinate must be unique per physical row in a clean run.
    coordinates = [
        (r["kafka_topic"], r["kafka_partition"], r["kafka_offset"])
        for r in harness.sink.rows
    ]
    assert len(set(coordinates)) == len(coordinates)


def test_batch_boundaries_respect_the_configured_caps(dev_config, clock):
    cfg = make_config(dev_config, batch={"per_partition_max_rows": 7})
    harness = build_harness(cfg, clock=clock, partitions=1)
    produce_market_workload(harness.cluster, events=60, partitions=1, seed=9)
    harness.finish()

    for attempt in harness.sink.attempts:
        assert attempt.rows <= 7, "a batch exceeded the row cap"
    assert any(a.rows == 7 for a in harness.sink.attempts), "the cap was exercised"


def test_single_partition_reference_pipeline(dev_config, clock):
    """Spec 17.3: the one-partition synchronous reference path.

    Kept as its own test because it is the configuration the spec says to make
    work first, and a regression here invalidates everything built on top.
    """
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={"per_partition_max_rows": 5},
    )
    harness = build_harness(cfg, clock=clock, partitions=1)
    for sequence in range(12):
        event = make_tick(sequence=sequence)
        harness.cluster.produce(
            TICKS_TOPIC, 0, key=b"EURUSD", value=event.SerializeToString()
        )

    status = harness.finish()
    assert status.complete, status.reason
    assert harness.sink.row_count == 12
    assert harness.committed() == {TopicPartition(TICKS_TOPIC, 0): 12}
