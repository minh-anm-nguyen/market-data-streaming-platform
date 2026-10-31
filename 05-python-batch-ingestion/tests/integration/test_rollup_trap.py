"""E2E-16 the naive materialized-bar trap, and the P06 handoff contract.

Spec 6.2 and E2E-16: an aggregate computed straight off ``market_events_raw``
double-counts whenever a retry produced a physical duplicate. The point of this
file is to *demonstrate the unsafety* rather than to paper over it -- the test
asserts that the naive rollup is wrong, and that the dedup-aware one is right.

That is the deliverable for P06 (spec 17.6): the raw schema, the logical
identity and digest contract, a strict conflict gate, and a worked example of
the aggregation that breaks.
"""

from __future__ import annotations

import pytest

from p05_ingest.model import EventIdentity
from p05_ingest.testkit import TICKS_TOPIC, Behaviour, make_tick
from tests.conftest import build_harness, make_config


def ticks_only(dev_config, **batch):
    return make_config(dev_config, kafka={"topics": ["md.ticks.v1"]}, batch=batch)


def naive_rollup(rows) -> dict[str, dict[str, int]]:
    """What a materialized view over the raw table would compute.

    Straight SUM/COUNT/MIN/MAX over every physical row -- no de-duplication.
    This is the implementation the spec warns against.
    """
    out: dict[str, dict[str, int]] = {}
    for row in rows:
        bucket = out.setdefault(
            row["symbol"],
            {"count": 0, "sum_price": 0, "high": None, "low": None},
        )
        price = row["price_ticks"]
        bucket["count"] += 1
        bucket["sum_price"] += price
        bucket["high"] = price if bucket["high"] is None else max(bucket["high"], price)
        bucket["low"] = price if bucket["low"] is None else min(bucket["low"], price)
    return out


def dedup_aware_rollup(rows) -> dict[str, dict[str, int]]:
    """The safe version: collapse to one row per logical identity first.

    Requires the conflict gate to have passed -- picking ``any()`` per identity
    is only valid once every physical row for that identity is known to carry
    the same semantic projection (spec 6.3).
    """
    canonical: dict[EventIdentity, dict] = {}
    digests: dict[EventIdentity, set[str]] = {}
    for row in rows:
        identity = EventIdentity(row["dataset_id"], row["symbol"], row["sequence"])
        digests.setdefault(identity, set()).add(row["payload_digest"])
        canonical.setdefault(identity, row)

    conflicted = {i for i, variants in digests.items() if len(variants) > 1}
    if conflicted:
        raise AssertionError(
            f"conflict gate: {len(conflicted)} identity(ies) carry more than one "
            "semantic payload; no aggregate may be computed until that is resolved"
        )
    return naive_rollup(list(canonical.values()))


def run_with_a_duplicate(dev_config, clock):
    """A run whose retry duplicates the rows, as after a window eviction."""
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(
        cfg,
        clock=clock,
        partitions=1,
        sink_script=(Behaviour.WRITE_THEN_LOSE_ACK,),
        dedup_window=1,
        evict_dedup_after_attempt=1,
    )
    prices = [100_000, 110_000, 120_000, 130_000]
    for sequence, price in enumerate(prices):
        harness.cluster.produce(
            TICKS_TOPIC,
            0,
            key=b"EURUSD",
            value=make_tick(sequence=sequence, price_ticks=price).SerializeToString(),
        )
    status = harness.finish()
    return harness, prices, status


def test_e2e_16_naive_rollup_double_counts_a_retry_duplicate(dev_config, clock):
    """The trap, demonstrated. This assertion documents a real hazard."""
    harness, prices, status = run_with_a_duplicate(dev_config, clock)

    assert status.complete, status.reason
    assert harness.sink.row_count == 2 * len(prices), "the retry duplicated the rows"

    naive = naive_rollup(harness.sink.rows)["EURUSD"]
    assert naive["count"] == 8, "count is double the number of market events"
    assert naive["sum_price"] == 2 * sum(prices), "sum is double"

    # A volume-weighted average built on these would still look plausible,
    # which is exactly why the error is dangerous rather than obvious.
    naive_mean = naive["sum_price"] / naive["count"]
    true_mean = sum(prices) / len(prices)
    assert naive_mean == pytest.approx(true_mean), (
        "the mean happens to survive, so a sanity check on averages would not "
        "catch this; only count and sum reveal it"
    )


def test_e2e_16_dedup_aware_rollup_is_correct_on_the_same_rows(dev_config, clock):
    harness, prices, _ = run_with_a_duplicate(dev_config, clock)

    safe = dedup_aware_rollup(harness.sink.rows)["EURUSD"]
    assert safe["count"] == len(prices)
    assert safe["sum_price"] == sum(prices)
    assert safe["high"] == max(prices)
    assert safe["low"] == min(prices)


def test_e2e_16_high_and_low_survive_duplication_but_count_does_not(
    dev_config, clock
):
    """Which aggregates are duplicate-safe, stated precisely.

    MIN and MAX are idempotent under duplication; COUNT, SUM and anything
    derived from them are not. An OHLC bar mixes both, so the bar as a whole is
    unsafe.
    """
    harness, prices, _ = run_with_a_duplicate(dev_config, clock)
    naive = naive_rollup(harness.sink.rows)["EURUSD"]
    safe = dedup_aware_rollup(harness.sink.rows)["EURUSD"]

    assert naive["high"] == safe["high"], "MAX is idempotent"
    assert naive["low"] == safe["low"], "MIN is idempotent"
    assert naive["count"] != safe["count"], "COUNT is not"
    assert naive["sum_price"] != safe["sum_price"], "SUM is not"


def test_conflict_gate_blocks_aggregation_entirely(dev_config, clock):
    """With a conflict present, no aggregate may be computed at all."""
    from p05_ingest.testkit import conflict_pair

    cfg = ticks_only(dev_config, per_partition_max_rows=2)
    harness = build_harness(cfg, clock=clock, partitions=1)
    for entry in conflict_pair():
        harness.cluster.produce(TICKS_TOPIC, 0, key=entry.key, value=entry.value)
    harness.finish()

    with pytest.raises(AssertionError, match="conflict gate"):
        dedup_aware_rollup(harness.sink.rows)


def test_p06_handoff_columns_are_all_present(dev_config, clock):
    """The contract P06 depends on: identity, digest, event time, provenance."""
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(cfg, clock=clock, partitions=1)
    for sequence in range(4):
        harness.cluster.produce(
            TICKS_TOPIC,
            0,
            key=b"EURUSD",
            value=make_tick(sequence=sequence).SerializeToString(),
        )
    harness.finish()

    required = {
        # logical identity
        "dataset_id",
        "symbol",
        "sequence",
        # semantic fingerprint, for the conflict gate
        "payload_digest",
        # event time, which is what analytics must order by
        "event_time_ns",
        "event_date",
        # the values, as exact integers
        "price_scale",
        "price_ticks",
        "bid_ticks",
        "ask_ticks",
        "event_kind",
        # provenance, so a finding can be traced back to Kafka
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "topic_generation",
        "ingest_run_id",
    }
    for row in harness.sink.rows:
        assert required <= set(row.values), (
            f"missing P06 handoff column(s): {sorted(required - set(row.values))}"
        )


def test_no_aggregate_table_is_created_by_p05(dev_config, clock):
    """Spec 6.4: P05 must not enable materialized bars."""
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(cfg, clock=clock, partitions=1)
    for sequence in range(4):
        harness.cluster.produce(
            TICKS_TOPIC,
            0,
            key=b"EURUSD",
            value=make_tick(sequence=sequence).SerializeToString(),
        )
    harness.finish()

    tables = {("market_events_raw")}
    # The fake sink tracks parts per (table, date); only the raw table appears.
    written_tables = {key[0] for key in harness.sink._parts}
    assert written_tables <= tables
