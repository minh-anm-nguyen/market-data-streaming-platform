"""Cut capture, the read-only scan, and the four boundary reconciliations."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from p07.contracts import QUOTE_TOPIC, TICK_TOPIC, EventKind, SemanticCore, SourceIdentity
from p07.dataplane.adapter import Adapter, limits_from_sealed
from p07.dataplane.clickhouse_fake import RawRow
from p07.dataplane.codec import DEFAULT_CODEC, frame_mdp1
from p07.dataplane.kafka_fake import FakeKafka
from p07.dataplane.producer import Producer
from p07.evidence.verdicts import Reason
from p07.oracle.semantic_digest import digest_core
from p07.reconcile.bar_reconciler import reconcile_bars
from p07.reconcile.canonical_reconciler import reconcile_canonical
from p07.reconcile.diff import (
    count_physical,
    diff_identity_maps,
    shard_of,
    sharded_diff,
)
from p07.reconcile.raw_reconciler import frontier_findings, reconcile_kafka_cut, reconcile_raw
from p07.recovery.cut_coordinator import CutRejected, capture_cut, cut_findings, fence_producers
from p07.replay.kafka_cut_scanner import (
    CutUnscannable,
    KafkaCut,
    per_symbol_sequences,
    scan_cut,
    tick_only_sequences,
)
from p07.replay.source_replayer import replay_source


def core(sequence: int, *, kind: EventKind = EventKind.TICK, price: int = 108000) -> SemanticCore:
    return SemanticCore(
        dataset_id="ds",
        symbol="EURUSD",
        sequence=sequence,
        event_time_ns=1_000_000 * sequence,
        price_scale=100000,
        event_kind=kind,
        source_schema_version="sim.v1",
        bid_ticks=None,
        ask_ticks=None,
        price_ticks=price,
    )


def identity_map(*cores: SemanticCore) -> dict[SourceIdentity, SemanticCore]:
    return {item.identity: item for item in cores}


def raw_row(body: SemanticCore, *, offset: int, generation: str = "gen-1") -> RawRow:
    return RawRow(
        dataset_id=body.dataset_id,
        symbol=body.symbol,
        sequence=body.sequence,
        event_time_ns=body.event_time_ns,
        price_scale=body.price_scale,
        event_kind=body.event_kind.wire_name,
        source_schema_version=body.source_schema_version,
        bid_ticks=body.bid_ticks,
        ask_ticks=body.ask_ticks,
        price_ticks=body.price_ticks,
        payload_digest=digest_core(body),
        topic_generation=generation,
        topic=TICK_TOPIC,
        partition=0,
        offset=offset,
        batch_id="b1",
        ingested_at_ns=offset + 1,
    )


# -- the identity diff ----------------------------------------------------


def test_equal_maps_diff_to_nothing() -> None:
    expected = identity_map(core(1), core(2))
    diff = diff_identity_maps(expected, dict(expected))
    assert diff.equal
    assert diff.findings() == ()


def test_missing_extra_and_conflicting_are_distinguished() -> None:
    expected = identity_map(core(1), core(2))
    observed = identity_map(core(1, price=999), core(3))
    diff = diff_identity_maps(expected, observed)
    assert [identity.sequence for identity in diff.missing] == [2]
    assert [identity.sequence for identity in diff.extra] == [3]
    assert diff.mismatched[0][1] == ("price_ticks",)
    reasons = {finding.reason for finding in diff.findings()}
    assert reasons == {
        Reason.MISSING_IDENTITIES,
        Reason.EXTRA_IDENTITIES,
        Reason.IDENTITY_CONFLICT,
    }


@pytest.mark.whatif("P07-G29-03")
def test_missing_data_on_an_unproven_cut_is_not_reported_as_loss() -> None:
    """The branch that keeps the harness from manufacturing failures.

    Same inputs, different ``cut_proven``: with a proven cut the absence is
    data loss; without one the lab simply could not see, and the finding maps
    to INCONCLUSIVE instead of FAIL.
    """
    expected = identity_map(core(1), core(2))
    observed = identity_map(core(1))
    proven = diff_identity_maps(expected, observed, cut_proven=True).findings()
    unproven = diff_identity_maps(expected, observed, cut_proven=False).findings()
    assert proven[0].reason == Reason.MISSING_IDENTITIES
    assert unproven[0].reason == Reason.CUT_NOT_FENCED


def test_physical_duplicates_are_counted_not_treated_as_errors() -> None:
    expected = identity_map(core(1))
    diff = diff_identity_maps(expected, dict(expected), physical_counts={core(1).identity: 3})
    assert diff.equal
    assert diff.as_dict()["physical_duplicate_rows"] == 2
    assert diff.findings() == ()


def test_shard_assignment_is_stable_across_processes() -> None:
    """SHA-256, not ``hash()``: ``PYTHONHASHSEED`` would move the shard map."""
    identity = SourceIdentity("ds", "EURUSD", 7)
    assert shard_of(identity, 16) == shard_of(identity, 16)
    assert 0 <= shard_of(identity, 16) < 16
    with pytest.raises(ValueError):
        shard_of(identity, 0)


@pytest.mark.whatif("P07-G17-05")
def test_the_sharded_diff_agrees_with_the_in_memory_one() -> None:
    """The bounded-memory path for large fixtures must give the same answer."""
    expected = identity_map(*[core(index) for index in range(1, 40)])
    # One missing, one wrong price, one physical duplicate and one extra. Every
    # physical copy of a given identity is *identical*, so both paths see the
    # same logical observation -- the in-memory path takes an already-reduced
    # map and therefore cannot see a disagreement between two copies, which is
    # a difference in input, not in behaviour.
    observed_rows = [core(index) for index in range(1, 40) if index not in {7, 5}]
    observed_rows.append(core(5, price=1))  # differs from the expected price
    observed_rows.append(core(9))  # a physical duplicate, byte-identical
    observed_rows.append(core(100))  # an extra

    in_memory = diff_identity_maps(
        expected,
        identity_map(*observed_rows),
        physical_counts=count_physical(row.identity for row in observed_rows),
    )
    sharded = sharded_diff(
        lambda: [(item.identity, item) for item in expected.values()],
        lambda: [(row.identity, row) for row in observed_rows],
        shards=8,
    )
    assert set(sharded.missing) == set(in_memory.missing)
    assert set(sharded.extra) == set(in_memory.extra)
    assert {identity for identity, _ in sharded.mismatched} == {
        identity for identity, _ in in_memory.mismatched
    }


# -- cut capture and scanning ---------------------------------------------


@pytest.fixture
def published(kafka: FakeKafka, sealed: Any) -> dict[str, Any]:
    """Replay a small sealed dataset through the adapter into the broker."""
    producer = Producer(kafka)
    adapter = Adapter(
        dataset_id=sealed.dataset_id,
        limits=limits_from_sealed(sealed.symbols),
        sink=producer.enqueue,
    )
    replay_source(sealed, adapter)
    producer.flush()
    return {"kafka": kafka, "producer": producer, "dataset": sealed}


def test_a_captured_cut_is_fenced_and_covers_every_partition(published: dict[str, Any]) -> None:
    cut, fence = capture_cut(published["kafka"], producers=[published["producer"]])
    assert cut.fenced is True
    assert fence.drained is True
    assert fence.unknown == 0
    partitions = published["kafka"].partition_count(TICK_TOPIC)
    assert len(cut.ranges) == 2 * partitions
    assert cut.record_count == published["producer"].counters.delivered


@pytest.mark.whatif("P07-G17-03")
def test_a_cut_is_refused_when_a_watermark_moves_during_capture(
    published: dict[str, Any],
) -> None:
    """Design 3.3: an unfenced writer must invalidate the cut.

    Fencing drains the producers the coordinator knows about, so the danger is
    the writer it does *not* know about. That shows up as a high watermark that
    differs between the two snapshots, which is what this simulates -- an append
    that lands between the reads.
    """
    kafka = published["kafka"]
    original = kafka.high_watermark
    state = {"appended": False}

    def moving(topic: str, partition: int) -> int:
        # Read first, then append: the second snapshot must see a value the
        # first did not, which is what an active writer looks like.
        observed = original(topic, partition)
        if not state["appended"]:
            state["appended"] = True
            kafka.produce(topic, partition, key=b"OTHER", value=DEFAULT_CODEC.encode(core(1)))
        return observed

    kafka.high_watermark = moving  # type: ignore[method-assign]
    try:
        with pytest.raises(CutRejected) as exc:
            capture_cut(kafka, producers=[published["producer"]], stability_checks=2)
    finally:
        kafka.high_watermark = original  # type: ignore[method-assign]
    assert exc.value.reason == Reason.CUT_WRITER_ACTIVE


def test_a_cut_with_an_unfenced_flag_cannot_be_scanned(published: dict[str, Any]) -> None:
    cut, _ = capture_cut(published["kafka"], producers=[published["producer"]])
    with pytest.raises(CutUnscannable) as exc:
        scan_cut(published["kafka"], replace(cut, fenced=False))
    assert exc.value.reason == Reason.CUT_NOT_FENCED


@pytest.mark.whatif("P07-G10-03")
def test_a_cut_from_another_generation_cannot_be_scanned(published: dict[str, Any]) -> None:
    kafka = published["kafka"]
    cut, _ = capture_cut(kafka, producers=[published["producer"]])
    kafka.new_generation()
    with pytest.raises(CutUnscannable) as exc:
        scan_cut(kafka, cut)
    assert exc.value.reason == Reason.CUT_GENERATION_MIXED


def test_merging_cuts_across_generations_is_refused(published: dict[str, Any]) -> None:
    cut, _ = capture_cut(published["kafka"], producers=[published["producer"]])
    other = replace(cut, topic_generation="gen-99")
    with pytest.raises(ValueError, match=Reason.CUT_GENERATION_MIXED):
        cut.merge(other)
    with pytest.raises(ValueError, match=Reason.CUT_CLUSTER_MISMATCH):
        cut.merge(replace(cut, cluster_id="other"))
    merged = cut.merge(cut)
    assert merged.ranges == cut.ranges


@pytest.mark.whatif("P07-G10-01")
def test_scanning_an_expired_range_is_inconclusive_not_empty(published: dict[str, Any]) -> None:
    kafka = published["kafka"]
    cut, _ = capture_cut(kafka, producers=[published["producer"]])
    busiest = max(
        range(kafka.partition_count(TICK_TOPIC)),
        key=lambda index: kafka.high_watermark(TICK_TOPIC, index),
    )
    kafka.expire_to(TICK_TOPIC, busiest, 2)
    with pytest.raises(CutUnscannable) as exc:
        scan_cut(kafka, cut)
    assert exc.value.reason == Reason.CUT_RANGE_EXPIRED


def test_the_scan_reconciles_exactly_with_the_expected_set(
    published: dict[str, Any], expectation: Any
) -> None:
    kafka = published["kafka"]
    cut, _ = capture_cut(kafka, producers=[published["producer"]])
    inventory = scan_cut(kafka, cut, expect_dataset_id=expectation.dataset_id)
    assert inventory.findings == ()
    result = reconcile_kafka_cut(expectation, inventory)
    assert result.diff.equal
    assert result.leaked_suppressed == ()
    assert result.leaked_rejected == ()
    assert result.physical_duplicate_rows == 0


@pytest.mark.whatif("P07-G08-05", "P07-G24-04")
def test_a_tick_only_view_shows_false_gaps_that_the_full_cut_does_not(
    published: dict[str, Any],
) -> None:
    """Case ``E2E-24``: the even numbers are quotes, not losses.

    This is why the scanner exposes both views: a gap conclusion drawn from one
    topic is wrong, and the test asserts the two views really do disagree.
    """
    kafka = published["kafka"]
    cut, _ = capture_cut(kafka, producers=[published["producer"]])
    inventory = scan_cut(kafka, cut)
    full = per_symbol_sequences(inventory)
    ticks_only = tick_only_sequences(inventory)
    symbol = next(iter(full))
    assert len(full[symbol]) > len(ticks_only[symbol])
    assert list(full[symbol]) == sorted(full[symbol])
    # The full view is contiguous for this clean fixture; the tick view is not.
    assert full[symbol][-1] - full[symbol][0] + 1 == len(full[symbol])
    assert ticks_only[symbol][-1] - ticks_only[symbol][0] + 1 > len(ticks_only[symbol])


@pytest.mark.whatif("P07-G03-12")
def test_the_scanner_flags_an_mdp1_prefixed_value(published: dict[str, Any]) -> None:
    kafka = published["kafka"]
    kafka.produce(
        TICK_TOPIC, 0, key=b"EURUSD", value=frame_mdp1(DEFAULT_CODEC.encode(core(1)))
    )
    cut, _ = capture_cut(kafka, producers=[published["producer"]])
    inventory = scan_cut(kafka, cut)
    assert any(
        finding.reason == Reason.KAFKA_VALUE_HAS_MDP1_PREFIX for finding in inventory.findings
    )


def test_the_scanner_flags_a_record_in_the_wrong_topic(published: dict[str, Any]) -> None:
    kafka = published["kafka"]
    kafka.produce(QUOTE_TOPIC, 0, key=b"EURUSD", value=DEFAULT_CODEC.encode(core(1)))
    cut, _ = capture_cut(kafka, producers=[published["producer"]])
    inventory = scan_cut(kafka, cut)
    assert any(
        finding.reason == Reason.KAFKA_WRONG_TOPIC_FOR_KIND for finding in inventory.findings
    )


def test_an_unresolved_producer_ack_becomes_a_finding_only_if_the_scan_missed_it() -> None:
    cut = KafkaCut(
        cluster_id="c",
        topic_generation="gen-1",
        ranges={(TICK_TOPIC, 0): (0, 1)},
        fenced=True,
        captured_at_ns=1,
        producer_unknown_identities=(core(1).identity,),
    )
    assert cut_findings(cut, resolved_identities=[core(1).identity]) == []
    findings = cut_findings(cut, resolved_identities=[])
    assert findings[0].reason == Reason.PRODUCER_ACK_UNKNOWN


def test_fencing_reports_unknowns_separately_from_the_queue_state(
    published: dict[str, Any],
) -> None:
    result = fence_producers([published["producer"]])
    assert result.drained is True
    assert "not 'not published'" in result.as_dict()["note"]


# -- raw, canonical and bars ----------------------------------------------


def test_raw_reconciliation_accepts_physical_duplicates(expectation: Any) -> None:
    rows = []
    offset = 0
    for body in expectation.accepted.values():
        rows.append(raw_row(body, offset=offset))
        offset += 1
    rows.append(raw_row(next(iter(expectation.accepted.values())), offset=offset))
    result = reconcile_raw(expectation, rows)
    assert result.diff.equal
    assert result.physical_duplicate_rows == 1
    assert result.findings == ()


def test_raw_reconciliation_flags_a_stored_digest_that_disagrees(expectation: Any) -> None:
    rows = [raw_row(body, offset=index) for index, body in enumerate(expectation.accepted.values())]
    rows[0] = replace(rows[0], payload_digest="0" * 64)
    result = reconcile_raw(expectation, rows)
    assert result.digest_mismatches
    assert any(finding.reason == Reason.SEMANTIC_MISMATCH for finding in result.findings)


@pytest.mark.whatif("P07-G20-02")
def test_rows_that_vanished_after_insertion_are_inconclusive_not_missing(
    expectation: Any,
) -> None:
    """A TTL deletion and an ingestion gap are different incidents.

    Reporting a deletion as missing data sends the operator to the wrong
    runbook, so the reconciler uses the ingestor's own insert count to tell them
    apart.
    """
    rows = [raw_row(body, offset=index) for index, body in enumerate(expectation.accepted.values())]
    survivors = rows[:5]
    result = reconcile_raw(expectation, survivors, rows_inserted=len(rows))
    reasons = {finding.reason for finding in result.findings}
    assert Reason.RAW_ROWS_EXPIRED in reasons
    assert Reason.MISSING_IDENTITIES not in reasons

    # Without the insert count, the same observation is reported as loss.
    plain = {finding.reason for finding in reconcile_raw(expectation, survivors).findings}
    assert Reason.MISSING_IDENTITIES in plain


def test_raw_rows_from_another_generation_are_unverified(expectation: Any, published: dict[str, Any]) -> None:
    kafka = published["kafka"]
    cut, _ = capture_cut(kafka, producers=[published["producer"]])
    inventory = scan_cut(kafka, cut)
    body = next(iter(expectation.accepted.values()))
    result = reconcile_raw(expectation, [raw_row(body, offset=0, generation="gen-99")], inventory=inventory)
    assert result.wrong_generation_rows == 1
    assert any(
        entry["reason"] == Reason.CUT_GENERATION_MIXED for entry in result.unverified_provenance
    )


def test_canonical_reconciliation_requires_one_row_per_identity(expectation: Any) -> None:
    from p07.dataplane.clickhouse_fake import CanonicalRow

    rows = [
        CanonicalRow(build_id="b1", core=body, payload_digest=digest_core(body))
        for body in expectation.accepted.values()
    ]
    clean = reconcile_canonical(expectation, "b1", rows)
    assert clean.verified and clean.diff.equal

    doubled = reconcile_canonical(expectation, "b1", rows + [rows[0]])
    assert not doubled.verified
    assert any(finding.reason == Reason.CANONICAL_NOT_UNIQUE for finding in doubled.findings)


def test_canonical_rows_from_another_build_are_a_finding(expectation: Any) -> None:
    from p07.dataplane.clickhouse_fake import CanonicalRow

    rows = [
        CanonicalRow(build_id="other", core=body, payload_digest=digest_core(body))
        for body in expectation.accepted.values()
    ]
    result = reconcile_canonical(expectation, "b1", rows)
    assert any(finding.reason == Reason.BUILD_DIRTY for finding in result.findings)


def test_bar_reconciliation_compares_against_the_expected_ticks(expectation: Any) -> None:
    from p07.dataplane.clickhouse_fake import BarRow
    from p07.oracle.reference_bars import reference_bars

    expected_bars = reference_bars(expectation.accepted.values())
    rows = [
        BarRow(
            build_id="b1",
            dataset_id=key.dataset_id,
            symbol=key.symbol,
            price_scale=key.price_scale,
            bar_start_ns=key.bar_start_ns,
            open_ticks=bar.open_ticks,
            high_ticks=bar.high_ticks,
            low_ticks=bar.low_ticks,
            close_ticks=bar.close_ticks,
            tick_count=bar.tick_count,
        )
        for key, bar in expected_bars.items()
    ]
    clean = reconcile_bars(expectation, "b1", rows)
    assert clean.verified
    assert clean.expected_tick_total == clean.observed_tick_total

    inflated = [replace(rows[0], tick_count=rows[0].tick_count + 1)] + rows[1:]
    bad = reconcile_bars(expectation, "b1", inflated)
    assert not bad.verified
    assert any(finding.reason == Reason.BARS_MISMATCH for finding in bad.findings)
    assert bad.observed_tick_total == bad.expected_tick_total + 1


# -- the frontier report --------------------------------------------------


def test_frontier_findings_catch_a_commit_past_completion() -> None:
    """``INV-03`` checked against the consumer's own report."""
    frontier = {
        "partitions": {
            "md.ticks.v1/0": {"committed_next": 10, "completed_contiguous_next": 5, "unknown_count": 0}
        }
    }
    findings = frontier_findings(frontier)
    assert findings[0].reason == Reason.COMMIT_AHEAD_OF_COMPLETION


def test_frontier_findings_catch_a_short_drain_and_unknowns() -> None:
    frontier = {
        "partitions": {
            "md.ticks.v1/0": {
                "committed_next": 3,
                "completed_contiguous_next": 3,
                "unknown_count": 2,
                "unknown_offsets": [3, 4],
            }
        }
    }
    reasons = {
        finding.reason for finding in frontier_findings(frontier, cut_end={"md.ticks.v1/0": 5})
    }
    assert reasons == {Reason.MISSING_IDENTITIES, Reason.SINK_ACK_UNKNOWN}


def test_a_complete_frontier_produces_no_findings() -> None:
    frontier = {
        "partitions": {
            "md.ticks.v1/0": {
                "committed_next": 5,
                "completed_contiguous_next": 5,
                "unknown_count": 0,
            }
        }
    }
    assert frontier_findings(frontier, cut_end={"md.ticks.v1/0": 5}) == []
