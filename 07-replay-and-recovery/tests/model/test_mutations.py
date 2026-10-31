"""Mutation tests: break the system on purpose and require the harness to notice.

Design 12.2 asks for exactly this and says what to break::

    cố tình cho code commit max-offset bỏ qua một hole, bỏ topic_generation,
    bỏ optional-presence trong digest, bỏ bar verification.
    Test suite phải FAIL với mutants đó -- nếu vẫn PASS, verifier chưa đủ mạnh.

Every test here introduces one such mutant and asserts the corresponding check
*fires*. A test that passed because the checker is weak would pass here too, so
these are the tests that give the other tests their value.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace
from typing import Any

import pytest

from p07.contracts import TICK_TOPIC, EventKind, SemanticCore
from p07.dataplane.clickhouse_fake import BarRow, CanonicalRow, FakeClickHouse, RawRow
from p07.dataplane.codec import DEFAULT_CODEC
from p07.dataplane.ingestion import BatchIngestor
from p07.dataplane.kafka_fake import FakeKafka
from p07.evidence.verdicts import Reason, Verdict, classify
from p07.oracle.reference_bars import reference_bars
from p07.oracle.semantic_digest import digest_core
from p07.reconcile.bar_reconciler import reconcile_bars
from p07.reconcile.canonical_reconciler import reconcile_canonical
from p07.reconcile.raw_reconciler import frontier_findings, reconcile_raw


def tick(sequence: int, *, price: int = 108000) -> SemanticCore:
    return SemanticCore(
        dataset_id="ds",
        symbol="EURUSD",
        sequence=sequence,
        event_time_ns=1_000_000 * sequence,
        price_scale=100000,
        event_kind=EventKind.TICK,
        source_schema_version="sim.v1",
        bid_ticks=None,
        ask_ticks=None,
        price_ticks=price,
    )


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


def reasons(findings: Any) -> set[str]:
    return {finding.reason for finding in findings}


# -- mutant 1: commit the maximum offset, skipping a hole -----------------


class MaxOffsetIngestor(BatchIngestor):
    """A worker that commits the highest completed offset, holes and all.

    This is the single most dangerous plausible bug in P05: it looks right, it
    keeps the lag at zero, and it silently loses whatever sat in the hole.
    """

    def _commit_frontier(self, tp: tuple[str, int], progress: Any) -> None:  # type: ignore[override]
        if not progress.completed:
            return
        target = max(progress.completed) + 1
        topic, partition = tp
        self._kafka.commit(self._group, topic, partition, target, epoch=self.epoch)
        progress.committed_next = target
        self.counters.commits += 1


@pytest.mark.whatif("P07-G14-01")
def test_the_frontier_check_catches_a_max_offset_commit() -> None:
    """``INV-03`` must fail for the mutant, not merely hold for the real one."""
    kafka = FakeKafka(partitions=1, topics=(TICK_TOPIC,))
    sink = FakeClickHouse()
    for sequence in range(1, 4):
        kafka.produce(
            TICK_TOPIC, 0, key=b"EURUSD", value=DEFAULT_CODEC.encode(tick(sequence))
        )

    worker = MaxOffsetIngestor(
        kafka, sink, assignment=[(TICK_TOPIC, 0)], batch_max_records=1, max_insert_retries=1
    )
    worker.resume_from_broker(cut_start={(TICK_TOPIC, 0): 0})
    # Offset 0's insert is left unresolved, so it never completes...
    sink.ack_loss_once = True
    with pytest.raises(Exception):
        worker.poll_once(up_to={(TICK_TOPIC, 0): 1})
    # ...but offsets 1 and 2 do, and the mutant commits past the hole.
    worker.poll_once(up_to={(TICK_TOPIC, 0): 3})

    progress = worker.progress[(TICK_TOPIC, 0)]
    assert progress.committed_next is not None
    assert progress.committed_next > progress.completed_contiguous_next()
    assert Reason.COMMIT_AHEAD_OF_COMPLETION in reasons(frontier_findings(worker.frontier()))


def test_the_real_ingestor_does_not_trip_that_check() -> None:
    """The control for the test above: the unmutated worker stays behind the hole."""
    kafka = FakeKafka(partitions=1, topics=(TICK_TOPIC,))
    sink = FakeClickHouse()
    for sequence in range(1, 4):
        kafka.produce(TICK_TOPIC, 0, key=b"EURUSD", value=DEFAULT_CODEC.encode(tick(sequence)))
    worker = BatchIngestor(
        kafka, sink, assignment=[(TICK_TOPIC, 0)], batch_max_records=1, max_insert_retries=1
    )
    worker.resume_from_broker(cut_start={(TICK_TOPIC, 0): 0})
    sink.ack_loss_once = True
    with pytest.raises(Exception):
        worker.poll_once(up_to={(TICK_TOPIC, 0): 1})
    worker.poll_once(up_to={(TICK_TOPIC, 0): 3})
    assert frontier_findings(worker.frontier()) != []  # it reports the unknown
    assert Reason.COMMIT_AHEAD_OF_COMPLETION not in reasons(
        frontier_findings(worker.frontier())
    )


# -- mutant 2: drop topic_generation from provenance ----------------------


@pytest.mark.whatif("P07-G10-03")
def test_the_provenance_check_catches_a_dropped_generation(expectation: Any) -> None:
    """Rows carrying a foreign generation must not be accepted as provenance."""
    from p07.recovery.cut_coordinator import capture_cut
    from p07.replay.kafka_cut_scanner import scan_cut

    kafka = FakeKafka(partitions=1, topics=(TICK_TOPIC,))
    body = next(iter(expectation.accepted.values()))
    kafka.produce(TICK_TOPIC, 0, key=body.symbol.encode(), value=DEFAULT_CODEC.encode(body))
    cut, _ = capture_cut(kafka, producers=[])
    inventory = scan_cut(kafka, cut)

    mutant_rows = [raw_row(body, offset=0, generation="some-other-generation")]
    result = reconcile_raw(expectation, mutant_rows, inventory=inventory)
    assert result.wrong_generation_rows == 1
    assert Reason.EXTRA_IDENTITIES in reasons(result.findings)


# -- mutant 3: a digest that ignores optional presence --------------------


def presence_blind_digest(core: SemanticCore) -> str:
    """A digest that treats an absent field as a zero -- the classic shortcut."""
    blob = "|".join(
        str(value if value is not None else 0)
        for value in (
            core.dataset_id,
            core.symbol,
            core.sequence,
            core.event_time_ns,
            core.price_scale,
            int(core.event_kind),
            core.source_schema_version,
            core.bid_ticks,
            core.ask_ticks,
            core.price_ticks,
        )
    )
    return hashlib.sha256(blob.encode()).hexdigest()


@pytest.mark.whatif("P07-G03-01", "P07-G03-10")
def test_the_real_digest_separates_what_the_presence_blind_mutant_merges() -> None:
    """``INV-01``: presence is semantics, so a zero is not an absence."""
    present = tick(1, price=0)
    absent = present._replace(price_ticks=None)
    assert presence_blind_digest(present) == presence_blind_digest(absent)  # the mutant merges
    assert digest_core(present) != digest_core(absent)  # the real one does not


@pytest.mark.whatif("P07-G03-10")
def test_the_canonical_check_catches_a_presence_blind_row(expectation: Any) -> None:
    """A row that lost a field's presence must be reported, not accepted."""
    body = next(
        core for core in expectation.accepted.values() if core.event_kind is EventKind.QUOTE
    )
    mutant = body._replace(ask_ticks=None)
    rows = [
        CanonicalRow(build_id="b1", core=core, payload_digest=digest_core(core))
        for core in expectation.accepted.values()
        if core.identity != body.identity
    ]
    rows.append(CanonicalRow(build_id="b1", core=mutant, payload_digest=digest_core(mutant)))
    result = reconcile_canonical(expectation, "b1", rows)
    assert not result.verified
    assert Reason.IDENTITY_CONFLICT in reasons(result.findings)
    conflict_fields = dict(result.diff.mismatched)[body.identity]
    assert "ask_ticks" in conflict_fields


# -- mutant 4: skip bar verification --------------------------------------


@pytest.mark.whatif("P07-G18-01")
def test_the_bar_check_catches_an_aggregation_that_counts_duplicates(
    expectation: Any,
) -> None:
    """A builder that aggregates raw rows instead of canonical ones.

    The mutant counts a physical retry twice, which is precisely the inflation
    ``INV-09`` forbids, and the bar reconciler must see it.
    """
    expected_bars = reference_bars(expectation.accepted.values())
    assert expected_bars, "the fixture must contain ticks"

    inflated = []
    for key, bar in expected_bars.items():
        inflated.append(
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
                tick_count=bar.tick_count + 1,  # one duplicate counted twice
            )
        )
    result = reconcile_bars(expectation, "b1", inflated)
    assert not result.verified
    assert Reason.BARS_MISMATCH in reasons(result.findings)
    assert result.observed_tick_total > result.expected_tick_total


@pytest.mark.whatif("P07-G18-03")
def test_the_bar_check_catches_a_wrong_tie_break(expectation: Any) -> None:
    """Swapping open and close is invisible to a count-only check."""
    expected_bars = reference_bars(expectation.accepted.values())
    swapped = []
    for key, bar in expected_bars.items():
        swapped.append(
            BarRow(
                build_id="b1",
                dataset_id=key.dataset_id,
                symbol=key.symbol,
                price_scale=key.price_scale,
                bar_start_ns=key.bar_start_ns,
                open_ticks=bar.close_ticks,
                high_ticks=bar.high_ticks,
                low_ticks=bar.low_ticks,
                close_ticks=bar.open_ticks,
                tick_count=bar.tick_count,
            )
        )
    result = reconcile_bars(expectation, "b1", swapped)
    assert result.observed_tick_total == result.expected_tick_total  # counts agree
    assert not result.verified  # and the prices do not


# -- mutant 5: a gate that treats "unknown" as "fine" ---------------------


@pytest.mark.whatif("P07-G29-04")
def test_a_gate_that_ignored_unknowns_would_pass_where_the_real_one_does_not() -> None:
    """``INV-07``: missing proof cannot produce a pass.

    The mutant filters out the inconclusive findings before classifying, which
    is the shape of every "the services came back, so we are fine" bug.
    """
    from p07.evidence.verdicts import Finding

    findings = [Finding(Reason.SINK_ACK_UNKNOWN), Finding(Reason.PRODUCER_ACK_UNKNOWN)]
    mutant_verdict = classify(
        [finding for finding in findings if finding.reason not in {Reason.SINK_ACK_UNKNOWN, Reason.PRODUCER_ACK_UNKNOWN}]
    ).verdict
    assert mutant_verdict is Verdict.PASS
    assert classify(findings).verdict is Verdict.INCONCLUSIVE


@pytest.mark.whatif("P07-G27-01")
def test_a_gate_that_ignored_the_fault_receipt_would_pass_an_invalid_run() -> None:
    """``INV-06``, as a mutation: the receipt is what makes a chaos test real."""
    assert classify([], fault_confirmed=True).verdict is Verdict.PASS  # the mutant
    assert classify([], fault_confirmed=False).verdict is Verdict.INVALID  # the real gate
