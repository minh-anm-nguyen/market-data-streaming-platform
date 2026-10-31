"""FIX-001 executed, plus the oracle properties it is designed to prove.

Section 10, FIX-001: ten sealed logical events over three symbols and two
topics, with one intentional physical duplicate of the fourth logical id.
Expected ``source_unique=10``, ``kafka_physical=11`` when the duplicate reaches
the broker, ``canonical_unique=10``, ``conflicts=0``.

The fixture is explicit that ``raw_physical`` must **not** be hardcoded to 11,
because an insert-side dedupe may collapse the physical copy. That refusal is
itself tested here: :meth:`TestFix001Expectations.test_raw_physical_is_not_pinned`.
"""

from __future__ import annotations

import pytest

from p08_bench.clock import ClockDomain, FakeClock
from p08_bench.event_driver import EventDriver
from p08_bench.goldens import (
    FIX001_DATASET_ID,
    FIX001_DUPLICATE_INDEX,
    FIX001_EVENTS,
    FIX001_EXPECTED_CANONICAL_UNIQUE,
    FIX001_EXPECTED_CONFLICTS,
    FIX001_EXPECTED_KAFKA_PHYSICAL_IF_DUPLICATE_LANDS,
    FIX001_EXPECTED_RAW_PHYSICAL,
    FIX001_EXPECTED_SOURCE_UNIQUE,
    FIX001_EXPECTED_UNEXPECTED_EXTRA,
)
from p08_bench.reconciliation import (
    ExpectedSet,
    Reconciliation,
    accept_all,
    reconcile_sets,
)
from p08_bench.scheduler import RateSchedule, Slot, SlotKind
from p08_bench.sut import Identity
from p08_bench.sut_mock import MockConfig, MockPipeline

MS = 1_000_000


def fixture_events() -> list[dict]:
    """The ten sealed events, each carrying its dataset id."""
    return [{**event, "dataset_id": FIX001_DATASET_ID} for event in FIX001_EVENTS]


def key_of(event: dict) -> tuple[str, str, int]:
    return (event["dataset_id"], event["symbol"], int(event["sequence"]))


def run_fixture(*, duplicate: bool = True) -> MockPipeline:
    """Drive the ten fixture events through the simulated pipeline.

    The duplicate is submitted as a second physical copy of the same identity,
    which is what at-least-once delivery produces and what INV-06 says must not
    change the canonical set.
    """
    events = fixture_events()
    pipeline = MockPipeline(MockConfig())
    clock = FakeClock(ClockDomain("bench", "driver"))
    for index, event in enumerate(events):
        identity = Identity(**{k: event[k] for k in ("dataset_id", "symbol")}, sequence=event["sequence"])
        payload = {**event, "kind": event["kind"]}
        pipeline.submit(identity, payload, clock.now_ns())
        clock.advance_ns(MS)
        if duplicate and index == FIX001_DUPLICATE_INDEX:
            pipeline.submit(identity, payload, clock.now_ns())
            clock.advance_ns(MS)
    pipeline.advance_to(clock.now_ns() + 1000 * MS)
    return pipeline


class TestFix001Expectations:
    def test_the_fixture_has_ten_logical_events(self):
        assert len(FIX001_EVENTS) == FIX001_EXPECTED_SOURCE_UNIQUE == 10

    def test_the_fixture_spans_three_symbols_and_two_kinds(self):
        assert len({e["symbol"] for e in FIX001_EVENTS}) == 3
        assert {e["kind"] for e in FIX001_EVENTS} == {"tick", "quote"}

    def test_identities_are_unique_per_symbol_sequence(self):
        keys = [key_of(e) for e in fixture_events()]
        assert len(set(keys)) == 10

    def test_sequences_are_per_symbol_across_both_kinds(self):
        # The P01 contract section 0.1 requires P08 to preserve: a symbol has
        # one sequence space shared by ticks and quotes.
        by_symbol: dict[str, list[int]] = {}
        for event in FIX001_EVENTS:
            by_symbol.setdefault(event["symbol"], []).append(event["sequence"])
        for symbol, sequences in by_symbol.items():
            assert sequences == list(range(1, len(sequences) + 1)), symbol

    def test_source_unique_is_ten(self):
        assert len(run_fixture().source_identities()) == 10

    def test_kafka_physical_is_eleven_when_the_duplicate_lands(self):
        pipeline = run_fixture(duplicate=True)
        assert pipeline.kafka_physical == FIX001_EXPECTED_KAFKA_PHYSICAL_IF_DUPLICATE_LANDS == 11

    def test_canonical_unique_stays_ten_despite_the_duplicate(self):
        # INV-06: physical duplicates may exist and must not change the
        # canonical unique set.
        pipeline = run_fixture(duplicate=True)
        assert len(pipeline.canonical_identities()) == FIX001_EXPECTED_CANONICAL_UNIQUE == 10

    def test_raw_physical_is_not_pinned(self):
        # FIX-001 says explicitly: do not hardcode raw=11, because insert dedupe
        # may drop the physical copy. The fixture therefore declines to assert it.
        assert FIX001_EXPECTED_RAW_PHYSICAL is None

    def test_raw_physical_is_at_least_the_logical_count(self):
        # The one bound the fixture does state for the lab case.
        pipeline = run_fixture(duplicate=True)
        assert pipeline.raw_physical >= 10


class TestFix001Reconciliation:
    def _expected(self):
        oracle = ExpectedSet(FIX001_DATASET_ID, sealed=True, accept=accept_all)
        return oracle.build(fixture_events())

    def test_a_clean_run_reconciles(self):
        identities, payloads = self._expected()
        pipeline = run_fixture(duplicate=True)
        result = reconcile_sets(
            identities,
            pipeline.canonical_identities(),
            expected_payload=payloads.get,
            canonical_payload=pipeline.canonical_payload,
            physical_duplicate_count=pipeline.kafka_physical - len(identities),
        )
        assert result.verdict is Reconciliation.PASS
        assert result.missing_count == 0
        assert result.unexpected_extra_count == FIX001_EXPECTED_UNEXPECTED_EXTRA == 0
        assert result.conflict_identity_count == FIX001_EXPECTED_CONFLICTS == 0

    def test_the_physical_duplicate_is_reported_separately(self):
        identities, payloads = self._expected()
        pipeline = run_fixture(duplicate=True)
        result = reconcile_sets(
            identities,
            pipeline.canonical_identities(),
            expected_payload=payloads.get,
            canonical_payload=pipeline.canonical_payload,
            physical_duplicate_count=1,
        )
        assert result.summary()["physical_duplicate_count"] == 1
        assert result.clean

    def test_a_lost_event_is_reported_as_missing(self):
        identities, payloads = self._expected()
        pipeline = MockPipeline(MockConfig(loss_fraction=1.0))
        clock = FakeClock(ClockDomain("bench", "driver"))
        for event in fixture_events():
            pipeline.submit(
                Identity(event["dataset_id"], event["symbol"], event["sequence"]),
                event, clock.now_ns(),
            )
            clock.advance_ns(MS)
        result = reconcile_sets(
            identities, pipeline.canonical_identities(),
            expected_payload=payloads.get, canonical_payload=pipeline.canonical_payload,
        )
        assert result.verdict is Reconciliation.FAIL
        assert result.missing_count == 10

    def test_an_injected_extra_identity_is_caught(self):
        # INV-05: a count-only check cannot see an extra that balances a missing.
        identities, payloads = self._expected()
        extra = (FIX001_DATASET_ID, "EURUSD", 9999)
        pipeline = MockPipeline(MockConfig(inject_extra_identities=(extra,)))
        clock = FakeClock(ClockDomain("bench", "driver"))
        for event in fixture_events():
            pipeline.submit(
                Identity(event["dataset_id"], event["symbol"], event["sequence"]),
                event, clock.now_ns(),
            )
            clock.advance_ns(MS)
        pipeline.advance_to(clock.now_ns() + 1000 * MS)
        result = reconcile_sets(
            identities, pipeline.canonical_identities(),
            expected_payload=payloads.get, canonical_payload=pipeline.canonical_payload,
        )
        assert result.unexpected_extra_count == 1
        assert result.verdict is Reconciliation.FAIL

    def test_a_conflicting_payload_is_caught_even_with_matching_counts(self):
        # The counts agree perfectly and the data is still wrong, which is why
        # section 4.1 refuses COUNT(DISTINCT) as proof.
        identities, payloads = self._expected()
        conflict = key_of(fixture_events()[0])
        pipeline = MockPipeline(MockConfig(conflict_identities=(conflict,)))
        clock = FakeClock(ClockDomain("bench", "driver"))
        for event in fixture_events():
            pipeline.submit(
                Identity(event["dataset_id"], event["symbol"], event["sequence"]),
                event, clock.now_ns(),
            )
            clock.advance_ns(MS)
        pipeline.advance_to(clock.now_ns() + 1000 * MS)
        result = reconcile_sets(
            identities, pipeline.canonical_identities(),
            expected_payload=payloads.get, canonical_payload=pipeline.canonical_payload,
        )
        assert result.expected_count == result.canonical_count == 10
        assert result.missing_count == 0
        assert result.conflict_identity_count == 1
        assert result.verdict is Reconciliation.FAIL

    def test_an_unsealed_source_cannot_produce_an_expected_set(self):
        # Case G01-03 at the oracle boundary.
        oracle = ExpectedSet(FIX001_DATASET_ID, sealed=False, accept=accept_all)
        with pytest.raises(Exception, match="unsealed source"):
            oracle.build(fixture_events())

    def test_an_acceptance_policy_shrinks_the_expected_set_not_the_denominator(self):
        # Section 0.1: an intentional rejection is not loss, so the expected set
        # excludes it rather than the comparison ignoring it.
        oracle = ExpectedSet(
            FIX001_DATASET_ID, sealed=True, accept=lambda e: e["symbol"] != "USDJPY"
        )
        identities, _ = oracle.build(fixture_events())
        assert len(identities) == 6
        assert all(key[1] != "USDJPY" for key in identities)
