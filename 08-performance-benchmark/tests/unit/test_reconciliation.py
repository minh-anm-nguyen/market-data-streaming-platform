"""Identity and semantic reconciliation (section 9.2; INV-02..06).

The streaming path is the one that matters for scale: section 9.2 forbids
loading 200M identities into RAM, so the merge is O(1) in identities and raises
rather than guessing when its inputs are not sorted.
"""

from __future__ import annotations

import pytest

from p08_bench.errors import MetrologyError
from p08_bench.reconciliation import (
    DEFAULT_SEMANTIC_FIELDS,
    ExpectedSet,
    MismatchKind,
    Reconciliation,
    accept_all,
    compare_payloads,
    reconcile_sets,
    reconcile_streams,
)

KEY = ("ds", "EURUSD", 1)


def payloads(**overrides):
    base = {"event_time_ns": 1_000, "price_ticks": 108347, "price_scale": 100000, "kind": "tick"}
    base.update(overrides)
    return base


class TestPayloadComparison:
    def test_identical_payloads_have_no_mismatches(self):
        assert compare_payloads(KEY, payloads(), payloads()) == []

    def test_a_changed_price_is_a_value_conflict(self):
        mismatches = compare_payloads(KEY, payloads(), payloads(price_ticks=108348))
        assert mismatches[0].kind is MismatchKind.VALUE_CONFLICT
        assert mismatches[0].field_name == "price_ticks"

    def test_a_missing_field_is_not_a_value_conflict(self):
        # P02 oneof presence: a field that vanished is a different defect from
        # one whose value changed.
        actual = payloads()
        del actual["price_ticks"]
        mismatches = compare_payloads(KEY, payloads(), actual)
        assert mismatches[0].kind is MismatchKind.FIELD_ABSENT

    def test_an_unexpected_field_is_reported_as_such(self):
        mismatches = compare_payloads(KEY, {"kind": "tick"}, {"kind": "tick", "price_ticks": 1})
        assert mismatches[0].kind is MismatchKind.FIELD_UNEXPECTED

    def test_fields_absent_from_both_sides_are_not_mismatches(self):
        assert compare_payloads(KEY, {"kind": "tick"}, {"kind": "tick"}) == []

    def test_uncompared_fields_are_ignored(self):
        # An added diagnostic field must not look like a data defect.
        left = payloads()
        right = {**payloads(), "ingest_host": "box-2"}
        assert compare_payloads(KEY, left, right) == []

    def test_a_float_price_is_refused(self):
        # INV-03: a value that arrived as a float has already lost exactness.
        with pytest.raises(MetrologyError, match="integer fixed-point"):
            compare_payloads(KEY, payloads(price_ticks=108347.0), payloads())

    def test_a_float_on_the_actual_side_is_also_refused(self):
        with pytest.raises(MetrologyError, match="INV-03"):
            compare_payloads(KEY, payloads(), payloads(event_time_ns=1000.0))

    def test_the_default_field_set_covers_prices_times_and_kind(self):
        for name in ("event_time_ns", "price_ticks", "bid_ticks", "ask_ticks", "kind"):
            assert name in DEFAULT_SEMANTIC_FIELDS


class TestSetReconciliation:
    def test_matching_sets_with_payloads_pass(self):
        result = reconcile_sets(
            [KEY], [KEY], expected_payload=lambda k: payloads(), canonical_payload=lambda k: payloads()
        )
        assert result.verdict is Reconciliation.PASS
        assert result.payloads_compared == 1

    def test_identity_only_comparison_cannot_pass(self):
        # Section 9.2 compares identity *and* semantics; without payload access
        # the semantic half is unknown, and unknown is not a pass.
        result = reconcile_sets([KEY], [KEY])
        assert result.verdict is Reconciliation.INCONCLUSIVE
        assert any("payload access" in b for b in result.blockers)

    def test_a_missing_identity_fails(self):
        result = reconcile_sets(
            [KEY], [], expected_payload=lambda k: payloads(), canonical_payload=lambda k: payloads()
        )
        assert result.missing_count == 1
        assert result.verdict is Reconciliation.FAIL

    def test_an_extra_identity_fails(self):
        result = reconcile_sets(
            [], [KEY], expected_payload=lambda k: payloads(), canonical_payload=lambda k: payloads()
        )
        assert result.unexpected_extra_count == 1
        assert result.verdict is Reconciliation.FAIL

    def test_a_balanced_missing_and_extra_still_fails(self):
        # Counts match exactly and the data is wrong: the reason section 4.1
        # refuses COUNT(DISTINCT) as proof.
        other = ("ds", "GBPUSD", 1)
        result = reconcile_sets(
            [KEY], [other],
            expected_payload=lambda k: payloads(), canonical_payload=lambda k: payloads(),
        )
        assert result.expected_count == result.canonical_count == 1
        assert (result.missing_count, result.unexpected_extra_count) == (1, 1)
        assert result.verdict is Reconciliation.FAIL

    def test_an_unavailable_payload_blocks_rather_than_passes(self):
        result = reconcile_sets(
            [KEY], [KEY], expected_payload=lambda k: payloads(), canonical_payload=lambda k: None
        )
        assert result.verdict is Reconciliation.INCONCLUSIVE
        assert any("payload unavailable" in b for b in result.blockers)

    def test_examples_are_capped_but_counts_are_exact(self):
        keys = [("ds", "EURUSD", i) for i in range(500)]
        result = reconcile_sets(
            keys, [], expected_payload=lambda k: payloads(), canonical_payload=lambda k: payloads(),
            example_limit=5,
        )
        assert result.missing_count == 500
        assert len(result.missing_examples) == 5

    def test_duplicate_inputs_collapse_in_the_set_path(self):
        # INV-06: a repeated physical record is one identity.
        result = reconcile_sets(
            [KEY, KEY], [KEY],
            expected_payload=lambda k: payloads(), canonical_payload=lambda k: payloads(),
        )
        assert result.expected_count == 1
        assert result.verdict is Reconciliation.PASS

    def test_the_method_is_recorded(self):
        result = reconcile_sets([KEY], [KEY])
        assert result.summary()["method"] == "in_memory_sets"


class TestStreamReconciliation:
    def test_matching_streams_find_no_identity_differences(self):
        keys = [("ds", "EURUSD", i) for i in range(1, 6)]
        result = reconcile_streams(iter(keys), iter(keys))
        assert result.missing_count == 0
        assert result.unexpected_extra_count == 0
        assert result.compared_identities == 5

    def test_a_streaming_comparison_cannot_pass_on_its_own(self):
        # It compares identities only, so semantics stay unverified.
        keys = [("ds", "EURUSD", 1)]
        result = reconcile_streams(iter(keys), iter(keys))
        assert result.verdict is Reconciliation.INCONCLUSIVE
        assert any("does not compare payloads" in b for b in result.blockers)

    def test_missing_and_extra_are_separated_in_one_pass(self):
        expected = [("ds", "A", 1), ("ds", "B", 1), ("ds", "C", 1)]
        canonical = [("ds", "B", 1), ("ds", "D", 1)]
        result = reconcile_streams(iter(expected), iter(canonical))
        assert result.missing_count == 2
        assert result.unexpected_extra_count == 1
        assert result.compared_identities == 1

    def test_counts_on_both_sides_are_tallied(self):
        expected = [("ds", "A", 1), ("ds", "B", 1)]
        canonical = [("ds", "B", 1)]
        result = reconcile_streams(iter(expected), iter(canonical))
        assert result.expected_count == 2
        assert result.canonical_count == 1

    def test_an_unsorted_expected_stream_raises(self):
        # An unsorted merge would report both sides as mutually missing, which
        # looks like total data loss.
        bad = [("ds", "B", 1), ("ds", "A", 1)]
        with pytest.raises(MetrologyError, match="not sorted ascending"):
            reconcile_streams(iter(bad), iter([]))

    def test_an_unsorted_canonical_stream_raises(self):
        bad = [("ds", "B", 1), ("ds", "A", 1)]
        with pytest.raises(MetrologyError, match="canonical stream is not sorted"):
            reconcile_streams(iter([]), iter(bad))

    def test_a_duplicated_stream_identity_raises(self):
        dup = [("ds", "A", 1), ("ds", "A", 1)]
        with pytest.raises(MetrologyError, match="duplicate identity"):
            reconcile_streams(iter(dup), iter([]))

    def test_memory_does_not_grow_with_the_identity_count(self):
        # The 200M path: a generator of a million identities must not be
        # materialised, and only the capped examples are retained.
        size = 1_000_000
        expected = (("ds", "EURUSD", i) for i in range(size))
        canonical = (("ds", "EURUSD", i) for i in range(size))
        result = reconcile_streams(expected, canonical, example_limit=3)
        assert result.compared_identities == size
        assert result.missing_examples == []

    def test_the_method_is_recorded(self):
        result = reconcile_streams(iter([]), iter([]))
        assert result.summary()["method"] == "streaming_merge"


class TestExpectedSet:
    def test_the_acceptance_predicate_is_injected_not_read_from_the_pipeline(self):
        # Case G01-12: an oracle that asked the pipeline what it should have
        # accepted would be circular.
        events = [
            {"dataset_id": "ds", "symbol": "EURUSD", "sequence": 1, "price_ticks": 1},
            {"dataset_id": "ds", "symbol": "EURUSD", "sequence": 2, "price_ticks": -1},
        ]
        oracle = ExpectedSet("ds", sealed=True, accept=lambda e: e["price_ticks"] > 0)
        identities, _ = oracle.build(events)
        assert identities == {("ds", "EURUSD", 1)}

    def test_accept_all_admits_every_sealed_event(self):
        events = [{"dataset_id": "ds", "symbol": "EURUSD", "sequence": i} for i in (1, 2)]
        identities, payloads_map = ExpectedSet("ds", True, accept_all).build(events)
        assert len(identities) == 2
        assert len(payloads_map) == 2

    def test_payloads_come_back_keyed_by_identity(self):
        events = [{"dataset_id": "ds", "symbol": "EURUSD", "sequence": 1, "price_ticks": 7}]
        _, payloads_map = ExpectedSet("ds", True, accept_all).build(events)
        assert payloads_map[("ds", "EURUSD", 1)]["price_ticks"] == 7

    def test_a_repeated_source_event_yields_one_expected_identity(self):
        events = [{"dataset_id": "ds", "symbol": "EURUSD", "sequence": 1}] * 3
        identities, _ = ExpectedSet("ds", True, accept_all).build(events)
        assert len(identities) == 1

    def test_an_unsealed_source_is_refused(self):
        with pytest.raises(MetrologyError, match="unsealed source"):
            ExpectedSet("ds", False, accept_all).build([])
