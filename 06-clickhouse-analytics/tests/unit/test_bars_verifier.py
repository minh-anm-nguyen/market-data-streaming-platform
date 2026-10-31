"""Bar verification: a wrong bar must be caught, not averaged away (group H, I)."""

from __future__ import annotations

import pytest

from _coverage import covers
from md_analytics.bars_verifier import compare_bars, verify_build_bars
from md_analytics.contracts import NS_PER_MINUTE
from md_analytics.errors import Verdict
from md_analytics.oracle import Bar, BarKey, reference_bars
from md_analytics.store.model import ModelStore
from tests.unit.test_oracle import BUILD, DATASET, NOON, tick


def key(minute: int = 0, **overrides) -> BarKey:
    base = dict(
        build_id=BUILD,
        dataset_id=DATASET,
        symbol="EURUSD",
        price_scale=100_000,
        bar_start_ns=NOON + minute * NS_PER_MINUTE,
    )
    base.update(overrides)
    return BarKey(**base)


def compare(reference, stored, *, canonical_rows=None, state_rows=1):
    rows = (
        canonical_rows
        if canonical_rows is not None
        else sum(b.tick_count for b in reference.values())
    )
    return compare_bars(
        reference, stored, build_id=BUILD, canonical_rows=rows, state_rows=state_rows
    )


class TestExactMatch:
    def test_identical_bar_sets_pass(self):
        bars = {key(): Bar(100, 200, 50, 150, 4)}
        report = compare(bars, dict(bars))
        assert report.verdict() is Verdict.PASS
        assert report.mismatch_count == 0
        assert "match the oracle exactly" in report.reasons[0]


class TestFieldMismatch:
    @covers("P06-H-01", "P06-I-05")
    @pytest.mark.parametrize(
        "field,wrong",
        [
            ("open_ticks", 101),
            ("high_ticks", 201),
            ("low_ticks", 49),
            ("close_ticks", 151),
            ("tick_count", 5),
        ],
    )
    def test_a_single_wrong_field_is_caught(self, field, wrong):
        good = Bar(100, 200, 50, 150, 4)
        bad = good._replace(**{field: wrong})
        report = compare({key(): good}, {key(): bad})
        assert report.verdict() is Verdict.FAIL
        assert [m.field_name for m in report.mismatches] == [field]
        assert report.mismatches[0].expected == getattr(good, field)
        assert report.mismatches[0].actual == wrong

    @covers("P06-I-08", "P06-Q-07")
    def test_a_wrong_tie_break_is_caught_even_with_the_right_count(self):
        # What case E2E-21 produces: argMin without the sequence key swaps OPEN
        # and CLOSE for two ticks sharing a timestamp. The count is unaffected.
        correct = Bar(open_ticks=100_200, high_ticks=100_200, low_ticks=99_900, close_ticks=99_900, tick_count=2)
        swapped = correct._replace(open_ticks=99_900, close_ticks=100_200)
        report = compare({key(): correct}, {key(): swapped})
        assert report.verdict() is Verdict.FAIL
        assert {m.field_name for m in report.mismatches} == {"open_ticks", "close_ticks"}
        assert report.stored_tick_total == report.reference_tick_total, (
            "the counts agree, so only a field-level comparison finds this"
        )


class TestMissingAndExtraBars:
    @covers("P06-G-05", "P06-K-06")
    def test_a_missing_bar_is_reported(self):
        reference = {key(0): Bar(100, 100, 100, 100, 1), key(1): Bar(200, 200, 200, 200, 1)}
        report = compare(reference, {key(0): reference[key(0)]})
        assert report.verdict() is Verdict.FAIL
        assert report.missing_bars == [tuple(key(1))]

    @covers("P06-H-09", "P06-I-07")
    def test_an_extra_bar_is_reported(self):
        reference = {key(0): Bar(100, 100, 100, 100, 1)}
        stored = {**reference, key(1): Bar(0, 0, 0, 0, 0)}
        report = compare(reference, stored)
        assert report.verdict() is Verdict.FAIL
        assert report.extra_bars == [tuple(key(1))]

    @covers("P06-H-07")
    def test_a_bar_under_the_wrong_dataset_is_extra_not_a_match(self):
        reference = {key(): Bar(100, 100, 100, 100, 1)}
        stored = {key(dataset_id="DS-OTHER"): Bar(100, 100, 100, 100, 1)}
        report = compare(reference, stored)
        assert len(report.missing_bars) == 1 and len(report.extra_bars) == 1


class TestTotals:
    @covers("P06-H-05", "P06-I-07")
    def test_a_doubled_count_fails_on_the_total_too(self):
        reference = {key(): Bar(100, 200, 50, 150, 4)}
        stored = {key(): Bar(100, 200, 50, 150, 8)}
        report = compare(reference, stored)
        assert report.verdict() is Verdict.FAIL
        assert report.stored_tick_total == 8 and report.reference_tick_total == 4
        assert any("tick totals differ" in r for r in report.reasons)

    @covers("P06-K-09")
    def test_bars_that_do_not_account_for_every_canonical_row_fail(self):
        bars = {key(): Bar(100, 200, 50, 150, 4)}
        report = compare(bars, dict(bars), canonical_rows=5)
        assert report.verdict() is Verdict.FAIL
        assert any("canonical rows but bars account for" in r for r in report.reasons)

    @covers("P06-I-01")
    def test_the_state_row_count_is_recorded_as_evidence(self):
        bars = {key(): Bar(100, 200, 50, 150, 4)}
        report = compare(bars, dict(bars), state_rows=4)
        assert report.counters()["bar_state_rows"] == 4
        assert "4 physical state rows merged at read time" in report.reasons[0]


class TestBoundsCheck:
    @covers("P06-H-12")
    def test_an_impossible_bar_is_flagged(self):
        broken = Bar(open_ticks=500, high_ticks=200, low_ticks=100, close_ticks=150, tick_count=2)
        report = compare({key(): broken}, {key(): broken})
        assert report.verdict() is Verdict.FAIL
        assert report.bounds_violations == [tuple(key())]

    def test_bounds_alone_do_not_prove_a_bar_right(self):
        # Both satisfy LOW <= OPEN/CLOSE <= HIGH; only one is correct.
        correct = Bar(100, 200, 50, 150, 4)
        plausible_but_wrong = Bar(150, 200, 50, 100, 4)
        report = compare({key(): correct}, {key(): plausible_but_wrong})
        assert report.bounds_violations == []
        assert report.verdict() is Verdict.FAIL, (
            "INV-07 is necessary and not sufficient: the field comparison is what "
            "catches this"
        )


class TestAgainstTheStore:
    @covers("P06-I-01", "P06-I-09")
    def test_a_real_build_verifies_before_and_after_a_merge(self):
        store = ModelStore()
        store.setup()
        rows = [tick(i + 1, i * 7_000_000_000, 100_000 + (i % 7) * 11) for i in range(30)]
        store.insert_canonical(rows, block_size=4)

        before = verify_build_bars(store, build_id=BUILD, canonical=rows)
        assert before.verdict() is Verdict.PASS
        assert before.state_rows > before.stored_bars, "partial states exist"

        store.optimize_bars()
        after = verify_build_bars(store, build_id=BUILD, canonical=rows)
        assert after.verdict() is Verdict.PASS
        assert after.counters()["bar_count"] == before.counters()["bar_count"]

    @covers("P06-K-06")
    def test_an_unreachable_view_target_fails_verification(self):
        store = ModelStore()
        store.setup()
        rows = [tick(i + 1, i * 1_000_000_000, 100 + i) for i in range(3)]
        store.fault_mv_target_unavailable = True
        with pytest.raises(Exception):
            store.insert_canonical(rows, block_size=3)

        report = verify_build_bars(store, build_id=BUILD, canonical=rows)
        assert report.verdict() is Verdict.FAIL
        assert report.missing_bars, "canonical rows landed with no bars derived"

    def test_the_oracle_is_recomputed_from_the_canonical_rows(self):
        store = ModelStore()
        store.setup()
        rows = [tick(i + 1, i * NS_PER_MINUTE, 100 + i) for i in range(5)]
        store.insert_canonical(rows, block_size=2)
        report = verify_build_bars(store, build_id=BUILD)
        assert report.reference_bars == len(reference_bars(rows)) == 5
        assert report.verdict() is Verdict.PASS

    def test_the_report_serialises_with_bounded_samples(self):
        reference = {
            key(i): Bar(100 + i, 100 + i, 100 + i, 100 + i, 1) for i in range(40)
        }
        report = compare(reference, {})
        payload = report.as_dict(sample_limit=3)
        assert payload["counters"]["bars_missing"] == 40
        assert len(payload["samples"]["missing_bars"]) == 3
        assert payload["verdict"] == "FAIL"
