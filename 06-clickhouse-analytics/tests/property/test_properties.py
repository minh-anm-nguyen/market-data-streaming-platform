"""Property-based and metamorphic tests: section 17.1 and 17.2.

The metamorphic relations MR-01 to MR-10 are each a statement of the form "this
change to the input must (or must not) change the output". They catch a class of
bug that example-based tests miss: a pipeline can produce exactly the right
answer for the golden corpus and still depend on insert order, batch size or the
host timezone.

Hypothesis generates the inputs. Where a relation needs a specific structure --
a shared timestamp, a scale change, a late tick -- the generator builds it
explicitly rather than hoping a random draw produces it.
"""

from __future__ import annotations

from datetime import date

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from _coverage import covers
from md_analytics.contracts import NS_PER_MINUTE, SourceOrder
from md_analytics.errors import Verdict
from md_analytics.fixtures import GOLDEN_BASE_NS, CorpusBuilder
from md_analytics.oracle import BarKey, reference_bars, reference_bars_sorted
from md_analytics.pipeline import run_build
from md_analytics.raw_auditor import audit_raw
from md_analytics.rows import CanonicalTick
from md_analytics.store.base import bar_states_from_block, merge_state_rows, states_to_bars
from md_analytics.store.model import ModelStore
from md_analytics.timebars import bar_start_ns

pytestmark = pytest.mark.property

BASE = GOLDEN_BASE_NS
SEC = 1_000_000_000

SETTINGS = settings(
    max_examples=60,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)


@st.composite
def tick_specs(draw, *, min_size=1, max_size=40, span_minutes=5):
    """A set of ticks with distinct sequences inside a bounded window."""
    size = draw(st.integers(min_value=min_size, max_value=max_size))
    offsets = draw(
        st.lists(
            st.integers(min_value=0, max_value=span_minutes * NS_PER_MINUTE - 1),
            min_size=size,
            max_size=size,
        )
    )
    prices = draw(
        st.lists(
            st.integers(min_value=1, max_value=10**9),
            min_size=size,
            max_size=size,
        )
    )
    return [
        (i + 1, offset, price) for i, (offset, price) in enumerate(zip(offsets, prices))
    ]


def canonical(specs, *, build_id="b", dataset_id="DS-P", symbol="EURUSD", scale=100_000):
    return [
        CanonicalTick(
            build_id=build_id,
            dataset_id=dataset_id,
            symbol=symbol,
            sequence=sequence,
            event_time_ns=BASE + offset,
            event_date=date(2026, 10, 30),
            price_scale=scale,
            price_ticks=price,
            payload_digest="0" * 64,
            source_schema_version="marketdata.v1",
        )
        for sequence, offset, price in specs
    ]


def bars_via_states(ticks, *, block_size):
    """Bars as the MV plus a ``*Merge`` read would produce them."""
    rows = []
    for start in range(0, len(ticks), block_size):
        rows.extend(bar_states_from_block(ticks[start : start + block_size]))
    return states_to_bars(merge_state_rows(rows))


class TestOracleAgreement:
    @covers("P06-AD-01", "P06-AD-02")
    @SETTINGS
    @given(specs=tick_specs())
    def test_both_oracles_agree_on_any_input(self, specs):
        ticks = canonical(specs)
        assert reference_bars(ticks) == reference_bars_sorted(ticks)

    @covers("P06-H-01", "P06-I-01")
    @SETTINGS
    @given(specs=tick_specs(), block_size=st.integers(min_value=1, max_value=16))
    def test_the_state_path_agrees_with_the_oracle(self, specs, block_size):
        ticks = canonical(specs)
        assert bars_via_states(ticks, block_size=block_size) == reference_bars(ticks)

    @covers("P06-H-07", "P06-H-12")
    @SETTINGS
    @given(specs=tick_specs())
    def test_every_bar_satisfies_the_bounds_and_the_count(self, specs):
        ticks = canonical(specs)
        bars = reference_bars(ticks)
        assert sum(bar.tick_count for bar in bars.values()) == len(ticks)
        for key, bar in bars.items():
            bar.check_bounds()
            in_bucket = [
                t for t in ticks if bar_start_ns(t.event_time_ns) == key.bar_start_ns
            ]
            assert bar.tick_count == len(in_bucket)
            assert bar.high_ticks == max(t.price_ticks for t in in_bucket)
            assert bar.low_ticks == min(t.price_ticks for t in in_bucket)
            ordered = sorted(in_bucket, key=lambda t: SourceOrder(t.event_time_ns, t.sequence))
            assert bar.open_ticks == ordered[0].price_ticks
            assert bar.close_ticks == ordered[-1].price_ticks


class TestMetamorphicRelations:
    @covers("P06-H-01", "P06-H-02")
    @SETTINGS
    @given(specs=tick_specs(min_size=2), seed=st.integers(min_value=0, max_value=10**6))
    def test_mr_01_insert_order_does_not_change_the_bars(self, specs, seed):
        """MR-01: permuting the insert order of canonical ticks."""
        import random

        ticks = canonical(specs)
        shuffled = ticks[:]
        random.Random(seed).shuffle(shuffled)
        assert bars_via_states(shuffled, block_size=3) == bars_via_states(
            ticks, block_size=3
        )

    @covers("P06-E-01", "P06-H-05")
    @SETTINGS
    @given(specs=tick_specs(max_size=12), copies=st.integers(min_value=1, max_value=4))
    def test_mr_02_physical_duplicates_do_not_change_the_output(self, specs, copies):
        """MR-02: re-landing identical raw rows."""
        plain = CorpusBuilder("MR-02")
        duped = CorpusBuilder("MR-02")
        for sequence, offset, price in specs:
            plain.tick("EURUSD", sequence, BASE + offset, price)
            row = duped.tick("EURUSD", sequence, BASE + offset, price)
            duped.duplicate(row, times=copies)

        results = []
        for builder in (plain, duped):
            store = ModelStore()
            store.setup()
            results.append(run_build(store, builder.seal(), publish=False))

        assert results[0].verdict is Verdict.PASS and results[1].verdict is Verdict.PASS
        assert len(results[0].canonical) == len(results[1].canonical)
        strip = lambda bars: {  # noqa: E731 -- build ids differ by construction
            key._replace(build_id=""): bar for key, bar in bars.items()
        }
        assert strip(results[0].stored_bars) == strip(results[1].stored_bars)

    @covers("P06-G-14", "P06-I-09")
    @SETTINGS
    @given(
        specs=tick_specs(min_size=2),
        a=st.integers(min_value=1, max_value=20),
        b=st.integers(min_value=1, max_value=20),
    )
    def test_mr_03_batch_size_does_not_change_the_bars(self, specs, a, b):
        """MR-03: splitting one batch into N after the canonical gate."""
        ticks = canonical(specs)
        assert bars_via_states(ticks, block_size=a) == bars_via_states(ticks, block_size=b)

    @covers("P06-A-01", "P06-H-03")
    @SETTINGS
    @given(specs=tick_specs(max_size=12), bid=st.integers(min_value=1, max_value=10**9))
    def test_mr_04_adding_quotes_does_not_change_tick_bars(self, specs, bid):
        """MR-04: interleaving quotes between ticks."""
        without = CorpusBuilder("MR-04")
        with_quotes = CorpusBuilder("MR-04")
        next_sequence = max(s for s, _, _ in specs) + 1
        for sequence, offset, price in specs:
            without.tick("EURUSD", sequence, BASE + offset, price)
            with_quotes.tick("EURUSD", sequence, BASE + offset, price)
            with_quotes.quote(
                "EURUSD", next_sequence, BASE + offset, bid, bid + 2
            )
            next_sequence += 1

        outputs = []
        for builder in (without, with_quotes):
            store = ModelStore()
            store.setup()
            result = run_build(store, builder.seal(), publish=False)
            assert result.verdict is Verdict.PASS
            outputs.append(
                {key._replace(build_id=""): bar for key, bar in result.stored_bars.items()}
            )
        assert outputs[0] == outputs[1], "a quote must not reach a tick bar"

    @covers("P06-J-02", "P06-G-12")
    @SETTINGS
    @given(specs=tick_specs(min_size=1, max_size=10))
    def test_mr_05_a_higher_late_tick_only_raises_high_in_the_new_build(self, specs):
        """MR-05: adding a higher tick in a new build."""
        store = ModelStore()
        store.setup()
        builder = CorpusBuilder("MR-05")
        for sequence, offset, price in specs:
            builder.tick("EURUSD", sequence, BASE + offset, price)
        first = run_build(store, builder.seal(), publish=False)
        assume(first.verdict is Verdict.PASS)
        first_bars = dict(first.stored_bars)

        highest = max(price for _, _, price in specs)
        builder.tick("EURUSD", max(s for s, _, _ in specs) + 1, BASE, highest + 1)
        second = run_build(store, builder.seal(), publish=False, attempt=2)
        assert second.verdict is Verdict.PASS

        target = BarKey(second.build_id, "MR-05", "EURUSD", 100_000, BASE)
        old = BarKey(first.build_id, "MR-05", "EURUSD", 100_000, BASE)
        assert second.stored_bars[target].high_ticks == highest + 1
        if old in first_bars:
            assert second.stored_bars[target].high_ticks >= first_bars[old].high_ticks
            assert (
                second.stored_bars[target].tick_count == first_bars[old].tick_count + 1
            )
        assert store.all_bars_1m(build_id=first.build_id) == first_bars

    @covers("P06-E-06", "P06-H-07")
    @SETTINGS
    @given(specs=tick_specs(max_size=20))
    def test_mr_06_renaming_the_dataset_keeps_the_values_and_changes_the_key(self, specs):
        """MR-06: same prices and times under a different dataset id."""
        a = reference_bars(canonical(specs, dataset_id="DS-A"))
        b = reference_bars(canonical(specs, dataset_id="DS-B"))
        assert {k.dataset_id for k in a} == {"DS-A"}
        assert {k.dataset_id for k in b} == {"DS-B"}
        assert sorted(a.values(), key=repr) == sorted(b.values(), key=repr)
        assert set(a) & set(b) == set(), "no key is shared"

    @covers("P06-D-04", "P06-D-07")
    @SETTINGS
    @given(specs=tick_specs(max_size=20), tz=st.sampled_from(
        ["UTC", "Asia/Ho_Chi_Minh", "America/New_York", "Australia/Eucla"]
    ))
    def test_mr_07_the_host_timezone_does_not_move_a_bucket(self, specs, tz, monkeypatch):
        """MR-07: changing the host timezone while keeping epoch nanoseconds."""
        monkeypatch.setenv("TZ", tz)
        bars = reference_bars(canonical(specs))
        assert all(key.bar_start_ns % NS_PER_MINUTE == 0 for key in bars)
        monkeypatch.setenv("TZ", "UTC")
        assert reference_bars(canonical(specs)) == bars

    @covers("P06-I-09", "P06-I-12")
    @SETTINGS
    @given(specs=tick_specs(min_size=2), block_size=st.integers(min_value=1, max_value=8))
    def test_mr_08_merged_and_unmerged_states_finalize_identically(self, specs, block_size):
        """MR-08: whether or not a background merge has run."""
        store = ModelStore()
        store.setup()
        ticks = canonical(specs)
        store.insert_canonical(ticks, block_size=block_size)
        before = store.all_bars_1m(build_id="b")
        store.optimize_bars()
        assert store.all_bars_1m(build_id="b") == before

    @covers("P06-U-07")
    @SETTINGS
    @given(specs=tick_specs(max_size=20))
    def test_mr_09_repeated_reads_return_the_same_values(self, specs):
        """MR-09: a read is a pure function of the stored build."""
        store = ModelStore()
        store.setup()
        store.insert_canonical(canonical(specs), block_size=4)
        reads = [store.all_bars_1m(build_id="b") for _ in range(3)]
        assert reads[0] == reads[1] == reads[2]

    @covers("P06-F-05", "P06-AD-10")
    @SETTINGS
    @given(specs=tick_specs(min_size=3, max_size=15), drop=st.integers(min_value=0, max_value=14))
    def test_mr_10_dropping_an_accepted_event_is_always_rejected(self, specs, drop):
        """MR-10: removing one accepted event, even if the OHLC is unchanged."""
        index = drop % len(specs)
        builder = CorpusBuilder("MR-10")
        rows = [
            builder.tick("EURUSD", sequence, BASE + offset, price)
            for sequence, offset, price in specs
        ]
        builder.suppress_from_raw(rows[index])
        corpus = builder.seal()
        report = audit_raw(corpus.raw_rows, manifest=corpus.manifest, cut=corpus.cut)

        assert report.verdict() is Verdict.NO_GO, (
            "the bars may coincidentally be identical; completeness is still "
            "violated and the build must not publish"
        )
        assert len(report.missing) == 1


class TestAuditProperties:
    @covers("P06-E-01", "P06-E-02")
    @SETTINGS
    @given(
        specs=tick_specs(max_size=15),
        duplicates=st.integers(min_value=0, max_value=3),
        conflict=st.booleans(),
    )
    def test_the_audit_verdict_follows_the_injected_defects(
        self, specs, duplicates, conflict
    ):
        builder = CorpusBuilder("PROP-AUD")
        rows = [
            builder.tick("EURUSD", sequence, BASE + offset, price)
            for sequence, offset, price in specs
        ]
        if duplicates:
            builder.duplicate(rows[0], times=duplicates)
        if conflict:
            builder.conflict(rows[-1], price_ticks=rows[-1].price_ticks + 1)

        corpus = builder.seal()
        report = audit_raw(corpus.raw_rows, manifest=corpus.manifest, cut=corpus.cut)

        if conflict:
            assert report.verdict() is Verdict.NO_GO
            assert len(report.conflicts) == 1
        else:
            assert report.verdict() is Verdict.PASS
            assert report.duplicate_row_count == duplicates
            assert len(report.clean_tick_identities) == len(specs)

    @covers("P06-E-01")
    @SETTINGS
    @given(specs=tick_specs(max_size=15), copies=st.integers(min_value=1, max_value=5))
    def test_duplicates_never_change_the_clean_identity_set(self, specs, copies):
        builder = CorpusBuilder("PROP-DUP")
        for sequence, offset, price in specs:
            row = builder.tick("EURUSD", sequence, BASE + offset, price)
            builder.duplicate(row, times=copies)
        corpus = builder.seal()
        report = audit_raw(corpus.raw_rows, manifest=corpus.manifest, cut=corpus.cut)
        assert len(report.clean_tick_identities) == len(specs)
        assert report.physical_rows == len(specs) * (copies + 1)
