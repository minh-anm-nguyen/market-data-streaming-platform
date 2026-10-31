"""The matrix cases not reached by the module-focused suites.

Grouped by theme rather than by module, because each of these is a specific
"what if" from section 15 whose answer spans a couple of components. Everything
here runs in-process; cases needing a server, Kafka, a cluster or hardware
pressure are declared in ``tests/registry`` with a reason instead.
"""

from __future__ import annotations

import itertools
import json
from datetime import date
from pathlib import Path

import pytest

from _coverage import covers
from md_analytics.benchmark import LatencyStats, verdict_for_target
from md_analytics.canonical_builder import BuildState, compute_build_id
from md_analytics.contracts import (
    INT64_MAX,
    NS_PER_MINUTE,
    SourceIdentity,
    display_price,
    utc_date_from_ns,
)
from md_analytics.errors import ContractError, QueryRejected, SchemaDriftError, Verdict
from md_analytics.fixtures import GOLDEN_BASE_NS, CorpusBuilder, golden_six_event
from md_analytics.oracle import reference_bars
from md_analytics.pipeline import bars_for_symbol, run_and_require_ready, run_build
from md_analytics.raw_auditor import EventTimeWindow, audit_raw
from md_analytics.schema_preflight import expected_raw_digest, preflight_raw
from md_analytics.store.model import CANONICAL_TABLE, ModelStore
from md_analytics.ttl_controller import (
    RetentionPolicy,
    evaluate_retention_gate,
    would_expire_on_arrival,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

BASE = GOLDEN_BASE_NS
SEC = 1_000_000_000
# 2026-10-01T00:00:00Z .. 2026-11-01T00:00:00Z: the plausible window for the
# fixtures in this module, declared by contract rather than derived from data.
WINDOW = EventTimeWindow(1_790_812_800_000_000_000, 1_793_491_200_000_000_000)


@pytest.fixture
def s() -> ModelStore:
    store = ModelStore()
    store.setup()
    return store


def audit(corpus, **kwargs):
    return audit_raw(corpus.raw_rows, manifest=corpus.manifest, cut=corpus.cut, **kwargs)


# ------------------------------------------------------------- upstream (A, F)


class TestUpstreamContracts:
    @covers("P06-A-05", "P06-B-08")
    def test_a_framing_or_schema_mismatch_stops_before_any_build(self):
        """``MDP1``-prefixed framing instead of the declared contract version."""
        b = CorpusBuilder("REM-A05")
        b.tick("EURUSD", 1, BASE, 100_000, schema_version="MDP1")
        corpus = b.seal()
        # The row is internally consistent, so the audit passes it; what catches
        # it is that the build pins the schema and this is not that schema.
        report = audit(corpus)
        assert report.verdict() is Verdict.PASS
        digest_v1 = compute_build_id(
            manifest_sha256=corpus.cut.manifest_sha256,
            cut_digest=corpus.cut.cut_digest(),
            schema_sha256=expected_raw_digest(),
        )
        digest_other = compute_build_id(
            manifest_sha256=corpus.cut.manifest_sha256,
            cut_digest=corpus.cut.cut_digest(),
            schema_sha256="0" * 64,
        )
        assert digest_v1 != digest_other

        # And a renamed or absent raw table cannot be read past the preflight.
        with pytest.raises(SchemaDriftError):
            preflight_raw(columns=[], engine="MergeTree")

    @covers("P06-A-07", "P06-J-07", "P06-J-12")
    def test_ingest_order_and_clock_jumps_do_not_affect_bars(self, s, catalog):
        """Quotes land a minute before their ticks; the ingest clock goes backwards."""
        b = CorpusBuilder("REM-A07")
        quote = b.quote("EURUSD", 2, BASE + 20 * SEC, 100_010, 100_012)
        t1 = b.tick("EURUSD", 1, BASE, 100_000)
        t3 = b.tick("EURUSD", 3, BASE + 30 * SEC, 100_200)
        corpus = b.seal()

        # Rewrite the ingest clock so arrival order contradicts event order and
        # is not even monotonic, as an NTP step would produce.
        corpus.raw_rows = [
            quote._replace(ingested_at=1_000),
            t3._replace(ingested_at=5),
            t1._replace(ingested_at=900),
        ]
        result = run_and_require_ready(s, corpus, catalog=catalog, block_size=1)

        bar = bars_for_symbol(result, "EURUSD")[BASE]
        assert bar.open_ticks == 100_000, "earliest event time opens"
        assert bar.close_ticks == 100_200
        assert bar.tick_count == 2, "the quote does not count"

    @covers("P06-A-08", "P06-E-11", "P06-AB-11")
    def test_a_replay_through_a_new_topic_generation_keeps_its_identities(self, s, catalog):
        b = CorpusBuilder("REM-A08", topic_generation=1)
        originals = [b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i) for i in range(5)]
        corpus = b.seal()
        first = audit(corpus)

        replay = CorpusBuilder("REM-A08", topic_generation=2)
        for i in range(5):
            replay.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
        second = audit(replay.seal())

        assert first.clean_tick_identities == second.clean_tick_identities, (
            "identity is (dataset, symbol, sequence); the transport coordinate "
            "changed and the logical events did not"
        )
        assert {r.topic_generation for r in originals} == {1}

    @covers("P06-A-09", "P06-F-10")
    def test_a_poisoned_record_routed_to_a_dlq_is_not_expected_in_raw(self, s, catalog):
        b = CorpusBuilder("REM-A09")
        for i in range(10):
            b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
        # P05 accepted the poison record and routed it to the DLQ. The ledger
        # records the disposition, so raw is complete without it.
        b.reject_at_source(SourceIdentity("REM-A09", "EURUSD", 11), "dlq:unparseable_payload")
        corpus = b.seal()
        report = audit(corpus)

        assert report.verdict() is Verdict.PASS
        assert report.missing == [], "a DLQ'd record is accounted for, not missing"
        assert len(corpus.manifest.rejected) == 1
        result = run_and_require_ready(s, corpus, catalog=catalog)
        assert len(result.canonical) == 10

    @covers("P06-G-07", "P06-J-11")
    def test_rows_appended_during_a_build_are_outside_the_sealed_cut(self, s, catalog):
        b = CorpusBuilder("REM-G07")
        for i in range(5):
            b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
        corpus = b.seal()

        # The source keeps producing while the build runs.
        b.after_cut("EURUSD", 6, BASE + 10 * SEC, 999_999)
        corpus.raw_rows = b.rows()

        result = run_and_require_ready(s, corpus, catalog=catalog)
        assert result.audit.rows_out_of_cut == 1
        assert len(result.canonical) == 5
        assert all(t.price_ticks != 999_999 for t in result.canonical)


# ------------------------------------------------------------- prices (C) ----


class TestPriceEdges:
    @covers("P06-C-06")
    def test_a_zero_price_is_representable_and_the_ledger_decides(self, s, catalog):
        """Zero is a valid Int64; whether it is a valid *market* tick is policy."""
        b = CorpusBuilder("REM-C06")
        b.tick("EURUSD", 1, BASE, 0)
        corpus = b.seal()
        assert audit(corpus).verdict() is Verdict.PASS, (
            "the integer contract admits 0; nothing here invents a price filter"
        )

        excluded = CorpusBuilder("REM-C06B")
        excluded.tick("EURUSD", 1, BASE, 100_000)
        row = excluded.tick("EURUSD", 2, BASE + SEC, 0)
        excluded.unledgered(row)
        excluded.reject_at_source(SourceIdentity("REM-C06B", "EURUSD", 2), "zero_price")
        report = audit(excluded.seal())
        assert report.extra == [("REM-C06B", "EURUSD", 2)], (
            "if the source policy rejects zero prices, a zero-price row in raw is "
            "an extra row and blocks the build"
        )

    @covers("P06-C-07")
    def test_a_negative_price_is_never_coerced(self, s, catalog):
        b = CorpusBuilder("REM-C07")
        b.tick("EURUSD", 1, BASE, -250)
        b.tick("EURUSD", 2, BASE + SEC, 100)
        result = run_and_require_ready(s, b.seal(), catalog=catalog)

        bar = bars_for_symbol(result, "EURUSD")[BASE]
        assert bar.low_ticks == -250, "no abs() and no clamp to zero"
        assert bar.open_ticks == -250 and bar.high_ticks == 100
        assert display_price(-250, 100_000) == "-0.00250"

    @covers("P06-C-08")
    def test_extreme_prices_never_go_through_a_sum(self, s, catalog):
        """Many near-``Int64`` prices: extrema are safe where a sum would overflow."""
        b = CorpusBuilder("REM-C08")
        for i in range(20):
            b.tick("EURUSD", i + 1, BASE + i * SEC, INT64_MAX - i)
        result = run_and_require_ready(s, b.seal(), catalog=catalog, block_size=3)

        bar = bars_for_symbol(result, "EURUSD")[BASE]
        assert bar.high_ticks == INT64_MAX
        assert bar.low_ticks == INT64_MAX - 19
        assert bar.tick_count == 20
        # A sum of these would exceed Int64 by a factor of ~20. No bar field is a
        # sum, which is why the question never arises (case C-08).
        from md_analytics.oracle import Bar

        assert set(Bar._fields) == {
            "open_ticks",
            "high_ticks",
            "low_ticks",
            "close_ticks",
            "tick_count",
        }

    @covers("P06-C-10")
    def test_no_quote_derived_price_product_exists_to_overflow(self):
        """A midpoint product would need an approved rounding rule; there is none."""
        from md_analytics import oracle, store

        names = set(dir(oracle)) | set(dir(store))
        assert not any(
            candidate in names for candidate in ("midprice", "mid_price", "quote_bars")
        ), (
            "a (bid+ask) midpoint is an optional separate product (section 3.2); "
            "it is deliberately absent rather than added without a rounding policy"
        )

    @covers("P06-C-12")
    def test_high_and_low_are_compared_as_integers(self, s, catalog):
        """Two prices that collide as Float64 but differ as Int64."""
        a = 2**53
        b_price = 2**53 + 1
        assert float(a) == float(b_price), "indistinguishable as doubles, by construction"

        builder = CorpusBuilder("REM-C12")
        builder.tick("EURUSD", 1, BASE, a)
        builder.tick("EURUSD", 2, BASE + SEC, b_price)
        result = run_and_require_ready(s, builder.seal(), catalog=catalog)

        bar = bars_for_symbol(result, "EURUSD")[BASE]
        assert bar.low_ticks == a and bar.high_ticks == b_price
        assert bar.high_ticks - bar.low_ticks == 1, "the integer difference survives"


# -------------------------------------------------- time, late data (D, J) ---


class TestTimeAndLateData:
    @covers("P06-D-09", "P06-J-06")
    def test_equal_timestamps_are_deterministic_over_many_permutations(self):
        prices = [(3, 100_200), (4, 99_900), (5, 100_100)]
        seen = set()
        for ordering in itertools.permutations(prices):
            store = ModelStore()
            store.setup()
            b = CorpusBuilder("REM-D09")
            for sequence, price in ordering:
                b.tick("EURUSD", sequence, BASE + 30 * SEC, price)
            result = run_build(store, b.seal(), publish=False, block_size=1)
            assert result.verdict is Verdict.PASS
            seen.add(bars_for_symbol(result, "EURUSD")[BASE])
        assert len(seen) == 1, "the sequence tie-break makes all six orders agree"
        bar = seen.pop()
        assert bar.open_ticks == 100_200 and bar.close_ticks == 100_100

    @covers("P06-D-10", "P06-J-08", "P06-O-12")
    def test_a_future_dated_tick_is_quarantined_not_published(self, s, catalog):
        """+100 years: representable, implausible, and it would evade TTL forever."""
        far_future = BASE + 100 * 365 * 86_400 * SEC
        assert far_future < INT64_MAX, "inside the Int64 domain, by construction"

        b = CorpusBuilder("REM-D10")
        b.tick("EURUSD", 1, BASE, 100_000)
        b.tick("EURUSD", 2, far_future, 100_100)
        corpus = b.seal()

        report = audit(corpus, event_time_window=WINDOW)
        assert report.verdict() is Verdict.NO_GO
        assert report.out_of_window == [("REM-D10", "EURUSD", 2)]

        # And it would never expire: TTL is evaluated on event time.
        assert not would_expire_on_arrival(
            event_date=utc_date_from_ns(far_future), today=date(2026, 10, 30), ttl_days=90
        )

    @covers("P06-J-05")
    def test_a_sealed_snapshot_needs_no_watermark_guess(self, s, catalog):
        """Unbounded late arrivals cannot reach a published build."""
        b = CorpusBuilder("REM-J05")
        b.tick("EURUSD", 1, BASE, 100_000)
        corpus = b.seal()
        published = run_and_require_ready(s, corpus, catalog=catalog)
        before = dict(published.stored_bars)

        # Late events keep arriving, indefinitely. The MVP publishes sealed
        # snapshots, so no lateness bound has to be assumed (section 8).
        for i in range(2, 12):
            b.after_cut("EURUSD", i, BASE + i, 999_000 + i)
        late_report = audit_raw(b.rows(), manifest=corpus.manifest, cut=corpus.cut)

        assert late_report.rows_out_of_cut == 10
        assert late_report.verdict() is Verdict.PASS
        assert s.all_bars_1m(build_id=published.build_id) == before

    @covers("P06-J-10")
    def test_a_late_event_already_past_the_ttl_is_refused_rather_than_dropped(self):
        assert would_expire_on_arrival(
            event_date=date(2026, 5, 1), today=date(2026, 10, 30), ttl_days=90
        )

    @covers("P06-J-13")
    def test_an_out_of_order_burst_across_eight_partitions_still_matches(self, s, catalog):
        import random

        b = CorpusBuilder("REM-J13")
        symbols = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF", "USDCAD", "NZDUSD", "EURGBP"]
        specs = [
            (symbol, i + 1, BASE + ((i * 7919) % 240) * SEC, 100_000 + i)
            for symbol in symbols
            for i in range(30)
        ]
        random.Random(99).shuffle(specs)
        for symbol, sequence, ns, price in specs:
            b.tick(symbol, sequence, ns, price)

        result = run_and_require_ready(s, b.seal(), catalog=catalog, block_size=17)
        partitions = {r.kafka_partition for r in b.rows()}
        assert len(partitions) >= 2, "the fixture really does span partitions"
        assert result.bars.verdict() is Verdict.PASS
        assert sum(bar.tick_count for bar in result.stored_bars.values()) == 240

    @covers("P06-J-14")
    def test_a_rebuild_recomputes_every_bar_and_only_one_changes(self, s, catalog):
        b = CorpusBuilder("REM-J14")
        for i in range(5):
            b.tick("EURUSD", i + 1, BASE + i * NS_PER_MINUTE, 100_000 + i)
        first = run_and_require_ready(s, b.seal(), catalog=catalog)
        before = {k.bar_start_ns: v for k, v in first.stored_bars.items()}

        b.tick("EURUSD", 99, BASE + 2 * NS_PER_MINUTE + SEC, 500_000)
        second = run_and_require_ready(
            s, b.seal(), catalog=catalog, expected_current=first.build_id,
            prior_build_bars=dict(first.stored_bars),
        )
        after = {k.bar_start_ns: v for k, v in second.stored_bars.items()}

        changed = [m for m in before if before[m] != after[m]]
        assert changed == [BASE + 2 * NS_PER_MINUTE], (
            "the whole build is recomputed, so correctness does not depend on "
            "scoping the rebuild to the affected minute"
        )
        assert after[BASE + 2 * NS_PER_MINUTE].high_ticks == 500_000


# --------------------------------------------------- view lifecycle (K) ------


class TestViewLifecycle:
    @covers("P06-K-02")
    def test_dropping_the_view_mid_build_leaves_a_detectable_gap(self, s, catalog):
        b = CorpusBuilder("REM-K02")
        for i in range(10):
            b.tick("EURUSD", i + 1, BASE + i * NS_PER_MINUTE, 100_000 + i)
        corpus = b.seal()
        report = audit(corpus)

        from md_analytics.bars_verifier import verify_build_bars
        from md_analytics.canonical_builder import build_canonical, select_canonical_rows

        rows = select_canonical_rows(corpus.raw_rows, audit=report, physical_build_id="k02.a1")
        s.insert_canonical(rows[:5], block_size=5)
        s.detach_mv()  # DROP VIEW while the build is still inserting
        s.insert_canonical(rows[5:], block_size=5)

        verification = verify_build_bars(s, build_id="k02.a1", canonical=rows)
        assert verification.verdict() is Verdict.FAIL
        assert len(verification.missing_bars) == 5, (
            "the second half produced no bars and nothing said so at insert time"
        )

    @covers("P06-G-09", "P06-K-12")
    def test_partially_written_state_rows_are_caught_by_the_verifier(self, s, catalog):
        from md_analytics.bars_verifier import verify_build_bars

        b = CorpusBuilder("REM-K12")
        for i in range(8):
            b.tick("EURUSD", i + 1, BASE + i * NS_PER_MINUTE, 100_000 + i)
        result = run_and_require_ready(s, b.seal(), catalog=catalog, block_size=2)
        assert result.bars.verdict() is Verdict.PASS

        # The server restarts; the view's metadata survives but some target parts
        # did not make it to disk.
        removed = s.drop_bar_state_rows(build_id=result.build_id, keep=3)
        assert removed > 0
        after = verify_build_bars(s, build_id=result.build_id)
        assert after.verdict() is Verdict.FAIL
        assert after.missing_bars

    @covers("P06-K-03", "P06-K-04", "P06-K-10", "P06-K-11", "P06-K-14")
    def test_the_view_definition_is_pinned_against_the_known_mistakes(self, sql_bank):
        mv = " ".join(sql_bank.statements("004_mv_canonical_to_bars.sql"))
        assert "TO md_analytics.bars_1m_state" in mv, "the TO target is pinned"
        assert "GROUP BY build_id, dataset_id, symbol, price_scale, bar_start_ns" in mv
        assert "now()" not in mv, "grouping by ingest time would not be a market bar"
        assert "dictGet" not in mv, "no hidden dictionary dependency"
        assert "REFRESH" not in mv, "incremental, not refreshable"
        # A changed group key would be caught at runtime too: the oracle compares
        # per bar key, so a missing dimension shows up as missing and extra bars.
        assert sql_bank.sha256("bars_1m") != sql_bank.sha256("canonical_range")

    @covers("P06-AA-09")
    def test_running_the_migration_twice_is_idempotent(self, s, catalog):
        b = CorpusBuilder("REM-AA09")
        for i in range(4):
            b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
        s.setup()  # a rerun of the DDL
        s.setup()
        result = run_and_require_ready(s, b.seal(), catalog=catalog)
        assert sum(bar.tick_count for bar in result.stored_bars.values()) == 4, (
            "a duplicated view definition would double every count"
        )


# ------------------------------------------------------------ retention (O) --


class TestRetentionDetails:
    @covers("P06-O-07", "P06-O-13")
    def test_ttl_is_defined_on_event_time_and_never_on_a_column(self, sql_bank, s):
        for name in ("002_canonical_ticks.sql", "003_bars_1m_state.sql"):
            for statement in sql_bank.statements(name):
                assert "TTL" not in statement

        # The modelled TTL predicate is the event date, not an ingest clock.
        old = date(2026, 1, 1)
        b = CorpusBuilder("REM-O07")
        row = b.tick("EURUSD", 1, BASE, 100_000)
        from md_analytics.rows import canonical_from_raw

        tick = canonical_from_raw(row, build_id="o07")._replace(event_date=old)
        s.insert_canonical([tick], block_size=1)
        assert s.apply_ttl(table=CANONICAL_TABLE, cutoff=date(2026, 6, 1)) == 1

    @covers("P06-O-08")
    def test_a_mixed_part_expires_only_its_old_rows(self, s):
        from md_analytics.rows import canonical_from_raw

        b = CorpusBuilder("REM-O08")
        rows = []
        for i, day in enumerate((date(2026, 1, 1), date(2026, 10, 30))):
            raw = b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
            rows.append(canonical_from_raw(raw, build_id="o08")._replace(event_date=day))
        s.insert_canonical(rows, block_size=2)  # one part, two dates

        assert s.apply_ttl(table=CANONICAL_TABLE, cutoff=date(2026, 6, 1)) == 1
        survivors = s.all_canonical(build_id="o08")
        assert len(survivors) == 1 and survivors[0].event_date == date(2026, 10, 30)

    @covers("P06-O-11")
    def test_the_gate_requires_headroom_for_backlog_and_downtime(self):
        tight = RetentionPolicy(
            raw_ttl_days=30,
            recovery_window_days=7,
            audit_window_days=30,
            rebuild_window_days=14,
            late_arrival_window_days=3,
            restore_drill_passed=True,
            backup_age_days=1,
            backup_max_age_days=2,
        )
        assert evaluate_retention_gate(tight).allowed, "exactly covers the audit window"

        with_backlog = RetentionPolicy(
            raw_ttl_days=30,
            recovery_window_days=7,
            audit_window_days=30,
            rebuild_window_days=14,
            # A week of ingestion backlog extends how late data can still arrive.
            late_arrival_window_days=37,
            restore_drill_passed=True,
            backup_age_days=1,
            backup_max_age_days=2,
        )
        gate = evaluate_retention_gate(with_backlog)
        assert not gate.allowed and gate.required_days == 37

    @covers("P06-O-14", "P06-AB-07")
    def test_a_whole_day_partition_drop_leaves_unauditable_bars(self, s, catalog):
        b = CorpusBuilder("REM-O14")
        for i in range(6):
            b.tick("EURUSD", i + 1, BASE + i * NS_PER_MINUTE, 100_000 + i)
        result = run_and_require_ready(s, b.seal(), catalog=catalog)

        removed = s.drop_canonical_partition(partition="20261030")
        assert removed == 6
        assert s.all_canonical(build_id=result.build_id) == []
        assert s.all_bars_1m(build_id=result.build_id), (
            "the bars survive the fast delete, so the manifest must record that a "
            "full raw audit is no longer possible"
        )
        from md_analytics.bars_verifier import verify_build_bars

        assert verify_build_bars(s, build_id=result.build_id).verdict() is Verdict.FAIL


# ----------------------------------------------------------- query API (R) ---


class TestQueryApiDetails:
    @pytest.fixture
    def service(self, s, catalog):
        from md_analytics.query_service import QueryService

        b = CorpusBuilder("REM-R")
        for i in range(6):
            b.tick("EURUSD", i + 1, BASE + i * NS_PER_MINUTE, 100_000 + i)
        result = run_and_require_ready(s, b.seal(), catalog=catalog, block_size=2)
        return QueryService(s, catalog, symbol_allowlist=["EURUSD"]), result.build_id

    @covers("P06-R-07", "P06-R-13")
    def test_prices_are_integers_and_nothing_is_called_volume(self, service):
        svc, build_id = service
        row = svc.get_bars(
            build_id=build_id,
            dataset_id="REM-R",
            symbol="EURUSD",
            from_ns=BASE,
            to_ns=BASE + NS_PER_MINUTE,
        ).bars[0].as_dict()
        assert all(isinstance(row[f], int) for f in ("open_ticks", "high_ticks", "low_ticks", "close_ticks"))
        assert not any(isinstance(v, float) for v in row.values())
        assert "volume" not in row
        assert row["price_scale"] == 100_000, "the scale travels with the prices"

    @covers("P06-R-08")
    def test_pagination_is_ordered_and_reports_truncation(self, service):
        svc, build_id = service
        page = svc.get_bars(
            build_id=build_id,
            dataset_id="REM-R",
            symbol="EURUSD",
            from_ns=BASE,
            to_ns=BASE + 6 * NS_PER_MINUTE,
            limit=2,
        )
        starts = [int(b.bar_start_ns) for b in page.bars]
        assert starts == sorted(starts) and len(set(starts)) == 2
        assert page.truncated

        # The build is pinned, so a second page cannot shift under the caller.
        again = svc.get_bars(
            build_id=build_id,
            dataset_id="REM-R",
            symbol="EURUSD",
            from_ns=BASE,
            to_ns=BASE + 6 * NS_PER_MINUTE,
            limit=2,
        )
        assert [int(b.bar_start_ns) for b in again.bars] == starts

    @covers("P06-R-10")
    def test_a_nanosecond_beyond_the_js_safe_integer_survives(self, service):
        svc, build_id = service
        unsafe = 1_700_000_000_000_000_001
        assert unsafe > 2**53
        with pytest.raises(QueryRejected) as exc:
            svc.get_bars(
                build_id=build_id,
                dataset_id="REM-R",
                symbol="EURUSD",
                from_ns=str(unsafe),
                to_ns=str(unsafe + NS_PER_MINUTE),
            )
        # Rejected for alignment, which proves the exact value was parsed rather
        # than rounded: a rounded value would have been minute-aligned.
        assert exc.value.code == "RANGE_NOT_ALIGNED"
        assert exc.value.context["start_ns"] == unsafe

    @covers("P06-R-14")
    def test_a_lowercase_symbol_is_refused_not_normalised(self, service):
        svc, build_id = service
        with pytest.raises(QueryRejected) as exc:
            svc.get_bars(
                build_id=build_id,
                dataset_id="REM-R",
                symbol="eurusd",
                from_ns=BASE,
                to_ns=BASE + NS_PER_MINUTE,
            )
        assert exc.value.code == "SYMBOL_NOT_ALLOWED"


# ------------------------------------------------------- operations (AA, AD) -


class TestOperations:
    @covers("P06-AA-07", "P06-AD-12")
    def test_changing_the_sql_or_the_schema_changes_the_recorded_digests(
        self, sql_bank, tmp_path
    ):
        import shutil

        altered = tmp_path / "sql"
        shutil.copytree(sql_bank.sql_dir, altered)
        target = altered / "005_read_queries.sql"
        target.write_text(
            target.read_text(encoding="utf-8").replace("LIMIT {row_limit:UInt32}", "LIMIT 7"),
            encoding="utf-8",
        )
        from md_analytics.sqlbank import SqlBank

        assert SqlBank(altered).bank_digest() != sql_bank.bank_digest(), (
            "a golden fixture recorded against the old SQL cannot silently apply "
            "to the new SQL"
        )
        assert compute_build_id(
            manifest_sha256="m", cut_digest="c", schema_sha256="old"
        ) != compute_build_id(manifest_sha256="m", cut_digest="c", schema_sha256="new")

    @covers("P06-AA-10")
    def test_optimize_final_is_not_part_of_any_read_or_build_path(self, sql_bank):
        def without_comments(sql: str) -> str:
            lines = [
                line for line in sql.splitlines() if not line.strip().startswith("--")
            ]
            return " ".join(lines).upper()

        for name in sql_bank.names():
            assert "OPTIMIZE" not in without_comments(sql_bank.get(name))

        import inspect

        from md_analytics import pipeline
        from md_analytics.store import model

        assert "optimize_bars" not in inspect.getsource(pipeline), (
            "ADR-010: forcing a merge is a lab tool, never a step in the pipeline"
        )
        assert "a background merge would" in inspect.getdoc(model.ModelStore.optimize_bars)

    @covers("P06-AA-11")
    def test_a_failed_new_build_leaves_a_working_revert_path(self, s, catalog):
        b = CorpusBuilder("REM-AA11")
        b.tick("EURUSD", 1, BASE, 100_000)
        good = run_and_require_ready(s, b.seal(), catalog=catalog)

        # New bar logic ships and its build fails verification.
        b.tick("EURUSD", 2, BASE + SEC, 100_100)
        s.fault_mv_target_unavailable = True
        bad = run_build(
            s, b.seal(), catalog=catalog, attempt=2, expected_current=good.build_id
        )
        assert bad.state is BuildState.DIRTY

        assert catalog.active_build_id() == good.build_id
        assert catalog.require_ready(good.build_id).bar_tick_count == 1, (
            "the previous build is still READY and still serves: that is the revert"
        )

    @covers("P06-AA-14", "P06-AD-11")
    def test_a_dry_run_cannot_be_reported_as_a_throughput_result(self):
        from md_analytics.benchmark import BenchmarkResult

        dry_run = BenchmarkResult(
            template="ohlc_1m",
            repetitions=10,
            cold=LatencyStats.of([1.0]),
            warm=LatencyStats.of([1.0] * 9),
            rows_returned=10,
            errors=0,
        )
        verdict, reason = verdict_for_target(
            dry_run,
            target_p95_ms=280.0,
            correctness_verdict=Verdict.PASS,
            dataset_unique_rows=1_000,
            required_unique_rows=200_000_000,
            same_profile_as_reference=False,
        )
        assert verdict is Verdict.INCONCLUSIVE
        assert "below the 200000000 the comparison requires" in reason

    @covers("P06-AB-14", "P06-Y-05")
    def test_no_secret_reaches_an_error_context_or_a_resolved_config(self, tmp_path):
        from md_analytics.config import load_config
        from md_analytics.errors import StoreError

        source = tmp_path / "c.toml"
        text = (
            (REPO_ROOT / "config" / "dev.toml")
            .read_text(encoding="utf-8")
            .replace('password = ""', 'password = "sup3rs3cret"')
        )
        source.write_text(text, encoding="utf-8")
        config = load_config(source)

        assert "sup3rs3cret" not in json.dumps(config.resolved())

        error = StoreError("connection failed", code="CLICKHOUSE_HTTP_ERROR", status=500)
        assert "sup3rs3cret" not in json.dumps(error.as_dict())

    @covers("P06-AD-14")
    def test_the_suite_states_its_own_incompleteness(self):
        """The matrix is a catalog, not a proof of exhaustiveness (section 15)."""
        from _coverage import covered_ids

        executed = covered_ids("automated", "model_only")
        assert len(executed) < 420, (
            "claiming every case is executed here would be the failure mode the "
            "design warns about; property, fuzz and soak testing cover combinations "
            "the catalog does not enumerate"
        )


# ------------------------------------------------------------- miscellaneous -


class TestGoldenInvariance:
    @covers("P06-B-14", "P06-AD-13")
    def test_the_golden_result_does_not_depend_on_block_size_or_timing(self, catalog):
        """A storage or timing change is a performance change, not a data change."""
        case = golden_six_event()
        outputs = []
        for block_size in (1, 2, 3, 6, 1_000):
            store = ModelStore()
            store.setup()
            result = run_build(store, case.corpus, publish=False, block_size=block_size)
            outputs.append(
                {k._replace(build_id=""): v for k, v in result.stored_bars.items()}
            )
        assert all(o == outputs[0] for o in outputs)
        assert outputs[0] == {
            k._replace(build_id=""): v for k, v in case.expected_bars.items()
        }

    @covers("P06-AD-10")
    def test_the_oracle_and_the_pipeline_disagree_when_a_row_is_removed(self):
        case = golden_six_event()
        ticks = [r for r in case.corpus.raw_rows if r.is_tick]
        from md_analytics.rows import canonical_from_raw

        full = [canonical_from_raw(r, build_id="b") for r in ticks]
        assert reference_bars(full) != reference_bars(full[:-1]), (
            "the oracle must be sensitive to the data it is given, or it proves "
            "nothing about the pipeline"
        )
