"""Integration tests against a real ClickHouse. **Evidence class: REAL_CLICKHOUSE.**

These are the only tests in the repo whose results may back a statement about
ClickHouse. Everything here is skipped unless ``P06_CLICKHOUSE_URL`` points at a
reachable server, and a skip means ``NOT_RUN`` -- not ``PASS`` (section 14.2).

Run them with::

    docker compose up -d
    P06_CLICKHOUSE_URL=http://127.0.0.1:8123 pytest tests/clickhouse -v

The questions here are exactly the ones the in-process model cannot answer,
because they are about the server's own behaviour:

* Does the DDL in ``sql/`` actually parse and create what it claims, on the
  pinned version? (group B)
* Does ``intDiv(event_time_ns, 60000000000)`` bucket the way the oracle does,
  with no ``DateTime64`` downcast in the middle? (group D)
* Do ``argMinState``/``argMaxState`` with a tuple ordering key really produce a
  deterministic OPEN/CLOSE, and does a ``*Merge`` read give the same answer
  before and after ``OPTIMIZE FINAL``? (group I)
* Do 64-bit integers survive the HTTP round trip exactly? (case B-12, I-11)

Each test uses its own database so a failure cannot contaminate the next run.
"""

from __future__ import annotations

import os
import uuid

import pytest

from _coverage import covers
from md_analytics.contracts import INT64_MAX, NS_PER_MINUTE
from md_analytics.errors import Verdict
from md_analytics.fixtures import GOLDEN_BASE_NS, CorpusBuilder, golden_six_event, rebase_expected
from md_analytics.oracle import reference_bars
from md_analytics.pipeline import run_and_require_ready, run_build
from md_analytics.rows import canonical_from_raw
from md_analytics.schema_preflight import (
    ColumnSpec,
    EXPECTED_CANONICAL_COLUMNS,
    preflight_columns,
    preflight_engine,
)

pytestmark = pytest.mark.clickhouse

BASE = GOLDEN_BASE_NS
SEC = 1_000_000_000


@pytest.fixture
def ch(clickhouse_store):
    """A real store whose tables are dropped afterwards."""
    yield clickhouse_store
    # Leave the server as we found it; a leftover table would make the next
    # run's CREATE TABLE IF NOT EXISTS silently reuse this one (case B-09).
    for statement in (
        "DROP VIEW IF EXISTS md_analytics.mv_canonical_ticks_to_bars_1m",
        "DROP TABLE IF EXISTS md_analytics.bars_1m_state",
        "DROP TABLE IF EXISTS md_analytics.canonical_ticks",
    ):
        try:
            clickhouse_store.client.execute(statement)
        except Exception:  # noqa: BLE001 -- cleanup must not mask a test failure
            pass


def canonical_of(corpus, build_id):
    return [
        canonical_from_raw(row, build_id=build_id)
        for row in corpus.raw_rows
        if row.is_tick
    ]


class TestServerAndDdl:
    @covers("P06-B-09", kind="needs_clickhouse")
    def test_the_server_reports_its_version(self, ch):
        info = ch.info()
        assert info.evidence_class == "REAL_CLICKHOUSE"
        assert info.server_version, "a result without a version cannot be cited"

    @covers("P06-B-09", "P06-Q-01", kind="needs_clickhouse")
    def test_the_canonical_ddl_creates_exactly_what_is_pinned(self, ch):
        rows = ch.client.query_rows(
            ch.bank.get("table_columns"),
            params={"database": "md_analytics", "table": "canonical_ticks"},
        )
        observed = [ColumnSpec(str(n), str(t), int(p)) for n, t, p in rows]
        assert observed, "the table must exist after setup()"
        preflight_columns(
            observed,
            expected=EXPECTED_CANONICAL_COLUMNS,
            table="md_analytics.canonical_ticks",
        )

    @covers("P06-B-04", kind="needs_clickhouse")
    def test_the_engines_are_what_the_design_specifies(self, ch):
        for table, expected in (
            ("canonical_ticks", "MergeTree"),
            ("bars_1m_state", "AggregatingMergeTree"),
        ):
            rows = ch.client.query_rows(
                ch.bank.get("table_engine"),
                params={"database": "md_analytics", "table": table},
            )
            assert rows, f"{table} is missing"
            preflight_engine(str(rows[0][0]), expected=expected, table=table)

    @covers("P06-I-06", kind="needs_clickhouse")
    def test_the_state_table_partitions_in_utc(self, ch):
        rows = ch.client.query_rows(
            ch.bank.get("table_engine"),
            params={"database": "md_analytics", "table": "bars_1m_state"},
        )
        partition_key = str(rows[0][1])
        assert "UTC" in partition_key, (
            f"the partition expression {partition_key!r} must pin UTC, or a server "
            f"in a local zone splits a UTC day across two partitions"
        )


class TestIntegerFidelity:
    @covers("P06-B-12", "P06-D-13", kind="needs_clickhouse")
    def test_a_nanosecond_survives_the_round_trip_exactly(self, ch):
        exact = 1_700_000_000_000_000_001
        returned = ch.client.query_scalar(
            "SELECT toString({ns:Int64})", params={"ns": exact}
        )
        assert int(returned) == exact, "a Float64 cast would have moved the last digit"

    @covers("P06-C-04", kind="needs_clickhouse")
    @pytest.mark.parametrize("value", [INT64_MAX, INT64_MAX - 1, -(2**63), 0, -1])
    def test_extreme_int64_values_survive_the_round_trip(self, ch, value):
        returned = ch.client.query_scalar(
            "SELECT toString({v:Int64})", params={"v": value}
        )
        assert int(returned) == value

    @covers("P06-I-11", kind="needs_clickhouse")
    def test_large_counts_come_back_as_exact_integers(self, ch):
        big = 2**53 + 1
        returned = ch.client.query_scalar(
            "SELECT toString(toUInt64({v:Int64}))", params={"v": big}
        )
        assert int(returned) == big, "beyond a JS safe integer, so it must not be a float"

    @covers("P06-D-04", "P06-B-03", kind="needs_clickhouse")
    def test_the_server_derives_the_utc_date_the_same_way_python_does(self, ch):
        from md_analytics.contracts import utc_date_from_ns

        for ns in (
            BASE,
            1_793_404_799_999_999_999,  # 2026-10-30T23:59:59.999999999Z
            1_793_404_800_000_000_000,  # 2026-10-31T00:00:00Z
        ):
            assert ch.seconds_to_utc_date_check(ns) == utc_date_from_ns(ns).isoformat()

    @covers("P06-D-01", "P06-D-02", kind="needs_clickhouse")
    def test_the_server_buckets_minutes_from_integer_nanoseconds(self, ch):
        from md_analytics.timebars import bar_start_ns

        for ns in (BASE, BASE + NS_PER_MINUTE - 1, BASE + NS_PER_MINUTE, BASE + 1):
            returned = ch.client.query_scalar(
                "SELECT toString(intDiv({ns:Int64}, 60000000000) * 60000000000)",
                params={"ns": ns},
            )
            assert int(returned) == bar_start_ns(ns)


class TestGoldenOnTheServer:
    @covers("P06-A-01", "P06-H-03", kind="needs_clickhouse")
    def test_the_six_event_golden_corpus_matches_on_a_real_server(self, ch, catalog):
        case = golden_six_event(f"CH-GOLDEN-{uuid.uuid4().hex[:8]}")
        result = run_and_require_ready(ch, case.corpus, catalog=catalog, block_size=2)

        assert len(result.canonical) == 4
        expected = rebase_expected(case.expected_bars, build_id=result.build_id)
        # The golden corpus is keyed on the dataset id, so rebuild the expectation
        # under the generated one.
        expected = {
            key._replace(dataset_id=case.corpus.dataset_id): bar
            for key, bar in expected.items()
        }
        assert result.stored_bars == expected
        assert result.bars.verdict() is Verdict.PASS

    @covers("P06-I-01", "P06-L-09", kind="needs_clickhouse")
    def test_a_merge_read_is_correct_before_and_after_optimize_final(self, ch, catalog):
        dataset = f"CH-MERGE-{uuid.uuid4().hex[:8]}"
        b = CorpusBuilder(dataset)
        for i in range(60):
            b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + (i % 13) * 7)
        result = run_and_require_ready(ch, b.seal(), catalog=catalog, block_size=1)

        state_rows = ch.bar_state_row_count(build_id=result.build_id)
        assert state_rows > 1, (
            "60 single-row inserts must leave several partial states, or this test "
            "is not exercising the merge path at all"
        )
        before = ch.all_bars_1m(build_id=result.build_id)
        assert before == reference_bars(result.canonical)

        ch.optimize_bars()
        after = ch.all_bars_1m(build_id=result.build_id)
        assert after == before
        assert ch.bar_state_row_count(build_id=result.build_id) <= state_rows

    @covers("P06-H-11", "P06-I-08", kind="needs_clickhouse")
    def test_the_server_tie_breaks_on_sequence_deterministically(self, ch, catalog):
        dataset = f"CH-TIE-{uuid.uuid4().hex[:8]}"
        b = CorpusBuilder(dataset)
        b.tick("EURUSD", 3, BASE + 30 * SEC, 100_200)
        b.tick("EURUSD", 4, BASE + 30 * SEC, 99_900)
        result = run_and_require_ready(ch, b.seal(), catalog=catalog, block_size=1)

        bar = next(iter(result.stored_bars.values()))
        assert bar.open_ticks == 100_200, "argMinState with the tuple key picks seq 3"
        assert bar.close_ticks == 99_900
        assert bar.tick_count == 2

    @covers("P06-E-01", "P06-H-05", kind="needs_clickhouse")
    def test_physical_duplicates_do_not_inflate_the_server_side_count(self, ch, catalog):
        dataset = f"CH-DUP-{uuid.uuid4().hex[:8]}"
        b = CorpusBuilder(dataset)
        for i in range(50):
            row = b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
            b.duplicate(row, times=2)
        result = run_and_require_ready(ch, b.seal(), catalog=catalog, block_size=10)

        assert result.audit.physical_rows == 150
        assert len(result.canonical) == 50
        assert sum(bar.tick_count for bar in result.stored_bars.values()) == 50

    @covers("P06-I-07", kind="needs_clickhouse")
    def test_inserting_the_same_canonical_rows_twice_double_counts_on_the_server(
        self, ch, catalog
    ):
        """The hazard the builder exists to prevent, confirmed on a real server."""
        dataset = f"CH-DBL-{uuid.uuid4().hex[:8]}"
        b = CorpusBuilder(dataset)
        for i in range(5):
            b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
        corpus = b.seal()
        result = run_and_require_ready(ch, corpus, catalog=catalog, block_size=5)

        ch.insert_canonical(list(result.canonical), block_size=5)
        bars = ch.all_bars_1m(build_id=result.build_id)
        assert sum(bar.tick_count for bar in bars.values()) == 10, (
            "no engine de-duplicates logical identities, and no merge can subtract "
            "an aggregate state that was already written"
        )
        offenders = ch.canonical_identity_counts(build_id=result.build_id)
        assert offenders, "the audit query must report the duplicated identities"

    @covers("P06-G-11", "P06-AC-03", kind="needs_clickhouse")
    def test_the_audit_queries_run_and_return_nothing_for_a_clean_build(self, ch, catalog):
        dataset = f"CH-AUDIT-{uuid.uuid4().hex[:8]}"
        b = CorpusBuilder(dataset)
        for i in range(20):
            b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
        result = run_and_require_ready(ch, b.seal(), catalog=catalog, block_size=4)

        assert ch.canonical_identity_counts(build_id=result.build_id) == {}
        for name in ("canonical_event_date_drift", "canonical_scale_variants"):
            rows = ch.client.query_rows(
                ch.bank.get(name),
                params={"build_id": result.build_id, "row_limit": 100},
            )
            assert rows == [], f"{name} must be empty for a clean build"


class TestResourceGuards:
    @covers("P06-R-04", "P06-W-06", kind="needs_clickhouse")
    def test_a_result_row_limit_is_enforced_server_side(self, ch, catalog):
        from md_analytics.errors import ResourceLimitExceeded

        dataset = f"CH-LIMIT-{uuid.uuid4().hex[:8]}"
        b = CorpusBuilder(dataset)
        for i in range(50):
            b.tick("EURUSD", i + 1, BASE + i * NS_PER_MINUTE, 100_000 + i)
        result = run_and_require_ready(ch, b.seal(), catalog=catalog, block_size=10)

        ch.client.settings = {
            **ch.client.settings,
            "max_result_rows": "5",
            "result_overflow_mode": "throw",
        }
        with pytest.raises(ResourceLimitExceeded):
            ch.select_bars_1m(
                build_id=result.build_id,
                dataset_id=dataset,
                symbol="EURUSD",
                start_ns=BASE,
                end_ns=BASE + 50 * NS_PER_MINUTE,
                limit=50,
            )

    @covers("P06-Y-04", kind="needs_clickhouse")
    def test_a_hostile_parameter_value_cannot_alter_the_statement(self, ch):
        """Typed parameters are values, never SQL."""
        hostile = "x' OR 1=1 --"
        returned = ch.client.query_scalar(
            "SELECT {s:String}", params={"s": hostile}
        )
        assert returned == hostile, "it came back as data, not as a predicate"

    @covers("P06-V-01", kind="needs_clickhouse")
    def test_part_pressure_is_observable(self, ch, catalog):
        dataset = f"CH-PARTS-{uuid.uuid4().hex[:8]}"
        b = CorpusBuilder(dataset)
        for i in range(40):
            b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
        run_and_require_ready(ch, b.seal(), catalog=catalog, block_size=1)

        parts = ch.parts()
        canonical = [p for p in parts if p.table == "canonical_ticks"]
        assert canonical, "system.parts must report the canonical table"
        assert sum(p.rows for p in canonical) >= 40
