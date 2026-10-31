"""The model store's parts, MV trigger, merges and TTL (groups I, K, L, M, O).

These results are evidence about *this pipeline* against a model of the
documented ClickHouse semantics, so they are registered as ``model_only``. The
same behaviours against a real server live in ``tests/clickhouse``.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from _coverage import covers
from md_analytics.contracts import NS_PER_MINUTE
from md_analytics.errors import AckUnknownError, ResourceLimitExceeded, StoreError
from md_analytics.oracle import reference_bars
from md_analytics.store.model import CANONICAL_TABLE, ModelStore
from tests.unit.test_oracle import BUILD, DATASET, NOON, tick


@pytest.fixture
def s() -> ModelStore:
    store = ModelStore()
    store.setup()
    return store


def ticks(n: int, *, step_ns: int = 1_000_000_000):
    return [tick(i + 1, i * step_ns, 100_000 + i) for i in range(n)]


class TestEvidenceClass:
    def test_the_model_declares_itself_as_a_model(self, s):
        info = s.info()
        assert info.evidence_class == "MODEL"
        assert info.server_version is None
        assert "not a server" in info.details["note"]


class TestParts:
    @covers("P06-M-01", kind="model_only")
    def test_each_insert_block_becomes_its_own_part(self, s):
        s.insert_canonical(ticks(10), block_size=3)
        canonical_parts = [p for p in s.parts() if p.table == "canonical_ticks"]
        assert sum(p.active_parts for p in canonical_parts) == 4, "10 rows in blocks of 3"

    @covers("P06-M-02", kind="model_only")
    def test_parts_are_grouped_by_utc_day_partition(self, s):
        day_two = [
            tick(100, 86_400 * 1_000_000_000, 100)._replace(event_date=date(2026, 10, 31))
        ]
        s.insert_canonical(ticks(2) + day_two, block_size=10)
        partitions = {p.partition for p in s.parts() if p.table == "canonical_ticks"}
        assert partitions == {"20261030", "20261031"}

    @covers("P06-M-07", kind="model_only")
    def test_many_tiny_inserts_create_many_parts(self, s):
        s.insert_canonical(ticks(50), block_size=1)
        canonical = [p for p in s.parts() if p.table == "canonical_ticks"]
        assert sum(p.active_parts for p in canonical) == 50, (
            "part pressure is a direct consequence of batch size, which is what "
            "the NFR-08 budget has to watch"
        )


class TestMaterializedView:
    @covers("P06-K-01", kind="model_only")
    def test_the_view_fires_per_block(self, s):
        s.insert_canonical(ticks(6), block_size=2)
        assert s.bar_state_row_count(build_id=BUILD) == 3, "three blocks, one minute"
        assert len(s.all_bars_1m(build_id=BUILD)) == 1

    @covers("P06-I-01", kind="model_only")
    def test_read_path_is_correct_before_any_merge(self, s):
        rows = ticks(6)
        s.insert_canonical(rows, block_size=1)
        assert s.bar_state_row_count(build_id=BUILD) == 6
        assert s.all_bars_1m(build_id=BUILD) == reference_bars(rows)

    @covers("P06-I-09", "P06-I-12", kind="model_only")
    def test_read_path_gives_the_same_answer_after_a_merge(self, s):
        rows = ticks(6)
        s.insert_canonical(rows, block_size=1)
        before = s.all_bars_1m(build_id=BUILD)
        s.optimize_bars()
        after = s.all_bars_1m(build_id=BUILD)
        assert before == after
        assert s.bar_state_row_count(build_id=BUILD) == 1, "the merge collapsed the parts"

    @covers("P06-L-09", kind="model_only")
    def test_stopping_merges_does_not_change_results(self, s):
        rows = ticks(4)
        s.stop_merges()
        s.insert_canonical(rows, block_size=1)
        s.optimize_bars()  # a no-op while merges are stopped
        assert s.bar_state_row_count(build_id=BUILD) == 4
        assert s.all_bars_1m(build_id=BUILD) == reference_bars(rows)

    @covers("P06-G-05", kind="model_only")
    def test_rows_inserted_before_the_view_exists_produce_no_bars(self):
        store = ModelStore()  # setup() not called: no MV attached
        store.insert_canonical(ticks(3), block_size=3)
        assert store.bar_state_row_count(build_id=BUILD) == 0
        assert store.all_canonical(build_id=BUILD) != []
        assert store.all_bars_1m(build_id=BUILD) == {}, (
            "attaching the view later does not backfill: the gap is invisible "
            "until an independent audit compares the two"
        )

    @covers("P06-I-07", kind="model_only")
    def test_inserting_the_same_canonical_rows_twice_double_counts(self, s):
        rows = ticks(3)
        s.insert_canonical(rows, block_size=3)
        s.insert_canonical(rows, block_size=3)
        bars = s.all_bars_1m(build_id=BUILD)
        assert next(iter(bars.values())).tick_count == 6
        assert s.canonical_identity_counts(build_id=BUILD)[(DATASET, "EURUSD", 1)] == 2, (
            "no engine de-duplicates logical identities; the builder must prevent this"
        )

    @covers("P06-K-06", kind="model_only")
    def test_an_unavailable_view_target_still_leaves_the_source_rows(self, s):
        s.fault_mv_target_unavailable = True
        with pytest.raises(StoreError) as exc:
            s.insert_canonical(ticks(3), block_size=3)
        assert exc.value.code == "MV_TARGET_UNAVAILABLE"
        assert "mv_target_unavailable" in s.fired
        assert s.all_canonical(build_id=BUILD), (
            "the source insert completed before the dependent insert failed, which "
            "is why this ends in a build that must not be READY"
        )
        assert s.all_bars_1m(build_id=BUILD) == {}


class TestFaults:
    @covers("P06-G-02", kind="model_only")
    def test_ack_unknown_leaves_the_rows_durable(self, s):
        s.fault_ack_unknown_after_insert = 1
        with pytest.raises(AckUnknownError):
            s.insert_canonical(ticks(4), block_size=2)
        assert "ack_unknown_after_insert" in s.fired
        assert len(s.all_canonical(build_id=BUILD)) == 4, "only the acknowledgement was lost"

    @covers("P06-G-01", kind="model_only")
    def test_aborting_mid_insert_leaves_a_partial_build(self, s):
        s.fault_stop_after_blocks = 2
        with pytest.raises(StoreError) as exc:
            s.insert_canonical(ticks(10), block_size=2)
        assert exc.value.context["rows_written"] == 4
        assert len(s.all_canonical(build_id=BUILD)) == 4

    @covers("P06-W-02", kind="model_only")
    def test_disk_full_is_a_bounded_error(self, s):
        s.fault_disk_full = True
        with pytest.raises(StoreError) as exc:
            s.insert_canonical(ticks(1), block_size=1)
        assert exc.value.code == "DISK_FULL"
        assert s.all_canonical(build_id=BUILD) == []

    @covers("P06-I-13", "P06-R-04", kind="model_only")
    def test_an_oversized_result_is_refused_rather_than_truncated(self):
        store = ModelStore(max_result_rows=5)
        store.setup()
        store.insert_canonical(
            [tick(i + 1, i * NS_PER_MINUTE, 100 + i) for i in range(10)], block_size=10
        )
        with pytest.raises(ResourceLimitExceeded):
            store.select_bars_1m(
                build_id=BUILD,
                dataset_id=DATASET,
                symbol="EURUSD",
                start_ns=NOON,
                end_ns=NOON + 10 * NS_PER_MINUTE,
                limit=10,
            )


class TestBuildIsolation:
    @covers("P06-G-04", "P06-S-08", kind="model_only")
    def test_dropping_a_build_removes_its_bars_too(self, s):
        s.insert_canonical(ticks(3), block_size=3)
        other = [t._replace(build_id="build-2") for t in ticks(2)]
        s.insert_canonical(other, block_size=2)

        s.drop_build(build_id=BUILD)
        assert s.all_canonical(build_id=BUILD) == []
        assert s.all_bars_1m(build_id=BUILD) == {}, (
            "a materialized view does not follow its source's deletes, so the "
            "state rows have to be removed explicitly"
        )
        assert len(s.all_canonical(build_id="build-2")) == 2, "the other build is untouched"
        assert s.all_bars_1m(build_id="build-2")

    @covers("P06-S-01", kind="model_only")
    def test_reads_are_scoped_to_one_build(self, s):
        s.insert_canonical(ticks(3), block_size=3)
        s.insert_canonical([t._replace(build_id="build-2") for t in ticks(3)], block_size=3)
        rows = s.select_canonical(
            build_id=BUILD,
            dataset_id=DATASET,
            symbol="EURUSD",
            start_ns=NOON,
            end_ns=NOON + NS_PER_MINUTE,
            limit=100,
        )
        assert {r.build_id for r in rows} == {BUILD}


class TestTTL:
    @covers("P06-O-01", kind="model_only")
    def test_ttl_only_removes_rows_when_it_runs(self, s):
        old = [t._replace(event_date=date(2026, 1, 1)) for t in ticks(3)]
        s.insert_canonical(old, block_size=3)
        assert len(s.all_canonical(build_id=BUILD)) == 3, (
            "rows past their deadline are still present until a merge runs: TTL is "
            "asynchronous and the deadline is not an SLA"
        )
        removed = s.apply_ttl(table=CANONICAL_TABLE, cutoff=date(2026, 6, 1))
        assert removed == 3
        assert s.all_canonical(build_id=BUILD) == []

    @covers("P06-O-03", kind="model_only")
    def test_ttl_keeps_rows_on_the_cutoff_date(self, s):
        cutoff = date(2026, 10, 30)
        s.insert_canonical(ticks(2), block_size=2)
        assert s.apply_ttl(table=CANONICAL_TABLE, cutoff=cutoff) == 0
        assert s.apply_ttl(table=CANONICAL_TABLE, cutoff=cutoff + timedelta(days=1)) == 2

    @covers("P06-O-04", kind="model_only")
    def test_ttl_on_canonical_does_not_touch_the_bars(self, s):
        s.insert_canonical(ticks(3), block_size=3)
        s.apply_ttl(table=CANONICAL_TABLE, cutoff=date(2026, 11, 1))
        assert s.all_canonical(build_id=BUILD) == []
        assert s.all_bars_1m(build_id=BUILD), (
            "bars can outlive their source, which means the full raw audit is no "
            "longer possible and the manifest has to say so"
        )

    def test_ttl_on_an_unmodelled_table_is_an_explicit_error(self, s):
        with pytest.raises(StoreError) as exc:
            s.apply_ttl(table="md_analytics.bars_1m_state", cutoff=date(2026, 1, 1))
        assert exc.value.code == "TTL_TABLE_UNKNOWN"
