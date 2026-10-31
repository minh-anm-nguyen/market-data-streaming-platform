"""Client read path: scope, alignment, limits, build pinning (groups R, S, Y)."""

from __future__ import annotations

import pytest

from _coverage import covers
from md_analytics.build_catalog import NO_ACTIVE_BUILD
from md_analytics.canonical_builder import BuildState
from md_analytics.contracts import NS_PER_MINUTE
from md_analytics.errors import PublishRejected, QueryRejected
from md_analytics.fixtures import GOLDEN_BASE_NS, CorpusBuilder
from md_analytics.pipeline import run_and_require_ready
from md_analytics.query_service import QueryLimits, QueryService
from md_analytics.store.model import ModelStore

BASE = GOLDEN_BASE_NS


@pytest.fixture
def served(catalog):
    """A READY build with 10 one-minute bars for EURUSD, plus a second symbol."""
    store = ModelStore()
    store.setup()
    b = CorpusBuilder("DS-Q")
    for i in range(10):
        b.tick("EURUSD", i + 1, BASE + i * NS_PER_MINUTE, 100_000 + i)
    for i in range(3):
        b.tick("GBPUSD", 100 + i, BASE + i * NS_PER_MINUTE, 126_000 + i)
    corpus = b.seal()
    result = run_and_require_ready(store, corpus, catalog=catalog, block_size=3)
    service = QueryService(
        store,
        catalog,
        symbol_allowlist=["EURUSD", "GBPUSD"],
        dataset_allowlist=["DS-Q"],
    )
    return service, result.build_id, store


class TestBuildPinning:
    @covers("P06-R-06", "P06-S-08")
    def test_a_ready_build_serves(self, served):
        service, build_id, _ = served
        response = service.get_bars(
            build_id=build_id,
            dataset_id="DS-Q",
            symbol="EURUSD",
            from_ns=BASE,
            to_ns=BASE + 10 * NS_PER_MINUTE,
        )
        assert len(response.bars) == 10
        assert response.bars[0].open_ticks == 100_000

    @covers("P06-R-06", "P06-S-08")
    def test_an_unknown_build_is_refused_with_no_fallback_to_raw(self, served):
        service, _, _ = served
        with pytest.raises(PublishRejected) as exc:
            service.get_bars(
                build_id="not-a-build",
                dataset_id="DS-Q",
                symbol="EURUSD",
                from_ns=BASE,
                to_ns=BASE + NS_PER_MINUTE,
            )
        assert exc.value.code == "MANIFEST_NOT_FOUND"

    @covers("P06-S-01", "P06-G-12")
    def test_a_dirty_build_is_never_served(self, served, catalog):
        service, build_id, _ = served
        manifest = catalog.read_manifest(build_id)
        from dataclasses import replace

        catalog.record_manifest(
            replace(manifest, build_id="dirty-build", state=BuildState.DIRTY)
        )
        with pytest.raises(PublishRejected) as exc:
            service.get_bars(
                build_id="dirty-build",
                dataset_id="DS-Q",
                symbol="EURUSD",
                from_ns=BASE,
                to_ns=BASE + NS_PER_MINUTE,
            )
        assert exc.value.code == "BUILD_NOT_READY"

    def test_a_missing_build_id_is_refused(self, served):
        service, _, _ = served
        with pytest.raises(QueryRejected) as exc:
            service.get_bars(
                build_id=None,
                dataset_id="DS-Q",
                symbol="EURUSD",
                from_ns=BASE,
                to_ns=BASE + NS_PER_MINUTE,
            )
        assert exc.value.code == "BUILD_SCOPE_REQUIRED"


class TestScope:
    @covers("P06-R-05", "P06-Y-02")
    def test_dataset_scope_is_mandatory(self, served):
        service, build_id, _ = served
        with pytest.raises(QueryRejected) as exc:
            service.get_bars(
                build_id=build_id,
                dataset_id=None,
                symbol="EURUSD",
                from_ns=BASE,
                to_ns=BASE + NS_PER_MINUTE,
            )
        assert exc.value.code == "DATASET_SCOPE_REQUIRED"

    @covers("P06-R-05")
    def test_symbol_scope_is_mandatory(self, served):
        service, build_id, _ = served
        with pytest.raises(QueryRejected) as exc:
            service.get_bars(
                build_id=build_id,
                dataset_id="DS-Q",
                symbol="",
                from_ns=BASE,
                to_ns=BASE + NS_PER_MINUTE,
            )
        assert exc.value.code == "SYMBOL_SCOPE_REQUIRED"

    @covers("P06-Y-02")
    def test_an_unauthorized_dataset_is_refused(self, served):
        service, build_id, _ = served
        with pytest.raises(QueryRejected) as exc:
            service.get_bars(
                build_id=build_id,
                dataset_id="DS-OTHER",
                symbol="EURUSD",
                from_ns=BASE,
                to_ns=BASE + NS_PER_MINUTE,
            )
        assert exc.value.code == "DATASET_NOT_AUTHORIZED"

    @covers("P06-I-13", "P06-Y-03")
    def test_a_symbol_outside_the_allowlist_is_refused(self, served):
        service, build_id, _ = served
        with pytest.raises(QueryRejected) as exc:
            service.get_bars(
                build_id=build_id,
                dataset_id="DS-Q",
                symbol="XXXYYY",
                from_ns=BASE,
                to_ns=BASE + NS_PER_MINUTE,
            )
        assert exc.value.code == "SYMBOL_NOT_ALLOWED"

    @covers("P06-H-07", "P06-R-05")
    def test_one_symbol_never_returns_another_symbols_bars(self, served):
        service, build_id, _ = served
        response = service.get_bars(
            build_id=build_id,
            dataset_id="DS-Q",
            symbol="GBPUSD",
            from_ns=BASE,
            to_ns=BASE + 10 * NS_PER_MINUTE,
        )
        assert len(response.bars) == 3
        assert {bar.symbol for bar in response.bars} == {"GBPUSD"}

    @covers("P06-Y-04")
    def test_a_sql_shaped_symbol_is_refused_not_escaped(self, served):
        service, build_id, _ = served
        with pytest.raises(QueryRejected):
            service.get_bars(
                build_id=build_id,
                dataset_id="DS-Q",
                symbol="EURUSD' OR 1=1 --",
                from_ns=BASE,
                to_ns=BASE + NS_PER_MINUTE,
            )


class TestRangeAndLimits:
    @covers("P06-R-09")
    def test_an_unaligned_bar_range_is_refused(self, served):
        service, build_id, _ = served
        with pytest.raises(QueryRejected) as exc:
            service.get_bars(
                build_id=build_id,
                dataset_id="DS-Q",
                symbol="EURUSD",
                from_ns=BASE + 1,
                to_ns=BASE + NS_PER_MINUTE,
            )
        assert exc.value.code == "RANGE_NOT_ALIGNED"

    @covers("P06-D-12")
    def test_adjacent_windows_cover_each_bar_exactly_once(self, served):
        service, build_id, _ = served

        def window(start_minute, end_minute):
            return service.get_bars(
                build_id=build_id,
                dataset_id="DS-Q",
                symbol="EURUSD",
                from_ns=BASE + start_minute * NS_PER_MINUTE,
                to_ns=BASE + end_minute * NS_PER_MINUTE,
            ).bars

        first = window(0, 5)
        second = window(5, 10)
        starts = [b.bar_start_ns for b in first] + [b.bar_start_ns for b in second]
        assert len(starts) == len(set(starts)) == 10

    @covers("P06-R-03")
    def test_a_window_wider_than_the_limit_is_refused(self, served):
        service, build_id, store = served
        narrow = QueryService(
            store,
            service.catalog,
            limits=QueryLimits(max_window_minutes=5),
            symbol_allowlist=["EURUSD"],
        )
        with pytest.raises(QueryRejected) as exc:
            narrow.get_bars(
                build_id=build_id,
                dataset_id="DS-Q",
                symbol="EURUSD",
                from_ns=BASE,
                to_ns=BASE + 10 * NS_PER_MINUTE,
            )
        assert exc.value.code == "RANGE_TOO_WIDE"

    @covers("P06-R-04")
    def test_truncation_is_reported_rather_than_silent(self, served):
        service, build_id, _ = served
        response = service.get_bars(
            build_id=build_id,
            dataset_id="DS-Q",
            symbol="EURUSD",
            from_ns=BASE,
            to_ns=BASE + 10 * NS_PER_MINUTE,
            limit=4,
        )
        assert len(response.bars) == 4
        assert response.truncated is True, (
            "a caller must be able to tell a truncated page from the end of the data"
        )

    @covers("P06-R-04")
    def test_a_limit_above_the_ceiling_is_clamped_not_honoured(self, served):
        service, build_id, store = served
        capped = QueryService(
            store,
            service.catalog,
            limits=QueryLimits(max_bars_returned=3),
            symbol_allowlist=["EURUSD"],
        )
        response = capped.get_bars(
            build_id=build_id,
            dataset_id="DS-Q",
            symbol="EURUSD",
            from_ns=BASE,
            to_ns=BASE + 10 * NS_PER_MINUTE,
            limit=10_000,
        )
        assert len(response.bars) == 3 and response.truncated

    @pytest.mark.parametrize("limit", [0, -1, "5", 1.5, True])
    def test_an_invalid_limit_is_refused(self, served, limit):
        service, build_id, _ = served
        with pytest.raises(QueryRejected):
            service.get_bars(
                build_id=build_id,
                dataset_id="DS-Q",
                symbol="EURUSD",
                from_ns=BASE,
                to_ns=BASE + NS_PER_MINUTE,
                limit=limit,
            )


class TestWireForm:
    @covers("P06-D-13", "P06-I-11")
    def test_nanoseconds_and_counts_are_emitted_as_strings(self, served):
        service, build_id, _ = served
        row = service.get_bars(
            build_id=build_id,
            dataset_id="DS-Q",
            symbol="EURUSD",
            from_ns=BASE,
            to_ns=BASE + NS_PER_MINUTE,
        ).bars[0].as_dict()
        assert isinstance(row["bar_start_ns"], str)
        assert isinstance(row["bar_end_ns"], str)
        assert isinstance(row["tick_count"], str)
        assert int(row["bar_start_ns"]) == BASE
        assert isinstance(row["open_ticks"], int), "prices stay integers"

    @covers("P06-H-06")
    def test_the_wire_form_has_no_volume_field(self, served):
        service, build_id, _ = served
        row = service.get_bars(
            build_id=build_id,
            dataset_id="DS-Q",
            symbol="EURUSD",
            from_ns=BASE,
            to_ns=BASE + NS_PER_MINUTE,
        ).bars[0].as_dict()
        assert "volume" not in row and "size" not in row
        assert "tick_count" in row

    @covers("P06-D-13")
    def test_string_nanosecond_bounds_are_accepted_exactly(self, served):
        service, build_id, _ = served
        response = service.get_bars(
            build_id=build_id,
            dataset_id="DS-Q",
            symbol="EURUSD",
            from_ns=str(BASE),
            to_ns=str(BASE + NS_PER_MINUTE),
        )
        assert response.from_ns == str(BASE)
        assert len(response.bars) == 1

    def test_the_response_states_the_bar_interval(self, served):
        service, build_id, _ = served
        payload = service.get_bars(
            build_id=build_id,
            dataset_id="DS-Q",
            symbol="EURUSD",
            from_ns=BASE,
            to_ns=BASE + NS_PER_MINUTE,
        ).as_dict()
        assert payload["bar_interval_ns"] == str(NS_PER_MINUTE)


class TestCanonicalRange:
    @covers("P06-R-02")
    def test_tick_reads_allow_an_unaligned_window(self, served):
        service, build_id, _ = served
        payload = service.get_canonical_range(
            build_id=build_id,
            dataset_id="DS-Q",
            symbol="EURUSD",
            from_ns=BASE + 1,
            to_ns=BASE + 3 * NS_PER_MINUTE,
        )
        assert [int(t["sequence"]) for t in payload["ticks"]] == [2, 3]

    @covers("P06-D-12")
    def test_tick_windows_are_half_open(self, served):
        service, build_id, _ = served
        payload = service.get_canonical_range(
            build_id=build_id,
            dataset_id="DS-Q",
            symbol="EURUSD",
            from_ns=BASE,
            to_ns=BASE + NS_PER_MINUTE,
        )
        assert [int(t["event_time_ns"]) for t in payload["ticks"]] == [BASE]

    @covers("P06-I-02")
    def test_the_tick_read_never_exposes_an_aggregate_state(self, served):
        service, build_id, _ = served
        payload = service.get_canonical_range(
            build_id=build_id,
            dataset_id="DS-Q",
            symbol="EURUSD",
            from_ns=BASE,
            to_ns=BASE + NS_PER_MINUTE,
        )
        assert set(payload["ticks"][0]) == {
            "event_time_ns",
            "sequence",
            "price_ticks",
            "price_scale",
        }
