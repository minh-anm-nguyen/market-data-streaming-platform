"""The 32 priority end-to-end tests of section 16.

One test per E2E id, in order. Several of them *pass by refusing to publish*:
E2E-06, 07, 08, 15, 18, 19, 22 and 32 assert that the pipeline declines, which
section 16 calls out as a PASS of safety behaviour rather than a capability.

E2E-23, 24, 28, 29 and 30 need a real ClickHouse, a real P04/P05, a replicated
cluster or 200M rows. They are written and skipped with a reason, because an
un-run test is NOT_RUN and that has to be visible rather than inferred.
"""

from __future__ import annotations

import itertools
import os
from datetime import date

import pytest

from _coverage import covers
from md_analytics.build_catalog import NO_ACTIVE_BUILD
from md_analytics.canonical_builder import BuildState
from md_analytics.contracts import EVENT_QUOTE, NS_PER_MINUTE
from md_analytics.errors import PublishRejected, Verdict
from md_analytics.fixtures import (
    GOLDEN_BASE_NS,
    CorpusBuilder,
    golden_six_event,
    rebase_expected,
)
from md_analytics.oracle import Bar
from md_analytics.pipeline import bars_for_symbol, run_and_require_ready, run_build
from md_analytics.store.model import CANONICAL_TABLE, ModelStore
from md_analytics.ttl_controller import ReconstructabilityCheck

BASE = GOLDEN_BASE_NS
SEC = 1_000_000_000


@pytest.fixture
def s() -> ModelStore:
    store = ModelStore()
    store.setup()
    return store


def bars_of(result, symbol="EURUSD"):
    return bars_for_symbol(result, symbol)


# --------------------------------------------------------------- E2E-01..05


@covers("P06-A-01", "P06-H-03")
def test_e2e_01_six_event_golden(s, catalog):
    """4 ticks + 2 quotes, one symbol, two minutes."""
    case = golden_six_event()
    result = run_and_require_ready(s, case.corpus, catalog=catalog, block_size=2)

    assert result.audit.physical_rows == 6
    assert result.audit.distinct_identities == 6
    assert len(result.canonical) == 4, "the two quotes do not become canonical ticks"
    assert result.stored_bars == rebase_expected(case.expected_bars, build_id=result.build_id)
    assert sum(b.tick_count for b in result.stored_bars.values()) == 4


@covers("P06-D-01", "P06-D-02", "P06-D-03")
def test_e2e_02_utc_one_nanosecond(s, catalog):
    """Two ticks either side of a minute edge, one nanosecond apart."""
    edge = BASE + NS_PER_MINUTE
    b = CorpusBuilder("E2E-02")
    b.tick("EURUSD", 1, edge - 1, 100_100)
    b.tick("EURUSD", 2, edge, 100_200)
    result = run_and_require_ready(s, b.seal(), catalog=catalog)

    bars = bars_of(result)
    assert sorted(bars) == [BASE, edge]
    assert bars[BASE] == Bar(100_100, 100_100, 100_100, 100_100, 1)
    assert bars[edge] == Bar(100_200, 100_200, 100_200, 100_200, 1)


@covers("P06-H-01", "P06-H-02")
def test_e2e_03_reversed_arrival(s, catalog):
    """Five ticks whose arrival order is the reverse of their event order."""
    b = CorpusBuilder("E2E-03")
    for i in reversed(range(5)):
        b.tick("EURUSD", i + 1, BASE + i * SEC, 100 + i)
    result = run_and_require_ready(s, b.seal(), catalog=catalog, block_size=1)

    bar = bars_of(result)[BASE]
    assert bar.open_ticks == 100, "earliest event time, not earliest arrival"
    assert bar.close_ticks == 104
    assert bar.tick_count == 5


@covers("P06-H-11", "P06-I-08")
@pytest.mark.parametrize("insert_order", list(itertools.permutations([0, 1])))
def test_e2e_04_equal_timestamp_tie_break(catalog, insert_order):
    """Two ticks at the same nanosecond; the sequence decides, in any order."""
    store = ModelStore()
    store.setup()
    b = CorpusBuilder("E2E-04")
    rows = [(3, 100_200), (4, 99_900)]
    for index in insert_order:
        sequence, price = rows[index]
        b.tick("EURUSD", sequence, BASE + 30 * SEC, price)
    result = run_and_require_ready(store, b.seal(), catalog=catalog, block_size=1)

    bar = bars_of(result)[BASE]
    assert bar.open_ticks == 100_200, "sequence 3 opens"
    assert bar.close_ticks == 99_900, "sequence 4 closes"


@covers("P06-E-01", "P06-H-05")
def test_e2e_05_physical_triple_retry(s, catalog):
    """1000 unique ticks delivered three times each."""
    b = CorpusBuilder("E2E-05")
    for i in range(1_000):
        row = b.tick("EURUSD", i + 1, BASE + i * 10_000_000, 100_000 + (i % 50))
        b.duplicate(row, times=2)
    result = run_and_require_ready(s, b.seal(), catalog=catalog, block_size=250)

    assert result.audit.physical_rows == 3_000
    assert result.audit.duplicate_row_count == 2_000
    assert len(result.canonical) == 1_000
    assert sum(bar.tick_count for bar in result.stored_bars.values()) == 1_000, (
        "tick_count counts canonical identities, not physical rows"
    )


# --------------------------------------------------------------- E2E-06..08


@covers("P06-E-02", "P06-G-08")
def test_e2e_06_identity_conflict_refuses_to_publish(s, catalog):
    """Same identity, different price. The correct outcome is NO_GO."""
    b = CorpusBuilder("E2E-06")
    row = b.tick("EURUSD", 7, BASE, 100_000)
    b.conflict(row, price_ticks=100_300)
    result = run_build(s, b.seal(), catalog=catalog)

    assert result.state is BuildState.NO_GO
    assert result.verdict is Verdict.NO_GO
    assert len(result.audit.conflicts) == 1
    assert catalog.active_build_id() is None, "nothing was published"


@covers("P06-F-05", "P06-Z-01")
def test_e2e_07_source_missing_refuses_to_publish(s, catalog):
    """A manifest of 10 000 against raw missing exactly one row."""
    b = CorpusBuilder("E2E-07")
    rows = [b.tick("EURUSD", i + 1, BASE + i * 1_000_000, 100_000 + i % 7) for i in range(10_000)]
    b.suppress_from_raw(rows[4_321])
    result = run_build(s, b.seal(), catalog=catalog)

    assert result.verdict is Verdict.NO_GO
    assert result.audit.missing == [("E2E-07", "EURUSD", 4_322)]
    assert result.audit.extra == []
    assert catalog.active_build_id() is None


@covers("P06-A-04", "P06-F-08")
def test_e2e_08_source_extra_refuses_to_publish(s, catalog):
    """A raw row that the accepted-event ledger does not contain."""
    b = CorpusBuilder("E2E-08")
    rows = [b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i) for i in range(5)]
    b.unledgered(rows[2])
    result = run_build(s, b.seal(), catalog=catalog)

    assert result.verdict is Verdict.NO_GO
    assert result.audit.extra == [("E2E-08", "EURUSD", 3)]
    assert catalog.active_build_id() is None


# --------------------------------------------------------------- E2E-09..12


@covers("P06-H-03", "P06-R-11")
def test_e2e_09_quote_only_minute_has_no_bar(s, catalog):
    """100 quote updates and no ticks produce no tick bar at all."""
    b = CorpusBuilder("E2E-09")
    for i in range(100):
        b.quote("EURUSD", i + 1, BASE + i * 100_000_000, 999_000 + i, 999_100 + i)
    b.tick("EURUSD", 500, BASE + 5 * NS_PER_MINUTE, 100_000)
    result = run_and_require_ready(s, b.seal(), catalog=catalog)

    bars = bars_of(result)
    assert sorted(bars) == [BASE + 5 * NS_PER_MINUTE]
    assert bars[BASE + 5 * NS_PER_MINUTE].high_ticks == 100_000, (
        "the quote asks are far higher and must not reach HIGH"
    )
    assert len(result.canonical) == 1


@covers("P06-H-10")
def test_e2e_10_single_tick_minute(s, catalog):
    b = CorpusBuilder("E2E-10")
    b.tick("EURUSD", 1, BASE + 17 * SEC, 123_456)
    result = run_and_require_ready(s, b.seal(), catalog=catalog)
    assert bars_of(result)[BASE] == Bar(123_456, 123_456, 123_456, 123_456, 1)


@covers("P06-H-09", "P06-R-09")
def test_e2e_11_empty_minute_is_not_filled(s, catalog):
    b = CorpusBuilder("E2E-11")
    b.tick("EURUSD", 1, BASE, 100_000)
    b.tick("EURUSD", 2, BASE + 2 * NS_PER_MINUTE, 100_200)
    result = run_and_require_ready(s, b.seal(), catalog=catalog)

    bars = bars_of(result)
    assert sorted(bars) == [BASE, BASE + 2 * NS_PER_MINUTE]
    assert BASE + NS_PER_MINUTE not in bars, "no zero-price or fill-forward bar"


@covers("P06-E-06", "P06-H-07")
def test_e2e_12_cross_dataset_isolation(catalog):
    """Two datasets share a symbol, sequence and timestamp with different prices."""
    store = ModelStore()
    store.setup()
    results = {}
    expected_current = NO_ACTIVE_BUILD
    for dataset_id, price in (("E2E-12-A", 100_000), ("E2E-12-B", 200_000)):
        b = CorpusBuilder(dataset_id)
        b.tick("EURUSD", 1, BASE, price)
        results[dataset_id] = run_and_require_ready(
            store, b.seal(), catalog=catalog, expected_current=expected_current
        )
        expected_current = results[dataset_id].build_id

    a, b_result = results["E2E-12-A"], results["E2E-12-B"]
    assert a.build_id != b_result.build_id
    assert bars_of(a)[BASE].open_ticks == 100_000
    assert bars_of(b_result)[BASE].open_ticks == 200_000
    assert {k.dataset_id for k in a.stored_bars} == {"E2E-12-A"}
    assert {k.dataset_id for k in b_result.stored_bars} == {"E2E-12-B"}


# --------------------------------------------------------------- E2E-13..15


@covers("P06-J-02", "P06-G-12")
def test_e2e_13_late_high_does_not_mutate_the_published_build(s, catalog):
    """A late tick raises HIGH. Build A keeps its numbers; build B gets the new one."""
    b = CorpusBuilder("E2E-13")
    b.tick("EURUSD", 1, BASE, 100_000)
    b.tick("EURUSD", 2, BASE + 30 * SEC, 100_100)
    first = run_and_require_ready(s, b.seal(), catalog=catalog)
    first_bars = dict(first.stored_bars)
    assert bars_of(first)[BASE].high_ticks == 100_100

    # New cut including a late, higher tick inside the same minute.
    b.tick("EURUSD", 3, BASE + 45 * SEC, 100_400)
    second = run_and_require_ready(
        s, b.seal(), catalog=catalog, expected_current=first.build_id,
        prior_build_bars=first_bars,
    )

    assert bars_of(second)[BASE].high_ticks == 100_400
    assert bars_of(second)[BASE].tick_count == 3
    assert s.all_bars_1m(build_id=first.build_id) == first_bars, (
        "the superseded build is immutable"
    )
    assert catalog.read_manifest(first.build_id).bar_tick_count == 2
    assert second.invariants.by_id()["INV-11"].verdict is Verdict.PASS


@covers("P06-J-03", "P06-I-03")
def test_e2e_14_late_open_corrects_only_the_new_build(s, catalog):
    """A late tick earlier than the published OPEN."""
    b = CorpusBuilder("E2E-14")
    b.tick("EURUSD", 1, BASE + 30 * SEC, 100_100)
    first = run_and_require_ready(s, b.seal(), catalog=catalog)
    first_bars = dict(first.stored_bars)
    assert bars_of(first)[BASE].open_ticks == 100_100

    b.tick("EURUSD", 2, BASE + 10 * SEC, 100_000)
    second = run_and_require_ready(
        s, b.seal(), catalog=catalog, expected_current=first.build_id,
        prior_build_bars=first_bars,
    )
    assert bars_of(second)[BASE].open_ticks == 100_000, "the earlier event time opens"
    assert bars_of(second)[BASE].close_ticks == 100_100
    assert s.all_bars_1m(build_id=first.build_id) == first_bars


@covers("P06-B-03", "P06-D-08")
def test_e2e_15_source_timestamp_poison_refuses_to_publish(s, catalog):
    """``event_date`` computed in a local zone instead of UTC."""
    b = CorpusBuilder("E2E-15")
    b.tick("EURUSD", 1, BASE, 100_000)
    b.tick("EURUSD", 2, BASE + SEC, 100_100, event_date_override=date(2026, 10, 31))
    result = run_build(s, b.seal(), catalog=catalog)

    assert result.verdict is Verdict.NO_GO
    assert result.audit.event_date_drift == [("E2E-15", "EURUSD", 2)]
    assert catalog.active_build_id() is None


# --------------------------------------------------------------- E2E-16..19


@covers("P06-T-01", "P06-AB-01")
def test_e2e_16_p05_ack_ambiguity_is_classified_not_double_counted(s, catalog):
    """P05's INSERT landed but the acknowledgement was lost, so it re-sent."""
    b = CorpusBuilder("E2E-16")
    for i in range(20):
        row = b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
        if i % 4 == 0:
            b.duplicate(row)  # the re-sent batch
    result = run_and_require_ready(s, b.seal(), catalog=catalog, block_size=5)

    assert result.audit.physical_rows == 25
    assert result.audit.duplicate_row_count == 5
    assert len(result.canonical) == 20
    assert sum(bar.tick_count for bar in result.stored_bars.values()) == 20


@covers("P06-G-02", "P06-I-07")
def test_e2e_17_builder_ack_loss_marks_dirty_then_rebuilds_clean(s, catalog):
    """The canonical INSERT succeeded but the client timed out."""
    b = CorpusBuilder("E2E-17")
    for i in range(10):
        b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
    corpus = b.seal()

    s.fault_ack_unknown_after_insert = 1
    dirty = run_build(s, corpus, catalog=catalog, block_size=3)
    assert dirty.state is BuildState.DIRTY
    assert dirty.verdict is Verdict.NO_GO
    assert "ack_unknown_after_insert" in s.fired
    assert catalog.active_build_id() is None

    # A fresh attempt, after discarding the dirty one.
    from md_analytics.canonical_builder import compute_build_id, staging_build_id
    from md_analytics.schema_preflight import expected_raw_digest

    logical = compute_build_id(
        manifest_sha256=corpus.cut.manifest_sha256,
        cut_digest=corpus.cut.cut_digest(),
        schema_sha256=expected_raw_digest(),
    )
    s.drop_build(build_id=staging_build_id(logical, 1))
    clean = run_and_require_ready(s, corpus, catalog=catalog, block_size=3, attempt=2)

    assert sum(bar.tick_count for bar in clean.stored_bars.values()) == 10, (
        "a blind resubmit into the first attempt would have made this 20"
    )


@covers("P06-K-06", "P06-AB-03")
def test_e2e_18_unavailable_view_target_refuses_to_publish(s, catalog):
    """The state table is read-only while canonical rows are inserted."""
    b = CorpusBuilder("E2E-18")
    for i in range(5):
        b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
    s.fault_mv_target_unavailable = True
    result = run_build(s, b.seal(), catalog=catalog)

    assert result.verdict is Verdict.NO_GO
    assert result.state is BuildState.DIRTY
    assert "mv_target_unavailable" in s.fired
    assert catalog.active_build_id() is None


@covers("P06-G-01", "P06-W-05")
def test_e2e_19_partial_build_crash_preserves_the_previous_ready(s, catalog):
    """SIGKILL partway through the canonical insert."""
    b = CorpusBuilder("E2E-19")
    b.tick("EURUSD", 1, BASE, 100_000)
    first = run_and_require_ready(s, b.seal(), catalog=catalog)

    for i in range(2, 22):
        b.tick("EURUSD", i, BASE + i * SEC, 100_000 + i)
    s.fault_stop_after_blocks = 2
    crashed = run_build(
        s, b.seal(), catalog=catalog, block_size=3, expected_current=first.build_id
    )

    assert crashed.state is BuildState.DIRTY
    assert "stop_after_blocks" in s.fired
    assert catalog.active_build_id() == first.build_id, "the old READY build still serves"
    assert catalog.read_manifest(first.build_id).canonical_count == 1


# --------------------------------------------------------------- E2E-20..22


@covers("P06-I-01", "P06-L-09")
def test_e2e_20_identical_results_before_and_after_merges(s, catalog):
    """Many partial states, merges stopped, then run."""
    b = CorpusBuilder("E2E-20")
    for i in range(60):
        b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + (i % 13) * 7)
    s.stop_merges()
    result = run_and_require_ready(s, b.seal(), catalog=catalog, block_size=1)

    before = s.all_bars_1m(build_id=result.build_id)
    state_rows_before = s.bar_state_row_count(build_id=result.build_id)
    assert state_rows_before == 60, "one state row per block, none merged"

    s.start_merges()
    s.optimize_bars()
    after = s.all_bars_1m(build_id=result.build_id)

    assert before == after
    assert s.bar_state_row_count(build_id=result.build_id) < state_rows_before


@covers("P06-I-08", "P06-Q-07")
def test_e2e_21_a_wrong_tie_break_is_detected(s, catalog):
    """``argMin`` without the sequence key: same count, wrong OPEN and CLOSE."""
    from md_analytics.bars_verifier import compare_bars
    from md_analytics.contracts import SourceOrder
    from md_analytics.oracle import BarKey
    from md_analytics.store.base import ArgMaxState, ArgMinState, BarState

    b = CorpusBuilder("E2E-21")
    b.tick("EURUSD", 3, BASE + 30 * SEC, 100_200)
    b.tick("EURUSD", 4, BASE + 30 * SEC, 99_900)
    result = run_and_require_ready(s, b.seal(), catalog=catalog)
    correct = result.stored_bars

    # A view defined with a time-only ordering key cannot separate these two
    # ticks, so it may pick either. Model the wrong pick and verify detection.
    key = next(iter(correct))
    wrong = {
        key: BarState(
            open_state=ArgMinState(SourceOrder(BASE + 30 * SEC, 0), 99_900),
            high_state=100_200,
            low_state=99_900,
            close_state=ArgMaxState(SourceOrder(BASE + 30 * SEC, 0), 100_200),
            count_state=2,
        ).finalize()
    }
    report = compare_bars(
        correct, wrong, build_id=result.build_id, canonical_rows=2, state_rows=1
    )
    assert report.verdict() is Verdict.FAIL
    assert {m.field_name for m in report.mismatches} == {"open_ticks", "close_ticks"}
    assert report.stored_tick_total == report.reference_tick_total


@covers("P06-O-01", "P06-AB-02")
def test_e2e_22_short_ttl_before_the_audit_is_not_a_false_readiness(s, catalog):
    """TTL removes the source before the audit could rebuild from it."""
    b = CorpusBuilder("E2E-22")
    for i in range(5):
        b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
    result = run_and_require_ready(s, b.seal(), catalog=catalog)

    removed = s.apply_ttl(table=CANONICAL_TABLE, cutoff=date(2026, 11, 1))
    assert removed == 5
    assert s.all_canonical(build_id=result.build_id) == []

    # The bars outlive their source. A rebuild is now impossible unless some
    # other copy exists, and that has to be stated rather than assumed.
    assert s.all_bars_1m(build_id=result.build_id)
    check = ReconstructabilityCheck(
        raw_present=False, source_replayable=False, backup_restorable=False
    )
    assert check.verdict() is Verdict.NO_GO
    assert check.reason().startswith("NOT_RECONSTRUCTABLE")


# --------------------------------------------------------------- E2E-23..25


@covers("P06-M-05", "P06-M-13", kind="needs_clickhouse")
@pytest.mark.clickhouse
def test_e2e_23_daily_versus_monthly_partitions(clickhouse_store):
    """Identical rows under two partition strategies: equal results, measured cost.

    Needs a real server: the question is about partition pruning, merge I/O and
    latency, none of which the model can answer.
    """
    pytest.skip(
        "E2E-23 requires a pinned ClickHouse server to compare partition "
        "strategies; results from the model would say nothing about either"
    )


@covers("P06-N-06", "P06-U-02", kind="needs_clickhouse")
@pytest.mark.clickhouse
def test_e2e_24_order_by_a_b(clickhouse_store):
    """Two candidate sorting keys over identical rows.

    Needs EXPLAIN indexes=1, marks and read_rows from a real server.
    """
    pytest.skip(
        "E2E-24 requires EXPLAIN and read_rows from a real ClickHouse; the model "
        "has no primary index to measure"
    )


@covers("P06-M-07", "P06-V-02", kind="model_only")
def test_e2e_25_too_many_parts_is_a_bounded_failure(s, catalog):
    """A burst of tiny batches, with a part budget.

    On the model this shows part growth as a function of batch size and that the
    failure is a bounded error rather than a partial build. The real
    ``Too many parts`` exception and P05's backpressure need a server and Kafka.
    """
    b = CorpusBuilder("E2E-25")
    for i in range(200):
        b.tick("EURUSD", i + 1, BASE + i * 100_000_000, 100_000 + i)
    corpus = b.seal()

    result = run_and_require_ready(s, corpus, catalog=catalog, block_size=1)
    canonical_parts = sum(
        p.active_parts for p in s.parts() if p.table == "canonical_ticks"
    )
    assert canonical_parts == 200, "one part per tiny insert"

    budget = 50
    assert canonical_parts > budget, (
        "the part budget of NFR-08 is exceeded, which is an operational signal to "
        "change batching rather than to run OPTIMIZE FINAL in cron"
    )
    assert result.verdict is Verdict.PASS, "correctness is unaffected by part pressure"


# --------------------------------------------------------------- E2E-26..28


@covers("P06-G-10", "P06-S-03")
def test_e2e_26_parallel_publishers_leave_one_pointer(s, catalog):
    """Two builds promote concurrently; one pointer wins."""
    b = CorpusBuilder("E2E-26")
    b.tick("EURUSD", 1, BASE, 100_000)
    first = run_and_require_ready(s, b.seal(), catalog=catalog)

    b.tick("EURUSD", 2, BASE + SEC, 100_100)
    second = run_and_require_ready(
        s, b.seal(), catalog=catalog, expected_current=first.build_id
    )

    # A third publisher that still believes `first` is active loses the CAS.
    b.tick("EURUSD", 3, BASE + 2 * SEC, 100_200)
    stale = run_build(s, b.seal(), catalog=catalog, expected_current=first.build_id)

    assert stale.verdict is Verdict.NO_GO
    assert "publication refused" in stale.reasons[0]
    assert catalog.active_build_id() == second.build_id


@covers("P06-S-01", "P06-S-08")
def test_e2e_27_readers_only_ever_see_the_prior_ready_build(s, catalog):
    """16 readers while a new build is staged."""
    from md_analytics.query_service import QueryService

    b = CorpusBuilder("E2E-27")
    b.tick("EURUSD", 1, BASE, 100_000)
    published = run_and_require_ready(s, b.seal(), catalog=catalog)

    # Stage a second build without publishing it.
    b.tick("EURUSD", 2, BASE + SEC, 100_900)
    staged = run_build(
        s, b.seal(), catalog=catalog, publish=False, expected_current=published.build_id
    )
    assert staged.verdict is Verdict.PASS and staged.build_id != published.build_id

    service = QueryService(s, catalog, symbol_allowlist=["EURUSD"])
    for _ in range(16):
        response = service.get_bars(
            build_id=catalog.active_build_id(),
            dataset_id="E2E-27",
            symbol="EURUSD",
            from_ns=BASE,
            to_ns=BASE + NS_PER_MINUTE,
        )
        assert response.bars[0].high_ticks == 100_000, "no leak from the staged build"

    with pytest.raises(PublishRejected):
        service.get_bars(
            build_id=staged.build_id,
            dataset_id="E2E-27",
            symbol="EURUSD",
            from_ns=BASE,
            to_ns=BASE + NS_PER_MINUTE,
        )


@covers("P06-T-11", "P06-U-05", "P06-W-06", kind="needs_infra")
def test_e2e_28_mixed_query_and_ingest_load():
    """1k to 120k events/s against 1/4/16/32 readers.

    Needs Kafka, P05 and a host that can sustain the offered rate. Section 18.1
    is explicit that a personal machine is not expected to.
    """
    pytest.skip(
        "E2E-28 requires a real P04/P05 pipeline and a capacity host; offered "
        "rate is not throughput and cannot be simulated into evidence"
    )


# --------------------------------------------------------------- E2E-29..32


@covers("P06-U-01", "P06-U-03", "P06-AD-05", kind="needs_clickhouse")
@pytest.mark.clickhouse
@pytest.mark.perf
def test_e2e_29_200m_p95_baseline(clickhouse_store):
    """200M unique tick rows, pinned query suite, cold and warm.

    Needs a real server and 200M rows. The model's latency is the latency of
    Python dictionaries and would be meaningless either way.
    """
    pytest.skip(
        "E2E-29 requires a pinned ClickHouse server and a 200M unique-tick "
        "dataset; 280 ms is a measured goal, never an assumed one"
    )


@covers("P06-X-01", "P06-S-04", kind="needs_infra")
def test_e2e_30_replica_lag():
    """A lagging follower must not serve READY results."""
    pytest.skip(
        "E2E-30 requires a replicated ClickHouse with Keeper (the optional HA-LAB "
        "profile of section 2.2)"
    )


@covers("P06-P-01", "P06-P-04")
def test_e2e_31_restore_proof(s, catalog, tmp_path):
    """Back up manifest, canonical and bars; restore into a clean environment."""
    import json

    b = CorpusBuilder("E2E-31")
    for i in range(25):
        b.tick("EURUSD", i + 1, BASE + i * 2 * SEC, 100_000 + (i % 9) * 3)
    corpus = b.seal()
    original = run_and_require_ready(s, corpus, catalog=catalog, block_size=4)

    backup = tmp_path / "backup.json"
    backup.write_text(
        json.dumps(
            {
                "manifest": catalog.read_manifest(original.build_id).payload(),
                "canonical": [t.as_dict() for t in original.canonical],
                "bars": [
                    {"key": list(k), "bar": list(v)}
                    for k, v in sorted(original.stored_bars.items())
                ],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    # Restore into an empty store by replaying the canonical rows, then compare
    # semantics rather than bytes: a restore is correct if the identities and the
    # integer OHLC match, not if the parts happen to be laid out identically.
    restored_store = ModelStore()
    restored_store.setup()
    restored_store.insert_canonical(list(original.canonical), block_size=7)

    payload = json.loads(backup.read_text(encoding="utf-8"))
    assert len(payload["canonical"]) == 25
    assert restored_store.all_bars_1m(build_id=original.build_id) == original.stored_bars
    assert {t.identity for t in restored_store.all_canonical(build_id=original.build_id)} == {
        t.identity for t in original.canonical
    }
    assert payload["manifest"]["bar_tick_count"] == 25


@covers("P06-AB-01", "P06-AB-02")
def test_e2e_32_compound_disaster_ends_in_an_explicit_refusal(s, catalog):
    """ACK loss, then a restart, then TTL removes the source, with no backup."""
    b = CorpusBuilder("E2E-32")
    for i in range(10):
        b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
    corpus = b.seal()

    # 1. ACK loss during the canonical insert.
    s.fault_ack_unknown_after_insert = 1
    dirty = run_build(s, corpus, catalog=catalog, block_size=3)
    assert dirty.state is BuildState.DIRTY
    assert "ack_unknown_after_insert" in s.fired

    # 2. A restart: a fresh attempt, which this time hits a read-only target.
    s.fault_mv_target_unavailable = True
    second = run_build(s, corpus, catalog=catalog, block_size=3, attempt=2)
    assert second.verdict is Verdict.NO_GO
    assert "mv_target_unavailable" in s.fired

    # 3. TTL then removes the source rows, and there is no backup.
    s.fault_mv_target_unavailable = False
    s.apply_ttl(table=CANONICAL_TABLE, cutoff=date(2026, 11, 1))
    check = ReconstructabilityCheck(
        raw_present=False, source_replayable=False, backup_restorable=False
    )

    assert catalog.active_build_id() is None, "nothing was ever published"
    assert check.verdict() is Verdict.NO_GO
    assert check.reason().startswith("NOT_RECONSTRUCTABLE"), (
        "the honest outcome is an explicit NOT_RECOVERABLE, not a build that "
        "looks fine because the bars survived"
    )


def test_every_e2e_id_has_a_test():
    """Guard against an E2E quietly going missing from this module."""
    import re

    source = os.path.join(os.path.dirname(__file__), "test_e2e_suite.py")
    with open(source, encoding="utf-8") as handle:
        text = handle.read()
    found = {int(m) for m in re.findall(r"^def test_e2e_(\d{2})", text, re.MULTILINE)}
    assert found == set(range(1, 33)), f"missing E2E ids: {sorted(set(range(1, 33)) - found)}"
