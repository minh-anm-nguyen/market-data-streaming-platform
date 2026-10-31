"""Canonical staging: build ids, dirty attempts, retries (group G)."""

from __future__ import annotations

import pytest

from _coverage import covers
from md_analytics.canonical_builder import (
    BuildState,
    build_canonical,
    compute_build_id,
    policy_digest,
    rebuild_fresh,
    select_canonical_rows,
    staging_build_id,
)
from md_analytics.errors import BuildDirtyError, ContractError
from md_analytics.fixtures import GOLDEN_BASE_NS, CorpusBuilder
from md_analytics.raw_auditor import audit_raw
from md_analytics.schema_preflight import expected_raw_digest
from md_analytics.store.model import ModelStore

BASE = GOLDEN_BASE_NS
SCHEMA = expected_raw_digest()


@pytest.fixture
def s() -> ModelStore:
    store = ModelStore()
    store.setup()
    return store


def corpus_of(n: int, dataset_id="DS-B1"):
    b = CorpusBuilder(dataset_id)
    for i in range(n):
        b.tick("EURUSD", i + 1, BASE + i * 1_000_000_000, 100_000 + i)
    return b.seal()


def audited(corpus):
    return audit_raw(corpus.raw_rows, manifest=corpus.manifest, cut=corpus.cut)


class TestBuildId:
    @covers("P06-G-06")
    def test_the_build_id_is_content_addressed(self):
        args = dict(manifest_sha256="m", cut_digest="c", schema_sha256="s")
        assert compute_build_id(**args) == compute_build_id(**args)

    @covers("P06-G-06")
    @pytest.mark.parametrize(
        "change",
        [
            {"manifest_sha256": "m2"},
            {"cut_digest": "c2"},
            {"schema_sha256": "s2"},
            {"builder_version": "canonical-builder.2"},
            {"canonical_policy_digest": "p2"},
        ],
    )
    def test_any_input_change_renames_the_build(self, change):
        base = dict(manifest_sha256="m", cut_digest="c", schema_sha256="s")
        assert compute_build_id(**base) != compute_build_id(**{**base, **change})

    @covers("P06-G-06")
    def test_the_policy_digest_covers_the_tie_break_rule(self):
        from md_analytics.canonical_builder import CANONICAL_POLICY

        altered = {**CANONICAL_POLICY, "order": "(event_time_ns)"}
        assert policy_digest(altered) != policy_digest()

    @covers("P06-G-04")
    def test_attempts_are_physically_distinct(self):
        assert staging_build_id("abc", 1) != staging_build_id("abc", 2)
        with pytest.raises(ValueError):
            staging_build_id("abc", 0)


class TestRowSelection:
    @covers("P06-E-01")
    def test_physical_duplicates_collapse_to_one_row(self):
        b = CorpusBuilder("DS-SEL")
        row = b.tick("EURUSD", 1, BASE, 100_000)
        b.duplicate(row, times=4)
        corpus = b.seal()
        rows = select_canonical_rows(
            corpus.raw_rows, audit=audited(corpus), physical_build_id="bid"
        )
        assert len(rows) == 1

    @covers("P06-A-01", "P06-H-03")
    def test_quotes_never_reach_the_canonical_table(self):
        b = CorpusBuilder("DS-SEL2")
        b.tick("EURUSD", 1, BASE, 100_000)
        b.quote("EURUSD", 2, BASE + 1, 999_999, 999_999)
        corpus = b.seal()
        rows = select_canonical_rows(
            corpus.raw_rows, audit=audited(corpus), physical_build_id="bid"
        )
        assert len(rows) == 1 and rows[0].price_ticks == 100_000

    @covers("P06-B-03", "P06-D-08")
    def test_a_wrong_event_date_is_not_laundered_into_canonical(self):
        from datetime import date

        from md_analytics.rows import canonical_from_raw

        b = CorpusBuilder("DS-SEL3")
        row = b.tick("EURUSD", 1, BASE, 100, event_date_override=date(2026, 1, 1))
        with pytest.raises(ContractError) as exc:
            canonical_from_raw(row, build_id="bid")
        assert exc.value.code == "EVENT_DATE_MISMATCH"

    def test_a_disagreement_between_audit_and_rows_is_an_error(self):
        corpus = corpus_of(3)
        audit = audited(corpus)
        with pytest.raises(ContractError) as exc:
            select_canonical_rows(corpus.raw_rows[:1], audit=audit, physical_build_id="bid")
        assert exc.value.code == "BUILDER_INPUT_DISAGREEMENT"

    def test_rows_are_emitted_in_sorting_key_order(self):
        corpus = corpus_of(20)
        rows = select_canonical_rows(
            corpus.raw_rows, audit=audited(corpus), physical_build_id="bid"
        )
        keys = [(r.dataset_id, r.symbol, r.event_time_ns, r.sequence) for r in rows]
        assert keys == sorted(keys)


class TestStaging:
    @covers("P06-G-14")
    def test_a_build_succeeds_and_reports_its_rows(self, s):
        corpus = corpus_of(10)
        result = build_canonical(
            s, corpus.raw_rows, audit=audited(corpus), cut=corpus.cut, schema_sha256=SCHEMA
        )
        assert result.state is BuildState.CANONICAL_BUILDING
        assert result.rows_written == result.identities == 10
        assert result.physical_build_id.endswith(".a1")

    @covers("P06-G-14")
    @pytest.mark.parametrize("block_size", [1, 3, 10, 1_000])
    def test_block_size_does_not_change_the_logical_output(self, block_size):
        corpus = corpus_of(10)
        store = ModelStore()
        store.setup()
        result = build_canonical(
            store,
            corpus.raw_rows,
            audit=audited(corpus),
            cut=corpus.cut,
            schema_sha256=SCHEMA,
            block_size=block_size,
        )
        bars = store.all_bars_1m(build_id=result.physical_build_id)
        assert sum(b.tick_count for b in bars.values()) == 10

    @covers("P06-G-08")
    def test_a_failed_audit_refuses_to_build(self, s):
        b = CorpusBuilder("DS-CONF")
        row = b.tick("EURUSD", 1, BASE, 100)
        b.conflict(row, price_ticks=200)
        corpus = b.seal()
        with pytest.raises(BuildDirtyError) as exc:
            build_canonical(
                s, corpus.raw_rows, audit=audited(corpus), cut=corpus.cut, schema_sha256=SCHEMA
            )
        assert exc.value.code == "AUDIT_NOT_PASSED"

    @covers("P06-G-04", "P06-G-03")
    def test_staging_into_a_non_empty_attempt_is_refused(self, s):
        corpus = corpus_of(5)
        audit = audited(corpus)
        build_canonical(
            s, corpus.raw_rows, audit=audit, cut=corpus.cut, schema_sha256=SCHEMA
        )
        with pytest.raises(BuildDirtyError) as exc:
            build_canonical(
                s, corpus.raw_rows, audit=audit, cut=corpus.cut, schema_sha256=SCHEMA
            )
        assert exc.value.code == "STAGING_NOT_EMPTY"


class TestDirtyOutcomes:
    @covers("P06-G-02", "P06-I-07")
    def test_an_unknown_ack_marks_the_build_dirty_and_never_retries(self, s):
        corpus = corpus_of(6)
        s.fault_ack_unknown_after_insert = 1
        with pytest.raises(BuildDirtyError) as exc:
            build_canonical(
                s,
                corpus.raw_rows,
                audit=audited(corpus),
                cut=corpus.cut,
                schema_sha256=SCHEMA,
                block_size=2,
            )
        assert exc.value.code == "ACK_UNKNOWN_BUILD_DIRTY"
        assert "ack_unknown_after_insert" in s.fired

        physical = exc.value.context["physical_build_id"]
        assert len(s.all_canonical(build_id=physical)) == 6, "the rows did land"
        bars = s.all_bars_1m(build_id=physical)
        assert sum(b.tick_count for b in bars.values()) == 6, (
            "a blind resubmit would make this 12 and nothing could subtract it"
        )

    @covers("P06-G-01")
    def test_a_partial_insert_marks_the_build_dirty(self, s):
        corpus = corpus_of(10)
        s.fault_stop_after_blocks = 2
        with pytest.raises(BuildDirtyError) as exc:
            build_canonical(
                s,
                corpus.raw_rows,
                audit=audited(corpus),
                cut=corpus.cut,
                schema_sha256=SCHEMA,
                block_size=2,
            )
        assert exc.value.code == "PARTIAL_INSERT_BUILD_DIRTY"
        assert len(s.all_canonical(build_id=exc.value.context["physical_build_id"])) == 4


class TestRebuild:
    @covers("P06-G-02", "P06-G-04")
    def test_a_fresh_rebuild_discards_the_dirty_attempt_and_its_bars(self, s):
        corpus = corpus_of(6)
        audit = audited(corpus)
        s.fault_ack_unknown_after_insert = 1
        with pytest.raises(BuildDirtyError) as exc:
            build_canonical(
                s, corpus.raw_rows, audit=audit, cut=corpus.cut, schema_sha256=SCHEMA, block_size=2
            )
        dirty = exc.value.context["physical_build_id"]

        result = rebuild_fresh(
            s,
            corpus.raw_rows,
            audit=audit,
            cut=corpus.cut,
            schema_sha256=SCHEMA,
            dirty_attempt=1,
            block_size=2,
        )
        assert result.attempt == 2
        assert result.physical_build_id.endswith(".a2")
        assert s.all_canonical(build_id=dirty) == []
        assert s.all_bars_1m(build_id=dirty) == {}, (
            "the dirty attempt's aggregate states must go too, or the rebuilt bars "
            "would be double counted at read time"
        )
        bars = s.all_bars_1m(build_id=result.physical_build_id)
        assert sum(b.tick_count for b in bars.values()) == 6

    @covers("P06-G-13")
    def test_two_symbols_do_not_carry_state_into_each_other(self, s):
        b = CorpusBuilder("DS-TWO")
        for i in range(10):
            b.tick("EURUSD" if i % 2 == 0 else "GBPUSD", i + 1, BASE + i, 100_000 + i)
        corpus = b.seal()
        result = build_canonical(
            s,
            corpus.raw_rows,
            audit=audited(corpus),
            cut=corpus.cut,
            schema_sha256=SCHEMA,
            block_size=3,
        )
        rows = s.all_canonical(build_id=result.physical_build_id)
        eur = {r.sequence for r in rows if r.symbol == "EURUSD"}
        gbp = {r.sequence for r in rows if r.symbol == "GBPUSD"}
        assert eur == {1, 3, 5, 7, 9} and gbp == {2, 4, 6, 8, 10}
        assert not eur & gbp
