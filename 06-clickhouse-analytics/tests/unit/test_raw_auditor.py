"""Raw reconciliation: duplicates, conflicts, missing, extra (groups A, E, F)."""

from __future__ import annotations

from datetime import date

import pytest

from _coverage import covers
from md_analytics.contracts import EVENT_QUOTE, NS_PER_MINUTE, SourceIdentity
from md_analytics.errors import Verdict
from md_analytics.fixtures import GOLDEN_BASE_NS, CorpusBuilder, golden_six_event
from md_analytics.raw_auditor import audit_raw

BASE = GOLDEN_BASE_NS


def audit(corpus):
    return audit_raw(corpus.raw_rows, manifest=corpus.manifest, cut=corpus.cut)


class TestCleanCorpus:
    @covers("P06-A-02")
    def test_the_golden_corpus_reconciles_exactly(self, golden):
        report = audit(golden.corpus)
        assert report.verdict() is Verdict.PASS
        assert report.physical_rows == 6
        assert report.distinct_identities == 6
        assert len(report.clean_tick_identities) == 4, "two of the six are quotes"
        assert report.missing == [] and report.extra == []

    @covers("P06-A-02")
    def test_interleaved_tick_and_quote_sequences_are_not_a_gap(self, golden):
        report = audit(golden.corpus)
        sequences = sorted(i.sequence for i in report.clean_tick_identities)
        assert sequences == [1, 3, 4, 6], (
            "the tick subsequence has holes because the quotes used 2 and 5; the "
            "expected set comes from the ledger, so this is not a missing-row report"
        )
        assert report.missing == []


class TestPhysicalDuplicates:
    @covers("P06-E-01", "P06-H-05")
    def test_identical_retries_are_classified_not_rejected(self):
        b = CorpusBuilder("DS-DUP")
        row = b.tick("EURUSD", 1, BASE, 100_000)
        b.duplicate(row, times=2)
        report = audit(b.seal())

        assert report.verdict() is Verdict.PASS
        assert report.physical_rows == 3
        assert report.distinct_identities == 1
        assert report.physical_duplicates == {("DS-DUP", "EURUSD", 1): 3}
        assert report.duplicate_row_count == 2
        assert len(report.clean_tick_identities) == 1

    @covers("P06-E-08")
    def test_a_retry_far_later_is_still_one_identity(self):
        b = CorpusBuilder("DS-DUP2")
        row = b.tick("EURUSD", 1, BASE, 100_000)
        b.duplicate(row)  # arbitrary later offset; no reliance on a dedup window
        report = audit(b.seal())
        assert len(report.clean_tick_identities) == 1
        assert report.duplicate_row_count == 1

    @covers("P06-E-13")
    def test_a_duplicate_with_a_wrong_event_date_is_caught_not_absorbed(self):
        b = CorpusBuilder("DS-DUP3")
        row = b.tick("EURUSD", 1, BASE, 100_000)
        b.conflict(row, event_date=date(2026, 10, 29))
        report = audit(b.seal())
        assert report.event_date_drift, "the audit is global by identity, across partitions"
        assert report.verdict() is Verdict.NO_GO


class TestConflicts:
    @covers("P06-E-02")
    def test_same_identity_different_price_is_a_conflict(self):
        b = CorpusBuilder("DS-C1")
        row = b.tick("EURUSD", 7, BASE, 100)
        b.conflict(row, price_ticks=101)
        report = audit(b.seal())

        assert report.verdict() is Verdict.NO_GO
        assert len(report.conflicts) == 1
        conflict = report.conflicts[0]
        assert conflict.identity == ("DS-C1", "EURUSD", 7)
        assert len(conflict.digests) == 2, "two distinct semantic digests"
        assert SourceIdentity(*conflict.identity) not in report.clean_tick_identities

    @covers("P06-E-03")
    def test_same_identity_one_nanosecond_apart_is_a_conflict(self):
        b = CorpusBuilder("DS-C2")
        row = b.tick("EURUSD", 7, BASE, 100)
        b.conflict(row, event_time_ns=BASE + 1)
        report = audit(b.seal())
        assert len(report.conflicts) == 1
        assert report.verdict() is Verdict.NO_GO

    @covers("P06-E-04")
    def test_same_identity_as_tick_and_quote_is_a_conflict_not_two_events(self):
        b = CorpusBuilder("DS-C3")
        row = b.tick("EURUSD", 7, BASE, 100)
        b.conflict(
            row,
            event_kind=EVENT_QUOTE,
            price_ticks=None,
            bid_ticks=99,
            ask_ticks=101,
        )
        report = audit(b.seal())
        assert len(report.conflicts) == 1
        assert report.verdict() is Verdict.NO_GO

    @covers("P06-E-02", "P06-G-08")
    def test_a_conflict_is_never_resolved_by_last_write_wins(self):
        b = CorpusBuilder("DS-C4")
        row = b.tick("EURUSD", 7, BASE, 100)
        b.conflict(row, price_ticks=100_300)
        report = audit(b.seal())
        assert report.clean_tick_identities == frozenset(), (
            "the higher price must not become HIGH just because it was written last"
        )


class TestDigestIntegrity:
    @covers("P06-E-05", "P06-B-05")
    def test_a_corrupted_digest_is_rejected(self):
        b = CorpusBuilder("DS-D1")
        b.tick("EURUSD", 1, BASE, 100, digest_override="f" * 64)
        report = audit(b.seal())
        assert report.digest_mismatches == [("DS-D1", "EURUSD", 1)]
        assert report.verdict() is Verdict.NO_GO

    @covers("P06-B-05")
    def test_a_malformed_digest_is_an_invalid_row(self):
        b = CorpusBuilder("DS-D2")
        b.tick("EURUSD", 1, BASE, 100, digest_override="abc")
        report = audit(b.seal())
        assert [r.code for r in report.invalid_rows] == ["DIGEST_BAD_LENGTH"]
        assert report.verdict() is Verdict.NO_GO

    @covers("P06-E-10")
    def test_a_colliding_digest_still_loses_to_the_typed_comparison(self):
        # Both rows claim the same digest but carry different prices. Grouping is
        # keyed on the *recomputed* digest, so the two are still distinguished.
        b = CorpusBuilder("DS-D3")
        forged = "a" * 64
        b.tick("EURUSD", 1, BASE, 100, digest_override=forged)
        b.tick("EURUSD", 1, BASE, 200, digest_override=forged, accepted=False)
        report = audit(b.seal())
        assert len(report.digest_mismatches) == 2
        assert report.verdict() is Verdict.NO_GO

    @covers("P06-E-09")
    def test_a_row_disagreeing_with_the_manifest_is_reported(self):
        b = CorpusBuilder("DS-D4")
        b.tick("EURUSD", 1, BASE, 100)
        corpus = b.seal()
        # Rewrite the ledger's expectation for that identity, re-sealing the
        # manifest so it is internally consistent: the raw row is now a
        # legitimate-looking row that disagrees with what the source published.
        from md_analytics.snapshot_cutter import AcceptedEvent, SourceManifest

        tampered = SourceManifest(
            dataset_id=corpus.manifest.dataset_id,
            accepted=(
                AcceptedEvent(
                    identity=SourceIdentity("DS-D4", "EURUSD", 1),
                    event_kind="tick",
                    payload_digest="b" * 64,
                ),
            ),
            rejected=(),
            generator_version=corpus.manifest.generator_version,
        )
        tampered = SourceManifest(
            dataset_id=tampered.dataset_id,
            accepted=tampered.accepted,
            rejected=tampered.rejected,
            declared_sha256=tampered.content_digest(),
            generator_version=tampered.generator_version,
        )
        report = audit_raw(corpus.raw_rows, manifest=tampered, cut=corpus.cut)
        assert report.manifest_digest_disagreements == [("DS-D4", "EURUSD", 1)]
        assert report.verdict() is Verdict.NO_GO


class TestCompleteness:
    @covers("P06-F-05", "P06-F-03")
    def test_a_missing_accepted_identity_is_reported_exactly(self):
        b = CorpusBuilder("DS-M1")
        rows = [b.tick("EURUSD", i + 1, BASE + i, 100 + i) for i in range(10)]
        b.suppress_from_raw(rows[4])
        report = audit(b.seal())
        assert report.missing == [("DS-M1", "EURUSD", 5)]
        assert report.extra == []
        assert report.verdict() is Verdict.NO_GO

    @covers("P06-F-08", "P06-A-04")
    def test_a_row_outside_the_accepted_ledger_is_extra(self):
        b = CorpusBuilder("DS-M2")
        rows = [b.tick("EURUSD", i + 1, BASE + i, 100 + i) for i in range(3)]
        b.unledgered(rows[1])
        report = audit(b.seal())
        assert report.extra == [("DS-M2", "EURUSD", 2)]
        assert report.missing == []
        assert report.verdict() is Verdict.NO_GO

    @covers("P06-A-04")
    def test_source_rejected_events_are_not_expected_in_raw(self):
        b = CorpusBuilder("DS-M3")
        for i in range(97):
            b.tick("EURUSD", i + 1, BASE + i, 100)
        for seq in (98, 99, 100):
            b.reject_at_source(SourceIdentity("DS-M3", "EURUSD", seq), "stale_timestamp")
        report = audit(b.seal())
        assert report.verdict() is Verdict.PASS
        assert len(report.clean_tick_identities) == 97, (
            "the expected set is the accepted ledger, not everything P01 generated"
        )

    @covers("P06-E-12")
    def test_a_row_past_the_cut_is_out_of_scope_not_extra(self):
        b = CorpusBuilder("DS-M4")
        b.tick("EURUSD", 1, BASE, 100)
        corpus = b.seal()
        b.after_cut("EURUSD", 2, BASE + NS_PER_MINUTE, 999)
        # Audit the rows as they stand now against the cut sealed before them.
        report = audit_raw(b.rows(), manifest=corpus.manifest, cut=corpus.cut)

        assert report.rows_out_of_cut == 1
        assert report.extra == [] and report.missing == []
        assert report.verdict() is Verdict.PASS

    @covers("P06-E-06")
    def test_another_dataset_rows_are_ignored_not_counted(self):
        b = CorpusBuilder("DS-A")
        b.tick("EURUSD", 1, BASE, 100)
        corpus = b.seal()
        other = CorpusBuilder("DS-B")
        other.tick("EURUSD", 1, BASE, 200)
        foreign = other.seal().raw_rows

        report = audit_raw(
            corpus.raw_rows + foreign, manifest=corpus.manifest, cut=corpus.cut
        )
        assert report.verdict() is Verdict.PASS
        assert report.rows_in_scope == 1
        assert report.physical_rows == 2


class TestScaleAndKind:
    @covers("P06-C-03", "P06-C-13")
    def test_two_scales_for_one_symbol_stops_the_build(self):
        b = CorpusBuilder("DS-S1", price_scale=100_000)
        b.tick("EURUSD", 1, BASE, 100_000)
        b.tick("EURUSD", 2, BASE + 1, 1_000, price_scale=1_000)
        report = audit(b.seal())
        assert report.scale_conflicts == {("DS-S1", "EURUSD"): (1_000, 100_000)}
        assert report.clean_tick_identities == frozenset(), "no mixed-unit bar may be built"
        assert report.verdict() is Verdict.NO_GO

    @covers("P06-C-11")
    def test_different_symbols_may_use_different_scales(self):
        b = CorpusBuilder("DS-S2", price_scale=100_000)
        b.tick("EURUSD", 1, BASE, 100_000)
        b.tick("USDJPY", 1, BASE, 151_230, price_scale=1_000)
        report = audit(b.seal())
        assert report.scale_conflicts == {}
        assert report.verdict() is Verdict.PASS
        assert len(report.clean_tick_identities) == 2

    @covers("P06-C-02")
    def test_a_zero_scale_row_cannot_even_be_constructed(self):
        from md_analytics.errors import ContractError

        b = CorpusBuilder("DS-S3")
        with pytest.raises(ContractError) as exc:
            # Rejected while building the row, which is the earliest possible
            # point: the contract layer owns this rule, so no later stage has to.
            b.tick("EURUSD", 1, BASE, 100, price_scale=0)
        assert exc.value.code == "PRICE_SCALE_INVALID"


class TestReportShape:
    def test_counters_cover_every_category(self, golden):
        counters = audit(golden.corpus).counters()
        for key in (
            "raw_physical_rows",
            "raw_duplicate_id_count",
            "raw_conflict_id_count",
            "missing_ids",
            "extra_ids",
            "clean_tick_identities",
        ):
            assert key in counters

    def test_as_dict_truncates_samples_but_not_counts(self):
        b = CorpusBuilder("DS-R1")
        rows = [b.tick("EURUSD", i + 1, BASE + i, 100) for i in range(50)]
        for row in rows:
            b.unledgered(row)
        report = audit(b.seal())
        payload = report.as_dict(sample_limit=5)
        assert payload["counters"]["extra_ids"] == 50
        assert len(payload["samples"]["extra"]) == 5

    def test_a_clean_report_states_why_it_passed(self, golden):
        report = audit(golden.corpus)
        assert report.reasons and "complete and consistent" in report.reasons[0]
