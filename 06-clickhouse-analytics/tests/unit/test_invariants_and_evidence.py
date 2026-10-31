"""INV-01..14, metrics labels, alert rules and evidence honesty (groups Z, AD, AA).

The evidence tests are the ones that matter most here. Everything else in this
repo can be correct and still worthless if a ``PASS`` nobody can reproduce
counts as a pass, so the downgrade rules of section 14.2 are asserted directly.
"""

from __future__ import annotations

import json

import pytest

from _coverage import covers
from md_analytics.errors import EvidenceClass, Verdict
from md_analytics.evidence import EvidenceLedger, EvidenceRecord
from md_analytics.fixtures import GOLDEN_BASE_NS, CorpusBuilder
from md_analytics.invariants import check_bar_interval_partition, check_build_invariants
from md_analytics.metrics import (
    MetricLabelError,
    MetricRegistry,
    counters_to_alert_inputs,
    evaluate_alerts,
    release_gate_verdict,
)
from md_analytics.oracle import BarKey
from md_analytics.pipeline import run_build
from md_analytics.store.model import ModelStore

BASE = GOLDEN_BASE_NS


def complete_record(**overrides) -> EvidenceRecord:
    kwargs = dict(
        case_id="P06-H-01",
        priority="P0",
        verdict=Verdict.PASS,
        backend="model",
        evidence_class=str(EvidenceClass.MODEL),
        dataset_manifest_sha="m" * 64,
        raw_table_ddl_sha="r" * 64,
        canonical_ddl_sha="c" * 64,
        sql_bank_digest="q" * 64,
        config_sha="g" * 64,
    )
    kwargs.update(overrides)
    return EvidenceRecord(**kwargs)


class TestBuildInvariants:
    @pytest.fixture
    def passing(self, catalog):
        store = ModelStore()
        store.setup()
        b = CorpusBuilder("DS-INV")
        for i in range(12):
            b.tick("EURUSD", i + 1, BASE + i * 10_000_000_000, 100_000 + i)
        result = run_build(store, b.seal(), catalog=catalog, block_size=5)
        return result

    @covers("P06-AD-03")
    def test_a_clean_build_satisfies_every_evaluated_invariant(self, passing):
        report = passing.invariants
        assert report.failures() == []
        by_id = report.by_id()
        for inv in ("INV-01", "INV-02", "INV-03", "INV-04", "INV-05", "INV-06", "INV-08"):
            assert by_id[inv].verdict is Verdict.PASS, inv

    @covers("P06-AD-04", "P06-Z-01")
    def test_unevaluated_invariants_are_inconclusive_not_passing(self, passing):
        by_id = passing.invariants.by_id()
        assert by_id["INV-12"].verdict is Verdict.INCONCLUSIVE
        assert by_id["INV-14"].verdict is Verdict.INCONCLUSIVE
        assert passing.invariants.verdict() is Verdict.INCONCLUSIVE, (
            "the invariant report as a whole is not PASS while anything is unproven"
        )

    @covers("P06-AD-04")
    def test_supplying_the_missing_evidence_turns_them_into_passes(self, catalog):
        store = ModelStore()
        store.setup()
        b = CorpusBuilder("DS-INV2")
        b.tick("EURUSD", 1, BASE, 100_000)
        result = run_build(
            store,
            b.seal(),
            catalog=catalog,
            retention_ok=True,
            query_bounds_tested=True,
            prior_build_bars={},
        )
        assert result.invariants.verdict() is Verdict.PASS
        assert result.invariants.unproven() == []

    @covers("P06-I-10")
    def test_a_doubled_canonical_row_fails_inv_04_and_inv_06(self, catalog):
        store = ModelStore()
        store.setup()
        b = CorpusBuilder("DS-INV3")
        for i in range(3):
            b.tick("EURUSD", i + 1, BASE + i, 100 + i)
        corpus = b.seal()
        result = run_build(store, corpus, catalog=catalog, publish=False)
        canonical = list(result.canonical)

        # Re-insert the same canonical rows: this is the hazard the builder
        # prevents, injected here so the invariant checker can be seen catching it.
        store.insert_canonical(canonical, block_size=3)
        report = check_build_invariants(
            audit=result.audit,
            bars=result.bars,
            canonical=canonical,
            stored_bars=store.all_bars_1m(build_id=result.build_id),
            identity_counts=store.canonical_identity_counts(build_id=result.build_id),
            accepted_tick_identities=result.audit.clean_tick_identities,
            published=False,
        )
        failed = {r.invariant for r in report.failures()}
        assert {"INV-04", "INV-05", "INV-06"} <= failed

    @covers("P06-I-06", "P06-D-01")
    def test_an_unaligned_bar_start_is_detected(self):
        bad = BarKey("b", "d", "EURUSD", 100_000, BASE + 1)
        assert check_bar_interval_partition([bad]) == [bad]
        assert check_bar_interval_partition([BarKey("b", "d", "EURUSD", 100_000, BASE)]) == []

    @covers("P06-AD-03")
    def test_a_published_build_evaluates_inv_10_non_vacuously(self, passing):
        detail = passing.invariants.by_id()["INV-10"].detail
        assert "audit=PASS" in detail and "bars=PASS" in detail


class TestMetricLabels:
    @covers("P06-Z-05")
    def test_only_allowlisted_label_keys_are_accepted(self):
        registry = MetricRegistry()
        registry.increment("rows", table="canonical_ticks", status="ok")
        assert registry.counter("rows", table="canonical_ticks", status="ok") == 1

    @covers("P06-Z-05", "P06-I-13")
    @pytest.mark.parametrize("label", ["event_id", "build_id", "symbol", "dataset_id"])
    def test_high_cardinality_labels_are_refused(self, label):
        registry = MetricRegistry()
        with pytest.raises(MetricLabelError):
            registry.increment("rows", **{label: "anything"})

    @covers("P06-Z-05")
    def test_an_oversized_label_value_is_refused(self):
        registry = MetricRegistry()
        with pytest.raises(MetricLabelError):
            registry.increment("rows", table="x" * 100)

    @covers("P06-Z-02")
    def test_an_unset_gauge_reads_as_unobserved_not_zero(self):
        registry = MetricRegistry()
        assert registry.gauge("merge_backlog_age_s", table="bars_1m_state") is None
        registry.set_gauge("merge_backlog_age_s", 0.0, table="bars_1m_state")
        assert registry.gauge("merge_backlog_age_s", table="bars_1m_state") == 0.0

    def test_the_snapshot_renders_labels_deterministically(self):
        registry = MetricRegistry()
        registry.increment("rows", table="t", status="ok")
        snapshot = registry.snapshot()
        assert snapshot["counters"] == {"rows{status=ok,table=t}": 1}


class TestAlertRules:
    @covers("P06-Z-03")
    def test_a_blocking_rule_with_no_observation_is_not_satisfied(self):
        evaluations = evaluate_alerts({})
        blocking = [e for e in evaluations if e.rule.blocking]
        assert blocking and all(e.unobserved and e.firing for e in blocking)
        assert release_gate_verdict(evaluations) is Verdict.INCONCLUSIVE, (
            "silence from the metric that is supposed to prove zero conflicts "
            "says nothing about conflicts"
        )

    @covers("P06-Z-03")
    def test_all_blocking_rules_at_zero_pass_the_gate(self):
        observed = {
            "raw_conflicts": 0,
            "canonical_duplicate_ids": 0,
            "bars_wrong_count": 0,
            "build_ready_without_proof": 0,
            "missing_ids": 0,
            "extra_ids": 0,
        }
        assert release_gate_verdict(evaluate_alerts(observed)) is Verdict.PASS

    @covers("P06-Z-04")
    def test_any_conflict_blocks_the_gate(self):
        observed = {
            "raw_conflicts": 1,
            "canonical_duplicate_ids": 0,
            "bars_wrong_count": 0,
            "build_ready_without_proof": 0,
            "missing_ids": 0,
            "extra_ids": 0,
        }
        assert release_gate_verdict(evaluate_alerts(observed)) is Verdict.NO_GO

    @covers("P06-V-01")
    def test_a_non_blocking_threshold_fires_without_blocking_the_gate(self):
        observed = {
            "raw_conflicts": 0,
            "canonical_duplicate_ids": 0,
            "bars_wrong_count": 0,
            "build_ready_without_proof": 0,
            "missing_ids": 0,
            "extra_ids": 0,
            "active_parts_per_partition": 5_000,
        }
        evaluations = evaluate_alerts(observed)
        parts = next(e for e in evaluations if e.rule.metric == "active_parts_per_partition")
        assert parts.firing and not parts.rule.blocking
        assert release_gate_verdict(evaluations) is Verdict.PASS

    def test_pipeline_counters_map_onto_the_alert_names(self, catalog):
        store = ModelStore()
        store.setup()
        b = CorpusBuilder("DS-ALERT")
        b.tick("EURUSD", 1, BASE, 100)
        result = run_build(store, b.seal(), catalog=catalog)
        inputs = counters_to_alert_inputs(result.counters())
        assert inputs["raw_conflicts"] == 0.0
        assert inputs["bars_wrong_count"] == 0.0


class TestEvidenceHonesty:
    @covers("P06-AD-05")
    def test_a_complete_record_keeps_its_pass(self):
        assert complete_record().effective_verdict() is Verdict.PASS

    @covers("P06-AD-05", "P06-Z-01")
    @pytest.mark.parametrize(
        "field",
        [
            "dataset_manifest_sha",
            "raw_table_ddl_sha",
            "canonical_ddl_sha",
            "sql_bank_digest",
            "config_sha",
            "backend",
            "evidence_class",
        ],
    )
    def test_a_pass_missing_provenance_is_downgraded_to_inconclusive(self, field):
        record = complete_record(**{field: ""})
        assert record.completeness_gaps() == [field]
        assert record.effective_verdict() is Verdict.INCONCLUSIVE

    @covers("P06-AD-06")
    def test_a_claimed_fault_with_no_proof_is_invalid(self):
        record = complete_record(claims_fault_injection=True)
        assert record.effective_verdict() is Verdict.INVALID, (
            "a chaos test whose injector never fired has not tested anything"
        )

    @covers("P06-AD-06")
    def test_a_claimed_fault_with_proof_keeps_its_pass(self):
        record = complete_record(
            claims_fault_injection=True, fault_injection_proof=("ack_unknown_after_insert",)
        )
        assert record.effective_verdict() is Verdict.PASS

    @covers("P06-AD-07")
    def test_a_failure_is_never_upgraded_by_missing_evidence(self):
        record = complete_record(verdict=Verdict.FAIL, config_sha="")
        assert record.effective_verdict() is Verdict.FAIL

    @covers("P06-AD-08")
    def test_a_model_result_cannot_support_a_clickhouse_claim(self):
        assert not complete_record().supports_clickhouse_claim()

    @covers("P06-AD-08")
    def test_a_server_result_with_a_version_can(self):
        record = complete_record(
            evidence_class=str(EvidenceClass.REAL_CLICKHOUSE), server_version="26.3.1.1"
        )
        assert record.supports_clickhouse_claim()

    @covers("P06-AD-08")
    def test_a_server_result_without_a_version_cannot(self):
        record = complete_record(
            evidence_class=str(EvidenceClass.REAL_CLICKHOUSE), server_version=None
        )
        assert not record.supports_clickhouse_claim()


class TestEvidenceLedger:
    @covers("P06-AA-04")
    def test_an_empty_ledger_is_not_run_not_passing(self):
        assert EvidenceLedger().release_verdict() is Verdict.NOT_RUN

    @covers("P06-AA-04")
    def test_one_failure_blocks_the_release(self):
        ledger = EvidenceLedger()
        ledger.add(complete_record())
        ledger.add(complete_record(case_id="P06-H-02", verdict=Verdict.FAIL))
        assert ledger.release_verdict() is Verdict.FAIL
        assert len(ledger.failures()) == 1

    @covers("P06-AA-04")
    def test_a_safe_no_go_does_not_block_the_release(self):
        ledger = EvidenceLedger()
        ledger.add(complete_record())
        ledger.add(complete_record(case_id="P06-E-02", verdict=Verdict.NO_GO))
        assert ledger.release_verdict() is Verdict.PASS, (
            "for a case whose correct outcome is refusing to publish, NO_GO is the "
            "asserted behaviour"
        )

    @covers("P06-AA-04", "P06-AD-05")
    def test_an_inconclusive_result_blocks_the_release(self):
        ledger = EvidenceLedger()
        ledger.add(complete_record(config_sha=""))
        assert ledger.release_verdict() is Verdict.INCONCLUSIVE

    def test_the_ledger_exports_jsonl_with_effective_verdicts(self, tmp_path):
        ledger = EvidenceLedger()
        ledger.add(complete_record())
        ledger.add(complete_record(case_id="P06-H-02", config_sha=""))
        path = tmp_path / "evidence.jsonl"
        assert ledger.write_jsonl(path) == 2

        records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        assert records[0]["effective_verdict"] == "PASS"
        assert records[1]["effective_verdict"] == "INCONCLUSIVE"
        assert records[1]["completeness_gaps"] == ["config_sha"]

    def test_the_summary_counts_clickhouse_backed_results(self):
        ledger = EvidenceLedger()
        ledger.add(complete_record())
        ledger.add(
            complete_record(
                case_id="P06-H-02",
                evidence_class=str(EvidenceClass.REAL_CLICKHOUSE),
                server_version="26.3.1.1",
            )
        )
        summary = ledger.summary()
        assert summary["records"] == 2
        assert summary["clickhouse_backed"] == 1
