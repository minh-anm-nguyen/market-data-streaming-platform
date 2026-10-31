"""Generated specs stay in sync, and the CLI behaves (sections 16, 21.2).

Both generated artifacts -- ``specs/test-catalog.csv`` and the three JSON
schemas -- are derived from a single source: the design document for the
catalog, the code for the schemas. Derived files drift silently, so these tests
re-run the generators and compare.

The CLI tests then exercise the commands section 21.2 lists, including the one
that matters most on a new machine: ``selftest``, which is how section 8's
requirement to validate the instruments before using them gets met.
"""

from __future__ import annotations

import functools
import json
import subprocess
import sys
from pathlib import Path

import pytest

from p08_bench import MANIFEST_VERSION, REPORT_VERSION
from p08_bench.catalog import CaseCatalog
from p08_bench.cli import build_parser, main
from p08_bench.config import RunSection, WorkloadConfig

ROOT = Path(__file__).resolve().parents[2]
SPECS = ROOT / "specs"
SCRIPTS = ROOT / "scripts"


@functools.cache
def load_schema(name: str) -> dict:
    """Read a generated schema once per session."""
    return json.loads((SPECS / name).read_text(encoding="utf-8"))


def run_cli(*args: str, capsys) -> tuple[int, dict]:
    code = main(list(args))
    captured = capsys.readouterr()
    payload = json.loads(captured.out) if captured.out.strip() else {}
    return code, payload


class TestGeneratedSpecsAreCurrent:
    def test_the_schemas_match_the_code_that_enforces_them(self):
        # Hand-editing a schema next to a strict parser creates two sources of
        # truth, and the drift is silent.
        result = subprocess.run(
            [sys.executable, str(SCRIPTS / "build_schemas.py"), "--check"],
            capture_output=True, text=True, cwd=str(ROOT),
        )
        assert result.returncode == 0, result.stdout + result.stderr

    def test_the_catalog_matches_the_design_document(self):
        # Re-deriving the catalog asserts the section 17 inventory on the way:
        # 32 groups of 16, split 242/206/64, plus 48 E2E tests.
        out = SPECS / "test-catalog.csv"
        before = out.read_bytes()
        result = subprocess.run(
            [sys.executable, str(SCRIPTS / "build_catalog.py")],
            capture_output=True, text=True, cwd=str(ROOT),
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert out.read_bytes() == before, "test-catalog.csv is stale; re-run the generator"

    def test_all_three_schemas_exist_and_parse(self):
        for name in ("workload.schema.json", "metric.schema.json", "report.schema.json"):
            payload = json.loads((SPECS / name).read_text(encoding="utf-8"))
            assert payload["$schema"].startswith("https://json-schema.org/")
            assert payload["title"].startswith("P08")


class TestWorkloadSchema:
    def test_the_required_sections_match_the_parser(self):
        assert load_schema("workload.schema.json")["required"] == sorted(WorkloadConfig.SECTIONS)

    def test_each_section_requires_exactly_the_parser_keys(self):
        assert load_schema("workload.schema.json")["properties"]["run"]["required"] == sorted(RunSection.KEYS)

    def test_unknown_keys_are_rejected_by_the_schema_too(self):
        # The parser rejects them; the published contract has to say so.
        assert load_schema("workload.schema.json")["additionalProperties"] is False
        for section in load_schema("workload.schema.json")["properties"].values():
            assert section["additionalProperties"] is False

    def test_the_manifest_version_is_pinned(self):
        assert (
            load_schema("workload.schema.json")["properties"]["run"]["properties"]["manifest_version"]["const"]
            == MANIFEST_VERSION
        )

    def test_the_partition_count_says_per_topic(self):
        # NFR-02 is about 8 partitions per topic, not 8 in total.
        description = load_schema("workload.schema.json")["properties"]["kafka"]["properties"]["partitions_per_topic"]["description"]
        assert "per topic, not in total" in description

    def test_the_tick_quote_ratio_warns_about_units(self):
        description = load_schema("workload.schema.json")["properties"]["dataset"]["properties"]["tick_to_quote_ratio"]["description"]
        assert "INV-12" in description


class TestMetricSchema:
    def test_the_nine_stage_counters_are_required(self):
        assert len(load_schema("metric.schema.json")["properties"]["stage_counters"]["required"]) == 9

    def test_every_stage_counter_is_nullable(self):
        # null means not measured and must never be read as zero.
        for spec in load_schema("metric.schema.json")["properties"]["stage_counters"]["properties"].values():
            assert "null" in spec["type"]

    def test_each_stage_counter_carries_its_scope(self):
        properties = load_schema("metric.schema.json")["properties"]["stage_counters"]["properties"]
        assert "retry duplicates" in properties["kafka_physical_count"]["description"]

    def test_a_histogram_is_always_marked_an_estimate(self):
        histogram = load_schema("metric.schema.json")["properties"]["histograms"]["additionalProperties"]
        assert histogram["properties"]["estimate"]["const"] is True

    def test_the_forbidden_label_keys_are_published(self):
        forbidden = load_schema("metric.schema.json")["properties"]["label_rules"]["properties"]["forbidden_label_keys"]
        assert "event_id" in forbidden["items"]["enum"]


class TestReportSchema:
    def test_the_version_is_in_the_title(self):
        assert REPORT_VERSION in load_schema("report.schema.json")["title"]

    def test_the_template_field_names_are_required(self):
        # The report has to stay diffable against the document template.
        assert "status_dimensions" in load_schema("report.schema.json")["required"]
        assert "proof" in load_schema("report.schema.json")["required"]

    def test_measurements_are_nullable(self):
        p95 = load_schema("report.schema.json")["properties"]["results"]["properties"]["query_client_p95_ms"]
        assert "null" in p95["type"]

    def test_the_evidence_for_pass_rules_are_published(self):
        rules = load_schema("report.schema.json")["x-evidence-required-for-pass"]
        assert "results.per_tp_backlog_slopes" in rules["capacity"]
        assert "workload.query_family" in rules["latency"]
        assert "proof.oracle_diff_path" in rules["correctness"]

    def test_the_schema_says_what_it_cannot_express(self):
        assert "JSON Schema cannot express" in load_schema("report.schema.json")["description"]


class TestCliSelftest:
    def test_the_selftest_passes_on_this_machine(self, capsys):
        # Section 8: the instruments are validated against known inputs before
        # they are pointed at anything.
        code, payload = run_cli("selftest", capsys=capsys)
        assert code == 0
        assert payload["ok"] is True
        assert payload["failed"] == 0

    def test_it_checks_every_golden_fixture(self, capsys):
        _, payload = run_cli("selftest", capsys=capsys)
        fixtures = {check["fixture"] for check in payload["checks"]}
        assert {"FIX-002", "FIX-003", "FIX-004"} <= fixtures

    def test_it_pins_the_fix002_answer(self, capsys):
        _, payload = run_cli("selftest", capsys=capsys)
        p95 = next(c for c in payload["checks"] if "nearest-rank p95" in c["check"])
        assert p95["expected"] == 1000.0
        assert p95["observed"] == 1000.0

    def test_it_states_what_it_does_not_prove(self, capsys):
        _, payload = run_cli("selftest", capsys=capsys)
        assert "say nothing about any pipeline" in payload["note"]

    def test_it_reports_platform_limitations(self, capsys):
        _, payload = run_cli("selftest", capsys=capsys)
        assert isinstance(payload["environment_limitations"], list)


class TestCliCatalogAndGate:
    def test_the_catalog_command_counts_the_inventory(self, capsys):
        code, payload = run_cli("catalog", capsys=capsys)
        assert code == 0
        assert payload["total_in_catalog"] == 560
        assert payload["default_status"] == "NOT_RUN"

    def test_it_filters_by_priority(self, capsys):
        _, payload = run_cli("catalog", "--priority", "P0", capsys=capsys)
        assert payload["matched"] == 242

    def test_it_filters_by_group(self, capsys):
        _, payload = run_cli("catalog", "--group", "G01", capsys=capsys)
        assert payload["matched"] == 16

    def test_it_can_list_individual_cases(self, capsys):
        _, payload = run_cli("catalog", "--group", "G01", "--limit", "2", capsys=capsys)
        assert len(payload["cases"]) == 2
        assert payload["cases"][0]["id"] == "P08-G01-01"

    def test_the_gate_is_no_go_with_nothing_planned(self, capsys):
        # Case E2E-48 from the command line.
        code, payload = run_cli("gate", capsys=capsys)
        assert code == 1
        assert payload["gate"]["verdict"] == "NO_GO"

    def test_the_coverage_statement_is_not_contradictory(self, capsys):
        _, payload = run_cli("gate", capsys=capsys)
        statement = payload["gate"]["coverage_statement"]
        assert "242 never planned" in statement
        assert "0 planned but not run" in statement

    def test_a_plan_file_is_honoured(self, tmp_path: Path, capsys):
        catalog = CaseCatalog.load()
        plan = {case_id: "PASS" for case_id in catalog.p0_ids[:2]}
        path = tmp_path / "plan.json"
        path.write_text(json.dumps(plan), encoding="utf-8")
        _, payload = run_cli("gate", "--plan", str(path), capsys=capsys)
        assert payload["gate"]["satisfied_p0"] == 2
        assert payload["gate"]["verdict"] == "NO_GO"

    def test_a_missing_plan_file_is_a_config_error(self, tmp_path: Path, capsys):
        code, payload = run_cli("gate", "--plan", str(tmp_path / "nope.json"), capsys=capsys)
        assert code == 2
        assert payload["code"] == "config_error"

    def test_an_unknown_status_in_a_plan_is_refused(self, tmp_path: Path, capsys):
        path = tmp_path / "plan.json"
        path.write_text(json.dumps({"P08-G01-01": "PROBABLY_FINE"}), encoding="utf-8")
        code, payload = run_cli("gate", "--plan", str(path), capsys=capsys)
        assert code == 2
        assert "unknown case status" in payload["error"]


class TestCliCalibrateAndBench:
    def test_calibrate_reports_both_units(self, capsys):
        code, payload = run_cli("calibrate", "--rate", "1000", "--seconds", "0.1", capsys=capsys)
        assert code == 0
        calibration = payload["calibration"]
        assert calibration["achieved_ticks_per_s"]["unit"] == "ticks/s"
        assert calibration["achieved_events_per_s"]["unit"] == "events/s"

    def test_calibrate_states_what_it_does_not_measure(self, capsys):
        # Schedule time is simulated, so this is a test of the driver logic.
        _, payload = run_cli("calibrate", "--rate", "100", "--seconds", "0.1", capsys=capsys)
        assert "not wall-clock issue capacity" in payload["note"]

    def test_a_delayed_sink_makes_calibration_suspect(self, capsys):
        code, payload = run_cli(
            "calibrate", "--rate", "100", "--seconds", "0.1",
            "--sink-delay-ns", "1000000", capsys=capsys,
        )
        assert code == 1
        assert payload["calibration"]["sink_is_suspect"] is True

    def test_bench_without_a_source_refuses_rather_than_inventing_one(self, tmp_path: Path, capsys):
        # The shipped profiles require a sealed source (case G01-03).
        code, payload = run_cli(
            "bench", "--config", str(ROOT / "configs" / "local_small.toml"),
            "--out", str(tmp_path / "run"), capsys=capsys,
        )
        assert code == 1
        assert payload["overall"] == "INVALID"
        assert payload["stop_reason"] == "PREFLIGHT_FAILED"

    def test_bench_against_the_p01_sut_requires_a_source(self, capsys):
        code, payload = run_cli(
            "bench", "--config", str(ROOT / "configs" / "local_small.toml"),
            "--sut", "p01", capsys=capsys,
        )
        assert code == 2
        assert "requires --source" in payload["error"]

    def test_preflight_lists_the_required_stages_and_limitations(self, capsys):
        code, payload = run_cli(
            "preflight", "--config", str(ROOT / "configs" / "workload_120k.toml"), capsys=capsys
        )
        assert code == 1
        assert "P04_BROKER" in payload["required_stages"]
        assert any("query lane disabled" in text for text in payload["claim_limitations"])

    def test_verify_on_a_missing_directory_is_a_config_error(self, tmp_path: Path, capsys):
        code, payload = run_cli("verify", "--run-dir", str(tmp_path / "nope"), capsys=capsys)
        assert code == 2
        assert payload["code"] == "config_error"


class TestCliSurface:
    def test_every_documented_subcommand_exists(self):
        parser = build_parser()
        actions = [a for a in parser._actions if a.dest == "command"]
        assert actions, "no subcommand action found"
        expected = {"selftest", "preflight", "calibrate", "bench", "verify", "catalog", "gate"}
        assert expected <= set(actions[0].choices)

    def test_the_description_disclaims_any_performance_claim(self):
        description = build_parser().description or ""
        assert "does not assert" in description

    def test_compact_output_is_a_single_line(self, capsys):
        main(["--compact", "catalog"])
        out = capsys.readouterr().out
        assert out.count("\n") == 1
