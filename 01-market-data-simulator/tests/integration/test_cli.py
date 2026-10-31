"""CLI contract: exit codes and fail-fast validation (E2E-11, case C01).

Every invalid configuration below must be rejected *before* a port is bound or a
run directory appears. The exit codes are the contract in
:mod:`market_sim.errors`, and a script is entitled to rely on them.
"""

from __future__ import annotations

import json

import pytest

from conftest import config_dict, free_port
from market_sim.cli import main


def run_cli(argv, capsys):
    code = main(argv)
    out = capsys.readouterr().out
    payload = json.loads(out) if out.strip() else {}
    return code, payload


def write_config(tmp_path, name="config.toml", **kwargs):
    from conftest import _to_toml

    path = tmp_path / name
    path.write_text(_to_toml(config_dict(**kwargs)), encoding="utf-8")
    return path


class TestSuccessPaths:
    def test_record_run_exits_zero_and_prints_a_report(self, tmp_path, capsys):
        cfg = write_config(tmp_path, mode="record", count=200, rate=1_000_000)
        code, payload = run_cli(["record", "--config", str(cfg)], capsys)
        assert code == 0
        assert payload["ok"] is True
        assert payload["state"] == "COMPLETED"
        assert payload["report"]["counts"]["recorded_events"] == 200

    def test_verify_of_a_good_run_exits_zero(self, tmp_path, capsys):
        cfg = write_config(tmp_path, mode="record", count=100, rate=1_000_000)
        _, payload = run_cli(["record", "--config", str(cfg)], capsys)
        code, verdict = run_cli(["verify", "--run-dir", payload["run_dir"]], capsys)
        assert code == 0
        assert verdict["verdict"] == "COMPLETE"

    def test_count_on_the_command_line_overrides_a_config_duration(
        self, tmp_path, capsys
    ):
        cfg = write_config(
            tmp_path, mode="record", count=None, duration_s=60.0, rate=1_000_000
        )
        code, payload = run_cli(
            ["record", "--config", str(cfg), "--count", "250"], capsys
        )
        assert code == 0
        assert payload["report"]["counts"]["logical_slots"] == 250

    def test_seed_override_changes_the_dataset_id(self, tmp_path, capsys):
        cfg = write_config(tmp_path, mode="record", count=100, rate=1_000_000)
        _, first = run_cli(["record", "--config", str(cfg)], capsys)
        _, second = run_cli(
            ["record", "--config", str(cfg), "--seed", "20261031"], capsys
        )
        assert first["report"]["dataset_id"] != second["report"]["dataset_id"]

    def test_compact_flag_works_before_or_after_the_subcommand(self, tmp_path, capsys):
        cfg = write_config(tmp_path, mode="record", count=50, rate=1_000_000)
        code, _ = run_cli(["--compact", "record", "--config", str(cfg)], capsys)
        assert code == 0
        out_before = capsys.readouterr().out
        code, _ = run_cli(["record", "--config", str(cfg), "--compact"], capsys)
        assert code == 0

    def test_quiet_verify_prints_only_the_verdict(self, tmp_path, capsys):
        cfg = write_config(tmp_path, mode="record", count=50, rate=1_000_000)
        _, payload = run_cli(["record", "--config", str(cfg)], capsys)
        _, verdict = run_cli(
            ["verify", "--run-dir", payload["run_dir"], "--quiet"], capsys
        )
        assert set(verdict) == {"ok", "verdict", "lines", "sha256"}


class TestE2E11InvalidConfigFailsFast:
    """Every one of these must fail before any output exists."""

    @pytest.mark.parametrize(
        "overrides,code",
        [
            ({"rate": -1}, "CONFIG_OUT_OF_RANGE"),
            ({"rate": 0}, "CONFIG_OUT_OF_RANGE"),
            ({"count": -1}, "CONFIG_OUT_OF_RANGE"),
            ({"max_queue_events": 0}, "CONFIG_OUT_OF_RANGE"),
            ({"max_queue_bytes": 0}, "CONFIG_OUT_OF_RANGE"),
            ({"count": 10, "duration_s": 10.0}, "CONFIG_COUNT_DURATION_CONFLICT"),
            ({"count": None, "duration_s": None}, "CONFIG_COUNT_DURATION_MISSING"),
            ({"bound_policy": "wrap"}, "CONFIG_INVALID_CHOICE"),
            ({"on_scheduler_lag": "panic"}, "CONFIG_INVALID_CHOICE"),
        ],
    )
    def test_invalid_run_settings(self, tmp_path, capsys, overrides, code):
        cfg = write_config(tmp_path, mode="record", **overrides)
        exit_code, payload = run_cli(["record", "--config", str(cfg)], capsys)
        assert exit_code == 2
        assert payload["ok"] is False
        assert payload["code"] == code
        assert not (tmp_path / "runs").exists()

    @pytest.mark.parametrize("port", [0, -1, 65536])
    def test_invalid_port_is_rejected_without_binding(self, tmp_path, capsys, port):
        cfg = write_config(tmp_path, mode="both", count=10, tcp_port=port)
        exit_code, payload = run_cli(["both", "--config", str(cfg)], capsys)
        assert exit_code == 2
        assert payload["code"] == "CONFIG_OUT_OF_RANGE"
        assert not (tmp_path / "runs").exists()

    def test_unknown_symbol_in_the_order(self, tmp_path, capsys):
        from conftest import symbol_dict

        cfg = write_config(
            tmp_path,
            mode="record",
            count=10,
            order=["EURUSD", "XAUUSD"],
            symbols={"EURUSD": symbol_dict()},
        )
        exit_code, payload = run_cli(["record", "--config", str(cfg)], capsys)
        assert exit_code == 2
        assert payload["code"] == "CONFIG_SYMBOL_UNDEFINED"

    def test_unknown_key_in_the_config(self, tmp_path, capsys):
        # Case C01: a typo in a safety limit must not fall back to a default.
        from conftest import _to_toml

        raw = config_dict(mode="record", count=10)
        raw["run"]["max_schedular_lag_ms"] = 10
        cfg = tmp_path / "typo.toml"
        cfg.write_text(_to_toml(raw), encoding="utf-8")
        exit_code, payload = run_cli(["record", "--config", str(cfg)], capsys)
        assert exit_code == 2
        assert payload["code"] == "CONFIG_UNKNOWN_KEY"
        assert "max_schedular_lag_ms" in payload["context"]["unknown"]

    def test_broken_toml(self, tmp_path, capsys):
        cfg = tmp_path / "bad.toml"
        cfg.write_text("[run\nmode = 'record'\n", encoding="utf-8")
        exit_code, payload = run_cli(["record", "--config", str(cfg)], capsys)
        assert exit_code == 2
        assert payload["code"] == "CONFIG_TOML_SYNTAX"

    def test_missing_config_file(self, tmp_path, capsys):
        exit_code, payload = run_cli(
            ["record", "--config", str(tmp_path / "nope.toml")], capsys
        )
        assert exit_code == 2
        assert payload["code"] == "CONFIG_FILE_NOT_FOUND"

    def test_mode_without_its_sink(self, tmp_path, capsys):
        cfg = write_config(tmp_path, mode="record", count=10, file_enabled=False)
        exit_code, payload = run_cli(["record", "--config", str(cfg)], capsys)
        assert exit_code == 2
        assert payload["code"] == "CONFIG_MODE_SINK_CONFLICT"

    def test_public_bind_without_opt_in(self, tmp_path, capsys):
        cfg = write_config(tmp_path, mode="both", count=10, tcp_host="0.0.0.0")
        exit_code, payload = run_cli(["both", "--config", str(cfg)], capsys)
        assert exit_code == 2
        assert payload["code"] == "CONFIG_PUBLIC_BIND_NOT_ALLOWED"


class TestFaultPlanViaCli:
    def test_a_plan_without_faults_enabled_is_refused(
        self, tmp_path, capsys, write_fault_plan
    ):
        # Loading a plan and quietly ignoring it would mean a faulted dataset
        # sharing a dataset_id with a clean one.
        plan = write_fault_plan(
            {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 10}
        )
        cfg = write_config(tmp_path, mode="record", count=100, rate=1_000_000)
        exit_code, payload = run_cli(
            ["record", "--config", str(cfg), "--fault-plan", str(plan)], capsys
        )
        assert exit_code == 2
        assert payload["code"] == "CONFIG_FAULTS_DISABLED"

    def test_an_enabled_plan_is_applied_and_changes_the_dataset_id(
        self, tmp_path, capsys, write_fault_plan
    ):
        plan = write_fault_plan(
            {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 10}
        )
        clean_cfg = write_config(
            tmp_path, name="clean.toml", mode="record", count=400, rate=1_000_000
        )
        faulted_cfg = write_config(
            tmp_path,
            name="faulted.toml",
            mode="record",
            count=400,
            rate=1_000_000,
            faults_enabled=True,
            plan_path=str(plan),
        )
        _, clean = run_cli(["record", "--config", str(clean_cfg)], capsys)
        code, faulted = run_cli(["record", "--config", str(faulted_cfg)], capsys)
        assert code == 0
        assert faulted["report"]["counts"]["intentional_omissions"] == 1
        assert faulted["report"]["dataset_id"] != clean["report"]["dataset_id"]

    def test_an_invalid_plan_is_rejected(self, tmp_path, capsys):
        plan = tmp_path / "plan.toml"
        plan.write_text(
            '[[fault]]\nkind = "explode"\nsymbol = "EURUSD"\nsequence = 1\n',
            encoding="utf-8",
        )
        cfg = write_config(
            tmp_path,
            mode="record",
            count=100,
            rate=1_000_000,
            faults_enabled=True,
            plan_path=str(plan),
        )
        exit_code, payload = run_cli(["record", "--config", str(cfg)], capsys)
        assert exit_code == 2
        assert payload["code"] == "FAULT_PLAN_UNKNOWN_KIND"

    def test_benchmark_refuses_to_measure_a_faulted_run(
        self, tmp_path, capsys, write_fault_plan
    ):
        # Case C12: a throughput number from a faulted run describes nothing.
        plan = write_fault_plan(
            {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 10}
        )
        cfg = write_config(
            tmp_path,
            mode="record",
            count=100,
            rate=1_000_000,
            faults_enabled=True,
            plan_path=str(plan),
        )
        exit_code, payload = run_cli(["benchmark", "--config", str(cfg)], capsys)
        assert exit_code == 2
        assert payload["code"] == "CONFIG_BENCHMARK_WITH_FAULTS"


class TestVerifyExitCodes:
    def test_verify_of_an_incomplete_run_exits_four(self, tmp_path, capsys):
        from pathlib import Path

        cfg = write_config(tmp_path, mode="record", count=100, rate=1_000_000)
        _, payload = run_cli(["record", "--config", str(cfg)], capsys)
        (Path(payload["run_dir"]) / "COMPLETED").unlink()
        exit_code, verdict = run_cli(["verify", "--run-dir", payload["run_dir"]], capsys)
        assert exit_code == 4
        assert verdict["verdict"] == "INCOMPLETE"

    def test_verify_of_a_missing_directory_exits_four(self, tmp_path, capsys):
        exit_code, _ = run_cli(["verify", "--run-dir", str(tmp_path / "nope")], capsys)
        assert exit_code == 4


class TestBenchmarkCommand:
    def test_benchmark_reports_median_and_worst(self, tmp_path, capsys):
        # Section 9.3: a single number is not a benchmark.
        cfg = write_config(tmp_path, mode="record", count=2_000, rate=1_000_000)
        exit_code, payload = run_cli(
            ["benchmark", "--config", str(cfg), "--repeat", "3", "--warmup"], capsys
        )
        assert exit_code == 0
        summary = payload["summary"]
        assert summary["repetitions"] == 3
        assert summary["measured_repetitions"] == 2  # the warm-up is excluded
        assert summary["median_generated_rate_per_sec"] is not None
        assert summary["worst_generated_rate_per_sec"] is not None
        assert "does not claim any TCP client received" in summary["interpretation"]
