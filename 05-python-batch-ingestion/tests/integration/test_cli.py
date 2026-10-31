"""The command surface, exercised rather than documented.

Spec 12.2 lists the intended commands. These tests run them for real, because
an entry point that nobody invokes is where a refactor breaks silently.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from p05_ingest.cli import build_parser, main
from p05_ingest.proto_schema import descriptor_sha256

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEV_CONFIG = str(PROJECT_ROOT / "config" / "dev.toml")


def run_cli(args: list[str]) -> subprocess.CompletedProcess:
    env = dict(os.environ, PYTHONPATH=str(PROJECT_ROOT / "src"))
    return subprocess.run(
        [sys.executable, "-m", "p05_ingest", *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(PROJECT_ROOT),
        timeout=300,
    )


# ------------------------------------------------------------------- parsing


def test_parser_requires_a_command():
    with pytest.raises(SystemExit):
        build_parser().parse_args([])


@pytest.mark.parametrize(
    "command", ["doctor", "run", "snapshot", "reconcile", "simulate"]
)
def test_every_documented_command_exists(command):
    parser = build_parser()
    choices = [
        action.choices
        for action in parser._actions
        if getattr(action, "choices", None)
    ]
    assert any(command in c for c in choices)


# ------------------------------------------------------------ descriptor-hash


def test_descriptor_hash_prints_the_pinned_value(capsys):
    assert main(["descriptor-hash"]) == 0
    out = capsys.readouterr().out
    assert descriptor_sha256() in out
    assert "marketdata.v1" in out


def test_descriptor_hash_via_module_entry_point():
    result = run_cli(["descriptor-hash"])
    assert result.returncode == 0, result.stderr
    assert descriptor_sha256() in result.stdout


# -------------------------------------------------------------------- doctor


def test_doctor_passes_on_the_shipped_profile(capsys):
    assert main(["doctor", "--config", DEV_CONFIG]) == 0
    out = capsys.readouterr().out
    assert "PREFLIGHT OK" in out
    assert "effective config fingerprint:" in out


def test_doctor_marks_unverified_checks_as_warnings(capsys):
    """Spec 0.2: an unverified claim is a WARN, never a PASS."""
    main(["doctor", "--config", DEV_CONFIG])
    out = capsys.readouterr().out
    assert "[WARN] clickhouse table schema" in out
    assert "[WARN] kafka retention headroom" in out
    assert "did not contact anything" in out


def test_doctor_fails_on_an_unsafe_offset_policy(tmp_path, capsys):
    bad = tmp_path / "bad.toml"
    bad.write_text(
        Path(DEV_CONFIG)
        .read_text(encoding="utf-8")
        .replace('auto_offset_reset = "error"', 'auto_offset_reset = "latest"'),
        encoding="utf-8",
    )
    assert main(["doctor", "--config", str(bad)]) == 2
    err = capsys.readouterr().err
    assert "startup refused" in err
    assert "auto_offset_reset" in err


def test_doctor_reports_a_descriptor_mismatch(tmp_path, capsys):
    bad = tmp_path / "pin.toml"
    bad.write_text(
        Path(DEV_CONFIG)
        .read_text(encoding="utf-8")
        .replace(descriptor_sha256(), "ab" * 32),
        encoding="utf-8",
    )
    assert main(["doctor", "--config", str(bad)]) == 2
    out = capsys.readouterr().out
    assert "[FAIL] protobuf descriptor pin" in out


def test_missing_config_file_is_reported(capsys):
    assert main(["doctor", "--config", "config/nope.toml"]) == 2
    assert "not found" in capsys.readouterr().err


# ------------------------------------------------------------------ simulate


def test_simulate_runs_the_whole_pipeline_without_infrastructure(capsys):
    assert main(["simulate", "--config", DEV_CONFIG, "--events", "300"]) == 0
    out = capsys.readouterr().out
    assert "simulated events produced: 300" in out
    assert "clickhouse physical rows:  300" in out
    assert "logical identities stored: 300" in out
    assert "RUN VERDICT: COMPLETE" in out
    # The completeness caveat must always be printed (spec I-08).
    assert "NOT a completeness claim against the source" in out


def test_simulate_writes_a_report_artifact(tmp_path):
    report = tmp_path / "run.json"
    assert (
        main(
            [
                "simulate",
                "--config",
                DEV_CONFIG,
                "--events",
                "100",
                "--report",
                str(report),
            ]
        )
        == 0
    )
    payload = json.loads(report.read_text(encoding="utf-8"))

    assert payload["verdict"] == "COMPLETE"
    assert payload["simulated"] is True
    assert payload["descriptor_sha256"] == descriptor_sha256()
    assert len(payload["config_fingerprint"]) == 64
    assert payload["metrics"]["counters"]
    assert payload["commit_state"]
    # The report must never carry a credential or a raw payload.
    blob = json.dumps(payload)
    assert "P05_CH_PASSWORD" not in blob
    assert "password" not in blob.lower()


def test_simulate_with_a_lost_ack_still_completes(capsys):
    """A scripted ambiguous outcome followed by a success."""
    assert (
        main(
            [
                "simulate",
                "--config",
                DEV_CONFIG,
                "--events",
                "100",
                "--sink-plan",
                "write_then_lose_ack,ack",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "RUN VERDICT: COMPLETE" in out
    assert "logical identities stored: 100" in out


def test_simulate_reports_incomplete_when_the_sink_fails(capsys):
    """A permanent sink error must produce a non-zero exit."""
    code = main(
        [
            "simulate",
            "--config",
            DEV_CONFIG,
            "--events",
            "50",
            "--sink-plan",
            "permanent_error,permanent_error,permanent_error,permanent_error",
        ]
    )
    out = capsys.readouterr().out
    assert code == 1
    assert "RUN VERDICT: INCOMPLETE" in out


def test_simulate_via_the_module_entry_point():
    result = run_cli(
        ["simulate", "--config", DEV_CONFIG, "--events", "200", "--partitions", "4"]
    )
    assert result.returncode == 0, result.stderr
    assert "RUN VERDICT: COMPLETE" in result.stdout


# ----------------------------------------------------------------- reconcile


def test_reconcile_refuses_to_fake_a_result_without_infrastructure(
    tmp_path, capsys
):
    """It must not fall back to a simulated source and report a verdict."""
    cut = tmp_path / "cut.json"
    cut.write_text("{}", encoding="utf-8")
    code = main(["reconcile", "--config", DEV_CONFIG, "--cut", str(cut)])
    err = capsys.readouterr().err
    assert code == 3
    assert "requires a Kafka broker" in err
    assert "does not fall back" in err


def test_reconcile_reports_a_missing_cut_file(tmp_path, capsys):
    code = main(
        ["reconcile", "--config", DEV_CONFIG, "--cut", str(tmp_path / "x.json")]
    )
    assert code == 2
    assert "cut file not found" in capsys.readouterr().err


# ----------------------------------------------------------- live-only paths


def test_run_requires_live_infrastructure_and_says_so(capsys):
    """``run`` must fail clearly rather than silently simulate.

    Nothing is listening on the configured ports here, so this exercises the
    real failure path: the preflight sink check cannot reach ClickHouse and
    startup is refused.
    """
    code = main(["run", "--config", DEV_CONFIG, "--max-idle-steps", "1"])
    assert code != 0
    captured = capsys.readouterr()
    combined = (captured.out + captured.err).lower()
    assert "clickhouse" in combined or "startup refused" in combined
