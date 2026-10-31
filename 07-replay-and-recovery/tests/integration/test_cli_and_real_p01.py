"""The CLI's contract, and the harness against genuine P01 output.

Two kinds of integration, neither of them a real broker.

**The CLI** is checked for what CI depends on: exit codes. A blocking verdict
must exit 1 and a broken invocation must exit 2, because a release gate has to
distinguish "the lab says no" from "the lab did not run".

**Real P01 output** is sealed and classified when a committed run directory is
available next door. The fixtures elsewhere in this suite are fabricated by
``tools.source_factory``, which means they share this harness's reading of the
``sim.v1`` contract. These tests are the check on that reading: if P01's real
writer and P07's oracle disagree, it shows up here and nowhere else. They skip
rather than fail when P01 has not been run, so the suite stays usable alone.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

import pytest

from p07.cli import main
from p07.dataset_sealer import seal_dataset, sha256_file
from p07.oracle.acceptance_oracle import build_expectation
from p07.oracle.reference_bars import reference_bars

ROOT = Path(__file__).resolve().parents[2]
P01_RUNS = ROOT.parent / "01-market-data-simulator" / "runs"


def real_p01_runs() -> list[Path]:
    """Committed P01 run directories, if any exist next to this project."""
    if not P01_RUNS.is_dir():
        return []
    return sorted(path for path in P01_RUNS.iterdir() if (path / "COMPLETED").is_file())


# -- the CLI ---------------------------------------------------------------


def test_validate_config_accepts_the_shipped_lab_config(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["validate-config", str(ROOT / "config" / "replay-lab.yaml")]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is True
    assert payload["weakening_settings"] == []


def test_validate_config_exits_non_zero_on_a_weakened_config(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A config that disables the gates is valid YAML and still unusable."""
    path = tmp_path / "weak.yaml"
    path.write_text(
        "version: p07.scenario.v1\nreconciliation:\n  allow_sample_only_pass: true\n",
        encoding="utf-8",
    )
    assert main(["validate-config", str(path)]) == 1
    assert "sampled pass" in capsys.readouterr().out


def test_validate_config_exits_two_on_a_broken_file(tmp_path: Path) -> None:
    path = tmp_path / "broken.yaml"
    path.write_text("version: wrong\n", encoding="utf-8")
    assert main(["validate-config", str(path)]) == 2


def test_verify_source_seals_and_reports(
    make_source: Callable[..., dict[str, Any]], capsys: pytest.CaptureFixture[str]
) -> None:
    info = make_source(count=80, dataset_id="cli-1")
    assert main(["verify-source", info["run_dir"], "--anchor", info["events_sha256"]]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["anchor_verified"] is True
    assert payload["event_lines"] == info["events"]


def test_verify_source_exits_one_on_an_unsealed_source(
    make_source: Callable[..., dict[str, Any]], capsys: pytest.CaptureFixture[str]
) -> None:
    info = make_source(count=40, leave_part_file=True)
    assert main(["verify-source", info["run_dir"]]) == 1
    assert "SOURCE_PART_FILE_PRESENT" in capsys.readouterr().err


def test_verify_source_warns_when_no_anchor_was_given(
    make_source: Callable[..., dict[str, Any]], capsys: pytest.CaptureFixture[str]
) -> None:
    info = make_source(count=40)
    assert main(["verify-source", info["run_dir"]]) == 0
    assert "no external anchor" in capsys.readouterr().err


def test_anchor_prints_the_digest_to_pin(
    make_source: Callable[..., dict[str, Any]], capsys: pytest.CaptureFixture[str]
) -> None:
    info = make_source(count=40)
    assert main(["anchor", str(Path(info["run_dir"]) / "events.jsonl")]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["sha256"] == info["events_sha256"]


def test_plan_validates_a_scenario_and_shows_its_schedule(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = main(["plan", str(ROOT / "scenarios" / "kafka" / "ack-unknown.yaml")])
    payload = json.loads(capsys.readouterr().out)
    assert payload["stages"]["pre_replay"] == ["kafka_ack_loss"]
    assert payload["faults"][0]["type"] == "kafka_ack_loss"
    # The exit code reflects whether the fixture is actually present.
    assert exit_code in {0, 1}
    assert payload["source_present"] is (exit_code == 0)


def test_plan_exits_two_on_an_invalid_scenario(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("version: p07.scenario.v1\nscenario_id: x\n", encoding="utf-8")
    assert main(["plan", str(path)]) == 2


@pytest.mark.slow
def test_run_then_report_then_reconcile_round_trips(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The three commands a CI job chains together."""
    runs = tmp_path / "runs"
    code = main(
        [
            "run",
            str(ROOT / "scenarios" / "simple" / "clean-baseline.yaml"),
            "--runs-dir",
            str(runs),
            "--run-id",
            "cli-run",
            "--no-fsync",
        ]
    )
    summary = json.loads(capsys.readouterr().out)
    assert code == 0
    assert summary["verdict"] == "PASS"
    run_dir = summary["run_dir"]

    assert main(["report", run_dir, "--format", "json"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["profile"] == "UNIT/MODEL"

    assert main(["report", run_dir]) == 0
    assert "**Verdict: PASS**" in capsys.readouterr().out

    assert main(["reconcile", run_dir]) == 0
    verification = json.loads(capsys.readouterr().out)
    assert verification["damaged"] == []
    assert verification["artifacts_checked"] > 5
    assert "rewrite the bundle" in verification["note"]


@pytest.mark.slow
def test_reconcile_detects_a_tampered_bundle(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs = tmp_path / "runs"
    main(
        [
            "run",
            str(ROOT / "scenarios" / "simple" / "clean-baseline.yaml"),
            "--runs-dir",
            str(runs),
            "--run-id",
            "tamper",
            "--no-fsync",
        ]
    )
    run_dir = Path(json.loads(capsys.readouterr().out)["run_dir"])
    (run_dir / "analytics" / "bars_diff.json").write_text("{}", encoding="utf-8")
    assert main(["reconcile", str(run_dir)]) == 1
    assert "bars_diff.json" in json.loads(capsys.readouterr().out)["damaged"][0]


def test_verify_release_blocks_on_an_uncovered_p0(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    catalog = tmp_path / "catalog.json"
    catalog.write_text(
        json.dumps(
            {
                "cases": [
                    {"case_id": "P07-G01-01", "priority": "P0", "group": "G01"},
                    {"case_id": "P07-G01-02", "priority": "P1", "group": "G01"},
                ]
            }
        ),
        encoding="utf-8",
    )
    coverage = tmp_path / "coverage.json"
    coverage.write_text(json.dumps([]), encoding="utf-8")
    out = tmp_path / "release.md"
    assert (
        main(
            [
                "verify-release",
                "--catalog",
                str(catalog),
                "--coverage",
                str(coverage),
                "--out",
                str(out),
            ]
        )
        == 1
    )
    text = out.read_text(encoding="utf-8")
    assert "NO-GO" in text
    assert "NOT VALIDATED" in text
    capsys.readouterr()


def test_the_shipped_catalog_holds_all_448_designed_cases() -> None:
    """The inventory is derived from the design document, not transcribed."""
    catalog = json.loads((ROOT / "catalog" / "what_if_matrix.json").read_text(encoding="utf-8"))
    assert catalog["totals"] == {
        "cases": 448,
        "groups": 32,
        "p0": 128,
        "p1": 256,
        "p2": 64,
        "e2e": 40,
    }
    assert len({case["case_id"] for case in catalog["cases"]}) == 448
    assert "NOT a set of results" in catalog["note"]


# -- real P01 output -------------------------------------------------------


@pytest.mark.skipif(not real_p01_runs(), reason="no committed P01 run directory available")
def test_real_p01_output_seals_and_classifies_without_false_positives() -> None:
    """The check on the fabricated fixtures: does the real writer agree?

    A clean P01 run must produce zero rejections, zero suppressions and zero
    untracked anomalies. Anything else means P07's reading of ``sim.v1`` has
    drifted from P01's -- which the fabricated fixtures could never reveal,
    because they are built from the same reading.
    """
    for run_dir in real_p01_runs():
        digest, _ = sha256_file(run_dir / "events.jsonl")
        dataset = seal_dataset(run_dir, anchor=digest)
        expectation = build_expectation(dataset)

        assert expectation.counts["source_lines"] == dataset.event_lines
        assert expectation.source_identity_conflicts == ()
        if not dataset.faults_enabled:
            assert expectation.counts["rejected"] == 0, (run_dir.name, expectation.counts)
            assert expectation.counts["suppressed"] == 0
            assert expectation.counts["accepted"] == dataset.event_lines
            assert expectation.untracked_anomalies == ()


@pytest.mark.skipif(not real_p01_runs(), reason="no committed P01 run directory available")
def test_real_p01_output_produces_bars_with_integer_prices() -> None:
    run_dir = real_p01_runs()[0]
    digest, _ = sha256_file(run_dir / "events.jsonl")
    expectation = build_expectation(seal_dataset(run_dir, anchor=digest))
    bars = reference_bars(expectation.accepted.values())
    assert bars
    for key, bar in bars.items():
        assert isinstance(bar.open_ticks, int) and isinstance(bar.close_ticks, int)
        assert bar.low_ticks <= bar.open_ticks <= bar.high_ticks
        assert bar.tick_count >= 1
        assert key.bar_start_ns % 60_000_000_000 == 0
