"""Run every shipped scenario file and hold it to its own declared expectations.

This is the chaos layer: the YAML says which fault goes in at which stage and
what verdict it expects, and :func:`run_scenario` has to agree. A scenario whose
real outcome differs from its claim fails here, which keeps the scenario files
honest as the harness changes.

Each run writes a full evidence bundle under ``tmp_path``, and the tests check
the bundle as well as the verdict -- a verdict with no evidence behind it is
exactly what design 9.2 refuses.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from p07.evidence.verdicts import Verdict
from p07.orchestration.scenario_runner import run_scenario
from p07.scenario_loader import load_scenario

pytestmark = [pytest.mark.chaos, pytest.mark.slow]

ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = sorted((ROOT / "scenarios").rglob("*.yaml"))


def test_scenarios_were_discovered() -> None:
    assert SCENARIOS, "no scenario files found"


@pytest.mark.parametrize("path", SCENARIOS, ids=lambda path: path.stem)
def test_a_scenario_meets_its_own_expectations(path: Path, tmp_path: Path) -> None:
    """The scenario's declared verdict must be the one it actually gets."""
    scenario = load_scenario(path)
    outcome = run_scenario(scenario, runs_dir=tmp_path / "runs", run_id="chaos-1", fsync=False)
    assert outcome.expectation_mismatches == (), (
        f"{path.name}: {outcome.expectation_mismatches}; "
        f"verdict={outcome.gate.verdict.value} reason={outcome.gate.primary_reason}"
    )


@pytest.mark.parametrize("path", SCENARIOS, ids=lambda path: path.stem)
def test_a_scenario_writes_a_complete_evidence_bundle(path: Path, tmp_path: Path) -> None:
    """Design 11.1's layout, plus a results document and a checksum file."""
    scenario = load_scenario(path)
    outcome = run_scenario(scenario, runs_dir=tmp_path / "runs", run_id="bundle-1", fsync=False)
    run_dir = Path(outcome.run_dir)
    for required in (
        "environment.json",
        "config.effective.json",
        "source/manifest.json",
        "fault/intent.json",
        "orchestration/journal.jsonl",
        "reports/results.json",
        "reports/summary.md",
        "SHA256SUMS",
    ):
        assert (run_dir / required).is_file(), f"{path.name} missing {required}"

    report = json.loads((run_dir / "reports" / "results.json").read_text(encoding="utf-8"))
    assert report["profile"] == "UNIT/MODEL"
    assert "does NOT exercise a real Kafka broker" in report["profile_note"]
    assert report["verdict"] == outcome.gate.verdict.value
    assert report["evidence"]["journal"]["chain_intact"] is True
    assert report["evidence"]["durability"]["fsync_enabled"] is False


@pytest.mark.parametrize("path", SCENARIOS, ids=lambda path: path.stem)
def test_no_scenario_publishes_ready_without_a_pass(path: Path, tmp_path: Path) -> None:
    """``NFR-03``: zero false READY, across every scenario in the repository.

    The most important cross-scenario property: whatever went wrong, a run that
    did not reach PASS must not have moved the catalog pointer.
    """
    scenario = load_scenario(path)
    outcome = run_scenario(scenario, runs_dir=tmp_path / "runs", run_id="ready-1", fsync=False)
    published = outcome.report["stages"]["published"]
    if outcome.gate.verdict is not Verdict.PASS:
        assert published is not True, f"{path.name} published on a non-PASS verdict"


@pytest.mark.parametrize("path", SCENARIOS, ids=lambda path: path.stem)
def test_every_declared_fault_produces_a_receipt(path: Path, tmp_path: Path) -> None:
    """A scenario that declares faults must end with receipts for them.

    Without this, a fault type whose injector silently stopped working would
    keep passing its own scenario -- the failure mode case ``E2E-37`` exists for.
    """
    scenario = load_scenario(path)
    outcome = run_scenario(scenario, runs_dir=tmp_path / "runs", run_id="receipt-1", fsync=False)
    faults = outcome.report["faults"]
    if scenario.faults:
        assert faults["faults_total"] >= 1, f"{path.name} declared faults but recorded none"
    else:
        assert faults["faults_total"] == 0


def test_a_scenario_run_is_reproducible(tmp_path: Path) -> None:
    """The same scenario twice must reach the same verdict and the same diffs.

    Not byte-identical: the bundle carries timestamps and a run id. What must
    match is every *semantic* result, which is design 4.1's "semantic replay".
    """
    scenario = load_scenario(ROOT / "scenarios" / "simple" / "clean-baseline.yaml")
    first = run_scenario(scenario, runs_dir=tmp_path / "a", run_id="r1", fsync=False)
    second = run_scenario(scenario, runs_dir=tmp_path / "b", run_id="r2", fsync=False)

    assert first.gate.verdict is second.gate.verdict
    assert first.report["expected"]["counts"] == second.report["expected"]["counts"]
    for boundary in ("transport", "raw", "canonical"):
        left = first.report["stages"][boundary]["diff"]
        right = second.report["stages"][boundary]["diff"]
        assert left["expected_count"] == right["expected_count"]
        assert left["equal"] == right["equal"]
    # ``build_id`` is derived from the run id and is deliberately *not* part of
    # the source identity (design 3.1), so it is expected to differ.
    left_bars = {k: v for k, v in first.report["stages"]["bars"].items() if k != "build_id"}
    right_bars = {k: v for k, v in second.report["stages"]["bars"].items() if k != "build_id"}
    assert left_bars == right_bars
