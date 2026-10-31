"""The config subset loader, and scenario validation that fails closed."""

from __future__ import annotations

from pathlib import Path

import pytest

from p07.scenario_loader import (
    FAULT_TYPES,
    SCENARIO_VERSION,
    ScenarioError,
    lab_config_findings,
    load_lab_config,
    load_scenario,
    parse_scenario,
)
from p07.yamlite import YamlLiteError, load_path, safe_load

# -- the loader ----------------------------------------------------------


def test_nested_mappings_and_sequences() -> None:
    document = safe_load(
        """
version: p07.scenario.v1
lab:
  name: md-p07
  partitions: 8
  protect_host: true
  ratio: 0.5
  nothing: null
faults:
  - type: ack_loss
    target: clickhouse_ingest
  - type: broker_stop
    target: kafka_broker
    broker_id: 1
chunk_sizes: [1, 17, 4096]
names:
  - alpha
  - beta
"""
    )
    assert document["lab"] == {
        "name": "md-p07",
        "partitions": 8,
        "protect_host": True,
        "ratio": 0.5,
        "nothing": None,
    }
    assert document["faults"][1]["broker_id"] == 1
    assert document["chunk_sizes"] == [1, 17, 4096]
    assert document["names"] == ["alpha", "beta"]


def test_comments_and_blank_lines_are_ignored() -> None:
    assert safe_load("# leading\n\na: 1  # trailing\n\n") == {"a": 1}


def test_a_hash_inside_quotes_is_not_a_comment() -> None:
    assert safe_load('colour: "#fff"') == {"colour": "#fff"}


def test_quoted_scalars_keep_their_type() -> None:
    document = safe_load("a: '1'\nb: \"true\"\nc: 1\nd: true")
    assert document == {"a": "1", "b": "true", "c": 1, "d": True}


def test_empty_document_is_none() -> None:
    assert safe_load("# only a comment\n") is None


@pytest.mark.whatif("P07-G28-02")
@pytest.mark.parametrize(
    ("document", "needle"),
    [
        ("a: &anchor 1", "anchors"),
        ("a: *alias", "anchors"),
        ("a: !!python/object/apply:os.system ['echo hi']", "tags"),
        ("a: !custom x", "tags"),
        ("---\na: 1", "multiple documents"),
        ("...\n", "document end"),
        ("%YAML 1.2\na: 1", "directives"),
        ("a: |\n  block", "block scalars"),
        ("a: >\n  folded", "block scalars"),
        ("a: {b: 1}", "flow mappings"),
        ("a: 1\na: 2", "duplicate key"),
        ("a: [1, 2", "unterminated flow"),
        ("\ta: 1", "tab"),
        ("just a scalar", "key: value"),
    ],
)
def test_everything_outside_the_subset_is_rejected_loudly(document: str, needle: str) -> None:
    """A construct the harness has no meaning for must error, never be skipped.

    These files control destructive operations, so an unparsed line is more
    dangerous than a failed load.
    """
    with pytest.raises(YamlLiteError) as exc:
        safe_load(document)
    assert needle in str(exc.value)


def test_json_files_load_through_the_same_entry_point(tmp_path: Path) -> None:
    path = tmp_path / "scenario.json"
    path.write_text('{"a": 1}', encoding="utf-8")
    assert load_path(path) == {"a": 1}


# -- scenario validation --------------------------------------------------


def minimal(**overrides: object) -> dict[str, object]:
    document: dict[str, object] = {
        "version": SCENARIO_VERSION,
        "scenario_id": "s1",
        "source": {"run_dir": "fixtures/demo-fx-001"},
    }
    document.update(overrides)
    return document


def test_minimal_scenario_parses_with_defaults() -> None:
    scenario = parse_scenario(minimal())
    assert scenario.chunk_sizes == (4096,)
    assert scenario.faults == ()
    assert scenario.expect.require_zero_conflicts is True
    assert scenario.max_duration_s == 600


def test_wrong_version_is_rejected() -> None:
    with pytest.raises(ScenarioError, match="version"):
        parse_scenario(minimal(version="p07.scenario.v99"))


@pytest.mark.whatif("P07-G28-02")
def test_unknown_keys_are_errors_at_every_level() -> None:
    """A typo would otherwise silently disable a check."""
    with pytest.raises(ScenarioError, match="unknown keys"):
        parse_scenario(minimal(unexpected=1))
    with pytest.raises(ScenarioError, match="unknown keys"):
        parse_scenario(minimal(expect={"require_zero_conflict": True}))
    with pytest.raises(ScenarioError, match="unknown keys"):
        parse_scenario(minimal(lab={"partitions": 8, "parititons": 8}))


def test_source_run_dir_is_required() -> None:
    with pytest.raises(ScenarioError, match="run_dir"):
        parse_scenario({"version": SCENARIO_VERSION, "scenario_id": "s", "source": {}})


def test_anchor_must_look_like_a_digest() -> None:
    with pytest.raises(ScenarioError, match="anchor"):
        parse_scenario(minimal(source={"run_dir": "x", "anchor_sha256": "short"}))


def test_fault_type_is_a_closed_set() -> None:
    with pytest.raises(ScenarioError, match="is not one of"):
        parse_scenario(minimal(faults=[{"type": "run_shell_command", "target": "kafka_broker"}]))


@pytest.mark.whatif("P07-G28-01")
def test_a_target_outside_the_allowlist_is_refused_at_load_time() -> None:
    """The dry-run guarantee: refused before anything starts."""
    with pytest.raises(ScenarioError, match="TARGET_NOT_ALLOWLISTED"):
        parse_scenario(
            minimal(faults=[{"type": "kafka_broker_stop", "target": "production_cluster"}])
        )


def test_every_fault_type_has_a_stage() -> None:
    for fault_type in FAULT_TYPES:
        scenario = parse_scenario(minimal(faults=[{"type": fault_type, "target": "kafka_broker"}]))
        assert scenario.faults[0].stage == FAULT_TYPES[fault_type]
        assert scenario.faults_at(FAULT_TYPES[fault_type])


def test_too_many_faults_for_the_declared_cap_is_an_error() -> None:
    """Design 8 caps concurrent faults; exceeding your own cap is a config bug."""
    with pytest.raises(ScenarioError, match="max_parallel_faults"):
        parse_scenario(
            minimal(
                faults=[
                    {"type": "kafka_broker_stop", "target": "kafka_broker"},
                    {"type": "ch_ack_loss", "target": "clickhouse_ingest"},
                    {"type": "ch_disk_full", "target": "clickhouse_server"},
                ],
                limits={"max_parallel_faults": 2},
            )
        )


def test_expected_verdict_must_be_terminal() -> None:
    with pytest.raises(ScenarioError, match="terminal verdict"):
        parse_scenario(minimal(expect={"verdict": "MAYBE"}))


def test_chunk_sizes_must_be_positive_integers() -> None:
    for bad in ([], [0], [-1], ["4096"]):
        with pytest.raises(ScenarioError):
            parse_scenario(minimal(replay={"chunk_sizes": bad}))


def test_the_shipped_scenarios_all_load_and_pin_an_anchor() -> None:
    root = Path(__file__).resolve().parents[2] / "scenarios"
    files = sorted(root.rglob("*.yaml"))
    assert files, "no scenarios found"
    for path in files:
        scenario = load_scenario(path)
        assert scenario.scenario_id
        assert scenario.source_anchor, f"{path.name} should pin an external anchor"


# -- lab config -----------------------------------------------------------


def test_the_shipped_lab_config_has_no_weakening_settings() -> None:
    root = Path(__file__).resolve().parents[2]
    config = load_lab_config(root / "config" / "replay-lab.yaml")
    assert lab_config_findings(config) == []


@pytest.mark.whatif("P07-G29-05")
@pytest.mark.parametrize(
    ("section", "key", "value", "needle"),
    [
        ("reconciliation", "allow_sample_only_pass", True, "sampled pass"),
        ("reconciliation", "allow_semantic_conflicts", True, "conflicts are P0"),
        ("reconciliation", "allow_missing", True, "missing identities"),
        ("reconciliation", "allow_extra", True, "extra identities"),
        ("reconciliation", "full_identity_scan", False, "exact diff"),
        ("reconciliation", "require_source_policy_oracle", False, "circular"),
        ("safety", "auto_offset_reset", "latest", "implicit reset"),
        ("safety", "publishing_requires_ready_catalog", False, "false READY"),
    ],
)
def test_a_config_that_disables_a_gate_is_flagged(
    section: str, key: str, value: object, needle: str
) -> None:
    """Valid YAML and still unusable; ``validate-config`` exits non-zero on these."""
    config: dict[str, dict[str, object]] = {
        "reconciliation": {
            "full_identity_scan": True,
            "require_source_policy_oracle": True,
            "allow_sample_only_pass": False,
            "allow_semantic_conflicts": False,
            "allow_missing": False,
            "allow_extra": False,
        },
        "safety": {"auto_offset_reset": "none", "publishing_requires_ready_catalog": True},
    }
    config[section][key] = value
    notes = lab_config_findings(config)
    assert any(needle in note for note in notes), notes


def test_the_checked_in_scenario_schema_matches_the_loader() -> None:
    """The schema is a projection of the loader, so it cannot drift.

    Regenerate with ``python -m tools.emit_scenario_schema`` after changing any
    key set in :mod:`p07.scenario_loader`. A hand-maintained schema would
    eventually tell an editor that a key the loader rejects is fine.
    """
    from tools.emit_scenario_schema import OUTPUT, render

    root = Path(__file__).resolve().parents[2]
    on_disk = (root / OUTPUT).read_text(encoding="utf-8")
    assert on_disk == render(), "run: python -m tools.emit_scenario_schema"
