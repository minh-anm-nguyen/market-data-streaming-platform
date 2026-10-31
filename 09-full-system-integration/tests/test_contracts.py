"""Tests for the M0 audit, the lockfile and the init renderer.

P09 FR-13 asks for negative controls: a suite that cannot fail is not evidence.
So most of what follows feeds *deliberately wrong* input and asserts that the
tool says so. The few cases that merely confirm the current tree agrees are
marked as such -- they would also pass against a checker that always said yes,
and it is the mutations next to them that give them meaning.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from p09_integration import (
    audit,
    deploy,
    descriptors,
    lockfile,
    pipeline,
    probes,
    protoparse,
    yamlsub,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
P02_SCHEMAS = REPO_ROOT / "02-market-data-protobuf-contracts" / "schemas"
CANONICAL = P02_SCHEMAS / "marketdata" / "v1" / "market_data.proto"

requires_repo = pytest.mark.skipif(
    not CANONICAL.exists(), reason="sibling projects are not present in this checkout"
)


# --------------------------------------------------------------------------
# yamlsub: the lockfile is only trustworthy if it round-trips exactly
# --------------------------------------------------------------------------

def test_yaml_round_trip_preserves_every_supported_type():
    document = {
        "int": 7,
        "zero": 0,
        "true": True,
        "false": False,
        "none": None,
        "empty_map": {},
        "empty_list": [],
        "numeric_string": "1234",
        "word_string": "true",
        "padded": " spaced ",
        "colon": "a: b",
        "hash": "#not-a-comment",
        "nested": {"a": {"b": [1, 2, {"c": "d"}]}},
        "rows": [{"k": 1, "sub": {"deep": True}}, {"k": 2, "sub": {"deep": False}}],
    }
    text = yamlsub.dumps(document)
    assert yamlsub.loads(text) == document
    # Idempotent: re-rendering what was parsed yields the same bytes.
    assert yamlsub.dumps(yamlsub.loads(text)) == text


@pytest.mark.parametrize(
    "source",
    [
        "a: 1\n b: 2",            # indent not a multiple of two
        "a: 1\na: 2",             # duplicate key
        "- 1\n- 2",               # list at the document root
        'a: "unterminated',       # unterminated quoted scalar
        "a:\n\t- 1",              # tab indentation
        "a:\n  - b: 1\n  c: 2",   # mapping key where a list item belongs
    ],
)
def test_yaml_refuses_input_outside_the_subset(source):
    with pytest.raises(yamlsub.YamlSubsetError):
        yamlsub.loads(source)


def test_yaml_refuses_to_write_an_unsupported_type():
    with pytest.raises(yamlsub.YamlSubsetError):
        yamlsub.dumps({"floats_lose_precision": 1.5})


# --------------------------------------------------------------------------
# protoparse: the normal form must ignore formatting and catch real changes
# --------------------------------------------------------------------------

_MINIMAL = """
syntax = "proto3";
package marketdata.v1;
message Tick { optional int64 price_ticks = 1; }
"""


def test_normal_form_ignores_comments_and_whitespace():
    reformatted = """
    // a comment that changes nothing
    syntax = "proto3";

    package   marketdata.v1;

    /* block comment */
    message Tick {
        optional int64 price_ticks = 1;   // trailing note
    }
    """
    left = protoparse.parse_text(_MINIMAL)
    right = protoparse.parse_text(reformatted)
    assert left.file_sha256 != right.file_sha256
    assert left.normalized_sha256() == right.normalized_sha256()
    assert protoparse.diff_contracts(left, right) == []


@pytest.mark.parametrize(
    "mutation, expect_in_diff",
    [
        ("optional int64 price_ticks = 1;", None),        # control: unchanged
        ("optional uint64 price_ticks = 1;", "uint64"),   # wire-compatible, still breaking
        ("optional int64 price_ticks = 2;", "field"),     # renumbered
        ("optional int64 px = 1;", "px"),                 # renamed
        ("repeated int64 price_ticks = 1;", "repeated"),  # label changed
        ("int64 price_ticks = 1;", "singular"),           # explicit presence dropped
    ],
)
def test_normal_form_detects_contract_changes(mutation, expect_in_diff):
    base = protoparse.parse_text(_MINIMAL)
    candidate = protoparse.parse_text(
        _MINIMAL.replace("optional int64 price_ticks = 1;", mutation)
    )
    diff = protoparse.diff_contracts(base, candidate)
    if expect_in_diff is None:
        assert diff == []
        assert candidate.normalized_sha256() == base.normalized_sha256()
        return
    assert diff, "mutation %r produced no diff" % mutation
    assert candidate.normalized_sha256() != base.normalized_sha256()
    assert any(expect_in_diff in line for line in diff), diff


@pytest.mark.parametrize(
    "source",
    [
        'syntax = "proto3";\npackage p;\nmessage M { int64 a = 19000; }',
        'syntax = "proto3";\npackage p;\nmessage M { int64 a = 1; int64 b = 1; }',
        "package p;\nmessage M { int64 a = 1; }",
        'syntax = "proto3";\npackage p;\nmessage M { Unknown a = 1; }',
        'syntax = "proto3";\npackage p;\nmessage M { /* unterminated',
    ],
)
def test_parser_refuses_rather_than_guessing(source):
    with pytest.raises(protoparse.ProtoParseError):
        protoparse.parse_text(source)


@requires_repo
def test_parser_rejects_every_p02_breaking_variant():
    """P02 ships deliberately-breaking schemas. None may normalise as equal."""
    base = protoparse.parse_file(CANONICAL)
    checked = 0
    for directory in sorted((P02_SCHEMAS / "evolution").iterdir()):
        candidate = directory / "marketdata" / "v1" / "market_data.proto"
        if not candidate.exists():
            continue
        checked += 1
        try:
            parsed = protoparse.parse_file(candidate)
        except protoparse.ProtoParseError:
            continue  # refusing to parse is also a rejection
        assert parsed.normalized_sha256() != base.normalized_sha256(), (
            "%s normalised identically to the canonical schema" % directory.name
        )
    assert checked >= 5, "expected several evolution variants, saw %d" % checked


# --------------------------------------------------------------------------
# probes: an unreadable fact must be unknown, never a default
# --------------------------------------------------------------------------

def test_absent_project_is_reported_absent(tmp_path):
    facts = probes.probe_project(tmp_path, "P04")
    assert facts.present is False
    assert facts.errors
    assert facts.get("ticks_topic") is None


def test_unreadable_toml_yields_unknown_not_zero(tmp_path):
    base = tmp_path / probes.PROJECT_DIRS["P04"]
    (base / "config").mkdir(parents=True)
    (base / "config" / "local-dev.example.toml").write_text(
        "this is not valid toml {{{", encoding="utf-8"
    )
    facts = probes.probe_project(tmp_path, "P04")
    fact = facts.facts["partitions_per_topic"]
    assert fact.known is False
    assert fact.value is None
    assert facts.errors


def test_empty_config_value_is_unknown_with_a_reason(tmp_path):
    base = tmp_path / probes.PROJECT_DIRS["P04"]
    (base / "config").mkdir(parents=True)
    (base / "config" / "local-dev.example.toml").write_text(
        '[run]\nproto_descriptor_sha256 = ""\n', encoding="utf-8"
    )
    facts = probes.probe_project(tmp_path, "P04")
    fact = facts.facts["descriptor_pin"]
    assert fact.known is False
    assert "not pinned" in (fact.unknown_reason or "")


# --------------------------------------------------------------------------
# audit: disagreement must be detectable, and a missing input must not pass
# --------------------------------------------------------------------------

@requires_repo
def test_audit_runs_and_every_finding_carries_evidence():
    result = audit.run_audit(REPO_ROOT, probes.probe_all(REPO_ROOT))
    assert result.findings
    assert result.verdict in (
        "M0_PASS",
        "M0_PASS_WITH_FINDINGS",
        "M0_INCONCLUSIVE",
        "M0_BLOCKED",
    )
    for finding in result.findings:
        assert finding.evidence, "%s has no evidence" % finding.id
        if finding.verdict != "PASS":
            assert finding.detail
    ids = [f.id for f in result.findings]
    assert len(ids) == len(set(ids)), "duplicate finding ids: %s" % ids


@requires_repo
def test_audit_detects_a_topic_name_disagreement():
    """The control is the real tree agreeing; the mutation gives it meaning."""
    facts = probes.probe_all(REPO_ROOT)
    before = {f.id: f.verdict for f in audit.run_audit(REPO_ROOT, facts).findings}
    assert before["P09-M0-09"] == "PASS"

    facts["P05"].add("ticks_topic", "md.ticks.v2", "synthetic mutation")
    after = {f.id: f.verdict for f in audit.run_audit(REPO_ROOT, facts).findings}
    assert after["P09-M0-09"] == "FAIL"


@requires_repo
def test_a_missing_fact_makes_a_check_inconclusive_not_passing():
    facts = probes.probe_all(REPO_ROOT)
    facts["P06"].unknown("bar_interval_ns", "synthetically removed")
    findings = {f.id: f for f in audit.run_audit(REPO_ROOT, facts).findings}
    bar = findings["P09-M0-14"]
    assert bar.verdict == "INCONCLUSIVE"
    assert bar.severity == "BLOCKER"


@requires_repo
def test_blockers_drive_the_overall_verdict():
    result = audit.run_audit(REPO_ROOT, probes.probe_all(REPO_ROOT))
    blocking = [f for f in result.findings if f.blocks_release]
    assert (result.verdict == "M0_BLOCKED") == bool(blocking)


def test_finding_rejects_an_unknown_verdict_or_severity():
    with pytest.raises(ValueError):
        audit.Finding("x", "t", "MAYBE", "HIGH", "d", "G0", "M0")
    with pytest.raises(ValueError):
        audit.Finding("x", "t", "PASS", "CATASTROPHIC", "d", "G0", "M0")


# --------------------------------------------------------------------------
# lockfile: a pinned contract is worthless if drift goes unnoticed
# --------------------------------------------------------------------------

@requires_repo
def test_lock_round_trips_and_reports_no_drift_against_itself(tmp_path):
    result = audit.run_audit(REPO_ROOT, probes.probe_all(REPO_ROOT))
    document = lockfile.build_lock(result)
    path = tmp_path / lockfile.LOCK_FILENAME
    lockfile.write_lock(path, document)

    drifted, _notes = lockfile.verify_lock(lockfile.load_lock(path), result)
    assert drifted == [], drifted


@requires_repo
@pytest.mark.parametrize(
    "dotted, value",
    [
        ("kafka.ticks_topic", "md.ticks.v2"),
        ("kafka.partitions_per_topic", 12),
        ("clickhouse.bar_interval_ns", 1),
        ("identity.semantic_digest", "P05-CORE-V2"),
        ("wire_schema.normalized_sha256", "0" * 64),
    ],
)
def test_lock_detects_a_tampered_cross_project_identity(dotted, value):
    result = audit.run_audit(REPO_ROOT, probes.probe_all(REPO_ROOT))
    document = lockfile.build_lock(result)
    node = document["cross_project_identity"]
    *parents, leaf = dotted.split(".")
    for part in parents:
        node = node[part]
    assert leaf in node, "test targets a key that does not exist: %s" % dotted
    node[leaf] = value

    drifted, _notes = lockfile.verify_lock(document, result)
    assert any(leaf in line for line in drifted), drifted


@requires_repo
def test_a_per_build_difference_is_a_note_not_drift():
    """Descriptor digests differ by design; that must not read as a violation."""
    result = audit.run_audit(REPO_ROOT, probes.probe_all(REPO_ROOT))
    document = lockfile.build_lock(result)
    document["per_build_identity"]["toolchains"]["P03"]["protobuf_root"] = "/elsewhere"

    drifted, notes = lockfile.verify_lock(document, result)
    assert drifted == []
    assert any("protobuf_root" in note for note in notes)


def test_lock_refuses_an_unknown_version(tmp_path):
    path = tmp_path / "contracts.lock.yaml"
    path.write_text("lock_version: 99\n", encoding="utf-8")
    with pytest.raises(ValueError, match="lock_version"):
        lockfile.load_lock(path)


# --------------------------------------------------------------------------
# deploy: the init jobs are derived from the lock, and judged separately
# --------------------------------------------------------------------------

def _minimal_lock() -> dict:
    return {
        "lock_version": 1,
        "generated_at_utc": "2026-10-31T00:00:00Z",
        "cross_project_identity": {
            "wire_schema": {"normalized_sha256": "a" * 64},
            "kafka": {
                "ticks_topic": "md.ticks.v1",
                "quotes_topic": "md.quotes.v1",
                "partitions_per_topic": 8,
                "cleanup_policy": "delete",
                "allow_auto_topic_create": False,
            },
            "clickhouse": {
                "raw_table": "md_ingest.market_events_raw",
                "canonical_table": "md_analytics.canonical_ticks",
                "bars_table": "md_analytics.bars_1m_state",
            },
        },
    }


def test_render_init_carries_the_locked_values_into_the_scripts(tmp_path):
    written = deploy.render_init(_minimal_lock(), REPO_ROOT, tmp_path)
    assert {path.name for path in written} == {
        "01-topics.sh",
        "02-apply-ddl.sh",
        "03-verify.sh",
        "README.md",
    }

    topics = (tmp_path / "01-topics.sh").read_text(encoding="utf-8")
    assert "PARTITIONS=8" in topics
    assert "md.ticks.v1" in topics and "md.quotes.v1" in topics
    assert 'CLEANUP="delete"' in topics
    # Create is not the check: a describe must follow it.
    assert "--describe" in topics
    assert "auto.create.topics.enable=true" in topics

    ddl = (tmp_path / "02-apply-ddl.sh").read_text(encoding="utf-8")
    for _owner, relative in deploy.DDL_SOURCES:
        assert relative in ddl
    # The SQL is referenced by path, never inlined.
    assert "CREATE TABLE" not in ddl


def test_render_init_changes_when_the_lock_changes(tmp_path):
    lock = _minimal_lock()
    deploy.render_init(lock, REPO_ROOT, tmp_path / "a")
    lock["cross_project_identity"]["kafka"]["partitions_per_topic"] = 16
    deploy.render_init(lock, REPO_ROOT, tmp_path / "b")

    assert "PARTITIONS=8" in (tmp_path / "a" / "01-topics.sh").read_text(encoding="utf-8")
    assert "PARTITIONS=16" in (tmp_path / "b" / "01-topics.sh").read_text(encoding="utf-8")


@pytest.mark.parametrize("drop", ["ticks_topic", "partitions_per_topic", "cleanup_policy"])
def test_render_init_refuses_an_incomplete_lock(tmp_path, drop):
    lock = _minimal_lock()
    del lock["cross_project_identity"]["kafka"][drop]
    with pytest.raises(deploy.DeployError):
        deploy.render_init(lock, REPO_ROOT, tmp_path)


def _observation(**overrides) -> dict:
    base = {
        "generated_at_utc": "2026-10-31T00:05:00Z",
        "cluster_id": "p09DevClusterAAAA",
        "clickhouse_version": "25.3.3.42",
        "topics": [
            {"name": "md.ticks.v1", "partitions": 8, "replication_factor": 1,
             "cleanup_policy": "delete"},
            {"name": "md.quotes.v1", "partitions": 8, "replication_factor": 1,
             "cleanup_policy": "delete"},
        ],
        "tables": [
            {"name": "md_ingest.market_events_raw", "columns": "dataset_id,symbol"},
            {"name": "md_analytics.canonical_ticks", "columns": "build_id,symbol"},
            {"name": "md_analytics.bars_1m_state", "columns": "build_id,symbol"},
        ],
    }
    base.update(overrides)
    return base


def test_a_matching_deployment_passes_g2():
    findings, verdict = deploy.compare_infra(_minimal_lock(), _observation())
    assert verdict == "G2_PASS"
    assert all(f.verdict == "PASS" for f in findings)


def test_the_wrong_partition_count_fails_g2():
    observed = _observation(
        topics=[
            {"name": "md.ticks.v1", "partitions": 4, "cleanup_policy": "delete"},
            {"name": "md.quotes.v1", "partitions": 8, "cleanup_policy": "delete"},
        ]
    )
    findings, verdict = deploy.compare_infra(_minimal_lock(), observed)
    assert verdict == "G2_NO_GO"
    failed = [f for f in findings if f.verdict == "FAIL"]
    assert len(failed) == 1
    assert "md.ticks.v1" in failed[0].subject


def test_a_compacted_topic_fails_g2():
    observed = _observation(
        topics=[
            {"name": "md.ticks.v1", "partitions": 8, "cleanup_policy": "compact"},
            {"name": "md.quotes.v1", "partitions": 8, "cleanup_policy": "delete"},
        ]
    )
    _findings, verdict = deploy.compare_infra(_minimal_lock(), observed)
    assert verdict == "G2_NO_GO"


def test_a_missing_table_fails_g2():
    observed = _observation(
        tables=[
            {"name": "md_ingest.market_events_raw", "columns": "dataset_id"},
            {"name": "md_analytics.canonical_ticks", "columns": "build_id"},
            {"name": "md_analytics.bars_1m_state", "columns": ""},
        ]
    )
    findings, verdict = deploy.compare_infra(_minimal_lock(), observed)
    assert verdict == "G2_NO_GO"
    assert any("bars_1m_state" in f.subject and f.verdict == "FAIL" for f in findings)


def test_an_unobserved_topic_is_inconclusive_not_passing():
    findings, verdict = deploy.compare_infra(_minimal_lock(), _observation(topics=[]))
    assert verdict == "G2_INCONCLUSIVE"
    assert all(
        f.verdict == "INCONCLUSIVE" for f in findings if f.subject.startswith("topic")
    )


def test_an_empty_observation_never_passes():
    _findings, verdict = deploy.compare_infra(_minimal_lock(), {})
    assert verdict != "G2_PASS"


@pytest.mark.parametrize("text", ["not json", "[]", "null", '{"topics": '])
def test_a_malformed_observation_is_refused(text):
    with pytest.raises(deploy.DeployError):
        deploy.parse_verify_report(text)


def test_infra_report_is_json_serialisable():
    report = deploy.infra_report(_minimal_lock(), _observation())
    assert json.loads(json.dumps(report))["verdict"] == "G2_PASS"


# --------------------------------------------------------------------------
# descriptors: the pins differ by encoding, and each one must be derivable
# --------------------------------------------------------------------------

requires_protobuf = pytest.mark.skipif(
    not descriptors.available(), reason="the protobuf runtime is not installed"
)
BASELINE = REPO_ROOT / descriptors.CANONICAL_BASELINE

requires_baseline = pytest.mark.skipif(
    not BASELINE.exists(), reason="P02 descriptor baseline is not present"
)


@requires_protobuf
@requires_baseline
def test_every_project_pin_derives_from_the_one_canonical_descriptor():
    """The point of ADR-0001: incomparable values, all derivable from one schema."""
    pins = descriptors.expected_pins(REPO_ROOT)
    assert set(pins) == set(descriptors.PROJECT_REPRESENTATION)

    facts = probes.probe_all(REPO_ROOT)
    for project, expected in pins.items():
        key = "descriptor_sha256" if project == "P02" else "descriptor_pin"
        actual = facts[project].get(key)
        if actual is None:
            continue  # unpinned is finding P09-M0-04's business, not this test's
        assert actual == expected, (
            "%s pins %s but its encoding of the canonical schema yields %s"
            % (project, actual, expected)
        )


@requires_protobuf
@requires_baseline
def test_the_three_encodings_really_do_differ():
    """If they agreed, the whole derivation would be pointless machinery."""
    canonical = descriptors.load_canonical(REPO_ROOT)
    digests = {
        name: descriptors.digest_for(canonical, name)
        for name in descriptors.REPRESENTATIONS
    }
    assert len(set(digests.values())) == len(digests), digests


@requires_protobuf
@requires_baseline
def test_p04_and_p05_share_an_encoding_and_therefore_a_digest():
    """Two runtimes, one encoding, one value: the evidence the runtimes agree."""
    assert (
        descriptors.PROJECT_REPRESENTATION["P04"]
        == descriptors.PROJECT_REPRESENTATION["P05"]
    )
    pins = descriptors.expected_pins(REPO_ROOT)
    assert pins["P04"] == pins["P05"]


@requires_protobuf
@requires_baseline
def test_identity_representation_is_one_of_the_known_encodings():
    assert descriptors.IDENTITY_REPRESENTATION in descriptors.REPRESENTATIONS
    canonical = descriptors.load_canonical(REPO_ROOT)
    assert descriptors.canonical_identity(canonical) == descriptors.digest_for(
        canonical, descriptors.IDENTITY_REPRESENTATION
    )


@requires_protobuf
def test_a_missing_baseline_raises_rather_than_returning_a_digest(tmp_path):
    with pytest.raises(descriptors.DescriptorError):
        descriptors.load_canonical(tmp_path)


@requires_protobuf
@requires_baseline
def test_an_unknown_representation_is_refused():
    canonical = descriptors.load_canonical(REPO_ROOT)
    with pytest.raises(descriptors.DescriptorError):
        descriptors.digest_for(canonical, "file_descriptor_set_invented")


@requires_protobuf
@requires_baseline
def test_a_changed_schema_changes_every_derived_pin():
    """The check has to fail when the schema really moves, in every encoding."""
    canonical = descriptors.load_canonical(REPO_ROOT)
    before = {
        name: descriptors.digest_for(canonical, name)
        for name in descriptors.REPRESENTATIONS
    }
    # Renumber a field: wire-breaking, and invisible to nothing.
    canonical.message_type[0].field[0].number += 50
    after = {
        name: descriptors.digest_for(canonical, name)
        for name in descriptors.REPRESENTATIONS
    }
    for name in before:
        assert before[name] != after[name], name


@requires_repo
def test_audit_detects_a_tampered_descriptor_pin():
    facts = probes.probe_all(REPO_ROOT)
    before = {f.id: f.verdict for f in audit.run_audit(REPO_ROOT, facts).findings}
    if before["P09-M0-03"] != "PASS":
        pytest.skip("P09-M0-03 is not currently passing; nothing to tamper with")

    facts["P05"].add("descriptor_pin", "f" * 64, "synthetic mutation")
    after = {f.id: f for f in audit.run_audit(REPO_ROOT, facts).findings}
    assert after["P09-M0-03"].verdict == "FAIL"
    assert after["P09-M0-03"].severity == "BLOCKER"


# --------------------------------------------------------------------------
# pipeline: the runner is generated, so its guard rails are testable
# --------------------------------------------------------------------------

def _runner(tmp_path) -> str:
    written = pipeline.render_pipeline(_minimal_lock(), REPO_ROOT, tmp_path)
    assert {path.name for path in written} == {"run-pipeline.sh", "STAGES.md"}
    return (tmp_path / "run-pipeline.sh").read_text(encoding="utf-8")


def test_the_runner_covers_every_declared_stage(tmp_path):
    script = _runner(tmp_path)
    # Each declared stage must actually appear as a recorded step, or STAGES.md
    # documents a pipeline the runner does not run.
    for stage in pipeline.STAGES:
        assert stage.id in script, stage.id


def test_every_container_stage_is_guarded(tmp_path):
    """The defect this prevents: a stage without `|| stage_failed`.

    The runner sets `-e`, so an unguarded failure aborts the whole script. That
    happened: stage 10 crashed, stages 11-13 never ran, and the run report was
    left without a verdict -- evidence that stops mid-sentence.
    """
    script = _runner(tmp_path)
    lines = script.splitlines()
    newline = chr(10)
    backslash = chr(92)
    unguarded: list[str] = []

    for index, line in enumerate(lines):
        if not line.startswith(("dr ", '"$P03_BIN"', '"$P04_BIN"')):
            continue
        # Walk to the end of this command. A heredoc runs to its terminator and
        # reports through an explicit RC check after it, so the window has to
        # reach past the body rather than guess at a line count.
        end = index
        if "<<'PY'" in line:
            while end < len(lines) - 1 and lines[end].strip() != "PY":
                end += 1
        else:
            while end < len(lines) - 1 and lines[end].rstrip().endswith(backslash):
                end += 1
        window = newline.join(lines[index : min(end + 4, len(lines))])
        if "stage_failed" not in window:
            unguarded.append(lines[index])

    assert not unguarded, "unguarded stages:" + newline + newline.join(unguarded)


def test_the_runner_closes_its_report_on_any_exit(tmp_path):
    """A backstop for the stages a future edit forgets to guard."""
    script = _runner(tmp_path)
    assert "trap on_exit EXIT" in script
    assert "REPORT_CLOSED" in script
    assert "ABORTED" in script


def test_the_runner_passes_the_locked_values_into_the_container(tmp_path):
    """Stage 10 crashed on KeyError: RAW_TABLE because dr() passed no env."""
    script = _runner(tmp_path)
    assert 'docker run --rm -i --network host' in script
    for name in ("RAW_TABLE", "CH_HTTP", "BOOTSTRAP", "TICKS", "QUOTES"):
        assert '-e "%s=$%s"' % (name, name) in script, name


def test_the_runner_carries_the_locked_contract_not_defaults(tmp_path):
    lock = _minimal_lock()
    lock["cross_project_identity"]["kafka"]["ticks_topic"] = "md.ticks.v9"
    lock["cross_project_identity"]["clickhouse"]["raw_table"] = "other.table"
    pipeline.render_pipeline(lock, REPO_ROOT, tmp_path / "b")
    script = (tmp_path / "b" / "run-pipeline.sh").read_text(encoding="utf-8")
    assert 'TICKS="md.ticks.v9"' in script
    assert 'RAW_TABLE="other.table"' in script


def test_p06_runs_build_from_raw_not_build(tmp_path):
    """`build` synthesises its own corpus; only build-from-raw reads P05."""
    script = _runner(tmp_path)
    assert "build-from-raw" in script
    assert "--p03-handoff" in script and "--p05-cut" in script


def test_p05_ingest_has_a_stop_condition(tmp_path):
    """Without --max-idle-steps P05 polls forever and the pipeline hangs."""
    script = _runner(tmp_path)
    assert "--max-idle-steps" in script


def test_render_pipeline_refuses_an_incomplete_lock(tmp_path):
    lock = _minimal_lock()
    del lock["cross_project_identity"]["clickhouse"]["raw_table"]
    with pytest.raises(deploy.DeployError):
        pipeline.render_pipeline(lock, REPO_ROOT, tmp_path)
