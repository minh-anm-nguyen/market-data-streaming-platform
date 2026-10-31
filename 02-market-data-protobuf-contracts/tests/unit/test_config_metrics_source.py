"""Config validation, stage reconciliation and source verification.

Covers the operational guards: a config that would weaken a stated guarantee is
refused at load, the stage identities catch events going missing between
stages, and an uncommitted Project 01 run is refused as a source (L02, L03).
"""

from __future__ import annotations

import json

import pytest

from md_contract.config import Config
from md_contract.errors import ConfigError, SourceArtifactError
from md_contract.metrics import Histogram, Metrics, merge
from md_contract.source import FaultIndex, SourceDataset
from md_contract.validation import ValidationMode


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------


def test_defaults_match_the_documented_budgets() -> None:
    config = Config()
    assert config.contract.max_payload_bytes == 4096
    assert config.contract.max_json_line_bytes == 4096
    assert config.stream.max_queue_events == 2048
    assert config.stream.max_queue_bytes == 8 * 1024 * 1024
    assert config.stream.port == 9010, "a different port from Project 01's 9009"
    assert config.validation.mode is ValidationMode.STRICT_NORMAL


def test_unknown_keys_are_rejected_not_ignored() -> None:
    # The failure this prevents: a typo'd key silently leaving the default in
    # place, so the config file does not describe the run it produced.
    with pytest.raises(ConfigError) as excinfo:
        Config.from_dict({"contract": {"max_payload_byte": 4096}})
    assert "unknown keys" in str(excinfo.value)
    assert "max_payload_byte" in str(excinfo.value)

    with pytest.raises(ConfigError):
        Config.from_dict({"nonsense": {}})


@pytest.mark.parametrize("value", [0, -1, -4096])
def test_non_positive_budgets_are_refused(value: int) -> None:
    with pytest.raises(ConfigError):
        Config.from_dict({"contract": {"max_payload_bytes": value}})


@pytest.mark.parametrize("field", ["prefix_header_deadline_ms", "payload_deadline_ms"])
def test_a_zero_deadline_is_refused_rather_than_meaning_forever(field: str) -> None:
    # Treating 0 as "no limit" is how a slowloris client holds a connection
    # open indefinitely (cases D12, I14).
    with pytest.raises(ConfigError):
        Config.from_dict({"stream": {field: 0}})


@pytest.mark.cases("J09")
def test_a_non_loopback_bind_needs_an_explicit_decision() -> None:
    with pytest.raises(ConfigError) as excinfo:
        Config.from_dict({"stream": {"host": "0.0.0.0"}})
    assert "allow_public_bind" in str(excinfo.value)
    # Still possible, but only when stated outright.
    config = Config.from_dict({"stream": {"host": "0.0.0.0", "allow_public_bind": True}})
    assert config.stream.host == "0.0.0.0"
    # Loopback needs no override.
    assert Config.from_dict({"stream": {"host": "127.0.0.1"}}).stream.host == "127.0.0.1"


def test_a_queue_budget_too_small_for_one_frame_is_refused() -> None:
    with pytest.raises(ConfigError) as excinfo:
        Config.from_dict({"stream": {"max_queue_bytes": 100}})
    assert "maximum frame" in str(excinfo.value)


def test_fault_passthrough_requires_the_manifest_restriction() -> None:
    with pytest.raises(ConfigError) as excinfo:
        Config.from_dict(
            {
                "validation": {
                    "mode": "fault_passthrough",
                    "allow_only_manifest_faults": False,
                }
            }
        )
    assert "defeats the purpose" in str(excinfo.value)


def test_an_unsupported_frame_protocol_or_source_version_is_refused() -> None:
    with pytest.raises(ConfigError):
        Config.from_dict({"contract": {"frame_protocol": "MDP2"}})
    with pytest.raises(ConfigError):
        Config.from_dict({"contract": {"source_schema_version": "sim.v2"}})


def test_an_unknown_mode_is_refused_with_the_valid_options_named() -> None:
    with pytest.raises(ConfigError) as excinfo:
        Config.from_dict({"validation": {"mode": "lenient"}})
    assert "strict_normal" in str(excinfo.value)


def test_with_mode_revalidates_the_whole_config() -> None:
    config = Config()
    switched = config.with_mode("fault_passthrough")
    assert switched.validation.mode is ValidationMode.FAULT_PASSTHROUGH
    assert config.validation.mode is ValidationMode.STRICT_NORMAL, "the original is unchanged"


def test_example_configs_on_disk_load(project_root) -> None:
    for name in ("example.toml", "benchmark.toml", "fault_passthrough.toml"):
        path = project_root / "config" / name
        if path.exists():
            config = Config.load(path)
            assert config.source_path == path


# ---------------------------------------------------------------------------
# metrics and reconciliation
# ---------------------------------------------------------------------------


def test_stage_identities_balance_on_a_clean_run() -> None:
    m = Metrics()
    m.incr("json_lines_read_total", 100)
    m.incr("mapped_total", 100)
    m.incr("protobuf_encoded_total", 100)
    m.incr("frames_received_total", 100)
    m.incr("payloads_submitted_total", 100)
    m.incr("protobuf_parsed_total", 100)
    m.incr("events_accepted_total", 100)
    assert m.reconciles
    assert m.unbalanced() == []


def test_an_unbalanced_identity_names_the_stage_that_lost_events() -> None:
    m = Metrics()
    m.incr("json_lines_read_total", 100)
    m.incr("mapped_total", 97)  # 3 vanished with no rejection recorded
    unbalanced = m.unbalanced()
    assert not m.reconciles
    assert any("json_lines_read" in r.name for r in unbalanced)
    offender = next(r for r in unbalanced if "json_lines_read" in r.name)
    assert (offender.left, offender.right) == (100, 97)


def test_rejections_are_counted_against_both_stage_and_reason() -> None:
    m = Metrics()
    m.incr("json_lines_read_total", 10)
    m.reject("json", "DUPLICATE_JSON_KEY")
    m.reject("json", "FLOAT_NOT_ALLOWED")
    m.incr("mapped_total", 8)
    # Every downstream stage has to be accounted for too, otherwise its identity
    # is left unbalanced -- which is the behaviour this module is for: 8 events
    # arriving at a stage and 0 leaving it is a loss, not a clean run.
    m.reject("policy", "BUSINESS_VIOLATION")
    m.incr("protobuf_encoded_total", 7)
    assert m.reconciles, "rejections close each identity"
    assert m.get("json_rejected_total") == 2
    assert m.get("policy_rejected_total") == 1
    assert m.reasons["DUPLICATE_JSON_KEY"] == 1
    assert m.reasons["BUSINESS_VIOLATION"] == 1


def test_no_peer_accepted_counter_is_ever_reported() -> None:
    # MDP1 has no application ACK, so a counter implying delivery would be a
    # lie about what a socket write proves (FR-13).
    payload = Metrics().as_dict()
    assert payload["peer_accepted_total"] is None
    assert "ACK" in payload["peer_accepted_note"]


def test_percentiles_are_exact_and_say_so() -> None:
    h = Histogram("t")
    for value in range(1, 101):
        h.observe(float(value))
    assert h.exact
    assert h.percentile(0.50) == pytest.approx(50.5)
    assert h.percentile(0.99) == pytest.approx(99.01, abs=0.1)
    summary = h.summary()
    # p50, p95 and p99 are always present: a mean alone hides the tail that GC
    # pauses and CPU throttling produce (section 10.3).
    assert {"p50", "p95", "p99", "mean", "min", "max"} <= set(summary)


def test_a_capped_histogram_admits_it_is_no_longer_exact() -> None:
    h = Histogram("t", cap=10)
    for value in range(20):
        h.observe(float(value))
    assert not h.exact
    assert h.dropped == 10
    assert h.summary()["exact"] is False


def test_merging_adds_counters_and_takes_the_max_of_peaks() -> None:
    a, b = Metrics(), Metrics()
    a.incr("events_accepted_total", 5)
    b.incr("events_accepted_total", 7)
    a.gauge("queue_depth_bytes", 100)
    b.gauge("queue_depth_bytes", 400)
    a.observe("decode_us", 1.0)
    b.observe("decode_us", 2.0)
    merged = merge([a, b])
    assert merged.get("events_accepted_total") == 12
    assert merged.peaks["queue_depth_bytes"] == 400
    assert merged.histograms["decode_us"].summary()["count"] == 2


# ---------------------------------------------------------------------------
# source verification
# ---------------------------------------------------------------------------


@pytest.mark.cases("L01")
def test_a_committed_project_01_run_loads_and_verifies(fault_run_dir) -> None:
    dataset = SourceDataset.load(fault_run_dir)
    assert dataset.completed
    assert dataset.schema_version == "sim.v1"
    assert dataset.dataset_id.startswith("ds-")
    assert dataset.events_lines == 5000
    # The hash is recomputed from the file, not taken from the marker.
    assert len(dataset.events_sha256) == 64
    assert dataset.faults_enabled
    assert len(dataset.fault_index.records) == 8


@pytest.mark.cases("L02")
def test_a_run_without_a_completed_marker_is_refused(fault_run_dir, tmp_path) -> None:
    import shutil

    staged = tmp_path / "uncommitted"
    staged.mkdir()
    for name in ("events.jsonl", "manifest.json"):
        shutil.copy(fault_run_dir / name, staged / name)
    # No COMPLETED marker: a run killed before it committed looks exactly like
    # this, and converting it would launder a possibly truncated file.
    with pytest.raises(SourceArtifactError) as excinfo:
        SourceDataset.load(staged)
    assert "COMPLETED" in str(excinfo.value)

    # The escape hatch exists only so this test can prove the default refuses.
    assert SourceDataset.load(staged, require_completed=False).completed is False


def test_a_file_modified_after_commit_fails_the_hash_check(fault_run_dir, tmp_path) -> None:
    import shutil

    staged = tmp_path / "tampered"
    staged.mkdir()
    for name in ("events.jsonl", "manifest.json", "COMPLETED"):
        shutil.copy(fault_run_dir / name, staged / name)
    with (staged / "events.jsonl").open("ab") as handle:
        handle.write(b'{"schema_version":"sim.v1"}\n')

    with pytest.raises(SourceArtifactError) as excinfo:
        SourceDataset.load(staged)
    assert "sha256" in str(excinfo.value)


@pytest.mark.cases("L03")
def test_passthrough_on_a_dataset_with_no_fault_manifest_is_refused(
    clean_run_dir,
) -> None:
    dataset = SourceDataset.load(clean_run_dir)
    assert dataset.fault_index.records == ()
    with pytest.raises(ConfigError) as excinfo:
        dataset.require_fault_manifest()
    assert "fault_passthrough requires" in str(excinfo.value)


def test_the_fault_index_reads_project_01_manifest_rows(fault_run_dir) -> None:
    index = FaultIndex.load(fault_run_dir / "fault-manifest.jsonl")
    assert index.applied_count == 8
    kinds = {r.kind for r in index.records}
    assert {"invalid_price", "invalid_spread", "stale_timestamp"} <= kinds
    # A reorder fault names two affected sequences, and both must be indexed.
    reorder = next(r for r in index.records if r.kind == "reorder_adjacent")
    assert len(reorder.sequences) == 2


def test_a_missing_fault_manifest_yields_an_empty_index(tmp_path) -> None:
    # Empty, not an error: a clean dataset legitimately has no manifest. The
    # refusal happens when a mode *needs* one (above), not on load.
    assert FaultIndex.load(tmp_path / "nope.jsonl").records == ()
