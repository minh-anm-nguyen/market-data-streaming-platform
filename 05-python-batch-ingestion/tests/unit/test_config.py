"""Configuration: the fail-closed startup gates and the effective fingerprint."""

from __future__ import annotations

import pytest

from p05_ingest.config import (
    DESCRIPTOR_PLACEHOLDER,
    AppConfig,
    BatchConfig,
    ClickHouseConfig,
    KafkaConfig,
    ProjectConfig,
)
from p05_ingest.errors import ConfigError

VALID_HASH = "ab" * 32


def raw_config(**sections) -> dict:
    base = {
        "project": {
            "project": "p05",
            "mode": "market_valid",
            "run_id": "r1",
            "topic_generation": "gen-a",
        },
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "group_id": "g1",
            "topics": ["md.ticks.v1", "md.quotes.v1"],
        },
        "contracts": {
            "protobuf_package": "marketdata.v1",
            "protobuf_descriptor_sha256": VALID_HASH,
        },
    }
    for name, values in sections.items():
        base.setdefault(name, {}).update(values)
    return base


def test_the_shipped_dev_profile_loads(dev_config):
    assert dev_config.project.project == "p05"
    assert dev_config.clickhouse.table_fqn == "md_ingest.market_events_raw"
    assert dev_config.clickhouse.insert_mode == "sync"


def test_every_shipped_profile_passes_its_own_gates():
    """A profile nobody loads is a profile that has quietly rotted.

    Each one must satisfy the same fail-closed checks as the dev profile, so a
    reliability or capacity run cannot start from a configuration that the
    correctness rules would reject.
    """
    from pathlib import Path

    from p05_ingest.proto_schema import descriptor_sha256

    config_dir = Path(__file__).resolve().parents[2] / "config"
    profiles = sorted(config_dir.glob("*.toml"))
    assert len(profiles) >= 3, f"expected the three lab profiles, found {profiles}"

    for path in profiles:
        cfg = AppConfig.from_toml(path)
        assert cfg.kafka.auto_offset_reset in ("error", "none"), path.name
        assert cfg.kafka.enable_auto_commit is False, path.name
        assert cfg.batch.max_inflight_batches_per_partition == 1, path.name
        assert len(cfg.fingerprint()) == 64, path.name
        # Every profile must pin the schema this build actually compiled, or a
        # run started from it would fail preflight rather than at load time.
        assert cfg.contracts.protobuf_descriptor_sha256 == descriptor_sha256(), (
            f"{path.name} pins a descriptor hash that is not the compiled one"
        )

    # The non-local profiles must require TLS: `secure = false` is only
    # defensible for a loopback lab.
    for path in profiles:
        cfg = AppConfig.from_toml(path)
        if "lab" in path.stem:
            assert cfg.clickhouse.secure, (
                f"{path.name} runs against remote hosts and must set secure = true"
            )


def test_profile_fingerprints_are_distinct():
    """Two profiles that fingerprint alike would be indistinguishable in a report."""
    from pathlib import Path

    config_dir = Path(__file__).resolve().parents[2] / "config"
    fingerprints = {
        path.stem: AppConfig.from_toml(path).fingerprint()
        for path in sorted(config_dir.glob("*.toml"))
    }
    assert len(set(fingerprints.values())) == len(fingerprints), fingerprints


def test_minimal_valid_config_loads():
    cfg = AppConfig.from_dict(raw_config())
    assert cfg.batch.per_partition_max_rows == 5000, "documented default"


# ------------------------------------------------------------ offset safety


@pytest.mark.parametrize("policy", ["latest", "earliest", "beginning", "end", ""])
def test_unsafe_offset_reset_is_refused(policy):
    """Spec I-11: latest/earliest silently skip or re-read on an expired offset."""
    with pytest.raises(ConfigError, match="auto_offset_reset"):
        AppConfig.from_dict(raw_config(kafka={"auto_offset_reset": policy}))


@pytest.mark.parametrize("policy", ["error", "none"])
def test_safe_offset_reset_is_accepted(policy):
    AppConfig.from_dict(raw_config(kafka={"auto_offset_reset": policy}))


def test_auto_commit_is_refused():
    with pytest.raises(ConfigError, match="enable_auto_commit"):
        AppConfig.from_dict(raw_config(kafka={"enable_auto_commit": True}))


def test_auto_offset_store_is_refused():
    """Storing offsets on poll decouples the commit from the sink decision."""
    with pytest.raises(ConfigError, match="enable_auto_offset_store"):
        AppConfig.from_dict(raw_config(kafka={"enable_auto_offset_store": True}))


def test_auto_topic_creation_is_refused():
    with pytest.raises(ConfigError, match="allow_auto_create_topics"):
        AppConfig.from_dict(raw_config(kafka={"allow_auto_create_topics": True}))


def test_topic_outside_the_contract_allowlist_is_refused():
    with pytest.raises(ConfigError, match="allowlist"):
        AppConfig.from_dict(raw_config(kafka={"topics": ["md.other.v1"]}))


def test_duplicate_topics_are_refused():
    with pytest.raises(ConfigError, match="duplicates"):
        AppConfig.from_dict(
            raw_config(kafka={"topics": ["md.ticks.v1", "md.ticks.v1"]})
        )


def test_empty_topic_list_is_refused():
    with pytest.raises(ConfigError, match="at least one topic"):
        AppConfig.from_dict(raw_config(kafka={"topics": []}))


@pytest.mark.parametrize("timeout", [0, -1, 1001, 5000])
def test_unreasonable_poll_timeout_is_refused(timeout):
    """A long poll timeout starves the completion drain and the commit path."""
    with pytest.raises(ConfigError, match="poll_timeout_ms"):
        AppConfig.from_dict(raw_config(kafka={"poll_timeout_ms": timeout}))


def test_poll_interval_must_leave_margin_over_the_worst_sink_stall():
    """Spec NFR-08: otherwise the group rebalances on slowness alone."""
    with pytest.raises(ConfigError, match="max_poll_interval_ms"):
        AppConfig.from_dict(
            raw_config(
                kafka={"max_poll_interval_ms": 30_000},
                retry={"overall_deadline_s": 30},
                clickhouse={"insert_timeout_s": 10},
            )
        )


# --------------------------------------------------------------- schema pin


def test_descriptor_placeholder_is_refused():
    with pytest.raises(ConfigError, match="placeholder"):
        AppConfig.from_dict(
            raw_config(
                contracts={"protobuf_descriptor_sha256": DESCRIPTOR_PLACEHOLDER}
            )
        )


@pytest.mark.parametrize("value", ["abc", "z" * 64, "AB" * 31])
def test_malformed_descriptor_hash_is_refused(value):
    with pytest.raises(ConfigError, match="descriptor_sha256"):
        AppConfig.from_dict(
            raw_config(contracts={"protobuf_descriptor_sha256": value})
        )


# ------------------------------------------------------------- insert safety


def test_async_insert_without_wait_is_refused():
    """Spec NFR-09: the ACK would only prove server-side buffering."""
    with pytest.raises(ConfigError, match="wait_for_async_insert"):
        AppConfig.from_dict(
            raw_config(
                clickhouse={"async_insert": True, "wait_for_async_insert": False}
            )
        )


def test_async_insert_with_wait_is_accepted():
    cfg = AppConfig.from_dict(
        raw_config(clickhouse={"async_insert": True, "wait_for_async_insert": True})
    )
    assert cfg.clickhouse.insert_mode == "async_wait"


def test_non_positive_insert_timeout_is_refused():
    with pytest.raises(ConfigError, match="insert_timeout_s"):
        AppConfig.from_dict(raw_config(clickhouse={"insert_timeout_s": 0}))


# -------------------------------------------------------------- batch sanity


def test_more_than_one_inflight_batch_per_partition_is_refused():
    """Spec ADR-003: it invalidates the contiguous-frontier argument."""
    with pytest.raises(ConfigError, match="max_inflight_batches_per_partition"):
        AppConfig.from_dict(
            raw_config(batch={"max_inflight_batches_per_partition": 2})
        )


@pytest.mark.parametrize(
    "high,low",
    [(0.5, 0.5), (0.4, 0.6), (1.0, 0.5), (0.8, 0.0), (0.8, 1.0)],
)
def test_watermarks_must_give_hysteresis(high, low):
    with pytest.raises(ConfigError, match="watermarks"):
        AppConfig.from_dict(
            raw_config(
                batch={"pause_high_watermark": high, "resume_low_watermark": low}
            )
        )


def test_a_batch_larger_than_the_global_budget_is_refused():
    """Such a partition could never seal, so the run would hang."""
    with pytest.raises(ConfigError, match="exceeds"):
        AppConfig.from_dict(
            raw_config(
                batch={
                    "per_partition_max_serialized_bytes": 1024,
                    "app_buffer_budget_bytes": 512,
                }
            )
        )


@pytest.mark.parametrize("field", ["per_partition_max_rows", "max_wait_ms"])
def test_non_positive_batch_bounds_are_refused(field):
    with pytest.raises(ConfigError, match=field):
        AppConfig.from_dict(raw_config(batch={field: 0}))


# --------------------------------------------------------------- isolation


def test_fault_lane_must_not_write_to_the_market_table():
    """Spec I-12: deliberately anomalous data must stay out of analytics."""
    with pytest.raises(ConfigError, match="isolated_fault_test"):
        AppConfig.from_dict(raw_config(project={"mode": "isolated_fault_test"}))


def test_fault_lane_with_a_separate_table_is_accepted():
    cfg = AppConfig.from_dict(
        raw_config(
            project={"mode": "isolated_fault_test"},
            clickhouse={"table": "fault_events_raw"},
            quarantine={"mode": "durable_dlq", "topic": "md.ingest.dlq.v1"},
        )
    )
    assert cfg.project.is_fault_lane
    assert cfg.quarantine.mode == "durable_dlq"


def test_dlq_is_refused_in_the_market_lane():
    with pytest.raises(ConfigError, match="isolated profile"):
        AppConfig.from_dict(
            raw_config(quarantine={"mode": "durable_dlq", "topic": "t"})
        )


def test_unknown_run_mode_is_refused():
    with pytest.raises(ConfigError, match="mode must be one of"):
        AppConfig.from_dict(raw_config(project={"mode": "production"}))


@pytest.mark.parametrize("field", ["run_id", "topic_generation"])
def test_empty_run_identity_is_refused(field):
    with pytest.raises(ConfigError, match=field):
        AppConfig.from_dict(raw_config(project={field: ""}))


# ----------------------------------------------------------------- typos


def test_unknown_key_is_an_error_not_a_warning():
    """A typo would otherwise fall back to a default and invalidate the run."""
    with pytest.raises(ConfigError, match="unknown key"):
        AppConfig.from_dict(raw_config(kafka={"sesion_timeout_ms": 1000}))


def test_unknown_section_is_refused():
    raw = raw_config()
    raw["clickhous"] = {"host": "x"}
    with pytest.raises(ConfigError, match="unknown key"):
        AppConfig.from_dict(raw)


def test_missing_required_section_is_refused():
    raw = raw_config()
    del raw["kafka"]
    with pytest.raises(ConfigError, match=r"\[kafka\]"):
        AppConfig.from_dict(raw)


def test_section_must_be_a_table():
    raw = raw_config()
    raw["batch"] = 5
    with pytest.raises(ConfigError, match="must be a table"):
        AppConfig.from_dict(raw)


def test_missing_file_is_reported_clearly():
    with pytest.raises(ConfigError, match="not found"):
        AppConfig.from_toml("config/does-not-exist.toml")


def test_invalid_toml_is_reported_clearly(tmp_path):
    bad = tmp_path / "bad.toml"
    bad.write_text("[project\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="invalid TOML"):
        AppConfig.from_toml(bad)


# --------------------------------------------------------------- fingerprint


def test_fingerprint_is_stable_and_sensitive(dev_config):
    from tests.conftest import make_config

    assert dev_config.fingerprint() == dev_config.fingerprint()
    changed = make_config(dev_config, batch={"per_partition_max_rows": 4999})
    assert changed.fingerprint() != dev_config.fingerprint()


def test_fingerprint_ignores_the_names_of_secret_env_vars(dev_config):
    """Two machines naming the same credential differently must still match."""
    from tests.conftest import make_config

    renamed = make_config(
        dev_config,
        clickhouse={"user_env": "OTHER_USER", "password_env": "OTHER_PASS"},
    )
    assert renamed.fingerprint() == dev_config.fingerprint()


def test_effective_dict_never_contains_a_secret_value(dev_config, monkeypatch):
    monkeypatch.setenv("P05_CH_PASSWORD", "super-secret")
    blob = str(dev_config.effective_dict(redact=True))
    assert "super-secret" not in blob
    assert "<redacted-env-name>" in blob


def test_credentials_come_from_the_environment(dev_config, monkeypatch):
    monkeypatch.setenv("P05_CH_USER", "p05")
    monkeypatch.setenv("P05_CH_PASSWORD", "pw")
    assert dev_config.clickhouse.credentials() == ("p05", "pw")


def test_credentials_default_when_unset(dev_config, monkeypatch):
    monkeypatch.delenv("P05_CH_USER", raising=False)
    monkeypatch.delenv("P05_CH_PASSWORD", raising=False)
    assert dev_config.clickhouse.credentials() == ("default", "")


def test_secure_mode_requires_a_username(dev_config, monkeypatch):
    from dataclasses import replace

    from p05_ingest.errors import PreflightError

    monkeypatch.delenv("P05_CH_USER", raising=False)
    secure = replace(dev_config.clickhouse, secure=True)
    with pytest.raises(PreflightError, match="requires a username"):
        secure.credentials()


# ------------------------------------------------------------------ helpers


def test_topic_to_kind_maps_the_allowlist(dev_config):
    assert dev_config.contracts.topic_to_kind == {
        "md.ticks.v1": "tick",
        "md.quotes.v1": "quote",
    }


def test_defaults_match_the_documented_values():
    """Spec NFR-04/NFR-05: 5000 rows, 4 MiB, 2000 ms, 64 MiB, 4 in flight."""
    batch = BatchConfig()
    assert batch.per_partition_max_rows == 5000
    assert batch.per_partition_max_serialized_bytes == 4 * 1024 * 1024
    assert batch.max_wait_ms == 2000
    assert batch.app_buffer_budget_bytes == 64 * 1024 * 1024
    assert batch.max_inflight_batches_global == 4
    assert batch.pause_high_watermark == 0.80
    assert batch.resume_low_watermark == 0.50

    kafka = KafkaConfig(bootstrap_servers="x", group_id="g", topics=("md.ticks.v1",))
    assert kafka.enable_auto_commit is False
    assert kafka.auto_offset_reset == "error"

    ch = ClickHouseConfig()
    assert ch.async_insert is False
    assert ch.insert_timeout_s == 10

    project = ProjectConfig(
        project="p", mode="market_valid", run_id="r", topic_generation="g"
    )
    assert not project.is_fault_lane
