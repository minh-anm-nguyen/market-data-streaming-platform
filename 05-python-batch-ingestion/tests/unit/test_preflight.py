"""Startup gates: every check, and the WARN-versus-PASS distinction.

The distinction is the point of this file. Spec 0.2 forbids reporting an
unverified condition as proven, so a check that could not contact anything must
come back WARN and must not count toward ``report.ok``. A gate that silently
passed when it could not run would be worse than no gate.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from p05_ingest.errors import PreflightError
from p05_ingest.preflight import (
    CheckResult,
    PreflightReport,
    check_budget_sanity,
    check_fault_lane,
    check_insert_mode,
    check_offset_policy,
    check_quarantine_policy,
    check_retention_headroom,
    check_schema_pin,
    check_sink_schema,
    run_preflight,
)
from p05_ingest.proto_schema import descriptor_sha256
from tests.conftest import make_config


# ---------------------------------------------------------- result semantics


def test_a_failing_required_check_reports_fail():
    result = CheckResult("x", False, "because", required=True)
    assert result.status == "FAIL"


def test_a_failing_optional_check_reports_warn():
    """The whole reason optional checks exist: unverified is not failed."""
    result = CheckResult("x", False, "not evaluated", required=False)
    assert result.status == "WARN"


def test_a_passing_check_reports_pass():
    assert CheckResult("x", True, "fine").status == "PASS"


def test_report_ok_ignores_optional_failures():
    report = PreflightReport()
    report.add(CheckResult("required", True, ""))
    report.add(CheckResult("optional", False, "", required=False))
    assert report.ok
    assert report.failures == []


def test_report_ok_is_false_when_a_required_check_fails():
    report = PreflightReport()
    report.add(CheckResult("required", False, "broken"))
    assert not report.ok
    assert [r.name for r in report.failures] == ["required"]
    with pytest.raises(PreflightError, match="required: broken"):
        report.raise_if_failed()


def test_raise_if_failed_is_silent_on_success():
    report = PreflightReport()
    report.add(CheckResult("required", True, ""))
    report.raise_if_failed()


def test_render_lists_every_check_and_the_overall_result():
    report = PreflightReport()
    report.add(CheckResult("alpha", True, "ok"))
    report.add(CheckResult("beta", False, "nope", required=False))
    text = report.render()
    assert "[PASS] alpha" in text
    assert "[WARN] beta" in text
    assert "PREFLIGHT OK" in text


def test_render_reports_failure_when_a_gate_failed():
    report = PreflightReport()
    report.add(CheckResult("alpha", False, "nope"))
    assert "PREFLIGHT FAILED" in report.render()


# ------------------------------------------------------------- the gates


def test_schema_pin_passes_for_the_shipped_profile(dev_config):
    result = check_schema_pin(dev_config)
    assert result.ok
    assert descriptor_sha256()[:16] in result.detail


def test_schema_pin_fails_on_a_mismatch(dev_config):
    cfg = make_config(dev_config, contracts={"protobuf_descriptor_sha256": "ab" * 32})
    result = check_schema_pin(cfg)
    assert not result.ok
    assert result.required


def test_insert_mode_passes_for_sync(dev_config):
    result = check_insert_mode(dev_config)
    assert result.ok
    assert "async_insert=0" in result.detail


def test_insert_mode_passes_for_async_with_wait(dev_config):
    cfg = make_config(
        dev_config,
        clickhouse={"async_insert": True, "wait_for_async_insert": True},
    )
    result = check_insert_mode(cfg)
    assert result.ok
    assert "wait_for_async_insert=1" in result.detail


def test_offset_policy_gate_passes_for_a_safe_value(dev_config):
    result = check_offset_policy(dev_config)
    assert result.ok
    assert "auto.offset.reset=error" in result.detail


def test_an_unsafe_offset_policy_cannot_even_be_constructed(dev_config):
    """Defence in depth, and the outer layer wins.

    `check_offset_policy` exists as a visible gate in the doctor output, but an
    unsafe value never reaches it: `KafkaConfig.__post_init__` refuses to build
    the object at all, and `dataclasses.replace` re-runs that validation. So
    the gate can only ever be reached with a safe value.
    """
    from p05_ingest.errors import ConfigError

    with pytest.raises(ConfigError, match="auto_offset_reset"):
        replace(dev_config.kafka, auto_offset_reset="latest")


def test_the_gate_itself_rejects_an_unsafe_value(dev_config):
    """Checked against a stand-in, since the real object cannot hold one."""
    from types import SimpleNamespace

    stub = SimpleNamespace(kafka=SimpleNamespace(auto_offset_reset="latest"))
    result = check_offset_policy(stub)
    assert not result.ok
    assert result.required


def test_fault_lane_gate_for_the_market_lane(dev_config):
    result = check_fault_lane(dev_config)
    assert result.ok
    assert "market_valid" in result.detail


def test_fault_lane_gate_for_an_isolated_profile(dev_config):
    cfg = make_config(
        dev_config,
        project={"mode": "isolated_fault_test"},
        clickhouse={"table": "fault_events_raw"},
        quarantine={"mode": "durable_dlq", "topic": "md.ingest.dlq.v1"},
    )
    result = check_fault_lane(cfg)
    assert result.ok
    assert "isolated_fault_test" in result.detail


def test_quarantine_gate_for_stop_mode(dev_config):
    result = check_quarantine_policy(dev_config)
    assert result.ok
    assert "fail-closed" in result.detail


def test_quarantine_gate_for_dlq_mode(dev_config):
    cfg = make_config(
        dev_config,
        project={"mode": "isolated_fault_test"},
        clickhouse={"table": "fault_events_raw"},
        quarantine={"mode": "durable_dlq", "topic": "md.ingest.dlq.v1"},
    )
    result = check_quarantine_policy(cfg)
    assert result.ok
    assert "md.ingest.dlq.v1" in result.detail


def test_budget_gate_states_that_it_is_not_an_rss_bound(dev_config):
    """The honesty requirement from I-05, surfaced to the operator."""
    result = check_budget_sanity(dev_config)
    assert result.ok
    assert "not" in result.detail and "RSS" in result.detail


# -------------------------------------------------------- unverifiable gates


def test_sink_schema_warns_when_there_is_no_sink(dev_config):
    result = check_sink_schema(dev_config, None)
    assert not result.ok
    assert not result.required
    assert result.status == "WARN"


def test_sink_schema_fails_when_the_sink_refuses(dev_config):
    from p05_ingest.testkit import FakeClickHouse

    sink = FakeClickHouse()
    sink.preflight_error = "market_events_raw is missing column(s): payload_digest"
    result = check_sink_schema(dev_config, sink)
    assert not result.ok
    assert "payload_digest" in result.detail


def test_sink_schema_passes_for_a_healthy_sink(dev_config):
    from p05_ingest.testkit import FakeClickHouse

    result = check_sink_schema(dev_config, FakeClickHouse())
    assert result.ok
    assert dev_config.clickhouse.table_fqn in result.detail


def test_retention_gate_warns_without_a_broker(dev_config):
    result = check_retention_headroom(dev_config, None)
    assert not result.ok
    assert not result.required
    assert "not evaluated" in result.detail


# ------------------------------------------------------------ the whole run


def test_run_preflight_passes_on_the_shipped_profile(dev_config):
    report = run_preflight(dev_config)
    assert report.ok, report.render()
    names = [r.name for r in report.results]
    assert names == [
        "protobuf descriptor pin",
        "insert acknowledgement mode",
        "offset reset policy",
        "fault lane isolation",
        "poison policy",
        "memory budget",
        "clickhouse table schema",
        "kafka retention headroom",
    ]


def test_run_preflight_without_live_services_warns_rather_than_passes(dev_config):
    report = run_preflight(dev_config)
    warned = {r.name for r in report.results if r.status == "WARN"}
    assert warned == {"clickhouse table schema", "kafka retention headroom"}
    assert report.ok, "a WARN must not block startup"


def test_run_preflight_fails_on_a_schema_mismatch(dev_config):
    cfg = make_config(dev_config, contracts={"protobuf_descriptor_sha256": "cd" * 32})
    report = run_preflight(cfg)
    assert not report.ok
    assert [r.name for r in report.failures] == ["protobuf descriptor pin"]


def test_preflight_never_creates_or_alters_anything():
    """Spec FR-18: preflight checks, it does not repair.

    A program that can reshape its own sink can hide a schema mismatch from the
    operator, so the module must contain no DDL at all.
    """
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[2] / "src" / "p05_ingest" / "preflight.py"
    ).read_text(encoding="utf-8")
    lowered = source.lower()
    for forbidden in ("create table", "alter table", "drop table", "create database"):
        assert forbidden not in lowered, f"preflight must not contain {forbidden!r}"
