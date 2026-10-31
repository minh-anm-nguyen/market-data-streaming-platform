"""Recovery decisions, injector receipts, and what the reports may claim."""

from __future__ import annotations

import pytest

from p07.contracts import TICK_TOPIC, SourceIdentity
from p07.dataplane.clickhouse_fake import FakeClickHouse
from p07.dataplane.kafka_fake import FakeKafka
from p07.dataplane.producer import Producer
from p07.evidence.verdicts import Reason, Verdict
from p07.injectors import clickhouse as ch_injectors
from p07.injectors import kafka as kafka_injectors
from p07.injectors import network as network_injectors
from p07.injectors import process as process_injectors
from p07.injectors.base import DEFAULT_ALLOWLIST, LabAllowlist, TargetNotAllowed, inject
from p07.injectors.resources import ResourceBudget, check_budget, rss_bytes
from p07.recovery.recovery_coordinator import (
    RecoveryMetrics,
    assess_replay,
    catch_up_seconds,
    choose_recovery,
    retention_headroom_ok,
)
from p07.replay.kafka_cut_scanner import KafkaCut
from p07.replay.source_replayer import TransportFaults
from p07.reporting.markdown_json import (
    render_release_report,
    render_summary,
    tally_verdicts,
)


def cut(kafka: FakeKafka, *, start: int = 0) -> KafkaCut:
    return KafkaCut(
        cluster_id=kafka.cluster_id,
        topic_generation=kafka.topic_generation,
        ranges={(TICK_TOPIC, 0): (start, kafka.high_watermark(TICK_TOPIC, 0))},
        fenced=True,
        captured_at_ns=1,
        partition_counts={TICK_TOPIC: kafka.partition_count(TICK_TOPIC)},
    )


def fill(kafka: FakeKafka, count: int = 5) -> None:
    for _ in range(count):
        kafka.produce(TICK_TOPIC, 0, key=b"EURUSD", value=b"payload")


# -- replay feasibility ---------------------------------------------------


def test_a_readable_range_and_a_sealed_source_are_feasible(kafka: FakeKafka) -> None:
    fill(kafka)
    assessment = assess_replay(kafka, cut(kafka), source_sealed=True, source_present=True)
    assert assessment.feasible
    assert assessment.kafka_range_available


@pytest.mark.whatif("P07-G01-08")
def test_losing_both_source_and_transport_is_blocked(kafka: FakeKafka) -> None:
    """Design 1.3: with neither, RPO=0 cannot be promised at all."""
    assessment = assess_replay(kafka, None, source_sealed=False, source_present=False)
    assert not assessment.feasible
    assert Reason.SOURCE_UNAVAILABLE in assessment.blocking
    assert assessment.findings()[0].reason == Reason.SOURCE_UNAVAILABLE


@pytest.mark.whatif("P07-G10-05")
def test_an_expired_range_is_fatal_only_without_the_source(kafka: FakeKafka) -> None:
    fill(kafka)
    old = cut(kafka)
    kafka.expire_to(TICK_TOPIC, 0, 3)

    with_source = assess_replay(kafka, old, source_sealed=True, source_present=True)
    assert with_source.feasible  # republish into a new generation
    assert not with_source.kafka_range_available

    without_source = assess_replay(kafka, old, source_sealed=False, source_present=False)
    assert not without_source.feasible


def test_schema_skew_blocks_replay(kafka: FakeKafka) -> None:
    assessment = assess_replay(
        kafka, None, source_sealed=True, source_present=True, schema_compatible=False
    )
    assert Reason.SOURCE_SCHEMA_SKEW in assessment.blocking


def test_recovery_prefers_resuming_then_republishing_then_restoring(kafka: FakeKafka) -> None:
    fill(kafka)
    resumable = assess_replay(kafka, cut(kafka), source_sealed=True, source_present=True)
    assert choose_recovery(resumable).method == "resume_and_reconcile"

    old = cut(kafka)
    kafka.expire_to(TICK_TOPIC, 0, 3)
    republish = assess_replay(kafka, old, source_sealed=True, source_present=True)
    assert choose_recovery(republish).method == "republish_new_generation"

    lost = assess_replay(kafka, None, source_sealed=False, source_present=False)
    assert choose_recovery(lost).method == "hold"
    assert (
        choose_recovery(lost, sink_data_lost=True, backup_restorable=True).method
        == "restore_backup"
    )


def test_holding_is_explicit_about_not_retrying(kafka: FakeKafka) -> None:
    """NFR-10: no automatic reset and no unbounded retry."""
    lost = assess_replay(kafka, None, source_sealed=False, source_present=False)
    plan = choose_recovery(lost)
    assert "no automatic reset" in plan.rationale


# -- RPO / RTO ------------------------------------------------------------


def test_rto_service_and_rto_correct_are_reported_separately() -> None:
    metrics = RecoveryMetrics(
        fault_confirmed_at_ns=1_000,
        service_restored_at_ns=3_000,
        reconciled_at_ns=10_000,
    )
    assert metrics.rto_service_ns == 2_000
    assert metrics.rto_correct_ns == 9_000
    payload = metrics.as_dict()
    assert "never summed or substituted" in payload["rto_note"]


def test_unmeasured_recovery_times_are_none_not_zero() -> None:
    metrics = RecoveryMetrics()
    assert metrics.rto_service_ns is None
    assert metrics.rto_correct_ns is None


@pytest.mark.whatif("P07-G29-02")
def test_rpo_zero_is_only_claimable_with_the_source_available() -> None:
    """NFR-11 ties the number to its precondition."""
    with_source = RecoveryMetrics(source_available_for_rpo=True).rpo_claim()
    assert with_source["rpo_events"] == 0
    assert with_source["rpo_zero_claimable"] is True

    without = RecoveryMetrics(source_available_for_rpo=False).rpo_claim()
    assert without["rpo_events"] == 0
    assert without["rpo_zero_claimable"] is False

    lossy = RecoveryMetrics(
        rpo_unrecoverable=(SourceIdentity("ds", "EURUSD", 5),), source_available_for_rpo=True
    ).rpo_claim()
    assert lossy["rpo_events"] == 1
    assert lossy["rpo_zero_claimable"] is False


def test_catch_up_is_none_when_the_backlog_cannot_drain() -> None:
    """``mu <= lambda`` has no finite catch-up time to report."""
    assert catch_up_seconds(6_000_000, 60_000, 100_000) == pytest.approx(150.0)
    assert catch_up_seconds(1_000, 100_000, 100_000) is None
    assert catch_up_seconds(1_000, 100_000, 50_000) is None


def test_retention_headroom_needs_a_bounded_catch_up() -> None:
    ok, note = retention_headroom_ok(
        retention_seconds=3600, outage_seconds=30, catch_up_seconds_value=150, verify_seconds=60
    )
    assert ok and "time dimension only" in note
    tight, _ = retention_headroom_ok(
        retention_seconds=60, outage_seconds=30, catch_up_seconds_value=150, verify_seconds=60
    )
    assert not tight
    unknown, note = retention_headroom_ok(
        retention_seconds=99999, outage_seconds=30, catch_up_seconds_value=None, verify_seconds=60
    )
    assert not unknown and "unbounded" in note


# -- injectors ------------------------------------------------------------


def test_a_target_outside_the_allowlist_is_refused_before_anything_happens() -> None:
    touched: list[str] = []
    with pytest.raises(TargetNotAllowed):
        inject(
            fault_id="f",
            intended_fault="x",
            target="production_broker",
            allowlist=DEFAULT_ALLOWLIST,
            precondition=lambda: touched.append("pre") or True,
            action=lambda: touched.append("act"),
            observe=lambda: (True, None),
        )
    assert touched == []


def test_a_destructive_injection_needs_explicit_confirmation() -> None:
    allowlist = LabAllowlist(targets=frozenset({"kafka_topic"}))
    with pytest.raises(TargetNotAllowed):
        inject(
            fault_id="f",
            intended_fault="x",
            target="kafka_topic",
            allowlist=allowlist,
            destructive=True,
            confirm_destructive=False,
            precondition=lambda: True,
            action=lambda: None,
            observe=lambda: (True, None),
        )


def test_a_failed_precondition_aborts_without_injecting() -> None:
    acted: list[str] = []
    receipt = inject(
        fault_id="f",
        intended_fault="x",
        target="kafka_broker",
        allowlist=DEFAULT_ALLOWLIST,
        precondition=lambda: False,
        action=lambda: acted.append("act"),
        observe=lambda: (True, "should not be consulted"),
    )
    assert acted == []
    assert receipt.confirmed is False
    assert "trigger_precondition_verified" in receipt.missing_conditions()


@pytest.mark.whatif("P07-G27-03")
def test_an_injector_with_no_independent_observation_is_unconfirmed() -> None:
    """Design 6.3: the command being acknowledged is not evidence of effect."""
    receipt = inject(
        fault_id="f",
        intended_fault="x",
        target="kafka_broker",
        allowlist=DEFAULT_ALLOWLIST,
        precondition=lambda: True,
        action=lambda: "ok",
        observe=lambda: (False, "nothing observed"),
    )
    assert receipt.injection_command_acknowledged is True
    assert receipt.confirmed is False


def test_an_action_that_raises_is_recorded_as_unacknowledged() -> None:
    def boom() -> None:
        raise RuntimeError("no")

    receipt = inject(
        fault_id="f",
        intended_fault="x",
        target="kafka_broker",
        allowlist=DEFAULT_ALLOWLIST,
        precondition=lambda: True,
        action=boom,
        observe=lambda: (False, None),
    )
    assert receipt.injection_command_acknowledged is False
    assert "RuntimeError" in str(receipt.detail["action_result"])


def test_a_failing_cleanup_makes_the_receipt_unconfirmed() -> None:
    def bad_cleanup() -> None:
        raise RuntimeError("stuck")

    receipt = inject(
        fault_id="f",
        intended_fault="x",
        target="kafka_broker",
        allowlist=DEFAULT_ALLOWLIST,
        precondition=lambda: True,
        action=lambda: None,
        observe=lambda: (True, "effect"),
        cleanup=bad_cleanup,
    )
    assert receipt.cleanup_recorded is False
    assert receipt.confirmed is False


def test_stopping_a_broker_is_observed_in_the_live_set(kafka: FakeKafka) -> None:
    receipt = kafka_injectors.stop_broker(kafka, 1)
    assert receipt.confirmed
    assert 1 not in kafka.live_brokers
    # Re-injecting the same fault fails its precondition rather than lying.
    assert kafka_injectors.stop_broker(kafka, 1).confirmed is False


def test_degrading_isr_is_observed_against_the_configured_minimum(kafka: FakeKafka) -> None:
    receipt = kafka_injectors.degrade_isr_below_min(kafka, TICK_TOPIC, 0)
    assert receipt.confirmed
    assert kafka.in_sync_replicas(TICK_TOPIC, 0) < kafka.config.min_insync_replicas


def test_retention_expiry_is_destructive_and_observed(kafka: FakeKafka) -> None:
    fill(kafka)
    assert kafka_injectors.expire_retention(kafka, TICK_TOPIC, 0, 3, confirm=True).confirmed
    assert kafka.log_start_offset(TICK_TOPIC, 0) == 3


def test_an_armed_fault_says_it_is_only_armed(sink: FakeClickHouse) -> None:
    """The effect arrives later; the receipt must not imply it already happened."""
    receipt = ch_injectors.drop_insert_ack(sink)
    assert receipt.confirmed
    assert receipt.detail["armed_only"] is True
    assert "cannot prove the server committed first" in receipt.detail["boundary_precision"]


def test_disk_and_server_faults_are_observed(sink: FakeClickHouse) -> None:
    assert ch_injectors.fill_disk(sink, free_bytes=0).confirmed
    assert sink.disk_free_bytes == 0
    assert ch_injectors.take_server_down(sink).confirmed
    assert sink.available is False


def test_a_crash_switch_fires_once_at_its_stage() -> None:
    switch = process_injectors.arm_crash_at("after_insert_before_commit", after_calls=1)
    switch("before_insert", tp=("t", 0))  # wrong stage: ignored
    switch("after_insert_before_commit")  # skipped by after_calls
    with pytest.raises(process_injectors.ProcessKilled) as exc:
        switch("after_insert_before_commit", batch_id="b2")
    assert exc.value.stage == "after_insert_before_commit"
    assert switch.fired
    switch("after_insert_before_commit")  # already fired: no second raise


@pytest.mark.whatif("P07-G27-04")
def test_a_crash_that_never_fired_produces_an_unconfirmed_receipt() -> None:
    """Case ``E2E-37``: the injector failed, so the verdict is INVALID."""
    switch = process_injectors.arm_crash_at("after_commit")
    receipt = process_injectors.confirm_crash_fired(switch, component_name="p05_ingestor")
    assert receipt.confirmed is False
    assert receipt.observed_effect == "crash never fired"


def test_killing_a_component_is_confirmed_by_reading_its_state_back() -> None:
    state = {"running": True}
    receipt = process_injectors.kill_component(
        component_name="p03_adapter",
        stop=lambda: state.__setitem__("running", False),
        is_stopped=lambda: not state["running"],
    )
    assert receipt.confirmed


def test_a_transport_fault_receipt_checks_what_was_actually_applied() -> None:
    faults = TransportFaults(truncate_frame_at=5)
    applied = network_injectors.arm_transport_fault(
        faults, description="truncate", applied_check=lambda: True
    )
    assert applied.confirmed
    not_applied = network_injectors.arm_transport_fault(
        faults, description="truncate", applied_check=lambda: False
    )
    assert not_applied.confirmed is False


def test_an_empty_transport_plan_fails_its_precondition() -> None:
    receipt = network_injectors.arm_transport_fault(
        TransportFaults(), description="nothing", applied_check=lambda: True
    )
    assert receipt.confirmed is False


def test_the_counting_proxy_reports_offered_and_forwarded_separately() -> None:
    """A ``send()`` that returns is not proof the receiver processed anything."""
    received: list[bytes] = []
    proxy = network_injectors.CountingProxy(forward=received.append, drop_after_bytes=4)
    proxy.feed(b"abcd")
    proxy.feed(b"efgh")
    assert proxy.bytes_offered == 8
    assert proxy.bytes_forwarded == 4
    assert proxy.dropped is True
    assert b"".join(received) == b"abcd"
    assert "not proof" in proxy.as_dict()["note"]


# -- resource budgets ------------------------------------------------------


def test_exceeding_a_budget_produces_a_finding_not_an_exception() -> None:
    """``INV-16``: a controlled stop with a reason code, not a crash."""
    budget = ResourceBudget(max_producer_queue_events=10, max_raw_rows=100)
    findings = check_budget(budget, producer_queue_events=11, raw_rows=101)
    assert len(findings) == 2
    assert {finding.reason for finding in findings} == {Reason.RESOURCE_BUDGET_EXCEEDED}
    assert check_budget(budget, producer_queue_events=10, raw_rows=100) == []


def test_the_budget_says_what_it_does_not_cover() -> None:
    note = ResourceBudget().as_dict()["note"]
    assert "No cgroup" in note


def test_rss_is_none_when_it_cannot_be_measured() -> None:
    """A zero would look like a measurement."""
    value = rss_bytes()
    assert value is None or value > 0


def test_shrinking_the_queue_cap_is_observed() -> None:
    producer = Producer(FakeKafka(partitions=2), max_queue_events=100)
    from p07.injectors.resources import shrink_producer_queue

    receipt = shrink_producer_queue(producer, max_events=1)
    assert receipt.confirmed
    assert receipt.detail["after"] == 1


# -- reporting -------------------------------------------------------------


def minimal_report(verdict: str = "PASS") -> dict[str, object]:
    return {
        "scenario_id": "sc",
        "run_id": "r1",
        "profile": "UNIT/MODEL",
        "profile_note": "simulation only",
        "verdict": verdict,
        "primary_reason": Reason.ALL_CHECKS_PROVEN,
        "release_blocking": verdict != "PASS",
        "stages": {
            "raw": {"diff": {"equal": True, "missing_count": 0, "extra_count": 0, "mismatched_count": 0}},
            "bars": {
                "expected_bars": 2,
                "observed_bars": 2,
                "expected_tick_total": 10,
                "observed_tick_total": 10,
                "mismatched_count": 0,
                "missing_count": 0,
                "extra_count": 0,
            },
        },
        "expected": {"policy_version": "p03.policy.v1", "counts": {}, "untracked_anomaly_count": 0},
        "faults": {"faults_total": 0, "faults_confirmed": 0, "faults_unconfirmed": 0},
        "recovery": {"rto_service_ns": None, "rto_correct_ns": None, "rpo_events": 0},
        "evidence": {"journal": {"entries_verified": 3, "chain_intact": True}, "anchor": {}, "artifact_bytes": 10},
        "findings": [],
    }


def test_the_summary_leads_with_the_verdict_and_the_profile_caveat() -> None:
    text = render_summary(minimal_report())
    assert "**Verdict: PASS**" in text
    assert "Profile `UNIT/MODEL`" in text
    assert "Limitations" in text


def test_the_summary_prints_not_measured_rather_than_zero() -> None:
    text = render_summary(minimal_report())
    assert "not measured" in text


def test_the_summary_lists_findings_when_there_are_some() -> None:
    report = minimal_report("FAIL")
    report["findings"] = [{"reason": Reason.BARS_MISMATCH, "detail": {"mismatched": 2}}]
    text = render_summary(report)
    assert "BARS_MISMATCH" in text


def test_tally_includes_every_bucket_even_at_zero() -> None:
    counts = tally_verdicts([{"verdict": "PASS"}, {"verdict": "FAIL"}])
    assert counts["PASS"] == 1 and counts["FAIL"] == 1
    assert counts["NOT_RUN"] == 0 and counts["INVALID"] == 0


@pytest.mark.whatif("P07-G29-01")
def test_the_release_report_counts_uncovered_cases_as_not_run() -> None:
    """Design 10.1: 448 designed is not 448 executed."""
    catalog = [
        {"case_id": "P07-G01-01", "priority": "P0", "group": "G01"},
        {"case_id": "P07-G01-02", "priority": "P0", "group": "G01"},
        {"case_id": "P07-G01-03", "priority": "P1", "group": "G01"},
    ]
    executed = [{"test": "t1", "verdict": "PASS", "cases": ["P07-G01-01"]}]
    text = render_release_report(catalog=catalog, executed=executed)
    assert "NO-GO" in text  # an uncovered P0 blocks
    assert "| NOT_RUN | 2 | 1 |" in text
    assert "NOT VALIDATED" in text


def test_a_failing_test_cannot_be_masked_by_a_passing_one() -> None:
    """Worst verdict wins when two tests cover the same case."""
    catalog = [{"case_id": "P07-G01-01", "priority": "P0", "group": "G01"}]
    executed = [
        {"test": "good", "verdict": "PASS", "cases": ["P07-G01-01"]},
        {"test": "bad", "verdict": "FAIL", "cases": ["P07-G01-01"]},
    ]
    text = render_release_report(catalog=catalog, executed=executed)
    assert "NO-GO" in text
    assert "| FAIL | 1 | 1 |" in text


def test_full_p0_coverage_is_go_for_the_tested_scope() -> None:
    catalog = [{"case_id": "P07-G01-01", "priority": "P0", "group": "G01"}]
    executed = [{"test": "t", "verdict": "PASS", "cases": ["P07-G01-01"]}]
    text = render_release_report(catalog=catalog, executed=executed)
    assert "GO (for the tested profile and scope)" in text
