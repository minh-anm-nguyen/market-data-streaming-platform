"""Execute a validated scenario: apply each fault at its declared stage.

The mapping from a fault type to an injector call is the whole content of this
module, and it is a closed table on purpose (see
:data:`~p07.scenario_loader.FAULT_TYPES`). A scenario file cannot reach anything
not listed here, which is what makes running an untrusted scenario merely
useless rather than dangerous.

Crash faults are installed *before* the data plane starts, because a crash hook
has to be in place at the boundary it fires on. Everything else is applied in
stage order: ``pre_replay``, ``replay``, ``post_cut``, ``pre_ingest``,
``post_ingest``, ``pre_build``.

A run whose expectations are not met returns its real verdict and a list of
mismatches. It does not raise: a scenario expecting ``PASS`` and getting
``INCONCLUSIVE`` is information, and losing the bundle to an exception would
destroy the evidence that explains why.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ..contracts import QUOTE_TOPIC, TICK_TOPIC
from ..dataplane.ingestion import PoisonRecord
from ..evidence.artifact_store import ArtifactStore
from ..evidence.verdicts import Finding, GateResult, Reason
from ..injectors import clickhouse as ch_injectors
from ..injectors import kafka as kafka_injectors
from ..injectors import network as network_injectors
from ..injectors import process as process_injectors
from ..injectors.process import ProcessKilled
from ..replay.source_replayer import TransportFaults
from ..scenario_loader import FaultSpec, Scenario
from .supervisor import LabConfig, ScenarioRun


@dataclass(frozen=True, slots=True)
class ScenarioOutcome:
    """What the run produced, and whether it matched the scenario's claims."""

    gate: GateResult
    report: Mapping[str, Any]
    expectation_mismatches: tuple[str, ...]
    run_dir: str

    @property
    def as_expected(self) -> bool:
        return not self.expectation_mismatches


def _lab_config(scenario: Scenario) -> LabConfig:
    overrides = scenario.lab_overrides
    base = LabConfig()
    return LabConfig(
        partitions=int(overrides.get("partitions", base.partitions)),
        brokers=int(overrides.get("brokers", base.brokers)),
        replication_factor=int(overrides.get("replication_factor", base.replication_factor)),
        min_insync_replicas=int(overrides.get("min_insync_replicas", base.min_insync_replicas)),
        group=str(overrides.get("group", base.group)),
        batch_max_records=int(overrides.get("batch_max_records", base.batch_max_records)),
        producer_idempotent=bool(overrides.get("producer_idempotent", base.producer_idempotent)),
        kafka_idempotent_dedupe=bool(
            overrides.get("kafka_idempotent_dedupe", base.kafka_idempotent_dedupe)
        ),
        strict_poison=bool(overrides.get("strict_poison", base.strict_poison)),
        policy=scenario.policy,
        budget=base.budget,
        allowlist=base.allowlist,
    )


def _transport_faults(specs: tuple[FaultSpec, ...]) -> TransportFaults | None:
    """Fold the ``replay``-stage faults into one :class:`TransportFaults`."""
    if not specs:
        return None
    kwargs: dict[str, int | None] = {}
    for spec in specs:
        index = spec.options.get("frame_index")
        if spec.type == "transport_truncate":
            kwargs["truncate_frame_at"] = int(index or 0)
        elif spec.type == "transport_close":
            kwargs["close_after_records"] = int(spec.options.get("records", 1))
        elif spec.type == "transport_malformed":
            kwargs["malformed_frame_at"] = int(index or 0)
        elif spec.type == "transport_duplicate":
            kwargs["duplicate_frame_at"] = int(index or 0)
        elif spec.type == "transport_bad_magic":
            kwargs["bad_magic_at"] = int(index or 0)
    return TransportFaults(**kwargs)  # type: ignore[arg-type]


def _topic_partition(spec: FaultSpec, run: ScenarioRun) -> tuple[str, int]:
    """Resolve the fault's ``(topic, partition)``.

    ``partition: -1`` (or an omitted partition) means "the busiest partition of
    this topic". With eight partitions and three symbols most partitions are
    empty, and a fault aimed at an empty one applies to nothing -- the injector
    then reports it unconfirmed and the whole scenario comes back ``INVALID``,
    which is correct but useless. Letting the scenario say "wherever the data
    is" keeps the declarative file honest without hard-coding a partition index
    that depends on the partitioner.
    """
    topic = str(spec.options.get("topic", TICK_TOPIC))
    if topic not in (TICK_TOPIC, QUOTE_TOPIC):
        raise ValueError(f"fault targets unknown topic {topic!r}")
    partition = int(spec.options.get("partition", -1))
    if partition >= 0:
        return topic, partition
    kafka = run.kafka
    assert kafka is not None
    counts = [
        (kafka.high_watermark(topic, index) - kafka.log_start_offset(topic, index), index)
        for index in range(kafka.partition_count(topic))
    ]
    return topic, max(counts)[1]


def _apply(run: ScenarioRun, spec: FaultSpec) -> None:
    """Apply one non-replay fault and record its receipt."""
    kafka = run.kafka
    sink = run.sink
    assert kafka is not None and sink is not None

    if spec.type == "producer_transient":
        assert run.producer is not None
        run.record_receipt(
            kafka_injectors.inject_transient_produce_failures(
                run.producer, int(spec.options.get("count", 1))
            )
        )
    elif spec.type == "kafka_ack_loss":
        topic, partition = _topic_partition(spec, run)
        run.record_receipt(kafka_injectors.lose_next_ack(kafka, topic, partition))
    elif spec.type == "kafka_broker_stop":
        run.record_receipt(
            kafka_injectors.stop_broker(kafka, int(spec.options.get("broker_id", 0)))
        )
    elif spec.type == "kafka_isr_degrade":
        topic, partition = _topic_partition(spec, run)
        run.record_receipt(kafka_injectors.degrade_isr_below_min(kafka, topic, partition))
    elif spec.type == "kafka_retention_expire":
        topic, partition = _topic_partition(spec, run)
        run.record_receipt(
            kafka_injectors.expire_retention(
                kafka,
                topic,
                partition,
                int(spec.options.get("new_log_start", 1)),
                confirm=bool(spec.options.get("confirm", False)),
            )
        )
    elif spec.type == "consumer_rebalance":
        run.record_receipt(kafka_injectors.force_rebalance(kafka, run.lab.group))
    elif spec.type == "ch_ack_loss":
        run.record_receipt(ch_injectors.drop_insert_ack(sink))
    elif spec.type == "ch_fail_before_commit":
        run.record_receipt(ch_injectors.fail_before_commit(sink))
    elif spec.type == "ch_disk_full":
        run.record_receipt(
            ch_injectors.fill_disk(sink, free_bytes=int(spec.options.get("free_bytes", 0)))
        )
    elif spec.type == "ch_server_down":
        run.record_receipt(ch_injectors.take_server_down(sink))
    elif spec.type == "ch_raw_ttl":
        run.record_receipt(
            ch_injectors.expire_raw_ttl(
                sink,
                before_ingested_at_ns=int(spec.options.get("before_ingested_at_ns", 1 << 62)),
                confirm=bool(spec.options.get("confirm", False)),
            )
        )
    elif spec.type == "ch_volume_loss":
        run.record_receipt(
            ch_injectors.drop_volume(sink, confirm=bool(spec.options.get("confirm", False)))
        )
    elif spec.type in {"ingest_crash", "build_crash"}:
        # Installed before the data plane started; the receipt is written after
        # the stage that was supposed to crash (see run_scenario).
        return
    else:  # pragma: no cover - the loader rejects unknown types first
        raise ValueError(f"unhandled fault type {spec.type!r}")


def run_scenario(
    scenario: Scenario,
    *,
    runs_dir: str | Path = "runs",
    run_id: str = "run-1",
    fsync: bool = True,
) -> ScenarioOutcome:
    """Execute ``scenario`` and return its outcome plus the written bundle.

    Stages that raise (an unscannable cut, a poisoned record, a killed process)
    are caught here. The findings they recorded are already in the run, so the
    gate still produces a verdict and the bundle is still written -- which is
    the difference between a harness that reports an outage and one that
    disappears during it.
    """
    store = ArtifactStore(
        runs_dir,
        scenario_id=scenario.scenario_id,
        run_id=run_id,
        quota_bytes=scenario.evidence_max_bytes,
        fsync=fsync,
    )
    store.write_json("fault/intent.json", scenario.as_dict())

    ingest_crash = next(
        (spec for spec in scenario.faults if spec.type == "ingest_crash"), None
    )
    build_crash = next((spec for spec in scenario.faults if spec.type == "build_crash"), None)
    ingest_switch = (
        process_injectors.arm_crash_at(
            str(ingest_crash.options.get("stage", "after_insert_before_commit")),
            after_calls=int(ingest_crash.options.get("after_calls", 0)),
        )
        if ingest_crash
        else None
    )
    build_switch = (
        process_injectors.arm_crash_at(
            str(build_crash.options.get("stage", "canonical_batch")),
            after_calls=int(build_crash.options.get("after_calls", 0)),
        )
        if build_crash
        else None
    )

    run = ScenarioRun(
        source_run_dir=scenario.source_run_dir,
        store=store,
        scenario_id=scenario.scenario_id,
        run_id=run_id,
        test_case_id=scenario.test_case_id,
        lab=_lab_config(scenario),
        source_anchor=scenario.source_anchor,
        require_fault_plan=scenario.require_fault_plan,
    )

    try:
        run.seal()
        run.start_dataplane(ingest_crash_hook=ingest_switch, build_crash_hook=build_switch)

        for spec in scenario.faults_at("pre_replay"):
            _apply(run, spec)

        replay_faults = _transport_faults(scenario.faults_at("replay"))
        report = run.replay(chunk_sizes=scenario.chunk_sizes, faults=replay_faults)
        if replay_faults is not None:
            applied = set(report.transport_faults_applied)
            run.record_receipt(
                network_injectors.arm_transport_fault(
                    replay_faults,
                    description="byte-stream fault from scenario",
                    applied_check=lambda: bool(applied),
                )
            )

        run.fence_and_cut()
        run.scan()

        for spec in scenario.faults_at("post_cut"):
            _apply(run, spec)
        for spec in scenario.faults_at("pre_ingest"):
            _apply(run, spec)

        try:
            run.ingest()
        except (PoisonRecord, ProcessKilled) as exc:
            # Both are expected outcomes for some scenarios. The findings are
            # recorded; the remaining stages are skipped because there is
            # nothing trustworthy to build on.
            run.add_findings(
                [Finding(Reason.EVIDENCE_INCOMPLETE, {"stage": "ingest", "aborted_by": type(exc).__name__})]
            )
        if ingest_switch is not None:
            run.record_receipt(
                process_injectors.confirm_crash_fired(ingest_switch, component_name="p05_ingestor")
            )

        for spec in scenario.faults_at("post_ingest"):
            _apply(run, spec)

        try:
            run.audit_and_build()
            for spec in scenario.faults_at("pre_build"):
                _apply(run, spec)
            run.materialize()
            run.verify_and_publish()
        except Exception as exc:  # noqa: BLE001 - includes ProcessKilled and BuildRefused
            # A refused build is often correct. Its reason is already a finding;
            # re-raising would lose the bundle.
            run.add_findings(
                [
                    Finding(
                        Reason.EVIDENCE_INCOMPLETE,
                        {"stage": "build", "aborted_by": type(exc).__name__, "detail": str(exc)[:200]},
                    )
                ]
            )
            run.ledger.record(
                phase=run.state.phase(),
                actor="scenario_runner",
                operation="stage_aborted",
                reason_code=type(exc).__name__,
                detail={"detail": str(exc)[:200]},
            )
        if build_switch is not None:
            run.record_receipt(
                process_injectors.confirm_crash_fired(build_switch, component_name="p06_builder")
            )
    except Exception as exc:  # noqa: BLE001
        run.add_findings(
            [
                Finding(
                    Reason.EVIDENCE_INCOMPLETE,
                    {"stage": "run", "aborted_by": type(exc).__name__, "detail": str(exc)[:200]},
                )
            ]
        )
        run.ledger.record(
            phase=run.state.phase(),
            actor="scenario_runner",
            operation="run_aborted",
            reason_code=type(exc).__name__,
            detail={"detail": str(exc)[:200]},
        )

    gate = run.gate(expect_fault=bool(scenario.faults) and scenario.expect.require_fault_confirmed)
    report_doc = run.finish(gate)

    mismatches: list[str] = []
    expect = scenario.expect
    if expect.verdict is not None and gate.verdict.value != expect.verdict:
        mismatches.append(f"expected verdict {expect.verdict}, got {gate.verdict.value}")
    if expect.require_published is not None and run.results.published != expect.require_published:
        mismatches.append(
            f"expected published={expect.require_published}, got {run.results.published}"
        )
    if expect.require_zero_conflicts:
        conflicts = 0
        if run.results.raw is not None:
            conflicts += len(run.results.raw.diff.mismatched)
        if run.results.canonical is not None:
            conflicts += len(run.results.canonical.diff.mismatched)
        if conflicts:
            mismatches.append(f"expected zero conflicts, got {conflicts}")
    if not expect.allow_physical_duplicates and run.results.raw is not None:
        if run.results.raw.physical_duplicate_rows:
            mismatches.append(
                f"physical duplicates not allowed, got {run.results.raw.physical_duplicate_rows}"
            )

    return ScenarioOutcome(
        gate=gate,
        report=report_doc,
        expectation_mismatches=tuple(mismatches),
        run_dir=str(store.run_dir),
    )
