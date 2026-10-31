"""The mandatory end-to-end cases from design section 11.

Each test names its ``E2E-nn`` case and follows the section's rule for what an
E2E may assert: a sealed fixture and expected policy, an exact fault receipt, a
cut per topic-partition, a full identity diff at raw and canonical, an
independent bars oracle, and a terminal verdict. A test that only checked "the
services came back" would not be one of these.

Cases unreachable in the ``UNIT/MODEL`` profile are present and **skipped** with
``needs_real_lab``, so they report ``NOT_RUN`` rather than quietly leaving the
inventory. Design 16.1 says mock-only execution means integration is NOT
VALIDATED, and keeping the unrunnable cases visible is how that stays true in
the report rather than only in prose.
"""

from __future__ import annotations

from typing import Any, Callable

import pytest

from p07.contracts import TICK_TOPIC, EventKind
from p07.dataplane.clickhouse_fake import BuildStatus
from p07.evidence.verdicts import Reason, Verdict
from p07.injectors import clickhouse as ch_injectors
from p07.injectors import kafka as kafka_injectors
from p07.injectors import network as network_injectors
from p07.injectors import process as process_injectors
from p07.oracle.acceptance_oracle import build_expectation
from p07.oracle.reference_bars import reference_bars
from p07.orchestration.supervisor import LabConfig
from p07.replay.source_replayer import TransportFaults
from tools.source_factory import FaultOp

pytestmark = pytest.mark.e2e


def busiest_partition(kafka: Any, topic: str = TICK_TOPIC) -> int:
    return max(
        range(kafka.partition_count(topic)),
        key=lambda index: kafka.high_watermark(topic, index) - kafka.log_start_offset(topic, index),
    )


# -- E2E-01..06: the clean path and declared anomalies --------------------


@pytest.mark.whatif("P07-G01-01", "P07-G17-01", "P07-G19-01")
def test_e2e_01_clean_run_reaches_a_published_ready_build(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-01``: A = K = W(logical) = C, and bars equal the reference."""
    source = make_source(count=400, dataset_id="e2e-01")
    run = run_factory(source=source, scenario_id="e2e-01", test_case_id="E2E-01")
    gate, report = run.run_clean()

    assert gate.verdict is Verdict.PASS, gate.as_dict()
    assert report["stages"]["published"] is True
    for boundary in ("transport", "raw", "canonical"):
        assert report["stages"][boundary]["diff"]["equal"] is True
    assert report["stages"]["bars"]["verified"] is True
    assert (
        report["stages"]["bars"]["expected_tick_total"]
        == report["stages"]["bars"]["observed_tick_total"]
    )
    assert run.sink is not None and run.sink.ready_build == report["stages"]["build_id"]


@pytest.mark.whatif("P07-G02-01")
def test_e2e_02_the_same_seed_reproduces_the_same_source_and_expected_set(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    """``E2E-02``: same seed/config -> same bytes and the same semantic set."""
    from p07.dataset_sealer import seal_dataset

    first = make_source(name="rep-a", dataset_id="e2e-02", count=200, seed=4242)
    second = make_source(name="rep-b", dataset_id="e2e-02", count=200, seed=4242)
    assert first["events_sha256"] == second["events_sha256"]

    left = build_expectation(seal_dataset(first["run_dir"], anchor=first["events_sha256"]))
    right = build_expectation(seal_dataset(second["run_dir"], anchor=second["events_sha256"]))
    assert left.accepted == right.accepted
    assert left.accepted_digests == right.accepted_digests

    different = make_source(name="rep-c", dataset_id="e2e-02", count=200, seed=99)
    assert different["events_sha256"] != first["events_sha256"]


@pytest.mark.whatif("P07-G03-03", "P07-G03-04")
def test_e2e_03_two_descriptors_agree_on_the_semantic_core(sealed: Any) -> None:
    """``E2E-03``: a cross-runtime round trip compares cores, never bytes."""
    from google.protobuf import descriptor_pb2 as dpb

    from p07.dataplane.codec import DEFAULT_CODEC, Codec, build_file_descriptor

    other_runtime = Codec(
        build_file_descriptor(
            extra_event_field=(77, "producer_hint", dpb.FieldDescriptorProto.TYPE_STRING)
        )
    )
    expectation = build_expectation(sealed)
    for core in list(expectation.accepted.values())[:50]:
        encoded = other_runtime.encode(core, extra={"producer_hint": "cpp"})
        assert DEFAULT_CODEC.decode(encoded) == core
        # The bytes differ; the meaning does not. Hashing the bytes would call
        # this a mismatch (case P07-G03-04).
        assert encoded != DEFAULT_CODEC.encode(core)


@pytest.mark.whatif("P07-G04-01", "P07-G04-02")
@pytest.mark.parametrize("chunk_sizes", [(1,), (1, 17)])
def test_e2e_04_pathological_segmentation_changes_nothing(
    make_source: Callable[..., dict[str, Any]],
    run_factory: Callable[..., Any],
    chunk_sizes: tuple[int, ...],
) -> None:
    """``E2E-04``: every expected accepted id appears exactly once, any chunking."""
    source = make_source(count=120, dataset_id="e2e-04")
    run = run_factory(source=source, scenario_id="e2e-04", test_case_id="E2E-04")
    run.seal()
    run.start_dataplane()
    run.replay(chunk_sizes=chunk_sizes)
    run.fence_and_cut()
    run.scan()
    run.ingest()
    run.audit_and_build()
    run.materialize()
    run.verify_and_publish()
    gate = run.gate()
    assert gate.verdict is Verdict.PASS, gate.as_dict()
    assert run.results.transport is not None and run.results.transport.diff.equal


@pytest.mark.whatif("P07-G01-05", "P07-G05-01", "P07-G06-01")
def test_e2e_05_and_06_declared_anomalies_are_not_transport_loss(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-05``/``E2E-06``: the omission is a source gap, not a Kafka loss.

    ``INV-15``: the suppressed and rejected events are *expected* to be absent
    downstream, so the run still passes and the absence is attributed to the
    policy rather than to the transport.
    """
    source = make_source(
        count=400,
        dataset_id="e2e-05",
        faults=(
            FaultOp("missing_sequence", "EURUSD", 20),
            FaultOp("duplicate_exact", "GBPUSD", 12),
            FaultOp("invalid_spread", "EURUSD", 40),
        ),
    )
    run = run_factory(source=source, scenario_id="e2e-05", test_case_id="E2E-05")
    gate, report = run.run_clean()

    assert gate.verdict is Verdict.PASS, gate.as_dict()
    assert report["stages"]["published"] is True
    expected = report["expected"]
    assert expected["counts"]["suppressed"] >= 1
    assert expected["counts"]["rejected"] >= 1
    assert expected["counts"]["gaps_detected"] >= 1
    assert expected["untracked_anomaly_count"] == 0
    # Nothing is reported missing: the expected set already excludes them.
    assert report["stages"]["raw"]["diff"]["missing_count"] == 0


# -- E2E-07/08/09: adapter and producer failures -------------------------


@pytest.mark.whatif("P07-G04-05")
def test_e2e_07_a_truncated_frame_publishes_nothing_of_itself(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-07``: a partial frame emits nothing; the fed prefix reconciles.

    The verdict is INCONCLUSIVE rather than PASS, and that is the right answer:
    the replay stopped early, so the events after the cut point were never
    offered and the harness cannot claim the pipeline handled them.
    """
    source = make_source(count=200, dataset_id="e2e-07")
    run = run_factory(source=source, scenario_id="e2e-07", test_case_id="E2E-07")
    run.seal()
    run.start_dataplane()
    faults = TransportFaults(truncate_frame_at=50)
    report = run.replay(faults=faults)
    assert "truncate_frame_at=50" in report.transport_faults_applied
    run.record_receipt(
        network_injectors.arm_transport_fault(
            faults, description="truncate", applied_check=lambda: True
        )
    )
    run.fence_and_cut()
    run.scan()
    run.ingest()

    assert run.adapter is not None
    assert run.adapter.counters.published == 50
    assert run.adapter.counters.closed_reason == "TRUNCATED_BODY"
    inventory = run.results.inventory
    assert inventory is not None and len(inventory.logical) == 50
    gate = run.gate()
    assert gate.verdict is Verdict.INCONCLUSIVE
    assert gate.verdict.is_release_blocking()


@pytest.mark.whatif("P07-G07-02", "P07-G07-03")
def test_e2e_08_and_09_a_lost_ack_is_resolved_only_by_the_scan(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-08``/``E2E-09``: no false "delivered", no false "lost".

    With idempotent dedupe the retry does not duplicate: the record was in the
    log all along, and the independent scan is what establishes it.
    """
    source = make_source(count=200, dataset_id="e2e-09")
    run = run_factory(source=source, scenario_id="e2e-09", test_case_id="E2E-09")
    run.seal()
    run.start_dataplane()
    assert run.kafka is not None
    run.record_receipt(kafka_injectors.lose_next_ack(run.kafka, TICK_TOPIC, 0))
    run.replay()
    run.fence_and_cut()
    run.scan()
    run.ingest()
    run.audit_and_build()
    run.materialize()
    run.verify_and_publish()

    gate = run.gate()
    assert gate.verdict is Verdict.PASS, gate.as_dict()
    assert run.results.raw is not None and run.results.raw.diff.equal
    assert run.results.canonical is not None and run.results.canonical.verified


@pytest.mark.whatif("P07-G15-03", "P07-G18-04")
def test_e2e_22_and_27_a_physical_duplicate_does_not_inflate_the_bars(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-22``/``E2E-27``: duplicates are allowed; the tick count is not.

    Dedupe is off, so the retry after the lost ACK really leaves two physical
    records -- the only way to test that the canonical build reduces them
    (``INV-09``).
    """
    source = make_source(count=200, dataset_id="e2e-22")
    run = run_factory(
        source=source,
        scenario_id="e2e-22",
        test_case_id="E2E-22",
        lab=LabConfig(partitions=4, kafka_idempotent_dedupe=False),
    )
    run.seal()
    run.start_dataplane()
    assert run.kafka is not None
    run.record_receipt(kafka_injectors.lose_next_ack(run.kafka, TICK_TOPIC, 0))
    run.replay()
    run.fence_and_cut()
    run.scan()
    run.ingest()
    run.audit_and_build()
    run.materialize()
    run.verify_and_publish()

    gate = run.gate()
    assert gate.verdict is Verdict.PASS, gate.as_dict()
    inventory = run.results.inventory
    assert inventory is not None
    assert sum(count - 1 for count in inventory.physical_counts.values() if count > 1) >= 1
    assert run.results.canonical is not None and run.results.canonical.verified
    assert run.results.bars is not None and run.results.bars.verified


# -- E2E-12: durability under ISR loss, and the recovery half ------------


@pytest.mark.whatif("P07-G09-03", "P07-G09-04")
def test_e2e_12_isr_loss_refuses_writes_then_recovers_completely(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-12``: hold rather than weaken durability, then prove the resume.

    The scenario file covers the degraded state; this covers the half it cannot
    express -- restore quorum, replay, and show the complete expected set
    arrives with nothing missing.
    """
    source = make_source(count=150, dataset_id="e2e-12")
    run = run_factory(source=source, scenario_id="e2e-12", test_case_id="E2E-12")
    run.seal()
    run.start_dataplane()
    kafka = run.kafka
    assert kafka is not None and run.producer is not None

    receipt = run.record_receipt(kafka_injectors.degrade_isr_below_min(kafka, TICK_TOPIC, 0))
    assert receipt.confirmed
    run.replay(flush=False)
    refused = run.producer.flush()
    assert refused["delivered"] == 0
    assert refused["failed"] > 0
    assert all(
        kafka.high_watermark(TICK_TOPIC, partition) == 0
        for partition in range(kafka.partition_count(TICK_TOPIC))
    )

    # Quorum returns. The source is intact, so a fresh replay must deliver all.
    run.begin_recovery("quorum restored, republishing from the sealed source")
    for broker in range(kafka.describe()["brokers_total"]):  # type: ignore[arg-type]
        kafka.start_broker(broker)
    # The same broker, with quorum back -- not a fresh one, which would make
    # this pass for the wrong reason.
    run.restart_clients()
    run.replay()
    run.fence_and_cut()
    run.scan()
    run.ingest()
    run.audit_and_build()
    run.materialize()
    published = run.verify_and_publish()

    assert published is True
    assert run.results.raw is not None and run.results.raw.diff.equal
    assert run.results.bars is not None and run.results.bars.verified


# -- E2E-13/14/15: ingestion crashes and ambiguous sink ACKs -------------


@pytest.mark.whatif("P07-G12-02", "P07-G14-04")
def test_e2e_13_and_15_a_crash_before_the_commit_replays_safely(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-13``/``E2E-15``: reconsume from the committed frontier, nothing missing.

    The worker dies after the sink ACK and before the Kafka commit. A fresh
    worker rebuilds position from the broker alone, so the batch is reprocessed:
    physical duplicates are expected, missing identities are not.
    """
    source = make_source(count=200, dataset_id="e2e-15")
    run = run_factory(
        source=source,
        scenario_id="e2e-15",
        test_case_id="E2E-15",
        lab=LabConfig(partitions=4, batch_max_records=10),
    )
    switch = process_injectors.arm_crash_at("after_insert_before_commit", after_calls=1)
    run.seal()
    run.start_dataplane(ingest_crash_hook=switch)
    run.replay()
    cut = run.fence_and_cut()
    run.scan()

    with pytest.raises(process_injectors.ProcessKilled):
        run.ingest()
    assert switch.fired
    run.record_receipt(
        process_injectors.confirm_crash_fired(switch, component_name="p05_ingestor")
    )
    assert run.sink is not None and len(run.sink.raw_rows) > 0

    from p07.dataplane.ingestion import BatchIngestor

    assert run.kafka is not None
    run.ingestor = BatchIngestor(run.kafka, run.sink, group=run.lab.group, batch_max_records=10)
    run.ingest(cut=cut)
    run.audit_and_build()
    run.materialize()
    published = run.verify_and_publish()

    assert published is True
    assert run.results.raw is not None
    assert run.results.raw.diff.equal
    assert run.results.raw.physical_duplicate_rows >= 1
    assert run.gate().verdict is Verdict.PASS


@pytest.mark.whatif("P07-G13-01", "P07-G13-03")
def test_e2e_14_an_ambiguous_insert_ack_still_yields_a_correct_build(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-14``: ClickHouse commits, the reply is lost, the retry duplicates."""
    source = make_source(count=200, dataset_id="e2e-14")
    run = run_factory(
        source=source,
        scenario_id="e2e-14",
        test_case_id="E2E-14",
        lab=LabConfig(partitions=4, batch_max_records=10),
    )
    run.seal()
    run.start_dataplane()
    assert run.sink is not None
    run.record_receipt(ch_injectors.drop_insert_ack(run.sink))
    run.replay()
    run.fence_and_cut()
    run.scan()
    run.ingest()
    run.audit_and_build()
    run.materialize()
    published = run.verify_and_publish()

    assert published is True
    assert run.results.raw is not None
    assert run.results.raw.physical_duplicate_rows >= 1
    assert run.results.raw.diff.equal  # logically complete despite the duplicates
    assert run.results.canonical is not None and run.results.canonical.verified
    assert run.gate().verdict is Verdict.PASS


# -- E2E-20/21: retention and republish ---------------------------------


@pytest.mark.whatif("P07-G10-01", "P07-G10-05")
def test_e2e_20_an_expired_range_is_no_go_not_a_reset(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-20``: explicit NO-GO, and no reset to the end of the log."""
    from p07.replay.kafka_cut_scanner import CutUnscannable

    source = make_source(count=150, dataset_id="e2e-20")
    run = run_factory(source=source, scenario_id="e2e-20", test_case_id="E2E-20")
    run.seal()
    run.start_dataplane()
    run.replay()
    run.fence_and_cut()
    assert run.kafka is not None
    partition = busiest_partition(run.kafka)
    run.record_receipt(
        kafka_injectors.expire_retention(run.kafka, TICK_TOPIC, partition, 3, confirm=True)
    )
    with pytest.raises(CutUnscannable) as exc:
        run.scan()
    assert exc.value.reason == Reason.CUT_RANGE_EXPIRED

    gate = run.gate()
    assert gate.verdict is Verdict.INCONCLUSIVE
    assert run.results.published is None
    assert run.sink is not None and run.sink.ready_build is None


@pytest.mark.whatif("P07-G10-04", "P07-G02-10")
def test_e2e_21_republishing_a_sealed_source_into_a_new_generation(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-21``: new coordinates, new cut, identical logical set."""
    from p07.recovery.recovery_coordinator import assess_replay, choose_recovery

    source = make_source(count=150, dataset_id="e2e-21")
    run = run_factory(source=source, scenario_id="e2e-21", test_case_id="E2E-21")
    run.seal()
    run.start_dataplane()
    run.replay()
    first_cut = run.fence_and_cut()
    first_inventory = run.scan()
    assert run.kafka is not None

    # Lose the log. The sealed source survives, so a republish is the route.
    partition = busiest_partition(run.kafka)
    run.record_receipt(
        kafka_injectors.expire_retention(
            run.kafka,
            TICK_TOPIC,
            partition,
            run.kafka.high_watermark(TICK_TOPIC, partition),
            confirm=True,
        )
    )
    feasibility = assess_replay(run.kafka, first_cut, source_sealed=True, source_present=True)
    assert choose_recovery(feasibility).method in {
        "republish_new_generation",
        "resume_and_reconcile",
    }

    run.begin_recovery("kafka range expired, republishing into a new generation")
    new_generation = run.kafka.new_generation()
    assert new_generation != first_cut.topic_generation
    run.restart_clients()
    run.replay()
    second_cut = run.fence_and_cut()
    second_inventory = run.scan()

    assert second_cut.topic_generation != first_cut.topic_generation
    assert set(second_inventory.logical) == set(first_inventory.logical)
    with pytest.raises(ValueError, match=Reason.CUT_GENERATION_MIXED):
        first_cut.merge(second_cut)


# -- E2E-23: conflicting identity ---------------------------------------


@pytest.mark.whatif("P07-G16-01", "P07-G17-02")
def test_e2e_23_a_conflicting_identity_blocks_the_build(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-23``: CONFLICT/NO-GO, and no arbitrary winner.

    The source carries two versions of one identity. The oracle rejects the
    second, so Kafka only ever sees one -- and the *source* conflict is
    reported, which is what stops the build.
    """
    source = make_source(
        count=200,
        dataset_id="e2e-23",
        faults=(FaultOp("duplicate_conflict", "EURUSD", 15, declared=False),),
    )
    run = run_factory(source=source, scenario_id="e2e-23", test_case_id="E2E-23")
    gate, report = run.run_clean()

    assert gate.verdict is Verdict.FAIL, gate.as_dict()
    reasons = {finding["reason"] for finding in report["findings"]}
    assert Reason.SOURCE_IDENTITY_CONFLICT in reasons
    assert report["stages"]["published"] is False
    assert run.sink is not None and run.sink.ready_build is None


# -- E2E-25/26/28: OHLC correctness and immutability ---------------------


@pytest.mark.whatif("P07-G18-01", "P07-G18-02")
def test_e2e_25_bars_match_the_reference_exactly(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-25``: exact open/high/low/close/tick_count against Python."""
    source = make_source(count=600, dataset_id="e2e-25", interval_ns=200_000_000)
    run = run_factory(source=source, scenario_id="e2e-25", test_case_id="E2E-25")
    gate, report = run.run_clean()
    assert gate.verdict is Verdict.PASS

    assert run.expectation is not None and run.sink is not None
    expected = reference_bars(run.expectation.accepted.values())
    assert len(expected) >= 2, "the fixture should span more than one minute"
    observed = {
        (row.dataset_id, row.symbol, row.price_scale, row.bar_start_ns): row
        for row in run.sink.scan_bars(report["stages"]["build_id"])
    }
    assert len(observed) == len(expected)
    for key, bar in expected.items():
        row = observed[(key.dataset_id, key.symbol, key.price_scale, key.bar_start_ns)]
        assert (row.open_ticks, row.high_ticks, row.low_ticks, row.close_ticks) == (
            bar.open_ticks,
            bar.high_ticks,
            bar.low_ticks,
            bar.close_ticks,
        )
        assert row.tick_count == bar.tick_count

    quote_count = sum(
        1 for core in run.expectation.accepted.values() if core.event_kind is EventKind.QUOTE
    )
    assert quote_count > 0  # quotes existed and stayed out of the price bars
    assert sum(bar.tick_count for bar in expected.values()) == (
        len(run.expectation.accepted) - quote_count
    )


@pytest.mark.whatif("P07-G18-03")
def test_e2e_26_same_nanosecond_ticks_tie_break_deterministically(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-26``: open and close decided by sequence, not arrival order."""
    source = make_source(
        count=300,
        dataset_id="e2e-26",
        faults=(
            FaultOp("same_nanosecond", "EURUSD", 10),
            FaultOp("same_nanosecond", "EURUSD", 20),
        ),
    )
    run = run_factory(source=source, scenario_id="e2e-26", test_case_id="E2E-26")
    gate, _ = run.run_clean()
    assert gate.verdict is Verdict.PASS
    assert run.results.bars is not None and run.results.bars.verified


@pytest.mark.whatif("P07-G19-07", "P07-G18-06")
def test_e2e_28_late_data_never_mutates_a_published_build(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-28``: the old READY build stays immutable; corrections are new builds."""
    from p07.dataplane.clickhouse_fake import SinkError

    source = make_source(count=120, dataset_id="e2e-28")
    run = run_factory(source=source, scenario_id="e2e-28", test_case_id="E2E-28")
    gate, report = run.run_clean()
    assert gate.verdict is Verdict.PASS
    published = report["stages"]["build_id"]
    assert run.sink is not None

    bars_before = sorted(
        (row.bar_start_ns, row.tick_count) for row in run.sink.scan_bars(published)
    )
    with pytest.raises(SinkError, match="READY_IMMUTABILITY_VIOLATED"):
        run.sink.insert_bars(published, [])
    assert (
        sorted((row.bar_start_ns, row.tick_count) for row in run.sink.scan_bars(published))
        == bars_before
    )


# -- E2E-29/30/31: publication ------------------------------------------


@pytest.mark.whatif("P07-G19-01", "P07-G19-06")
def test_e2e_29_a_crash_before_the_catalog_swap_leaves_the_old_pointer(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-29``: no false READY when the builder dies before the swap."""
    source = make_source(count=150, dataset_id="e2e-29")
    run = run_factory(source=source, scenario_id="e2e-29", test_case_id="E2E-29")
    switch = process_injectors.arm_crash_at("bars_complete")
    run.seal()
    run.start_dataplane(build_crash_hook=switch)
    run.replay()
    run.fence_and_cut()
    run.scan()
    run.ingest()
    run.audit_and_build()
    with pytest.raises(process_injectors.ProcessKilled):
        run.materialize()
    run.record_receipt(
        process_injectors.confirm_crash_fired(switch, component_name="p06_builder")
    )

    assert run.sink is not None
    assert run.sink.ready_build is None
    assert run.sink.build_status(run.results.build_id or "") is BuildStatus.CANDIDATE
    gate = run.gate()
    assert gate.verdict is Verdict.INCONCLUSIVE
    assert gate.verdict.is_release_blocking()


@pytest.mark.whatif("P07-G19-03", "P07-G19-04")
def test_e2e_30_and_31_the_pointer_is_a_compare_and_swap(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-30``/``E2E-31``: readback resolves the crash; the race has one winner."""
    source = make_source(count=120, dataset_id="e2e-31")
    run = run_factory(source=source, scenario_id="e2e-31", test_case_id="E2E-31")
    gate, report = run.run_clean()
    assert gate.verdict is Verdict.PASS
    assert run.sink is not None and run.builder is not None and run.expectation is not None
    winner = report["stages"]["build_id"]

    # E2E-30: publishing is idempotent, so a retry after a lost reply confirms.
    assert run.builder.publish(winner, expected_current=winner) is True
    assert run.sink.ready_build == winner

    # E2E-31: a second builder with a stale expectation loses the swap.
    audit = run.builder.audit_raw(run.expectation.dataset_id)
    run.builder.build_canonical(
        "rival", audit=audit, source_manifest_sha256="x", expected_identity_count=audit.identities
    )
    run.builder.mark_verified(
        "rival",
        proof={
            "canonical_verified": True,
            "bars_verified": True,
            "source_cut_sealed": True,
            "evidence_ref": "e",
        },
    )
    assert run.builder.publish("rival", expected_current=None) is False
    assert run.sink.ready_build == winner


# -- E2E-32/33: TTL and restore -----------------------------------------


@pytest.mark.whatif("P07-G20-02", "P07-G20-04")
def test_e2e_33_raw_ttl_during_the_audit_is_inconclusive(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-33``: missing raw stops the audit; it does not finish on survivors."""
    source = make_source(count=150, dataset_id="e2e-33")
    run = run_factory(source=source, scenario_id="e2e-33", test_case_id="E2E-33")
    run.seal()
    run.start_dataplane()
    run.replay()
    run.fence_and_cut()
    run.scan()
    run.ingest()
    assert run.sink is not None
    run.record_receipt(
        ch_injectors.expire_raw_ttl(run.sink, before_ingested_at_ns=1 << 60, confirm=True)
    )
    run.audit_and_build()
    run.materialize()
    published = run.verify_and_publish()

    assert published is False
    gate = run.gate()
    assert gate.verdict is Verdict.INCONCLUSIVE
    assert Reason.RAW_ROWS_EXPIRED in {finding.reason for finding in gate.findings}
    assert run.sink.ready_build is None


@pytest.mark.whatif("P07-G20-05", "P07-G20-06")
def test_e2e_32_a_restore_is_only_viable_once_rehearsed(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-32``: restore into a cleared store, then compare identities and bars.

    ``INV-18``: taking a backup proves nothing. The proof is that the restored
    data still reconciles against the oracle.
    """
    from p07.reconcile.bar_reconciler import reconcile_bars
    from p07.reconcile.canonical_reconciler import reconcile_canonical

    source = make_source(count=150, dataset_id="e2e-32")
    run = run_factory(source=source, scenario_id="e2e-32", test_case_id="E2E-32")
    gate, report = run.run_clean()
    assert gate.verdict is Verdict.PASS
    assert run.sink is not None and run.expectation is not None
    build_id = report["stages"]["build_id"]

    snapshot = run.sink.backup()
    run.sink.drop_everything()
    assert run.sink.raw_row_count() == 0
    run.sink.restore(snapshot)

    canonical = reconcile_canonical(
        run.expectation, build_id, list(run.sink.scan_canonical(build_id))
    )
    bars = reconcile_bars(run.expectation, build_id, list(run.sink.scan_bars(build_id)))
    assert canonical.verified
    assert bars.verified
    assert run.sink.ready_build == build_id


# -- E2E-37/38: the harness's own honesty --------------------------------


@pytest.mark.whatif("P07-G27-01", "P07-G27-04")
def test_e2e_37_an_injector_that_did_not_fire_makes_the_run_invalid(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-37``: INVALID, never PASS -- however clean the data looks."""
    source = make_source(count=120, dataset_id="e2e-37")
    run = run_factory(source=source, scenario_id="e2e-37", test_case_id="E2E-37")
    switch = process_injectors.arm_crash_at("a_stage_that_never_happens")
    run.seal()
    run.start_dataplane(ingest_crash_hook=switch)
    run.replay()
    run.fence_and_cut()
    run.scan()
    run.ingest()
    run.audit_and_build()
    run.materialize()
    run.verify_and_publish()
    run.record_receipt(
        process_injectors.confirm_crash_fired(switch, component_name="p05_ingestor")
    )

    gate = run.gate()
    assert gate.verdict is Verdict.INVALID
    assert gate.primary_reason == Reason.FAULT_NOT_CONFIRMED
    # The data really was fine; the *test* was not.
    assert run.results.raw is not None and run.results.raw.diff.equal


@pytest.mark.whatif("P07-G01-03", "P07-G29-03")
def test_e2e_38_a_missing_source_anchor_is_inconclusive_not_pass(
    make_source: Callable[..., dict[str, Any]], run_factory: Callable[..., Any]
) -> None:
    """``E2E-38``: without an external anchor, integrity is unproven.

    Everything reconciles and the run still cannot publish: a snapshot whose
    source cannot be shown unmodified would be a false READY.
    """
    source = make_source(count=120, dataset_id="e2e-38")
    run = run_factory(source=source, scenario_id="e2e-38", test_case_id="E2E-38", anchor=None)
    gate, report = run.run_clean()

    assert gate.verdict is Verdict.INCONCLUSIVE
    assert gate.primary_reason == Reason.SOURCE_ANCHOR_MISSING
    assert report["stages"]["raw"]["diff"]["equal"] is True
    assert report["stages"]["published"] is False


# -- cases the model profile cannot reach -------------------------------


@pytest.mark.needs_real_lab
@pytest.mark.skip(reason="needs a real 3-broker Kafka cluster; NOT_RUN in the UNIT/MODEL profile")
@pytest.mark.whatif("P07-G09-01", "P07-G09-02")
def test_e2e_11_leader_election_on_a_real_cluster() -> None:
    """``E2E-11``: durability under an actual surviving ISR.

    The simulation models the ISR *decision* but not replication, leader
    election or the produce protocol, so a pass here would be a claim the
    harness cannot support (design 2.2 and 9.3).
    """


@pytest.mark.needs_real_lab
@pytest.mark.skip(reason="needs a real ClickHouse server with materialised views; NOT_RUN")
@pytest.mark.whatif("P07-G18-08", "P07-G18-09")
def test_e2e_27_materialised_view_partial_states() -> None:
    """``E2E-27`` (MV half): unsettled ``AggregateFunction`` states.

    :mod:`p07.dataplane.analytics` computes bars directly rather than through an
    incremental materialised view, so partial-state and merge behaviour is out
    of reach here.
    """


@pytest.mark.needs_real_lab
@pytest.mark.skip(reason="needs cgroups and a real network stack; NOT_RUN")
@pytest.mark.whatif("P07-G23-01", "P07-G24-05")
def test_e2e_35_and_36_oom_and_packet_loss() -> None:
    """``E2E-35``/``E2E-36``: kernel memory pressure and real packet loss.

    Application-level budgets are covered in ``tests/unit``; cgroup pressure and
    a lossy network are not reachable in-process.
    """


@pytest.mark.needs_real_lab
@pytest.mark.skip(reason="capacity profile: needs real brokers and hardware; NOT_RUN")
@pytest.mark.whatif("P07-G31-01", "P07-G31-02")
def test_e2e_39_capacity_at_120k_events_per_second() -> None:
    """``E2E-39``: offered versus achieved throughput under query load.

    Design 13.1's S1-S6 profiles are NOT_RUN, and no throughput figure appears
    in any report this harness writes, because none was measured.
    """
