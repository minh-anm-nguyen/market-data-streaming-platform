"""E2E-03 lost ClickHouse response, and E2E-15 de-duplication window eviction.

These are the scenarios the whole design exists for. The fake sink writes the
rows and *then* loses the acknowledgement, so the tests can assert on the one
thing a real client can never see: that the data is present while the client
believes nothing.
"""

from __future__ import annotations

from p05_ingest.metrics import (
    BATCHES_INSERT_ATTEMPTED,
    CH_BATCHES_ACKED,
    CH_BATCHES_UNKNOWN,
    KAFKA_COMMIT_ATTEMPTS,
)
from p05_ingest.model import SinkOutcome, TopicPartition
from p05_ingest.testkit import TICKS_TOPIC, Behaviour, make_tick
from tests.conftest import build_harness, make_config

TP0 = TopicPartition(TICKS_TOPIC, 0)


def produce_ticks(harness, count: int, *, partition: int = 0) -> None:
    for sequence in range(count):
        event = make_tick(sequence=sequence)
        harness.cluster.produce(
            TICKS_TOPIC, partition, key=b"EURUSD", value=event.SerializeToString()
        )


def ticks_only(dev_config, **batch):
    return make_config(
        dev_config, kafka={"topics": ["md.ticks.v1"]}, batch=batch or {}
    )


# ---------------------------------------------------------------- E2E-03


def test_e2e_03_ambiguous_ack_then_success_commits_exactly_once(dev_config, clock):
    """The server wrote the rows; the response was lost; the retry succeeds.

    PASS oracle: no commit while the outcome is unknown, the retry carries the
    identical batch, and the run completes.
    """
    cfg = ticks_only(dev_config, per_partition_max_rows=5)
    harness = build_harness(
        cfg,
        clock=clock,
        partitions=1,
        sink_script=(Behaviour.WRITE_THEN_LOSE_ACK,),
    )
    produce_ticks(harness, 5)

    status = harness.finish()

    attempts = harness.sink.attempts
    assert len(attempts) == 2, "one lost ACK, then one retry"
    assert attempts[0].behaviour is Behaviour.WRITE_THEN_LOSE_ACK
    assert attempts[0].applied, "the rows really did land on the first attempt"

    # I-04: the retry re-sent the identical immutable batch.
    assert attempts[1].token == attempts[0].token
    assert attempts[1].rows == attempts[0].rows

    # The second attempt was suppressed by block de-duplication, because the
    # token matched a write still inside the window.
    assert attempts[1].deduplicated
    assert not attempts[1].applied

    assert harness.sink.row_count == 5, "no physical duplicate this time"
    assert len(harness.logical_identities()) == 5

    assert status.complete, status.reason
    assert harness.committed() == {TP0: 5}
    assert harness.metrics.counter_total(CH_BATCHES_UNKNOWN) == 1
    assert harness.metrics.counter_total(CH_BATCHES_ACKED) == 1


def test_e2e_03_unknown_outcome_never_commits_before_it_resolves(dev_config, clock):
    """Step-level check: while the batch is ambiguous, nothing is committed."""
    cfg = ticks_only(dev_config, per_partition_max_rows=3)
    harness = build_harness(
        cfg,
        clock=clock,
        partitions=1,
        sink_script=(
            Behaviour.WRITE_THEN_LOSE_ACK,
            Behaviour.WRITE_THEN_LOSE_ACK,
            Behaviour.WRITE_THEN_LOSE_ACK,
        ),
        sink_default=Behaviour.WRITE_THEN_LOSE_ACK,
    )
    produce_ticks(harness, 3)

    for _ in range(40):
        harness.engine.step()
        assert harness.committed() == {}, (
            "an offset was committed although every attempt ended ambiguous"
        )
        if harness.engine.blocked_partitions:
            break
        harness.tick_timer()

    status = harness.engine.shutdown()
    assert not status.complete
    assert TP0 in harness.engine.blocked_partitions
    assert "ambiguous" in harness.engine.blocked_partitions[TP0]
    assert harness.committed() == {}


def test_e2e_03_retry_budget_exhaustion_blocks_the_partition(dev_config, clock):
    """Bounded retry: after the budget, stop rather than loop (spec NFR-07)."""
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={"per_partition_max_rows": 2},
        retry={"max_attempts": 3, "overall_deadline_s": 10},
    )
    harness = build_harness(
        cfg, clock=clock, partitions=1, sink_default=Behaviour.REFUSE_BEFORE_WRITE
    )
    produce_ticks(harness, 2)

    status = harness.finish()

    assert len(harness.sink.attempts) == 3, "exactly max_attempts, then stop"
    assert harness.sink.row_count == 0, "a pre-write refusal applies nothing"
    assert not status.complete
    assert TP0 in harness.engine.blocked_partitions
    assert "budget exhausted" in harness.engine.blocked_partitions[TP0]
    assert harness.committed() == {}
    assert harness.metrics.counter(
        BATCHES_INSERT_ATTEMPTED, result_category=str(SinkOutcome.TRANSIENT)
    ) == 3


def test_e2e_03_backoff_is_bounded_and_jittered(dev_config, clock):
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={"per_partition_max_rows": 2},
        retry={
            "max_attempts": 4,
            "initial_backoff_ms": 100,
            "max_backoff_ms": 400,
            "overall_deadline_s": 20,
        },
    )
    harness = build_harness(
        cfg, clock=clock, partitions=1, sink_default=Behaviour.REFUSE_BEFORE_WRITE
    )
    produce_ticks(harness, 2)
    harness.finish()

    sleeps = clock.sleeps
    assert len(sleeps) == 3, "one sleep between each of four attempts"
    for delay in sleeps:
        assert 0.05 <= delay <= 0.4
    assert sleeps == sorted(sleeps), "monotonically increasing under the cap"


def test_permanent_error_is_not_retried(dev_config, clock):
    """Spec 5.2: a structural refusal must alert, not loop."""
    cfg = ticks_only(dev_config, per_partition_max_rows=2)
    harness = build_harness(
        cfg, clock=clock, partitions=1, sink_default=Behaviour.PERMANENT_ERROR
    )
    produce_ticks(harness, 2)

    status = harness.finish()

    assert len(harness.sink.attempts) == 1, "no retry of a permanent error"
    assert not status.complete
    assert "permanent" in harness.engine.blocked_partitions[TP0].lower()
    assert harness.committed() == {}


def test_unclassified_error_is_treated_as_ambiguous(dev_config, clock):
    """Pessimistic default: it might have been applied, so do not commit."""
    cfg = ticks_only(dev_config, per_partition_max_rows=2)
    harness = build_harness(
        cfg, clock=clock, partitions=1, sink_default=Behaviour.UNKNOWN_ERROR
    )
    produce_ticks(harness, 2)

    harness.finish()
    assert harness.committed() == {}
    assert harness.metrics.counter_total(CH_BATCHES_UNKNOWN) >= 1


# ---------------------------------------------------------------- E2E-15


def test_e2e_15_dedup_window_eviction_produces_a_physical_duplicate(
    dev_config, clock
):
    """Spec E2E-15: physical duplicates are allowed; logical count stays one.

    The window is emptied between the lost ACK and the retry, exactly as a
    long delay or a server restart would do. The retry then writes the rows a
    second time -- and that is the honest, documented outcome, not a bug.
    """
    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(
        cfg,
        clock=clock,
        partitions=1,
        sink_script=(Behaviour.WRITE_THEN_LOSE_ACK,),
        dedup_window=1,
        evict_dedup_after_attempt=1,
    )
    produce_ticks(harness, 4)

    status = harness.finish()

    assert len(harness.sink.attempts) == 2
    assert harness.sink.attempts[1].applied, "the retry wrote the rows again"
    assert harness.sink.row_count == 8, "four rows, twice: a physical duplicate"

    # The logical view still counts four events, and there is no conflict.
    assert len(harness.logical_identities()) == 4
    counts = harness.sink.physical_counts_by_identity()
    assert all(n == 2 for n in counts.values())
    digests = harness.sink.digests_by_identity()
    assert all(len(v) == 1 for v in digests.values()), "duplicates, not conflicts"

    assert status.complete, status.reason


def test_e2e_15_reconciler_reports_duplicates_without_failing(dev_config, clock):
    """At-least-once means duplicates are expected, so they are not a NO-GO."""
    from p05_ingest.reconciler import (
        Verdict,
        observed_from_sink_rows,
        reconcile,
    )

    cfg = ticks_only(dev_config, per_partition_max_rows=4)
    harness = build_harness(
        cfg,
        clock=clock,
        partitions=1,
        sink_script=(Behaviour.WRITE_THEN_LOSE_ACK,),
        dedup_window=1,
        evict_dedup_after_attempt=1,
    )
    produce_ticks(harness, 4)
    harness.finish()

    from p05_ingest.canonical_digest import payload_digest
    from p05_ingest.contract_validator import ContractValidator
    from p05_ingest.reconciler import observed_from_kafka_scan

    kafka_observed, refused = observed_from_kafka_scan(
        harness.cluster.scan(),
        validator=ContractValidator(cfg.contracts, cfg.project),
        mapper_digest=payload_digest,
    )
    assert refused == []

    report = reconcile(
        expected=kafka_observed,
        kafka_observed=kafka_observed,
        sink_observed=observed_from_sink_rows(harness.sink.rows),
    )
    assert report.verdict is Verdict.GO, report.summary()
    assert report.sink_physical_count == 8
    assert report.sink_logical_count == 4
    assert report.duplicate_rows == 4
    assert len(report.duplicate_physical) == 4
    assert report.conflicts == {}
    assert any("physical row" in reason for reason in report.reasons)


def test_token_is_reused_across_every_retry_of_one_batch(dev_config, clock):
    """I-04 at the token level: de-duplication can only work if it is stable."""
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={"per_partition_max_rows": 3},
        retry={"max_attempts": 4, "overall_deadline_s": 20},
    )
    harness = build_harness(
        cfg,
        clock=clock,
        partitions=1,
        sink_script=(
            Behaviour.REFUSE_BEFORE_WRITE,
            Behaviour.REFUSE_BEFORE_WRITE,
            Behaviour.ACK,
        ),
    )
    produce_ticks(harness, 3)
    harness.finish()

    tokens = {a.token for a in harness.sink.attempts}
    assert len(tokens) == 1, f"the token changed between retries: {tokens}"
    assert len(harness.sink.attempts) == 3


def test_commit_follows_a_late_success_after_transient_failures(dev_config, clock):
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={"per_partition_max_rows": 3},
        retry={"max_attempts": 5, "overall_deadline_s": 20},
    )
    harness = build_harness(
        cfg,
        clock=clock,
        partitions=1,
        sink_script=(Behaviour.REFUSE_BEFORE_WRITE, Behaviour.ACK),
    )
    produce_ticks(harness, 3)

    status = harness.finish()

    assert status.complete, status.reason
    assert harness.sink.row_count == 3
    assert harness.committed() == {TP0: 3}
    assert harness.metrics.counter(KAFKA_COMMIT_ATTEMPTS, status="confirmed") >= 1
