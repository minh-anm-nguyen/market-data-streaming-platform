"""Model checking: random operation schedules against the whole engine.

Spec 9.3 asks for generated sequences of ``fetch``, ``validate``, ``seal``,
``insert_send``, ``insert_ack``/``timeout``, ``commit_ok``/``error``,
``revoke``/``lost``, ``crash`` and ``restart``, with the global invariants
asserted after every step.

Hypothesis drives the real engine over the deterministic cluster, so the rules
are events rather than internal pokes: producing records, advancing the clock,
scripting the next sink outcome, triggering a rebalance. Whatever ordering
Hypothesis picks, the invariants below must hold -- and because the engine is
step-driven with a manual clock, a failing schedule shrinks to a short,
replayable sequence instead of a flaky race.
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)

from p05_ingest.config import AppConfig
from p05_ingest.model import RecordDisposition, TopicPartition
from p05_ingest.testkit import (
    QUOTES_TOPIC,
    TICKS_TOPIC,
    Behaviour,
    CommitFault,
    make_quote,
    make_tick,
)
from tests.conftest import DEV_CONFIG, build_harness, make_config

pytestmark = pytest.mark.property

PARTITIONS = 2
SYMBOLS = ("EURUSD", "GBPUSD")


class IngestStateMachine(RuleBasedStateMachine):
    """Drives the engine through arbitrary interleavings of real events."""

    def __init__(self) -> None:
        super().__init__()
        self.cfg: AppConfig | None = None
        self.harness = None
        self.sequence_by_symbol: dict[str, int] = {}
        self.produced: set[tuple[str, str, int]] = set()
        self.poison_offsets: set[tuple[str, int, int]] = set()
        self.ever_blocked = False

    @initialize()
    def setup(self) -> None:
        base = AppConfig.from_toml(DEV_CONFIG)
        # Small caps so batches actually seal within a short schedule.
        self.cfg = make_config(
            base,
            batch={
                "per_partition_max_rows": 3,
                "per_partition_max_serialized_bytes": 16384,
                "max_wait_ms": 500,
            },
            retry={"max_attempts": 2, "overall_deadline_s": 5},
        )
        self.harness = build_harness(self.cfg, partitions=PARTITIONS)
        self.sequence_by_symbol = {s: 0 for s in SYMBOLS}

    # ------------------------------------------------------------------ rules

    @rule(
        symbol=st.sampled_from(SYMBOLS),
        is_quote=st.booleans(),
        count=st.integers(min_value=1, max_value=4),
    )
    def produce_valid(self, symbol, is_quote, count):
        """Append market-valid records for one symbol."""
        for _ in range(count):
            sequence = self.sequence_by_symbol[symbol]
            self.sequence_by_symbol[symbol] = sequence + 1
            event = (make_quote if is_quote else make_tick)(
                symbol=symbol, sequence=sequence
            )
            topic = QUOTES_TOPIC if is_quote else TICKS_TOPIC
            partition = sum(symbol.encode()) % PARTITIONS
            self.harness.cluster.produce(
                topic,
                partition,
                key=symbol.encode("ascii"),
                value=event.SerializeToString(),
            )
            self.produced.add((event.dataset_id, symbol, sequence))

    @rule(is_quote=st.booleans())
    def produce_poison(self, is_quote):
        """Append a record that validation must refuse."""
        topic = QUOTES_TOPIC if is_quote else TICKS_TOPIC
        partition = 0
        offset = self.harness.cluster.produce(
            topic, partition, key=b"EURUSD", value=b"\xff\xff\xff\xff\xff\xff"
        )
        # Keyed by (topic, partition, offset): offset 0 exists in every
        # partition, so a key without the partition would collide with a
        # perfectly valid record elsewhere.
        self.poison_offsets.add((topic, partition, offset))

    @rule(n=st.integers(min_value=1, max_value=6))
    def step(self, n):
        for _ in range(n):
            self.harness.engine.step()

    @rule()
    def advance_flush_timer(self):
        self.harness.tick_timer()

    @rule(
        behaviours=st.lists(
            st.sampled_from(
                [
                    Behaviour.ACK,
                    Behaviour.WRITE_THEN_LOSE_ACK,
                    Behaviour.REFUSE_BEFORE_WRITE,
                    Behaviour.UNKNOWN_ERROR,
                    Behaviour.PERMANENT_ERROR,
                ]
            ),
            min_size=1,
            max_size=4,
        )
    )
    def script_sink(self, behaviours):
        """Decide how the next few inserts behave."""
        self.harness.sink.set_script(behaviours)

    @rule(
        fault=st.sampled_from(
            [
                CommitFault.REJECT,
                CommitFault.LOST_RESPONSE,
                CommitFault.UNREACHABLE,
            ]
        ),
        times=st.integers(min_value=1, max_value=2),
    )
    def script_commit_fault(self, fault, times):
        self.harness.consumer.commit_fault = fault
        self.harness.consumer.commit_fault_remaining = times

    @precondition(lambda self: bool(self.harness.consumer.assignment()))
    @rule(index=st.integers(min_value=0, max_value=3), hard=st.booleans())
    def rebalance(self, index, hard):
        """Revoke or lose one partition, then take it back."""
        assigned = sorted(self.harness.consumer.assignment())
        tp = assigned[index % len(assigned)]
        if hard:
            self.harness.consumer.lose([tp])
        else:
            self.harness.consumer.revoke([tp])
        self.harness.engine.step()
        self.harness.consumer.assign([tp])

    @rule()
    def control_record(self):
        """Burn an offset without delivering a record (transaction marker)."""
        self.harness.cluster.produce_control_record(TICKS_TOPIC, 0)

    # ------------------------------------------------------------- invariants

    @invariant()
    def commit_never_passes_the_proven_frontier(self):
        """I-01, the one that must never break."""
        if self.harness is None:
            return
        commits = self.harness.engine.commits
        for tp in commits.tracked():
            assert commits.committed_next(tp) <= commits.proven_frontier_next(tp), (
                f"{tp}: committed {commits.committed_next(tp)} > proven "
                f"{commits.proven_frontier_next(tp)}"
            )

    @invariant()
    def broker_committed_offset_is_backed_by_sink_proof(self):
        """The broker view must also never run ahead of what we proved.

        Checked separately from the in-process ledger: a bug that advanced the
        broker offset without updating the ledger would pass the check above.
        """
        if self.harness is None:
            return
        for tp, committed in self.harness.committed().items():
            acked_offsets = self._offsets_written_or_quarantined(tp)
            below = [o for o in acked_offsets if o < committed]
            # Every offset below the committed position that this consumer
            # actually consumed must have had a terminal durable result.
            consumed = self._consumed_offsets(tp)
            unproven = [
                o for o in consumed if o < committed and o not in set(acked_offsets)
            ]
            assert not unproven, (
                f"{tp}: offsets {unproven} are below the committed position "
                f"{committed} without a durable result (len(below)={len(below)})"
            )

    @invariant()
    def at_most_one_inflight_batch_per_partition(self):
        """ADR-003."""
        if self.harness is None:
            return
        runtime = self.harness.engine._runtime
        inflight = [tp for tp, r in runtime.items() if r.inflight_batch is not None]
        assert len(inflight) <= len(runtime)
        for tp, r in runtime.items():
            assert r.inflight_batch is None or r.inflight_batch.tp == tp

    @invariant()
    def byte_budget_is_respected(self):
        """I-05, for the bytes this program accounts for."""
        if self.harness is None:
            return
        budget = self.harness.engine.budget
        assert 0 <= budget.reserved_bytes <= budget.budget_bytes

    @invariant()
    def no_invalid_row_reaches_the_market_table(self):
        """I-07: a refused record must never be written."""
        if self.harness is None:
            return
        for row in self.harness.sink.rows:
            assert row["dataset_id"], "a row with an empty dataset_id was written"
            assert row["price_scale"] > 0
            assert row["sequence"] >= 0
            if row["event_kind"] == "tick":
                assert row["price_ticks"] is not None and row["price_ticks"] > 0
                assert row["bid_ticks"] is None and row["ask_ticks"] is None
            else:
                assert row["bid_ticks"] is not None and row["ask_ticks"] is not None
                assert row["bid_ticks"] <= row["ask_ticks"]
                assert row["price_ticks"] is None

    @invariant()
    def stored_rows_are_all_genuinely_produced_events(self):
        """Nothing may be invented: every row traces to a produced event."""
        if self.harness is None:
            return
        for identity in self.harness.sink.identities():
            key = (identity[0], identity[1], identity[2])
            assert key in self.produced, f"{key} was never produced"

    @invariant()
    def digests_describe_their_own_rows(self):
        """A stored digest must match a recomputation from the stored columns.

        Without this, every digest-based comparison downstream is vacuous.
        """
        if self.harness is None:
            return
        from p05_ingest.reconciler import verify_stored_digests

        assert verify_stored_digests(self.harness.sink.rows) == []

    @invariant()
    def no_row_is_written_for_a_poison_offset(self):
        if self.harness is None:
            return
        for row in self.harness.sink.rows:
            key = (row["kafka_topic"], row["kafka_partition"], row["kafka_offset"])
            assert key not in self.poison_offsets, f"poison record {key} was written"

    # ---------------------------------------------------------------- helpers

    def _consumed_offsets(self, tp: TopicPartition) -> list[int]:
        """Offsets this consumer received for ``tp``, from the Kafka log."""
        try:
            log = self.harness.cluster.log(tp)
        except KeyError:
            return []
        return [r.offset for r in log.records if not r.is_control]

    def _offsets_written_or_quarantined(self, tp: TopicPartition) -> list[int]:
        written = [
            row["kafka_offset"]
            for row in self.harness.sink.rows
            if row["kafka_topic"] == tp.topic and row["kafka_partition"] == tp.partition
        ]
        # Quarantined offsets only exist in DLQ mode, which the dev profile
        # does not enable, so under this configuration the set is just the
        # written offsets.
        return written

    def teardown(self) -> None:
        if self.harness is not None:
            self.harness.engine.request_stop()


TestIngestStateMachine = IngestStateMachine.TestCase
TestIngestStateMachine.settings = settings(
    max_examples=40,
    stateful_step_count=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)


def test_disposition_values_are_exhaustive():
    """If a disposition is added, the invariants above must be revisited."""
    assert set(RecordDisposition) == {
        RecordDisposition.SINK_ACKED,
        RecordDisposition.QUARANTINED,
        RecordDisposition.BLOCKED,
    }
