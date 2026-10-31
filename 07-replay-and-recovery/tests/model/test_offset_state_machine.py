"""A stateful model of one partition's publish/poll/insert/commit interleavings.

Design 12.2 sketches this model and names the safety properties worth exploring
over random schedules::

    Commit(next, epoch) requires owner current AND every o in [start,next) completed
    WorkerCompletion with a stale epoch may affect the sink, but cannot authorise a commit

Hypothesis generates the interleavings. The point is not the happy path -- the
unit tests pin that -- but the orderings nobody thought to write down: an
ambiguous insert followed by a rebalance followed by a retry, a crash between
the insert and the commit, a fenced worker that keeps writing.

The model runs over the **real** ingestor, broker and sink rather than an
abstract transition system, so a violation is a bug in the code under test and
not in a second description of it.
"""

from __future__ import annotations

from hypothesis import HealthCheck, settings
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from p07.contracts import TICK_TOPIC, EventKind, SemanticCore
from p07.dataplane.clickhouse_fake import FakeClickHouse, SinkAckUnknown, SinkError
from p07.dataplane.codec import DEFAULT_CODEC
from p07.dataplane.ingestion import BatchIngestor, identities_in_raw
from p07.dataplane.kafka_fake import FakeKafka

PARTITION = 0
GROUP = "model-group"


def core(sequence: int) -> SemanticCore:
    return SemanticCore(
        dataset_id="ds",
        symbol="EURUSD",
        sequence=sequence,
        event_time_ns=1_000_000 * sequence,
        price_scale=100000,
        event_kind=EventKind.TICK,
        source_schema_version="sim.v1",
        bid_ticks=None,
        ask_ticks=None,
        price_ticks=100_000 + sequence,
    )


class OffsetMachine(RuleBasedStateMachine):
    """One topic-partition, one consumer group, arbitrary interleavings."""

    def __init__(self) -> None:
        super().__init__()
        self.kafka = FakeKafka(partitions=1, topics=(TICK_TOPIC,))
        self.sink = FakeClickHouse()
        self.next_sequence = 1
        self.published_identities: set[tuple[str, str, int]] = set()
        self.worker = self._new_worker(epoch=self.kafka.group_epoch(GROUP))
        self.stale_worker: BatchIngestor | None = None

    def _new_worker(self, *, epoch: int) -> BatchIngestor:
        worker = BatchIngestor(
            self.kafka,
            self.sink,
            group=GROUP,
            assignment=[(TICK_TOPIC, PARTITION)],
            epoch=epoch,
            batch_max_records=3,
            max_insert_retries=2,
            worker_id=f"w{epoch}",
        )
        worker.resume_from_broker(cut_start={(TICK_TOPIC, PARTITION): 0})
        return worker

    # -- rules ------------------------------------------------------------

    @rule()
    def publish(self) -> None:
        body = core(self.next_sequence)
        self.next_sequence += 1
        self.kafka.produce(
            TICK_TOPIC,
            PARTITION,
            key=body.symbol.encode(),
            value=DEFAULT_CODEC.encode(body),
            headers={"dataset_id": body.dataset_id},
        )
        self.published_identities.add(tuple(body.identity))

    @rule()
    def poll_and_ingest(self) -> None:
        try:
            self.worker.poll_once()
        except (SinkAckUnknown, SinkError):
            # An unresolved or failed insert. Whether the frontier stayed put is
            # checked by the invariants, not by this handler.
            pass

    @rule()
    def arm_ambiguous_insert(self) -> None:
        self.sink.ack_loss_once = True

    @rule()
    def arm_failing_insert(self) -> None:
        self.sink.fail_before_commit_once = True

    @rule()
    def crash_and_restart_worker(self) -> None:
        """The worker dies; a fresh one rebuilds position from the broker alone."""
        self.worker = self._new_worker(epoch=self.kafka.group_epoch(GROUP))

    @rule()
    def rebalance(self) -> None:
        """A new owner takes over; the old worker keeps its stale epoch."""
        self.stale_worker = self.worker
        epoch = self.kafka.bump_group_epoch(GROUP)
        self.worker = self._new_worker(epoch=epoch)

    @precondition(lambda self: self.stale_worker is not None)
    @rule()
    def stale_worker_keeps_working(self) -> None:
        """A fenced worker may still write to the sink. It may not commit."""
        assert self.stale_worker is not None
        try:
            self.stale_worker.poll_once()
        except (SinkAckUnknown, SinkError):
            pass

    # -- safety invariants -------------------------------------------------

    @invariant()
    def commit_never_runs_ahead_of_completion(self) -> None:
        """``INV-03``, over every interleaving the machine can produce."""
        for worker in (self.worker, self.stale_worker):
            if worker is None:
                continue
            progress = worker.progress.get((TICK_TOPIC, PARTITION))
            if progress is None or progress.committed_next is None:
                continue
            assert progress.committed_next <= progress.completed_contiguous_next()

    @invariant()
    def a_committed_offset_is_backed_by_rows_in_the_sink(self) -> None:
        """Nothing may be committed whose rows are not in the sink.

        The strong form of ``INV-03``: a commit asserts durability, so every
        offset below the committed frontier must have produced a raw row. It is
        checked against the sink rather than the worker's own bookkeeping, so a
        worker that miscounts is caught.
        """
        committed = self.kafka.committed(GROUP, TICK_TOPIC, PARTITION)
        if not committed:
            return
        offsets_in_sink = {row.offset for row in self.sink.scan_raw(dataset_id="ds")}
        for offset in range(committed):
            assert offset in offsets_in_sink, f"offset {offset} committed but absent from the sink"

    @invariant()
    def a_fenced_worker_never_holds_the_current_epoch(self) -> None:
        """``INV-04``: the broker refuses its commits once it is fenced."""
        if self.stale_worker is None:
            return
        assert self.stale_worker.epoch <= self.kafka.group_epoch(GROUP)

    @invariant()
    def physical_duplicates_never_create_new_identities(self) -> None:
        """Retries may duplicate rows; they may not invent an event."""
        counts = identities_in_raw(self.sink.scan_raw(dataset_id="ds"))
        for identity in counts:
            assert tuple(identity) in self.published_identities

    @invariant()
    def the_sink_never_holds_an_unpublished_offset(self) -> None:
        high = self.kafka.high_watermark(TICK_TOPIC, PARTITION)
        for row in self.sink.scan_raw(dataset_id="ds"):
            assert row.offset < high


OffsetMachine.TestCase.settings = settings(
    max_examples=40,
    stateful_step_count=25,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)

TestOffsetMachine = OffsetMachine.TestCase
