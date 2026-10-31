"""The owner loop: the only place Kafka is touched and offsets are committed.

Spec section 4.4. The loop is exposed as a single ``step()`` so tests can drive
it one iteration at a time against a deterministic fake cluster, and ``run()``
is ``step()`` in a while loop. That is not a convenience bolted on afterwards:
a step function is the only way to assert "after exactly this sequence of
events the committed offset is X", which is what the property and
model-checking tests in spec 9.3 require.

Ordering inside one step is load-bearing:

  1. drain sink completions, so a slot freed by a finished batch is usable in
     this same iteration rather than one later;
  2. advance the frontier and commit, so proven progress is persisted before
     any new work is admitted;
  3. poll and validate new records -- but only while the byte budget can
     certainly accommodate the largest record the contract allows;
  4. seal and submit batches that are ready;
  5. apply pause/resume.

Doing (3) before (1) would let the buffer fill while results sat unread, which
is how a bounded queue becomes an unbounded one.

Why step (3) checks the budget *before* polling rather than after: once
``poll()`` returns a record it is out of the client fetch queue, and the only
way to see it again is ``seek()``. There is therefore no such thing as
"refusing" a record that has already been handed over -- it must either be
buffered or the run must stop. So the loop stops polling while headroom for a
maximum-size record is not available, and admission after validation cannot
fail. Backpressure is expressed as not-fetching, never as dropping.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from .assignment_manager import AssignmentManager
from .budget_manager import BudgetManager
from .commit_coordinator import CommitCoordinator
from .config import AppConfig
from .contract_validator import ContractValidator, ValidationOk
from .crash_hooks import (
    CRASH_01_AFTER_FETCH_BEFORE_DECODE,
    CRASH_02_AFTER_DECODE_BEFORE_BATCH_SEAL,
    CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT,
    CRASH_05_AFTER_COMMIT_RESPONSE_BEFORE_REPORT_FSYNC,
    NO_CRASH,
    CrashHooks,
)
from .errors import CommitNotConfirmedError, InvariantViolation, RetentionLostError
from .event_mapper import EventMapper, estimate_row_bytes
from .kafka_source import KafkaSource
from .metrics import (
    BATCHES_SEALED,
    BUFFER_BYTES,
    COMMITTED_NEXT_OFFSET,
    INFLIGHT_BATCHES,
    KAFKA_COMMIT_ATTEMPTS,
    KAFKA_LAG_OFFSETS,
    KAFKA_RECORDS_FETCHED,
    PARTITION_STATE,
    PROVEN_FRONTIER_NEXT_OFFSET,
    QUARANTINED_RECORDS,
    REBALANCES,
    RECORDS_VALIDATED,
    STALE_COMPLETIONS_FENCED,
    MetricsRegistry,
)
from .model import (
    ConsumedRecord,
    EventKind,
    LogicalProjection,
    PartitionState,
    RecordDisposition,
    RunStatus,
    SealedBatch,
    SealReason,
    SinkOutcome,
    SinkResult,
    TopicPartition,
)
from .partition_batcher import PartitionBatcher
from .quarantine import QuarantinePolicy
from .retry_policy import CircuitBreaker
from .sink_worker import SinkExecutor


@dataclass(slots=True)
class StepReport:
    """What one iteration did. Tests assert on this instead of on log text."""

    records_fetched: int = 0
    records_accepted: int = 0
    records_refused: int = 0
    batches_sealed: int = 0
    batches_submitted: int = 0
    results_drained: int = 0
    commits_confirmed: int = 0
    commits_unknown: int = 0
    stale_fenced: int = 0
    paused: tuple[TopicPartition, ...] = ()
    resumed: tuple[TopicPartition, ...] = ()
    fetch_withheld_for_budget: bool = False
    idle: bool = False

    @property
    def did_work(self) -> bool:
        return bool(
            self.records_fetched
            or self.batches_sealed
            or self.batches_submitted
            or self.results_drained
            or self.commits_confirmed
            or self.paused
            or self.resumed
        )


@dataclass(slots=True)
class _PartitionRuntime:
    """Per-partition engine state not owned by another component."""

    epoch: int
    inflight_batch: SealedBatch | None = None
    quarantined: int = 0
    dropped_after_block: int = 0


class IngestEngine:
    """Single-threaded owner of the consumer, the batches and the commits."""

    def __init__(
        self,
        cfg: AppConfig,
        *,
        source: KafkaSource,
        executor: SinkExecutor,
        metrics: MetricsRegistry | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        crash_hooks: CrashHooks = NO_CRASH,
        quarantine: QuarantinePolicy | None = None,
        validator: ContractValidator | None = None,
    ) -> None:
        self._cfg = cfg
        self._source = source
        self._executor = executor
        self._metrics = metrics or MetricsRegistry()
        self._monotonic = monotonic
        self._crash = crash_hooks

        self._validator = validator or ContractValidator(cfg.contracts, cfg.project)
        self._mapper = EventMapper(
            ingest_run_id=cfg.project.run_id,
            kafka_cluster_id=cfg.kafka.cluster_id,
            topic_generation=cfg.project.topic_generation,
        )
        self._batcher = PartitionBatcher(
            cfg.batch,
            cfg.clickhouse,
            topic_generation=cfg.project.topic_generation,
            monotonic=monotonic,
        )
        self._budget = BudgetManager(
            budget_bytes=cfg.batch.app_buffer_budget_bytes,
            high_watermark=cfg.batch.pause_high_watermark,
            low_watermark=cfg.batch.resume_low_watermark,
        )
        self._assignments = AssignmentManager()
        self._commits = CommitCoordinator()
        self._quarantine = quarantine or QuarantinePolicy(
            cfg.quarantine, ingest_run_id=cfg.project.run_id
        )
        self._breaker = CircuitBreaker(threshold=max(2, cfg.retry.max_attempts))

        self._runtime: dict[TopicPartition, _PartitionRuntime] = {}
        self._blocked: dict[TopicPartition, str] = {}
        self._stopping = False
        self._fatal: str | None = None
        self._lost_partitions: list[TopicPartition] = []
        #: Partitions paused because their batch is full and the sink is
        #: busy. Tracked apart from the budget pause set so that clearing
        #: one does not resume a partition the other still wants paused.
        self._batch_paused: set[TopicPartition] = set()
        self._worst_case_row_bytes = self._compute_worst_case_row_bytes()

    def _compute_worst_case_row_bytes(self) -> int:
        """Upper bound on the budget cost of any contract-legal record.

        Built from the configured caps rather than from observed data, so the
        headroom check cannot be invalidated by an unusually large but still
        legal record.
        """
        contracts = self._cfg.contracts
        worst = LogicalProjection(
            dataset_id="d" * contracts.max_dataset_id_bytes,
            symbol="S" * contracts.max_key_bytes,
            sequence=0,
            event_time_ns=0,
            price_scale=1,
            kind=EventKind.QUOTE,
            source_schema_version="v" * 64,
        )
        return estimate_row_bytes(worst, contracts.max_record_value_bytes)

    # ------------------------------------------------------------- properties

    @property
    def metrics(self) -> MetricsRegistry:
        return self._metrics

    @property
    def commits(self) -> CommitCoordinator:
        return self._commits

    @property
    def assignments(self) -> AssignmentManager:
        return self._assignments

    @property
    def budget(self) -> BudgetManager:
        return self._budget

    @property
    def batcher(self) -> PartitionBatcher:
        return self._batcher

    @property
    def blocked_partitions(self) -> dict[TopicPartition, str]:
        return dict(self._blocked)

    @property
    def fatal_reason(self) -> str | None:
        return self._fatal

    @property
    def lost_partitions(self) -> tuple[TopicPartition, ...]:
        return tuple(self._lost_partitions)

    # -------------------------------------------------------------- lifecycle

    def start(self) -> None:
        self._source.subscribe(
            self._cfg.kafka.topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )

    def _on_assign(self, tps: Sequence[TopicPartition]) -> None:
        """Take ownership and anchor each frontier to the broker position.

        Spec I-01 note: the frontier starts at the authoritative committed next
        offset. Starting at zero would re-read the whole partition and imply
        every earlier commit was unproven.
        """
        self._metrics.inc(REBALANCES, kind="assign")
        granted = self._assignments.assign(list(tps))
        for tp, epoch in granted.items():
            committed = self._source.committed_next(tp)
            self._commits.start_partition(tp, committed_next=committed, epoch=epoch)
            self._runtime[tp] = _PartitionRuntime(epoch=epoch)
            self._metrics.set_gauge(
                COMMITTED_NEXT_OFFSET,
                committed,
                topic=tp.topic,
                partition=str(tp.partition),
            )
            self._set_partition_state(tp, PartitionState.ASSIGNED)

    def _on_revoke(self, tps: Sequence[TopicPartition]) -> None:
        """Cooperative revoke: commit what is already proven, then let go."""
        self._metrics.inc(REBALANCES, kind="revoke")
        self._assignments.revoke(list(tps))
        # Drain whatever the workers have already finished before letting go.
        # Without this the drain window is nominal: a batch that ClickHouse
        # acknowledged a moment ago would be thrown away unrecorded and
        # re-read by the next owner, turning a clean handover into a
        # guaranteed duplicate (spec 4.3, on_revoke).
        self._drain_results(StepReport())
        for tp in tps:
            # Discard the open batch: it was never sent, so the next owner
            # simply re-reads those records.
            self._release_open(tp)
            self._try_commit(tp)
        self._assignments.complete_revoke(list(tps))
        for tp in tps:
            self._forget(tp)

    def _on_lost(self, tps: Sequence[TopicPartition]) -> None:
        """Ownership gone. No commits, and the run is incomplete here (I-02)."""
        self._metrics.inc(REBALANCES, kind="lost")
        for tp in tps:
            self._release_open(tp)
            self._lost_partitions.append(tp)
        self._assignments.lose(list(tps))
        for tp in tps:
            self._forget(tp)

    def _release_open(self, tp: TopicPartition) -> None:
        held = self._batcher.discard(tp)
        if held:
            self._budget.release(tp, held)

    def _forget(self, tp: TopicPartition) -> None:
        self._batch_paused.discard(tp)
        self._budget.release_all(tp)
        self._commits.forget_partition(tp)
        self._runtime.pop(tp, None)

    def request_stop(self) -> None:
        """Ask the loop to drain and exit after the current iteration."""
        self._stopping = True

    # ------------------------------------------------------------------- step

    def step(self) -> StepReport:
        """One iteration of the owner loop."""
        report = StepReport()

        self._drain_results(report)
        self._commit_proven(report)

        if self._fatal is None:
            if not self._stopping:
                self._fetch_and_validate(report)
            # Sealing continues while stopping: that is how a graceful drain
            # flushes partial batches instead of hanging on them.
            self._seal_and_submit(report)

        self._apply_backpressure(report)
        self._publish_gauges()

        report.idle = not report.did_work
        return report

    def run(
        self, *, max_idle_steps: int | None = None, max_steps: int | None = None
    ) -> RunStatus:
        """Drive ``step()`` until stopped, fatal, or idle for long enough.

        ``max_idle_steps`` is for finite test runs. It is explicitly NOT an
        end-of-dataset detector: spec I-08 forbids concluding completeness from
        empty polls, so a caller using it must still reconcile against a
        captured Kafka cut before claiming the run was complete.
        """
        idle_streak = 0
        steps = 0
        while True:
            if max_steps is not None and steps >= max_steps:
                break
            report = self.step()
            steps += 1
            idle_streak = idle_streak + 1 if report.idle else 0
            if self._fatal is not None:
                break
            if max_idle_steps is not None and idle_streak >= max_idle_steps:
                break
            if self._stopping and self._is_quiesced():
                break
        return self.shutdown()

    def _is_quiesced(self) -> bool:
        return (
            self._executor.inflight == 0
            and not self._batcher.open_partitions
            and all(r.inflight_batch is None for r in self._runtime.values())
        )

    # --------------------------------------------------------------- fetching

    def _has_budget_headroom(self) -> bool:
        """True when one more maximum-size record can certainly be buffered."""
        return (
            self._budget.reserved_bytes + self._worst_case_row_bytes
            <= self._budget.budget_bytes
        ) and not self._budget.should_pause()

    def _fetch_and_validate(self, report: StepReport) -> None:
        """Poll a bounded number of records and buffer them as rows.

        Bounded per step so one iteration cannot starve the completion drain
        and the commit path, which is how a slow sink turns into a rebalance
        (spec NFR-08).
        """
        per_step = max(1, min(512, self._cfg.batch.per_partition_max_rows))
        for _ in range(per_step):
            if not self._has_budget_headroom():
                report.fetch_withheld_for_budget = True
                return
            try:
                record = self._source.poll(self._cfg.kafka.poll_timeout_ms / 1000.0)
            except RetentionLostError as exc:
                # Nothing safe to do: the data the group needs is gone from the
                # log and only a verified upstream replay can recover it.
                self._fatal = f"RETENTION_LOST: {exc}"
                return
            if record is None:
                return
            report.records_fetched += 1
            self._metrics.inc(
                KAFKA_RECORDS_FETCHED,
                topic=record.tp.topic,
                partition=str(record.tp.partition),
            )
            self._crash.maybe_crash(
                CRASH_01_AFTER_FETCH_BEFORE_DECODE,
                detail=f"{record.tp}@{record.offset}",
            )
            self._handle_record(record, report)
            if self._fatal is not None:
                return
            if self._batcher.seal_reason(record.tp) is not None:
                # The batch has just reached a threshold. Stop fetching so it
                # is sealed in this same iteration: continuing would push it
                # past the configured cap, and FR-06 requires every batch to
                # be bounded.
                return

    def _handle_record(self, record: ConsumedRecord, report: StepReport) -> None:
        tp = record.tp
        epoch = self._assignments.epoch_of(tp)
        if epoch is None:
            # A record for a partition we no longer own. Dropping it is the
            # only correct action: we must not buffer it and must not commit
            # it, and the new owner will read it from the log.
            report.stale_fenced += 1
            self._metrics.inc(STALE_COMPLETIONS_FENCED, kind="record")
            return

        if tp in self._blocked:
            # The partition is already stopped and paused, but records that
            # were in the client buffer can still arrive. They are dropped
            # without entering the ledger: the frontier is pinned behind the
            # blocking record, so these offsets are never committed and are
            # re-read after the operator resolves the block.
            runtime = self._runtime.get(tp)
            if runtime is not None:
                runtime.dropped_after_block += 1
            return

        self._commits.note_consumed(tp, record.offset)

        result = self._validator.validate(record)
        if not isinstance(result, ValidationOk):
            report.records_refused += 1
            self._metrics.inc(RECORDS_VALIDATED, kind="refused", result=result.code)
            self._refuse(record, result.code, result.detail)
            return

        self._crash.maybe_crash(
            CRASH_02_AFTER_DECODE_BEFORE_BATCH_SEAL, detail=f"{tp}@{record.offset}"
        )

        projection = result.projection
        self._metrics.inc(
            RECORDS_VALIDATED, kind=projection.kind.ch_name, result="accepted"
        )
        row = self._mapper.to_row(
            projection,
            topic=tp.topic,
            partition=tp.partition,
            offset=record.offset,
            raw_value_len=len(record.value or b""),
        )

        if not self._budget.try_reserve(tp, row.accounted_bytes):
            # Unreachable: the headroom check before polling guarantees room
            # for a worst-case row. If it ever fires, the bound is wrong and
            # continuing would mean an unbounded buffer, so stop loudly.
            raise InvariantViolation(
                f"I-05 VIOLATION {tp}@{record.offset}: budget refused "
                f"{row.accounted_bytes} bytes with "
                f"{self._budget.reserved_bytes}/{self._budget.budget_bytes} "
                f"reserved, although the pre-poll headroom check reserved space "
                f"for a worst-case {self._worst_case_row_bytes}-byte row"
            )

        self._batcher.append(tp, epoch, row)
        report.records_accepted += 1

    def _refuse(self, record: ConsumedRecord, code: str, detail: str) -> None:
        """Apply the poison policy to a record validation refused."""
        decision = self._quarantine.handle(
            record, reason_code=code, reason_detail=detail
        )
        self._commits.mark(record.tp, record.offset, decision.disposition)
        if decision.disposition is RecordDisposition.QUARANTINED:
            self._metrics.inc(QUARANTINED_RECORDS, reason=code)
            runtime = self._runtime.get(record.tp)
            if runtime is not None:
                runtime.quarantined += 1
        else:
            self._block(record.tp, decision.reason)

    # ---------------------------------------------------------------- sealing

    def _seal_and_submit(self, report: StepReport) -> None:
        for tp in sorted(self._batcher.open_partitions):
            if tp in self._blocked:
                continue
            runtime = self._runtime.get(tp)
            if runtime is None or runtime.inflight_batch is not None:
                # One in-flight batch per partition (ADR-003): the next batch
                # waits even when it is already over threshold.
                continue
            if self._executor.inflight >= self._cfg.batch.max_inflight_batches_global:
                return
            reason = (
                SealReason.SHUTDOWN if self._stopping else self._batcher.seal_reason(tp)
            )
            if reason is None:
                continue
            batch = self._batcher.seal(tp, reason)
            if batch is None:
                continue
            report.batches_sealed += 1
            self._metrics.inc(BATCHES_SEALED, topic=tp.topic, reason=str(reason))
            runtime.inflight_batch = batch
            self._set_partition_state(tp, PartitionState.INFLIGHT)
            self._executor.submit(batch)
            report.batches_submitted += 1

    # ---------------------------------------------------------------- results

    def _drain_results(self, report: StepReport) -> None:
        for result in self._executor.poll_results():
            report.results_drained += 1
            self._apply_result(result, report)

    def _apply_result(self, result: SinkResult, report: StepReport) -> None:
        tp = result.tp

        if not self._assignments.is_current(tp, result.assignment_epoch):
            # Stale completion. The write may well have landed in ClickHouse --
            # it cannot be retracted -- but this consumer has no right to turn
            # it into committed progress (spec section 4.3).
            report.stale_fenced += 1
            self._metrics.inc(STALE_COMPLETIONS_FENCED, kind="batch")
            return

        runtime = self._runtime.get(tp)
        if runtime is None or runtime.inflight_batch is None:
            self._metrics.inc(STALE_COMPLETIONS_FENCED, kind="orphan")
            return
        batch = runtime.inflight_batch
        if batch.batch_id != result.batch_id:
            # A duplicate or mismatched callback. Ignoring it is correct;
            # acting on it would retire the wrong batch.
            self._metrics.inc(STALE_COMPLETIONS_FENCED, kind="batch_id_mismatch")
            return

        runtime.inflight_batch = None
        self._budget.release(tp, batch.accounted_bytes)

        if result.outcome is SinkOutcome.ACKED:
            self._breaker.record_success()
            self._commits.mark_batch(tp, batch.offsets, RecordDisposition.SINK_ACKED)
            self._set_partition_state(tp, PartitionState.ASSIGNED)
            self._crash.maybe_crash(
                CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT,
                detail=f"{tp} batch={batch.batch_id} rows={len(batch)}",
            )
            return

        self._breaker.record_failure(now=self._monotonic())
        # Every non-ACK path ends here. The batch has no terminal durable
        # result, so its records stay unfinished, the frontier cannot pass them
        # and the partition stops instead of silently skipping (I-03, I-07).
        self._commits.mark_batch(tp, batch.offsets, RecordDisposition.BLOCKED)
        self._block(
            tp,
            f"batch {batch.batch_id} ended {result.outcome} after "
            f"{result.attempts} attempt(s): {result.detail}",
        )

    # ---------------------------------------------------------------- commits

    def _commit_proven(self, report: StepReport) -> None:
        for tp in sorted(self._commits.tracked()):
            self._try_commit(tp, report)

    def _try_commit(self, tp: TopicPartition, report: StepReport | None = None) -> None:
        epoch = self._assignments.epoch_of(tp)
        if epoch is None:
            return
        if not self._assignments.may_commit(tp, epoch):
            return
        self._commits.advance_frontier(tp, epoch=epoch)
        candidate = self._commits.commit_candidate(tp)
        if candidate is None:
            return
        # Hard gate: raises rather than returning, because a commit ahead of
        # the proven frontier is unrecoverable (I-01).
        self._commits.assert_commit_safe(tp, candidate)
        self._metrics.inc(KAFKA_COMMIT_ATTEMPTS, status="attempt")
        try:
            self._source.commit_offset(tp, candidate)
        except CommitNotConfirmedError:
            # Unknown, not failed. committed_next is left untouched so the next
            # iteration retries and a restart re-reads the broker position.
            self._commits.note_commit_unknown(tp)
            self._metrics.inc(KAFKA_COMMIT_ATTEMPTS, status="unknown")
            if report is not None:
                report.commits_unknown += 1
            return
        self._commits.note_committed(tp, candidate)
        self._metrics.inc(KAFKA_COMMIT_ATTEMPTS, status="confirmed")
        self._metrics.set_gauge(
            COMMITTED_NEXT_OFFSET,
            candidate,
            topic=tp.topic,
            partition=str(tp.partition),
        )
        if report is not None:
            report.commits_confirmed += 1
        self._crash.maybe_crash(
            CRASH_05_AFTER_COMMIT_RESPONSE_BEFORE_REPORT_FSYNC,
            detail=f"{tp} committed_next={candidate}",
        )

    # ----------------------------------------------------------- backpressure

    def _apply_batch_pressure(self) -> None:
        """Pause a partition whose full batch cannot be submitted yet.

        With one in-flight batch per partition (ADR-003), a partition whose
        batch is already at its cap while the previous one is still in flight
        has nowhere to put new records. Returning early from the fetch loop is
        not enough on its own -- ``poll()`` would hand over more records for
        that partition on the next iteration. Pausing it at the consumer is
        what actually keeps the open batch bounded, and it is also the honest
        signal: this partition is waiting on the sink.
        """
        for tp in sorted(self._assignments.owned_for_fetch):
            runtime = self._runtime.get(tp)
            if runtime is None:
                continue
            blocked_on_sink = runtime.inflight_batch is not None
            batch_full = self._batcher.seal_reason(tp) is not None
            if batch_full and blocked_on_sink:
                if tp not in self._batch_paused:
                    self._batch_paused.add(tp)
                    self._source.pause([tp])
            elif tp in self._batch_paused and not blocked_on_sink:
                self._batch_paused.discard(tp)
                if tp not in self._budget.paused and tp not in self._blocked:
                    self._source.resume([tp])

    def _apply_backpressure(self, report: StepReport) -> None:
        self._apply_batch_pressure()
        assigned = self._assignments.owned_for_fetch
        to_pause = self._budget.partitions_to_pause(assigned)
        if to_pause:
            self._source.pause(to_pause)
            for tp in to_pause:
                self._set_partition_state(tp, PartitionState.PAUSED)
            report.paused = tuple(to_pause)
        to_resume = [
            tp
            for tp in self._budget.partitions_to_resume()
            if tp in assigned and tp not in self._batch_paused
        ]
        if to_resume:
            self._source.resume(to_resume)
            for tp in to_resume:
                if tp not in self._blocked:
                    self._set_partition_state(tp, PartitionState.ASSIGNED)
            report.resumed = tuple(to_resume)

    # --------------------------------------------------------------- blocking

    def _block(self, tp: TopicPartition, reason: str) -> None:
        if tp in self._blocked:
            return
        self._blocked[tp] = reason
        # Release the open batch. Those rows were buffered but never sent, so
        # no sink request exists for them and a restart will re-read them from
        # the log. Keeping them would be worse than useless: a blocked
        # partition is skipped by the sealer, so the batch could never be
        # submitted and the engine would never reach a quiescent state.
        self._release_open(tp)
        self._batch_paused.discard(tp)
        self._commits.block(tp, reason)
        self._assignments.block(tp)
        self._set_partition_state(tp, PartitionState.BLOCKED)
        try:
            self._source.pause([tp])
        except Exception:  # pragma: no cover - pausing a lost TP may fail
            pass

    def _set_partition_state(self, tp: TopicPartition, state: PartitionState) -> None:
        self._assignments.set_state(tp, state)
        self._metrics.set_gauge(
            PARTITION_STATE,
            1.0,
            topic=tp.topic,
            partition=str(tp.partition),
            state=str(state),
        )

    # ----------------------------------------------------------------- gauges

    def _publish_gauges(self) -> None:
        self._metrics.set_gauge(
            BUFFER_BYTES, self._budget.reserved_bytes, component="app"
        )
        self._metrics.set_gauge(INFLIGHT_BATCHES, float(self._executor.inflight))
        for tp in self._commits.tracked():
            self._metrics.set_gauge(
                PROVEN_FRONTIER_NEXT_OFFSET,
                self._commits.proven_frontier_next(tp),
                topic=tp.topic,
                partition=str(tp.partition),
            )
            try:
                _low, high = self._source.watermarks(tp)
            except Exception:  # pragma: no cover - watermarks are best-effort
                continue
            self._metrics.set_gauge(
                KAFKA_LAG_OFFSETS,
                max(0, high - self._commits.committed_next(tp)),
                topic=tp.topic,
                partition=str(tp.partition),
            )

    # --------------------------------------------------------------- shutdown

    def shutdown(self) -> RunStatus:
        """Drain within the deadline, commit only proven prefixes, then report.

        The verdict is pessimistic by construction: anything blocked, lost, or
        still unfinished when the deadline expires makes the run INCOMPLETE.
        """
        self._stopping = True
        deadline = self._monotonic() + self._cfg.shutdown.drain_timeout_s

        while self._monotonic() < deadline:
            report = self.step()
            if self._is_quiesced() and not report.did_work:
                break
            if not report.did_work:
                break

        self._executor.shutdown(max(0.0, deadline - self._monotonic()))
        try:
            self._source.close()
        except Exception:  # pragma: no cover
            pass

        return self._verdict()

    def _verdict(self) -> RunStatus:
        reasons: list[str] = []
        if self._fatal:
            reasons.append(self._fatal)
        if self._blocked:
            reasons.append(
                "blocked partitions: "
                + ", ".join(f"{tp} ({why})" for tp, why in sorted(self._blocked.items()))
            )
        if self._lost_partitions:
            reasons.append(
                "lost ownership without commit: "
                + ", ".join(str(tp) for tp in sorted(set(self._lost_partitions)))
            )
        unfinished = [
            f"{tp}@{self._commits.first_pending_offset(tp)}"
            for tp in sorted(self._commits.tracked())
            if self._commits.pending_count(tp) > 0
        ]
        if unfinished:
            reasons.append(
                "frontier held by records that never reached a committable "
                "state, oldest first: " + ", ".join(unfinished)
            )

        if reasons:
            return RunStatus(
                complete=False,
                reason="; ".join(reasons),
                blocked_partitions=[str(tp) for tp in sorted(self._blocked)],
            )
        return RunStatus(
            complete=True,
            reason=(
                "every consumed record reached a terminal durable result and all "
                "commits were confirmed; completeness against the source still "
                "requires independent reconciliation (I-08)"
            ),
        )
