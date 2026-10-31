"""P05: batch ingestion from Kafka into the raw table, with an honest frontier.

The one invariant this module exists to get right is ``INV-03``::

    committed_next_offset(tp) <= proven_completed_contiguous_next_offset(tp)

A commit says "everything below this offset is durably in the sink". Committing
past an unfinished record is how data goes missing while every dashboard stays
green, so :class:`PartitionProgress` tracks *completed* offsets as a set and
computes the contiguous frontier from it. An offset whose insert is in flight,
failed or ambiguous is simply not in that set, so the frontier cannot advance
past it -- case ``E2E-17``, the delayed middle offset of a batch, is a direct
test of this.

Three more rules, all from design 5.2 and 5.3:

* **At-least-once, not exactly-once.** A crash after the INSERT commits but
  before the Kafka commit replays the batch, and the raw table then holds
  physical duplicates. That is correct behaviour, not a bug, and the canonical
  build is where it gets reduced to one logical row (``E2E-15``).
* **An ambiguous sink ACK resolves nothing.** :class:`SinkAckUnknown` leaves the
  batch's offsets uncompleted and records them as unknown. The retry may
  duplicate; the commit still waits.
* **No implicit offset reset.** :meth:`BatchIngestor.resume_from_broker` reads
  the committed offset and, if the group never committed, refuses to guess. It
  does not fall back to the end of the log, because that silently skips
  everything in between (``INV-08``).

On restart the ingestor keeps no local memory: :meth:`resume_from_broker` is the
only way to seed position, so a test that kills and restarts it really does
exercise the recovery path rather than an in-process shortcut.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from ..contracts import SemanticCore, SourceIdentity
from ..oracle.semantic_digest import digest_core
from .clickhouse_fake import DiskFull, FakeClickHouse, RawRow, SinkAckUnknown, SinkError, SinkUnavailable
from .codec import Codec, CodecError, DEFAULT_CODEC, looks_like_mdp1
from .kafka_fake import FakeKafka, KafkaError, OffsetExpired

#: Design 8: in-flight batches are capped at one per topic-partition in the MVP,
#: so an ambiguous insert blocks only its own partition's frontier.
MAX_IN_FLIGHT_PER_TP = 1


class PoisonRecord(Exception):
    """A record that cannot be decoded in a strict topic.

    Raised rather than skipped. Case ``E2E-18``: a poisoned record in a strict
    topic means STOP/NO-GO unless a verified dead-letter policy exists, because
    skipping it is an untracked drop.
    """

    def __init__(self, topic: str, partition: int, offset: int, reason: str) -> None:
        super().__init__(f"{topic}/{partition}@{offset}: {reason}")
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.reason = reason


@dataclass(slots=True)
class PartitionProgress:
    """Per topic-partition position, with completion tracked exactly.

    ``completed`` holds offsets whose rows are durably in the sink. It is a set,
    not a high-water mark, because completion genuinely arrives out of order
    once retries are involved -- and collapsing it to a maximum is precisely the
    bug ``INV-03`` guards.
    """

    start_offset: int
    next_to_fetch: int
    completed: set[int] = field(default_factory=set)
    unknown: set[int] = field(default_factory=set)
    committed_next: int | None = None

    def completed_contiguous_next(self) -> int:
        """Lowest offset not yet proven complete, counted from ``start_offset``.

        The cursor starts at ``start_offset`` and **not** at ``committed_next``.
        Seeding it from the commit would make this number depend on the thing it
        exists to check: a worker that committed past a hole would then report a
        frontier at its own commit, and ``INV-03`` could never fire. A mutation
        test (``tests/model/test_mutations.py``) pins that distinction.

        ``start_offset`` is itself the broker's committed offset at resume time,
        so a legitimately resumed worker still counts from the right place.
        """
        cursor = self.start_offset
        while cursor in self.completed:
            cursor += 1
        return cursor

    def as_dict(self) -> dict[str, Any]:
        return {
            "start_offset": self.start_offset,
            "next_to_fetch": self.next_to_fetch,
            "committed_next": self.committed_next,
            "completed_contiguous_next": self.completed_contiguous_next(),
            "completed_count": len(self.completed),
            "unknown_count": len(self.unknown),
            "unknown_offsets": sorted(self.unknown)[:20],
        }


@dataclass(slots=True)
class IngestCounters:
    polls: int = 0
    records_decoded: int = 0
    rows_inserted: int = 0
    insert_calls: int = 0
    insert_unknown: int = 0
    insert_failed: int = 0
    commits: int = 0
    commits_refused: int = 0
    duplicate_rows_written: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "polls": self.polls,
            "records_decoded": self.records_decoded,
            "rows_inserted": self.rows_inserted,
            "insert_calls": self.insert_calls,
            "insert_unknown": self.insert_unknown,
            "insert_failed": self.insert_failed,
            "commits": self.commits,
            "commits_refused": self.commits_refused,
            "duplicate_rows_written": self.duplicate_rows_written,
        }


class BatchIngestor:
    """One P05 worker: polls assigned partitions, inserts batches, commits.

    ``crash_after`` is the test hook. It is a callable taking a stage name and
    context; raising from it simulates the process dying at that exact point.
    The stages are the boundaries from design 5.3: ``before_insert``,
    ``after_insert_before_commit`` and ``after_commit``. A ``sleep`` would not
    do -- design 6.3 is explicit that a sleep is not a boundary proof.
    """

    def __init__(
        self,
        kafka: FakeKafka,
        sink: FakeClickHouse,
        *,
        group: str = "p05-ingest",
        assignment: Sequence[tuple[str, int]] = (),
        epoch: int | None = None,
        codec: Codec = DEFAULT_CODEC,
        batch_max_records: int = 500,
        max_insert_retries: int = 3,
        strict_poison: bool = True,
        worker_id: str = "p05-worker-1",
        crash_after: Any = None,
    ) -> None:
        self._kafka = kafka
        self._sink = sink
        self._group = group
        self._codec = codec
        self._batch_max_records = batch_max_records
        self._max_insert_retries = max_insert_retries
        self._strict_poison = strict_poison
        self._worker_id = worker_id
        self._crash_after = crash_after
        self._batch_ids = itertools.count(1)
        self._clock_ns = itertools.count(1)
        self.epoch = epoch if epoch is not None else kafka.group_epoch(group)
        self.assignment: list[tuple[str, int]] = list(assignment)
        self.progress: dict[tuple[str, int], PartitionProgress] = {}
        self.counters = IngestCounters()
        self._identities_written: set[SourceIdentity] = set()

    # -- position ---------------------------------------------------------

    def resume_from_broker(self, *, cut_start: Mapping[tuple[str, int], int] | None = None) -> None:
        """Seed each partition's position from the broker's committed offset.

        When the group has never committed, the start offset must come from an
        explicit cut (``cut_start``). With neither, this raises: guessing is the
        ``auto.offset.reset`` failure the lab config forbids outright.
        """
        for tp in self.assignment:
            topic, partition = tp
            committed = self._kafka.committed(self._group, topic, partition)
            if committed is None:
                if cut_start is None or tp not in cut_start:
                    raise KafkaError(
                        "OFFSET_RESET_FORBIDDEN",
                        f"{topic}/{partition} has no committed offset and no explicit cut start",
                    )
                start = cut_start[tp]
                self.progress[tp] = PartitionProgress(start_offset=start, next_to_fetch=start)
            else:
                self.progress[tp] = PartitionProgress(
                    start_offset=committed, next_to_fetch=committed, committed_next=committed
                )

    def frontier(self) -> dict[str, Any]:
        """The per-partition frontier report for the evidence bundle."""
        return {
            "group": self._group,
            "worker_id": self._worker_id,
            "epoch": self.epoch,
            "partitions": {
                f"{topic}/{partition}": progress.as_dict()
                for (topic, partition), progress in sorted(self.progress.items())
            },
        }

    # -- the loop ---------------------------------------------------------

    def _checkpoint(self, stage: str, **context: Any) -> None:
        if self._crash_after is not None:
            self._crash_after(stage, **context)

    def _to_raw(
        self, core: SemanticCore, *, topic: str, partition: int, offset: int, batch_id: str
    ) -> RawRow:
        return RawRow(
            dataset_id=core.dataset_id,
            symbol=core.symbol,
            sequence=core.sequence,
            event_time_ns=core.event_time_ns,
            price_scale=core.price_scale,
            event_kind=core.event_kind.wire_name,
            source_schema_version=core.source_schema_version,
            bid_ticks=core.bid_ticks,
            ask_ticks=core.ask_ticks,
            price_ticks=core.price_ticks,
            payload_digest=digest_core(core),
            topic_generation=self._kafka.topic_generation,
            topic=topic,
            partition=partition,
            offset=offset,
            batch_id=batch_id,
            ingested_at_ns=next(self._clock_ns),
        )

    def poll_once(self, *, up_to: Mapping[tuple[str, int], int] | None = None) -> int:
        """One pass over the assignment. Returns how many records were handled.

        ``up_to`` is the sealed cut: an exclusive end offset per partition, so a
        drain stops exactly at the cut rather than at "whatever has arrived".
        """
        handled = 0
        self.counters.polls += 1
        for tp in list(self.assignment):
            topic, partition = tp
            progress = self.progress.get(tp)
            if progress is None:
                raise KafkaError("ASSIGNMENT_NOT_SEEDED", f"{topic}/{partition}")
            limit = self._batch_max_records
            if up_to is not None:
                remaining = up_to.get(tp, progress.next_to_fetch) - progress.next_to_fetch
                if remaining <= 0:
                    continue
                limit = min(limit, remaining)
            try:
                records = self._kafka.fetch(topic, partition, progress.next_to_fetch, limit)
            except OffsetExpired:
                # The offset the consumer needs has been deleted by retention.
                # Re-raised so the caller decides; resetting here would be the
                # silent skip ``INV-08`` forbids.
                raise
            if not records:
                continue

            batch_id = f"{self._worker_id}-b{next(self._batch_ids)}"
            rows: list[RawRow] = []
            offsets: list[int] = []
            for record in records:
                if looks_like_mdp1(record.value):
                    # P04 must publish raw Protobuf. An MDP1 prefix in a Kafka
                    # value is a contract violation, not a parsing nuisance.
                    raise PoisonRecord(topic, partition, record.offset, "KAFKA_VALUE_HAS_MDP1_PREFIX")
                try:
                    core = self._codec.decode(record.value)
                except CodecError as exc:
                    if self._strict_poison:
                        raise PoisonRecord(topic, partition, record.offset, exc.reason) from exc
                    continue
                self.counters.records_decoded += 1
                rows.append(
                    self._to_raw(
                        core, topic=topic, partition=partition, offset=record.offset, batch_id=batch_id
                    )
                )
                offsets.append(record.offset)

            progress.next_to_fetch = records[-1].offset + 1
            handled += len(records)
            if rows:
                self._insert_batch(tp, progress, rows, offsets, batch_id)
            self._commit_frontier(tp, progress)
        return handled

    def _insert_batch(
        self,
        tp: tuple[str, int],
        progress: PartitionProgress,
        rows: list[RawRow],
        offsets: list[int],
        batch_id: str,
    ) -> None:
        """Insert one batch, then mark its offsets complete -- in that order."""
        self._checkpoint("before_insert", tp=tp, batch_id=batch_id, offsets=offsets)
        attempts = 0
        while attempts < self._max_insert_retries:
            attempts += 1
            self.counters.insert_calls += 1
            try:
                written = self._sink.insert_raw(rows)
            except SinkAckUnknown:
                # Committed or not -- unknowable from here. The offsets stay out
                # of ``completed``, so the frontier cannot advance, and a retry
                # is allowed to produce physical duplicates.
                self.counters.insert_unknown += 1
                progress.unknown.update(offsets)
                continue
            except (DiskFull, SinkUnavailable) as exc:
                self.counters.insert_failed += 1
                progress.unknown.discard(min(offsets, default=0))
                raise exc
            except SinkError:
                self.counters.insert_failed += 1
                raise
            duplicates = sum(1 for row in rows if row.identity in self._identities_written)
            self.counters.duplicate_rows_written += duplicates
            self._identities_written.update(row.identity for row in rows)
            self.counters.rows_inserted += written
            progress.completed.update(offsets)
            progress.unknown.difference_update(offsets)
            self._checkpoint("after_insert_before_commit", tp=tp, batch_id=batch_id, offsets=offsets)
            return
        # Retry budget exhausted with the outcome still ambiguous. NFR-10: stop,
        # hold the state and let the reconciler resolve it. Do not keep retrying.
        raise SinkAckUnknown(0)

    def _commit_frontier(self, tp: tuple[str, int], progress: PartitionProgress) -> None:
        topic, partition = tp
        target = progress.completed_contiguous_next()
        if progress.committed_next is not None and target <= progress.committed_next:
            return
        try:
            self._kafka.commit(self._group, topic, partition, target, epoch=self.epoch)
        except KafkaError as exc:
            if exc.reason == "COMMIT_BY_STALE_OWNER":
                # Fenced. The worker lost ownership; its side effects on the
                # sink may already exist and are the raw audit's problem, but it
                # has no authority to move the offset (``INV-04``).
                self.counters.commits_refused += 1
                return
            raise
        progress.committed_next = target
        self.counters.commits += 1
        self._checkpoint("after_commit", tp=tp, committed_next=target)

    def drain_to_cut(
        self, cut: Mapping[tuple[str, int], int], *, max_passes: int = 10_000
    ) -> dict[str, Any]:
        """Poll until every partition reaches its cut end offset, or stall.

        Returns a report including whether the drain actually finished. A stall
        with unknown offsets outstanding is ``INCONCLUSIVE`` territory, and the
        caller is expected to treat it that way rather than loop forever.
        """
        passes = 0
        while passes < max_passes:
            passes += 1
            if all(
                self.progress[tp].next_to_fetch >= end for tp, end in cut.items() if tp in self.progress
            ):
                break
            if self.poll_once(up_to=cut) == 0:
                break
        complete = all(
            self.progress[tp].completed_contiguous_next() >= end
            for tp, end in cut.items()
            if tp in self.progress
        )
        return {
            "passes": passes,
            "frontier_reached_cut": complete,
            "unknown_offsets_outstanding": sum(
                len(progress.unknown) for progress in self.progress.values()
            ),
            "frontier": self.frontier(),
        }

    def counters_report(self) -> dict[str, Any]:
        return {"counters": self.counters.as_dict(), "frontier": self.frontier()}


def rebalance(
    kafka: FakeKafka,
    *,
    group: str,
    workers: Sequence[BatchIngestor],
    assignments: Sequence[Sequence[tuple[str, int]]],
) -> int:
    """Bump the group epoch and hand out new assignments. Returns the new epoch.

    Workers not listed keep their old epoch and are therefore fenced: their
    commits will be refused while their in-flight sink writes may still land.
    That asymmetry is the whole point of case ``E2E-16`` -- fencing stops the
    commit, not the side effect.
    """
    epoch = kafka.bump_group_epoch(group)
    for worker, assignment in zip(workers, assignments, strict=True):
        worker.epoch = epoch
        worker.assignment = list(assignment)
        worker.progress = {
            tp: progress for tp, progress in worker.progress.items() if tp in set(assignment)
        }
    return epoch


def identities_in_raw(rows: Iterable[RawRow]) -> dict[SourceIdentity, int]:
    """Physical row count per identity. More than one means a physical duplicate."""
    counts: dict[SourceIdentity, int] = {}
    for row in rows:
        counts[row.identity] = counts.get(row.identity, 0) + 1
    return counts
