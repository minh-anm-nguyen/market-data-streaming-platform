"""A deterministic simulated pipeline, for testing the measuring tools (section 8).

Section 8 sets aside a whole fault class for *metrology failures* and requires
the instruments to be checked against golden inputs -- known latencies, known
offsets, known provenance -- before they are pointed at a real system. Section 9.1
lists "statistical validation" as its own layer of the pyramid for the same
reason. This module is that known input: a discrete-event simulation of
``source -> adapter -> producer -> broker -> consumer -> raw -> canonical`` whose
service rates, queue bounds, batch sizes and fault behaviour are configuration.

What it buys:

* An end-to-end lane that **runs** today, so the orchestrator, the analyzer and
  the verdict logic are exercised rather than merely written.
* Exact expected answers. With a 100k/s sink and a 120k/s offer, the backlog
  must grow by 20k/s -- FIX-003's arithmetic -- so the analyzer can be checked
  against a number derived by hand instead of against itself.
* Reproducible faults. Dropping an event, duplicating one, or stalling a
  partition happens on a seeded schedule, which is what makes case E2E-26-style
  tests assertable.

What it is not: evidence about Kafka or ClickHouse. Every stage reports
:attr:`~p08_bench.sut.Availability.SIMULATED`, which makes
:attr:`is_simulation` true, which makes the verdict refuse to read any of it as
a capacity claim. The simulation is a ruler being checked against a known
length, not a measurement of the building.
"""

from __future__ import annotations

import random
import zlib
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Mapping

from .errors import ConfigError
from .metrics import StageCounters
from .sut import (
    Availability,
    Backlog,
    Identity,
    Observation,
    Stage,
    StageStatus,
    SubmitOutcome,
    SubmitStatus,
    SutBase,
)

NS_PER_S = 1_000_000_000


@dataclass
class MockConfig:
    """Everything the simulation does, stated up front.

    The defaults describe a pipeline that keeps up and loses nothing, so a test
    that wants a pathology has to ask for it by name. A default that quietly
    dropped events would make every other test ambiguous.
    """

    #: Events per second each stage can retire. ``None`` means "not the
    #: bottleneck": the stage passes work through without queueing.
    adapter_rate_per_s: float | None = None
    producer_rate_per_s: float | None = None
    broker_rate_per_s: float | None = None
    consumer_rate_per_s: float | None = None
    canonical_rate_per_s: float | None = None

    #: Bounded queue depths, in records. A full queue backpressures; it never
    #: grows without limit (INV-10, section 8).
    adapter_queue_records: int = 1_000_000
    broker_queue_records: int = 10_000_000
    consumer_queue_records: int = 1_000_000

    #: Topics and partitioning, mirroring the P04 contract.
    tick_topic: str = "md.ticks.v1"
    quote_topic: str = "md.quotes.v1"
    partitions_per_topic: int = 8

    #: Fraction of accepted events the adapter rejects as business-inadmissible.
    #: A reject is not loss (section 0.1), and the simulation counts it as such.
    reject_fraction: float = 0.0

    #: Physical duplicates, as a fraction of events. At-least-once delivery
    #: makes these expected; they must not change the canonical unique set
    #: (INV-06).
    duplicate_fraction: float = 0.0

    #: Events silently lost between the broker and the raw table. Only a fault
    #: injection should ever set this: it is the thing reconciliation exists to
    #: catch, so it defaults off.
    loss_fraction: float = 0.0

    #: Per-stage service latency, in milliseconds, added to each event.
    stage_latency_ms: float = 0.0

    #: Identities the canonical build publishes without the source ever sending
    #: them (INV-05 violation), for testing that the oracle notices.
    inject_extra_identities: tuple[tuple[str, str, int], ...] = ()

    #: Identities whose canonical payload disagrees with the source.
    conflict_identities: tuple[tuple[str, str, int], ...] = ()

    #: Partitions whose consumer stalls, for hot-partition and lag tests.
    stalled_partitions: tuple[tuple[str, int], ...] = ()

    #: Partitions where retention deleted the backlog rather than it being
    #: processed (FIX-003, case E2E-30).
    retention_truncated_partitions: tuple[tuple[str, int], ...] = ()

    seed: int = 20261030

    def __post_init__(self) -> None:
        for name in (
            "reject_fraction", "duplicate_fraction", "loss_fraction",
        ):
            value = getattr(self, name)
            if not (0.0 <= value <= 1.0):
                raise ConfigError(f"{name} must be a fraction in [0, 1]", value=value)
        for name in (
            "adapter_rate_per_s", "producer_rate_per_s", "broker_rate_per_s",
            "consumer_rate_per_s", "canonical_rate_per_s",
        ):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ConfigError(f"{name} must be positive when set", value=value)
        if self.partitions_per_topic <= 0:
            raise ConfigError("partitions_per_topic must be positive")

    def as_dict(self) -> dict[str, Any]:
        return {
            "adapter_rate_per_s": self.adapter_rate_per_s,
            "producer_rate_per_s": self.producer_rate_per_s,
            "broker_rate_per_s": self.broker_rate_per_s,
            "consumer_rate_per_s": self.consumer_rate_per_s,
            "canonical_rate_per_s": self.canonical_rate_per_s,
            "adapter_queue_records": self.adapter_queue_records,
            "broker_queue_records": self.broker_queue_records,
            "consumer_queue_records": self.consumer_queue_records,
            "partitions_per_topic": self.partitions_per_topic,
            "reject_fraction": self.reject_fraction,
            "duplicate_fraction": self.duplicate_fraction,
            "loss_fraction": self.loss_fraction,
            "stage_latency_ms": self.stage_latency_ms,
            "stalled_partitions": [f"{t}-{p}" for t, p in self.stalled_partitions],
            "seed": self.seed,
        }


@dataclass
class _Record:
    """One physical record moving through the simulation."""

    identity: Identity
    payload: dict[str, Any]
    topic: str
    partition: int
    submitted_ns: int
    is_duplicate: bool = False


class _ServiceStage:
    """A bounded FIFO queue drained at a fixed rate.

    Service is modelled as a token budget rather than per-event timers: at rate
    ``r``, the number of records retired by time ``t`` is ``floor(r * t)`` minus
    what has already been retired. That keeps the arithmetic exact for the
    FIX-003 comparison and keeps the cost O(records processed) rather than
    O(simulated time).
    """

    __slots__ = ("name", "rate_per_s", "capacity", "_queue", "_credit_ns", "overflow_count", "high_water")

    def __init__(self, name: str, rate_per_s: float | None, capacity: int) -> None:
        self.name = name
        self.rate_per_s = rate_per_s
        self.capacity = capacity
        self._queue: deque[_Record] = deque()
        self._credit_ns = 0
        self.overflow_count = 0
        self.high_water = 0

    @property
    def depth(self) -> int:
        return len(self._queue)

    @property
    def unbounded_passthrough(self) -> bool:
        return self.rate_per_s is None

    def offer(self, record: _Record) -> bool:
        """Enqueue unless full. ``False`` means backpressure, not loss."""
        if self.unbounded_passthrough:
            return True
        if len(self._queue) >= self.capacity:
            self.overflow_count += 1
            return False
        self._queue.append(record)
        self.high_water = max(self.high_water, len(self._queue))
        return True

    def service(self, elapsed_ns: int) -> list[_Record]:
        """Retire whatever the rate allows over ``elapsed_ns``."""
        if self.unbounded_passthrough or elapsed_ns <= 0:
            return []
        self._credit_ns += elapsed_ns
        per_record_ns = NS_PER_S / self.rate_per_s
        count = int(self._credit_ns / per_record_ns)
        if count <= 0:
            return []
        count = min(count, len(self._queue))
        self._credit_ns -= int(count * per_record_ns)
        return [self._queue.popleft() for _ in range(count)]

    def as_dict(self) -> dict[str, Any]:
        return {
            "stage": self.name,
            "rate_per_s": self.rate_per_s,
            "capacity_records": self.capacity if not self.unbounded_passthrough else None,
            "depth_records": self.depth,
            "high_water_records": self.high_water,
            "backpressure_events": self.overflow_count,
        }


class MockPipeline(SutBase):
    """Simulated P01-to-P06 pipeline. Every stage reports ``SIMULATED``."""

    def __init__(self, config: MockConfig | None = None) -> None:
        self.config = config or MockConfig()
        self._rng = random.Random(self.config.seed)
        self._now_ns = 0
        self.STAGES = {
            stage: StageStatus(
                stage,
                Availability.SIMULATED,
                "deterministic discrete-event simulation; not evidence about the "
                "real component",
            )
            for stage in (
                Stage.SOURCE, Stage.ADAPTER, Stage.PRODUCER, Stage.BROKER,
                Stage.CONSUMER, Stage.RAW, Stage.CANONICAL, Stage.QUERY,
            )
        }

        self._adapter = _ServiceStage(
            "adapter", self.config.adapter_rate_per_s, self.config.adapter_queue_records
        )
        self._broker = _ServiceStage(
            "broker", self.config.broker_rate_per_s, self.config.broker_queue_records
        )
        self._consumer = _ServiceStage(
            "consumer", self.config.consumer_rate_per_s, self.config.consumer_queue_records
        )

        # Counters, each with the scope section 4.1 gives it.
        self.offered = 0
        self.issued = 0
        self.accepted = 0
        self.rejected = 0
        self.producer_acked = 0
        self.kafka_physical = 0
        self.raw_physical = 0
        self.lost = 0
        self.dropped_backpressure = 0

        self._source_identities: set[tuple[str, str, int]] = set()
        self._accepted_identities: set[tuple[str, str, int]] = set()
        self._canonical: dict[tuple[str, str, int], dict[str, Any]] = {}
        self._source_payloads: dict[tuple[str, str, int], dict[str, Any]] = {}
        self._backlog_per_partition: dict[tuple[str, int], int] = {}
        self._stalled = set(self.config.stalled_partitions)
        self._accepting = True

        for key in self.config.inject_extra_identities:
            # Published without ever being sent: the INV-05 violation the oracle
            # must notice.
            self._canonical[key] = {"injected": True}

    # -- topology ---------------------------------------------------------
    def _route(self, identity: Identity, kind: str) -> tuple[str, int]:
        """Key by symbol, exactly like P04, so per-symbol order is preserved.

        ``crc32`` rather than ``hash``: str hashing is salted per interpreter
        run, which would move a symbol between partitions between runs and
        quietly break the reproducibility the whole simulation exists to provide.
        """
        topic = self.config.quote_topic if kind == "quote" else self.config.tick_topic
        partition = zlib.crc32(identity.symbol.encode("utf-8")) % self.config.partitions_per_topic
        return topic, partition

    # -- the SystemUnderTest surface --------------------------------------
    def submit(self, identity: Identity, payload: Mapping[str, Any], t_ns: int) -> SubmitOutcome:
        self.advance_to(t_ns)
        self.offered += 1
        if not self._accepting:
            return SubmitOutcome(SubmitStatus.DROPPED, identity, "pipeline is draining")

        key = identity.as_tuple()
        self._source_identities.add(key)
        self._source_payloads.setdefault(key, dict(payload))
        self.issued += 1

        if self.config.reject_fraction and self._rng.random() < self.config.reject_fraction:
            # A business rejection per P03 policy. Not loss, and not an error.
            self.rejected += 1
            return SubmitOutcome(SubmitStatus.REJECTED, identity, "validation policy")

        topic, partition = self._route(identity, str(payload.get("kind", "tick")))
        record = _Record(identity, dict(payload), topic, partition, t_ns)
        if not self._adapter.offer(record):
            self.dropped_backpressure += 1
            return SubmitOutcome(SubmitStatus.BACKPRESSURED, identity, "adapter queue full")

        self.accepted += 1
        self._accepted_identities.add(key)
        if self._adapter.unbounded_passthrough:
            self._to_broker([record])
        return SubmitOutcome(SubmitStatus.ACCEPTED, identity)

    def advance_to(self, t_ns: int) -> None:
        if t_ns < self._now_ns:
            return
        elapsed = t_ns - self._now_ns
        self._now_ns = t_ns
        self._to_broker(self._adapter.service(elapsed))
        self._to_consumer(self._broker.service(elapsed))
        self._to_raw(self._consumer.service(elapsed))
        self._refresh_backlog()

    def _to_broker(self, records: list[_Record]) -> None:
        for record in records:
            self.producer_acked += 1
            copies = [record]
            if self.config.duplicate_fraction and self._rng.random() < self.config.duplicate_fraction:
                # At-least-once: a physical retry appends a second copy.
                copies.append(
                    _Record(
                        record.identity, dict(record.payload), record.topic,
                        record.partition, record.submitted_ns, is_duplicate=True,
                    )
                )
            for copy in copies:
                self.kafka_physical += 1
                if self._broker.unbounded_passthrough:
                    self._to_consumer([copy])
                else:
                    self._broker.offer(copy)

    def _to_consumer(self, records: list[_Record]) -> None:
        for record in records:
            if (record.topic, record.partition) in self._stalled:
                # A stalled partition keeps its records queued; they are not lost.
                self._broker.offer(record)
                continue
            if self._consumer.unbounded_passthrough:
                self._to_raw([record])
            else:
                self._consumer.offer(record)

    def _to_raw(self, records: list[_Record]) -> None:
        for record in records:
            if self.config.loss_fraction and self._rng.random() < self.config.loss_fraction:
                # Silent loss: the thing reconciliation exists to catch.
                self.lost += 1
                continue
            self.raw_physical += 1
            key = record.identity.as_tuple()
            if key in self.config.conflict_identities:
                payload = dict(record.payload)
                payload["price_ticks"] = int(payload.get("price_ticks", 0)) + 1
                self._canonical[key] = payload
            else:
                # Dedupe on identity: a physical duplicate must not change the
                # canonical unique set (INV-06).
                self._canonical.setdefault(key, dict(record.payload))

    def _refresh_backlog(self) -> None:
        self._backlog_per_partition = {}
        for stage_queue in (self._broker, self._consumer):
            for record in stage_queue._queue:  # noqa: SLF001 - simulation internals
                key = (record.topic, record.partition)
                self._backlog_per_partition[key] = self._backlog_per_partition.get(key, 0) + 1
        for key in self._stalled:
            self._backlog_per_partition.setdefault(key, 0)

    # -- observation ------------------------------------------------------
    def counters(self) -> StageCounters:
        return StageCounters(
            offered_count=self.offered,
            issued_count=self.issued,
            source_sealed_count=len(self._source_identities),
            adapter_accepted_count=self.accepted,
            producer_ack_count=self.producer_acked,
            kafka_physical_count=self.kafka_physical,
            raw_physical_count=self.raw_physical,
            canonical_unique_count=len(self._canonical),
            # Left unmeasured on purpose: the expected set is the independent
            # oracle's job, and the SUT must not supply its own answer key.
            oracle_expected_count=None,
        )

    def observe(self, t_ns: int) -> Observation:
        self.advance_to(t_ns)
        backlog = Backlog()
        for (topic, partition), records in self._backlog_per_partition.items():
            backlog.set(topic, partition, records)
        backlog.retention_truncated = set(self.config.retention_truncated_partitions)
        return Observation(
            t_ns=t_ns,
            counters=self.counters(),
            backlog=backlog,
            notes=["simulated pipeline: not evidence about a real component"],
        )

    def drain(self, deadline_ns: int) -> Observation:
        """Stop accepting and run the clock to the deadline.

        The deadline matters: section 11 gives the drain a recorded deadline so
        an incomplete drain is visible rather than being waited out forever.
        """
        self._accepting = False
        self.advance_to(deadline_ns)
        observation = self.observe(deadline_ns)
        remaining = sum(self._backlog_per_partition.values())
        if remaining:
            observation.notes.append(
                f"{remaining} record(s) still queued at the drain deadline; "
                "processing debt remains"
            )
        return observation

    def canonical_identities(self) -> set[tuple[str, str, int]]:
        return set(self._canonical)

    def canonical_payload(self, key: tuple[str, str, int]) -> dict[str, Any] | None:
        payload = self._canonical.get(key)
        return dict(payload) if payload is not None else None

    def source_identities(self) -> set[tuple[str, str, int]]:
        return set(self._source_identities)

    def accepted_identities(self) -> set[tuple[str, str, int]]:
        """Identities the adapter admitted, which is what READY should contain."""
        return set(self._accepted_identities)

    def source_payload(self, key: tuple[str, str, int]) -> dict[str, Any] | None:
        payload = self._source_payloads.get(key)
        return dict(payload) if payload is not None else None

    # -- diagnostics ------------------------------------------------------
    def queue_report(self) -> dict[str, Any]:
        return {
            "adapter": self._adapter.as_dict(),
            "broker": self._broker.as_dict(),
            "consumer": self._consumer.as_dict(),
            "dropped_backpressure": self.dropped_backpressure,
            "silently_lost": self.lost,
            "rejected_by_policy": self.rejected,
            "config": self.config.as_dict(),
        }
