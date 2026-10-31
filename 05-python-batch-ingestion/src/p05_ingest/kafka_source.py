"""Kafka consumer protocol and the real ``confluent-kafka`` adapter.

The protocol exists so the engine can be driven by a deterministic in-memory
cluster in tests (``p05_ingest.testkit.fake_kafka``) while production uses
librdkafka. Everything the engine needs is here and nothing else: the engine
must never reach for a driver object directly, because only the owner thread is
allowed to touch the consumer at all (spec ADR-002).

Two API details the spec insists on, both encoded in the signatures:

  * ``commit`` takes an explicit ``(topic, partition, next_offset)``. The
    offset-less ``commit()`` form commits the consumer *position*, which can be
    ahead of what we have proven, so it is not offered here at all
    (spec section 4.3).
  * ``commit`` must surface per-partition errors. A call that returns without
    raising means the broker confirmed that exact offset.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Protocol, runtime_checkable

from .errors import CommitNotConfirmedError, RetentionLostError
from .model import ConsumedRecord, TopicPartition

#: Callbacks the engine installs. ``on_lost`` is distinct from ``on_revoke``
#: because it forbids committing (spec I-02).
AssignCallback = Callable[[list[TopicPartition]], None]


@runtime_checkable
class KafkaSource(Protocol):
    """The consumer surface the engine is allowed to use."""

    def subscribe(
        self,
        topics: Sequence[str],
        *,
        on_assign: AssignCallback,
        on_revoke: AssignCallback,
        on_lost: AssignCallback,
    ) -> None: ...

    def poll(self, timeout_s: float) -> ConsumedRecord | None:
        """Return one record, or None on timeout.

        An empty return means "nothing right now" and never means the dataset
        has ended (spec I-08).
        """
        ...

    def commit_offset(self, tp: TopicPartition, next_offset: int) -> None:
        """Synchronously commit one explicit next offset.

        Raises ``CommitNotConfirmedError`` when the broker did not confirm.
        """
        ...

    def committed_next(self, tp: TopicPartition) -> int:
        """Authoritative committed next offset as the broker reports it."""
        ...

    def watermarks(self, tp: TopicPartition) -> tuple[int, int]:
        """``(log_start_offset, high_watermark)`` for retention checks."""
        ...

    def pause(self, tps: Sequence[TopicPartition]) -> None: ...

    def resume(self, tps: Sequence[TopicPartition]) -> None: ...

    def assignment(self) -> list[TopicPartition]: ...

    def close(self) -> None: ...


class ConfluentKafkaSource:
    """Adapter over ``confluent_kafka.Consumer``.

    Only constructed when a live broker is configured; the import is lazy so
    the package remains usable for unit tests on a machine with no librdkafka
    runtime available.
    """

    def __init__(self, cfg, *, logger=None) -> None:
        self._cfg = cfg
        self._logger = logger
        self._consumer = None
        self._lost_handler: AssignCallback | None = None

    # --------------------------------------------------------- client config

    def client_properties(self) -> dict[str, object]:
        """The effective librdkafka properties, ready to log with no secrets.

        Spec section 12.1 warns that TOML key names and librdkafka property
        names do not line up, and that a wrapper may not support every
        property. Keeping the mapping in one visible place is what makes that
        auditable.
        """
        return {
            "bootstrap.servers": self._cfg.bootstrap_servers,
            "group.id": self._cfg.group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": self._cfg.auto_offset_reset,
            "allow.auto.create.topics": self._cfg.allow_auto_create_topics,
            "isolation.level": self._cfg.isolation_level,
            "session.timeout.ms": self._cfg.session_timeout_ms,
            "max.poll.interval.ms": self._cfg.max_poll_interval_ms,
            "queued.max.messages.kbytes": self._cfg.queued_max_messages_kbytes,
            "partition.assignment.strategy": "cooperative-sticky",
        }

    def _ensure(self):
        if self._consumer is not None:
            return self._consumer
        from confluent_kafka import Consumer

        self._consumer = Consumer(self.client_properties())
        return self._consumer

    # -------------------------------------------------------------- lifecycle

    def subscribe(
        self,
        topics,
        *,
        on_assign: AssignCallback,
        on_revoke: AssignCallback,
        on_lost: AssignCallback,
    ) -> None:
        from confluent_kafka import TopicPartition as CkTopicPartition  # noqa: F401

        consumer = self._ensure()
        self._lost_handler = on_lost

        def _wrap(handler):
            def _cb(_consumer, partitions):
                handler([TopicPartition(p.topic, p.partition) for p in partitions])

            return _cb

        consumer.subscribe(
            list(topics),
            on_assign=_wrap(on_assign),
            on_revoke=_wrap(on_revoke),
            on_lost=_wrap(on_lost),
        )

    def poll(self, timeout_s: float) -> ConsumedRecord | None:
        from confluent_kafka import KafkaError

        msg = self._ensure().poll(timeout_s)
        if msg is None:
            return None
        err = msg.error()
        if err is not None:
            if err.code() == KafkaError._PARTITION_EOF:
                return None
            if err.code() in (
                KafkaError.OFFSET_OUT_OF_RANGE,
                KafkaError._AUTO_OFFSET_RESET,
            ):
                # The offset the group needs has fallen off the log. There is
                # no safe automatic recovery: resetting would silently skip or
                # re-read data (spec I-11, section 5.2 RETENTION_LOST).
                raise RetentionLostError(
                    f"{msg.topic()}[{msg.partition()}]: required offset is no "
                    f"longer available ({err.str()})"
                )
            raise RuntimeError(f"Kafka poll error: {err.str()}")

        headers = tuple(msg.headers() or ())
        return ConsumedRecord(
            tp=TopicPartition(msg.topic(), msg.partition()),
            offset=msg.offset(),
            key=msg.key(),
            value=msg.value(),
            headers=tuple((name, value or b"") for name, value in headers),
            timestamp_ms=msg.timestamp()[1] if msg.timestamp() else None,
        )

    # ---------------------------------------------------------------- commits

    def commit_offset(self, tp: TopicPartition, next_offset: int) -> None:
        from confluent_kafka import TopicPartition as CkTopicPartition

        results = self._ensure().commit(
            offsets=[CkTopicPartition(tp.topic, tp.partition, next_offset)],
            asynchronous=False,
        )
        if results is None:
            raise CommitNotConfirmedError(
                f"{tp}: commit returned no per-partition result; the committed "
                "position is unknown and must be re-read"
            )
        for result in results:
            if result.error is not None:
                raise CommitNotConfirmedError(
                    f"{tp}: commit rejected: {result.error}"
                )

    def committed_next(self, tp: TopicPartition) -> int:
        from confluent_kafka import OFFSET_INVALID
        from confluent_kafka import TopicPartition as CkTopicPartition

        results = self._ensure().committed(
            [CkTopicPartition(tp.topic, tp.partition)], timeout=10.0
        )
        if not results:
            raise CommitNotConfirmedError(f"{tp}: broker returned no committed offset")
        offset = results[0].offset
        if offset == OFFSET_INVALID or offset < 0:
            # No prior commit for this group. The caller decides what to do;
            # this adapter must not invent a position.
            return 0
        return offset

    def watermarks(self, tp: TopicPartition) -> tuple[int, int]:
        from confluent_kafka import TopicPartition as CkTopicPartition

        low, high = self._ensure().get_watermark_offsets(
            CkTopicPartition(tp.topic, tp.partition), timeout=10.0, cached=False
        )
        return low, high

    # ----------------------------------------------------------- flow control

    def pause(self, tps) -> None:
        from confluent_kafka import TopicPartition as CkTopicPartition

        if tps:
            self._ensure().pause([CkTopicPartition(t.topic, t.partition) for t in tps])

    def resume(self, tps) -> None:
        from confluent_kafka import TopicPartition as CkTopicPartition

        if tps:
            self._ensure().resume([CkTopicPartition(t.topic, t.partition) for t in tps])

    def assignment(self) -> list[TopicPartition]:
        return [
            TopicPartition(p.topic, p.partition) for p in self._ensure().assignment()
        ]

    def close(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            finally:
                self._consumer = None
