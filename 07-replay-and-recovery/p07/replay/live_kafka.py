"""A real broker behind the same read-only surface the cut scanner already uses.

P07's status has been ``UNIT/MODEL``: its reconcilers are sound but they have
only ever observed ``FakeKafka``, so a PASS said something about protocol logic
and nothing about a deployment. P09 records that as a blocker, because an
evidence plane that cannot see the real system cannot answer G3 or G4.

What makes this small is that :func:`p07.replay.kafka_cut_scanner.scan_cut` was
already written against a narrow surface rather than against the fake:

* ``cluster_id`` and ``topic_generation`` -- compared against the cut, so a cut
  from another cluster or another topic generation is refused rather than
  scanned.
* ``scan(topic, partition, start, end_exclusive)`` -- yielding ``Record``.

So nothing in the scanner, the oracles or the reconcilers changes. Only the
object they read from does, and every classification they already make --
duplicate identities, conflicts, MDP1 prefixes inside a value, key/symbol
disagreement -- starts applying to real records.

Two properties this adapter keeps, because the scanner's correctness rests on
them:

**It refuses rather than truncates.** A start below the log start raises
``OffsetExpired``; an end past the high watermark raises ``KafkaError``. The
scanner then counts what it received against the offset arithmetic and raises
``CUT_OFFSET_HOLE`` on any shortfall. Returning a shorter range would make a
missing record look like an empty one, which is the most dangerous thing a read
path can do here.

**It never joins the consumer group.** Partitions are assigned explicitly and
nothing is committed, so scanning cannot move P05's offsets. An evidence plane
that perturbed the system it measures would not be independent of it.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator, Mapping

from ..dataplane.kafka_fake import KafkaError, OffsetExpired, Record
from .kafka_cut_scanner import KafkaCut

try:
    from confluent_kafka import Consumer, TopicPartition
except ImportError:  # pragma: no cover - reported, never worked around
    Consumer = None  # type: ignore[assignment]
    TopicPartition = None  # type: ignore[assignment]


class LiveKafkaUnavailable(RuntimeError):
    """The driver or the broker is not reachable, so nothing may be claimed."""


class LiveKafka:
    """Read-only view of a real Kafka cluster, shaped like ``FakeKafka``.

    Construct it, scan the ranges a cut names, close it. One consumer is held
    for the lifetime of the scan because assigning and seeking per partition is
    cheaper than a connection per range.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        topic_generation: str,
        timeout_s: float = 20.0,
        poll_timeout_s: float = 1.0,
    ) -> None:
        if Consumer is None:
            raise LiveKafkaUnavailable(
                "confluent_kafka is not installed; install the 'lab' extra "
                "before claiming anything about a real broker"
            )
        self._timeout_s = timeout_s
        self._poll_timeout_s = poll_timeout_s
        self._topic_generation = topic_generation
        # A group id is required by the driver even when nothing is committed.
        # Scoped to the evidence plane so it can never be mistaken for a
        # consumer that owns offsets.
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": "p07-evidence-scanner",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "error",
            }
        )
        try:
            metadata = self._consumer.list_topics(timeout=timeout_s)
        except Exception as exc:  # the driver raises its own types
            raise LiveKafkaUnavailable("cannot reach the broker: %s" % exc) from exc
        self._cluster_id = metadata.cluster_id or ""
        self._partition_counts = {
            name: len(topic.partitions) for name, topic in metadata.topics.items()
        }

    # -- the surface scan_cut depends on ----------------------------------

    @property
    def cluster_id(self) -> str:
        return self._cluster_id

    @property
    def topic_generation(self) -> str:
        """Kafka has no such concept, so the operator supplies it.

        P09 issues the generation tag and records it in the lock; it is the only
        component that knows when a topic was deleted and recreated. Deriving it
        from the broker would mean inventing it.
        """
        return self._topic_generation

    def partition_count(self, topic: str) -> int:
        return self._partition_counts.get(topic, 0)

    def scan(
        self, topic: str, partition: int, start: int, end_exclusive: int
    ) -> Iterator[Record]:
        """Read-only scan of ``[start, end_exclusive)``. Refuses to truncate."""
        if partition >= self._partition_counts.get(topic, 0):
            raise KafkaError("UNKNOWN_PARTITION", "%s/%d" % (topic, partition))
        if end_exclusive < start:
            raise KafkaError(
                "CUT_RANGE_INVALID",
                "%s/%d end=%d < start=%d" % (topic, partition, end_exclusive, start),
            )
        if end_exclusive == start:
            return

        tp = TopicPartition(topic, partition)
        try:
            low, high = self._consumer.get_watermark_offsets(
                tp, timeout=self._timeout_s, cached=False
            )
        except Exception as exc:
            raise KafkaError("WATERMARK_UNAVAILABLE", str(exc)) from exc

        if start < low:
            raise OffsetExpired(
                "CUT_RANGE_EXPIRED",
                "%s/%d start=%d < log_start=%d" % (topic, partition, start, low),
            )
        if end_exclusive > high:
            # Past the high watermark the records do not exist yet. Raising
            # keeps "not written" distinct from "written and missing".
            raise KafkaError(
                "CUT_RANGE_BEYOND_HIGH_WATERMARK",
                "%s/%d end=%d > high=%d" % (topic, partition, end_exclusive, high),
            )

        self._consumer.assign([TopicPartition(topic, partition, start)])
        try:
            yield from self._read_range(topic, partition, start, end_exclusive)
        finally:
            self._consumer.assign([])

    def _read_range(
        self, topic: str, partition: int, start: int, end_exclusive: int
    ) -> Iterator[Record]:
        wanted = end_exclusive - start
        delivered = 0
        empty_polls = 0
        # Bounded on empty polls rather than wall clock. The scanner compares
        # what it received against the offset arithmetic, so stopping early
        # surfaces as CUT_OFFSET_HOLE rather than as a silent short read.
        max_empty_polls = 20

        while delivered < wanted:
            message = self._consumer.poll(self._poll_timeout_s)
            if message is None:
                empty_polls += 1
                if empty_polls > max_empty_polls:
                    return
                continue
            empty_polls = 0
            if message.error() is not None:
                raise KafkaError("SCAN_ERROR", str(message.error()))
            offset = message.offset()
            if offset is None or offset < start:
                continue
            if offset >= end_exclusive:
                return

            headers: dict[str, str] = {}
            for key, value in message.headers() or []:
                headers[key] = (
                    value.decode("utf-8", errors="replace")
                    if isinstance(value, (bytes, bytearray))
                    else str(value)
                )
            _kind, timestamp_ms = message.timestamp()
            yield Record(
                offset=offset,
                key=message.key() or b"",
                value=message.value() or b"",
                headers=headers,
                timestamp_ns=int(timestamp_ms) * 1_000_000 if timestamp_ms > 0 else 0,
            )
            delivered += 1

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception:  # closing twice must not mask a real finding
            pass

    def __enter__(self) -> "LiveKafka":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()


def cut_from_p09_json(path: str | Path, *, fenced: bool) -> KafkaCut:
    """Build a :class:`KafkaCut` from the cut P09 sealed.

    ``fenced`` is an argument with no default rather than a field read from the
    file. The scanner refuses an unfenced cut because a producer still appending
    makes the window move while it is read, and only the operator who stopped
    the producers can assert that they were stopped. A cut file cannot assert
    that about itself.
    """
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    raw = payload.get("cut")
    if not isinstance(raw, dict) or not raw:
        raise LiveKafkaUnavailable("the cut carries no topic-partition ranges")

    ranges: dict[tuple[str, int], tuple[int, int]] = {}
    for key, entry in raw.items():
        if ":" not in key:
            raise LiveKafkaUnavailable("malformed topic-partition key %r" % key)
        topic, _, partition_text = key.rpartition(":")
        ranges[(topic, int(partition_text))] = (
            int(entry.get("log_start", 0)),
            int(entry["end"]),
        )

    counts: dict[str, int] = {}
    for topic, _partition in ranges:
        counts[topic] = counts.get(topic, 0) + 1

    return KafkaCut(
        cluster_id=str(payload.get("cluster_id", "")),
        topic_generation=str(payload.get("topic_generation", "unknown")),
        ranges=ranges,
        fenced=fenced,
        captured_at_ns=int(payload.get("captured_at_ns", 0)),
        partition_counts=counts,
    )


def scan_live_cut(
    bootstrap_servers: str,
    cut_path: str | Path,
    *,
    fenced: bool,
    expect_dataset_id: str | None = None,
) -> Mapping[str, Any]:
    """Scan a real cut and summarise the inventory.

    Thin on purpose: the classification is :func:`scan_cut`'s, unchanged, so a
    real scan and a modelled one are judged by exactly the same code.
    """
    from .kafka_cut_scanner import scan_cut

    cut = cut_from_p09_json(cut_path, fenced=fenced)
    with LiveKafka(bootstrap_servers, topic_generation=cut.topic_generation) as kafka:
        # A P09-sealed cut may not record a cluster id. Adopting the live one is
        # safe because it makes no competing claim; a cut that DOES name a
        # cluster is still compared, and a mismatch still refuses.
        if not cut.cluster_id:
            cut = KafkaCut(
                cluster_id=kafka.cluster_id,
                topic_generation=cut.topic_generation,
                ranges=cut.ranges,
                fenced=cut.fenced,
                captured_at_ns=cut.captured_at_ns,
                partition_counts=cut.partition_counts,
            )
        inventory = scan_cut(kafka, cut, expect_dataset_id=expect_dataset_id)

    return {
        "cluster_id": inventory.cut.cluster_id,
        "topic_generation": inventory.cut.topic_generation,
        "physical_records": inventory.physical_records,
        "logical_identities": len(inventory.logical),
        "duplicate_identities": len(inventory.duplicate_identities),
        "identity_conflicts": len(inventory.identity_conflicts),
        "findings": [f.reason.value for f in inventory.findings],
        "profile": "LIVE",
    }
