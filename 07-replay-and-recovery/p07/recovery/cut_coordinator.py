"""Fence the writers, then capture a cut that cannot move underneath the scan.

Design 3.3 lays out the sequence and the trap in it. The trap is that every
cheap signal for "everything has arrived" is wrong:

* ``consumer lag == 0`` only means the consumer caught up with what the broker
  had *at that moment*. A producer still holding an unacknowledged record can
  extend the log a millisecond later.
* a producer's own delivery ledger reporting all-delivered is its own account,
  not the log's.
* a container being ``Up`` and a health endpoint returning 200 say nothing at
  all about data.

So :func:`capture_cut` does it in the only order that holds: stop new
generation, wait for producer callbacks to reach a terminal state within a
bounded timeout, record whatever is still ``UNKNOWN`` as unknown, read each
partition's high watermark, then read them a second time and require them to be
*unchanged*. A watermark that moved between the two reads means a writer is
still active, and the cut is rejected rather than used.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ..contracts import SourceIdentity
from ..dataplane.kafka_fake import FakeKafka
from ..dataplane.producer import Producer
from ..evidence.verdicts import Finding, Reason
from ..replay.kafka_cut_scanner import KafkaCut, TopicPartition


class CutRejected(Exception):
    """The cut could not be captured safely."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True, slots=True)
class FenceResult:
    """What the producer fence achieved."""

    drained: bool
    delivered: int
    failed: int
    unknown: int
    unknown_identities: tuple[SourceIdentity, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "drained": self.drained,
            "delivered": self.delivered,
            "failed": self.failed,
            "unknown": self.unknown,
            "unknown_identities": [list(identity) for identity in self.unknown_identities],
            "note": (
                "UNKNOWN means the producer could not determine the outcome. It is not "
                "'not published'; only an independent scan can resolve it."
            ),
        }


def fence_producers(producers: Sequence[Producer]) -> FenceResult:
    """Flush every producer and classify what is left unresolved.

    ``drained`` is about the queue, not about certainty: a producer can drain
    its queue and still leave records whose fate is unknown. The two facts are
    reported separately because collapsing them is how an ambiguous ACK turns
    into a false "all published".
    """
    delivered = failed = unknown = 0
    unknown_identities: list[SourceIdentity] = []
    for producer in producers:
        result = producer.flush()
        delivered += result["delivered"]
        failed += result["failed"]
        unknown += result["unknown"]
        unknown_identities.extend(producer.unresolved_identities())
    drained = all(producer.queue_depth == 0 for producer in producers)
    return FenceResult(drained, delivered, failed, unknown, tuple(unknown_identities))


def capture_cut(
    kafka: FakeKafka,
    *,
    producers: Sequence[Producer] = (),
    start_offsets: Mapping[TopicPartition, int] | None = None,
    topics: Sequence[str] | None = None,
    stability_checks: int = 2,
) -> tuple[KafkaCut, FenceResult]:
    """Fence, then capture a stable ``[start, end)`` range per topic-partition.

    ``start_offsets`` defaults to each partition's current ``log_start_offset``,
    which is the earliest offset still readable. Using 0 instead would produce a
    cut whose beginning has already been deleted by retention, and the scan
    would then fail on a range the operator never meant to claim.

    ``stability_checks`` is how many times the watermarks are read and required
    to agree. Two is the minimum that can detect an active writer at all; more
    is available for a noisy lab. It is a check, not a guarantee -- a writer
    quiet across both reads is still possible, which is why fencing comes first
    and is not replaced by this.
    """
    fence = fence_producers(producers)
    if not fence.drained:
        raise CutRejected(Reason.CUT_WRITER_ACTIVE, "a producer queue is not empty")

    names = tuple(topics) if topics is not None else kafka.topics()
    observed: list[dict[TopicPartition, int]] = []
    for _ in range(max(2, stability_checks)):
        snapshot: dict[TopicPartition, int] = {}
        for topic in names:
            for partition in range(kafka.partition_count(topic)):
                snapshot[(topic, partition)] = kafka.high_watermark(topic, partition)
        observed.append(snapshot)

    first = observed[0]
    for later in observed[1:]:
        moved = [tp for tp in first if first[tp] != later.get(tp)]
        if moved:
            raise CutRejected(
                Reason.CUT_WRITER_ACTIVE,
                f"high watermark moved during capture: {sorted(moved)[:5]}",
            )

    ranges: dict[TopicPartition, tuple[int, int]] = {}
    for tp, end in first.items():
        topic, partition = tp
        default_start = kafka.log_start_offset(topic, partition)
        start = (start_offsets or {}).get(tp, default_start)
        if start < default_start:
            raise CutRejected(
                Reason.CUT_RANGE_EXPIRED,
                f"{topic}/{partition} requested start {start} < log_start {default_start}",
            )
        ranges[tp] = (start, end)

    cut = KafkaCut(
        cluster_id=kafka.cluster_id,
        topic_generation=kafka.topic_generation,
        ranges=ranges,
        fenced=True,
        captured_at_ns=time.time_ns(),
        partition_counts={topic: kafka.partition_count(topic) for topic in names},
        producer_unknown_identities=fence.unknown_identities,
    )
    return cut, fence


def cut_findings(cut: KafkaCut, *, resolved_identities: Sequence[SourceIdentity] = ()) -> list[Finding]:
    """Findings a cut carries forward into the gate.

    An unresolved producer ``UNKNOWN`` that the scan *did* find is resolved and
    produces no finding -- that is the scan doing its job. One the scan did not
    find stays unresolved and becomes
    :data:`~p07.evidence.verdicts.Reason.PRODUCER_ACK_UNKNOWN`, which the gate
    maps to ``INCONCLUSIVE``.
    """
    resolved = set(resolved_identities)
    outstanding = [
        identity for identity in cut.producer_unknown_identities if identity not in resolved
    ]
    findings: list[Finding] = []
    if not cut.fenced:
        findings.append(Finding(Reason.CUT_NOT_FENCED, {}))
    if outstanding:
        findings.append(
            Finding(
                Reason.PRODUCER_ACK_UNKNOWN,
                {
                    "count": len(outstanding),
                    "sample": [list(identity) for identity in outstanding[:10]],
                },
            )
        )
    return findings
