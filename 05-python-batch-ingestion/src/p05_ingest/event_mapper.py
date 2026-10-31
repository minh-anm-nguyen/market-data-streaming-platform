"""Turn a validated projection into a ClickHouse-bound row.

Deliberately boring: it computes the semantic digest, attaches the transport
coordinate and estimates the byte cost. It does no arithmetic on prices, no
time-zone conversion and no narrowing (spec module table: event_mapper must not
round prices or downcast nanoseconds).

The byte estimate deserves a warning. ``accounted_bytes`` is the figure the
budget manager charges against ``app_buffer_budget_bytes``; it is a *model* of
the retained Python objects plus the encoded insert payload, not a measurement.
Spec I-05 and section 5.4 are explicit that a 64 MiB queue budget does not
prove a 64 MiB RSS, so the soak tests assert on real RSS from ``psutil`` and
this number is only used to keep the queue bounded.
"""

from __future__ import annotations

import sys

from .canonical_digest import payload_digest
from .model import EventRow, LogicalProjection, TransportCoordinate

#: Per-row constant overhead: the EventRow, LogicalProjection and
#: TransportCoordinate wrappers plus the tuple handed to the driver. Measured
#: once with sys.getsizeof on CPython 3.12/3.14 and rounded up, because the
#: exact figure moves with the interpreter and must not be load-bearing.
_ROW_FIXED_OVERHEAD_BYTES = 512

#: A 64-char hex digest as a Python str.
_DIGEST_BYTES = 64 + sys.getsizeof("") if hasattr(sys, "getsizeof") else 128


def estimate_row_bytes(projection: LogicalProjection, raw_value_len: int) -> int:
    """Model the retained cost of one row.

    Counts the raw Kafka value (held until the batch is sealed), the decoded
    strings, and a fixed allowance for object headers and the encoded insert
    payload. Interning means the real cost of repeated ``symbol`` and
    ``dataset_id`` values is lower, so this over-counts -- which is the safe
    direction for a cap.
    """
    text_bytes = (
        len(projection.dataset_id.encode("utf-8"))
        + len(projection.symbol)
        + len(projection.source_schema_version.encode("utf-8"))
    )
    return raw_value_len + text_bytes + _DIGEST_BYTES + _ROW_FIXED_OVERHEAD_BYTES


class EventMapper:
    """Builds rows for one run. Holds the run identity, nothing mutable."""

    def __init__(self, *, ingest_run_id: str, kafka_cluster_id: str, topic_generation: str) -> None:
        self._run_id = ingest_run_id
        self._cluster_id = kafka_cluster_id
        self._topic_generation = topic_generation

    def to_row(
        self,
        projection: LogicalProjection,
        *,
        topic: str,
        partition: int,
        offset: int,
        raw_value_len: int,
    ) -> EventRow:
        coordinate = TransportCoordinate(
            kafka_cluster_id=self._cluster_id,
            topic_generation=self._topic_generation,
            topic=topic,
            partition=partition,
            offset=offset,
        )
        return EventRow(
            projection=projection,
            coordinate=coordinate,
            payload_digest=payload_digest(projection),
            ingest_run_id=self._run_id,
            accounted_bytes=estimate_row_bytes(projection, raw_value_len),
        )
