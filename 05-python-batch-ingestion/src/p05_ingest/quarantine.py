"""Poison-record policy: fail-closed by default, durable DLQ opt-in.

Spec FR-12 and I-07. The important property is what the two modes are allowed
to return:

  * ``stop`` (default) returns ``BLOCKED``. The partition halts, the run ends
    INCOMPLETE and the frontier never moves past the offending record. Nothing
    is skipped, nothing is written to the market table.
  * ``durable_dlq`` returns ``QUARANTINED`` **only after the DLQ write is
    acknowledged**. Until that ACK exists the record is ``BLOCKED`` exactly as
    in stop mode. A quarantine without a durable ACK is a silent drop wearing a
    different name.

The raw payload is capped before it is handed to the DLQ, and the quarantine
record carries the Kafka coordinate so that a replay produces the same
quarantine identity instead of accumulating copies.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from .config import QuarantineConfig
from .model import ConsumedRecord, RecordDisposition

#: Hard cap on the payload bytes copied into a quarantine record. A poison
#: record is already untrusted input; copying it unbounded into another topic
#: would hand the same problem to the DLQ consumer.
MAX_QUARANTINED_PAYLOAD_BYTES = 4096


@dataclass(frozen=True, slots=True)
class QuarantineRecord:
    """What gets published to the DLQ. Carries no market semantics."""

    quarantine_id: str
    topic: str
    partition: int
    offset: int
    reason_code: str
    reason_detail: str
    ingest_run_id: str
    payload_sha256: str
    payload_prefix_hex: str
    payload_truncated: bool

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "quarantine_id": self.quarantine_id,
                "topic": self.topic,
                "partition": self.partition,
                "offset": self.offset,
                "reason_code": self.reason_code,
                "reason_detail": self.reason_detail,
                "ingest_run_id": self.ingest_run_id,
                "payload_sha256": self.payload_sha256,
                "payload_prefix_hex": self.payload_prefix_hex,
                "payload_truncated": self.payload_truncated,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")


@runtime_checkable
class DeadLetterPublisher(Protocol):
    """Durable sink for quarantined records.

    ``publish`` must only return normally when the write is acknowledged to the
    configured durability level (``acks=all`` for a Kafka DLQ). Any other
    outcome must raise, so that the caller keeps the record blocked.
    """

    def publish(self, record: QuarantineRecord) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class QuarantineDecision:
    disposition: RecordDisposition
    reason: str
    quarantine_id: str | None = None


def build_quarantine_record(
    record: ConsumedRecord,
    *,
    reason_code: str,
    reason_detail: str,
    ingest_run_id: str,
) -> QuarantineRecord:
    payload = bytes(record.value or b"")
    truncated = len(payload) > MAX_QUARANTINED_PAYLOAD_BYTES
    prefix = payload[:MAX_QUARANTINED_PAYLOAD_BYTES]
    # The identity is the Kafka coordinate, so replaying the same poison record
    # yields the same quarantine_id and the DLQ stays logically idempotent.
    quarantine_id = hashlib.sha256(
        f"{record.tp.topic}\x00{record.tp.partition}\x00{record.offset}".encode()
    ).hexdigest()
    return QuarantineRecord(
        quarantine_id=quarantine_id,
        topic=record.tp.topic,
        partition=record.tp.partition,
        offset=record.offset,
        reason_code=reason_code,
        reason_detail=reason_detail[:512],
        ingest_run_id=ingest_run_id,
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        payload_prefix_hex=prefix.hex(),
        payload_truncated=truncated,
    )


class QuarantinePolicy:
    """Applies the configured poison policy to one refused record."""

    def __init__(
        self,
        cfg: QuarantineConfig,
        *,
        ingest_run_id: str,
        publisher: DeadLetterPublisher | None = None,
    ) -> None:
        self._cfg = cfg
        self._run_id = ingest_run_id
        self._publisher = publisher
        if cfg.mode == "durable_dlq" and publisher is None:
            raise ValueError(
                "quarantine mode 'durable_dlq' requires a DeadLetterPublisher; "
                "without one the records would be dropped silently"
            )

    @property
    def mode(self) -> str:
        return self._cfg.mode

    def handle(
        self, record: ConsumedRecord, *, reason_code: str, reason_detail: str
    ) -> QuarantineDecision:
        """Decide the disposition of a record validation refused."""
        if self._cfg.mode == "stop":
            return QuarantineDecision(
                RecordDisposition.BLOCKED,
                reason=(
                    f"{reason_code}: fail-closed policy stops partition "
                    f"{record.tp} at offset {record.offset}"
                ),
            )

        quarantine_record = build_quarantine_record(
            record,
            reason_code=reason_code,
            reason_detail=reason_detail,
            ingest_run_id=self._run_id,
        )
        try:
            self._publisher.publish(quarantine_record)
        except Exception as exc:
            # No durable ACK, so the record is NOT skipped. This is the branch
            # that keeps I-07 true when the DLQ itself is unavailable.
            return QuarantineDecision(
                RecordDisposition.BLOCKED,
                reason=(
                    f"{reason_code}: DLQ publish was not acknowledged ({exc}); "
                    "the record stays blocked rather than being skipped"
                ),
            )
        return QuarantineDecision(
            RecordDisposition.QUARANTINED,
            reason=f"{reason_code}: durably quarantined",
            quarantine_id=quarantine_record.quarantine_id,
        )

    def close(self) -> None:
        if self._publisher is not None:
            self._publisher.close()
