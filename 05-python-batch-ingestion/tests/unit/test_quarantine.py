"""Poison policy: fail-closed by default, skip only on a durable ACK (I-07)."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from p05_ingest.config import QuarantineConfig
from p05_ingest.errors import ConfigError
from p05_ingest.model import ConsumedRecord, RecordDisposition, TopicPartition
from p05_ingest.quarantine import (
    MAX_QUARANTINED_PAYLOAD_BYTES,
    QuarantinePolicy,
    build_quarantine_record,
)

TP = TopicPartition("md.ticks.v1", 0)


def record(offset: int = 7, value: bytes = b"\xde\xad\xbe\xef") -> ConsumedRecord:
    return ConsumedRecord(tp=TP, offset=offset, key=b"EURUSD", value=value)


class RecordingPublisher:
    """A DLQ that acknowledges, and counts what it was asked to publish."""

    def __init__(self) -> None:
        self.published = []
        self.closed = False

    def publish(self, quarantine_record) -> None:
        self.published.append(quarantine_record)

    def close(self) -> None:
        self.closed = True


class FailingPublisher:
    """A DLQ that never acknowledges."""

    def __init__(self, exc: Exception | None = None) -> None:
        self.exc = exc or RuntimeError("no acks=all acknowledgement")
        self.attempts = 0

    def publish(self, quarantine_record) -> None:
        self.attempts += 1
        raise self.exc

    def close(self) -> None:
        pass


# ------------------------------------------------------------------ stop mode


def test_stop_mode_blocks_and_never_skips():
    policy = QuarantinePolicy(QuarantineConfig(mode="stop"), ingest_run_id="run-1")
    decision = policy.handle(record(), reason_code="TICK_PRICE_ABSENT", reason_detail="x")
    assert decision.disposition is RecordDisposition.BLOCKED
    assert decision.quarantine_id is None
    assert "fail-closed" in decision.reason


def test_stop_mode_needs_no_publisher():
    QuarantinePolicy(QuarantineConfig(mode="stop"), ingest_run_id="run-1")


# ------------------------------------------------------------- durable DLQ


def test_dlq_mode_skips_only_after_an_acknowledged_publish():
    publisher = RecordingPublisher()
    policy = QuarantinePolicy(
        QuarantineConfig(mode="durable_dlq", topic="md.ingest.dlq.v1"),
        ingest_run_id="run-1",
        publisher=publisher,
    )
    decision = policy.handle(
        record(), reason_code="PROTO_DECODE_ERROR", reason_detail="corrupt"
    )
    assert decision.disposition is RecordDisposition.QUARANTINED
    assert decision.quarantine_id
    assert len(publisher.published) == 1


def test_dlq_failure_keeps_the_record_blocked():
    """The branch that keeps I-07 true when the DLQ itself is down."""
    publisher = FailingPublisher()
    policy = QuarantinePolicy(
        QuarantineConfig(mode="durable_dlq", topic="md.ingest.dlq.v1"),
        ingest_run_id="run-1",
        publisher=publisher,
    )
    decision = policy.handle(
        record(), reason_code="PROTO_DECODE_ERROR", reason_detail="corrupt"
    )
    assert decision.disposition is RecordDisposition.BLOCKED
    assert "not acknowledged" in decision.reason
    assert publisher.attempts == 1


def test_dlq_mode_without_a_publisher_is_refused_at_construction():
    """Otherwise poison records would be dropped with no sink at all."""
    with pytest.raises(ValueError, match="requires a DeadLetterPublisher"):
        QuarantinePolicy(
            QuarantineConfig(mode="durable_dlq", topic="t"), ingest_run_id="run-1"
        )


def test_dlq_config_requires_a_topic():
    with pytest.raises(ConfigError, match="topic is required"):
        QuarantineConfig(mode="durable_dlq")


def test_require_ack_cannot_be_disabled():
    """Skipping without a durable ACK is a silent drop by another name."""
    with pytest.raises(ConfigError, match="cannot be false"):
        QuarantineConfig(mode="durable_dlq", topic="t", require_ack=False)


def test_unknown_mode_is_refused():
    with pytest.raises(ConfigError, match="must be one of"):
        QuarantineConfig(mode="skip")


# -------------------------------------------------------- quarantine record


def test_quarantine_identity_is_the_kafka_coordinate():
    """A replayed poison record must re-quarantine under the same id.

    Otherwise a restart accumulates a fresh DLQ copy on every pass.
    """
    first = build_quarantine_record(
        record(offset=7), reason_code="X", reason_detail="d", ingest_run_id="run-1"
    )
    replayed = build_quarantine_record(
        record(offset=7), reason_code="X", reason_detail="d", ingest_run_id="run-2"
    )
    assert first.quarantine_id == replayed.quarantine_id

    other_offset = build_quarantine_record(
        record(offset=8), reason_code="X", reason_detail="d", ingest_run_id="run-1"
    )
    assert other_offset.quarantine_id != first.quarantine_id


def test_payload_is_capped_and_flagged_when_truncated():
    """A poison record is untrusted input; copying it unbounded helps nobody."""
    oversized = b"A" * (MAX_QUARANTINED_PAYLOAD_BYTES + 100)
    quarantined = build_quarantine_record(
        record(value=oversized),
        reason_code="VALUE_TOO_LARGE",
        reason_detail="d",
        ingest_run_id="run-1",
    )
    assert quarantined.payload_truncated
    assert len(bytes.fromhex(quarantined.payload_prefix_hex)) == MAX_QUARANTINED_PAYLOAD_BYTES
    # The digest still covers the whole payload, so the original is identifiable.
    import hashlib

    assert quarantined.payload_sha256 == hashlib.sha256(oversized).hexdigest()


def test_reason_detail_is_bounded():
    quarantined = build_quarantine_record(
        record(), reason_code="X", reason_detail="y" * 5000, ingest_run_id="run-1"
    )
    assert len(quarantined.reason_detail) <= 512


def test_quarantine_record_serialises_to_stable_json():
    quarantined = build_quarantine_record(
        record(), reason_code="X", reason_detail="d", ingest_run_id="run-1"
    )
    payload = json.loads(quarantined.to_json())
    assert payload["topic"] == TP.topic
    assert payload["partition"] == TP.partition
    assert payload["offset"] == 7
    assert payload["reason_code"] == "X"
    # The raw market payload is never embedded as plain bytes, only as hex with
    # an explicit cap, so a log of this record stays bounded.
    assert set(payload) == {
        "quarantine_id",
        "topic",
        "partition",
        "offset",
        "reason_code",
        "reason_detail",
        "ingest_run_id",
        "payload_sha256",
        "payload_prefix_hex",
        "payload_truncated",
    }


def test_close_propagates_to_the_publisher():
    publisher = RecordingPublisher()
    policy = QuarantinePolicy(
        QuarantineConfig(mode="durable_dlq", topic="t"),
        ingest_run_id="run-1",
        publisher=publisher,
    )
    policy.close()
    assert publisher.closed
