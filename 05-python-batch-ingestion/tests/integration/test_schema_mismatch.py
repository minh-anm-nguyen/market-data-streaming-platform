"""E2E-17 schema mismatch rollout.

PASS oracle (spec 9.5): a decode or version mismatch is explicit. Nothing is
written and nothing is committed on the strength of a schema this build does
not understand.

The interesting half is the *silent* mismatch: a v2 producer that adds a field
the pinned schema has never heard of. protobuf does not complain about that --
the field lands in the unknown-field set and the message otherwise parses
perfectly. Without the compatibility gate it would be ingested as if nothing
had happened.
"""

from __future__ import annotations

import pytest

from p05_ingest.errors import PreflightError
from p05_ingest.metrics import RECORDS_VALIDATED
from p05_ingest.model import TopicPartition
from p05_ingest.preflight import check_schema_pin, run_preflight
from p05_ingest.proto_schema import descriptor_sha256, verify_descriptor_pin
from p05_ingest.testkit import TICKS_TOPIC, make_tick
from tests.conftest import build_harness, make_config

TP0 = TopicPartition(TICKS_TOPIC, 0)


def varint(n: int) -> bytes:
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        out.append(byte | (0x80 if n else 0))
        if not n:
            return bytes(out)


def ticks_only(dev_config, **batch):
    return make_config(dev_config, kafka={"topics": ["md.ticks.v1"]}, batch=batch)


# ----------------------------------------------------- the startup schema gate


def test_mismatched_descriptor_pin_refuses_to_start(dev_config):
    cfg = make_config(
        dev_config, contracts={"protobuf_descriptor_sha256": "ab" * 32}
    )
    result = check_schema_pin(cfg)
    assert not result.ok
    assert result.required

    report = run_preflight(cfg)
    assert not report.ok
    with pytest.raises(PreflightError, match="preflight failed"):
        report.raise_if_failed()


def test_matching_descriptor_pin_passes(dev_config):
    assert dev_config.contracts.protobuf_descriptor_sha256 == descriptor_sha256()
    assert check_schema_pin(dev_config).ok


def test_wrong_package_refuses_to_start(dev_config):
    with pytest.raises(PreflightError, match="package mismatch"):
        verify_descriptor_pin(descriptor_sha256(), expected_package="marketdata.v2")


# ------------------------------------------------- a v2 producer at runtime


def test_e2e_17_unknown_v2_field_is_refused_not_silently_accepted(
    dev_config, clock
):
    """A v2 writer adds field 7. The pinned v1 schema must refuse the record.

    One row per batch, so the v1 record is sealed and acknowledged before the
    v2 one arrives. That makes both halves observable: the compatible record
    is ingested, and the incompatible one is refused rather than written.
    """
    cfg = ticks_only(dev_config, per_partition_max_rows=1)
    harness = build_harness(cfg, clock=clock, partitions=1)

    v1 = make_tick(sequence=0).SerializeToString()
    # Field 7 does not exist in marketdata.v1. protobuf parses this happily.
    v2 = make_tick(sequence=1).SerializeToString() + varint((7 << 3) | 0) + varint(5)
    harness.cluster.produce(TICKS_TOPIC, 0, key=b"EURUSD", value=v1)
    harness.cluster.produce(TICKS_TOPIC, 0, key=b"EURUSD", value=v2)

    status = harness.finish()

    # The v1 record went through; the v2 record stopped the partition.
    assert harness.sink.row_count == 1
    assert harness.sink.rows[0]["sequence"] == 0
    assert not status.complete
    assert TP0 in harness.engine.blocked_partitions
    assert harness.metrics.counter(
        RECORDS_VALIDATED, kind="refused", result="SCHEMA_UNKNOWN_FIELDS"
    ) == 1


def test_e2e_17_nothing_is_committed_past_the_incompatible_record(
    dev_config, clock
):
    cfg = ticks_only(dev_config, per_partition_max_rows=1)
    harness = build_harness(cfg, clock=clock, partitions=1)

    harness.cluster.produce(
        TICKS_TOPIC, 0, key=b"EURUSD", value=make_tick(sequence=0).SerializeToString()
    )
    bad_offset = harness.cluster.produce(
        TICKS_TOPIC,
        0,
        key=b"EURUSD",
        value=make_tick(sequence=1).SerializeToString()
        + varint((7 << 3) | 0)
        + varint(5),
    )
    harness.cluster.produce(
        TICKS_TOPIC, 0, key=b"EURUSD", value=make_tick(sequence=2).SerializeToString()
    )

    harness.finish()

    committed = harness.cluster.committed(cfg.kafka.group_id, TP0) or 0
    assert committed == bad_offset, "the frontier stops exactly at the bad record"
    assert all(r["kafka_offset"] < bad_offset for r in harness.sink.rows)


def test_a_new_oneof_variant_is_refused(dev_config, clock):
    """A v2 payload variant (field 12) must not be guessed at."""
    cfg = ticks_only(dev_config, per_partition_max_rows=2)
    harness = build_harness(cfg, clock=clock, partitions=1)

    # A message with only an unknown oneof member set: no known payload.
    raw = (
        varint((1 << 3) | 2)
        + varint(len(b"demo-a1b2c3"))
        + b"demo-a1b2c3"
        + varint((2 << 3) | 2)
        + varint(len(b"EURUSD"))
        + b"EURUSD"
        + varint((3 << 3) | 0)
        + varint(1)
        + varint((4 << 3) | 0)
        + varint(1)
        + varint((5 << 3) | 0)
        + varint(100_000)
        + varint((12 << 3) | 2)
        + varint(2)
        + b"\x08\x01"
    )
    harness.cluster.produce(TICKS_TOPIC, 0, key=b"EURUSD", value=raw)

    status = harness.finish()

    assert harness.sink.row_count == 0
    assert not status.complete


def test_header_declared_schema_mismatch_is_refused(dev_config, clock):
    """A producer declaring a different descriptor hash must be refused."""
    cfg = ticks_only(dev_config, per_partition_max_rows=2)
    harness = build_harness(cfg, clock=clock, partitions=1)
    harness.cluster.produce(
        TICKS_TOPIC,
        0,
        key=b"EURUSD",
        value=make_tick(sequence=0).SerializeToString(),
        headers=[("md-descriptor-sha256", b"c" * 64)],
    )

    status = harness.finish()

    assert harness.sink.row_count == 0
    assert not status.complete
    assert harness.metrics.counter(
        RECORDS_VALIDATED, kind="refused", result="HEADER_DESCRIPTOR_MISMATCH"
    ) == 1


def test_header_declared_schema_match_is_accepted(dev_config, clock):
    cfg = ticks_only(dev_config, per_partition_max_rows=2)
    harness = build_harness(cfg, clock=clock, partitions=1)
    harness.cluster.produce(
        TICKS_TOPIC,
        0,
        key=b"EURUSD",
        value=make_tick(sequence=0).SerializeToString(),
        headers=[
            ("md-descriptor-sha256", descriptor_sha256().encode("ascii")),
            ("md-schema", b"marketdata.v1"),
        ],
    )

    status = harness.finish()
    assert status.complete, status.reason
    assert harness.sink.row_count == 1


def test_schema_version_allowlist_blocks_an_unapproved_writer(dev_config, clock):
    """Pinning the version ladder: an unlisted writer is refused."""
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        contracts={"allowed_source_schema_versions": ["marketdata.v1"]},
        batch={"per_partition_max_rows": 2},
    )
    harness = build_harness(cfg, clock=clock, partitions=1)
    harness.cluster.produce(
        TICKS_TOPIC,
        0,
        key=b"EURUSD",
        value=make_tick(
            sequence=0, source_schema_version="marketdata.v2"
        ).SerializeToString(),
    )

    status = harness.finish()
    assert harness.sink.row_count == 0
    assert not status.complete
