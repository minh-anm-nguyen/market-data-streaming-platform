"""Driver-error classification: the ambiguity split that correctness rests on."""

from __future__ import annotations

import pytest

from p05_ingest.clickhouse_sink import ClickHouseConnectSink, classify_exception
from p05_ingest.model import SinkOutcome


def outcome_for(exc: BaseException) -> SinkOutcome:
    return classify_exception(exc)[0]


# ----------------------------------------------------------------- permanent


@pytest.mark.parametrize(
    "message",
    [
        "Unknown identifier: price_tick",
        "DB::Exception: No such column bid_tick in table",
        "Code: 53. DB::Exception: Type mismatch",
        "Not enough privileges to insert",
        "Authentication failed for user p05",
        "Unknown table md_ingest.market_events_rawx",
        "Syntax error at position 42",
        "Cannot parse input: expected Int64",
    ],
)
def test_structural_refusals_are_permanent(message):
    """These cannot succeed as formed, so retrying only delays the alert."""
    assert outcome_for(RuntimeError(message)) is SinkOutcome.PERMANENT


# ----------------------------------------------------------------- transient


@pytest.mark.parametrize(
    "message",
    [
        "503 Service Unavailable",
        "429 Too Many Requests",
        "Connection refused",
        "Too many simultaneous queries",
        "Memory limit (total) exceeded",
        "Too many parts in partition",
        "socket timeout while connecting to localhost:8123",
    ],
)
def test_pre_send_refusals_are_transient(message):
    """The server said no before applying anything, so nothing was written."""
    assert outcome_for(RuntimeError(message)) is SinkOutcome.TRANSIENT


def test_connect_timeout_is_transient_not_ambiguous():
    """A connect-phase timeout is the one timeout that is definitely safe.

    The request was never sent, so it cannot have been applied. Everything
    else containing "timeout" must stay ambiguous.
    """
    assert (
        outcome_for(OSError("socket timeout while connecting"))
        is SinkOutcome.TRANSIENT
    )


# ----------------------------------------------------------------- ambiguous


@pytest.mark.parametrize(
    "message",
    [
        "Read timed out",
        "The read operation timed out",
        "Connection reset by peer",
        "Connection aborted",
        "Broken pipe",
        "Remote end closed connection without response",
        "IncompleteRead(512 bytes read)",
        "EOF occurred in violation of protocol",
    ],
)
def test_post_send_failures_are_ambiguous(message):
    """Spec section 7: a timeout does NOT mean the row was not written.

    Classifying any of these as a definitive failure is the single most
    dangerous mistake available in this module.
    """
    assert outcome_for(RuntimeError(message)) is SinkOutcome.AMBIGUOUS


def test_unrecognised_error_defaults_to_ambiguous():
    """Pessimistic by policy: a possible duplicate beats a possible loss."""
    outcome, detail = classify_exception(RuntimeError("something nobody expected"))
    assert outcome is SinkOutcome.AMBIGUOUS
    assert "unclassified" in detail


def test_classification_is_case_insensitive():
    assert outcome_for(RuntimeError("READ TIMED OUT")) is SinkOutcome.AMBIGUOUS
    assert outcome_for(RuntimeError("UNKNOWN IDENTIFIER: x")) is SinkOutcome.PERMANENT


def test_permanent_wins_over_an_ambiguous_substring():
    """Order matters: a structural error mentioning a reset is still fatal."""
    assert (
        outcome_for(RuntimeError("Unknown identifier after connection reset"))
        is SinkOutcome.PERMANENT
    )


def test_exception_type_name_participates_in_matching():
    class ReadTimeoutError(Exception):
        pass

    assert outcome_for(ReadTimeoutError("")) is SinkOutcome.AMBIGUOUS


# ------------------------------------------------------------ insert settings


def test_insert_settings_pin_async_mode_explicitly(dev_config):
    """Spec 6.4: never rely on the server default for async_insert."""
    sink = ClickHouseConnectSink(dev_config.clickhouse)
    settings = sink._settings()
    assert settings["async_insert"] == 0
    assert settings["insert_deduplicate"] == 1
    assert "wait_for_async_insert" not in settings, "irrelevant when sync"


def test_async_mode_always_sends_wait_for_async_insert(dev_config):
    from dataclasses import replace

    cfg = replace(dev_config.clickhouse, async_insert=True, wait_for_async_insert=True)
    settings = ClickHouseConnectSink(cfg)._settings()
    assert settings["async_insert"] == 1
    assert settings["wait_for_async_insert"] == 1


def test_insert_mode_string_distinguishes_dedup_domains(dev_config):
    """The same rows under different settings is a different request."""
    from dataclasses import replace

    sync = dev_config.clickhouse
    async_wait = replace(sync, async_insert=True, wait_for_async_insert=True)
    assert sync.insert_mode == "sync"
    assert async_wait.insert_mode == "async_wait"
    assert sync.insert_mode != async_wait.insert_mode


def test_sink_never_raises_from_insert_batch(dev_config):
    """The contract is that the outcome is the return value, not an exception."""
    from p05_ingest.model import (
        EventKind,
        EventRow,
        LogicalProjection,
        SealedBatch,
        SealReason,
        TopicPartition,
        TransportCoordinate,
    )

    sink = ClickHouseConnectSink(dev_config.clickhouse)
    projection = LogicalProjection(
        dataset_id="demo",
        symbol="EURUSD",
        sequence=1,
        event_time_ns=1_704_171_845_000_000_000,
        price_scale=100_000,
        kind=EventKind.TICK,
        source_schema_version="marketdata.v1",
        price_ticks=1,
    )
    row = EventRow(
        projection=projection,
        coordinate=TransportCoordinate("c", "gen", "md.ticks.v1", 0, 0),
        payload_digest="0" * 64,
        ingest_run_id="run-1",
    )
    batch = SealedBatch(
        tp=TopicPartition("md.ticks.v1", 0),
        assignment_epoch=1,
        rows=(row,),
        batch_id="b",
        insert_token="t" * 64,
        seal_reason=SealReason.FORCED,
        accounted_bytes=1,
        sealed_at_monotonic=0.0,
    )
    # No server is listening on localhost:8123 here, so this exercises the
    # real driver failure path rather than a mock.
    result = sink.insert_batch(batch)
    assert result.outcome is not SinkOutcome.ACKED
    assert result.detail
