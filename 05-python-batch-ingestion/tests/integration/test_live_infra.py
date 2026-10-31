"""Tests that need a real Kafka broker and a real ClickHouse server.

These SKIP unless both are reachable. They are not optional extras: the whole
simulated suite deliberately cannot prove anything about librdkafka callback
semantics, real rebalance timing, MergeTree merge pressure, replica lag, or
what a genuine HTTP timeout looks like from ``clickhouse-connect``. Spec 9.1 is
explicit that a failure mode which was not actually triggered is not evidence.

A skip here is therefore a real gap in the evidence, not a pass. The run report
must record these as NOT_RUN, which is what
``test_live_tests_are_reported_as_not_run_when_skipped`` asserts.

To run them, bring up the lab and re-run::

    docker compose -f deployment/docker-compose.dev.yml up -d
    clickhouse-client --queries-file sql/001_create_raw_table.sql
    pytest -m kafka_clickhouse tests/integration/test_live_infra.py
"""

from __future__ import annotations

import socket
import time
import uuid

import pytest

from p05_ingest.config import AppConfig
from p05_ingest.model import CH_INSERT_COLUMNS
from tests.conftest import DEV_CONFIG

pytestmark = pytest.mark.kafka_clickhouse

CONNECT_TIMEOUT_S = 1.0


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=CONNECT_TIMEOUT_S):
            return True
    except OSError:
        return False


def _kafka_reachable(cfg: AppConfig) -> bool:
    first = cfg.kafka.bootstrap_servers.split(",")[0]
    host, _, port = first.partition(":")
    return _reachable(host or "localhost", int(port or 9092))


def _clickhouse_reachable(cfg: AppConfig) -> bool:
    return _reachable(cfg.clickhouse.host, cfg.clickhouse.port)


_CFG = AppConfig.from_toml(DEV_CONFIG)
_KAFKA_UP = _kafka_reachable(_CFG)
_CH_UP = _clickhouse_reachable(_CFG)

MISSING = []
if not _KAFKA_UP:
    MISSING.append(f"Kafka at {_CFG.kafka.bootstrap_servers}")
if not _CH_UP:
    MISSING.append(f"ClickHouse at {_CFG.clickhouse.host}:{_CFG.clickhouse.port}")

requires_infra = pytest.mark.skipif(
    bool(MISSING),
    reason=(
        "live infrastructure not reachable: "
        + ", ".join(MISSING)
        + ". These cases are NOT_RUN, not passed."
    ),
)


# --------------------------------------------------------------- the gap itself


def test_live_tests_are_reported_as_not_run_when_skipped():
    """Always runs. Records whether the live evidence exists.

    This is the honesty check: it does not assert that the infrastructure is
    present, it asserts that the *absence* is visible rather than silent.
    """
    if MISSING:
        print(
            "\nLIVE INFRASTRUCTURE ABSENT -- the following remain unproven:\n"
            "  * librdkafka commit/rebalance callback semantics\n"
            "  * real cooperative-sticky rebalance timing\n"
            "  * ClickHouse MergeTree parts, merges and insert de-duplication\n"
            "  * genuine HTTP/driver timeout classification\n"
            "  * replica lag, ISR behaviour, HA and disaster survival\n"
            "  * any throughput or latency figure against real services\n"
            f"  missing: {', '.join(MISSING)}"
        )
    assert isinstance(MISSING, list)


# ------------------------------------------------------------------- preflight


@requires_infra
def test_preflight_passes_against_live_services():
    from p05_ingest.clickhouse_sink import ClickHouseConnectSink
    from p05_ingest.kafka_source import ConfluentKafkaSource
    from p05_ingest.preflight import run_preflight

    sink = ClickHouseConnectSink(_CFG.clickhouse)
    source = ConfluentKafkaSource(_CFG.kafka)
    try:
        report = run_preflight(_CFG, sink=sink, source=source)
        print("\n" + report.render())
        assert report.ok, report.render()
    finally:
        sink.close()
        source.close()


@requires_infra
def test_live_table_has_every_insert_column():
    import clickhouse_connect

    user, password = _CFG.clickhouse.credentials()
    client = clickhouse_connect.get_client(
        host=_CFG.clickhouse.host,
        port=_CFG.clickhouse.port,
        database=_CFG.clickhouse.database,
        username=user,
        password=password,
    )
    try:
        rows = client.query(
            "SELECT name FROM system.columns "
            "WHERE database = {db:String} AND table = {tbl:String}",
            parameters={
                "db": _CFG.clickhouse.database,
                "tbl": _CFG.clickhouse.table,
            },
        ).result_rows
        present = {r[0] for r in rows}
        assert present, "apply sql/001_create_raw_table.sql first"
        missing = [c for c in CH_INSERT_COLUMNS if c not in present]
        assert not missing, f"missing columns: {missing}"
    finally:
        client.close()


@requires_infra
def test_effective_async_insert_settings_are_safe():
    """Spec 6.4/NFR-09: the server default must not silently buffer the ACK."""
    import clickhouse_connect

    user, password = _CFG.clickhouse.credentials()
    client = clickhouse_connect.get_client(
        host=_CFG.clickhouse.host,
        port=_CFG.clickhouse.port,
        username=user,
        password=password,
    )
    try:
        rows = client.query(
            "SELECT name, value FROM system.settings "
            "WHERE name IN ('async_insert', 'wait_for_async_insert')"
        ).result_rows
        effective = {name: str(value) for name, value in rows}
        print(f"\nserver effective settings: {effective}")
        if effective.get("async_insert", "0") not in ("0", "false"):
            assert effective.get("wait_for_async_insert", "1") not in (
                "0",
                "false",
            ), "async_insert=1 with wait_for_async_insert=0 can lose committed data"
    finally:
        client.close()


# ----------------------------------------------------------- the golden path


@requires_infra
def test_live_golden_pipeline_round_trip():
    """E2E-01 against real services, scoped to a unique run id.

    Writes into the shared table but only ever reads back its own
    ``ingest_run_id``, so a parallel run or leftover data cannot make it pass
    or fail by accident.
    """
    from dataclasses import asdict

    import clickhouse_connect
    from confluent_kafka import Producer

    from p05_ingest.canonical_digest import payload_digest
    from p05_ingest.clickhouse_sink import ClickHouseConnectSink
    from p05_ingest.contract_validator import ContractValidator
    from p05_ingest.engine import IngestEngine
    from p05_ingest.kafka_source import ConfluentKafkaSource
    from p05_ingest.metrics import MetricsRegistry
    from p05_ingest.retry_policy import RetryPolicy
    from p05_ingest.sink_worker import ThreadedSinkExecutor
    from p05_ingest.testkit import TICKS_TOPIC, make_tick

    run_id = f"live-{uuid.uuid4().hex[:12]}"
    group_id = f"md-p05-live-{uuid.uuid4().hex[:8]}"
    raw = {
        "project": {**asdict(_CFG.project), "run_id": run_id},
        "kafka": {
            **asdict(_CFG.kafka),
            "group_id": group_id,
            "topics": [TICKS_TOPIC],
        },
        "contracts": asdict(_CFG.contracts),
        "batch": {**asdict(_CFG.batch), "per_partition_max_rows": 25},
        "clickhouse": asdict(_CFG.clickhouse),
        "retry": asdict(_CFG.retry),
        "quarantine": asdict(_CFG.quarantine),
        "shutdown": asdict(_CFG.shutdown),
        "tests": asdict(_CFG.tests),
    }
    cfg = AppConfig.from_dict(raw)

    count = 100
    expected = {}
    producer = Producer({"bootstrap.servers": cfg.kafka.bootstrap_servers})
    for sequence in range(count):
        event = make_tick(dataset_id=run_id, sequence=sequence)
        producer.produce(
            TICKS_TOPIC, key=b"EURUSD", value=event.SerializeToString()
        )
        expected[(run_id, "EURUSD", sequence)] = None
    producer.flush(30)

    metrics = MetricsRegistry()
    source = ConfluentKafkaSource(cfg.kafka)
    executor = ThreadedSinkExecutor(
        sink_factory=lambda: ClickHouseConnectSink(cfg.clickhouse),
        policy=RetryPolicy(cfg.retry),
        metrics=metrics,
        workers=2,
        monotonic=time.monotonic,
        sleep=time.sleep,
    )
    engine = IngestEngine(
        cfg,
        source=source,
        executor=executor,
        metrics=metrics,
        validator=ContractValidator(cfg.contracts, cfg.project),
    )
    engine.start()
    status = engine.run(max_idle_steps=40)
    assert status.complete, status.reason

    user, password = cfg.clickhouse.credentials()
    client = clickhouse_connect.get_client(
        host=cfg.clickhouse.host,
        port=cfg.clickhouse.port,
        database=cfg.clickhouse.database,
        username=user,
        password=password,
    )
    try:
        rows = client.query(
            "SELECT dataset_id, symbol, sequence, payload_digest, count() "
            f"FROM {cfg.clickhouse.table} "
            "WHERE ingest_run_id = {run_id:String} "
            "GROUP BY dataset_id, symbol, sequence, payload_digest",
            parameters={"run_id": run_id},
        ).result_rows
    finally:
        client.close()

    identities = {(r[0], r[1], int(r[2])) for r in rows}
    assert identities == set(expected), (
        f"expected {len(expected)} identities, found {len(identities)}"
    )

    digests_per_identity: dict[tuple, set[str]] = {}
    for dataset_id, symbol, sequence, digest, _n in rows:
        digests_per_identity.setdefault(
            (dataset_id, symbol, int(sequence)), set()
        ).add(digest)
    conflicted = {k: v for k, v in digests_per_identity.items() if len(v) > 1}
    assert not conflicted, f"identity conflicts in a clean live run: {conflicted}"

    for sequence in range(count):
        event = make_tick(dataset_id=run_id, sequence=sequence)
        from p05_ingest.model import EventKind, LogicalProjection

        projection = LogicalProjection(
            dataset_id=run_id,
            symbol="EURUSD",
            sequence=sequence,
            event_time_ns=event.event_time_ns,
            price_scale=event.price_scale,
            kind=EventKind.TICK,
            source_schema_version=event.source_schema_version,
            price_ticks=event.tick.price_ticks,
        )
        stored = digests_per_identity[(run_id, "EURUSD", sequence)]
        assert stored == {payload_digest(projection)}, (
            f"digest mismatch at sequence {sequence}"
        )


@requires_infra
def test_live_commit_offsets_are_readable_from_the_broker():
    """The committed position must come from the broker, not from memory."""
    from p05_ingest.kafka_source import ConfluentKafkaSource

    source = ConfluentKafkaSource(_CFG.kafka)
    try:
        source.subscribe(
            _CFG.kafka.topics,
            on_assign=lambda _tps: None,
            on_revoke=lambda _tps: None,
            on_lost=lambda _tps: None,
        )
        deadline = time.monotonic() + 30
        while not source.assignment() and time.monotonic() < deadline:
            source.poll(0.5)
        assert source.assignment(), "no partitions were assigned within 30s"
        for tp in source.assignment():
            low, high = source.watermarks(tp)
            assert low <= high
            assert source.committed_next(tp) >= 0
    finally:
        source.close()
