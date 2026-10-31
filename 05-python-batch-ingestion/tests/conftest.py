"""Shared fixtures.

The central one is ``harness``: a fully wired engine over the deterministic
in-memory cluster, with a manual clock. Every integration test drives it by
stepping, so a scenario reads as a sequence of events rather than a sleep.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from p05_ingest.config import AppConfig
from p05_ingest.contract_validator import ContractValidator
from p05_ingest.engine import IngestEngine
from p05_ingest.metrics import MetricsRegistry
from p05_ingest.model import TopicPartition
from p05_ingest.quarantine import QuarantinePolicy
from p05_ingest.retry_policy import RetryPolicy
from p05_ingest.sink_worker import InlineSinkExecutor
from p05_ingest.testkit import (
    QUOTES_TOPIC,
    TICKS_TOPIC,
    Behaviour,
    FakeClickHouse,
    FakeClock,
    FakeConsumer,
    FakeKafkaCluster,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEV_CONFIG = PROJECT_ROOT / "config" / "dev.toml"


@pytest.fixture
def dev_config() -> AppConfig:
    """The shipped dev profile, loaded from disk.

    Loading the real file rather than a hand-built object means a change to
    config/dev.toml that breaks a gate shows up here.
    """
    return AppConfig.from_toml(DEV_CONFIG)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def make_config(base: AppConfig, **overrides) -> AppConfig:
    """Rebuild a config with section-level overrides.

    Used by tests that need, say, a 3-row batch cap. Goes through
    ``AppConfig.from_dict`` so every validation gate still applies -- a test
    cannot construct a configuration the program would refuse to start with.
    """
    from dataclasses import asdict

    raw = {
        "project": asdict(base.project),
        "kafka": asdict(base.kafka),
        "contracts": asdict(base.contracts),
        "batch": asdict(base.batch),
        "clickhouse": asdict(base.clickhouse),
        "retry": asdict(base.retry),
        "quarantine": asdict(base.quarantine),
        "shutdown": asdict(base.shutdown),
        "tests": asdict(base.tests),
    }
    for section, values in overrides.items():
        raw[section].update(values)
    return AppConfig.from_dict(raw)


@dataclass
class Harness:
    """Engine plus every fake it is wired to, for assertions."""

    cfg: AppConfig
    clock: FakeClock
    cluster: FakeKafkaCluster
    consumer: FakeConsumer
    sink: FakeClickHouse
    metrics: MetricsRegistry
    engine: IngestEngine
    executor: InlineSinkExecutor
    manifest: list = field(default_factory=list)

    # ------------------------------------------------------------- driving

    def step(self, n: int = 1):
        reports = [self.engine.step() for _ in range(n)]
        return reports[-1] if n == 1 else reports

    def tick_timer(self) -> None:
        """Advance past ``max_wait_ms`` so age-based flushes fire."""
        self.clock.advance_ms(self.cfg.batch.max_wait_ms + 1)

    def drain(self, *, max_steps: int = 2000):
        """Step until nothing changes, nudging the flush timer when idle.

        Returns the number of steps taken. Raises if the engine never settles,
        which would mean a liveness bug rather than a slow test.
        """
        steps = 0
        idle_rounds = 0
        while steps < max_steps:
            report = self.engine.step()
            steps += 1
            if report.did_work:
                idle_rounds = 0
                continue
            if self.engine.batcher.open_partitions:
                self.tick_timer()
                idle_rounds = 0
                continue
            idle_rounds += 1
            if idle_rounds >= 2:
                return steps
        raise AssertionError(
            f"engine did not settle within {max_steps} steps; "
            f"open={sorted(self.engine.batcher.open_partitions)} "
            f"inflight={self.executor.inflight} "
            f"blocked={self.engine.blocked_partitions}"
        )

    def finish(self):
        self.drain()
        return self.engine.shutdown()

    # ---------------------------------------------------------- inspection

    def committed(self) -> dict[TopicPartition, int]:
        return self.cluster.all_committed(self.cfg.kafka.group_id)

    def stored_identities(self) -> list[tuple[str, str, int]]:
        return self.sink.identities()

    def logical_identities(self) -> set[tuple[str, str, int]]:
        return set(self.sink.identities())


def build_harness(
    cfg: AppConfig,
    *,
    clock: FakeClock | None = None,
    partitions: int = 2,
    sink_script: tuple[Behaviour, ...] = (),
    sink_default: Behaviour = Behaviour.ACK,
    dedup_window: int = 1000,
    evict_dedup_after_attempt: int | None = None,
    dlq_publisher=None,
    assign_on_subscribe: bool = True,
    start: bool = True,
) -> Harness:
    clock = clock or FakeClock()
    cluster = FakeKafkaCluster(cluster_id=cfg.kafka.cluster_id)
    cluster.create_topic(TICKS_TOPIC, partitions)
    cluster.create_topic(QUOTES_TOPIC, partitions)

    consumer = FakeConsumer(
        cluster,
        group_id=cfg.kafka.group_id,
        auto_offset_reset=cfg.kafka.auto_offset_reset,
        assign_on_subscribe=assign_on_subscribe,
    )
    sink = FakeClickHouse(
        script=sink_script,
        default_behaviour=sink_default,
        dedup_window=dedup_window,
        evict_dedup_after_attempt=evict_dedup_after_attempt,
    )
    metrics = MetricsRegistry()
    executor = InlineSinkExecutor(
        sink=sink,
        policy=RetryPolicy(cfg.retry),
        metrics=metrics,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    quarantine = QuarantinePolicy(
        cfg.quarantine, ingest_run_id=cfg.project.run_id, publisher=dlq_publisher
    )
    engine = IngestEngine(
        cfg,
        source=consumer,
        executor=executor,
        metrics=metrics,
        monotonic=clock.monotonic,
        quarantine=quarantine,
        validator=ContractValidator(cfg.contracts, cfg.project),
    )
    harness = Harness(
        cfg=cfg,
        clock=clock,
        cluster=cluster,
        consumer=consumer,
        sink=sink,
        metrics=metrics,
        engine=engine,
        executor=executor,
    )
    if start:
        engine.start()
    return harness


@pytest.fixture
def harness(dev_config, clock) -> Harness:
    return build_harness(dev_config, clock=clock)


@pytest.fixture
def validator(dev_config) -> ContractValidator:
    return ContractValidator(dev_config.contracts, dev_config.project)


@pytest.fixture
def tp_tick0() -> TopicPartition:
    return TopicPartition(TICKS_TOPIC, 0)


@pytest.fixture
def tp_quote0() -> TopicPartition:
    return TopicPartition(QUOTES_TOPIC, 0)
