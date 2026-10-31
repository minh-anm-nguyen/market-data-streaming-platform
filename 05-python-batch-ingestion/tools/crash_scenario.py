"""Runnable crash scenario: a real process that a real signal can kill.

Spec 9.6 requires that a crash test prove the process reached the intended
point before dying, and that recovery be checked against what actually
survived. An in-process simulation cannot do that: nothing survives a function
call, and nothing is durable.

So this script gives the two fakes a disk-backed durable layer that stands in
for the only two things that really survive a crash:

  * ``rows.jsonl``   -- the ClickHouse table. Appended and ``fsync``-ed
    *before* the sink returns an ACK, so a crash after the write and before the
    commit leaves exactly the E2E-02 state.
  * ``offsets.json`` -- the Kafka committed offsets. Rewritten and
    ``fsync``-ed only after the broker confirms.

The Kafka *log* is regenerated deterministically from the seed on every run,
which is what makes a restart see the same records at the same offsets.

Usage::

    python tools/crash_scenario.py --state-dir DIR --config config/dev.toml \\
        [--events N] [--seed S]

    P05_CRASH_HOOK=CRASH_04_AFTER_CH_ACK_BEFORE_KAFKA_COMMIT \\
    P05_CRASH_MARKER=DIR/marker P05_CRASH_AFTER_N=1 python tools/crash_scenario.py ...

Exit status: 0 complete, 1 incomplete, 137 killed at the armed hook.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from p05_ingest.clickhouse_sink import SinkInsertResult  # noqa: E402
from p05_ingest.config import AppConfig  # noqa: E402
from p05_ingest.contract_validator import ContractValidator  # noqa: E402
from p05_ingest.crash_hooks import CrashHooks  # noqa: E402
from p05_ingest.engine import IngestEngine  # noqa: E402
from p05_ingest.metrics import MetricsRegistry  # noqa: E402
from p05_ingest.model import CH_INSERT_COLUMNS, SinkOutcome  # noqa: E402
from p05_ingest.quarantine import QuarantinePolicy  # noqa: E402
from p05_ingest.retry_policy import RetryPolicy  # noqa: E402
from p05_ingest.sink_worker import InlineSinkExecutor  # noqa: E402
from p05_ingest.testkit import (  # noqa: E402
    QUOTES_TOPIC,
    TICKS_TOPIC,
    FakeConsumer,
    FakeKafkaCluster,
)
from p05_ingest.testkit.workload import produce_market_workload  # noqa: E402


def _fsync_write(path: Path, payload: bytes) -> None:
    """Write and flush all the way to the device, then fsync the directory.

    Without the directory fsync the file contents can be durable while the
    directory entry is not, which on a crash looks like the write never
    happened (spec 9.6: an unflushed marker is not a durability proof).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        dir_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        # Windows cannot fsync a directory handle opened this way.
        pass


def _fsync_append(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)


class DurableSink:
    """A sink whose rows survive the process. Append-then-ACK, never reversed.

    Block de-duplication is modelled by remembering the tokens already written
    in the same file, so a retry of an identical batch does not duplicate --
    the behaviour a real ClickHouse insert-dedup window provides inside its
    window.
    """

    def __init__(self, rows_path: Path, *, dedup: bool = True) -> None:
        self._path = rows_path
        self._dedup = dedup
        self._tokens = {r["_token"] for r in self.load(rows_path)} if dedup else set()

    @staticmethod
    def load(path: Path) -> list[dict]:
        if not path.exists():
            return []
        out = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                out.append(json.loads(line))
        return out

    def insert_batch(self, batch) -> SinkInsertResult:
        if self._dedup and batch.insert_token in self._tokens:
            return SinkInsertResult(SinkOutcome.ACKED, 0.0, "deduplicated")
        payload = []
        for row in batch.rows:
            values = dict(zip(CH_INSERT_COLUMNS, row.to_ch_tuple(), strict=True))
            values["event_date"] = str(values["event_date"])
            values["_token"] = batch.insert_token
            payload.append(json.dumps(values, sort_keys=True))
        # Durable BEFORE the ACK. This ordering is the whole point: it creates
        # the state where ClickHouse holds the rows and Kafka does not know.
        _fsync_append(self._path, ("\n".join(payload) + "\n").encode("utf-8"))
        self._tokens.add(batch.insert_token)
        return SinkInsertResult(SinkOutcome.ACKED, 0.0, "", rows=len(batch.rows))

    def preflight(self) -> None:
        return None

    def close(self) -> None:
        return None


class DurableOffsetConsumer(FakeConsumer):
    """A consumer whose committed offsets survive the process."""

    def __init__(self, cluster, *, group_id, offsets_path: Path, **kwargs) -> None:
        self._offsets_path = offsets_path
        stored = self._load()
        for key, next_offset in stored.items():
            topic, partition = key.rsplit(":", 1)
            from p05_ingest.model import TopicPartition

            cluster.set_committed(
                group_id, TopicPartition(topic, int(partition)), next_offset
            )
        super().__init__(cluster, group_id=group_id, **kwargs)

    def _load(self) -> dict[str, int]:
        if not self._offsets_path.exists():
            return {}
        return json.loads(self._offsets_path.read_text(encoding="utf-8"))

    def commit_offset(self, tp, next_offset: int) -> None:
        super().commit_offset(tp, next_offset)
        # Persist only after the broker confirmed, never before.
        stored = self._load()
        stored[f"{tp.topic}:{tp.partition}"] = next_offset
        _fsync_write(
            self._offsets_path,
            json.dumps(stored, sort_keys=True, indent=2).encode("utf-8"),
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--config", default="config/dev.toml")
    parser.add_argument("--events", type=int, default=40)
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--max-rows", type=int, default=4)
    parser.add_argument(
        "--no-dedup",
        action="store_true",
        help="disable insert de-duplication, so a replay really duplicates",
    )
    args = parser.parse_args(argv)

    state = Path(args.state_dir)
    rows_path = state / "rows.jsonl"
    offsets_path = state / "offsets.json"

    base = AppConfig.from_toml(args.config)
    from dataclasses import asdict

    raw = {
        "project": asdict(base.project),
        "kafka": asdict(base.kafka),
        "contracts": asdict(base.contracts),
        "batch": {**asdict(base.batch), "per_partition_max_rows": args.max_rows},
        "clickhouse": asdict(base.clickhouse),
        "retry": asdict(base.retry),
        "quarantine": asdict(base.quarantine),
        "shutdown": asdict(base.shutdown),
        "tests": asdict(base.tests),
    }
    cfg = AppConfig.from_dict(raw)

    # The Kafka log is regenerated deterministically, so a restart sees exactly
    # the same records at exactly the same offsets.
    cluster = FakeKafkaCluster(cluster_id=cfg.kafka.cluster_id)
    cluster.create_topic(TICKS_TOPIC, args.partitions)
    cluster.create_topic(QUOTES_TOPIC, args.partitions)
    manifest = produce_market_workload(
        cluster, events=args.events, partitions=args.partitions, seed=args.seed
    )

    crash_hooks = CrashHooks.from_env()
    metrics = MetricsRegistry()
    sink = DurableSink(rows_path, dedup=not args.no_dedup)
    consumer = DurableOffsetConsumer(
        cluster,
        group_id=cfg.kafka.group_id,
        offsets_path=offsets_path,
        auto_offset_reset=cfg.kafka.auto_offset_reset,
    )
    executor = InlineSinkExecutor(
        sink=sink,
        policy=RetryPolicy(cfg.retry),
        metrics=metrics,
        monotonic=_monotonic,
        sleep=lambda _s: None,
        crash_hooks=crash_hooks,
    )
    engine = IngestEngine(
        cfg,
        source=consumer,
        executor=executor,
        metrics=metrics,
        monotonic=_monotonic,
        crash_hooks=crash_hooks,
        quarantine=QuarantinePolicy(
            cfg.quarantine, ingest_run_id=cfg.project.run_id
        ),
        validator=ContractValidator(cfg.contracts, cfg.project),
    )
    engine.start()

    for _ in range(args.events * 8 + 500):
        report = engine.step()
        if report.idle:
            if engine.batcher.open_partitions:
                _advance(cfg.batch.max_wait_ms + 1)
                continue
            break
    status = engine.shutdown()

    stored = DurableSink.load(rows_path)
    identities = {(r["dataset_id"], r["symbol"], r["sequence"]) for r in stored}
    summary = {
        "verdict": status.verdict,
        "reason": status.reason,
        "expected_events": len(manifest),
        "physical_rows": len(stored),
        "logical_identities": len(identities),
        "committed": {
            f"{tp.topic}:{tp.partition}": next_offset
            for tp, next_offset in cluster.all_committed(cfg.kafka.group_id).items()
        },
        "crash_hook_armed": crash_hooks.armed_hook,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if status.complete else 1


#: A monotonic clock that the scenario can advance without sleeping, so a
#: subprocess run finishes in milliseconds rather than seconds.
_NOW = [1000.0]


def _monotonic() -> float:
    return _NOW[0]


def _advance(milliseconds: float) -> None:
    _NOW[0] += milliseconds / 1000.0


if __name__ == "__main__":
    sys.exit(main())
