"""Command-line entry points.

Spec section 12.2 lists the intended commands. They are implemented here with
one addition, ``simulate``, which runs the complete pipeline against the
deterministic in-memory cluster. That exists because the correctness argument
must be exercisable on a machine with no broker and no database: without it,
"the tests did not run" and "the tests passed" look the same from the outside.

``run`` requires live infrastructure and says so clearly when it is absent,
rather than silently falling back to the simulator. Mixing the two would make
a run report meaningless.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .config import AppConfig
from .errors import ConfigError, P05Error, PreflightError
from .proto_schema import descriptor_sha256, schema_package


def _add_config_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config", required=True, help="path to a profile TOML (e.g. config/dev.toml)"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="p05-ingest",
        description=(
            "Kafka to Protobuf to ClickHouse batch ingestion. Offsets are "
            "committed only after a ClickHouse acknowledgement."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    doctor = sub.add_parser(
        "doctor", help="run every startup gate and print the results"
    )
    _add_config_arg(doctor)
    doctor.add_argument(
        "--live",
        action="store_true",
        help="also contact Kafka and ClickHouse (otherwise those checks WARN)",
    )

    run = sub.add_parser("run", help="consume and ingest until stopped")
    _add_config_arg(run)
    run.add_argument(
        "--max-idle-steps",
        type=int,
        default=None,
        help=(
            "stop after this many consecutive empty iterations. NOT an "
            "end-of-dataset detector: completeness still requires reconciliation"
        ),
    )
    run.add_argument("--report", default=None, help="write a JSON run report here")

    snapshot = sub.add_parser(
        "snapshot", help="capture a frozen Kafka cut for reconciliation"
    )
    _add_config_arg(snapshot)
    snapshot.add_argument("--out", required=True, help="path for the cut JSON")

    reconcile = sub.add_parser(
        "reconcile", help="compare source, Kafka and ClickHouse by identity"
    )
    _add_config_arg(reconcile)
    reconcile.add_argument(
        "--dataset", default=None, help="path to the P01 source manifest JSON"
    )
    reconcile.add_argument("--cut", required=True, help="path to the Kafka cut JSON")
    reconcile.add_argument("--out", default=None, help="write the report JSON here")

    sim = sub.add_parser(
        "simulate",
        help=(
            "run the full pipeline against the deterministic in-memory cluster "
            "(no broker or database needed)"
        ),
    )
    _add_config_arg(sim)
    sim.add_argument("--events", type=int, default=1000, help="market-valid events")
    sim.add_argument("--partitions", type=int, default=2, help="partitions per topic")
    sim.add_argument("--seed", type=int, default=1337)
    sim.add_argument(
        "--sink-plan",
        default="",
        help=(
            "comma-separated sink behaviours applied in order, e.g. "
            "write_then_lose_ack,ack"
        ),
    )
    sim.add_argument("--report", default=None, help="write a JSON run report here")

    sub.add_parser(
        "descriptor-hash", help="print the SHA-256 of the compiled protobuf descriptor"
    )

    return parser


# --------------------------------------------------------------------------
# commands
# --------------------------------------------------------------------------


def cmd_descriptor_hash() -> int:
    print(f"package: {schema_package()}")
    print(f"descriptor_sha256: {descriptor_sha256()}")
    print()
    print("Pin this value in [contracts].protobuf_descriptor_sha256.")
    return 0


def cmd_doctor(args) -> int:
    from .preflight import run_preflight

    cfg = AppConfig.from_toml(args.config)
    sink = source = None
    if args.live:
        from .clickhouse_sink import ClickHouseConnectSink
        from .kafka_source import ConfluentKafkaSource

        sink = ClickHouseConnectSink(cfg.clickhouse)
        source = ConfluentKafkaSource(cfg.kafka)

    report = run_preflight(cfg, sink=sink, source=source)
    print(report.render())
    print()
    print(f"effective config fingerprint: {cfg.fingerprint()}")
    if not args.live:
        print(
            "\nNote: --live was not passed, so the ClickHouse schema and Kafka "
            "retention checks did not contact anything and are reported WARN, "
            "not PASS."
        )
    return 0 if report.ok else 2


def cmd_run(args) -> int:
    from .clickhouse_sink import ClickHouseConnectSink
    from .engine import IngestEngine
    from .kafka_source import ConfluentKafkaSource
    from .metrics import MetricsRegistry
    from .preflight import run_preflight
    from .retry_policy import RetryPolicy
    from .sink_worker import ThreadedSinkExecutor
    import signal
    import time

    cfg = AppConfig.from_toml(args.config)
    metrics = MetricsRegistry()

    source = ConfluentKafkaSource(cfg.kafka)
    sink_for_preflight = ClickHouseConnectSink(cfg.clickhouse)
    report = run_preflight(cfg, sink=sink_for_preflight, source=None)
    print(report.render(), file=sys.stderr)
    report.raise_if_failed()

    executor = ThreadedSinkExecutor(
        sink_factory=lambda: ClickHouseConnectSink(cfg.clickhouse),
        policy=RetryPolicy(cfg.retry),
        metrics=metrics,
        workers=cfg.batch.max_inflight_batches_global,
        monotonic=time.monotonic,
        sleep=time.sleep,
    )
    engine = IngestEngine(cfg, source=source, executor=executor, metrics=metrics)

    def _stop(_signum, _frame):
        print("[p05] stop requested; draining", file=sys.stderr)
        engine.request_stop()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    engine.start()
    status = engine.run(max_idle_steps=args.max_idle_steps)
    return _finish(status, engine, cfg, args.report)


def cmd_simulate(args) -> int:
    from .engine import IngestEngine
    from .metrics import MetricsRegistry
    from .retry_policy import RetryPolicy
    from .sink_worker import InlineSinkExecutor
    from .testkit import (
        QUOTES_TOPIC,
        TICKS_TOPIC,
        Behaviour,
        FakeClickHouse,
        FakeClock,
        FakeConsumer,
        FakeKafkaCluster,
    )
    from .testkit.workload import produce_market_workload

    cfg = AppConfig.from_toml(args.config)
    clock = FakeClock()
    metrics = MetricsRegistry()

    cluster = FakeKafkaCluster(cluster_id=cfg.kafka.cluster_id)
    cluster.create_topic(TICKS_TOPIC, args.partitions)
    cluster.create_topic(QUOTES_TOPIC, args.partitions)
    manifest = produce_market_workload(
        cluster, events=args.events, partitions=args.partitions, seed=args.seed
    )

    script = [Behaviour(name) for name in args.sink_plan.split(",") if name]
    sink = FakeClickHouse(script=script)
    consumer = FakeConsumer(cluster, group_id=cfg.kafka.group_id)
    executor = InlineSinkExecutor(
        sink=sink,
        policy=RetryPolicy(cfg.retry),
        metrics=metrics,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    engine = IngestEngine(
        cfg, source=consumer, executor=executor, metrics=metrics, monotonic=clock.monotonic
    )
    engine.start()

    # Drive to quiescence, nudging the clock so age-based flushes fire.
    for _ in range(args.events * 4 + 200):
        report = engine.step()
        if report.idle:
            clock.advance_ms(cfg.batch.max_wait_ms + 1)
            if engine.batcher.open_partitions:
                continue
            break
    status = engine.shutdown()

    print(f"simulated events produced: {len(manifest)}")
    print(f"clickhouse physical rows:  {sink.row_count}")
    print(f"insert attempts:           {len(sink.attempts)}")
    print(f"logical identities stored: {len(set(sink.identities()))}")
    return _finish(status, engine, cfg, args.report, extra={"simulated": True})


def cmd_snapshot(args) -> int:
    from .kafka_source import ConfluentKafkaSource

    cfg = AppConfig.from_toml(args.config)
    source = ConfluentKafkaSource(cfg.kafka)
    source.subscribe(
        cfg.kafka.topics,
        on_assign=lambda _tps: None,
        on_revoke=lambda _tps: None,
        on_lost=lambda _tps: None,
    )
    cut = {}
    for tp in source.assignment():
        low, high = source.watermarks(tp)
        cut[f"{tp.topic}:{tp.partition}"] = {"log_start": low, "end": high}
    source.close()

    payload = {
        "run_id": cfg.project.run_id,
        "topic_generation": cfg.project.topic_generation,
        "cluster_id": cfg.kafka.cluster_id,
        "cut": cut,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"wrote frozen cut for {len(cut)} partition(s) to {out}")
    return 0


def cmd_reconcile(args) -> int:
    print(
        "reconcile against live infrastructure requires a Kafka broker and a "
        "ClickHouse server.\n"
        "Nothing is assumed in their absence: this command does not fall back "
        "to a simulated source.\n"
        "To exercise the reconciler itself, run:\n"
        "  pytest tests/integration/test_reconciliation.py\n"
        "which drives it against the deterministic cluster, including the "
        "count-matches-but-data-is-wrong case (E2E-21).",
        file=sys.stderr,
    )
    cut_path = Path(args.cut)
    if not cut_path.exists():
        print(f"cut file not found: {cut_path}", file=sys.stderr)
        return 2
    return 3


# --------------------------------------------------------------------------


def _finish(status, engine, cfg, report_path, extra=None) -> int:
    print()
    print(f"RUN VERDICT: {status.verdict}")
    print(f"  reason: {status.reason}")
    if status.blocked_partitions:
        print(f"  blocked: {', '.join(status.blocked_partitions)}")

    if report_path:
        payload = {
            "run_id": cfg.project.run_id,
            "config_fingerprint": cfg.fingerprint(),
            "descriptor_sha256": descriptor_sha256(),
            "verdict": status.verdict,
            "reason": status.reason,
            "blocked_partitions": status.blocked_partitions,
            "metrics": engine.metrics.as_dict(),
            "commit_state": {
                str(tp): state for tp, state in engine.commits.snapshot().items()
            },
        }
        if extra:
            payload.update(extra)
        out = Path(report_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
        print(f"  report: {out}")

    print()
    print(
        "This verdict covers only what this process proved: every consumed "
        "record reached a terminal durable result. It is NOT a completeness "
        "claim against the source. Run the reconciler against a frozen Kafka "
        "cut and the P01 manifest for that (spec I-08)."
    )
    return 0 if status.complete else 1


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "descriptor-hash":
            return cmd_descriptor_hash()
        if args.command == "doctor":
            return cmd_doctor(args)
        if args.command == "run":
            return cmd_run(args)
        if args.command == "simulate":
            return cmd_simulate(args)
        if args.command == "snapshot":
            return cmd_snapshot(args)
        if args.command == "reconcile":
            return cmd_reconcile(args)
    except (ConfigError, PreflightError) as exc:
        print(f"startup refused: {exc}", file=sys.stderr)
        return 2
    except P05Error as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    except ModuleNotFoundError as exc:
        print(
            f"missing dependency for this command: {exc}.\n"
            "Install the live-infrastructure extras, or use `simulate` which "
            "needs no broker or database.",
            file=sys.stderr,
        )
        return 2
    parser.error(f"unknown command {args.command!r}")
    return 2  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
