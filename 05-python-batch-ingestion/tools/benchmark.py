"""Capacity measurement with a run manifest, and no borrowed claims.

Spec 10.4 and NFR-11. Two rules the script enforces rather than documents:

  * **a requested rate is not an achieved rate.** Everything reported is work
    that completed: rows the sink accepted and offsets the broker confirmed.
    The requested figure appears only as the label of a ladder rung.
  * **every number carries its manifest.** Rate, partition count, batch caps,
    config fingerprint, descriptor hash, platform and sink type are written
    beside the measurement, because a throughput figure without them cannot be
    compared with anything.

The sink type matters more than everything else here. Against the in-memory
sink there is no network, no MergeTree and no merge pressure, so the figure
measures the Python pipeline alone. It is NOT evidence for any events/s claim
against real Kafka and real ClickHouse, and the report says so in the file.

Usage::

    python tools/benchmark.py --rates 1000,10000 --events 20000
    python tools/benchmark.py --rates 1000 --out artifacts/bench.json
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from p05_ingest.config import AppConfig  # noqa: E402
from p05_ingest.metrics import CH_INSERT_LATENCY  # noqa: E402
from p05_ingest.testkit import FakeClock  # noqa: E402
from p05_ingest.testkit.workload import produce_market_workload  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEV_CONFIG = PROJECT_ROOT / "config" / "dev.toml"


@dataclass
class RungResult:
    requested_events_per_s: int
    events_produced: int
    #: Rows the sink actually accepted. The only throughput number that counts.
    rows_written: int
    logical_identities: int
    insert_attempts: int
    active_parts: int
    wall_seconds: float
    achieved_events_per_s: float
    peak_accounted_bytes: int
    peak_rss_bytes: int | None
    p95_insert_latency_s: float
    verdict: str
    backlog_remaining: int


def _rss() -> int | None:
    try:
        import psutil
    except ImportError:
        return None
    return psutil.Process().memory_info().rss


def run_rung(
    *,
    requested_rate: int,
    events: int,
    partitions: int,
    max_rows: int,
    seed: int,
) -> RungResult:
    from tests.conftest import build_harness, make_config

    base = AppConfig.from_toml(DEV_CONFIG)
    cfg = make_config(
        base,
        batch={"per_partition_max_rows": max_rows, "max_inflight_batches_global": 4},
    )
    harness = build_harness(cfg, clock=FakeClock(), partitions=partitions)
    manifest = produce_market_workload(
        harness.cluster, events=events, partitions=partitions, seed=seed
    )

    peak_bytes = 0
    peak_rss = _rss() or 0
    started = time.perf_counter()
    steps = 0
    while steps < events * 8 + 1000:
        report = harness.engine.step()
        steps += 1
        peak_bytes = max(peak_bytes, harness.engine.budget.reserved_bytes)
        if steps % 200 == 0:
            peak_rss = max(peak_rss, _rss() or 0)
        if report.idle:
            if harness.engine.batcher.open_partitions:
                harness.tick_timer()
                continue
            break
    status = harness.engine.shutdown()
    wall = time.perf_counter() - started
    peak_rss = max(peak_rss, _rss() or 0)

    hist = harness.metrics.histogram(CH_INSERT_LATENCY)
    backlog = sum(
        log.high_watermark - (harness.cluster.committed(cfg.kafka.group_id, tp) or 0)
        for tp, log in zip(
            harness.cluster.end_offsets(),
            [harness.cluster.log(tp) for tp in harness.cluster.end_offsets()],
            strict=True,
        )
    )

    return RungResult(
        requested_events_per_s=requested_rate,
        events_produced=len(manifest),
        rows_written=harness.sink.row_count,
        logical_identities=len(harness.logical_identities()),
        insert_attempts=len(harness.sink.attempts),
        active_parts=harness.sink.active_parts,
        wall_seconds=round(wall, 3),
        achieved_events_per_s=round(harness.sink.row_count / wall, 1) if wall else 0.0,
        peak_accounted_bytes=peak_bytes,
        peak_rss_bytes=peak_rss or None,
        p95_insert_latency_s=round(hist.quantile(0.95), 6) if hist else 0.0,
        verdict=status.verdict,
        backlog_remaining=backlog,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rates",
        default="1000,10000",
        help=(
            "comma-separated REQUESTED events/s ladder. These label the rungs; "
            "the achieved rate is measured, never assumed"
        ),
    )
    parser.add_argument("--events", type=int, default=20_000)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--max-rows", type=int, default=500)
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--out", default=None)
    args = parser.parse_args(argv)

    rates = [int(r) for r in args.rates.split(",") if r.strip()]
    cfg = AppConfig.from_toml(DEV_CONFIG)

    from p05_ingest.proto_schema import descriptor_sha256

    manifest = {
        "run_manifest": {
            "sink": "in-memory FakeClickHouse (no network, no MergeTree)",
            "source": "in-memory FakeKafkaCluster (no broker, no fetch queue)",
            "partitions_per_topic": args.partitions,
            "per_partition_max_rows": args.max_rows,
            "events_per_rung": args.events,
            "seed": args.seed,
            "config_fingerprint": cfg.fingerprint(),
            "descriptor_sha256": descriptor_sha256(),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "processor": platform.processor(),
        },
        "caveat": (
            "These figures measure the Python pipeline against in-memory fakes. "
            "The ClickHouse insert round trip, MergeTree merge pressure and the "
            "librdkafka fetch path are all absent, and they dominate production "
            "throughput. This is NOT evidence for any events/s claim against "
            "real infrastructure (spec 10.4). CAPACITY_GO remains NOT_RUN."
        ),
        "rungs": [],
    }

    print(f"{'requested':>10} {'achieved':>10} {'rows':>8} {'logical':>8} "
          f"{'inserts':>8} {'parts':>6} {'peak_buf':>10} {'verdict':>10}")
    for rate in rates:
        result = run_rung(
            requested_rate=rate,
            events=args.events,
            partitions=args.partitions,
            max_rows=args.max_rows,
            seed=args.seed,
        )
        manifest["rungs"].append(asdict(result))
        print(
            f"{result.requested_events_per_s:>10} "
            f"{result.achieved_events_per_s:>10.0f} "
            f"{result.rows_written:>8} {result.logical_identities:>8} "
            f"{result.insert_attempts:>8} {result.active_parts:>6} "
            f"{result.peak_accounted_bytes:>10} {result.verdict:>10}"
        )
        if result.backlog_remaining > 0:
            print(
                f"           WARNING: {result.backlog_remaining} offsets of "
                "backlog remained; this rung did not keep up"
            )

    print()
    print(manifest["caveat"])

    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
        )
        print(f"\nmanifest: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
