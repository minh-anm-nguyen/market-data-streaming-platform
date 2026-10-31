"""The independent verifier: read Kafka and ClickHouse, then judge.

Spec 9.2. This is deliberately a *separate program* from the ingester. It
shares no state with it, uses its own read-only Kafka consumer and its own
ClickHouse client, and never reads the engine counters. A bug in the ingester
must not be able to vouch for itself.

What it compares, per identity rather than by count::

    E = expected identities from the P01 manifest (optional)
    K = identities observed in Kafka up to a frozen cut
    D = identities observed in the ClickHouse read-back

and it reports MISSING / EXTRA / DUPLICATE / CONFLICT plus a verdict. Counts
never decide anything: the canonical trap is one event missing and another
duplicated, where count(*) matches exactly (spec E2E-21).

Usage::

    python tools/verifier.py --config config/dev.toml \\
        --cut artifacts/kafka_cut.json \\
        [--manifest fixtures/golden/manifest.json] \\
        [--dataset demo-a1b2c3] [--out artifacts/reconciliation.json]

Exit status: 0 GO, 1 NO-GO, 2 INCONCLUSIVE, 3 could not gather the evidence.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from p05_ingest.canonical_digest import payload_digest  # noqa: E402
from p05_ingest.config import AppConfig  # noqa: E402
from p05_ingest.contract_validator import ContractValidator, ValidationOk  # noqa: E402
from p05_ingest.model import ConsumedRecord, EventIdentity, TopicPartition  # noqa: E402
from p05_ingest.reconciler import (  # noqa: E402
    EvidenceGaps,
    ObservedEvent,
    Verdict,
    observed_from_sink_rows,
    reconcile,
    verify_stored_digests,
)

EXIT_BY_VERDICT = {
    Verdict.GO: 0,
    Verdict.NO_GO: 1,
    Verdict.INCONCLUSIVE: 2,
    Verdict.NOT_RUN: 2,
}


def load_cut(path: Path) -> dict[TopicPartition, int]:
    """Load a frozen Kafka cut written by ``p05-ingest snapshot``."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    cut: dict[TopicPartition, int] = {}
    for key, bounds in payload.get("cut", {}).items():
        topic, _, partition = key.rpartition(":")
        cut[TopicPartition(topic, int(partition))] = int(bounds["end"])
    if not cut:
        raise ValueError(f"{path} contains no partition bounds")
    return cut


def load_manifest(path: Path) -> list[ObservedEvent]:
    """Load the P01 expected set.

    Accepts either a list of records or ``{"events": [...]}``. Each entry needs
    ``dataset_id``, ``symbol``, ``sequence`` and ``payload_digest``. A manifest
    without digests could only support presence checks, not field equality, so
    this refuses it rather than silently weakening the oracle.
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    entries = payload["events"] if isinstance(payload, dict) else payload
    out = []
    for entry in entries:
        missing = [
            k
            for k in ("dataset_id", "symbol", "sequence", "payload_digest")
            if k not in entry
        ]
        if missing:
            raise ValueError(
                f"manifest entry is missing {missing}; a manifest without "
                "payload_digest cannot support field-level reconciliation"
            )
        out.append(
            ObservedEvent(
                identity=EventIdentity(
                    entry["dataset_id"], entry["symbol"], int(entry["sequence"])
                ),
                payload_digest=entry["payload_digest"],
            )
        )
    return out


def scan_kafka(
    cfg: AppConfig, cut: dict[TopicPartition, int]
) -> tuple[list[ObservedEvent], list[str], list[str]]:
    """Read every partition from its log start to the frozen cut.

    Uses ``assign`` with explicit seeks rather than ``subscribe``, so the
    verifier joins no consumer group and cannot disturb the ingester offsets.
    It stops at the captured end offsets rather than waiting for an empty poll,
    because an empty poll is not an end-of-dataset signal (spec I-08).
    """
    from confluent_kafka import Consumer, KafkaError
    from confluent_kafka import TopicPartition as CkTopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": cfg.kafka.bootstrap_servers,
            # A unique, non-committing reader, so this can never move the
            # ingester position.
            "group.id": f"p05-verifier-{cfg.project.run_id}",
            "enable.auto.commit": False,
            "auto.offset.reset": "error",
        }
    )
    validator = ContractValidator(cfg.contracts, cfg.project)
    observed: list[ObservedEvent] = []
    refused: list[str] = []
    notes: list[str] = []

    try:
        assignments = []
        for tp, end in cut.items():
            low, high = consumer.get_watermark_offsets(
                CkTopicPartition(tp.topic, tp.partition), timeout=20.0, cached=False
            )
            if low > 0:
                notes.append(
                    f"{tp}: log start offset is {low}, so records below it have "
                    "been deleted by retention and cannot be verified"
                )
            if high < end:
                notes.append(
                    f"{tp}: high watermark {high} is below the recorded cut "
                    f"{end}; the cut does not describe this log"
                )
            assignments.append(CkTopicPartition(tp.topic, tp.partition, low))
        consumer.assign(assignments)

        def reached_cut() -> bool:
            for tp, end in cut.items():
                position = consumer.position(
                    [CkTopicPartition(tp.topic, tp.partition)]
                )[0].offset
                if position < 0:
                    return False
                if position < end:
                    return False
            return True

        while not reached_cut():
            message = consumer.poll(5.0)
            if message is None:
                notes.append(
                    "poll returned nothing before the cut was reached; the scan "
                    "is incomplete"
                )
                break
            err = message.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"verifier poll error: {err.str()}")
            tp = TopicPartition(message.topic(), message.partition())
            if message.offset() >= cut.get(tp, 0):
                continue
            record = ConsumedRecord(
                tp=tp,
                offset=message.offset(),
                key=message.key(),
                value=message.value(),
                headers=tuple((n, v or b"") for n, v in (message.headers() or ())),
            )
            result = validator.validate(record)
            if isinstance(result, ValidationOk):
                observed.append(
                    ObservedEvent(
                        identity=result.projection.identity,
                        payload_digest=payload_digest(result.projection),
                    )
                )
            else:
                refused.append(result.code)
    finally:
        consumer.close()

    return observed, refused, notes


def read_sink(cfg: AppConfig, dataset_id: str | None):
    """Read the table back. Scoped by dataset to keep the scan bounded."""
    import clickhouse_connect

    from p05_ingest.model import CH_INSERT_COLUMNS

    user, password = cfg.clickhouse.credentials()
    client = clickhouse_connect.get_client(
        host=cfg.clickhouse.host,
        port=cfg.clickhouse.port,
        database=cfg.clickhouse.database,
        username=user,
        password=password,
        secure=cfg.clickhouse.secure,
    )
    columns = ", ".join(CH_INSERT_COLUMNS)
    where = "WHERE dataset_id = {dataset_id:String}" if dataset_id else ""
    try:
        result = client.query(
            f"SELECT {columns} FROM {cfg.clickhouse.table} {where}",
            parameters={"dataset_id": dataset_id} if dataset_id else {},
        )
        return [
            dict(zip(CH_INSERT_COLUMNS, row, strict=True))
            for row in result.result_rows
        ]
    finally:
        client.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/dev.toml")
    parser.add_argument("--cut", required=True, help="frozen Kafka cut JSON")
    parser.add_argument("--manifest", default=None, help="P01 expected set JSON")
    parser.add_argument("--dataset", default=None, help="scope the read-back")
    parser.add_argument("--out", default=None, help="write the report JSON here")
    parser.add_argument(
        "--settled",
        action="store_true",
        help=(
            "assert that the settle window has elapsed since the last write. "
            "Without it the report is INCONCLUSIVE, because a read-back taken "
            "too early can be missing rows that are merely not visible yet"
        ),
    )
    args = parser.parse_args(argv)

    cfg = AppConfig.from_toml(args.config)
    gaps = EvidenceGaps()
    if not args.settled:
        gaps.settle_window_not_elapsed = True

    try:
        cut = load_cut(Path(args.cut))
    except Exception as exc:
        print(f"cannot read the cut: {exc}", file=sys.stderr)
        return 3

    expected = None
    if args.manifest:
        try:
            expected = load_manifest(Path(args.manifest))
        except Exception as exc:
            print(f"cannot read the manifest: {exc}", file=sys.stderr)
            return 3

    try:
        kafka_observed, refused, notes = scan_kafka(cfg, cut)
    except ModuleNotFoundError as exc:
        print(
            f"confluent-kafka is required for the Kafka scan: {exc}", file=sys.stderr
        )
        return 3
    except Exception as exc:
        print(f"the Kafka scan failed: {exc}", file=sys.stderr)
        return 3

    try:
        rows = read_sink(cfg, args.dataset)
    except ModuleNotFoundError as exc:
        print(
            f"clickhouse-connect is required for the read-back: {exc}",
            file=sys.stderr,
        )
        return 3
    except Exception as exc:
        print(f"the ClickHouse read-back failed: {exc}", file=sys.stderr)
        print(
            "evidence gap: without the read-back no verdict is possible, so "
            "this is reported as INCONCLUSIVE rather than as a failure",
            file=sys.stderr,
        )
        return 3

    gaps.notes.extend(notes)
    if refused:
        gaps.notes.append(
            f"{len(refused)} record(s) in Kafka could not be interpreted: "
            f"{sorted(set(refused))}"
        )
    if any("retention" in note for note in notes):
        gaps.retention_loss_detected = True

    # A stored digest that does not describe its own row would make every
    # digest comparison below vacuous, so check that first.
    mismatches = verify_stored_digests(rows)
    if mismatches:
        gaps.notes.append(
            f"{len(mismatches)} stored row(s) carry a payload_digest that does "
            "not match their own columns"
        )

    report = reconcile(
        expected=expected,
        kafka_observed=kafka_observed,
        sink_observed=observed_from_sink_rows(rows),
        gaps=gaps,
    )

    print(report.summary())
    if mismatches:
        print(f"\nSELF-INCONSISTENT ROWS: {len(mismatches)}")
        for identity, stored, recomputed in mismatches[:10]:
            print(f"  {identity}: stored={stored[:16]}... actual={recomputed[:16]}...")

    if args.out:
        out = Path(args.out)
        report.write_json(out)
        print(f"\nreport: {out}")

    verdict = report.verdict
    if mismatches and verdict is Verdict.GO:
        verdict = Verdict.NO_GO
    return EXIT_BY_VERDICT[verdict]


if __name__ == "__main__":
    sys.exit(main())
