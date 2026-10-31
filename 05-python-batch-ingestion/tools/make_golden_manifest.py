"""Regenerate the golden expected set and the no-fault plan.

The manifest is the *expected* side of reconciliation, so it must be produced
independently of the ingestion path. It is fully determined by the seed, which
means a diff in the committed file signals a real change in either the workload
generator or the canonical digest -- not noise.

Usage::

    python tools/make_golden_manifest.py
    python tools/make_golden_manifest.py --events 5000 --seed 42
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from p05_ingest.canonical_digest import CANONICAL_ENCODING_VERSION  # noqa: E402
from p05_ingest.proto_schema import descriptor_sha256  # noqa: E402
from p05_ingest.testkit import (  # noqa: E402
    QUOTES_TOPIC,
    TICKS_TOPIC,
    FakeKafkaCluster,
)
from p05_ingest.testkit.workload import produce_market_workload  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def build(events: int, partitions: int, seed: int) -> dict:
    cluster = FakeKafkaCluster(cluster_id="local-dev")
    cluster.create_topic(TICKS_TOPIC, partitions)
    cluster.create_topic(QUOTES_TOPIC, partitions)
    manifest = produce_market_workload(
        cluster, events=events, partitions=partitions, seed=seed
    )

    body = {
        "note": (
            "Expected set for the golden finite run. Regenerate with "
            "tools/make_golden_manifest.py; it is deterministic from the seed, "
            "so a diff here means the workload generator or the digest changed."
        ),
        "generator": {
            "seed": seed,
            "events": events,
            "partitions_per_topic": partitions,
            "canonical_encoding": CANONICAL_ENCODING_VERSION,
            "descriptor_sha256": descriptor_sha256(),
        },
        "cut": {
            f"{tp.topic}:{tp.partition}": {"log_start": 0, "end": end}
            for tp, end in cluster.end_offsets().items()
        },
        "events": [
            {
                "dataset_id": e.identity.dataset_id,
                "symbol": e.identity.symbol,
                "sequence": e.identity.sequence,
                "payload_digest": e.payload_digest,
                "event_kind": e.kind.ch_name,
                "event_time_ns": e.event_time_ns,
                "topic": e.topic,
                "partition": e.partition,
            }
            for e in manifest
        ],
    }
    blob = json.dumps(body, indent=2, sort_keys=True)
    body["manifest_sha256"] = hashlib.sha256(blob.encode("utf-8")).hexdigest()
    return body


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=2000)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--seed", type=int, default=1337)
    args = parser.parse_args(argv)

    body = build(args.events, args.partitions, args.seed)
    manifest_path = PROJECT_ROOT / "fixtures" / "golden" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(body, indent=2, sort_keys=True), encoding="utf-8"
    )
    print(
        f"{manifest_path}: {len(body['events'])} events, "
        f"sha256 {body['manifest_sha256'][:16]}"
    )

    fault_path = PROJECT_ROOT / "fixtures" / "faults" / "none.json"
    fault_path.parent.mkdir(parents=True, exist_ok=True)
    fault_path.write_text(
        json.dumps(
            {
                "note": (
                    "The no-fault plan. A scenario that injects nothing must "
                    "still declare that explicitly, so a run cannot be mistaken "
                    "for a fault test that happened to pass (spec 9.1)."
                ),
                "plan_id": "none",
                "faults": [],
                "expected_fault_confirmations": {},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    print(f"{fault_path}: no-fault plan")
    return 0


if __name__ == "__main__":
    sys.exit(main())
