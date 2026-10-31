"""Seal a Kafka cut: the high-water mark and the committed frontier per partition.

P06 needs a bounded source before it may build anything, and a cut has to carry
two different numbers to be useful:

* ``end_offset`` -- the high-water mark at the moment of capture. This bounds
  the source.
* ``consumer_frontier`` -- P05's durable committed offset. Everything below it
  has been acknowledged by the sink, not merely fetched.

``TopicPartitionCut.covers_cut()`` is the comparison between them, and it is the
whole reason both are recorded: a cut that assumed the frontier equals the end
would report full coverage of a partition the consumer never finished.

Why P09 writes this rather than P05's ``snapshot`` command. Two reasons, and the
second is the one that matters:

1. ``p05_ingest snapshot`` subscribes and then reads ``assignment()``
   immediately. Subscription is asynchronous, so the group has no assignment yet
   and the cut comes out empty -- observed here as ``"cut": {}``, which P06 then
   correctly refuses.
2. More fundamentally, that command records only watermarks. It carries no
   committed frontier at all, so the distinction above could not be expressed.

P09 owns the topics and the operator role, so it can read both without joining
the consumer group and without perturbing P05's offsets: watermarks come from a
non-subscribed consumer, and the committed offsets are queried for the group by
name. Nothing here consumes a record or commits anything.

A partition whose committed offset is unknown is written as ``null`` rather than
as the end offset. Unknown and complete are different answers, and defaulting
one to the other is how a partial ingest starts reporting as a full one.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from confluent_kafka import Consumer, TopicPartition


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Seal a Kafka cut for P06, with watermarks and the committed frontier."
        )
    )
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument(
        "--group", required=True, help="the P05 consumer group to read offsets for"
    )
    parser.add_argument("--topics", nargs="+", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--topic-generation", default="unknown")
    parser.add_argument(
        "--p05-state",
        default="DRAINED",
        help="terminal state P05 reached; a cut taken mid-run bounds nothing",
    )
    parser.add_argument("--timeout", type=float, default=15.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Not subscribed: this consumer is only a handle for metadata, watermark and
    # committed-offset lookups. Joining the group would trigger a rebalance and
    # move the very offsets this tool exists to read.
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "enable.auto.commit": False,
            "auto.offset.reset": "error",
        }
    )

    try:
        metadata = consumer.list_topics(timeout=args.timeout)
    except Exception as exc:  # the driver raises its own types
        print("refused: cannot read cluster metadata: %s" % exc, file=sys.stderr)
        return 2

    partitions: list[TopicPartition] = []
    for topic in args.topics:
        if topic not in metadata.topics:
            print("refused: topic %s does not exist" % topic, file=sys.stderr)
            return 2
        for partition in sorted(metadata.topics[topic].partitions):
            partitions.append(TopicPartition(topic, partition))

    if not partitions:
        print("refused: the topics carry no partitions", file=sys.stderr)
        return 2

    # Committed offsets for the group, by name. A partition the group never
    # committed comes back with a negative offset, recorded below as unknown.
    try:
        committed = {
            (tp.topic, tp.partition): tp.offset
            for tp in consumer.committed(partitions, timeout=args.timeout)
        }
    except Exception as exc:
        print("refused: cannot read committed offsets: %s" % exc, file=sys.stderr)
        return 2

    cut: dict[str, dict[str, int]] = {}
    frontiers: dict[str, int | None] = {}
    unknown: list[str] = []
    for tp in partitions:
        key = "%s:%d" % (tp.topic, tp.partition)
        try:
            low, high = consumer.get_watermark_offsets(
                tp, timeout=args.timeout, cached=False
            )
        except Exception as exc:
            print(
                "refused: cannot read watermarks for %s: %s" % (key, exc),
                file=sys.stderr,
            )
            return 2
        cut[key] = {"log_start": low, "end": high}
        offset = committed.get((tp.topic, tp.partition), -1001)
        if offset is None or offset < 0:
            frontiers[key] = None
            unknown.append(key)
        else:
            frontiers[key] = offset

    consumer.close()

    payload = {
        "run_id": args.run_id,
        "topic_generation": args.topic_generation,
        "consumer_group": args.group,
        "p05_state": args.p05_state,
        "seal_reason": "p09-sealed",
        "captured_at_ns": time.time_ns(),
        "cut": cut,
        "committed": frontiers,
    }
    with open(args.out, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    total_end = sum(entry["end"] for entry in cut.values())
    covered = sum(
        1
        for key, entry in cut.items()
        if frontiers.get(key) is not None and frontiers[key] >= entry["end"]
    )
    print(
        "  sealed %d partition(s): end offsets sum %d, %d fully covered by the "
        "committed frontier" % (len(cut), total_end, covered)
    )
    if unknown:
        # Reported, never defaulted: a partition with no committed offset is one
        # P05 did not finish, and the cut says so.
        print(
            "  %d partition(s) have no committed offset: %s"
            % (len(unknown), ", ".join(unknown))
        )
    print("  wrote %s" % args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
