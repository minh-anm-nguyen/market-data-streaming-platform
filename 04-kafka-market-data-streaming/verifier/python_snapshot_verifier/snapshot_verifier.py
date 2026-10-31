#!/usr/bin/env python3
"""The independent snapshot verifier (section 5.1, section 5.2, E2E-01, NFR-02).

What makes this an *independent* oracle rather than a second opinion from the
same code:

* The Kafka client is kafka-python, a pure-Python implementation of the Kafka
  protocol. The publisher uses librdkafka. A bug in either one cannot hide
  itself from the other.
* The protobuf decode goes through a descriptor set loaded at runtime, not
  through generated Python code and not through the C++ classes. Same pinned
  schema, two runtimes, which is what NFR-01 asks for.
* The expected set of market-valid events is re-derived here, in Python, from
  the Project 01 dataset using the documented classification rules. It is not
  read back from anything Project 04 produced. Section 10.1, oracle rule 2: the
  system under test may not generate its own golden data.
* The expected partition for each key is computed with this project's own
  murmur2 implementation (tools/gen_partition_golden.py), so a key landing on
  the wrong partition is caught by a third implementation.

What it reads from Project 04 is the publish ledger, and only to *reconcile*:
the ledger says what the producer believed, the scan says what the broker holds,
and the interesting cases are where the two disagree. In particular an attempt
the producer recorded as UNKNOWN is resolved here, which is the whole point of
section 4.2.

What it does not prove, and says so in its report: that anything was written to
ClickHouse, that delivery is exactly-once, or that a live source never lost an
event. Those are Project 05 and upstream concerns.

Offsets: the scan is bounded by begin and end offsets captured *before* reading,
so the result is a finite snapshot (section 5.1). It does not join a consumer
group and it never commits.
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# The murmur2 implementation lives with the golden generator so there is exactly
# one Python copy of it.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from gen_partition_golden import partition_for  # noqa: E402

try:
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
except ImportError as exc:  # pragma: no cover
    print(f"FATAL: python protobuf runtime missing: {exc}", file=sys.stderr)
    raise

try:
    from kafka import KafkaConsumer, TopicPartition
except ImportError as exc:  # pragma: no cover
    print(f"FATAL: kafka-python missing: {exc}", file=sys.stderr)
    raise


SCHEMA_VERSION = "sim.v1"
QUOTE_FIELDS = {
    "ask_ticks", "bid_ticks", "dataset_id", "event_time_ns", "event_type",
    "price_scale", "schema_version", "sequence", "symbol",
}
TICK_FIELDS = {
    "dataset_id", "event_time_ns", "event_type", "price_scale", "price_ticks",
    "schema_version", "sequence", "symbol",
}
SYMBOL_ALPHABET = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.")


def load_message_class(descriptor_set_path: Path):
    """Builds a MarketDataEvent class from the pinned descriptor set.

    Using the descriptor rather than a generated _pb2 module keeps this verifier
    free of generated-code/runtime version coupling, and means the schema it
    decodes with is literally the one the C++ build compiled.
    """
    file_descriptor_set = descriptor_pb2.FileDescriptorSet()
    file_descriptor_set.ParseFromString(descriptor_set_path.read_bytes())
    pool = descriptor_pool.DescriptorPool()
    for file_proto in file_descriptor_set.file:
        pool.Add(file_proto)
    descriptor = pool.FindMessageTypeByName("marketdata.v1.MarketDataEvent")
    # message_factory moved between protobuf versions; both spellings are tried
    # so the verifier is not pinned to one minor release.
    if hasattr(message_factory, "GetMessageClass"):
        return message_factory.GetMessageClass(descriptor)
    return message_factory.MessageFactory(pool).GetPrototype(descriptor)


@dataclass(frozen=True)
class Identity:
    dataset_id: str
    symbol: str
    sequence: int
    kind: str  # "quote" or "tick"

    def __str__(self) -> str:
        return f"{self.dataset_id}/{self.symbol}/{self.sequence}/{self.kind}"


@dataclass(frozen=True)
class Core:
    """The semantic projection equality is defined over (NFR-01, case D-14).

    Presence is part of the value: None and 0 are different, which is what makes
    case D-03 observable from the consumer side too.
    """

    event_time_ns: Optional[int]
    price_scale: Optional[int]
    bid_ticks: Optional[int]
    ask_ticks: Optional[int]
    price_ticks: Optional[int]
    source_schema_version: Optional[str]


@dataclass
class Observed:
    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    core: Core
    headers: Dict[str, bytes] = field(default_factory=dict)


@dataclass
class Findings:
    missing: List[str] = field(default_factory=list)
    duplicates: List[str] = field(default_factory=list)
    conflicts: List[str] = field(default_factory=list)
    unexpected: List[str] = field(default_factory=list)
    contract: List[str] = field(default_factory=list)
    resolved_unknowns: List[str] = field(default_factory=list)
    unresolved_unknowns: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


def classify_source(run_dir: Path) -> Tuple[Dict[Identity, Core], Dict[str, int], List[str]]:
    """Re-derives the expected market-valid set from the Project 01 dataset.

    An independent reimplementation of the documented rules, in Python:

      * structural: required fields present for the event type, nothing extra,
        integers really integers, symbol in the allowed alphabet, scale positive;
      * business: a quote with ask < bid, or any non-positive price, is rejected;
      * duplicate: the second appearance of an identity is suppressed.

    A sequence gap is NOT treated as a problem: it may be a deliberate Project
    01 omission, and case B-10 forbids reading a gap as loss.

    Returns the expected identity to core mapping, counters, and parse problems.
    """
    events_path = run_dir / "events.jsonl"
    expected: Dict[Identity, Core] = {}
    counts: collections.Counter = collections.Counter()
    problems: List[str] = []
    seen: Set[Tuple[str, str, int]] = set()

    with events_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            counts["source_lines"] += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                problems.append(f"line {line_number}: invalid json: {exc.msg}")
                counts["structural_invalid"] += 1
                continue
            if not isinstance(record, dict):
                problems.append(f"line {line_number}: not an object")
                counts["structural_invalid"] += 1
                continue

            event_type = record.get("event_type")
            if record.get("schema_version") != SCHEMA_VERSION or event_type not in ("quote", "tick"):
                counts["structural_invalid"] += 1
                continue
            allowed = QUOTE_FIELDS if event_type == "quote" else TICK_FIELDS
            if set(record) != allowed:
                counts["structural_invalid"] += 1
                continue

            symbol = record["symbol"]
            dataset_id = record["dataset_id"]
            if (
                not isinstance(symbol, str)
                or not set(symbol) <= SYMBOL_ALPHABET
                or not 3 <= len(symbol) <= 16
                or not isinstance(dataset_id, str)
                or not dataset_id
            ):
                counts["structural_invalid"] += 1
                continue

            def as_int(name: str) -> Optional[int]:
                value = record.get(name)
                # bool is an int subclass in Python and must not pass as one.
                if isinstance(value, bool) or not isinstance(value, int):
                    return None
                return value

            sequence = as_int("sequence")
            event_time_ns = as_int("event_time_ns")
            price_scale = as_int("price_scale")
            if sequence is None or event_time_ns is None or price_scale is None:
                counts["structural_invalid"] += 1
                continue
            if price_scale <= 0 or sequence < 0:
                counts["structural_invalid"] += 1
                continue

            if event_type == "quote":
                bid = as_int("bid_ticks")
                ask = as_int("ask_ticks")
                if bid is None or ask is None:
                    counts["structural_invalid"] += 1
                    continue
                if ask < bid or bid <= 0 or ask <= 0:
                    counts["business_reject"] += 1
                    continue
                price = None
            else:
                price = as_int("price_ticks")
                if price is None:
                    counts["structural_invalid"] += 1
                    continue
                if price <= 0:
                    counts["business_reject"] += 1
                    continue
                bid = None
                ask = None

            logical = (dataset_id, symbol, sequence)
            if logical in seen:
                counts["duplicate_suppressed"] += 1
                continue
            seen.add(logical)

            identity = Identity(dataset_id, symbol, sequence, event_type)
            expected[identity] = Core(
                event_time_ns=event_time_ns,
                price_scale=price_scale,
                bid_ticks=bid,
                ask_ticks=ask,
                price_ticks=price,
                source_schema_version=SCHEMA_VERSION,
            )
            counts["market_valid"] += 1

    return expected, dict(counts), problems


def read_ledger(path: Path) -> Tuple[Dict[Identity, List[dict]], Dict[str, int]]:
    """Reads the Project 04 publish ledger, grouped by identity."""
    by_identity: Dict[Identity, List[dict]] = collections.defaultdict(list)
    counts: collections.Counter = collections.Counter()
    if not path.exists():
        return {}, {}
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            attempt = json.loads(line)
            identity = Identity(
                attempt["dataset_id"], attempt["symbol"], attempt["sequence"], attempt["kind"]
            )
            by_identity[identity].append(attempt)
            counts[attempt["state"]] += 1
            counts["attempts"] += 1
    return dict(by_identity), dict(counts)


def capture_offsets(bootstrap: str, topics: List[str]) -> Dict[str, int]:
    """Records the current end offsets, to be used as the begin bounds later.

    Called before a publish run. Section 5.1 requires the scan to be bounded by
    explicitly captured offsets, and the lower bound has to come from before the
    run or the snapshot includes whatever an earlier run left on the topic. The
    first integration run against a reused cluster shows exactly that: 44096
    records observed for 20000 expected, every identity reported as a duplicate,
    and none of it a defect in the publisher.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=10000,
    )
    partitions: List[TopicPartition] = []
    for topic in topics:
        ids = consumer.partitions_for_topic(topic) or set()
        partitions.extend(TopicPartition(topic, p) for p in sorted(ids))
    consumer.assign(partitions)
    end = consumer.end_offsets(partitions) if partitions else {}
    consumer.close()
    return {f"{tp.topic}:{tp.partition}": offset for tp, offset in end.items()}


def capture_and_scan(
    bootstrap: str,
    topics: List[str],
    message_class,
    timeout_ms: int,
    begin_override: Optional[Dict[str, int]] = None,
) -> Tuple[Dict[Identity, List[Observed]], Dict[str, Any], List[str]]:
    """Reads exactly the captured offset range.

    The upper bound is captured here, before reading. The lower bound is either
    the earliest available offset or, when begin_override is supplied, the end
    offsets recorded before the publish run. Bounding both ends is what makes
    the result a finite snapshot rather than a timing-dependent sample.
    """
    contract_problems: List[str] = []
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        group_id=None,  # no group: this verifier never commits (section 5.1)
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout_ms,
        max_partition_fetch_bytes=4 * 1024 * 1024,
    )

    partitions: List[TopicPartition] = []
    partition_counts: Dict[str, int] = {}
    for topic in topics:
        ids = consumer.partitions_for_topic(topic)
        if not ids:
            contract_problems.append(f"topic {topic} reports no partitions")
            continue
        partition_counts[topic] = len(ids)
        partitions.extend(TopicPartition(topic, p) for p in sorted(ids))

    if not partitions:
        consumer.close()
        return {}, {"partitions": [], "total_records_in_range": 0, "records_scanned": 0}, contract_problems

    consumer.assign(partitions)
    begin = consumer.beginning_offsets(partitions)
    end = consumer.end_offsets(partitions)

    if begin_override:
        for tp in partitions:
            recorded = begin_override.get(f"{tp.topic}:{tp.partition}")
            if recorded is None:
                continue
            # Never move the bound below what the broker still has: retention may
            # have deleted the segment the override names, and silently reading
            # from earliest instead would hide that (case C-12). max() keeps the
            # bound honest and the retention gap shows up as a missing identity.
            begin[tp] = max(begin[tp], recorded)

    snapshot: Dict[str, Any] = {
        "partitions": [
            {
                "topic": tp.topic,
                "partition": tp.partition,
                "begin_offset": begin[tp],
                "end_offset": end[tp],
                "records_in_range": end[tp] - begin[tp],
            }
            for tp in partitions
        ],
        "total_records_in_range": sum(end[tp] - begin[tp] for tp in partitions),
    }

    for tp in partitions:
        consumer.seek(tp, begin[tp])

    remaining = {tp: end[tp] - begin[tp] for tp in partitions if end[tp] > begin[tp]}
    observed: Dict[Identity, List[Observed]] = collections.defaultdict(list)
    # Per topic-partition ordering state, for the only ordering claim this
    # project makes (section 3.3).
    last_sequence: Dict[Tuple[str, int, str], int] = {}
    last_offset: Dict[Tuple[str, int], int] = {}
    ordering_problems: List[str] = []
    scanned = 0

    while remaining:
        batches = consumer.poll(timeout_ms=2000, max_records=2000)
        if not batches:
            break
        for tp, records in batches.items():
            for record in records:
                if record.offset >= end[tp]:
                    continue  # beyond the captured bound: not part of this snapshot
                scanned += 1
                remaining[tp] = remaining.get(tp, 0) - 1
                if remaining.get(tp, 0) <= 0:
                    remaining.pop(tp, None)

                previous_offset = last_offset.get((tp.topic, tp.partition), -1)
                if record.offset <= previous_offset:
                    ordering_problems.append(
                        f"{tp.topic}/{tp.partition}: offset {record.offset} did not "
                        f"advance past {previous_offset}"
                    )
                last_offset[(tp.topic, tp.partition)] = record.offset

                raw = record.value or b""
                # Cases D-01 and D-02 from the consumer side: a framed value must
                # not be sitting on a market topic. Checked before parsing,
                # because a framed buffer sometimes parses into garbage fields.
                if raw[:4] == b"MDP1" or (len(raw) >= 4 and raw[0] == 0):
                    contract_problems.append(
                        f"{tp.topic}/{tp.partition}@{record.offset}: value carries a "
                        f"transport framing that must never reach Kafka"
                    )
                    continue

                message = message_class()
                try:
                    message.ParseFromString(raw)
                except Exception as exc:  # noqa: BLE001
                    contract_problems.append(
                        f"{tp.topic}/{tp.partition}@{record.offset}: value does not "
                        f"parse as MarketDataEvent: {exc}"
                    )
                    continue

                kind = message.WhichOneof("payload")
                if kind is None:
                    contract_problems.append(
                        f"{tp.topic}/{tp.partition}@{record.offset}: payload oneof is unset"
                    )
                    continue
                expected_topic = "md.quotes.v1" if kind == "quote" else "md.ticks.v1"
                if tp.topic != expected_topic:
                    contract_problems.append(
                        f"{tp.topic}/{tp.partition}@{record.offset}: a {kind} landed on "
                        f"{tp.topic}, expected {expected_topic}"
                    )

                if not message.HasField("symbol"):
                    contract_problems.append(
                        f"{tp.topic}/{tp.partition}@{record.offset}: symbol absent"
                    )
                    continue
                symbol = message.symbol
                key = record.key
                if key is None or key.decode("utf-8", errors="replace") != symbol:
                    contract_problems.append(
                        f"{tp.topic}/{tp.partition}@{record.offset}: key {key!r} does "
                        f"not equal value.symbol {symbol!r}"
                    )
                if symbol != symbol.upper():
                    contract_problems.append(
                        f"{tp.topic}/{tp.partition}@{record.offset}: symbol {symbol!r} "
                        f"is not canonical uppercase"
                    )

                # The third implementation of the partitioner.
                expected_partition = partition_for(symbol, partition_counts.get(tp.topic, 0))
                if expected_partition != tp.partition:
                    contract_problems.append(
                        f"{tp.topic}/{tp.partition}@{record.offset}: key {symbol} should "
                        f"map to partition {expected_partition} under the pinned partitioner"
                    )

                headers = {name: value for name, value in (record.headers or [])}
                dataset_id = message.dataset_id if message.HasField("dataset_id") else None
                dataset_header = headers.get("md-dataset-id")
                if dataset_header is not None and dataset_id is not None:
                    if dataset_header.decode("utf-8", errors="replace") != dataset_id:
                        contract_problems.append(
                            f"{tp.topic}/{tp.partition}@{record.offset}: md-dataset-id "
                            f"header disagrees with value.dataset_id"
                        )

                if not message.HasField("sequence"):
                    contract_problems.append(
                        f"{tp.topic}/{tp.partition}@{record.offset}: sequence absent, "
                        f"which must never be published"
                    )
                    continue
                sequence = message.sequence

                # Per (topic, partition, symbol) the sequence must strictly
                # increase. Not necessarily by one: the other event type lives on
                # the other topic and the adapter suppresses some events, which
                # is exactly what case B-10 warns against misreading.
                order_key = (tp.topic, tp.partition, symbol)
                previous = last_sequence.get(order_key)
                if previous is not None and sequence <= previous:
                    ordering_problems.append(
                        f"{tp.topic}/{tp.partition} {symbol}: sequence {sequence} did "
                        f"not increase past {previous}"
                    )
                last_sequence[order_key] = sequence if previous is None else max(sequence, previous)

                quote = message.quote if kind == "quote" else None
                tick = message.tick if kind == "tick" else None
                core = Core(
                    event_time_ns=message.event_time_ns if message.HasField("event_time_ns") else None,
                    price_scale=message.price_scale if message.HasField("price_scale") else None,
                    bid_ticks=quote.bid_ticks if quote is not None and quote.HasField("bid_ticks") else None,
                    ask_ticks=quote.ask_ticks if quote is not None and quote.HasField("ask_ticks") else None,
                    price_ticks=tick.price_ticks if tick is not None and tick.HasField("price_ticks") else None,
                    source_schema_version=(
                        message.source_schema_version
                        if message.HasField("source_schema_version")
                        else None
                    ),
                )
                identity = Identity(dataset_id or "", symbol, sequence, kind)
                observed[identity].append(
                    Observed(tp.topic, tp.partition, record.offset, key, core, headers)
                )

    consumer.close()
    snapshot["records_scanned"] = scanned
    contract_problems.extend(ordering_problems)
    return dict(observed), snapshot, contract_problems


def reconcile(
    expected: Dict[Identity, Core],
    observed: Dict[Identity, List[Observed]],
    ledger: Dict[Identity, List[dict]],
) -> Findings:
    """Joins the three domains: source truth, broker contents, producer belief."""
    findings = Findings()

    for identity, core in expected.items():
        records = observed.get(identity, [])
        attempts = ledger.get(identity, [])
        acked = [a for a in attempts if a["state"] == "ACKED"]
        unknown = [a for a in attempts if a["state"] == "UNKNOWN"]
        refused = [a for a in attempts if a["state"] in ("FAILED_DEFINITE", "ENQUEUE_FAILED")]

        if not records:
            if acked:
                # The producer was told the broker had it and the broker does
                # not. This is the serious one.
                findings.missing.append(
                    f"{identity}: acknowledged by the broker but absent from the scan"
                )
            elif unknown:
                findings.unresolved_unknowns.append(
                    f"{identity}: producer recorded UNKNOWN and the scan did not find it; "
                    f"either not written, or written outside the captured range"
                )
            elif refused:
                findings.notes.append(
                    f"{identity}: not published, producer reported {refused[0]['state']} "
                    f"({refused[0].get('error_class')})"
                )
            elif not attempts:
                findings.missing.append(
                    f"{identity}: expected from the source but the producer never attempted it"
                )
            else:
                findings.missing.append(f"{identity}: expected but absent, with no explanation")
            continue

        if len(records) > 1:
            findings.duplicates.append(
                f"{identity}: {len(records)} records at "
                + ", ".join(f"{r.topic}/{r.partition}@{r.offset}" for r in records)
            )

        for record in records:
            if record.core != core:
                findings.conflicts.append(
                    f"{identity} at {record.topic}/{record.partition}@{record.offset}: "
                    f"observed {record.core} but the source says {core}"
                )

        if unknown:
            findings.resolved_unknowns.append(
                f"{identity}: producer recorded UNKNOWN, the scan found it at "
                f"{records[0].topic}/{records[0].partition}@{records[0].offset}"
            )

    for identity, records in observed.items():
        if identity not in expected:
            findings.unexpected.append(
                f"{identity}: present on Kafka but not expected by the source oracle "
                f"({len(records)} records)"
            )

    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Project 04 snapshot verifier")
    parser.add_argument("--bootstrap", default="127.0.0.1:9092")
    parser.add_argument("--topics", default="md.ticks.v1,md.quotes.v1")
    parser.add_argument("--descriptor", required=True, help="market_data.desc")
    parser.add_argument("--source-run-dir", required=True, help="a Project 01 run directory")
    parser.add_argument("--ledger", default="", help="publish-ledger.jsonl from the P04 run")
    parser.add_argument("--report", default="snapshot-verifier-report.json")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    parser.add_argument(
        "--capture-begin-offsets",
        default="",
        help="write the current end offsets to this file and exit; run this "
             "before publishing so the later scan can be bounded below",
    )
    parser.add_argument(
        "--begin-offsets",
        default="",
        help="a file written by --capture-begin-offsets, used as the lower bound",
    )
    args = parser.parse_args()

    topic_list = [t for t in args.topics.split(",") if t]

    if args.capture_begin_offsets:
        offsets = capture_offsets(args.bootstrap, topic_list)
        Path(args.capture_begin_offsets).write_text(
            json.dumps(offsets, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(json.dumps({"captured_begin_offsets": offsets}, indent=2, sort_keys=True))
        return 0

    begin_override: Optional[Dict[str, int]] = None
    if args.begin_offsets:
        begin_override = json.loads(Path(args.begin_offsets).read_text(encoding="utf-8"))

    run_dir = Path(args.source_run_dir)
    message_class = load_message_class(Path(args.descriptor))

    expected, source_counts, source_problems = classify_source(run_dir)
    ledger, ledger_counts = read_ledger(Path(args.ledger)) if args.ledger else ({}, {})

    completed_marker = (run_dir / "COMPLETED").exists()
    observed, snapshot, contract_problems = capture_and_scan(
        args.bootstrap, topic_list, message_class, args.timeout_ms, begin_override
    )

    findings = reconcile(expected, observed, ledger)
    findings.contract.extend(contract_problems)
    findings.notes.extend(f"source parse: {p}" for p in source_problems[:20])

    observed_unique = len(observed)
    observed_records = sum(len(v) for v in observed.values())

    # The verdict. PASS requires a completeness oracle *and* a clean
    # reconciliation; anything less is INCOMPLETE or FAIL, never a pass with
    # caveats (section 14).
    if not completed_marker:
        verdict = "INCOMPLETE"
        reason = "the source run has no COMPLETED marker, so it cannot be a completeness oracle"
    elif findings.conflicts:
        verdict = "FAIL"
        reason = "a record on Kafka disagrees with the source on a core field (P0)"
    elif findings.missing:
        verdict = "FAIL"
        reason = "an expected identity is absent from the captured offset range"
    elif findings.contract:
        verdict = "FAIL"
        reason = "a record violates the key, topic, partition or schema contract"
    elif findings.duplicates:
        verdict = "FAIL"
        reason = "an identity appears more than once and nothing accounts for it"
    elif findings.unexpected:
        verdict = "FAIL"
        reason = "Kafka holds records the source oracle does not expect"
    elif findings.unresolved_unknowns:
        verdict = "INCOMPLETE"
        reason = "attempts the producer could not resolve were also not found by the scan"
    elif observed_records == 0:
        verdict = "INCONCLUSIVE"
        reason = "the scan read no records, so it proves nothing"
    else:
        verdict = "PASS"
        reason = "every expected identity was observed exactly once with matching core fields"

    report = {
        "tool": "python_snapshot_verifier",
        "verdict": verdict,
        "verdict_reason": reason,
        "independent_of": [
            "librdkafka: this uses kafka-python, a separate protocol implementation",
            "the C++ protobuf runtime: this decodes through the descriptor set",
            "Project 04 classification: the expected set is re-derived here",
        ],
        "does_not_prove": [
            "any write to ClickHouse: Project 05 owns the sink",
            "exactly-once delivery",
            "that a live source never lost an event before Project 04 saw it",
            "chronological order across md.ticks.v1 and md.quotes.v1",
        ],
        "bootstrap": args.bootstrap,
        "source_run_dir": str(run_dir),
        "source_completed_marker": completed_marker,
        "begin_offsets_were_captured_before_the_run": begin_override is not None,
        "source_counts": source_counts,
        "expected_market_valid": len(expected),
        "observed_unique_identities": observed_unique,
        "observed_records": observed_records,
        "ledger_counts": ledger_counts,
        "offset_snapshot": snapshot,
        "missing": findings.missing[:100],
        "missing_count": len(findings.missing),
        "duplicates": findings.duplicates[:100],
        "duplicate_count": len(findings.duplicates),
        "conflicts": findings.conflicts[:100],
        "conflict_count": len(findings.conflicts),
        "unexpected": findings.unexpected[:100],
        "unexpected_count": len(findings.unexpected),
        "contract_violations": findings.contract[:100],
        "contract_violation_count": len(findings.contract),
        "resolved_unknowns": findings.resolved_unknowns[:100],
        "resolved_unknown_count": len(findings.resolved_unknowns),
        "unresolved_unknowns": findings.unresolved_unknowns[:100],
        "unresolved_unknown_count": len(findings.unresolved_unknowns),
        "notes": findings.notes[:50],
    }

    Path(args.report).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    summary_keys = (
        "verdict", "verdict_reason", "expected_market_valid",
        "observed_unique_identities", "observed_records", "missing_count",
        "duplicate_count", "conflict_count", "contract_violation_count",
        "resolved_unknown_count", "unresolved_unknown_count",
    )
    print(json.dumps({k: report[k] for k in summary_keys}, indent=2))

    if verdict == "PASS":
        return 0
    if verdict in ("INCOMPLETE", "INCONCLUSIVE"):
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
