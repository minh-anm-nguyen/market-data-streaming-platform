"""Test infrastructure shipped with the package.

It lives under ``src`` rather than under ``tests`` so that ``tools/`` scripts
(the scenario runner and the benchmark harness) can import the same fakes the
test suite uses. A divergence between "what the tests exercise" and "what the
scenario runner exercises" would be a silent source of false confidence.

Nothing here is imported by the production code path.
"""

from .builders import (
    BASE_EVENT_TIME_NS,
    DEFAULT_DATASET,
    DEFAULT_PRICE_SCALE,
    DEFAULT_SCHEMA_VERSION,
    QUOTES_TOPIC,
    TICKS_TOPIC,
    GoldenEntry,
    conflict_pair,
    duplicate_pair,
    golden_corpus,
    make_event,
    make_quote,
    make_tick,
    poison_corpus,
    topic_for,
)
from .clock import FakeClock
from .fake_clickhouse import Behaviour, FakeClickHouse, InsertAttempt, StoredRow
from .fake_kafka import CommitFault, FakeConsumer, FakeKafkaCluster, PartitionLog

__all__ = [
    "BASE_EVENT_TIME_NS",
    "Behaviour",
    "CommitFault",
    "DEFAULT_DATASET",
    "DEFAULT_PRICE_SCALE",
    "DEFAULT_SCHEMA_VERSION",
    "FakeClickHouse",
    "FakeClock",
    "FakeConsumer",
    "FakeKafkaCluster",
    "GoldenEntry",
    "InsertAttempt",
    "PartitionLog",
    "QUOTES_TOPIC",
    "StoredRow",
    "TICKS_TOPIC",
    "conflict_pair",
    "duplicate_pair",
    "golden_corpus",
    "make_event",
    "make_quote",
    "make_tick",
    "poison_corpus",
    "topic_for",
]
