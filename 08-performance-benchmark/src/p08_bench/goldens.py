"""The four golden fixtures of section 10, as data rather than prose.

These live in the package, not in the test tree, for one reason: section 8
requires the measuring tools to be validated against known inputs *before* they
are pointed at a real system, and section 27 lists "fixture latencies with exact
p95" in the minimum set needed to start. That makes them a shipped artifact the
CLI can run as a self-test (``p08-bench selftest``), not a test-only detail.

Every expected value here is quoted from the design document. Where the document
deliberately refuses to pin a number -- FIX-001's raw row count, which depends
on whether the sink dedupes -- this module refuses too, with ``None`` and a
note, rather than inventing a plausible constant.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# FIX-002: query latency unit/self-test
# ---------------------------------------------------------------------------

#: Ten synthetic client latencies in milliseconds, quoted from FIX-002.
FIX002_LATENCIES_MS: tuple[float, ...] = (
    100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0, 180.0, 1000.0,
)

#: Exact nearest-rank p95 with N=10: rank ceil(0.95*10)=10, the tenth value.
FIX002_EXPECTED_P95_MS = 1000.0

#: The two answers FIX-002 names as wrong: the mean, and an average of
#: per-bucket p95s. Kept so the self-test can assert it did not produce them.
FIX002_FORBIDDEN_P95_MS: tuple[float, ...] = (226.0,)

#: Second half of FIX-002: add ten timeouts and the accounting must read
#: scheduled=20, completed=10, timeout=10 with a conditional p95 of 1000 ms.
FIX002_ADDED_TIMEOUTS = 10
FIX002_EXPECTED_SCHEDULED = 20
FIX002_EXPECTED_COMPLETED = 10


# ---------------------------------------------------------------------------
# FIX-001: 10 events, 3 symbols, 2 topics, one intentional duplicate
# ---------------------------------------------------------------------------

#: Business identity is ``(dataset_id, symbol, sequence)`` per INV-02, and the
#: sequence is per symbol across both ticks and quotes (P01 contract).
FIX001_DATASET_ID = "FIX001"

#: The ten sealed logical events. ``kind`` picks the topic: ticks and quotes are
#: separate topics in P04, so this fixture spans two.
FIX001_EVENTS: tuple[dict[str, Any], ...] = (
    {"symbol": "EURUSD", "sequence": 1, "kind": "tick", "price_ticks": 108347, "event_time_ns": 1_793_322_000_000_000_000},
    {"symbol": "EURUSD", "sequence": 2, "kind": "quote", "bid_ticks": 108345, "ask_ticks": 108349, "event_time_ns": 1_793_322_000_000_100_000},
    {"symbol": "EURUSD", "sequence": 3, "kind": "tick", "price_ticks": 108350, "event_time_ns": 1_793_322_000_000_200_000},
    {"symbol": "GBPUSD", "sequence": 1, "kind": "tick", "price_ticks": 126540, "event_time_ns": 1_793_322_000_000_050_000},
    {"symbol": "GBPUSD", "sequence": 2, "kind": "tick", "price_ticks": 126544, "event_time_ns": 1_793_322_000_000_150_000},
    {"symbol": "GBPUSD", "sequence": 3, "kind": "quote", "bid_ticks": 126541, "ask_ticks": 126549, "event_time_ns": 1_793_322_000_000_250_000},
    {"symbol": "USDJPY", "sequence": 1, "kind": "tick", "price_ticks": 151230, "event_time_ns": 1_793_322_000_000_075_000},
    {"symbol": "USDJPY", "sequence": 2, "kind": "quote", "bid_ticks": 151228, "ask_ticks": 151234, "event_time_ns": 1_793_322_000_000_175_000},
    {"symbol": "USDJPY", "sequence": 3, "kind": "tick", "price_ticks": 151236, "event_time_ns": 1_793_322_000_000_275_000},
    {"symbol": "USDJPY", "sequence": 4, "kind": "tick", "price_ticks": 151240, "event_time_ns": 1_793_322_000_000_375_000},
)

#: FIX-001 duplicates "the fourth logical ID", which in the order above is
#: ``GBPUSD/1``. The duplicate is physical: same identity, same payload.
FIX001_DUPLICATE_INDEX = 3

FIX001_EXPECTED_SOURCE_UNIQUE = 10
FIX001_EXPECTED_CANONICAL_UNIQUE = 10
FIX001_EXPECTED_CONFLICTS = 0
FIX001_EXPECTED_UNEXPECTED_EXTRA = 0

#: Physical Kafka records, *if* the duplicate actually reaches the broker.
FIX001_EXPECTED_KAFKA_PHYSICAL_IF_DUPLICATE_LANDS = 11

#: FIX-001 states this explicitly: do not hardcode 11 raw rows, because an
#: insert-side dedupe may collapse the physical copy. ``None`` means the
#: fixture does not pin it and the run has to report what it measured.
FIX001_EXPECTED_RAW_PHYSICAL: int | None = None
FIX001_RAW_PHYSICAL_NOTE = (
    "raw_physical depends on the retry/sink path; at least 10 in the lab case, "
    "and not asserted as 11 because insert dedupe may drop the physical copy"
)


# ---------------------------------------------------------------------------
# FIX-003: queue stability cannot be checked from a final snapshot
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QueueScenario:
    """The worked example of FIX-003, as checkable numbers."""

    offered_per_s: float = 120_000.0
    sink_per_s: float = 100_000.0
    duration_s: float = 600.0
    free_capacity_records: float = 1_000_000.0

    @property
    def excess_per_s(self) -> float:
        return self.offered_per_s - self.sink_per_s

    @property
    def expected_backlog_growth(self) -> float:
        """20k/s x 600 s = 12,000,000 records with no drop or admission limit."""
        return self.excess_per_s * self.duration_s

    @property
    def expected_time_to_full_s(self) -> float:
        """1M free / 20k per s = 50 s, the idealised figure FIX-003 quotes."""
        return self.free_capacity_records / self.excess_per_s


FIX003_SCENARIO = QueueScenario()
FIX003_EXPECTED_BACKLOG_GROWTH = 12_000_000.0
FIX003_EXPECTED_TIME_TO_FULL_S = 50.0

#: FIX-003's other half: a backlog that reached zero because retention deleted
#: the segments has not been processed, and must not read as recovery.
FIX003_RETENTION_ZERO_IS_NOT_RECOVERY = True


# ---------------------------------------------------------------------------
# FIX-004: the 200M unique tick benchmark
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Fix004Preconditions:
    """Gates FIX-004 requires *before* anyone generates 200M rows.

    All of them are host-dependent, so the defaults are the document's intent
    (an explicit check happened) rather than a sizing recommendation.
    """

    required_unique_ticks: int = 200_000_000
    disk_checked: bool = False
    generation_time_estimated: bool = False
    merge_budget_checked: bool = False
    replay_source_available: bool = False
    chunked_fixtures: bool = True
    notes: list[str] = field(default_factory=lambda: [
        "200M unique canonical ticks is not 200M physical raw rows",
        "primary p95 is client full-response verified RTT; server duration is secondary",
        "warm and cold cache profiles are separate labelled runs",
    ])

    @property
    def ready(self) -> bool:
        return all(
            (
                self.disk_checked,
                self.generation_time_estimated,
                self.merge_budget_checked,
                self.replay_source_available,
            )
        )

    def blockers(self) -> list[str]:
        missing = []
        if not self.disk_checked:
            missing.append("available disk not checked")
        if not self.generation_time_estimated:
            missing.append("dataset generation time not estimated")
        if not self.merge_budget_checked:
            missing.append("memory/merge budget not checked")
        if not self.replay_source_available:
            missing.append("replay source not available")
        return missing


FIX004_PRECONDITIONS = Fix004Preconditions()

#: The named query family the 280 ms figure is attached to (section 1.2, FIX-004).
FIX004_PINNED_QUERY_FAMILY = "Q02"
FIX004_PINNED_CONCURRENCY = 4
FIX004_PINNED_CACHE_PROFILE = "warm"
FIX004_CLIENT_P95_BUDGET_MS = 280.0


ALL_FIXTURES: tuple[str, ...] = ("FIX-001", "FIX-002", "FIX-003", "FIX-004")
