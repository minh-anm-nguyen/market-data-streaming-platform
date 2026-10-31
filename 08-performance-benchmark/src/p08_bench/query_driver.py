"""The query driver and the result oracle (sections 3.3, 4.3; INV-13, case E2E-38).

A query lane measures two things at once, and section 4.3 insists they stay
apart:

* ``range_query_client_rtt`` -- send to fully-received-and-verified response,
  measured on one monotonic clock in the driver. This is the primary number the
  280 ms budget attaches to.
* ``range_query_server_ms`` -- the server-side duration from
  ``system.query_log``. Useful for finding the bottleneck, never a substitute
  for the client number, and joined by ``query_id`` rather than by timing
  coincidence.

The oracle is the part that makes "fast" mean something. Case E2E-38 is a driver
that returns empty results in 1 ms: every latency gate passes and the lane must
still fail. So :class:`QueryResultOracle` checks row count, an ordered digest of
the returned rows, and emptiness, and a response that fails any of those is
recorded as completed-but-incorrect -- which keeps it in the denominator while
excluding it from the latency distribution (INV-13, INV-16).

A timeout gets no latency at all. It is a right-censored observation and the
registry in :mod:`p08_bench.samples` is what carries it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping, Sequence

from .clock import FakeClock, MonotonicClock
from .errors import ConfigError, MetrologyError
from .manifest import QueryCatalogEntry, digest_of
from .percentiles import (
    PercentileBasis,
    PercentileClaim,
    PercentileResult,
    sample_count_adequate,
)
from .samples import AttemptRegistry, Outcome

NS_PER_S = 1_000_000_000


class QueryFailure(str, Enum):
    """Why one query was not a success."""

    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"
    WRONG_ROW_COUNT = "WRONG_ROW_COUNT"
    WRONG_DIGEST = "WRONG_DIGEST"
    EMPTY_RESULT = "EMPTY_RESULT"


@dataclass(frozen=True)
class QueryResponse:
    """What the client got back from one query.

    ``rows`` are the values as returned. Nothing here converts a fixed-point
    integer to a float: INV-03 applies to the comparison path as much as to the
    pipeline, and a float round-trip is how an exact digest stops being exact.
    """

    query_id: str
    rows: Sequence[Mapping[str, Any]]
    server_duration_ms: float | None = None
    read_rows: int | None = None
    read_bytes: int | None = None
    error: str | None = None
    timed_out: bool = False

    @property
    def row_count(self) -> int:
        return len(self.rows)


@dataclass(frozen=True)
class ExpectedResult:
    """The answer key for one query, computed independently of the server.

    ``row_count`` and ``digest`` are separate checks on purpose: a result with
    the right number of wrong rows passes the first and fails the second, and
    knowing which one broke is the difference between a pagination bug and a
    data bug.
    """

    row_count: int
    digest: str
    allow_empty: bool = False

    @classmethod
    def from_rows(
        cls, rows: Sequence[Mapping[str, Any]], *, allow_empty: bool = False
    ) -> "ExpectedResult":
        return cls(
            row_count=len(rows),
            digest=digest_of([dict(sorted(row.items())) for row in rows]),
            allow_empty=allow_empty,
        )

    def as_dict(self) -> dict[str, Any]:
        return {"row_count": self.row_count, "digest": self.digest, "allow_empty": self.allow_empty}


@dataclass(frozen=True)
class OracleVerdict:
    correct: bool
    failure: QueryFailure | None = None
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "correct": self.correct,
            "failure": self.failure.value if self.failure else None,
            "detail": self.detail,
        }


class QueryResultOracle:
    """Compares a response against an independently computed expected result."""

    __slots__ = ("expected",)

    def __init__(self, expected: Mapping[str, ExpectedResult]) -> None:
        if not expected:
            raise ConfigError("the oracle needs at least one expected result")
        self.expected = dict(expected)

    def judge(self, binding_key: str, response: QueryResponse) -> OracleVerdict:
        if response.timed_out:
            return OracleVerdict(False, QueryFailure.TIMEOUT, "deadline exceeded")
        if response.error:
            return OracleVerdict(False, QueryFailure.ERROR, response.error)

        expected = self.expected.get(binding_key)
        if expected is None:
            # An unchecked response is an evidence gap, not a pass (INV-13).
            raise MetrologyError(
                "no expected result for this binding; a response cannot be "
                "judged correct without an answer key",
                binding_key=binding_key,
            )

        if response.row_count == 0 and not expected.allow_empty:
            # Case E2E-38: all-empty results at 1 ms must fail regardless of
            # how good the latency looks.
            return OracleVerdict(
                False, QueryFailure.EMPTY_RESULT,
                "response is empty but the expected result is not",
            )
        if response.row_count != expected.row_count:
            return OracleVerdict(
                False, QueryFailure.WRONG_ROW_COUNT,
                f"got {response.row_count} rows, expected {expected.row_count}",
            )
        actual = digest_of([dict(sorted(row.items())) for row in response.rows])
        if actual != expected.digest:
            return OracleVerdict(
                False, QueryFailure.WRONG_DIGEST,
                f"row digest {actual[:16]}... does not match {expected.digest[:16]}...",
            )
        return OracleVerdict(True)


#: A query executor takes (catalog entry, binding key, binds) and returns a
#: response. Real implementations talk to ClickHouse; tests supply a stub.
QueryExecutor = Callable[[QueryCatalogEntry, str, Mapping[str, Any]], QueryResponse]


@dataclass(frozen=True)
class QueryBinding:
    """One scheduled query: which catalog entry, which binds, when it is due."""

    key: str
    due_ns: int
    binds: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class QueryLaneResult:
    """Everything a query lane produced, including what it may not claim."""

    registry: AttemptRegistry
    entry: QueryCatalogEntry
    concurrency: int
    cache_profile: str
    dataset_build_id: str | None
    server_durations_ms: list[float]
    failures_by_kind: dict[str, int]
    min_complete_samples: int

    def claim(self, q: float = 0.95) -> PercentileClaim:
        """The client-RTT percentile, labelled with its workload (INV-17)."""
        return PercentileClaim(
            result=self.registry.percentile(q, basis=PercentileBasis.ATTEMPTED_WITH_CENSORING),
            query_family=self.entry.qid,
            concurrency=self.concurrency,
            cache_profile=self.cache_profile,
            dataset_build_id=self.dataset_build_id,
            labels={"query_sql_sha256": self.entry.sql_sha256},
        )

    def conditional_claim(self, q: float = 0.95) -> PercentileClaim:
        """The completed-only percentile, which is a conditional distribution."""
        return PercentileClaim(
            result=self.registry.percentile(q, basis=PercentileBasis.COMPLETED_ONLY),
            query_family=self.entry.qid,
            concurrency=self.concurrency,
            cache_profile=self.cache_profile,
            dataset_build_id=self.dataset_build_id,
            labels={"basis": "completed_only"},
        )

    def server_percentile(self, q: float = 0.95) -> PercentileResult | None:
        """Server-side duration percentile, for bottleneck analysis only.

        Returned as a bare :class:`PercentileResult` rather than a
        :class:`PercentileClaim` so it cannot be handed to something that
        compares it against the client budget (INV-14).
        """
        if not self.server_durations_ms:
            return None
        from .percentiles import percentile_completed_only

        return percentile_completed_only(self.server_durations_ms, q)

    @property
    def sample_count_adequate(self) -> bool:
        return self.registry.correct_completed >= self.min_complete_samples

    def blockers(self) -> list[str]:
        """Why this lane cannot support a latency claim, if it cannot."""
        out: list[str] = []
        if not self.sample_count_adequate:
            out.append(
                f"{self.registry.correct_completed} correct completions is below the "
                f"configured floor of {self.min_complete_samples}"
            )
        if not sample_count_adequate(self.registry.correct_completed):
            out.append(
                "below the NFR-08 guardrail of 10000 completed samples; the p95 is "
                "discovery-grade, not a claim"
            )
        if self.registry.unchecked_completed:
            out.append(
                f"{self.registry.unchecked_completed} response(s) were never compared "
                "against the oracle; an unchecked result is an evidence gap (INV-13)"
            )
        if self.registry.incorrect_completed:
            out.append(
                f"{self.registry.incorrect_completed} response(s) were incorrect; "
                "latency is irrelevant until results are right"
            )
        return out

    def as_dict(self) -> dict[str, Any]:
        claim = self.claim()
        server = self.server_percentile()
        return {
            "query": self.entry.as_dict(),
            "concurrency": self.concurrency,
            "cache_profile": self.cache_profile,
            "dataset_build_id": self.dataset_build_id,
            "attempts": self.registry.summary(),
            "failures_by_kind": dict(sorted(self.failures_by_kind.items())),
            "client_p95": claim.as_dict(),
            # Named to make the distinction of INV-14 impossible to miss.
            "server_p95_secondary_diagnostic": server.as_dict() if server else None,
            "sample_count_adequate": self.sample_count_adequate,
            "blockers": self.blockers(),
        }


class QueryDriver:
    """Issues a query schedule, times the client RTT and judges every response."""

    __slots__ = ("entry", "oracle", "executor", "clock", "concurrency", "cache_profile", "dataset_build_id", "min_complete_samples")

    def __init__(
        self,
        entry: QueryCatalogEntry,
        oracle: QueryResultOracle,
        executor: QueryExecutor,
        clock: MonotonicClock | FakeClock,
        *,
        concurrency: int = 1,
        cache_profile: str = "warm",
        dataset_build_id: str | None = None,
        min_complete_samples: int = 1,
    ) -> None:
        if concurrency <= 0:
            raise ConfigError("concurrency must be positive", value=concurrency)
        self.entry = entry
        self.oracle = oracle
        self.executor = executor
        self.clock = clock
        self.concurrency = concurrency
        self.cache_profile = cache_profile
        self.dataset_build_id = dataset_build_id
        self.min_complete_samples = min_complete_samples

    def run(self, bindings: Sequence[QueryBinding]) -> QueryLaneResult:
        """Issue every binding and record the outcome of each.

        Every binding becomes exactly one attempt in the registry, including the
        ones that time out and the ones that come back wrong. That is what keeps
        ``scheduled`` as the denominator (case E2E-41).
        """
        registry = AttemptRegistry(
            f"query:{self.entry.qid}", deadline_ns=self.entry.client_timeout_ms * 1_000_000
        )
        server_durations: list[float] = []
        failures: dict[str, int] = {}

        for seq, binding in enumerate(bindings):
            if isinstance(self.clock, FakeClock) and self.clock.now_ns() < binding.due_ns:
                self.clock.set_ns(binding.due_ns)
            issued_ns = self.clock.now_ns()

            try:
                response = self.executor(self.entry, binding.key, binding.binds)
            except Exception as exc:  # noqa: BLE001 - an executor failure is a datum
                registry.record(
                    seq, binding.due_ns, issued_ns=issued_ns,
                    outcome=Outcome.ERROR, reason=f"executor raised: {exc}",
                )
                failures[QueryFailure.ERROR.value] = failures.get(QueryFailure.ERROR.value, 0) + 1
                continue

            completed_ns = self.clock.now_ns()
            if completed_ns < issued_ns:
                raise MetrologyError(
                    "query completed before it was issued", query_id=response.query_id
                )

            verdict = self.oracle.judge(binding.key, response)
            if response.server_duration_ms is not None:
                server_durations.append(response.server_duration_ms)

            if verdict.failure is QueryFailure.TIMEOUT:
                # A censored observation: no latency, still in the denominator.
                registry.record(
                    seq, binding.due_ns, issued_ns=issued_ns,
                    outcome=Outcome.TIMEOUT, reason=verdict.detail,
                )
            elif verdict.failure is QueryFailure.ERROR:
                registry.record(
                    seq, binding.due_ns, issued_ns=issued_ns,
                    outcome=Outcome.ERROR, reason=verdict.detail,
                )
            else:
                registry.record(
                    seq, binding.due_ns, issued_ns=issued_ns, completed_ns=completed_ns,
                    outcome=Outcome.COMPLETED, correct=verdict.correct,
                    reason=verdict.detail or None,
                )
            if verdict.failure is not None:
                failures[verdict.failure.value] = failures.get(verdict.failure.value, 0) + 1

        return QueryLaneResult(
            registry=registry,
            entry=self.entry,
            concurrency=self.concurrency,
            cache_profile=self.cache_profile,
            dataset_build_id=self.dataset_build_id,
            server_durations_ms=server_durations,
            failures_by_kind=failures,
            min_complete_samples=self.min_complete_samples,
        )


def uniform_bindings(
    keys: Sequence[str], *, qps: float, start_ns: int = 0, binds_for: Callable[[str], Mapping[str, Any]] | None = None
) -> list[QueryBinding]:
    """Evenly spaced bindings at ``qps``, cycling through ``keys``.

    Open loop like the event schedule: due times are fixed up front, so a slow
    server produces late and censored observations rather than a thinner
    schedule.
    """
    if not keys:
        raise ConfigError("need at least one binding key")
    if qps <= 0:
        raise ConfigError("qps must be positive", value=qps)
    step_ns = int(NS_PER_S / qps)
    return [
        QueryBinding(
            key=key,
            due_ns=start_ns + index * step_ns,
            binds=dict(binds_for(key)) if binds_for else {},
        )
        for index, key in enumerate(keys)
    ]
