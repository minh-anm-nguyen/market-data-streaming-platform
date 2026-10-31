"""The system-under-test port, and what to do when a stage does not exist.

P08 measures a pipeline whose stages live in other projects. In this repository
P01 is implemented and P02-P07 are design documents, so most of the pipeline
cannot be exercised. Section 20 names the required behaviour:
``BLOCKED_BY_UPSTREAM_IMPLEMENTATION`` / ``NOT_RUN``. It is never a FAIL of the
system under test -- nothing failed, there is nothing there -- and never a pass
by omission.

That rule is enforced structurally. Every implementation declares a
:class:`StageStatus` per stage, and :meth:`SystemUnderTest.require_stages`
raises :class:`~p08_bench.errors.UpstreamUnavailable` listing exactly which
stages are missing. A lane cannot accidentally run against half a pipeline and
report the half that worked.

:attr:`SystemUnderTest.is_simulation` is the second honesty hook. A simulated
pipeline is genuinely useful -- section 8 requires the measuring tools to be
validated against known inputs before they touch a real system -- but a number
measured against a simulation is a fact about the harness, not about Kafka. The
flag travels into the report and the verdict refuses to treat it as capacity
evidence.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Mapping, Protocol, Sequence, runtime_checkable

from .errors import UpstreamUnavailable
from .metrics import StageCounters


class Stage(str, Enum):
    """The pipeline stages of section 2, in order.

    The boundaries between them are the ones section 8 lists as failure
    boundaries, which is why they are enumerated rather than left as strings.
    """

    SOURCE = "P01_SOURCE"
    ADAPTER = "P03_ADAPTER"
    PRODUCER = "P04_PRODUCER"
    BROKER = "P04_BROKER"
    CONSUMER = "P05_CONSUMER"
    RAW = "P06_RAW"
    CANONICAL = "P06_CANONICAL"
    QUERY = "P06_QUERY"


#: Declared separately because an Enum body cannot reference its own members.
STAGE_ORDER: tuple[Stage, ...] = (
    Stage.SOURCE,
    Stage.ADAPTER,
    Stage.PRODUCER,
    Stage.BROKER,
    Stage.CONSUMER,
    Stage.RAW,
    Stage.CANONICAL,
    Stage.QUERY,
)

#: Which project owns each stage, for the message a blocked lane prints.
STAGE_OWNER: Mapping[Stage, str] = {
    Stage.SOURCE: "P01 market data simulator",
    Stage.ADAPTER: "P03 C++ market data adapter",
    Stage.PRODUCER: "P04 Kafka producer",
    Stage.BROKER: "P04 Kafka broker",
    Stage.CONSUMER: "P05 Python batch ingestion",
    Stage.RAW: "P06 ClickHouse raw audit",
    Stage.CANONICAL: "P06 canonical READY build",
    Stage.QUERY: "P06 ClickHouse query workload",
}


class Availability(str, Enum):
    """Why a stage can or cannot be measured.

    ``NOT_IMPLEMENTED`` and ``NOT_CONFIGURED`` are deliberately different: the
    first is "no code exists for this yet", which no amount of lab setup fixes;
    the second is "the code exists and this run was not pointed at it", which is
    an operator problem. Reporting them as one thing would hide which.
    """

    AVAILABLE = "AVAILABLE"
    SIMULATED = "SIMULATED"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"
    NOT_CONFIGURED = "NOT_CONFIGURED"
    UNHEALTHY = "UNHEALTHY"

    @property
    def can_be_exercised(self) -> bool:
        return self in (Availability.AVAILABLE, Availability.SIMULATED)


@dataclass(frozen=True)
class StageStatus:
    stage: Stage
    availability: Availability
    detail: str = ""

    @property
    def usable(self) -> bool:
        return self.availability.can_be_exercised

    def as_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage.value,
            "owner": STAGE_OWNER[self.stage],
            "availability": self.availability.value,
            "usable": self.usable,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class Identity:
    """The business identity of one market event (INV-02).

    ``(dataset_id, symbol, sequence)`` and nothing else. A Kafka offset is not
    an identity: it is a position in one incarnation of one partition, and using
    it as one is the mistake INV-02 names.
    """

    dataset_id: str
    symbol: str
    sequence: int

    def as_tuple(self) -> tuple[str, str, int]:
        return (self.dataset_id, self.symbol, self.sequence)

    def __str__(self) -> str:
        return f"{self.dataset_id}/{self.symbol}/{self.sequence}"


class SubmitStatus(str, Enum):
    """What the pipeline did with one offered event.

    ``REJECTED`` is a business decision by P03 and is not loss (section 0.1).
    ``DROPPED`` is the harness refusing to issue because its own bounded queue
    was full, which invalidates a rate claim rather than describing the system.
    """

    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    BACKPRESSURED = "BACKPRESSURED"
    DROPPED = "DROPPED"
    ERROR = "ERROR"


@dataclass(frozen=True)
class SubmitOutcome:
    status: SubmitStatus
    identity: Identity | None = None
    reason: str = ""

    @property
    def issued(self) -> bool:
        """Whether the event left the driver at all."""
        return self.status not in (SubmitStatus.DROPPED,)


@dataclass
class Backlog:
    """Per-topic-partition backlog observed at one instant.

    Kept as a mapping rather than a total: INV-09 forbids a global average from
    standing in for the per-partition picture, and a total is one step from an
    average.
    """

    per_partition: dict[tuple[str, int], int] = field(default_factory=dict)
    retention_truncated: set[tuple[str, int]] = field(default_factory=set)

    def set(self, topic: str, partition: int, records: int) -> None:
        self.per_partition[(topic, partition)] = records

    def worst(self) -> int | None:
        return max(self.per_partition.values()) if self.per_partition else None

    def as_dict(self) -> dict[str, Any]:
        return {
            "per_partition": {f"{t}-{p}": v for (t, p), v in sorted(self.per_partition.items())},
            "retention_truncated": [f"{t}-{p}" for t, p in sorted(self.retention_truncated)],
            "worst_partition_records": self.worst(),
        }


@dataclass
class Observation:
    """One scrape of the pipeline: counters plus backlog, at a timestamp."""

    t_ns: int
    counters: StageCounters
    backlog: Backlog
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "t_ns": self.t_ns,
            "counters": self.counters.as_dict(),
            "backlog": self.backlog.as_dict(),
            "notes": list(self.notes),
        }


@runtime_checkable
class SystemUnderTest(Protocol):
    """What a benchmark lane needs from whatever it is measuring."""

    @property
    def name(self) -> str: ...

    @property
    def is_simulation(self) -> bool:
        """True when results describe the harness rather than a real pipeline."""
        ...

    def stage_statuses(self) -> Sequence[StageStatus]: ...

    def submit(self, identity: Identity, payload: Mapping[str, Any], t_ns: int) -> SubmitOutcome: ...

    def advance_to(self, t_ns: int) -> None:
        """Let the pipeline do work up to ``t_ns``.

        Present so a deterministic simulation and a real pipeline share one
        driver loop: against a real system this is a no-op, because wall time
        advances on its own.
        """
        ...

    def observe(self, t_ns: int) -> Observation: ...

    def drain(self, deadline_ns: int) -> Observation:
        """Stop accepting work and let the backlog clear until the deadline."""
        ...

    def canonical_identities(self) -> set[tuple[str, str, int]]:
        """Identities present in the verified READY build."""
        ...


class SutBase:
    """Shared stage bookkeeping and the blocked-lane refusal."""

    #: Subclasses override. Mapping every stage to its status.
    STAGES: Mapping[Stage, StageStatus] = {}

    @property
    def name(self) -> str:
        return type(self).__name__

    @property
    def is_simulation(self) -> bool:
        return any(
            status.availability is Availability.SIMULATED
            for status in self.stage_statuses()
        )

    def stage_statuses(self) -> Sequence[StageStatus]:
        return [self.STAGES[stage] for stage in STAGE_ORDER if stage in self.STAGES]

    def status_of(self, stage: Stage) -> StageStatus:
        return self.STAGES.get(
            stage,
            StageStatus(stage, Availability.NOT_IMPLEMENTED, "stage not declared by this SUT"),
        )

    def unusable_stages(self, required: Iterable[Stage]) -> list[StageStatus]:
        return [self.status_of(stage) for stage in required if not self.status_of(stage).usable]

    def require_stages(self, required: Iterable[Stage]) -> None:
        """Raise unless every required stage can be exercised.

        The error carries the stage list and the owning project, so a blocked
        lane reports *which* implementation it is waiting on rather than a bare
        failure (section 20).
        """
        missing = self.unusable_stages(required)
        if missing:
            raise UpstreamUnavailable(
                "lane needs stages that cannot be exercised here: "
                + ", ".join(
                    f"{s.stage.value} ({s.availability.value}, owned by {STAGE_OWNER[s.stage]})"
                    for s in missing
                ),
                sut=self.name,
                stages=[s.stage.value for s in missing],
                availability={s.stage.value: s.availability.value for s in missing},
            )

    def stage_report(self) -> dict[str, Any]:
        statuses = self.stage_statuses()
        return {
            "sut": self.name,
            "is_simulation": self.is_simulation,
            "stages": [status.as_dict() for status in statuses],
            "usable_stages": [s.stage.value for s in statuses if s.usable],
            "blocked_stages": [s.stage.value for s in statuses if not s.usable],
            "evidence_scope": (
                "harness self-test: numbers describe the measurement tools, not a "
                "real pipeline"
                if self.is_simulation
                else "measurements describe the configured pipeline"
            ),
        }


#: Stage sets the documented lanes require, so a lane declares its dependency
#: once instead of each caller guessing.
LANE_REQUIREMENTS: Mapping[str, tuple[Stage, ...]] = {
    "L00": (Stage.SOURCE,),
    "L01": STAGE_ORDER[:-1],
    "L02": STAGE_ORDER[:-1],
    "L03": STAGE_ORDER[:-1],
    "L04": STAGE_ORDER[:-1],
    "L05": STAGE_ORDER[:-1],
    "L06": STAGE_ORDER[:-1],
    "L07": STAGE_ORDER[:-1],
    "L08": STAGE_ORDER[:-1],
    "L09": (Stage.CANONICAL, Stage.QUERY),
    "L10": STAGE_ORDER,
    "L11": STAGE_ORDER,
    "L12": STAGE_ORDER,
}


def requirements_for(lane: str) -> tuple[Stage, ...]:
    """Stages a lane needs, defaulting to the whole pipeline.

    An unknown lane defaults to requiring everything rather than nothing: the
    conservative direction is to block, not to run half a pipeline quietly.
    """
    return LANE_REQUIREMENTS.get(lane, STAGE_ORDER)
