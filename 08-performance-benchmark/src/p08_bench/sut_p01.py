"""The real P01 simulator as a source stage; everything downstream is blocked.

P01 is the one pipeline component that exists as running code in this
repository, so it is the one stage P08 can measure for real. This adapter reads
a sealed P01 run directory -- manifest, ``COMPLETED`` marker, hashed
``events.jsonl`` -- and exposes it as :class:`~p08_bench.sut.Stage.SOURCE`.

Every other stage reports ``NOT_IMPLEMENTED``. That is the honest answer: P02 to
P07 are design documents. A lane that needs Kafka therefore raises
:class:`~p08_bench.errors.UpstreamUnavailable` naming the project it is waiting
on, which is what section 20 asks for, instead of producing a number from the
part of the pipeline that happens to exist.

What can genuinely be measured here is the source side: whether the dataset is
sealed, how fast it can be read and replayed, and what the true expected
identity set is. That last one matters most -- it is the input the independent
oracle needs, and it comes from the sealed file rather than from any component
counter.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping

from .errors import UpstreamUnavailable
from .manifest import SourceSeal
from .metrics import StageCounters
from .sut import (
    Availability,
    Backlog,
    Identity,
    Observation,
    Stage,
    StageStatus,
    SubmitOutcome,
    SubmitStatus,
    SutBase,
)

#: Fields a P01 canonical record always carries (P01 model contract).
REQUIRED_EVENT_FIELDS = ("dataset_id", "symbol", "sequence", "event_time_ns", "event_type")


@dataclass(frozen=True)
class SourceEvent:
    """One sealed P01 event, parsed into identity plus payload.

    ``payload`` keeps the integer fixed-point fields exactly as written. INV-03
    forbids rounding prices to floats anywhere in P08, so nothing here converts
    them.
    """

    identity: Identity
    event_type: str
    event_time_ns: int
    payload: dict[str, Any]

    @property
    def key(self) -> tuple[str, str, int]:
        return self.identity.as_tuple()

    @property
    def is_tick(self) -> bool:
        return self.event_type == "tick"


class P01SourceReader:
    """Streaming reader for a sealed P01 dataset.

    Streaming, not loading: section 9.2 is explicit that a 200M-identity compare
    must not pull the whole set into RAM, and the same applies to reading the
    source. Callers that need the identity set build it themselves and accept
    the memory cost knowingly.
    """

    __slots__ = ("run_dir", "seal")

    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.seal = SourceSeal.from_p01_run_dir(run_dir)

    @property
    def events_path(self) -> Path:
        return Path(self.seal.path)

    def events(self) -> Iterator[SourceEvent]:
        """Yield every sealed event in file order.

        Raises when the source is not sealed. Reading a ``.part`` file would
        produce an expected set that is short through no fault of the pipeline,
        which is case G01-03.
        """
        if not self.seal.sealed:
            raise UpstreamUnavailable(
                f"source dataset is {self.seal.state.value}; it cannot anchor an "
                "expected set",
                path=self.seal.path,
                reasons=list(self.seal.reasons),
            )
        with self.events_path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                yield self._parse(line, line_no)

    @staticmethod
    def _parse(line: str, line_no: int) -> SourceEvent:
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise UpstreamUnavailable(
                f"sealed source line {line_no} does not parse", detail=str(exc)
            ) from exc
        missing = [field for field in REQUIRED_EVENT_FIELDS if field not in record]
        if missing:
            raise UpstreamUnavailable(
                f"sealed source line {line_no} is missing required fields",
                fields=missing,
            )
        identity = Identity(
            dataset_id=str(record["dataset_id"]),
            symbol=str(record["symbol"]),
            sequence=int(record["sequence"]),
        )
        return SourceEvent(
            identity=identity,
            event_type=str(record["event_type"]),
            event_time_ns=int(record["event_time_ns"]),
            payload={k: v for k, v in record.items()},
        )

    def identity_set(self) -> set[tuple[str, str, int]]:
        """Every identity in the sealed file.

        Memory is one tuple per event. Fine for a golden fixture; for a 200M
        lane the chunked comparison in :mod:`p08_bench.reconciliation` is the
        right tool instead.
        """
        return {event.key for event in self.events()}

    def counts_by_type(self) -> dict[str, int]:
        """Tick and quote counts, kept separate (INV-12)."""
        counts: dict[str, int] = {}
        for event in self.events():
            counts[event.event_type] = counts.get(event.event_type, 0) + 1
        return counts

    def as_dict(self) -> dict[str, Any]:
        return {"run_dir": str(self.run_dir), "seal": self.seal.as_dict()}


class P01SourcePipeline(SutBase):
    """A SUT whose only usable stage is the P01 source.

    Submitting is accepted and counted, and then the event has nowhere to go:
    there is no adapter to hand it to. So ``submit`` records the offer and says
    ``ACCEPTED`` for the source stage alone, while
    :meth:`~p08_bench.sut.SutBase.require_stages` refuses any lane that needs
    more. The alternative -- pretending to accept into a pipeline that does not
    exist -- would let a lane report a throughput for nothing.
    """

    def __init__(self, reader: P01SourceReader) -> None:
        self.reader = reader
        source_detail = (
            f"sealed P01 dataset at {reader.seal.path}"
            if reader.seal.sealed
            else f"P01 dataset is {reader.seal.state.value}: " + "; ".join(reader.seal.reasons)
        )
        self.STAGES = {
            Stage.SOURCE: StageStatus(
                Stage.SOURCE,
                Availability.AVAILABLE if reader.seal.sealed else Availability.UNHEALTHY,
                source_detail,
            ),
            **{
                stage: StageStatus(
                    stage,
                    Availability.NOT_IMPLEMENTED,
                    "design document only; no implementation in this repository",
                )
                for stage in (
                    Stage.ADAPTER, Stage.PRODUCER, Stage.BROKER, Stage.CONSUMER,
                    Stage.RAW, Stage.CANONICAL, Stage.QUERY,
                )
            },
        }
        self._offered = 0
        self._issued = 0
        self._now_ns = 0

    @property
    def is_simulation(self) -> bool:
        """False: the source really is P01. The rest is simply absent."""
        return False

    def submit(self, identity: Identity, payload: Mapping[str, Any], t_ns: int) -> SubmitOutcome:
        self._now_ns = max(self._now_ns, t_ns)
        self._offered += 1
        if not self.reader.seal.sealed:
            return SubmitOutcome(
                SubmitStatus.ERROR, identity, f"source is {self.reader.seal.state.value}"
            )
        self._issued += 1
        return SubmitOutcome(SubmitStatus.ACCEPTED, identity, "source stage only")

    def advance_to(self, t_ns: int) -> None:
        self._now_ns = max(self._now_ns, t_ns)

    def observe(self, t_ns: int) -> Observation:
        self.advance_to(t_ns)
        return Observation(
            t_ns=t_ns,
            counters=StageCounters(
                offered_count=self._offered,
                issued_count=self._issued,
                source_sealed_count=self.reader.seal.event_count,
                # Everything downstream is absent. ``None`` is the only honest
                # value: a zero would read as "measured, never happened".
                adapter_accepted_count=None,
                producer_ack_count=None,
                kafka_physical_count=None,
                raw_physical_count=None,
                canonical_unique_count=None,
                oracle_expected_count=None,
            ),
            backlog=Backlog(),
            notes=[
                "only the P01 source stage exists; P02-P07 are design documents",
            ],
        )

    def drain(self, deadline_ns: int) -> Observation:
        return self.observe(deadline_ns)

    def canonical_identities(self) -> set[tuple[str, str, int]]:
        """Always empty, and that is not a loss finding.

        There is no canonical build to read. Reconciliation against this SUT
        must not run; the lane is blocked, and :meth:`require_stages` is what
        says so.
        """
        raise UpstreamUnavailable(
            "no canonical READY build exists; P06 is not implemented",
            sut=self.name,
            stages=[Stage.CANONICAL.value],
        )
