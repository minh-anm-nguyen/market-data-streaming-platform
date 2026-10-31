"""Walk one scenario through the lifecycle, collecting evidence at each stage.

:class:`ScenarioRun` is the integration point: it owns the simulated data plane,
the evidence store, the journal and the reconcilers, and exposes the lifecycle
as explicit steps. Tests drive the steps directly, because the interesting cases
need a fault between two specific stages; :meth:`ScenarioRun.run_clean` walks
all of them for the no-fault baseline.

Three design rules are enforced here rather than left to each caller.

**Findings accumulate; they are never cleared.** Every stage appends to
``self.findings``, and :meth:`gate` classifies the whole list. A later stage
succeeding cannot erase an earlier contradiction.

**Publication requires the whole path.** :meth:`verify_and_publish` checks
:func:`~p07.orchestration.run_state.ready_preconditions_met` against the run's
*history* before it will mark a build verified. Skipping a stage is therefore not
possible by forgetting to call it (``INV-05``).

**The profile travels with the verdict.** Every report carries
``profile: "UNIT/MODEL"`` and the ``PROFILE_NOT_VALIDATED`` note, so a ``PASS``
from this harness can never be read as a statement about a real broker or a real
ClickHouse (design 9.3).
"""

from __future__ import annotations

import platform
import sys
import time
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Callable, Mapping, Sequence

from ..dataplane.adapter import Adapter, limits_from_sealed
from ..dataplane.analytics import BuildPipeline, BuildRefused, RawAudit
from ..dataplane.clickhouse_fake import FakeClickHouse
from ..dataplane.codec import Codec, DEFAULT_CODEC
from ..dataplane.ingestion import BatchIngestor, PoisonRecord
from ..dataplane.kafka_fake import (
    DEFAULT_MIN_ISR,
    DEFAULT_PARTITIONS,
    FakeKafka,
    OffsetExpired,
)
from ..dataplane.producer import Producer, QueueFull
from ..dataset_sealer import SealedDataset, seal_dataset
from ..evidence.artifact_store import ArtifactStore, QuotaExceeded
from ..evidence.proof_ledger import FaultReceipt, Phase, ProofLedger, summarise_receipts
from ..evidence.verdicts import Finding, GateResult, Reason, Verdict, classify
from ..injectors.base import DEFAULT_ALLOWLIST, LabAllowlist
from ..injectors.resources import ResourceBudget, check_budget, rss_bytes
from ..oracle.acceptance_oracle import AcceptanceExpectation, PolicyConfig, build_expectation
from ..reconcile.bar_reconciler import BarReconciliation, reconcile_bars
from ..reconcile.canonical_reconciler import CanonicalReconciliation, reconcile_canonical
from ..reconcile.raw_reconciler import (
    RawReconciliation,
    TransportReconciliation,
    frontier_findings,
    reconcile_kafka_cut,
    reconcile_raw,
)
from ..recovery.cut_coordinator import CutRejected, capture_cut, cut_findings
from ..recovery.recovery_coordinator import RecoveryMetrics
from ..replay.kafka_cut_scanner import CutInventory, CutUnscannable, KafkaCut, scan_cut
from ..replay.source_replayer import ReplayReport, TransportFaults, replay_source
from .run_state import RunState, State, ready_preconditions_met

#: The only profile this package implements. Stated as data so it ends up in
#: every artifact rather than only in prose.
PROFILE = "UNIT/MODEL"

PROFILE_NOTE = (
    "In-process simulation of P02-P06. Proves protocol and state-machine logic only. "
    "It does NOT exercise a real Kafka broker, ClickHouse server, cgroups, or the network "
    "stack, so no result here supports a claim about real-broker behaviour, HA or throughput "
    "(design 2.2, 9.3)."
)


@dataclass(frozen=True, slots=True)
class LabConfig:
    """Topology and limits for one run. Written verbatim into the bundle."""

    partitions: int = DEFAULT_PARTITIONS
    brokers: int = 3
    replication_factor: int = 3
    min_insync_replicas: int = DEFAULT_MIN_ISR
    group: str = "p05-ingest"
    batch_max_records: int = 500
    producer_idempotent: bool = True
    kafka_idempotent_dedupe: bool = True
    strict_poison: bool = True
    policy: PolicyConfig = field(default_factory=PolicyConfig)
    budget: ResourceBudget = field(default_factory=ResourceBudget)
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST

    def as_dict(self) -> dict[str, Any]:
        return {
            "profile": PROFILE,
            "partitions_per_topic": self.partitions,
            "brokers": self.brokers,
            "replication_factor": self.replication_factor,
            "min_insync_replicas": self.min_insync_replicas,
            "group": self.group,
            "batch_max_records": self.batch_max_records,
            "producer_idempotent": self.producer_idempotent,
            "kafka_idempotent_dedupe": self.kafka_idempotent_dedupe,
            "strict_poison": self.strict_poison,
            "budget": self.budget.as_dict(),
            "lab_name": self.allowlist.lab_name,
            "allowlisted_targets": sorted(self.allowlist.targets),
        }


@lru_cache(maxsize=1)
def _static_environment() -> dict[str, Any]:
    """The parts of the fingerprint that cannot change within a process.

    Cached because ``platform.processor()`` queries WMI on Windows and takes
    seconds; paying that once per process instead of once per run is the
    difference between a usable test suite and an unusable one.
    """
    try:
        from google.protobuf import __version__ as protobuf_version
    except ImportError:  # pragma: no cover - protobuf is a hard dependency
        protobuf_version = "unavailable"
    return {
        "python_version": sys.version.split()[0],
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "protobuf_version": protobuf_version,
        "profile": PROFILE,
        "profile_note": PROFILE_NOTE,
    }


def environment_fingerprint() -> dict[str, Any]:
    """What the run ran on. Enough to tell two runs apart, no secrets."""
    return {**_static_environment(), "rss_bytes_at_start": rss_bytes()}


@dataclass(slots=True)
class StageResults:
    """Everything the stages produced, for the report and for assertions."""

    replay: ReplayReport | None = None
    cut: KafkaCut | None = None
    inventory: CutInventory | None = None
    transport: TransportReconciliation | None = None
    drain: dict[str, Any] | None = None
    audit: RawAudit | None = None
    raw: RawReconciliation | None = None
    canonical: CanonicalReconciliation | None = None
    bars: BarReconciliation | None = None
    published: bool | None = None
    build_id: str | None = None


class ScenarioRun:
    """One scenario, one run id, one evidence bundle."""

    def __init__(
        self,
        *,
        source_run_dir: str,
        store: ArtifactStore,
        scenario_id: str,
        run_id: str,
        test_case_id: str = "",
        lab: LabConfig | None = None,
        codec: Codec = DEFAULT_CODEC,
        source_anchor: str | None = None,
        require_fault_plan: bool = False,
    ) -> None:
        self.scenario_id = scenario_id
        self.run_id = run_id
        self.test_case_id = test_case_id
        self.lab = lab or LabConfig()
        self.codec = codec
        self.store = store
        self.state = RunState()
        self.findings: list[Finding] = []
        self.results = StageResults()
        self.metrics = RecoveryMetrics()
        self.receipts: list[FaultReceipt] = []
        self.fault_expected = False
        self._source_run_dir = source_run_dir
        self._source_anchor = source_anchor
        self._require_fault_plan = require_fault_plan
        self._environment = environment_fingerprint()

        self.store.write_json("environment.json", self._environment)
        self.store.write_json("config.effective.json", self.lab.as_dict())
        self.ledger = ProofLedger(
            store,
            scenario_id=scenario_id,
            run_id=run_id,
            test_case_id=test_case_id,
            env_fingerprint=self._environment["platform"],
        )
        self.ledger.record(phase=Phase.CREATED, actor="orchestrator", operation="run_created")

        # Populated by :meth:`start_dataplane`.
        self.dataset: SealedDataset | None = None
        self.expectation: AcceptanceExpectation | None = None
        self.kafka: FakeKafka | None = None
        self.sink: FakeClickHouse | None = None
        self.producer: Producer | None = None
        self.adapter: Adapter | None = None
        self.ingestor: BatchIngestor | None = None
        self.builder: BuildPipeline | None = None

    # -- helpers ----------------------------------------------------------

    def add_findings(self, findings: Sequence[Finding]) -> None:
        self.findings.extend(findings)

    def record_receipt(self, receipt: FaultReceipt) -> FaultReceipt:
        """Store a fault receipt and remember that a fault was expected."""
        self.fault_expected = True
        self.receipts.append(receipt)
        self.ledger.record_receipt(receipt)
        if receipt.confirmed and self.state.state is State.RUNNING:
            self.state.to(State.FAULT_CONFIRMED)
        return receipt

    def _write(self, path: str, payload: Any) -> None:
        """Write an artifact, turning a quota overrun into a finding.

        A quota overrun must end the run as ``INCONCLUSIVE`` (design 8), so it
        is recorded rather than raised: raising here would lose the findings
        already collected, which are the only reason the bundle exists.
        """
        try:
            self.store.write_json(path, payload)
        except QuotaExceeded as exc:
            self.add_findings(
                [Finding(Reason.EVIDENCE_QUOTA_EXCEEDED, {"path": exc.path, "needed": exc.needed})]
            )

    # -- stages -----------------------------------------------------------

    def seal(self) -> SealedDataset:
        """Seal the source and build the expected sets."""
        self.state.to(State.ENV_READY)
        self.ledger.record(phase=Phase.ENV_READY, actor="orchestrator", operation="env_ready")
        dataset = seal_dataset(
            self._source_run_dir,
            anchor=self._source_anchor,
            require_fault_plan=self._require_fault_plan,
        )
        self.dataset = dataset
        self.state.to(State.SOURCE_SEALED)
        self._write("source/manifest.json", dataset.seal_receipt())
        self.ledger.record(
            phase=Phase.SOURCE_SEALED,
            actor="dataset_sealer",
            operation="seal",
            detail={"dataset_id": dataset.dataset_id, "lines": dataset.event_lines},
            evidence_artifact_refs=("source/manifest.json",),
        )
        if not dataset.anchor_verified:
            # Self-consistent is not the same as unmodified. Design 5.1 and case
            # G01-03: without an external anchor, source integrity is unproven.
            self.add_findings(
                [
                    Finding(
                        Reason.SOURCE_ANCHOR_MISSING,
                        {"dataset_id": dataset.dataset_id, "events_sha256": dataset.events_sha256},
                    )
                ]
            )

        expectation = build_expectation(dataset, config=self.lab.policy)
        self.expectation = expectation
        self._write("source/expected_policy.json", expectation.receipt())
        if expectation.source_identity_conflicts:
            self.add_findings(
                [
                    Finding(
                        Reason.SOURCE_IDENTITY_CONFLICT,
                        {
                            "count": len(expectation.source_identity_conflicts),
                            "sample": [
                                {"identity": list(identity), "fields": list(fields)}
                                for identity, fields in expectation.source_identity_conflicts[:10]
                            ],
                        },
                    )
                ]
            )
        if expectation.untracked_anomalies:
            self.add_findings(
                [
                    Finding(
                        Reason.SOURCE_UNTRACKED_ANOMALY,
                        {
                            "count": len(expectation.untracked_anomalies),
                            "sample": [
                                event.as_dict() for event in expectation.untracked_anomalies[:10]
                            ],
                        },
                    )
                ]
            )
        if expectation.symbols_without_events:
            self.add_findings(
                [
                    Finding(
                        Reason.SOURCE_SYMBOL_MISSING,
                        {"symbols": list(expectation.symbols_without_events)},
                    )
                ]
            )
        return dataset

    def start_dataplane(
        self,
        *,
        ingest_crash_hook: Callable[..., None] | None = None,
        build_crash_hook: Callable[..., None] | None = None,
    ) -> None:
        """Construct the simulated P02-P06 components."""
        if self.dataset is None:
            raise RuntimeError("seal() must run before start_dataplane()")
        self.kafka = FakeKafka(
            brokers=self.lab.brokers,
            partitions=self.lab.partitions,
            replication_factor=self.lab.replication_factor,
            min_insync_replicas=self.lab.min_insync_replicas,
            idempotent_dedupe=self.lab.kafka_idempotent_dedupe,
        )
        self.sink = FakeClickHouse()
        self.restart_clients(
            ingest_crash_hook=ingest_crash_hook, build_crash_hook=build_crash_hook
        )
        if self.state.can(State.RUNNING):
            self.state.to(State.RUNNING)
        self.ledger.record(
            phase=Phase.RUNNING,
            actor="orchestrator",
            operation="dataplane_started",
            topic_generation=self.kafka.topic_generation,
            detail=self.kafka.describe(),
        )

    def restart_clients(
        self,
        *,
        ingest_crash_hook: Callable[..., None] | None = None,
        build_crash_hook: Callable[..., None] | None = None,
    ) -> None:
        """Rebuild the P02-P06 processes against the *existing* broker and sink.

        This is what a restart or a republish actually is: the processes come
        back with no in-memory state, while the infrastructure and its data
        survive. Recreating the broker too would make a recovery test pass for
        the wrong reason -- a fresh empty log is not the same as a recovered one.
        """
        if self.dataset is None or self.kafka is None or self.sink is None:
            raise RuntimeError("start_dataplane() must run before restart_clients()")
        self.producer = Producer(
            self.kafka, codec=self.codec, idempotent=self.lab.producer_idempotent
        )
        producer = self.producer

        def publish(core: Any) -> None:
            try:
                producer.enqueue(core)
            except QueueFull as exc:
                # Backpressure, surfaced as a finding rather than a dropped
                # event. Design 8: never a silent drop.
                self.add_findings(
                    [
                        Finding(
                            Reason.RESOURCE_BUDGET_EXCEEDED,
                            {"stage": "producer_queue", "detail": str(exc)},
                        )
                    ]
                )
                raise

        self.adapter = Adapter(
            dataset_id=self.dataset.dataset_id,
            limits=limits_from_sealed(self.dataset.symbols),
            sink=publish,
            codec=self.codec,
        )
        self.ingestor = BatchIngestor(
            self.kafka,
            self.sink,
            group=self.lab.group,
            codec=self.codec,
            batch_max_records=self.lab.batch_max_records,
            strict_poison=self.lab.strict_poison,
            crash_after=ingest_crash_hook,
        )
        self.builder = BuildPipeline(self.sink, crash_after=build_crash_hook)

    def begin_recovery(self, reason: str) -> None:
        """Enter ``RECOVERING``: the run is now repairing rather than measuring.

        Design 7.1 puts recovery between the confirmed fault and the new cut, so
        a republish or a restart goes through here. Keeping it an explicit step
        means the state history shows that the run recovered, which the gate
        reads when it checks the path to READY.
        """
        if self.state.can(State.RECOVERING):
            self.state.to(State.RECOVERING)
        self.ledger.record(
            phase=Phase.RECOVERING,
            actor="recovery_coordinator",
            operation="begin_recovery",
            reason_code=reason,
        )

    def replay(
        self,
        *,
        chunk_sizes: Sequence[int] = (4096,),
        faults: TransportFaults | None = None,
        flush: bool = True,
    ) -> ReplayReport:
        """Feed the sealed source through the adapter into the producer."""
        if self.dataset is None or self.adapter is None or self.producer is None:
            raise RuntimeError("start_dataplane() must run first")
        report = replay_source(
            self.dataset, self.adapter, codec=self.codec, chunk_sizes=chunk_sizes, faults=faults
        )
        self.results.replay = report
        self._write("replay/report.json", report.as_dict())
        self._write("replay/adapter_counters.json", self.adapter.counters.as_dict())
        if flush:
            self.producer.flush()
        self._write("replay/producer_ledger.json", self.producer.ledger_receipt())
        self.ledger.record(
            phase=Phase.RUNNING,
            actor="source_replayer",
            operation="replay",
            detail={"frames_sent": report.frames_sent, "bytes": report.bytes_sent},
            evidence_artifact_refs=("replay/report.json",),
        )
        return report

    def fence_and_cut(self, *, start_offsets: Mapping[tuple[str, int], int] | None = None) -> KafkaCut:
        """Fence producers and capture a stable cut, or record why not."""
        if self.kafka is None or self.producer is None:
            raise RuntimeError("start_dataplane() must run first")
        try:
            cut, fence = capture_cut(
                self.kafka, producers=[self.producer], start_offsets=start_offsets
            )
        except CutRejected as exc:
            self.add_findings([Finding(exc.reason, {"detail": exc.detail})])
            self.state.to(State.INCONCLUSIVE)
            raise
        self.results.cut = cut
        if self.state.can(State.CUT_CAPTURED):
            self.state.to(State.CUT_CAPTURED)
        self._write("kafka/cut.json", {**cut.as_dict(), "fence": fence.as_dict()})
        self.ledger.record(
            phase=Phase.CUT_CAPTURED,
            actor="cut_coordinator",
            operation="capture_cut",
            topic_generation=cut.topic_generation,
            detail={"records": cut.record_count, "unknown_acks": fence.unknown},
            evidence_artifact_refs=("kafka/cut.json",),
        )
        return cut

    def scan(self) -> CutInventory:
        """Independently scan the cut and reconcile it against the expectation."""
        if self.kafka is None or self.results.cut is None or self.expectation is None:
            raise RuntimeError("fence_and_cut() must run first")
        try:
            inventory = scan_cut(
                self.kafka,
                self.results.cut,
                codec=self.codec,
                expect_dataset_id=self.expectation.dataset_id,
            )
        except CutUnscannable as exc:
            self.add_findings([Finding(exc.reason, {"detail": exc.detail})])
            raise
        self.results.inventory = inventory
        transport = reconcile_kafka_cut(self.expectation, inventory)
        self.results.transport = transport
        self.add_findings(transport.findings)
        self.add_findings(
            cut_findings(self.results.cut, resolved_identities=tuple(inventory.logical))
        )
        self._write("kafka/record_inventory.json", inventory.as_dict())
        self._write("kafka/transport_diff.json", transport.as_dict())
        self.ledger.record(
            phase=Phase.CUT_CAPTURED,
            actor="kafka_scanner",
            operation="scan_cut",
            detail={
                "physical_records": inventory.physical_records,
                "logical_identities": len(inventory.logical),
            },
            evidence_artifact_refs=("kafka/record_inventory.json",),
        )
        return inventory

    def ingest(self, *, cut: KafkaCut | None = None) -> dict[str, Any]:
        """Drain the cut into the raw table and report the frontier."""
        if self.ingestor is None or self.kafka is None:
            raise RuntimeError("start_dataplane() must run first")
        cut = cut or self.results.cut
        if cut is None:
            raise RuntimeError("a cut is required before ingesting")
        self.ingestor.assignment = list(cut.ranges)
        self.ingestor.resume_from_broker(cut_start=cut.start_offsets())
        try:
            drain = self.ingestor.drain_to_cut(cut.end_offsets())
        except OffsetExpired as exc:
            self.add_findings([Finding(Reason.CUT_RANGE_EXPIRED, {"detail": exc.detail})])
            raise
        except PoisonRecord as exc:
            # Strict topic: a record that cannot be decoded stops ingestion.
            # Skipping it would be an untracked drop (case E2E-18).
            self.add_findings(
                [
                    Finding(
                        Reason.KAFKA_DECODE_ERROR,
                        {
                            "topic": exc.topic,
                            "partition": exc.partition,
                            "offset": exc.offset,
                            "reason": exc.reason,
                            "policy": "strict_stop",
                        },
                    )
                ]
            )
            raise
        self.results.drain = drain
        cut_end = {f"{topic}/{partition}": end for (topic, partition), end in cut.end_offsets().items()}
        self.add_findings(frontier_findings(drain["frontier"], cut_end=cut_end))
        self._write("ingestion/frontier.json", drain)
        self.ledger.record(
            phase=Phase.RUNNING,
            actor="p05_ingestor",
            operation="drain_to_cut",
            detail={"frontier_reached_cut": drain["frontier_reached_cut"]},
            evidence_artifact_refs=("ingestion/frontier.json",),
        )
        return drain

    def audit_and_build(self, *, build_id: str | None = None) -> RawAudit:
        """Audit raw, reconcile it, then stage a canonical candidate."""
        if self.builder is None or self.sink is None or self.expectation is None:
            raise RuntimeError("start_dataplane() must run first")
        self.state.to(State.RAW_AUDIT)
        audit = self.builder.audit_raw(self.expectation.dataset_id)
        self.results.audit = audit

        raw = reconcile_raw(
            self.expectation,
            list(self.sink.scan_raw(dataset_id=self.expectation.dataset_id)),
            inventory=self.results.inventory,
            cut_proven=bool(self.results.cut and self.results.cut.fenced),
            rows_inserted=self.ingestor.counters.rows_inserted if self.ingestor else None,
        )
        self.results.raw = raw
        self.add_findings(raw.findings)
        self._write("clickhouse/raw_audit.json", audit.as_dict())
        self._write("clickhouse/raw_diff.json", raw.as_dict())
        self.ledger.record(
            phase=Phase.RAW_AUDIT,
            actor="raw_reconciler",
            operation="audit_raw",
            detail={"physical_rows": audit.physical_rows, "identities": audit.identities},
            evidence_artifact_refs=("clickhouse/raw_diff.json",),
        )

        self.results.build_id = build_id or f"build-{self.run_id}"
        self.state.to(State.CANONICAL_BUILD)
        try:
            self.builder.build_canonical(
                self.results.build_id,
                audit=audit,
                source_manifest_sha256=self.dataset.events_sha256 if self.dataset else "",
                expected_identity_count=len(self.expectation.accepted),
            )
        except BuildRefused as exc:
            # A refused build is often the *correct* outcome; the caller decides
            # whether it was expected. Recorded either way.
            self.add_findings([Finding(exc.reason, {"stage": "build_canonical", "detail": exc.detail})])
            raise
        canonical = reconcile_canonical(
            self.expectation,
            self.results.build_id,
            list(self.sink.scan_canonical(self.results.build_id)),
            cut_proven=bool(self.results.cut and self.results.cut.fenced),
        )
        self.results.canonical = canonical
        self.add_findings(canonical.findings)
        self._write("analytics/canonical_diff.json", canonical.as_dict())
        self.ledger.record(
            phase=Phase.CANONICAL_BUILD,
            actor="canonical_reconciler",
            operation="build_and_verify",
            build_id=self.results.build_id,
            detail={"rows": canonical.rows, "verified": canonical.verified},
            evidence_artifact_refs=("analytics/canonical_diff.json",),
        )
        return audit

    def materialize(self) -> BarReconciliation:
        """Materialise bars and compare them with the reference OHLC."""
        if self.builder is None or self.sink is None or self.expectation is None:
            raise RuntimeError("audit_and_build() must run first")
        build_id = self.results.build_id
        if build_id is None:
            raise RuntimeError("no build to materialise")
        self.state.to(State.BAR_VERIFY)
        try:
            self.builder.materialize_bars(build_id)
        except BuildRefused as exc:
            self.add_findings([Finding(exc.reason, {"stage": "materialize_bars", "detail": exc.detail})])
            raise
        bars = reconcile_bars(self.expectation, build_id, list(self.sink.scan_bars(build_id)))
        self.results.bars = bars
        self.add_findings(bars.findings)
        self._write("analytics/bars_diff.json", bars.as_dict())
        self.ledger.record(
            phase=Phase.BAR_VERIFY,
            actor="bar_reconciler",
            operation="materialize_and_verify",
            build_id=build_id,
            detail={"expected_bars": bars.expected_bars, "observed_bars": bars.observed_bars},
            evidence_artifact_refs=("analytics/bars_diff.json",),
        )
        return bars

    def verify_and_publish(self) -> bool:
        """Publish READY, but only with the full path and a clean reconciliation.

        Returns ``False`` when publication was correctly withheld. That is not a
        failure of this method -- refusing to publish unproven data is the
        behaviour ``INV-05`` and ``NFR-03`` demand.
        """
        if self.builder is None or self.sink is None:
            raise RuntimeError("materialize() must run first")
        build_id = self.results.build_id
        if build_id is None:
            raise RuntimeError("no build to publish")

        met, missing = ready_preconditions_met(self.state)
        if not met:
            self.add_findings([Finding(Reason.BUILD_NOT_VERIFIED, {"missing_stages": list(missing)})])
            self.results.published = False
            return False
        # *Any* outstanding finding blocks publication, including a missing
        # external source anchor. Publishing a snapshot whose source integrity
        # cannot be proven from outside the bundle would be a false READY
        # (NFR-03), so a run without an anchor can reconcile and report but
        # cannot publish.
        blocking = list(self.findings)
        if blocking:
            # The outstanding findings are already recorded; adding another
            # would double-count the same problem in the report. What this adds
            # is the fact that publication was withheld because of them.
            self.builder.mark_dirty(build_id, "reconciliation findings outstanding")
            self.results.published = False
            self.ledger.record(
                phase=Phase.BAR_VERIFY,
                actor="verdict_engine",
                operation="publication_withheld",
                build_id=build_id,
                reason_code=blocking[0].reason,
                detail={"outstanding_findings": len(blocking)},
            )
            return False

        self.builder.mark_verified(
            build_id,
            proof={
                "canonical_verified": bool(self.results.canonical and self.results.canonical.verified),
                "bars_verified": bool(self.results.bars and self.results.bars.verified),
                "source_cut_sealed": bool(self.results.cut and self.results.cut.fenced),
                "evidence_ref": "analytics/canonical_diff.json",
            },
        )
        published = self.builder.publish(
            build_id,
            expected_current=self.sink.ready_build,
            evidence_refs=("analytics/canonical_diff.json", "analytics/bars_diff.json"),
            now_ns=time.time_ns(),
        )
        self.results.published = published
        if published:
            self.state.to(State.READY)
            self.metrics.reconciled_at_ns = time.monotonic_ns()
            self.ledger.record(
                phase=Phase.READY,
                actor="build_catalog",
                operation="publish_ready",
                build_id=build_id,
                result_code="READY",
            )
        else:
            self.add_findings([Finding(Reason.READY_PUBLISH_RACE_LOST, {"build_id": build_id})])
        return published

    # -- gate -------------------------------------------------------------

    def incomplete_stages(self) -> tuple[str, ...]:
        """Stages that produced no result. Empty means the whole path ran.

        A run that stopped early has not *proven* anything, however few findings
        it collected: an exception on the way to the sink leaves the data
        unexamined, not correct. This is what keeps an aborted run from gating
        as ``PASS`` with an empty finding list.
        """
        required = (
            ("replay", self.results.replay),
            ("cut", self.results.cut),
            ("scan", self.results.inventory),
            ("ingest", self.results.drain),
            ("raw_audit", self.results.raw),
            ("canonical", self.results.canonical),
            ("bars", self.results.bars),
            ("publish_decision", self.results.published),
        )
        return tuple(name for name, value in required if value is None)

    def gate(self, *, expect_fault: bool | None = None, require_full_path: bool = True) -> GateResult:
        """Classify the accumulated findings into one verdict.

        ``expect_fault`` defaults to whether any receipt was recorded. When a
        fault *was* expected and none is confirmed, the run is ``INVALID``
        regardless of how clean the data looks -- design 6.3, and ``INV-06``.

        ``require_full_path`` adds an ``EVIDENCE_INCOMPLETE`` finding when any
        stage was skipped, which makes an aborted run ``INCONCLUSIVE``. Tests
        that deliberately gate a partial path pass ``False``.
        """
        if require_full_path:
            missing = self.incomplete_stages()
            if missing:
                self.add_findings(
                    [Finding(Reason.EVIDENCE_INCOMPLETE, {"incomplete_stages": list(missing)})]
                )
        if self.dataset is not None and self.store is not None:
            self.add_findings(
                check_budget(
                    self.lab.budget,
                    producer_queue_events=self.producer.counters.peak_queue_depth if self.producer else 0,
                    producer_queue_bytes=self.producer.counters.peak_queue_bytes if self.producer else 0,
                    raw_rows=len(self.sink.raw_rows) if self.sink else 0,
                    evidence_bytes=self.store.bytes_used,
                )
            )
        chain = self.ledger.verify()
        if not chain["chain_intact"] or chain["torn_tail"]:
            self.add_findings([Finding(Reason.EVIDENCE_INCOMPLETE, dict(chain))])

        expected = self.fault_expected if expect_fault is None else expect_fault
        fault_confirmed = (not expected) or (
            bool(self.receipts) and all(receipt.confirmed for receipt in self.receipts)
        )
        result = classify(self.findings, fault_confirmed=fault_confirmed)

        if not self.state.terminal:
            target = {
                Verdict.PASS: State.PASS,
                Verdict.FAIL: State.FAIL,
                Verdict.INVALID: State.INVALID,
                Verdict.INCONCLUSIVE: State.INCONCLUSIVE,
            }[result.verdict]
            if self.state.can(target):
                self.state.to(target)
        self.ledger.record(
            phase=Phase.TERMINAL,
            actor="verdict_engine",
            operation="gate",
            result_code=result.verdict.value,
            reason_code=result.primary_reason,
            detail={"findings": len(result.findings)},
        )
        return result

    def report(self, gate_result: GateResult) -> dict[str, Any]:
        """Assemble the machine-readable results document."""
        return {
            "scenario_id": self.scenario_id,
            "run_id": self.run_id,
            "test_case_id": self.test_case_id,
            "profile": PROFILE,
            "profile_note": PROFILE_NOTE,
            "verdict": gate_result.verdict.value,
            "primary_reason": gate_result.primary_reason,
            "release_blocking": gate_result.verdict.is_release_blocking(),
            "state": self.state.as_dict(),
            "environment": self._environment,
            "lab": self.lab.as_dict(),
            "dataset": self.dataset.seal_receipt() if self.dataset else None,
            "expected": self.expectation.receipt() if self.expectation else None,
            "faults": summarise_receipts(self.receipts),
            "stages": {
                "replay": self.results.replay.as_dict() if self.results.replay else None,
                "cut": self.results.cut.as_dict() if self.results.cut else None,
                "transport": self.results.transport.as_dict() if self.results.transport else None,
                "drain": self.results.drain,
                "raw": self.results.raw.as_dict() if self.results.raw else None,
                "canonical": self.results.canonical.as_dict() if self.results.canonical else None,
                "bars": self.results.bars.as_dict() if self.results.bars else None,
                "published": self.results.published,
                "build_id": self.results.build_id,
            },
            "recovery": self.metrics.as_dict(),
            "evidence": {
                "journal": self.ledger.verify(),
                "anchor": self.ledger.anchor(),
                "artifact_bytes": self.store.bytes_used,
                "durability": self.store.durability(),
                "artifacts": sorted(self.store.inventory()),
            },
            "findings": gate_result.as_dict()["findings"],
        }

    def finish(self, gate_result: GateResult) -> dict[str, Any]:
        """Write the report and the checksum file. Returns the report."""
        report = self.report(gate_result)
        self._write("reports/results.json", report)
        from ..reporting.markdown_json import render_summary  # local import avoids a cycle

        try:
            self.store.write_text("reports/summary.md", render_summary(report))
        except QuotaExceeded as exc:
            self.add_findings([Finding(Reason.EVIDENCE_QUOTA_EXCEEDED, {"path": exc.path})])
        self.store.finalise()
        return report

    # -- the clean path ----------------------------------------------------

    def run_clean(self) -> tuple[GateResult, dict[str, Any]]:
        """Walk every stage with no injected fault. The baseline, ``E2E-01``."""
        self.seal()
        self.start_dataplane()
        self.replay()
        self.fence_and_cut()
        self.scan()
        self.ingest()
        self.audit_and_build()
        self.materialize()
        self.verify_and_publish()
        result = self.gate()
        return result, self.finish(result)
