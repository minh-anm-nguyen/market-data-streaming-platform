"""The append-only run journal and fault-receipt ledger, with a hash chain.

Record shape is design 5.1's ``JournalRecord``. The chain is simple: each entry
carries ``sha256_prev_entry`` and ``sha256_this_entry``, where the latter is the
digest of the canonical JSON of the entry *including* the previous hash. Breaking
or reordering any entry breaks every entry after it.

**What the chain proves and what it does not.** It detects accidental corruption,
a torn tail from a kill, and after-the-fact edits by someone who did not rewrite
the rest. It is **not** a signature and offers nothing against an actor who can
rewrite the whole file: they recompute the chain and it verifies. Design 5.1
says so directly, and the only real defence is keeping
:attr:`ProofLedger.head_hash` somewhere outside this failure domain --
:meth:`ProofLedger.anchor` returns exactly that value for a caller to ship out.

Fault receipts live in the same structure but answer a different question. Design
6.3 defines ``fault_confirmed`` as the conjunction of six facts, and
:meth:`FaultReceipt.confirmed` computes that conjunction rather than trusting a
boolean someone set. A receipt missing independent observation of the effect is
not a confirmed fault, which makes the test ``INVALID`` -- never ``PASS``.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Iterable, Iterator, Mapping, Sequence

from .artifact_store import ArtifactStore

JOURNAL_PATH = "orchestration/journal.jsonl"
RECEIPTS_PATH = "fault/receipts.jsonl"

#: Genesis link for the first entry, so every entry has a predecessor hash and
#: the verification loop needs no special case.
GENESIS_HASH = "0" * 64


class Phase(str, Enum):
    """Lifecycle phases from the state diagram in design section 5."""

    CREATED = "CREATED"
    ENV_READY = "ENV_READY"
    SOURCE_SEALED = "SOURCE_SEALED"
    RUNNING = "RUNNING"
    FAULT_CONFIRMED = "FAULT_CONFIRMED"
    RECOVERING = "RECOVERING"
    CUT_CAPTURED = "CUT_CAPTURED"
    RAW_AUDIT = "RAW_AUDIT"
    CANONICAL_BUILD = "CANONICAL_BUILD"
    BAR_VERIFY = "BAR_VERIFY"
    READY = "READY"
    TERMINAL = "TERMINAL"


def _canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


@dataclass(frozen=True, slots=True)
class JournalEntry:
    """One immutable journal record."""

    index: int
    test_case_id: str
    scenario_id: str
    run_id: str
    dataset_id: str
    attempt_id: int
    phase: str
    actor: str
    operation: str
    timestamp_monotonic_ns: int
    wall_time_utc_ns: int
    target_service: str | None = None
    intended_fault: str | None = None
    observed_fault: str | None = None
    result_code: str | None = None
    reason_code: str | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)
    evidence_artifact_refs: tuple[str, ...] = ()
    topic_generation: str | None = None
    build_id: str | None = None
    parent_run_id: str | None = None
    env_fingerprint: str | None = None
    sha256_prev_entry: str = GENESIS_HASH
    sha256_this_entry: str = ""

    def payload(self) -> dict[str, Any]:
        """The hashed body: everything except ``sha256_this_entry``."""
        return {
            "index": self.index,
            "test_case_id": self.test_case_id,
            "scenario_id": self.scenario_id,
            "run_id": self.run_id,
            "dataset_id": self.dataset_id,
            "attempt_id": self.attempt_id,
            "phase": self.phase,
            "actor": self.actor,
            "operation": self.operation,
            "timestamp_monotonic_ns": self.timestamp_monotonic_ns,
            "wall_time_utc_ns": self.wall_time_utc_ns,
            "target_service": self.target_service,
            "intended_fault": self.intended_fault,
            "observed_fault": self.observed_fault,
            "result_code": self.result_code,
            "reason_code": self.reason_code,
            "detail": dict(self.detail),
            "evidence_artifact_refs": list(self.evidence_artifact_refs),
            "topic_generation": self.topic_generation,
            "build_id": self.build_id,
            "parent_run_id": self.parent_run_id,
            "env_fingerprint": self.env_fingerprint,
            "sha256_prev_entry": self.sha256_prev_entry,
        }

    def compute_hash(self) -> str:
        return hashlib.sha256(_canonical(self.payload())).hexdigest()

    def as_record(self) -> dict[str, Any]:
        record = self.payload()
        record["sha256_this_entry"] = self.sha256_this_entry
        return record


@dataclass(frozen=True, slots=True)
class FaultReceipt:
    """Evidence that an injected fault actually happened, where it was aimed.

    Each flag is a separate observation on purpose. A receipt where the command
    was acknowledged but nothing independently observed the effect is the case
    design 6.1 warns about: a proxy that cannot see the server commit has not
    tested the ambiguous-ACK boundary, however confident the log looks.
    """

    fault_id: str
    intended_fault: str
    target: str
    trigger_precondition_verified: bool
    intended_target_matched: bool
    injection_command_acknowledged: bool
    independent_observation_of_effect: bool
    observed_effect: str | None
    started_at_ns: int
    ended_at_ns: int | None
    cleanup_recorded: bool
    detail: Mapping[str, Any] = field(default_factory=dict)

    @property
    def timestamp_range_known(self) -> bool:
        return self.started_at_ns > 0 and self.ended_at_ns is not None

    @property
    def confirmed(self) -> bool:
        """Design 6.3's six-way conjunction. All six, or the test is INVALID."""
        return (
            self.trigger_precondition_verified
            and self.intended_target_matched
            and self.injection_command_acknowledged
            and self.independent_observation_of_effect
            and self.timestamp_range_known
            and self.cleanup_recorded
        )

    def missing_conditions(self) -> tuple[str, ...]:
        """Which of the six are absent -- the reason an INVALID is INVALID."""
        checks = {
            "trigger_precondition_verified": self.trigger_precondition_verified,
            "intended_target_matched": self.intended_target_matched,
            "injection_command_acknowledged": self.injection_command_acknowledged,
            "independent_observation_of_effect": self.independent_observation_of_effect,
            "timestamp_range_known": self.timestamp_range_known,
            "cleanup_recorded": self.cleanup_recorded,
        }
        return tuple(name for name, ok in checks.items() if not ok)

    def as_dict(self) -> dict[str, Any]:
        return {
            "fault_id": self.fault_id,
            "intended_fault": self.intended_fault,
            "target": self.target,
            "trigger_precondition_verified": self.trigger_precondition_verified,
            "intended_target_matched": self.intended_target_matched,
            "injection_command_acknowledged": self.injection_command_acknowledged,
            "independent_observation_of_effect": self.independent_observation_of_effect,
            "observed_effect": self.observed_effect,
            "started_at_ns": self.started_at_ns,
            "ended_at_ns": self.ended_at_ns,
            "timestamp_range_known": self.timestamp_range_known,
            "cleanup_recorded": self.cleanup_recorded,
            "confirmed": self.confirmed,
            "missing_conditions": list(self.missing_conditions()),
            "detail": dict(self.detail),
        }


class ChainBroken(Exception):
    """The journal does not verify. Carries the failing index."""

    def __init__(self, index: int, detail: str) -> None:
        super().__init__(f"journal entry {index}: {detail}")
        self.index = index
        self.detail = detail


class ProofLedger:
    """Append-only journal plus fault receipts for one run.

    The ledger writes through an :class:`~p07.evidence.artifact_store.ArtifactStore`,
    so the quota and the atomic-write behaviour apply to it as well. It keeps no
    authoritative in-memory copy: :meth:`verify` re-reads the file, which is the
    only way to detect a torn tail from a kill.
    """

    def __init__(
        self,
        store: ArtifactStore,
        *,
        scenario_id: str,
        run_id: str,
        dataset_id: str = "",
        test_case_id: str = "",
        env_fingerprint: str | None = None,
    ) -> None:
        self._store = store
        self._scenario_id = scenario_id
        self._run_id = run_id
        self._dataset_id = dataset_id
        self._test_case_id = test_case_id
        self._env_fingerprint = env_fingerprint
        self._index = 0
        self._head = GENESIS_HASH
        self.receipts: list[FaultReceipt] = []

    @property
    def head_hash(self) -> str:
        return self._head

    @property
    def entry_count(self) -> int:
        return self._index

    def anchor(self) -> dict[str, Any]:
        """The value to store outside this failure domain to make the chain mean something."""
        return {
            "scenario_id": self._scenario_id,
            "run_id": self._run_id,
            "entries": self._index,
            "head_sha256": self._head,
        }

    def record(
        self,
        *,
        phase: Phase | str,
        actor: str,
        operation: str,
        attempt_id: int = 1,
        target_service: str | None = None,
        intended_fault: str | None = None,
        observed_fault: str | None = None,
        result_code: str | None = None,
        reason_code: str | None = None,
        detail: Mapping[str, Any] | None = None,
        evidence_artifact_refs: Sequence[str] = (),
        topic_generation: str | None = None,
        build_id: str | None = None,
        parent_run_id: str | None = None,
    ) -> JournalEntry:
        """Append one entry and extend the chain."""
        entry = JournalEntry(
            index=self._index,
            test_case_id=self._test_case_id,
            scenario_id=self._scenario_id,
            run_id=self._run_id,
            dataset_id=self._dataset_id,
            attempt_id=attempt_id,
            phase=phase.value if isinstance(phase, Phase) else str(phase),
            actor=actor,
            operation=operation,
            timestamp_monotonic_ns=time.monotonic_ns(),
            wall_time_utc_ns=time.time_ns(),
            target_service=target_service,
            intended_fault=intended_fault,
            observed_fault=observed_fault,
            result_code=result_code,
            reason_code=reason_code,
            detail=dict(detail or {}),
            evidence_artifact_refs=tuple(evidence_artifact_refs),
            topic_generation=topic_generation,
            build_id=build_id,
            parent_run_id=parent_run_id,
            env_fingerprint=self._env_fingerprint,
            sha256_prev_entry=self._head,
        )
        entry = replace(entry, sha256_this_entry=entry.compute_hash())
        self._store.append_jsonl(JOURNAL_PATH, entry.as_record())
        self._head = entry.sha256_this_entry
        self._index += 1
        return entry

    def record_receipt(self, receipt: FaultReceipt) -> FaultReceipt:
        """Append a fault receipt and journal the fact that it exists."""
        self.receipts.append(receipt)
        self._store.append_jsonl(RECEIPTS_PATH, receipt.as_dict())
        self.record(
            phase=Phase.FAULT_CONFIRMED if receipt.confirmed else Phase.RUNNING,
            actor="fault_scheduler",
            operation="fault_receipt",
            target_service=receipt.target,
            intended_fault=receipt.intended_fault,
            observed_fault=receipt.observed_effect,
            result_code="CONFIRMED" if receipt.confirmed else "NOT_CONFIRMED",
            detail={"fault_id": receipt.fault_id, "missing": list(receipt.missing_conditions())},
            evidence_artifact_refs=(RECEIPTS_PATH,),
        )
        return receipt

    def all_faults_confirmed(self) -> bool:
        """Whether every recorded fault is confirmed.

        An empty ledger returns ``True``: a scenario with no injected fault is a
        clean-path run, and demanding a receipt it never needed would make
        ``E2E-01`` permanently ``INVALID``. The orchestrator is the one that
        knows whether a fault was *supposed* to happen, and it passes that
        expectation to the gate.
        """
        return all(receipt.confirmed for receipt in self.receipts)

    def unconfirmed_faults(self) -> tuple[FaultReceipt, ...]:
        return tuple(receipt for receipt in self.receipts if not receipt.confirmed)

    # -- verification -----------------------------------------------------

    def read_entries(self) -> Iterator[dict[str, Any]]:
        for line in self._store.iter_lines(JOURNAL_PATH):
            line = line.strip()
            if line:
                yield json.loads(line)

    def verify(self) -> dict[str, Any]:
        """Re-read the journal and verify the chain.

        Returns a report rather than raising, because a broken chain is itself a
        finding the gate needs to see (``EVIDENCE_INCOMPLETE``). A trailing
        partial line is reported as ``torn_tail``: the run was killed mid-append,
        which is information, not corruption to be hidden.
        """
        previous = GENESIS_HASH
        verified = 0
        torn_tail = False
        broken_at: int | None = None
        try:
            for index, record in enumerate(self.read_entries()):
                if record.get("sha256_prev_entry") != previous:
                    broken_at = index
                    break
                claimed = record.pop("sha256_this_entry", None)
                recomputed = hashlib.sha256(_canonical(record)).hexdigest()
                if claimed != recomputed:
                    broken_at = index
                    break
                previous = recomputed
                verified += 1
        except json.JSONDecodeError:
            torn_tail = True
        return {
            "entries_verified": verified,
            "chain_intact": broken_at is None,
            "broken_at_index": broken_at,
            "torn_tail": torn_tail,
            "head_sha256": previous,
            "anchor_matches_head": previous == self._head or torn_tail,
            "note": (
                "A hash chain detects edits only while an independently stored anchor survives; "
                "it is not a signature."
            ),
        }


def summarise_receipts(receipts: Iterable[FaultReceipt]) -> dict[str, Any]:
    """Aggregate receipts for the report's fault-validity section."""
    receipts = list(receipts)
    confirmed = [receipt for receipt in receipts if receipt.confirmed]
    return {
        "faults_total": len(receipts),
        "faults_confirmed": len(confirmed),
        "faults_unconfirmed": len(receipts) - len(confirmed),
        "trigger_success_ratio": (len(confirmed) / len(receipts)) if receipts else None,
        "unconfirmed": [
            {"fault_id": receipt.fault_id, "missing": list(receipt.missing_conditions())}
            for receipt in receipts
            if not receipt.confirmed
        ],
    }
