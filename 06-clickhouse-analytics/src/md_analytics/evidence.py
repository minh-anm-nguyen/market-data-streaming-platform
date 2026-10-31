"""Evidence records: what a test result has to carry to count as one.

Section 14.2 lists the fields, and the list is the point. A verdict without the
fixture hash, the schema hashes, the backend and the fault-injection proof is not
reproducible, so this module makes the incomplete case *representable and
detectable* rather than impossible -- :meth:`EvidenceRecord.completeness_gaps`
reports what is missing, and :meth:`EvidenceRecord.effective_verdict` downgrades
a ``PASS`` with gaps to ``INCONCLUSIVE``.

That downgrade is the whole module. It is easy to write a test that passes; the
hard part is making a pass that nobody can reproduce *stop counting as one*.

A second rule from section 17.4: if a fault injector did not fire, the result is
``INVALID``, never ``PASS``. A chaos record therefore has to carry proof the
stimulus happened, and a record that claims a fault without that proof is
downgraded the same way.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import EvidenceClass, Verdict

#: Fields of section 14.2 that must be present for a PASS to stand.
REQUIRED_FIELDS: tuple[str, ...] = (
    "case_id",
    "priority",
    "backend",
    "evidence_class",
    "dataset_manifest_sha",
    "raw_table_ddl_sha",
    "canonical_ddl_sha",
    "sql_bank_digest",
    "config_sha",
)


@dataclass(slots=True)
class EvidenceRecord:
    """One test's result, with its provenance."""

    case_id: str
    priority: str
    verdict: Verdict
    reason: str = ""
    backend: str = ""
    evidence_class: str = ""
    server_version: str | None = None
    dataset_manifest_sha: str = ""
    raw_table_ddl_sha: str = ""
    canonical_ddl_sha: str = ""
    sql_bank_digest: str = ""
    config_sha: str = ""
    build_id: str | None = None
    kafka_cut: dict[str, Any] | None = None
    #: What the fault injector actually did. Empty for a test that injects none;
    #: required for one that claims to.
    fault_injection_proof: tuple[str, ...] = ()
    claims_fault_injection: bool = False
    counters: dict[str, int] = field(default_factory=dict)
    resource_peaks: dict[str, float] = field(default_factory=dict)
    timings_ms: dict[str, float] = field(default_factory=dict)
    recorded_at_ns: int = field(default_factory=time.time_ns)
    notes: tuple[str, ...] = ()

    def completeness_gaps(self) -> list[str]:
        gaps = [name for name in REQUIRED_FIELDS if not getattr(self, name)]
        if self.claims_fault_injection and not self.fault_injection_proof:
            gaps.append("fault_injection_proof")
        return gaps

    def effective_verdict(self) -> Verdict:
        """The verdict after applying the evidence rules of section 14.2.

        A ``PASS`` with missing provenance becomes ``INCONCLUSIVE``; a claimed
        fault with no proof it fired becomes ``INVALID``. Failures are never
        upgraded -- a ``FAIL`` with incomplete evidence is still a ``FAIL``,
        because the thing that went wrong went wrong regardless.
        """
        if self.verdict in (Verdict.FAIL, Verdict.NO_GO, Verdict.INVALID, Verdict.NOT_RUN):
            return self.verdict
        gaps = self.completeness_gaps()
        if self.claims_fault_injection and not self.fault_injection_proof:
            return Verdict.INVALID
        if gaps:
            return Verdict.INCONCLUSIVE
        return self.verdict

    def supports_clickhouse_claim(self) -> bool:
        """May this record back a statement about ClickHouse itself?

        Only a real server can answer a question about a real server. A model run
        may support a claim about this pipeline's logic and nothing more
        (section 4 note, section 22).
        """
        return (
            self.evidence_class == EvidenceClass.REAL_CLICKHOUSE
            and bool(self.server_version)
            and self.effective_verdict() is Verdict.PASS
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "priority": self.priority,
            "verdict": str(self.verdict),
            "effective_verdict": str(self.effective_verdict()),
            "completeness_gaps": self.completeness_gaps(),
            "reason": self.reason,
            "backend": self.backend,
            "evidence_class": self.evidence_class,
            "server_version": self.server_version,
            "dataset_manifest_sha": self.dataset_manifest_sha,
            "raw_table_ddl_sha": self.raw_table_ddl_sha,
            "canonical_ddl_sha": self.canonical_ddl_sha,
            "sql_bank_digest": self.sql_bank_digest,
            "config_sha": self.config_sha,
            "build_id": self.build_id,
            "kafka_cut": self.kafka_cut,
            "fault_injection_proof": list(self.fault_injection_proof),
            "claims_fault_injection": self.claims_fault_injection,
            "counters": dict(sorted(self.counters.items())),
            "resource_peaks": dict(sorted(self.resource_peaks.items())),
            "timings_ms": dict(sorted(self.timings_ms.items())),
            "recorded_at_ns": self.recorded_at_ns,
            "notes": list(self.notes),
        }


class EvidenceLedger:
    """Append-only collection of records, exportable as JSONL."""

    def __init__(self) -> None:
        self._records: list[EvidenceRecord] = []

    def add(self, record: EvidenceRecord) -> EvidenceRecord:
        self._records.append(record)
        return record

    def extend(self, records: Iterable[EvidenceRecord]) -> None:
        self._records.extend(records)

    def __len__(self) -> int:
        return len(self._records)

    def records(self) -> Sequence[EvidenceRecord]:
        return tuple(self._records)

    def by_verdict(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for record in self._records:
            key = str(record.effective_verdict())
            counts[key] = counts.get(key, 0) + 1
        return dict(sorted(counts.items()))

    def failures(self) -> list[EvidenceRecord]:
        return [
            r
            for r in self._records
            if r.effective_verdict() in (Verdict.FAIL, Verdict.INVALID)
        ]

    def release_verdict(self) -> Verdict:
        """Section 21: one failure or one invalid result blocks the release.

        ``NO_GO`` records do not block: for a case whose correct outcome is a
        refusal to publish, a ``NO_GO`` from the system is the asserted
        behaviour. ``INCONCLUSIVE`` does block, because section 21.2 lists an
        unreproducible result as a NO-GO condition in its own right.
        """
        verdicts = {r.effective_verdict() for r in self._records}
        if not self._records:
            return Verdict.NOT_RUN
        if Verdict.FAIL in verdicts or Verdict.INVALID in verdicts:
            return Verdict.FAIL
        if Verdict.INCONCLUSIVE in verdicts or Verdict.NOT_RUN in verdicts:
            return Verdict.INCONCLUSIVE
        return Verdict.PASS

    def write_jsonl(self, path: Path) -> int:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = "".join(
            json.dumps(r.as_dict(), sort_keys=True, ensure_ascii=False) + "\n"
            for r in self._records
        )
        path.write_text(payload, encoding="utf-8")
        return len(self._records)

    def summary(self) -> dict[str, Any]:
        return {
            "records": len(self._records),
            "by_verdict": self.by_verdict(),
            "release_verdict": str(self.release_verdict()),
            "clickhouse_backed": sum(
                1 for r in self._records if r.supports_clickhouse_claim()
            ),
            "failures": [
                {"case_id": r.case_id, "reason": r.reason} for r in self.failures()
            ],
        }
