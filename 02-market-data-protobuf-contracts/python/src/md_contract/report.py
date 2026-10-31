"""The test report of section 15, filled from real runs only.

The verdict vocabulary is the point of this module. ``NOT_RUN`` and
``SKIPPED_WITH_REASON`` are distinct from ``PASS`` and are never folded into it
(section 11.2). A suite that reports "all green" because half of it never
executed is worse than one that reports honestly, because the first is trusted.

So :class:`TestReport` tracks every what-if case id from the matrix, starts them
all at ``NOT_RUN``, and the release verdict is computed from what was actually
observed -- including counting how much was never run.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterable


class Verdict(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    INCOMPLETE = "INCOMPLETE"
    NOT_RUN = "NOT_RUN"
    SKIPPED_WITH_REASON = "SKIPPED_WITH_REASON"
    INCONCLUSIVE = "INCONCLUSIVE"


#: Case counts per group, from section 8. Used to enumerate the full matrix so
#: an unexecuted case is visible rather than merely absent.
CASE_GROUPS: dict[str, int] = {
    "A": 14, "B": 12, "C": 18, "D": 20, "E": 14, "F": 12,
    "G": 12, "H": 18, "I": 14, "J": 12, "K": 12, "L": 12,
}
TOTAL_CASES = sum(CASE_GROUPS.values())


def all_case_ids() -> list[str]:
    return [
        f"{group}{index:02d}"
        for group, count in CASE_GROUPS.items()
        for index in range(1, count + 1)
    ]


@dataclass
class CaseResult:
    case: str
    verdict: Verdict
    evidence: str = ""
    test: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "case": self.case,
            "verdict": self.verdict.value,
            "evidence": self.evidence,
            "test": self.test,
        }


@dataclass
class TestReport:
    """Accumulates results, then renders the section 15 document."""

    #: Not a pytest test class, despite the name. Without this pytest tries to
    #: collect it and warns, because it starts with "Test" and has __init__.
    __test__ = False

    run_id: str = ""
    validation_mode: str = "strict_normal"
    cases: dict[str, CaseResult] = field(default_factory=dict)
    sections: dict[str, Any] = field(default_factory=dict)
    known_limitations: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        for case in all_case_ids():
            self.cases.setdefault(case, CaseResult(case, Verdict.NOT_RUN))

    def record(
        self,
        case: str,
        verdict: Verdict | str,
        evidence: str = "",
        test: str = "",
    ) -> None:
        verdict = Verdict(verdict) if isinstance(verdict, str) else verdict
        existing = self.cases.get(case)
        # A FAIL is never overwritten by a later PASS for the same case: if one
        # execution of a scenario failed, the case failed.
        if existing is not None and existing.verdict is Verdict.FAIL:
            return
        self.cases[case] = CaseResult(case, verdict, evidence, test)

    def record_many(
        self, cases: Iterable[str], verdict: Verdict | str, evidence: str = "", test: str = ""
    ) -> None:
        for case in cases:
            self.record(case, verdict, evidence, test)

    def add_section(self, name: str, payload: Any) -> None:
        self.sections[name] = payload

    def counts(self) -> dict[str, int]:
        tally: dict[str, int] = {v.value: 0 for v in Verdict}
        for result in self.cases.values():
            tally[result.verdict.value] += 1
        return tally

    def release_verdict(self) -> tuple[str, list[str]]:
        """``(GO | NO_GO | NOT_EVALUATED, reasons)`` per section 14.1."""
        tally = self.counts()
        blockers: list[str] = []
        if tally[Verdict.FAIL.value]:
            failed = sorted(
                c for c, r in self.cases.items() if r.verdict is Verdict.FAIL
            )
            blockers.append(f"{len(failed)} case(s) FAIL: {', '.join(failed[:20])}")
        if tally[Verdict.INCOMPLETE.value]:
            blockers.append(f"{tally[Verdict.INCOMPLETE.value]} case(s) INCOMPLETE")
        if tally[Verdict.INCONCLUSIVE.value]:
            blockers.append(f"{tally[Verdict.INCONCLUSIVE.value]} case(s) INCONCLUSIVE")

        if blockers:
            return "NO_GO", blockers

        executed = tally[Verdict.PASS.value]
        if executed == 0:
            return "NOT_EVALUATED", ["no case was executed"]
        not_run = tally[Verdict.NOT_RUN.value]
        if not_run:
            # Deliberately not a GO. Section 14.1 requires the matrix to have
            # run, and unexecuted cases are unknowns, not successes.
            return "NOT_EVALUATED", [
                f"{executed}/{TOTAL_CASES} cases PASS but {not_run} are still NOT_RUN; "
                "GO requires the matrix to have been executed"
            ]
        return "GO", []

    def as_dict(self) -> dict[str, Any]:
        verdict, blockers = self.release_verdict()
        tally = self.counts()
        status = (
            Verdict.FAIL.value
            if tally[Verdict.FAIL.value]
            else Verdict.PASS.value
            if tally[Verdict.PASS.value] and not tally[Verdict.NOT_RUN.value]
            else Verdict.INCOMPLETE.value
        )
        return {
            "project": "market-data-protobuf-contracts",
            "contract": "marketdata.v1",
            "source_schema_version": "sim.v1",
            "frame_protocol": "MDP1",
            "document_version": "1.0",
            "status": status,
            "run_id": self.run_id,
            "validation_mode": self.validation_mode,
            "what_if_total_cases": TOTAL_CASES,
            "what_if_counts": tally,
            "what_if_case_results": [
                self.cases[case].as_dict() for case in all_case_ids()
            ],
            "sections": self.sections,
            "known_limitations": self.known_limitations,
            "release_verdict": verdict,
            "release_blockers": blockers,
        }

    def write_json(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.as_dict(), indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        return path

    def summary_lines(self) -> list[str]:
        tally = self.counts()
        verdict, blockers = self.release_verdict()
        lines = [
            f"what-if matrix: {TOTAL_CASES} cases",
            "  " + "  ".join(f"{name}={count}" for name, count in tally.items() if count),
            f"release verdict: {verdict}",
        ]
        lines.extend(f"  blocker: {reason}" for reason in blockers)
        return lines
