"""The test catalog and the release coverage gate (sections 16, 17, 24; case E2E-48).

``specs/test-catalog.csv`` is generated from the design document by
``scripts/build_catalog.py``; this module reads it and answers two questions:

1. **What is the status of every case?** The default is ``NOT_RUN`` for all 512
   what-if cases and all 48 end-to-end tests, and section 16 makes that the only
   honest starting state. A status moves only when a result is supplied.
2. **Is the plan allowed to ship?** Case E2E-48 is the adversarial version of
   this: a CI plan with one critical P0 test quietly removed must come out
   ``NO_GO`` and must not print an all-checked status. :class:`CoverageGate`
   therefore compares the plan against the catalog rather than against itself,
   so a case that was never scheduled is as blocking as one that failed.

A waiver is a first-class object with an owner and a reason, because section 24
accepts P1 waivers with a named owner and accepts nothing anonymous.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from .errors import ConfigError

#: Where the generated catalog lives relative to the package root.
DEFAULT_CATALOG_PATH = Path(__file__).resolve().parents[2] / "specs" / "test-catalog.csv"

#: Inventory from section 17, asserted on load so a truncated CSV is caught.
EXPECTED_WHAT_IF = 512
EXPECTED_E2E = 48
EXPECTED_PRIORITY_COUNTS = {"P0": 242, "P1": 206, "P2": 64}


class CaseStatus(str, Enum):
    """Per-case status. The five verdicts of section 7, plus two plan states.

    ``BLOCKED_BY_UPSTREAM`` is not in section 7's table; section 20 introduces
    it for milestones whose dependencies have no implementation. It is kept
    distinct from ``NOT_RUN`` so a reader can tell "nobody has run this yet"
    apart from "this cannot be run in this repository yet".
    """

    NOT_RUN = "NOT_RUN"
    PASS = "PASS"
    FAIL = "FAIL"
    INCONCLUSIVE = "INCONCLUSIVE"
    INVALID = "INVALID"
    BLOCKED_BY_UPSTREAM = "BLOCKED_BY_UPSTREAM_IMPLEMENTATION"
    WAIVED = "WAIVED"


#: Statuses that let a release proceed for the case they attach to.
_SATISFYING = frozenset({CaseStatus.PASS, CaseStatus.WAIVED})


class Priority(str, Enum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"
    #: Section 18 assigns no P-level to the 48 E2E tests.
    UNSET = "UNSET"


class Kind(str, Enum):
    WHAT_IF = "WHAT_IF"
    E2E = "E2E"


@dataclass(frozen=True)
class CatalogCase:
    """One row of the catalog, as written in the design document."""

    id: str
    kind: Kind
    group: str
    group_title: str
    priority: Priority
    what_if: str
    stimulus: str
    expected_evidence: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind.value,
            "group": self.group,
            "group_title": self.group_title,
            "priority": self.priority.value,
            "what_if": self.what_if,
            "stimulus": self.stimulus,
            "expected_evidence": self.expected_evidence,
        }


@dataclass(frozen=True)
class Waiver:
    """A deliberate, attributed decision not to run a case."""

    case_id: str
    owner: str
    reason: str
    expires: str | None = None

    def __post_init__(self) -> None:
        if not self.owner.strip():
            raise ConfigError("a waiver needs a named owner", case_id=self.case_id)
        if not self.reason.strip():
            raise ConfigError("a waiver needs a reason", case_id=self.case_id)

    def as_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "owner": self.owner,
            "reason": self.reason,
            "expires": self.expires,
        }


@dataclass(frozen=True)
class CaseResult:
    """An observed outcome for one case, with a pointer to its evidence."""

    case_id: str
    status: CaseStatus
    reason: str = ""
    evidence_ref: str | None = None
    run_id: str | None = None

    def __post_init__(self) -> None:
        if self.status is CaseStatus.PASS and not self.evidence_ref:
            # Section 16: a PASS without an artifact reference is not a PASS.
            raise ConfigError(
                "a PASS result must cite an evidence reference",
                case_id=self.case_id,
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "status": self.status.value,
            "reason": self.reason,
            "evidence_ref": self.evidence_ref,
            "run_id": self.run_id,
        }


class CaseCatalog:
    """The 560 cases, immutable, with status looked up separately."""

    __slots__ = ("_cases",)

    def __init__(self, cases: Iterable[CatalogCase]) -> None:
        ordered = list(cases)
        ids = [case.id for case in ordered]
        duplicates = sorted({i for i in ids if ids.count(i) > 1})
        if duplicates:
            raise ConfigError("duplicate case ids in the catalog", ids=duplicates)
        self._cases = {case.id: case for case in ordered}

    # -- loading ----------------------------------------------------------
    @classmethod
    def load(cls, path: Path | None = None, *, verify_inventory: bool = True) -> "CaseCatalog":
        target = path or DEFAULT_CATALOG_PATH
        if not target.is_file():
            raise ConfigError(
                "test catalog not found; run scripts/build_catalog.py",
                path=str(target),
            )
        cases: list[CatalogCase] = []
        with target.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                cases.append(
                    CatalogCase(
                        id=row["id"],
                        kind=Kind(row["kind"]),
                        group=row["group"],
                        group_title=row["group_title"],
                        priority=Priority(row["priority"]),
                        what_if=row["what_if"],
                        stimulus=row["stimulus"],
                        expected_evidence=row["expected_evidence"],
                    )
                )
        catalog = cls(cases)
        if verify_inventory:
            catalog.verify_inventory()
        return catalog

    def verify_inventory(self) -> None:
        """Check the catalog against the counts section 17 declares."""
        problems = []
        what_if = [c for c in self if c.kind is Kind.WHAT_IF]
        e2e = [c for c in self if c.kind is Kind.E2E]
        if len(what_if) != EXPECTED_WHAT_IF:
            problems.append(f"{len(what_if)} what-if cases, expected {EXPECTED_WHAT_IF}")
        if len(e2e) != EXPECTED_E2E:
            problems.append(f"{len(e2e)} E2E tests, expected {EXPECTED_E2E}")
        counts: dict[str, int] = {}
        for case in what_if:
            counts[case.priority.value] = counts.get(case.priority.value, 0) + 1
        if counts != EXPECTED_PRIORITY_COUNTS:
            problems.append(f"priority counts {counts}, expected {EXPECTED_PRIORITY_COUNTS}")
        if problems:
            raise ConfigError("catalog inventory mismatch", problems=problems)

    # -- access -----------------------------------------------------------
    def __iter__(self) -> Iterator[CatalogCase]:
        return iter(self._cases.values())

    def __len__(self) -> int:
        return len(self._cases)

    def __contains__(self, case_id: object) -> bool:
        return case_id in self._cases

    def get(self, case_id: str) -> CatalogCase:
        case = self._cases.get(case_id)
        if case is None:
            raise ConfigError("unknown case id", case_id=case_id)
        return case

    @property
    def ids(self) -> list[str]:
        return list(self._cases)

    def by_priority(self, priority: Priority) -> list[CatalogCase]:
        return [c for c in self if c.priority is priority]

    def by_group(self, group: str) -> list[CatalogCase]:
        return [c for c in self if c.group == group]

    def by_kind(self, kind: Kind) -> list[CatalogCase]:
        return [c for c in self if c.kind is kind]

    @property
    def p0_ids(self) -> list[str]:
        return [c.id for c in self.by_priority(Priority.P0)]

    @property
    def groups(self) -> list[str]:
        return sorted({c.group for c in self})


class Gate(str, Enum):
    GO = "GO"
    NO_GO = "NO_GO"
    INCONCLUSIVE = "INCONCLUSIVE"
    INVALID = "INVALID"


@dataclass
class CoverageGate:
    """Decide GO/NO-GO for a plan against the catalog (section 24, case E2E-48).

    ``planned`` is what CI intends to run. The gate compares it against the
    catalog's P0 set, so dropping a case from the plan is detected by its
    *absence* -- which is the whole point of E2E-48, where a plan that omits a
    P0 test would otherwise report everything it ran as green.

    ``profile_scope`` narrows which P0 cases apply: section 24 says "all P0
    applying to the profile", and a ``QUERY_ONLY`` run cannot be held to the
    ingestion cases. Narrowing is recorded in the result, so a shrunken scope is
    visible rather than silent.
    """

    catalog: CaseCatalog
    planned: set[str] = field(default_factory=set)
    results: dict[str, CaseResult] = field(default_factory=dict)
    waivers: dict[str, Waiver] = field(default_factory=dict)
    profile_scope: set[str] | None = None

    # -- plan construction ------------------------------------------------
    def plan(self, case_ids: Iterable[str]) -> None:
        for case_id in case_ids:
            self.catalog.get(case_id)  # raises on an unknown id
            self.planned.add(case_id)

    def record(self, result: CaseResult) -> None:
        self.catalog.get(result.case_id)
        if result.case_id not in self.planned:
            raise ConfigError(
                "result for a case that was never planned; add it to the plan "
                "so coverage accounting stays honest",
                case_id=result.case_id,
            )
        self.results[result.case_id] = result

    def waive(self, waiver: Waiver) -> None:
        self.catalog.get(waiver.case_id)
        self.waivers[waiver.case_id] = waiver

    # -- status -----------------------------------------------------------
    def status(self, case_id: str) -> CaseStatus:
        if case_id in self.waivers:
            return CaseStatus.WAIVED
        result = self.results.get(case_id)
        return result.status if result else CaseStatus.NOT_RUN

    @property
    def applicable_p0(self) -> list[str]:
        ids = self.catalog.p0_ids
        if self.profile_scope is None:
            return ids
        return [i for i in ids if i in self.profile_scope]

    # -- the gate ---------------------------------------------------------
    def evaluate(self) -> dict[str, Any]:
        """Render the gate decision with its blocking findings.

        Precedence follows section 24: an invalid experiment outranks a failure,
        because a FAIL from a broken measurement is not a fact about the system.
        """
        applicable = self.applicable_p0
        unplanned = sorted(set(applicable) - self.planned)
        statuses = {case_id: self.status(case_id) for case_id in applicable}

        failed = sorted(i for i, s in statuses.items() if s is CaseStatus.FAIL)
        invalid = sorted(i for i, s in statuses.items() if s is CaseStatus.INVALID)
        inconclusive = sorted(i for i, s in statuses.items() if s is CaseStatus.INCONCLUSIVE)
        # Only planned cases can be "planned but not run". Counting an unplanned
        # case in both buckets would make the coverage statement contradict
        # itself ("242 never planned, 242 planned but not run").
        not_run = sorted(
            i for i, s in statuses.items()
            if s is CaseStatus.NOT_RUN and i in self.planned
        )
        blocked = sorted(i for i, s in statuses.items() if s is CaseStatus.BLOCKED_BY_UPSTREAM)
        satisfied = sorted(i for i, s in statuses.items() if s in _SATISFYING)

        findings: list[dict[str, str]] = []
        for case_id in unplanned:
            findings.append(
                {
                    "case_id": case_id,
                    "kind": "coverage",
                    "detail": "P0 case is not in the CI plan (case E2E-48)",
                }
            )
        for case_id in invalid:
            findings.append({"case_id": case_id, "kind": "validity", "detail": "P0 case INVALID"})
        for case_id in failed:
            findings.append({"case_id": case_id, "kind": "failure", "detail": "P0 case FAIL"})
        for case_id in blocked:
            findings.append(
                {
                    "case_id": case_id,
                    "kind": "upstream",
                    "detail": "P0 case blocked by an unimplemented dependency",
                }
            )
        for case_id in not_run:
            findings.append({"case_id": case_id, "kind": "coverage", "detail": "P0 case NOT_RUN"})
        for case_id in inconclusive:
            findings.append(
                {"case_id": case_id, "kind": "evidence", "detail": "P0 case INCONCLUSIVE"}
            )

        if invalid:
            verdict = Gate.INVALID
        elif failed or unplanned or not_run or blocked:
            verdict = Gate.NO_GO
        elif inconclusive:
            verdict = Gate.INCONCLUSIVE
        else:
            verdict = Gate.GO

        return {
            "verdict": verdict.value,
            "profile_scoped": self.profile_scope is not None,
            "applicable_p0": len(applicable),
            "catalog_p0_total": len(self.catalog.p0_ids),
            "planned_p0": len(set(applicable) & self.planned),
            "satisfied_p0": len(satisfied),
            "unplanned_p0": unplanned,
            "failed_p0": failed,
            "invalid_p0": invalid,
            "inconclusive_p0": inconclusive,
            "not_run_p0": not_run,
            "blocked_p0": blocked,
            "waivers": [w.as_dict() for w in self.waivers.values()],
            "blocking_findings": findings,
            # Never a bare "all checked": the number of cases the plan did not
            # cover is part of the headline (case E2E-48).
            "coverage_statement": (
                f"{len(satisfied)}/{len(applicable)} applicable P0 cases satisfied; "
                f"{len(unplanned)} never planned, {len(not_run)} planned but not run"
            ),
        }

    def summary_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {status.value: 0 for status in CaseStatus}
        for case in self.catalog:
            counts[self.status(case.id).value] += 1
        return counts


def statuses_from_mapping(mapping: Mapping[str, str]) -> dict[str, CaseStatus]:
    """Parse a plain ``{case_id: status}`` mapping, rejecting unknown statuses."""
    out: dict[str, CaseStatus] = {}
    for case_id, raw in mapping.items():
        try:
            out[case_id] = CaseStatus(raw)
        except ValueError as exc:
            raise ConfigError("unknown case status", case_id=case_id, status=raw) from exc
    return out
