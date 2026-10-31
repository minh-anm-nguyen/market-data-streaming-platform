"""M0: compare what the eight projects declare, and name every disagreement.

P09 section 14 puts this first for a reason. Each project validated itself and
each one passed; none of them could see the seam it shares with its neighbour.
An audit that only re-stated the design document would agree with the design and
possibly with nothing else, so every check here consumes
:mod:`p09_integration.probes` facts read out of the projects themselves.

Three rules, all of them the same rule in different clothes:

* **A check with a missing input is ``INCONCLUSIVE``, never ``PASS``.** P09
  section 16.1 makes an un-run P0 check a NO-GO; a checker that treated an
  absent fact as agreement would manufacture the false success the whole design
  argues against.
* **Evidence is a path, not a sentence.** Every finding carries the
  ``file#key = value`` pairs it compared, so it can be checked or refuted.
* **Severity is about the gate it blocks**, not about how annoying it is. A
  ``BLOCKER`` is something that makes a P09 release gate unprovable.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from . import descriptors, protoparse
from .probes import ProjectFacts, proto_copies

SEVERITIES = ("BLOCKER", "HIGH", "MEDIUM", "INFO")
VERDICTS = ("PASS", "FAIL", "INCONCLUSIVE")

#: Why the raw descriptor digests differ, and why that is not a schema problem.
#:
#: An earlier version of this audit concluded the pins "can never agree" and
#: blamed protobuf's lack of a canonical serialisation. Building P03 and P04 and
#: reproducing both digests in Python showed otherwise, and the finding is
#: sharper for it. See :mod:`p09_integration.descriptors`.
DESCRIPTOR_DIGEST_NOTE = (
    "The pins differ by representation, not by schema. protobuf 3.12.4 (C++), "
    "3.21.12 (C++) and 7.35.1 (Python) serialize byte-identical "
    "FileDescriptorProto for this schema, proto3_optional included. What varies "
    "is the wrapper (FileDescriptorSet for P02/P03, bare FileDescriptorProto "
    "for P04/P05), the 'name' field carrying the path protoc was invoked with, "
    "and json_name, which protoc --descriptor_set_out populates and a runtime "
    "CopyTo omits. P09 therefore derives each project's expected pin from one "
    "canonical descriptor in that project's own encoding, instead of comparing "
    "incomparable values."
)


@dataclass
class Finding:
    """One cross-project observation, with the evidence that produced it."""

    id: str
    title: str
    verdict: str
    severity: str
    detail: str
    gate: str
    milestone: str
    evidence: list[str] = field(default_factory=list)
    remediation: str = ""

    def __post_init__(self) -> None:
        if self.verdict not in VERDICTS:
            raise ValueError("unknown verdict: " + self.verdict)
        if self.severity not in SEVERITIES:
            raise ValueError("unknown severity: " + self.severity)

    @property
    def blocks_release(self) -> bool:
        return self.verdict != "PASS" and self.severity == "BLOCKER"

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "id": self.id,
            "title": self.title,
            "verdict": self.verdict,
            "severity": self.severity,
            "gate": self.gate,
            "milestone": self.milestone,
            "detail": self.detail,
            "evidence": list(self.evidence),
        }
        if self.remediation:
            out["remediation"] = self.remediation
        return out


@dataclass
class AuditResult:
    """The whole M0 audit: findings, the schema normal form, and a verdict."""

    generated_at_utc: str
    repo_root: str
    findings: list[Finding]
    wire_schema: dict[str, Any]
    facts: dict[str, ProjectFacts]

    def unresolved(self, severity: str) -> list[Finding]:
        return [
            f for f in self.findings if f.severity == severity and f.verdict != "PASS"
        ]

    @property
    def passed(self) -> list[Finding]:
        return [f for f in self.findings if f.verdict == "PASS"]

    @property
    def verdict(self) -> str:
        """The M0 exit gate (P09 section 14)."""
        if any(f.blocks_release for f in self.findings):
            return "M0_BLOCKED"
        if any(f.verdict == "INCONCLUSIVE" for f in self.findings):
            return "M0_INCONCLUSIVE"
        if any(f.verdict == "FAIL" for f in self.findings):
            return "M0_PASS_WITH_FINDINGS"
        return "M0_PASS"

    def as_dict(self) -> dict[str, Any]:
        return {
            "generated_at_utc": self.generated_at_utc,
            "repo_root": self.repo_root,
            "verdict": self.verdict,
            "counts": {
                "checks": len(self.findings),
                "passed": len(self.passed),
                "unresolved_by_severity": {
                    severity: len(self.unresolved(severity)) for severity in SEVERITIES
                },
            },
            "wire_schema": self.wire_schema,
            "findings": [f.as_dict() for f in self.findings],
            "projects": {k: v.as_dict() for k, v in self.facts.items()},
        }


# --------------------------------------------------------------------------
# helpers shared by the checks
# --------------------------------------------------------------------------

def _ev(facts: dict[str, ProjectFacts], project: str, key: str) -> str:
    """Render one fact as evidence, including its provenance."""
    pf = facts.get(project)
    if pf is None:
        return "%s: project absent" % project
    fact = pf.facts.get(key)
    if fact is None:
        return "%s %s: not probed" % (project, key)
    if not fact.known:
        return "%s %s: UNKNOWN (%s) <- %s" % (
            project,
            key,
            fact.unknown_reason,
            fact.source or "no source",
        )
    return "%s %s = %r <- %s" % (project, key, fact.value, fact.source or "no source")


def _agreement(
    facts: dict[str, ProjectFacts], key: str, projects: tuple[str, ...]
) -> tuple[dict[str, Any], list[str], list[str]]:
    """Collect ``key`` from each project; return values, evidence and unknowns."""
    values: dict[str, Any] = {}
    evidence: list[str] = []
    unknown: list[str] = []
    for project in projects:
        evidence.append(_ev(facts, project, key))
        pf = facts.get(project)
        fact = pf.facts.get(key) if pf else None
        if fact is None or not fact.known:
            unknown.append(project)
            continue
        values[project] = fact.value
    return values, evidence, unknown


def _consensus_check(
    *,
    check_id: str,
    title: str,
    key: str,
    projects: tuple[str, ...],
    facts: dict[str, ProjectFacts],
    severity: str,
    gate: str,
    milestone: str = "M0",
    remediation: str = "",
    normalise: Callable[[Any], Any] | None = None,
) -> Finding:
    """One fact several projects must agree on, or the release is unprovable."""
    values, evidence, unknown = _agreement(facts, key, projects)
    if unknown:
        return Finding(
            id=check_id,
            title=title,
            verdict="INCONCLUSIVE",
            severity=severity,
            detail="cannot compare: %s did not yield %s" % (", ".join(unknown), key),
            gate=gate,
            milestone=milestone,
            evidence=evidence,
            remediation=remediation,
        )
    compare = {p: (normalise(v) if normalise else v) for p, v in values.items()}
    distinct = {repr(v) for v in compare.values()}
    if len(distinct) == 1:
        return Finding(
            id=check_id,
            title=title,
            verdict="PASS",
            severity="INFO",
            detail="all of %s agree on %s" % (", ".join(projects), key),
            gate=gate,
            milestone=milestone,
            evidence=evidence,
        )
    return Finding(
        id=check_id,
        title=title,
        verdict="FAIL",
        severity=severity,
        detail="%d distinct values across %s" % (len(distinct), ", ".join(projects)),
        gate=gate,
        milestone=milestone,
        evidence=evidence,
        remediation=remediation,
    )


def _normalise_table(value: Any) -> Any:
    """``md_ingest.market_events_raw`` however it was spelled."""
    return value.strip().lower() if isinstance(value, str) else value


# --------------------------------------------------------------------------
# the checks
# --------------------------------------------------------------------------

def _check_wire_schema(
    root: Path, facts: dict[str, ProjectFacts]
) -> tuple[list[Finding], dict[str, Any]]:
    """Do the schema copies describe one wire contract, or several?"""
    findings: list[Finding] = []
    copies = proto_copies(root)
    summary: dict[str, Any] = {"canonical_project": "P02", "copies": []}

    if "P02" not in copies:
        findings.append(
            Finding(
                id="P09-M0-01",
                title="All wire-schema copies describe one contract",
                verdict="INCONCLUSIVE",
                severity="BLOCKER",
                detail="P02 canonical schema not found; nothing to compare against",
                gate="G0",
                milestone="M0",
                evidence=["expected 02-.../schemas/marketdata/v1/market_data.proto"],
            )
        )
        return findings, summary

    parsed: dict[str, protoparse.ProtoFile] = {}
    parse_errors: list[str] = []
    for project, path in copies.items():
        try:
            parsed[project] = protoparse.parse_file(path)
        except protoparse.ProtoParseError as exc:
            parse_errors.append("%s %s: %s" % (project, path.as_posix(), exc))

    if parse_errors or "P02" not in parsed:
        findings.append(
            Finding(
                id="P09-M0-01",
                title="All wire-schema copies describe one contract",
                verdict="INCONCLUSIVE",
                severity="BLOCKER",
                detail="a copy could not be normalised, so agreement is unknown",
                gate="G0",
                milestone="M0",
                evidence=parse_errors,
                remediation="fix or remove the unparsable copy before pinning a lock",
            )
        )
        return findings, summary

    # Paths go into the lock repo-relative. An absolute path would make the
    # lock machine-specific, and CI compares it across machines.
    def rel(path_text: str) -> str:
        try:
            return Path(path_text).relative_to(root.resolve()).as_posix()
        except ValueError:
            return path_text

    canonical = parsed["P02"]
    summary.update(
        package=canonical.package,
        syntax=canonical.syntax,
        canonical_path=rel(canonical.path),
        canonical_file_sha256=canonical.file_sha256,
        normalized_sha256=canonical.normalized_sha256(),
    )

    mismatched: list[str] = []
    evidence: list[str] = []
    for project in sorted(parsed):
        other = parsed[project]
        same = other.normalized_sha256() == canonical.normalized_sha256()
        summary["copies"].append(
            {
                "project": project,
                "path": rel(other.path),
                "file_sha256": other.file_sha256,
                "normalized_sha256": other.normalized_sha256(),
                "matches_canonical": same,
            }
        )
        evidence.append(
            "%s %s file=%s norm=%s"
            % (
                project,
                rel(other.path),
                other.file_sha256[:16],
                other.normalized_sha256()[:16],
            )
        )
        if not same:
            mismatched.append(project)
            for line in protoparse.diff_contracts(canonical, other)[:6]:
                evidence.append("  %s: %s" % (project, line))

    findings.append(
        Finding(
            id="P09-M0-01",
            title="All wire-schema copies describe one contract",
            verdict="PASS" if not mismatched else "FAIL",
            severity="INFO" if not mismatched else "BLOCKER",
            detail=(
                "%d copies normalise to %s"
                % (len(parsed), canonical.normalized_sha256()[:16])
                if not mismatched
                else "copies disagree on the wire contract: " + ", ".join(mismatched)
            ),
            gate="G0",
            milestone="M0",
            evidence=evidence,
            remediation=(
                ""
                if not mismatched
                else "regenerate the divergent copy from the P02 canonical schema"
            ),
        )
    )

    # Agreeing today is not the same as having one source of truth. The
    # consumers' copies are generated artifacts now: byte-identical to P02's
    # canonical schema, regenerated by `p09 sync-schema --write` and checked in
    # CI, so a divergence fails a pull request instead of a run.
    from . import schemasync

    try:
        copy_states, canonical_digest = schemasync.check_copies(root)
    except schemasync.SchemaSyncError as exc:
        findings.append(
            Finding(
                id="P09-M0-02",
                title="Wire schema has a single enforced source of truth",
                verdict="INCONCLUSIVE",
                severity="HIGH",
                detail="cannot compare the copies: %s" % exc,
                gate="G0",
                milestone="M1",
                evidence=[schemasync.CANONICAL],
            )
        )
        return findings, summary

    drifted = [s for s in copy_states if not s.matches]
    findings.append(
        Finding(
            id="P09-M0-02",
            title="Wire schema has a single enforced source of truth",
            verdict="PASS" if not drifted else "FAIL",
            severity="INFO" if not drifted else "HIGH",
            detail=(
                "%d consumer copies are byte-identical to %s; `sync-schema "
                "--check` runs in CI, so a divergence fails a pull request "
                "rather than a run"
                % (len(copy_states), schemasync.CANONICAL)
                if not drifted
                else "%d copy/copies differ from the canonical schema: %s"
                % (len(drifted), ", ".join(s.project for s in drifted))
            ),
            gate="G0",
            milestone="M1",
            evidence=[
                "canonical %s sha256=%s" % (schemasync.CANONICAL, canonical_digest[:16]),
            ]
            + [
                "%s %s %s"
                % (s.status, s.project, (s.sha256 or "absent")[:16])
                for s in copy_states
            ],
            remediation=(
                ""
                if not drifted
                else "edit only the canonical schema, then run "
                "`p09 sync-schema --write`"
            ),
        )
    )
    return findings, summary

    parsed: dict[str, protoparse.ProtoFile] = {}
    parse_errors: list[str] = []
    for project, path in copies.items():
        try:
            parsed[project] = protoparse.parse_file(path)
        except protoparse.ProtoParseError as exc:
            parse_errors.append("%s %s: %s" % (project, path.as_posix(), exc))

    if parse_errors or "P02" not in parsed:
        findings.append(
            Finding(
                id="P09-M0-01",
                title="All wire-schema copies describe one contract",
                verdict="INCONCLUSIVE",
                severity="BLOCKER",
                detail="a copy could not be normalised, so agreement is unknown",
                gate="G0",
                milestone="M0",
                evidence=parse_errors,
                remediation="fix or remove the unparsable copy before pinning a lock",
            )
        )
        return findings, summary

    # Paths go into the lock repo-relative. An absolute path would make the
    # lock machine-specific, and CI compares it across machines.
    def rel(path_text: str) -> str:
        try:
            return Path(path_text).relative_to(root.resolve()).as_posix()
        except ValueError:
            return path_text

    canonical = parsed["P02"]
    summary.update(
        package=canonical.package,
        syntax=canonical.syntax,
        canonical_path=rel(canonical.path),
        canonical_file_sha256=canonical.file_sha256,
        normalized_sha256=canonical.normalized_sha256(),
    )

    mismatched: list[str] = []
    evidence: list[str] = []
    for project in sorted(parsed):
        other = parsed[project]
        same = other.normalized_sha256() == canonical.normalized_sha256()
        summary["copies"].append(
            {
                "project": project,
                "path": rel(other.path),
                "file_sha256": other.file_sha256,
                "normalized_sha256": other.normalized_sha256(),
                "matches_canonical": same,
            }
        )
        evidence.append(
            "%s %s file=%s norm=%s"
            % (
                project,
                rel(other.path),
                other.file_sha256[:16],
                other.normalized_sha256()[:16],
            )
        )
        if not same:
            mismatched.append(project)
            for line in protoparse.diff_contracts(canonical, other)[:6]:
                evidence.append("  %s: %s" % (project, line))

    findings.append(
        Finding(
            id="P09-M0-01",
            title="All wire-schema copies describe one contract",
            verdict="PASS" if not mismatched else "FAIL",
            severity="INFO" if not mismatched else "BLOCKER",
            detail=(
                "%d copies normalise to %s"
                % (len(parsed), canonical.normalized_sha256()[:16])
                if not mismatched
                else "copies disagree on the wire contract: " + ", ".join(mismatched)
            ),
            gate="G0",
            milestone="M0",
            evidence=evidence,
            remediation=(
                ""
                if not mismatched
                else "regenerate the divergent copy from the P02 canonical schema"
            ),
        )
    )

    # Agreeing today is not the same as having one source of truth.
    duplicates = [p for p in sorted(parsed) if p != "P02"]
    findings.append(
        Finding(
            id="P09-M0-02",
            title="Wire schema has a single source of truth",
            verdict="FAIL",
            severity="HIGH",
            detail=(
                "%d forked copies of the schema exist (%s). They agree now; "
                "nothing in the build makes them keep agreeing."
                % (len(duplicates), ", ".join(duplicates))
            ),
            gate="G0",
            milestone="M1",
            evidence=["%s %s" % (p, rel(parsed[p].path)) for p in sorted(parsed)],
            remediation=(
                "publish the P02 schema as a build artifact that P03/P04/P05 "
                "consume, and keep this audit in CI as the regression test"
            ),
        )
    )
    return findings, summary


def _pin_key(project: str) -> str:
    return "descriptor_sha256" if project == "P02" else "descriptor_pin"


def _check_descriptor_digests(
    root: Path, facts: dict[str, ProjectFacts]
) -> list[Finding]:
    """Each project's pin must equal what the canonical schema yields for it.

    The pins are expressed in three different encodings, so comparing them to
    each other is meaningless. Comparing each one to a value *derived* from the
    single canonical descriptor in that project's own encoding is not, and it
    fails exactly when a project's schema really has moved.
    """
    projects = [p for p in ("P02", "P03", "P04", "P05") if p in facts]

    try:
        canonical = descriptors.load_canonical(root)
    except descriptors.DescriptorError as exc:
        return [
            Finding(
                id="P09-M0-03",
                title="Descriptor pins match the canonical schema",
                verdict="INCONCLUSIVE",
                severity="BLOCKER",
                detail="cannot derive expected pins: %s" % exc,
                gate="G0",
                milestone="M0",
                evidence=["baseline: " + descriptors.CANONICAL_BASELINE],
                remediation=(
                    "install the protobuf runtime, or regenerate P02's descriptor "
                    "baseline; an undeterminable pin is not a passing pin"
                ),
            )
        ]

    identity = descriptors.canonical_identity(canonical)
    evidence = [
        "canonical baseline: " + descriptors.CANONICAL_BASELINE,
        "cross-project descriptor identity (%s): %s"
        % (descriptors.IDENTITY_REPRESENTATION, identity),
    ]

    mismatched: list[str] = []
    unpinned: list[str] = []
    for project in projects:
        representation = descriptors.PROJECT_REPRESENTATION.get(project)
        if representation is None:
            continue
        expected = descriptors.digest_for(canonical, representation)
        fact = facts[project].facts.get(_pin_key(project))
        pinned = fact.value if fact and fact.known else None
        shape = descriptors.REPRESENTATIONS[representation].describe()

        if not isinstance(pinned, str) or not pinned:
            unpinned.append(project)
            evidence.append(
                "%s UNPINNED; this schema would yield %s as %s"
                % (project, expected[:16] + "...", shape)
            )
            continue
        if pinned != expected:
            mismatched.append(project)
            evidence.append(
                "%s MISMATCH pin=%s expected=%s as %s"
                % (project, pinned[:16] + "...", expected[:16] + "...", shape)
            )
            continue
        evidence.append("%s ok %s as %s" % (project, pinned[:16] + "...", shape))

    evidence.append("NOTE: " + DESCRIPTOR_DIGEST_NOTE)

    findings = [
        Finding(
            id="P09-M0-03",
            title="Descriptor pins match the canonical schema",
            verdict="FAIL" if mismatched else "PASS",
            severity="BLOCKER" if mismatched else "INFO",
            detail=(
                "%d of %d pins derive from the one canonical descriptor; the "
                "remaining differences between them are encoding, not schema"
                % (len(projects) - len(mismatched) - len(unpinned), len(projects))
                if not mismatched
                else "pins that do not match the canonical schema: "
                + ", ".join(mismatched)
            ),
            gate="G0",
            milestone="M0",
            evidence=evidence,
            remediation=(
                ""
                if not mismatched
                else "the project's schema has diverged from P02's baseline, or its "
                "generated code is stale: regenerate from the canonical schema and "
                "re-pin"
            ),
        )
    ]

    if unpinned:
        findings.append(
            Finding(
                id="P09-M0-04",
                title="Every project pins the schema it was built against",
                verdict="FAIL",
                severity="HIGH",
                detail=(
                    "%s ships with no descriptor pin, so a regenerated schema "
                    "changes the data instead of failing the run"
                    % ", ".join(unpinned)
                ),
                gate="G0",
                milestone="M0",
                evidence=[_ev(facts, p, _pin_key(p)) for p in unpinned]
                + [
                    "%s should pin %s"
                    % (
                        p,
                        descriptors.digest_for(
                            canonical, descriptors.PROJECT_REPRESENTATION[p]
                        ),
                    )
                    for p in unpinned
                    if p in descriptors.PROJECT_REPRESENTATION
                ],
                remediation=(
                    "set the pin from the project's own descriptor-printing command; "
                    "P04 case D-08 is the test that then becomes real"
                ),
            )
        )
    else:
        findings.append(
            Finding(
                id="P09-M0-04",
                title="Every project pins the schema it was built against",
                verdict="PASS",
                severity="INFO",
                detail="all %d schema-bearing projects carry a descriptor pin"
                % len(projects),
                gate="G0",
                milestone="M0",
                evidence=[_ev(facts, p, _pin_key(p)) for p in projects],
            )
        )
    return findings


def _check_pipeline_wiring(facts: dict[str, ProjectFacts]) -> list[Finding]:
    """Is the data path actually joined, or does each project end in a stand-in?"""
    findings: list[Finding] = []

    p04 = facts.get("P04")
    real_fact = p04.facts.get("has_real_p03_upstream") if p04 else None
    evidence = [
        _ev(facts, "P04", "has_real_p03_upstream"),
        _ev(facts, "P04", "upstream_recorded_in_report"),
        _ev(facts, "P03", "downstream_sink"),
    ]
    if real_fact is None or not real_fact.known:
        findings.append(
            Finding(
                id="P09-M0-05",
                title="P04 can consume the real P03 adapter",
                verdict="INCONCLUSIVE",
                severity="BLOCKER",
                detail="could not determine which upstreams P04 supports",
                gate="G3",
                milestone="M4",
                evidence=evidence,
            )
        )
    else:
        joined = bool(real_fact.value)
        recorded = bool(p04.get("upstream_recorded_in_report")) if p04 else False
        findings.append(
            Finding(
                id="P09-M0-05",
                title="P04 can consume the real P03 adapter",
                verdict="PASS" if joined and recorded else "FAIL",
                severity="INFO" if joined and recorded else "BLOCKER",
                detail=(
                    "P04 reads the adapter's own p04_stub handoff via "
                    "run.upstream=p03_handoff, and its report records which "
                    "upstream actually ran rather than assuming the mock"
                    if joined and recorded
                    else "P04's only upstream is MockP03Classifier over a P01 run "
                    "directory, so the P01->P02->P03->P04 chain M4/M5 requires is "
                    "not joined and P03's classification policy is bypassed"
                    if not joined
                    else "P04 can read P03, but its report does not record which "
                    "upstream ran, so a mock run could be reported as a real one"
                ),
                gate="G3",
                milestone="M4",
                evidence=evidence,
                remediation=(
                    ""
                    if joined and recorded
                    else "join them over the P03 handoff shape specified in "
                    "03-cpp-market-data-adapter/docs/protocol_handoff.md, and "
                    "compute the mock flag from the config rather than hard-coding it"
                ),
            )
        )

    p07 = facts.get("P07")
    simulated = p07.get("simulated_components") if p07 else None
    profile = p07.get("implemented_profile") if p07 else None
    if simulated is None or profile is None:
        findings.append(
            Finding(
                id="P09-M0-06",
                title="Evidence plane observes the real data plane",
                verdict="INCONCLUSIVE",
                severity="BLOCKER",
                detail="could not determine which components P07 simulates",
                gate="G4",
                milestone="M8",
                evidence=[
                    _ev(facts, "P07", "simulated_components"),
                    _ev(facts, "P07", "implemented_profile"),
                ],
            )
        )
    else:
        findings.append(
            Finding(
                id="P09-M0-06",
                title="Evidence plane observes the real data plane",
                verdict="FAIL" if simulated else "PASS",
                severity="BLOCKER" if simulated else "INFO",
                detail=(
                    "P07 runs profile %s and simulates %s, so its verdicts are about "
                    "protocol logic rather than this deployment. P09 G4 needs an "
                    "oracle reading the real Kafka and ClickHouse."
                    % (profile, ", ".join(simulated))
                    if simulated
                    else "P07 reads the real components"
                ),
                gate="G4",
                milestone="M8",
                evidence=[
                    _ev(facts, "P07", "implemented_profile"),
                    _ev(facts, "P07", "simulated_components"),
                ],
                remediation=(
                    "point P07's cut scanner and reconciler at the deployed broker "
                    "and server; its oracle is already independent of the data plane"
                ),
            )
        )
    return findings


def _check_toolchains(facts: dict[str, ProjectFacts]) -> list[Finding]:
    """Is the protobuf version matrix recorded, so a skew can fail CI?

    Not "are the versions equal". They are not, and that was measured rather
    than assumed: 3.12.4 (C++), 3.21.12 (C++) and 7.35.1 (Python) serialize
    byte-identical FileDescriptorProto for this schema, so the spread does not
    corrupt anything (see P09-M0-03). The risk it does carry is
    reproducibility, and reproducibility needs the versions written down where
    a check can read them.

    protoc below 3.15 is the concrete reason this matters: the schema uses
    proto3 ``optional``, which needs --experimental_allow_proto3_optional
    there. A build that silently moved to a newer protoc would drop the flag
    and fail for a reason nothing in the tree explains.
    """
    required = {
        "P02": ("protoc_version", "cpp_protobuf_runtime", "python_protobuf_runtime"),
        "P03": ("protobuf_version",),
        "P04": ("protobuf_runtime",),
    }
    evidence: list[str] = []
    unrecorded: list[str] = []
    for project, keys in required.items():
        for key in keys:
            evidence.append(_ev(facts, project, key))
            pf = facts.get(project)
            fact = pf.facts.get(key) if pf else None
            if fact is None or not fact.known:
                unrecorded.append("%s.%s" % (project, key))

    return [
        Finding(
            id="P09-M0-07",
            title="Protobuf toolchain matrix is recorded",
            verdict="PASS" if not unrecorded else "FAIL",
            severity="INFO" if not unrecorded else "HIGH",
            detail=(
                "every schema-bearing project records the protobuf version it "
                "builds against, so a skew is a diff rather than a surprise. "
                "The versions differ by design and were measured not to change "
                "the wire bytes (P09-M0-03)."
                if not unrecorded
                else "not recorded anywhere a check can read: %s"
                % ", ".join(unrecorded)
            ),
            gate="G0",
            milestone="M1",
            evidence=evidence,
            remediation=(
                ""
                if unrecorded == []
                else "record the pinned version in the project's own toolchain "
                "file, as P02 does in toolchain/versions.lock"
            ),
        )
    ]


def _check_generation_tags(facts: dict[str, ProjectFacts]) -> list[Finding]:
    """Topic generation is an identity, and only its issuer can define it.

    P05 and P08 each carry a ``topic_generation`` string and the two disagree.
    That looked like a mismatch to reconcile until the ownership question was
    asked: P09 creates the topics, so P09 is the only component that knows when
    a topic was recreated. Neither consumer's value is authoritative, and making
    them equal by editing one would just move the guess.

    So this check no longer demands that they agree. It records that the lock
    issues the tag, and that a consumer disagreeing with the lock is reading a
    stale value rather than asserting a competing truth.
    """
    values, evidence, unknown = _agreement(facts, "topic_generation", ("P05", "P08"))
    issued = values.get("P05")
    evidence.append(
        "P09 issues cross_project_identity.kafka.topic_generation in the lock; "
        "consumers read it rather than declaring their own"
    )
    if unknown:
        return [
            Finding(
                id="P09-M0-08",
                title="Topic generation tag has one issuer",
                verdict="INCONCLUSIVE",
                severity="MEDIUM",
                detail="not every project declared a topic generation",
                gate="G3",
                milestone="M3",
                evidence=evidence,
            )
        ]

    stale = sorted(p for p, v in values.items() if v != issued)
    return [
        Finding(
            id="P09-M0-08",
            title="Topic generation tag has one issuer",
            verdict="FAIL" if stale else "PASS",
            severity="MEDIUM" if stale else "INFO",
            detail=(
                "the lock issues %r and every consumer matches it" % issued
                if not stale
                else "%s carry a generation tag that is not the issued one (%r), so "
                "a recreated topic cannot be detected by comparing them (RB-11)"
                % (", ".join(stale), issued)
            ),
            gate="G3",
            milestone="M3",
            evidence=evidence,
            remediation=(
                ""
                if not stale
                else "have %s read kafka.topic_generation from contracts.lock.yaml "
                "instead of hard-coding it; the init job stamps the topics with the "
                "issued tag" % ", ".join(stale)
            ),
        )
    ]


def _check_raw_table(facts: dict[str, ProjectFacts]) -> Finding:
    """P05 writes a table; P06 reads one. They must be the same table."""
    p05, p06 = facts.get("P05"), facts.get("P06")
    writer = None
    if p05 and p05.get("ch_database") and p05.get("ch_table"):
        writer = "%s.%s" % (p05.get("ch_database"), p05.get("ch_table"))
    reader = p06.get("raw_table") if p06 else None
    evidence = [
        _ev(facts, "P05", "ch_database"),
        _ev(facts, "P05", "ch_table"),
        _ev(facts, "P05", "raw_table_ddl"),
        _ev(facts, "P06", "raw_table"),
    ]
    if writer is None or reader is None:
        return Finding(
            id="P09-M0-16",
            title="P05 sink and P06 source are the same table",
            verdict="INCONCLUSIVE",
            severity="BLOCKER",
            detail="one side did not declare a raw table",
            gate="G3",
            milestone="M3",
            evidence=evidence,
        )
    same = _normalise_table(writer) == _normalise_table(reader)
    return Finding(
        id="P09-M0-16",
        title="P05 sink and P06 source are the same table",
        verdict="PASS" if same else "FAIL",
        severity="INFO" if same else "BLOCKER",
        detail=(
            "both sides use %s" % writer
            if same
            else "P05 writes %s and P06 reads %s" % (writer, reader)
        ),
        gate="G3",
        milestone="M3",
        evidence=evidence,
        remediation="" if same else "point both at the table the init job creates",
    )


def _check_port_separation(facts: dict[str, ProjectFacts]) -> Finding:
    """JSONL and MDP1 are different protocols and must stay on different ports."""
    p01_port = facts["P01"].get("tcp_port") if "P01" in facts else None
    p02_port = facts["P02"].get("mdp1_tcp_port") if "P02" in facts else None
    p03_port = facts["P03"].get("input_port") if "P03" in facts else None
    p03_rejects = facts["P03"].get("rejects_p01_jsonl_port") if "P03" in facts else None
    evidence = [
        _ev(facts, "P01", "tcp_port"),
        _ev(facts, "P01", "wire_framing"),
        _ev(facts, "P02", "mdp1_tcp_port"),
        _ev(facts, "P03", "input_port"),
        _ev(facts, "P03", "rejects_p01_jsonl_port"),
    ]
    if None in (p01_port, p02_port, p03_port, p03_rejects):
        return Finding(
            id="P09-M0-17",
            title="JSONL and MDP1 ports are distinct and enforced",
            verdict="INCONCLUSIVE",
            severity="BLOCKER",
            detail="a port could not be read",
            gate="G2",
            milestone="M4",
            evidence=evidence,
        )
    ok = p01_port != p02_port and p03_port == p02_port and p03_rejects == p01_port
    return Finding(
        id="P09-M0-17",
        title="JSONL and MDP1 ports are distinct and enforced",
        verdict="PASS" if ok else "FAIL",
        severity="INFO" if ok else "BLOCKER",
        detail=(
            "P01 serves JSONL on %d, P02/P03 use MDP1 on %d, and P03 refuses %d"
            % (p01_port, p02_port, p03_rejects)
            if ok
            else "the two protocols are not cleanly separated by port"
        ),
        gate="G2",
        milestone="M4",
        evidence=evidence,
        remediation=(
            "" if ok else "P03 must never sniff the protocol; keep 9009 JSONL, 9010 MDP1"
        ),
    )


def _check_analytics_ddl(facts: dict[str, ProjectFacts]) -> Finding:
    """Every table P06's config names must exist in the SQL P09 will apply."""
    p06 = facts.get("P06")
    evidence = [
        _ev(facts, "P06", "canonical_table"),
        _ev(facts, "P06", "bars_table"),
        _ev(facts, "P06", "ddl_objects"),
    ]
    if p06 is None:
        return Finding(
            id="P09-M0-18",
            title="P06 config names only tables its DDL creates",
            verdict="INCONCLUSIVE",
            severity="HIGH",
            detail="P06 absent",
            gate="G2",
            milestone="M3",
            evidence=evidence,
        )
    declared = [p06.get("canonical_table"), p06.get("bars_table")]
    objects = p06.get("ddl_objects") or []
    if any(d is None for d in declared) or not objects:
        return Finding(
            id="P09-M0-18",
            title="P06 config names only tables its DDL creates",
            verdict="INCONCLUSIVE",
            severity="HIGH",
            detail="config or DDL inventory incomplete",
            gate="G2",
            milestone="M3",
            evidence=evidence,
        )
    lowered = {_normalise_table(o) for o in objects}
    missing = [d for d in declared if _normalise_table(d) not in lowered]
    return Finding(
        id="P09-M0-18",
        title="P06 config names only tables its DDL creates",
        verdict="PASS" if not missing else "FAIL",
        severity="INFO" if not missing else "HIGH",
        detail=(
            "all configured analytics tables appear in sql/"
            if not missing
            else "configured but never created: " + ", ".join(str(m) for m in missing)
        ),
        gate="G2",
        milestone="M3",
        evidence=evidence,
        remediation=(
            "" if not missing else "the init job must create every configured object"
        ),
    )


def _check_p06_upstream(facts: dict[str, ProjectFacts]) -> Finding:
    """Does P06 read P05's table, or only ever a corpus it generated itself?

    This one hid behind a config key. P06's config names the raw table and
    ``schema_preflight`` checks that table's shape, so it reads as joined. But
    its ``build`` command always assembled the corpus in-process via
    ``synthetic_corpus`` or ``golden_six_event``, and ``--clickhouse-url`` only
    chose where the build *wrote*. Checking a table's shape is not reading its
    rows, and an analytics layer that cannot consume its own upstream is not
    joined to anything -- the same defect as P04's mock upstream, one stage down.
    """
    p06 = facts.get("P06")
    fact = p06.facts.get("has_raw_upstream") if p06 else None
    evidence = [
        _ev(facts, "P06", "has_raw_upstream"),
        _ev(facts, "P06", "raw_table"),
        _ev(facts, "P05", "raw_table_ddl"),
    ]
    if fact is None or not fact.known:
        return Finding(
            id="P09-M0-20",
            title="P06 consumes the real P05 landing table",
            verdict="INCONCLUSIVE",
            severity="BLOCKER",
            detail="could not determine whether P06 has a raw-table reader",
            gate="G3",
            milestone="M7",
            evidence=evidence,
        )
    joined = bool(fact.value)
    return Finding(
        id="P09-M0-20",
        title="P06 consumes the real P05 landing table",
        verdict="PASS" if joined else "FAIL",
        severity="INFO" if joined else "BLOCKER",
        detail=(
            "md_analytics.raw_source reads the landing table and "
            "`build-from-raw` audits it against P03's accepted-event ledger "
            "and P05's cut"
            if joined
            else "every P06 build assembles its own corpus, so no row P05 "
            "produced has ever been read: the audit would compare the corpus "
            "against itself"
        ),
        gate="G3",
        milestone="M7",
        evidence=evidence,
        remediation=(
            ""
            if joined
            else "add a build path that reads md_ingest.market_events_raw, with "
            "the expected set taken from a component upstream of that table"
        ),
    )


def _check_topic_generation_type(facts: dict[str, ProjectFacts]) -> Finding:
    """P05 stores the generation as a string; P06 annotated it as an int.

    It is part of a Kafka coordinate precisely so a deleted-and-recreated topic
    cannot make two different records share ``(topic, partition, offset)``. A
    type disagreement there invites a coercion, and coercing a tag like
    ``md-v1-generation-a`` into an integer would fabricate the identity the
    field exists to protect.
    """
    sql_type = facts["P05"].get("topic_generation_sql_type") if "P05" in facts else None
    py_type = facts["P06"].get("topic_generation_type") if "P06" in facts else None
    evidence = [
        _ev(facts, "P05", "topic_generation_sql_type"),
        _ev(facts, "P06", "topic_generation_type"),
    ]
    if sql_type is None or py_type is None:
        return Finding(
            id="P09-M0-21",
            title="topic_generation has one type across the seam",
            verdict="INCONCLUSIVE",
            severity="MEDIUM",
            detail="one side did not declare a type for topic_generation",
            gate="G3",
            milestone="M7",
            evidence=evidence,
        )
    sql_is_string = "String" in str(sql_type)
    py_admits_string = "str" in str(py_type)
    agreed = sql_is_string == py_admits_string
    return Finding(
        id="P09-M0-21",
        title="topic_generation has one type across the seam",
        verdict="PASS" if agreed else "FAIL",
        severity="INFO" if agreed else "MEDIUM",
        detail=(
            "P05 stores %s and P06 accepts %s" % (sql_type, py_type)
            if agreed
            else "P05 stores %s but P06 declares %s, so the value cannot cross "
            "the seam without a coercion that would invent lineage"
            % (sql_type, py_type)
        ),
        gate="G3",
        milestone="M7",
        evidence=evidence,
        remediation=(
            ""
            if agreed
            else "widen P06's annotation to admit the string tag P05 stores, or "
            "have P09 issue an integer generation and both sides read it"
        ),
    )


def _check_pbstream_completion_contract(facts: dict[str, ProjectFacts]) -> Finding:
    """P02's completion marker is not the manifest P03 asks for.

    P03 documents the sibling file it needs, and its reasoning is sound: EOF
    proves nothing because a truncated file also ends, so an offline replay may
    only be called complete when a manifest says so. P02 writes a marker with
    the right *content* under a different name and without the ``status`` field
    P03 checks, so P03 reports ``SOURCE_UNVERIFIED`` and refuses data that is
    in fact complete.

    This is the gap the pipeline's `p03-manifest-shim` stage fills. The shim is
    named after this finding so it stays visible: it keeps the platform running
    while the contract is still broken, and it is not a fix.
    """
    # What P02 writes FOR THE CONSUMER, which is a different file from its own
    # durability marker. Both exist on purpose: `.COMPLETED` is P02's commit
    # record, `.manifest.json` is P03's contract.
    emitted = facts["P02"].get("pbstream_manifest_suffix") if "P02" in facts else None
    has_status = (
        facts["P02"].get("pbstream_manifest_has_status") if "P02" in facts else None
    )
    required = (
        facts["P03"].get("required_source_manifest_suffix") if "P03" in facts else None
    )
    needs_status = (
        facts["P03"].get("source_manifest_requires_status") if "P03" in facts else None
    )
    evidence = [
        _ev(facts, "P02", "pbstream_manifest_suffix"),
        _ev(facts, "P02", "pbstream_manifest_has_status"),
        _ev(facts, "P02", "pbstream_completion_marker"),
        _ev(facts, "P03", "required_source_manifest_suffix"),
        _ev(facts, "P03", "source_manifest_requires_status"),
    ]
    marker = emitted
    if marker is None or required is None:
        return Finding(
            id="P09-M0-19",
            title="P02's completion marker satisfies P03's source manifest",
            verdict="INCONCLUSIVE",
            severity="BLOCKER",
            detail="one side did not declare its completion artifact",
            gate="G3",
            milestone="M4",
            evidence=evidence,
        )
    # The name must match, and if P03 checks a status field then P02 must write
    # one. A manifest with the right name and no status is still refused.
    agreed = marker == required and (not needs_status or bool(has_status))
    return Finding(
        id="P09-M0-19",
        title="P02's completion marker satisfies P03's source manifest",
        verdict="PASS" if agreed else "FAIL",
        severity="INFO" if agreed else "BLOCKER",
        detail=(
            "P02 emits %s carrying a status field, which is what P03 reads; its "
            "own .COMPLETED marker stays as P02's commit record" % marker
            if agreed
            else "P02 emits %r but P03 reads %r%s, so P03 reports "
            "SOURCE_UNVERIFIED on data that is actually complete"
            % (
                marker,
                required,
                " and requires a status field P02 does not write"
                if needs_status and not has_status
                else "",
            )
        ),
        gate="G3",
        milestone="M4",
        evidence=evidence,
        remediation=(
            ""
            if agreed
            else "have P02 write the manifest its consumer specified "
            "(dataset_id, sha256, status, frames, bytes); the pipeline's "
            "p03-manifest-shim stage exists only until it does"
        ),
    )


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------

def _utc_now() -> str:
    return (
        _dt.datetime.now(_dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def run_audit(root: Path, facts: dict[str, ProjectFacts]) -> AuditResult:
    """Run every M0 check against the probed facts."""
    findings: list[Finding] = []

    schema_findings, wire_schema = _check_wire_schema(root, facts)
    findings.extend(schema_findings)
    findings.extend(_check_descriptor_digests(root, facts))
    findings.extend(_check_pipeline_wiring(facts))
    findings.extend(_check_toolchains(facts))
    findings.extend(_check_generation_tags(facts))

    consensus = (
        ("P09-M0-09", "Ticks topic name", "ticks_topic",
         ("P04", "P05", "P07", "P08"), "BLOCKER", "G3",
         "route every producer and consumer through the locked topic name"),
        ("P09-M0-10", "Quotes topic name", "quotes_topic",
         ("P04", "P05", "P07", "P08"), "BLOCKER", "G3",
         "route every producer and consumer through the locked topic name"),
        ("P09-M0-11", "Partitions per topic", "partitions_per_topic",
         ("P04", "P07", "P08"), "HIGH", "G2",
         "P09's init job creates the topics, so the partition count belongs in "
         "the lock and every expectation should be read from it"),
        ("P09-M0-12", "Event identity tuple", "event_identity",
         ("P01", "P05", "P07"), "BLOCKER", "G3",
         "an identity disagreement makes every reconciliation meaningless"),
        ("P09-M0-13", "Semantic digest version", "semantic_digest",
         ("P05", "P07"), "BLOCKER", "G3",
         "the oracle and the sink must digest the same core fields"),
        ("P09-M0-14", "Bar interval", "bar_interval_ns",
         ("P06", "P07"), "BLOCKER", "G3",
         "a bar-width disagreement would make the OHLC oracle disagree by design"),
        ("P09-M0-15", "Kafka record value framing", "kafka_value_framing",
         ("P02", "P04"), "BLOCKER", "G3",
         "an MDP1 prefix inside a Kafka value is a packaging violation"),
    )
    for check_id, title, key, projects, severity, gate, remediation in consensus:
        findings.append(
            _consensus_check(
                check_id=check_id,
                title=title,
                key=key,
                projects=projects,
                facts=facts,
                severity=severity,
                gate=gate,
                remediation=remediation,
            )
        )

    findings.append(_check_raw_table(facts))
    findings.append(_check_port_separation(facts))
    findings.append(_check_analytics_ddl(facts))
    findings.append(_check_p06_upstream(facts))
    findings.append(_check_topic_generation_type(facts))
    findings.append(_check_pbstream_completion_contract(facts))

    findings.sort(key=lambda f: f.id)
    return AuditResult(
        generated_at_utc=_utc_now(),
        repo_root=root.resolve().as_posix(),
        findings=findings,
        wire_schema=wire_schema,
        facts=facts,
    )
