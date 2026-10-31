"""Coverage of the 170-case what-if matrix, and the report's honesty rules.

This file exists so the matrix cannot quietly rot. It reports how many of the
170 documented cases the suite actually claims, which cases are unclaimed, and
-- most importantly -- asserts that an unclaimed case can never be presented as
a pass.

It deliberately does **not** assert 100% coverage. Several cases in the matrix
need things this environment does not have: cgroup CPU and memory limits
(H09/H10), a 60-minute soak (H08), ENOSPC injection (I11), a tampered protoc
binary (J11), a real Kafka broker (L07-L10). Claiming those as covered would be
exactly the false success the whole design argues against. They stay NOT_RUN and
are listed below with a reason, which is the honest state.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from md_contract.report import CASE_GROUPS, TOTAL_CASES, TestReport, Verdict, all_case_ids

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def collect_claimed_cases() -> dict[str, list[str]]:
    """Scan the suite for ``@pytest.mark.cases(...)`` ids, with their files."""
    # Anchored at the start of a line so the scanner does not match prose
    # mentioning the marker -- including this function's own docstring.
    pattern = re.compile(r"^[ 	]*@pytest\.mark\.cases\(([^)]*)\)", re.MULTILINE)
    claimed: dict[str, list[str]] = {}
    for path in sorted((PROJECT_ROOT / "tests").rglob("test_*.py")):
        text = path.read_text(encoding="utf-8")
        for match in pattern.finditer(text):
            for raw in match.group(1).split(","):
                case = raw.strip().strip("\"'")
                if case:
                    claimed.setdefault(case, []).append(path.name)
    # The C++ suite tags its cases in test names rather than pytest marks.
    cpp = PROJECT_ROOT / "cpp" / "tests" / "md_cpp_tests.cpp"
    if cpp.exists():
        for match in re.finditer(
            r'BeginCase\("([A-L]\d{2})(?:/([A-L]\d{2}))?', cpp.read_text(encoding="utf-8")
        ):
            for case in match.groups():
                if case:
                    claimed.setdefault(case, []).append("md_cpp_tests.cpp")
    return claimed


#: Cases this environment genuinely cannot exercise, with the reason. Listed
#: explicitly so "not covered" is a documented decision rather than an
#: oversight, and so the list can be reviewed when the environment changes.
UNREACHABLE_HERE: dict[str, str] = {
    "A02": "needs a multi-release descriptor history with a reserved-then-reused tag",
    "A08": "int64 -> string on event_time_ns; the type gate that blocks it is proven by A06",
    "A10": "needs a schema moving existing scalars into an existing oneof",
    "A12": "a scale semantic change is invisible to a descriptor diff by design",
    "B11": "no enum exists in marketdata.v1, so there is no unknown-enum path",
    "E08": "stale .pb.cc detection is generate_proto.sh --check-stale, a CI step",
    "E10": "needs a 32-bit or big-endian build target",
    "E11": "needs O0/O2/O3 comparative builds in CI",
    "E12": "C++ thread-safety needs a TSan build",
    "G08": "needs a repeated-field future schema fixture",
    "G10": "needs two protobuf runtime versions to compare serialisation",
    "G12": "needs a descriptor history where a tag was reserved then reclaimed",
    "H05": "a 30-second 3x burst is outside the test-suite time budget",
    "H09": "needs cgroup CPU throttling",
    "H10": "needs a cgroup memory limit below the working set",
    "H14": "logging on/off comparison needs a logging hot path to exist first",
    "H16": "C++ allocator profiling needs ASan/heap profiling builds",
    "H18": "CPU hotspot attribution needs perf/flamegraph tooling",
    "I02": "an RST landing exactly between header bytes is not reliably reproducible",
    "I09": "SIGTERM mid-encode needs process-level signal injection",
    "I11": "needs an ENOSPC-injected filesystem",
    "J10": "dependency CVE handling is a release process, not a unit test",
    "J11": "needs a tampered pinned protoc binary",
    "J12": "log sanitisation needs a logging hot path to exist first",
    "K01": "covered by generate_proto.sh --check-stale, which runs in CI not pytest",
    "K02": "needs a second CI machine to compare build metadata against",
    "K04": "a missing-protobuf link failure is a build-time check, not a pytest",
    "K05": "needs a multi-release descriptor history",
    "K06": "a staged two-version rollout rehearsal needs a deployment target",
    "K08": "fixture drift detection is a review gate plus manifest digests",
    "K09": "report schema validation is asserted in this file instead",
    "K10": "fuzz seeds are printed on failure; a crash artifact needs a crash",
    "K11": "rollback with an old binary needs two deployed versions",
    "K12": "benchmark regression needs a pinned baseline machine and history",
    "L05": "replay pacing at 0.5x/10x belongs to Project 01's replay mode",
    "L06": "stale-quote policy is Project 03's adapter, not this codec",
    "L07": "needs a Kafka broker or mock; the contract is documented for P04",
    "L08": "needs a Kafka record key/value fixture",
    "L09": "Kafka retry duplication is Project 04/05 behaviour",
    "L10": "a mixed-version Kafka topic needs a broker",
    "L11": "dead-letter artifact handling is a Project 04/05 concern",
    "L12": "needs a simulator schema change simulated end to end",
}


def test_the_matrix_totals_match_the_design_document() -> None:
    # A14 + B12 + C18 + D20 + E14 + F12 + G12 + H18 + I14 + J12 + K12 + L12
    assert CASE_GROUPS == {
        "A": 14, "B": 12, "C": 18, "D": 20, "E": 14, "F": 12,
        "G": 12, "H": 18, "I": 14, "J": 12, "K": 12, "L": 12,
    }
    assert TOTAL_CASES == 170
    assert len(all_case_ids()) == 170


def test_every_claimed_case_id_is_a_real_case() -> None:
    """A typo'd case id would silently claim coverage of nothing."""
    valid = set(all_case_ids())
    claimed = collect_claimed_cases()
    unknown = sorted(set(claimed) - valid)
    assert not unknown, f"tests claim case ids that do not exist: {unknown}"


def test_every_unreachable_entry_is_real_and_is_not_also_claimed() -> None:
    valid = set(all_case_ids())
    bogus = sorted(set(UNREACHABLE_HERE) - valid)
    assert not bogus, f"UNREACHABLE_HERE names cases that do not exist: {bogus}"

    # If a case became testable, it must leave the exclusion list, so the list
    # cannot drift into excusing things that are in fact covered.
    claimed = collect_claimed_cases()
    both = sorted(set(UNREACHABLE_HERE) & set(claimed))
    assert not both, (
        f"these cases are listed as unreachable but tests claim them: {both} -- "
        "remove them from UNREACHABLE_HERE"
    )


def test_coverage_is_reported_and_every_gap_has_a_stated_reason() -> None:
    """The gap between what is covered and what is documented must be explained."""
    claimed = collect_claimed_cases()
    all_cases = set(all_case_ids())
    uncovered = sorted(all_cases - set(claimed))
    unexplained = [case for case in uncovered if case not in UNREACHABLE_HERE]

    by_group: dict[str, int] = {}
    for case in claimed:
        by_group[case[0]] = by_group.get(case[0], 0) + 1
    print(
        f"\n  what-if coverage: {len(claimed)}/{TOTAL_CASES} cases claimed by tests"
        f"\n  documented as unreachable in this environment: {len(UNREACHABLE_HERE)}"
        f"\n  unexplained gaps: {len(unexplained)}"
        "\n  per group: "
        + "  ".join(
            f"{group}={by_group.get(group, 0)}/{count}"
            for group, count in CASE_GROUPS.items()
        )
    )

    assert not unexplained, (
        f"{len(unexplained)} cases are neither tested nor documented as "
        f"unreachable: {unexplained}"
    )


def test_an_unrun_case_can_never_be_reported_as_a_pass() -> None:
    """The honesty rule of section 11.2, asserted on the report itself."""
    report = TestReport(run_id="coverage-check")
    # A fresh report has every case NOT_RUN, and that is not a GO.
    assert report.counts()[Verdict.NOT_RUN.value] == TOTAL_CASES
    verdict, blockers = report.release_verdict()
    assert verdict == "NOT_EVALUATED"
    assert blockers

    # One pass does not make the rest passes.
    report.record("A01", Verdict.PASS, evidence="test_schema_evolution.py")
    verdict, blockers = report.release_verdict()
    assert verdict == "NOT_EVALUATED"
    assert "NOT_RUN" in blockers[0]

    # A single FAIL is a NO_GO regardless of how much else passed.
    for case in all_case_ids():
        report.record(case, Verdict.PASS)
    assert report.release_verdict()[0] == "GO"
    report.record("D07", Verdict.FAIL, evidence="simulated")
    verdict, blockers = report.release_verdict()
    assert verdict == "NO_GO"
    assert "D07" in blockers[0]


def test_a_failure_is_not_overwritten_by_a_later_pass() -> None:
    report = TestReport()
    report.record("D07", Verdict.FAIL, evidence="first run failed")
    report.record("D07", Verdict.PASS, evidence="second run passed")
    # A scenario that failed once is not covered because another test of it
    # passed; the FAIL stands until the cause is understood.
    assert report.cases["D07"].verdict is Verdict.FAIL


def test_skipped_is_distinct_from_passed_and_from_not_run() -> None:
    report = TestReport()
    report.record("E05", Verdict.SKIPPED_WITH_REASON, evidence="needs two protobuf installs")
    counts = report.counts()
    assert counts[Verdict.SKIPPED_WITH_REASON.value] == 1
    assert counts[Verdict.PASS.value] == 0
    assert counts[Verdict.NOT_RUN.value] == TOTAL_CASES - 1
    assert report.as_dict()["status"] == Verdict.INCOMPLETE.value


def test_the_report_carries_everything_needed_to_attribute_a_result() -> None:
    """Case K09: a report missing its toolchain versions is not publishable."""
    from md_contract.conformance import environment_metadata

    report = TestReport(run_id="attribution-check")
    report.add_section("environment", environment_metadata())
    payload = report.as_dict()
    environment = payload["sections"]["environment"]
    for key in (
        "python_version",
        "python_protobuf_runtime_version",
        "python_protobuf_implementation",
        "platform",
        "toolchain_lock",
    ):
        assert environment[key], f"the report must carry {key}"
    lock = environment["toolchain_lock"]
    for key in ("protoc_version", "cpp_protobuf_runtime_version", "proto_v1_sha256"):
        assert lock.get(key), f"the toolchain lock must carry {key}"


def test_the_case_results_file_from_the_last_run_is_consistent() -> None:
    """If a previous run wrote reports/what-if-cases.json, sanity-check it."""
    path = PROJECT_ROOT / "reports" / "what-if-cases.json"
    if not path.exists():
        pytest.skip("no previous run recorded; run the full suite first")
    data = json.loads(path.read_text(encoding="utf-8"))
    valid = set(all_case_ids())
    for case, entry in data["cases"].items():
        assert case in valid, f"recorded case {case} is not in the matrix"
        assert entry["verdict"] in {v.value for v in Verdict}
        assert entry["tests"], f"{case} has a verdict but names no test"
