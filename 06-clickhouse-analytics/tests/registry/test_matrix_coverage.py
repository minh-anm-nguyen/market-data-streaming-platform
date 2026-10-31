"""Check this suite against the 420-case matrix, and write a coverage report.

The registry is built from ``@covers`` decorators at import time, so by the time
these tests run, pytest has collected every module and the registry is complete.

This file deliberately does **not** assert that all 420 cases are automated.
They are not, and claiming otherwise would be the exact failure mode the design
warns about. What it asserts is that:

* the extracted matrix still matches the design document (420 cases, 30 groups,
  150/210/60 by priority, 32 E2E);
* every case id a test claims to cover actually exists in the matrix -- a typo
  must not inflate the coverage number;
* the environment-bound cases are *declared* with a reason rather than silently
  uncovered;
* the report written to disk reflects the registry rather than a hand-kept list.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from _coverage import KINDS, REGISTRY, covered_ids, declare_unexecutable, summary
from md_analytics.errors import Verdict

REGISTRY_DIR = Path(__file__).resolve().parent
MATRIX = REGISTRY_DIR / "whatif_matrix.jsonl"
E2E_MATRIX = REGISTRY_DIR / "e2e_matrix.jsonl"
REPORT = REGISTRY_DIR / "coverage_report.json"

#: Groups whose cases are, as a group, bound to infrastructure this repo does
#: not have. Declared here so they appear in the report with a reason instead of
#: as a silent gap. Individual cases from these groups that *are* testable are
#: still registered by their own tests, and the stronger kind wins in the report.
_ENVIRONMENT_GROUPS = {
    "L": "needs a real ClickHouse to observe merges, mutations and FINAL",
    "M": "needs system.parts and real partition pruning from a server",
    "N": "needs EXPLAIN indexes=1, marks and read_rows from a server",
    "P": "needs real backup/restore tooling against a server",
    "Q": "needs live ALTER/migration against a server",
    "S": "needs concurrent sessions against a server",
    "T": "needs a real P04/P05 ingestion pipeline",
    "U": "needs a server and a 200M-row dataset for latency",
    "V": "needs the real merge scheduler and disk I/O",
    "W": "needs cgroup/CPU/RAM/filesystem pressure on a server process",
    "X": "needs replicated ClickHouse with Keeper (optional HA-LAB)",
    "Y": "needs RBAC and roles configured on a server",
    "Z": "needs system.query_log and part_log from a server",
    "AC": "needs 200M-to-billions scale on real hardware",
}


def _import_every_test_module() -> list[str]:
    """Import every test module so the ``@covers`` registry is complete.

    The decorators run at import time, so when the whole suite is collected the
    registry is already populated. But ``pytest tests/registry`` on its own
    collects nothing else, and the coverage report would then describe an empty
    registry and happily write ``executed_here: 0``. Importing the modules here
    makes this directory self-sufficient however it is invoked.

    Modules pytest has already imported under a different name are skipped, so a
    full-suite run does not register everything twice.
    """
    import importlib
    import sys

    tests_root = REGISTRY_DIR.parent
    repo_root = tests_root.parent
    for path in (repo_root, repo_root / "src", tests_root):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))

    failures: list[str] = []
    for path in sorted(tests_root.rglob("test_*.py")):
        if path.parent == REGISTRY_DIR:
            continue
        if path.stem in sys.modules:
            continue  # already imported by pytest's own collection
        dotted = ".".join(path.relative_to(repo_root).with_suffix("").parts)
        try:
            importlib.import_module(dotted)
        except Exception as exc:  # noqa: BLE001 -- reported, not swallowed
            failures.append(f"{dotted}: {type(exc).__name__}: {exc}")
    return failures


def load_matrix() -> list[dict]:
    return [json.loads(line) for line in MATRIX.read_text(encoding="utf-8").splitlines() if line]


def load_e2e() -> list[dict]:
    return [json.loads(line) for line in E2E_MATRIX.read_text(encoding="utf-8").splitlines() if line]


@pytest.fixture(scope="module")
def matrix() -> list[dict]:
    return load_matrix()


#: Individual cases from otherwise-covered groups that still need something this
#: host does not have. Listed one by one, with the specific missing capability,
#: so "not covered" cannot quietly become "forgotten".
_ENVIRONMENT_CASES = {
    "P06-F-09": ("needs_infra", "needs two replicas with injectable apply lag"),
    "P06-J-09": ("needs_infra", "needs a capacity host to produce real tail latency"),
    "P06-K-05": (
        "needs_clickhouse",
        "insert de-duplication with dependent views is version-specific; only a "
        "pinned server can answer it (section 4.2, release notes 26.1)",
    ),
    "P06-K-07": ("needs_clickhouse", "needs POPULATE racing a concurrent INSERT on a server"),
    "P06-K-13": ("needs_clickhouse", "needs real ingestion cost measurement on a server"),
    "P06-O-06": ("needs_infra", "needs disk pressure delaying TTL merges"),
    "P06-O-09": ("needs_clickhouse", "needs ALTER TABLE ... MATERIALIZE TTL on a server"),
    "P06-O-10": ("needs_infra", "needs real backup tooling and a backup window"),
    "P06-R-12": ("needs_infra", "needs an HTTP server to time out after the query succeeds"),
    "P06-AA-06": ("needs_infra", "needs a container registry and CI to float an image tag"),
    "P06-AA-08": ("needs_clickhouse", "needs server-side async_insert defaults"),
    "P06-AA-12": ("needs_infra", "needs a real P05 with a backpressure threshold"),
    "P06-AA-13": ("needs_infra", "needs a shared CI runner to produce timing variance"),
    "P06-AB-06": ("needs_infra", "Kafka skew + real merges + 16 concurrent readers"),
    "P06-AB-08": ("needs_infra", "server restart + follower lag + query failover"),
    "P06-AB-09": ("needs_clickhouse", "live migration while the builder is writing"),
    "P06-AB-10": ("needs_infra", "server OOM + in-flight backup + near-full disk"),
    "P06-AB-12": ("needs_infra", "NTP step at 120k offered events/s with expired TTL"),
    "P06-AB-13": ("needs_infra", "query cache thrash + paused merges + slow disk"),
}


@pytest.fixture(scope="module", autouse=True)
def declare_environment_groups(matrix):
    """Populate the registry, then declare the environment-bound cases."""
    import_failures = _import_every_test_module()
    assert not import_failures, (
        "a test module could not be imported, so the coverage report would "
        f"understate what the suite covers: {import_failures}"
    )
    for group, reason in _ENVIRONMENT_GROUPS.items():
        ids = [c["case_id"] for c in matrix if c["group"] == group]
        kind = "needs_clickhouse" if "server" in reason else "needs_infra"
        declare_unexecutable(ids, kind=kind, reason=reason)
    for case_id, (kind, reason) in _ENVIRONMENT_CASES.items():
        declare_unexecutable([case_id], kind=kind, reason=reason)


class TestMatrixIntegrity:
    def test_the_matrix_has_420_cases_in_30_groups(self, matrix):
        assert len(matrix) == 420
        assert len({c["group"] for c in matrix}) == 30
        by_group: dict[str, int] = {}
        for case in matrix:
            by_group[case["group"]] = by_group.get(case["group"], 0) + 1
        assert set(by_group.values()) == {14}, "30 groups of 14"

    def test_the_priority_split_matches_the_design(self, matrix):
        counts: dict[str, int] = {}
        for case in matrix:
            counts[case["priority"]] = counts.get(case["priority"], 0) + 1
        assert counts == {"P0": 150, "P1": 210, "P2": 60}

    def test_every_case_id_is_unique_and_well_formed(self, matrix):
        import re

        ids = [c["case_id"] for c in matrix]
        assert len(set(ids)) == 420
        assert all(re.fullmatch(r"P06-[A-Z]{1,2}-\d{2}", i) for i in ids)

    def test_every_case_carries_a_stimulus_a_behaviour_and_an_oracle(self, matrix):
        for case in matrix:
            assert case["stimulus"], case["case_id"]
            assert case["required_behaviour"], case["case_id"]
            assert case["oracle"], case["case_id"]

    def test_there_are_32_e2e_entries_referencing_real_cases(self, matrix):
        e2e = load_e2e()
        assert len(e2e) == 32
        known = {c["case_id"] for c in matrix}
        for entry in e2e:
            assert entry["related_cases"], entry["e2e_id"]
            unknown = set(entry["related_cases"]) - known
            assert not unknown, f"{entry['e2e_id']} references unknown {unknown}"


class TestRegistryHygiene:
    def test_every_claimed_case_exists_in_the_matrix(self, matrix):
        known = {c["case_id"] for c in matrix}
        claimed = set(REGISTRY)
        unknown = claimed - known
        assert not unknown, (
            f"tests claim case ids that are not in the matrix: {sorted(unknown)}. "
            f"A typo here would inflate the coverage number."
        )

    def test_every_registered_kind_is_valid(self):
        for case_id, by_kind in REGISTRY.items():
            for kind in by_kind:
                assert kind in KINDS, f"{case_id} has unknown kind {kind}"

    def test_the_suite_executes_a_meaningful_share_of_the_matrix(self):
        executed = covered_ids("automated", "model_only")
        assert len(executed) >= 150, (
            f"only {len(executed)} cases are executed here; the correctness-first "
            f"groups (A-K, O, R, AA, AD) should be well covered without a server"
        )

    def test_the_correctness_groups_are_substantially_executed(self, matrix):
        """Groups that need no server should be mostly executed here."""
        executed = covered_ids("automated", "model_only")
        shortfalls = {}
        for group in ("C", "D", "E", "F", "G", "H", "I"):
            ids = {c["case_id"] for c in matrix if c["group"] == group}
            covered = ids & executed
            if len(covered) < len(ids) * 0.5:
                shortfalls[group] = f"{len(covered)}/{len(ids)}"
        assert not shortfalls, f"under-covered correctness groups: {shortfalls}"

    def test_no_case_is_left_entirely_unaccounted_for(self, matrix):
        """Every case is either executed or declared with a reason."""
        accounted = covered_ids()
        missing = sorted({c["case_id"] for c in matrix} - accounted)
        assert not missing, (
            f"{len(missing)} cases are neither executed nor declared "
            f"unexecutable: {missing[:20]}"
        )


class TestCoverageReport:
    def test_the_report_is_written_from_the_registry(self, matrix):
        """Write the report and assert it describes the registry, not a wish."""
        executed = covered_ids("automated", "model_only")
        by_kind = summary()

        groups: dict[str, dict[str, int]] = {}
        for case in matrix:
            entry = groups.setdefault(
                case["group"], {"total": 0, "executed": 0, "declared": 0}
            )
            entry["total"] += 1
            if case["case_id"] in executed:
                entry["executed"] += 1
            elif case["case_id"] in covered_ids("needs_clickhouse", "needs_infra"):
                entry["declared"] += 1

        unaccounted = sorted({c["case_id"] for c in matrix} - covered_ids())
        report = {
            "matrix": {
                "cases": len(matrix),
                "groups": len({c["group"] for c in matrix}),
                "e2e": len(load_e2e()),
            },
            "executed_here": len(executed),
            "declared_unexecutable": len(covered_ids("needs_clickhouse", "needs_infra")),
            "unaccounted": unaccounted,
            "counts_by_kind": by_kind["counts"],
            "by_group": dict(sorted(groups.items())),
            "notes": [
                "executed_here counts cases a test in this repo exercises.",
                "model_only results are evidence about this pipeline's logic, never "
                "about ClickHouse: section 4 and section 22.",
                "declared_unexecutable cases need a real ClickHouse, Kafka/P05, a "
                "cluster or hardware fault injection; they are NOT_RUN, which is "
                "not the same as passing (section 14.2).",
            ],
            "verdict": str(Verdict.INCONCLUSIVE),
            "verdict_reason": (
                "Full-matrix execution needs the integration and capacity profiles "
                "of section 2.2. This host has no ClickHouse and no container "
                "runtime, so ClickHouse-semantics and scale cases are NOT_RUN."
            ),
        }
        REPORT.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        written = json.loads(REPORT.read_text(encoding="utf-8"))
        assert written["matrix"]["cases"] == 420
        assert written["executed_here"] == len(executed)
        assert written["unaccounted"] == []
        assert sum(g["total"] for g in written["by_group"].values()) == 420
        for group in written["by_group"].values():
            assert group["executed"] + group["declared"] <= group["total"]

    def test_the_report_does_not_claim_a_full_pass(self):
        written = json.loads(REPORT.read_text(encoding="utf-8"))
        assert written["verdict"] == "INCONCLUSIVE", (
            "the matrix is not fully executed on this host, and the report must "
            "say so rather than implying a clean sweep"
        )
        assert written["executed_here"] < written["matrix"]["cases"]
