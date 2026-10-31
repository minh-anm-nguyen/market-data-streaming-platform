"""Shared fixtures, and the plugin that ties tests to what-if case ids.

Every test that provides evidence for a documented scenario marks itself::

    @pytest.mark.cases("C01", "C02")

The plugin below collects those marks and writes ``reports/what-if-cases.json``
with a verdict per case. That file is what turns section 15's report from a
hand-maintained table into something derived from runs that actually happened.

A case is ``PASS`` only if every test claiming it passed. A failure or an error
makes it ``FAIL``; a skip makes it ``SKIPPED_WITH_REASON``; a case no test
claims stays ``NOT_RUN``. ``NOT_RUN`` is never folded into ``PASS``
(section 11.2) -- which is why :mod:`tests.test_matrix_coverage` can report
honestly on how much of the matrix the suite reaches.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PROJECT_ROOT.parent
P01_RUNS = REPO_ROOT / "01-market-data-simulator" / "runs"

#: A clean Project 01 dataset and a fault-injected one, both committed.
P01_CLEAN_RUN = P01_RUNS / "smoke-rec-1"
P01_FAULT_RUN = P01_RUNS / "p02-business-faults"


# ---------------------------------------------------------------------------
# case-id plugin
# ---------------------------------------------------------------------------

_CASE_OUTCOMES: dict[str, list[tuple[str, str]]] = defaultdict(list)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "cases(*ids): what-if matrix case ids this test provides evidence for"
    )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Iterator[None]:
    outcome = yield
    report = outcome.get_result()
    if report.when != "call" and not (report.when == "setup" and report.skipped):
        return
    marker = item.get_closest_marker("cases")
    if marker is None:
        return
    if report.passed:
        verdict = "PASS"
    elif report.skipped:
        verdict = "SKIPPED_WITH_REASON"
    else:
        verdict = "FAIL"
    for case in marker.args:
        _CASE_OUTCOMES[str(case)].append((verdict, item.nodeid))


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if not _CASE_OUTCOMES:
        return
    results: dict[str, dict[str, Any]] = {}
    for case, outcomes in _CASE_OUTCOMES.items():
        verdicts = {verdict for verdict, _ in outcomes}
        # A single failure decides the case: a scenario that failed once is not
        # covered just because another test of it passed.
        if "FAIL" in verdicts:
            verdict = "FAIL"
        elif verdicts == {"SKIPPED_WITH_REASON"}:
            verdict = "SKIPPED_WITH_REASON"
        else:
            verdict = "PASS"
        results[case] = {
            "verdict": verdict,
            "tests": [node for _, node in outcomes],
        }
    out = PROJECT_ROOT / "reports" / "what-if-cases.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps({"cases": dict(sorted(results.items()))}, indent=2) + "\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def project_root() -> Path:
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def clean_run_dir() -> Path:
    if not (P01_CLEAN_RUN / "events.jsonl").exists():
        pytest.skip(f"no Project 01 clean dataset at {P01_CLEAN_RUN}")
    return P01_CLEAN_RUN


@pytest.fixture(scope="session")
def fault_run_dir() -> Path:
    if not (P01_FAULT_RUN / "fault-manifest.jsonl").exists():
        pytest.skip(f"no Project 01 fault dataset at {P01_FAULT_RUN}")
    return P01_FAULT_RUN


@pytest.fixture(scope="session")
def clean_jsonl(clean_run_dir: Path) -> Path:
    return clean_run_dir / "events.jsonl"


@pytest.fixture(scope="session")
def fault_jsonl(fault_run_dir: Path) -> Path:
    return fault_run_dir / "events.jsonl"


@pytest.fixture(scope="session")
def small_jsonl(clean_jsonl: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The first 300 lines of a clean dataset, for tests that do not need 5000."""
    out = tmp_path_factory.mktemp("small") / "events.jsonl"
    with clean_jsonl.open("rb") as src, out.open("wb") as dst:
        for index, line in enumerate(src):
            if index >= 300:
                break
            dst.write(line)
    return out


@pytest.fixture(scope="session")
def cpp_tools():
    from md_contract.conformance import CppTools

    tools = CppTools()
    if not tools.available:
        pytest.skip(
            "C++ tools not built; run: cmake -S cpp -B build/cpp -G Ninja && "
            "cmake --build build/cpp"
        )
    return tools


@pytest.fixture(scope="session")
def descriptors(project_root: Path) -> dict[str, Path]:
    """Descriptor sets for the baseline and every schema variant.

    Generated here if missing, so the schema-gate tests do not depend on a
    separate build step having been run first.
    """
    out: dict[str, Path] = {}
    baseline = project_root / "toolchain" / "descriptor-baselines" / "marketdata-v1.desc"
    if not baseline.exists():
        pytest.skip("run scripts/generate_proto.sh to produce the descriptor baseline")
    out["baseline"] = baseline

    build = project_root / "build" / "descriptors"
    for name in ("v1", "v1_1", "v2_probe"):
        path = build / f"marketdata-{name}.desc"
        if path.exists():
            out[name] = path

    protoc = shutil.which("protoc") or r"C:\msys64\ucrt64\bin\protoc.exe"
    variants_dir = build / "variants"
    variants_dir.mkdir(parents=True, exist_ok=True)
    evolution = project_root / "schemas" / "evolution"
    for variant in (
        "breaking_tag_reuse",
        "breaking_type_change",
        "breaking_unsigned_seq",
        "breaking_removed_field",
    ):
        target = variants_dir / f"{variant}.desc"
        if not target.exists():
            if not Path(protoc).exists():
                continue
            subprocess.run(
                [
                    protoc,
                    "-I",
                    str(evolution / variant),
                    f"--descriptor_set_out={target}",
                    "marketdata/v1/market_data.proto",
                ],
                check=True,
                capture_output=True,
            )
        out[variant] = target
    return out


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    return tmp_path


def quote_line(**overrides: Any) -> bytes:
    """A valid sim.v1 quote line, with fields overridden verbatim.

    Values are substituted as raw JSON text so a test can inject things a typed
    builder could not express -- ``1.0``, ``true``, ``null``, a 24-digit integer.
    """
    fields: dict[str, str] = {
        "ask_ticks": "108348",
        "bid_ticks": "108345",
        "dataset_id": '"ds-test"',
        "event_time_ns": "1793322000000000000",
        "event_type": '"quote"',
        "price_scale": "100000",
        "schema_version": '"sim.v1"',
        "sequence": "1",
        "symbol": '"EURUSD"',
    }
    for key, value in overrides.items():
        if value is _REMOVE:
            fields.pop(key, None)
        else:
            fields[key] = value
    body = ",".join(f'"{k}":{v}' for k, v in sorted(fields.items()))
    return ("{" + body + "}").encode("utf-8")


def tick_line(**overrides: Any) -> bytes:
    fields: dict[str, str] = {
        "dataset_id": '"ds-test"',
        "event_time_ns": "1793322000000000000",
        "event_type": '"tick"',
        "price_scale": "100000",
        "price_ticks": "108346",
        "schema_version": '"sim.v1"',
        "sequence": "1",
        "symbol": '"EURUSD"',
    }
    for key, value in overrides.items():
        if value is _REMOVE:
            fields.pop(key, None)
        else:
            fields[key] = value
    body = ",".join(f'"{k}":{v}' for k, v in sorted(fields.items()))
    return ("{" + body + "}").encode("utf-8")


class _Remove:
    def __repr__(self) -> str:
        return "<remove>"


#: Sentinel for :func:`quote_line` / :func:`tick_line`: drop the key entirely.
_REMOVE = _Remove()
REMOVE = _REMOVE
