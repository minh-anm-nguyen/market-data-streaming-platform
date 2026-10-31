"""Shared fixtures, and the hook that turns test runs into matrix coverage.

Two things worth knowing before reading the tests.

**Coverage is recorded, not claimed.** A test tags itself with the what-if cases
it exercises::

    @pytest.mark.whatif("P07-G05-03", "P07-G05-04")

:func:`pytest_sessionfinish` writes those tags, with each test's real outcome,
to ``catalog/coverage.json``. ``python -m p07 verify-release`` then joins that
against the 448-case catalog, so a case nobody tagged shows up as ``NOT_RUN``
rather than vanishing from the denominator. A failing test contributes its
failure, so coverage can never be inflated by a test that does not pass.

**Fixtures are small on purpose.** Every event costs a protobuf encode, a
decode, several SHA-256 digests and four boundary comparisons, so the default
fixture is a few hundred events rather than the 100k the design's E2E table
calls for. ``tests/perf`` carries the larger ones. Where a case genuinely needs
volume -- the OHLC and duplicate-counting cases -- the test says so and uses a
bigger fixture.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Callable

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:  # pragma: no cover - import plumbing
    sys.path.insert(0, str(ROOT))

from p07.dataplane.adapter import Adapter, limits_from_sealed  # noqa: E402
from p07.dataplane.analytics import BuildPipeline  # noqa: E402
from p07.dataplane.clickhouse_fake import FakeClickHouse  # noqa: E402
from p07.dataplane.codec import DEFAULT_CODEC  # noqa: E402
from p07.dataplane.ingestion import BatchIngestor  # noqa: E402
from p07.dataplane.kafka_fake import FakeKafka  # noqa: E402
from p07.dataplane.producer import Producer  # noqa: E402
from p07.dataset_sealer import SealedDataset, seal_dataset  # noqa: E402
from p07.evidence.artifact_store import ArtifactStore  # noqa: E402
from p07.oracle.acceptance_oracle import AcceptanceExpectation, build_expectation  # noqa: E402
from p07.orchestration.supervisor import LabConfig, ScenarioRun  # noqa: E402
from tools.source_factory import FaultOp, write_run_dir  # noqa: E402

_COVERAGE: list[dict[str, Any]] = []


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "whatif(*case_ids): what-if matrix cases this test covers")


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]):
    outcome = yield
    report = outcome.get_result()
    # A skipped test reports at setup and never reaches "call", so both phases
    # are considered: otherwise the deliberately-skipped ``needs_real_lab``
    # cases would contribute nothing and their NOT_RUN status would only be
    # inferred from absence rather than recorded.
    if report.when not in {"call", "setup"}:
        return
    if report.when == "setup" and not report.skipped:
        return
    marker = item.get_closest_marker("whatif")
    if marker is None or not marker.args:
        return
    verdict = {"passed": "PASS", "failed": "FAIL", "skipped": "NOT_RUN"}.get(
        report.outcome, "NOT_RUN"
    )
    _COVERAGE.append({"test": item.nodeid, "verdict": verdict, "cases": list(marker.args)})


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if not _COVERAGE:
        return
    out = ROOT / "catalog" / "coverage.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(_COVERAGE, indent=2, sort_keys=True) + "\n", encoding="utf-8")


# -- source fixtures ----------------------------------------------------


@pytest.fixture
def make_source(tmp_path: Path) -> Callable[..., dict[str, Any]]:
    """Factory for sealed P01-shaped run directories inside ``tmp_path``."""
    counter = {"n": 0}

    def build(**kwargs: Any) -> dict[str, Any]:
        counter["n"] += 1
        name = kwargs.pop("name", f"src{counter['n']}")
        kwargs.setdefault("count", 300)
        kwargs.setdefault("interval_ns", 100_000_000)
        return write_run_dir(tmp_path / name, **kwargs)

    return build


@pytest.fixture
def sealed(make_source: Callable[..., dict[str, Any]]) -> SealedDataset:
    """A small, clean, anchored sealed dataset."""
    info = make_source(dataset_id="t-clean")
    return seal_dataset(info["run_dir"], anchor=info["events_sha256"])


@pytest.fixture
def expectation(sealed: SealedDataset) -> AcceptanceExpectation:
    return build_expectation(sealed)


@pytest.fixture
def faulted(make_source: Callable[..., dict[str, Any]]) -> SealedDataset:
    """A sealed dataset with declared anomalies of each interesting kind."""
    info = make_source(
        name="faulted",
        dataset_id="t-faults",
        count=300,
        faults=(
            FaultOp("missing_sequence", "EURUSD", 20),
            FaultOp("duplicate_exact", "GBPUSD", 10),
            FaultOp("duplicate_conflict", "USDJPY", 8),
            FaultOp("stale_timestamp", "EURUSD", 40, params={"back_ns": 10_000_000_000}),
            FaultOp("invalid_spread", "GBPUSD", 25),
        ),
    )
    return seal_dataset(info["run_dir"], anchor=info["events_sha256"])


# -- data plane fixtures ------------------------------------------------


@pytest.fixture
def kafka() -> FakeKafka:
    """A 3-broker, RF=3, minISR=2 lab with 4 partitions per topic.

    Four rather than the design's eight: the behaviour under test is per
    partition, and halving the count halves the per-run bookkeeping in every
    test that walks all of them.
    """
    return FakeKafka(partitions=4)


@pytest.fixture
def sink() -> FakeClickHouse:
    return FakeClickHouse()


@pytest.fixture
def plane(
    kafka: FakeKafka, sink: FakeClickHouse, sealed: SealedDataset
) -> dict[str, Any]:
    """A wired adapter -> producer -> kafka -> ingestor -> sink -> builder chain."""
    producer = Producer(kafka, codec=DEFAULT_CODEC)
    adapter = Adapter(
        dataset_id=sealed.dataset_id,
        limits=limits_from_sealed(sealed.symbols),
        sink=producer.enqueue,
        codec=DEFAULT_CODEC,
    )
    ingestor = BatchIngestor(kafka, sink, codec=DEFAULT_CODEC, batch_max_records=50)
    builder = BuildPipeline(sink)
    return {
        "kafka": kafka,
        "sink": sink,
        "producer": producer,
        "adapter": adapter,
        "ingestor": ingestor,
        "builder": builder,
        "dataset": sealed,
    }


@pytest.fixture
def run_factory(tmp_path: Path) -> Callable[..., ScenarioRun]:
    """Build a :class:`ScenarioRun` writing into ``tmp_path``, without fsync.

    ``fsync=False`` because ``FlushFileBuffers`` costs over a second per call on
    some Windows volumes, which would make the suite unusable. The bundle
    records that it was not flushed, so nothing here can claim crash durability;
    ``tests/unit/test_artifact_store.py`` covers the fsync path separately.
    """

    def build(
        *,
        source: dict[str, Any],
        scenario_id: str = "test-scenario",
        run_id: str = "r1",
        test_case_id: str = "",
        lab: LabConfig | None = None,
        anchor: str | None = "auto",
    ) -> ScenarioRun:
        store = ArtifactStore(
            tmp_path / "runs", scenario_id=scenario_id, run_id=run_id, fsync=False
        )
        return ScenarioRun(
            source_run_dir=source["run_dir"],
            store=store,
            scenario_id=scenario_id,
            run_id=run_id,
            test_case_id=test_case_id,
            lab=lab or LabConfig(partitions=4),
            source_anchor=source["events_sha256"] if anchor == "auto" else anchor,
        )

    return build
