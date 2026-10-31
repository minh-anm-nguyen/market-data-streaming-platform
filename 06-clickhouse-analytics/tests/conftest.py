"""Shared fixtures.

The ClickHouse fixtures skip rather than fail when no server is configured, and
they say so loudly in the skip reason. That is the honest behaviour: a group B or
group I case about *ClickHouse* semantics has not been run on this machine, and
section 14.2 calls an un-run test ``NOT_RUN``, never ``PASS``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from md_analytics.build_catalog import BuildCatalog  # noqa: E402
from md_analytics.config import load_config  # noqa: E402
from md_analytics.fixtures import CorpusBuilder, golden_six_event  # noqa: E402
from md_analytics.sqlbank import SqlBank  # noqa: E402
from md_analytics.store.model import ModelStore  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def store() -> ModelStore:
    """A fresh model store with the MV attached, as `setup()` would leave it."""
    s = ModelStore()
    s.setup()
    return s


@pytest.fixture
def catalog(tmp_path: Path) -> BuildCatalog:
    return BuildCatalog(tmp_path / "catalog")


@pytest.fixture
def signed_catalog(tmp_path: Path) -> BuildCatalog:
    return BuildCatalog(tmp_path / "signed-catalog", signing_key=b"test-signing-key")


@pytest.fixture(scope="session")
def sql_bank() -> SqlBank:
    return SqlBank(REPO_ROOT / "sql")


@pytest.fixture(scope="session")
def dev_config():
    return load_config(REPO_ROOT / "config" / "dev.toml")


@pytest.fixture
def golden():
    return golden_six_event()


@pytest.fixture
def builder() -> CorpusBuilder:
    return CorpusBuilder("TEST-01")


@pytest.fixture(scope="session")
def clickhouse_url() -> str | None:
    return os.environ.get("P06_CLICKHOUSE_URL")


@pytest.fixture
def clickhouse_store(clickhouse_url: str | None):
    """A real ClickHouse-backed store, or skip with the reason recorded.

    Each test gets its own database so a failed run cannot contaminate the next
    one, and so two runs in parallel do not fight over the same tables.
    """
    if not clickhouse_url:
        pytest.skip(
            "P06_CLICKHOUSE_URL is not set: ClickHouse-semantics cases are NOT_RUN "
            "on this host, which is not the same as passing"
        )
    from md_analytics.store.clickhouse import ClickHouseHttpClient, ClickHouseStore

    client = ClickHouseHttpClient(
        clickhouse_url,
        user=os.environ.get("P06_CLICKHOUSE_USER", "default"),
        password=os.environ.get("P06_CLICKHOUSE_PASSWORD", ""),
    )
    if not client.ping():
        pytest.skip(f"ClickHouse at {clickhouse_url} did not answer SELECT 1")
    store = ClickHouseStore(client)
    store.setup()
    yield store


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "covers(*case_ids): what-if matrix cases this test exercises"
    )
