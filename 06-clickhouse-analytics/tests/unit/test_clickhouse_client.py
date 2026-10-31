"""Unit tests for the ClickHouse HTTP client's response handling.

This file exists because the client had none. Its 22 integration tests all skip
without a live server, so the one place that turns bytes on a socket into rows
P06 then audits was never exercised by the suite that runs in CI -- and it
contained a fail-open: an empty HTTP 200 body returned ``[]``.

That is not a hypothetical. P09's pipeline refused a build with "no raw rows for
dataset ds-556f96b184c834c2" while the table held 103,438 rows for exactly that
dataset, and the refusal blamed the data for what the transport had done. These
tests pin the distinction: a result that is unknown must raise, and only a
server that actually says ``"data":[]`` may read as empty.
"""

from __future__ import annotations

import io
import json
import urllib.error

import pytest

from md_analytics.errors import StoreError
from md_analytics.store import clickhouse as ch


class _Response(io.BytesIO):
    """Just enough of an ``http.client.HTTPResponse`` for ``urlopen``."""

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
        return False


@pytest.fixture
def client(monkeypatch):
    """A client whose every request returns the next queued body."""
    bodies: list[bytes] = []

    def fake_urlopen(request, timeout=None):
        if not bodies:
            raise AssertionError("the client made an unexpected request")
        return _Response(bodies.pop(0))

    monkeypatch.setattr(ch.urllib.request, "urlopen", fake_urlopen)
    c = ch.ClickHouseHttpClient("http://clickhouse.invalid:8123")
    c._test_bodies = bodies  # type: ignore[attr-defined]
    return c


def _jsoncompact(rows: list[list[object]]) -> bytes:
    return json.dumps(
        {"meta": [{"name": "n", "type": "Int64"}], "data": rows, "rows": len(rows)}
    ).encode("utf-8")


# --------------------------------------------------------------------------
# Unknown is not zero
# --------------------------------------------------------------------------

@pytest.mark.parametrize("body", [b"", b"   ", b"\n", b"\r\n\t "])
def test_an_empty_body_raises_instead_of_reading_as_no_rows(client, body):
    """The defect. A SELECT that returns nothing at all has an unknown result."""
    client._test_bodies.append(body)
    with pytest.raises(StoreError) as caught:
        client.query_rows("SELECT 1")
    assert caught.value.code == "CLICKHOUSE_EMPTY_RESPONSE"


def test_a_truncated_body_raises_rather_than_parsing_partially(client):
    client._test_bodies.append(b'{"meta":[],"data":[[1],[2')
    with pytest.raises(StoreError) as caught:
        client.query_rows("SELECT 1")
    assert caught.value.code == "CLICKHOUSE_BAD_RESPONSE"


def test_a_response_without_a_data_key_raises(client):
    """``.get("data", [])`` made an error object look like an empty table."""
    client._test_bodies.append(json.dumps({"exception": "Code: 60"}).encode())
    with pytest.raises(StoreError) as caught:
        client.query_rows("SELECT 1")
    assert caught.value.code == "CLICKHOUSE_NO_DATA_KEY"


def test_a_json_array_is_not_mistaken_for_a_result_object(client):
    client._test_bodies.append(b"[[1],[2]]")
    with pytest.raises(StoreError) as caught:
        client.query_rows("SELECT 1")
    assert caught.value.code == "CLICKHOUSE_NO_DATA_KEY"


def test_undecodable_bytes_raise(client):
    client._test_bodies.append(b"\xff\xfe\x00not utf-8")
    with pytest.raises(StoreError) as caught:
        client.query_rows("SELECT 1")
    assert caught.value.code == "CLICKHOUSE_BAD_RESPONSE"


# --------------------------------------------------------------------------
# ...but a genuinely empty result is still empty
# --------------------------------------------------------------------------

def test_a_server_that_says_data_is_empty_reads_as_no_rows(client):
    """The fix must not turn "nothing matched" into an error."""
    client._test_bodies.append(_jsoncompact([]))
    assert client.query_rows("SELECT 1 WHERE 0") == []


def test_rows_are_returned_in_order(client):
    client._test_bodies.append(_jsoncompact([[1], [2], [3]]))
    assert client.query_rows("SELECT n") == [[1], [2], [3]]


def test_query_scalar_still_refuses_an_empty_result(client):
    client._test_bodies.append(_jsoncompact([]))
    with pytest.raises(StoreError) as caught:
        client.query_scalar("SELECT version()")
    assert caught.value.code == "EMPTY_RESULT"


# --------------------------------------------------------------------------
# Transport faults keep their own verdicts
# --------------------------------------------------------------------------

def test_a_broken_connection_is_ack_unknown_not_an_empty_result(client, monkeypatch):
    """A dropped socket must not be confused with either an error or no rows."""

    def boom(request, timeout=None):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(ch.urllib.request, "urlopen", boom)
    with pytest.raises(ch.AckUnknownError):
        client.query_rows("SELECT 1")


def test_an_http_error_body_is_surfaced(client, monkeypatch):
    def boom(request, timeout=None):
        raise urllib.error.HTTPError(
            "http://clickhouse.invalid:8123/",
            500,
            "Internal Server Error",
            {},
            io.BytesIO(b"Code: 62. DB::Exception: Syntax error"),
        )

    monkeypatch.setattr(ch.urllib.request, "urlopen", boom)
    with pytest.raises(StoreError) as caught:
        client.query_rows("SELECT bad syntax")
    assert caught.value.code == "CLICKHOUSE_HTTP_ERROR"
    assert "Syntax error" in str(caught.value)
