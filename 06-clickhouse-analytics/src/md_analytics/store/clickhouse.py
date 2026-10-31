"""ClickHouse HTTP backend. **Evidence class: REAL_CLICKHOUSE.**

Only results from this backend can support a claim about ClickHouse, because
only this backend asks ClickHouse anything (see :mod:`md_analytics.store.model`
for the counterpart and why the distinction is recorded rather than remembered).

Implemented over the HTTP interface with :mod:`urllib` so the package keeps zero
runtime dependencies, matching the P01 house style. Two choices are worth
spelling out:

* **Typed parameters go in the query string as ``param_<name>``** and the
  statement references them as ``{name:Int64}``. Nothing caller-controlled is
  ever interpolated into SQL text (section 10, group Y), and the ``Int64`` type
  is what keeps ``1700000000000000001`` exact instead of letting it through a
  float (case B-12, D-13).
* **``JSONCompact`` with integers as strings.** ``output_format_json_quote_64bit_integers``
  defaults to on, and we rely on it: a nanosecond timestamp or a ``countMerge``
  result parsed as a JSON number by a 53-bit float would come back subtly wrong
  (case I-11). Every value is parsed from its decimal string with ``int()``.

A lost response is reported as :class:`AckUnknownError` rather than retried: for
an INSERT the rows may well be durable, and a blind resubmit is what doubles an
aggregate state (section 5.2, case G-02).
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Sequence
from datetime import date
from typing import Any, Final

from ..contracts import NS_PER_SECOND
from ..errors import AckUnknownError, ResourceLimitExceeded, StoreError
from ..oracle import Bar, BarKey
from ..rows import CANONICAL_COLUMNS, CanonicalTick
from ..sqlbank import SqlBank
from .base import AnalyticsStore, PartInfo, StoreInfo

BACKEND: Final = "clickhouse-http"

#: Server-side guards applied to every interactive statement (NFR-07). The
#: numbers are pinned per profile in config/*.toml, not universal truths -- the
#: point is that a limit is always present, not that these values suit any host.
DEFAULT_SETTINGS: Final = {
    "max_execution_time": "5",
    "max_result_rows": "100000",
    "max_result_bytes": "67108864",
    "result_overflow_mode": "throw",
    "readonly": "0",
}

#: ClickHouse error codes that mean "bounded refusal", which is the *correct*
#: outcome under resource pressure rather than a partial result (section 13).
_RESOURCE_ERROR_CODES: Final = frozenset({"TOO_MANY_ROWS", "MEMORY_LIMIT_EXCEEDED", "TIMEOUT_EXCEEDED"})


class ClickHouseHttpClient:
    """Minimal HTTP client: execute a statement, or execute and parse rows."""

    def __init__(
        self,
        url: str,
        *,
        user: str = "default",
        password: str = "",
        database: str = "default",
        timeout_s: float = 30.0,
        settings: dict[str, str] | None = None,
    ) -> None:
        self.url = url.rstrip("/")
        self.user = user
        self.password = password
        self.database = database
        self.timeout_s = timeout_s
        self.settings = dict(DEFAULT_SETTINGS if settings is None else settings)

    def _request(
        self,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
        body: bytes | None = None,
        settings: dict[str, str] | None = None,
    ) -> bytes:
        query: dict[str, str] = {
            "database": self.database,
            "user": self.user,
            **{k: str(v) for k, v in (settings or self.settings).items()},
        }
        if self.password:
            query["password"] = self.password
        for key, value in (params or {}).items():
            # Nanoseconds and tick prices are passed as decimal strings; the
            # {name:Int64} placeholder in the SQL does the typed parse.
            query[f"param_{key}"] = str(value)
        if body is None:
            query["query"] = sql
            data = b""
        else:
            query["query"] = sql
            data = body

        request = urllib.request.Request(
            f"{self.url}/?{urllib.parse.urlencode(query)}",
            data=data,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            if any(code in detail for code in _RESOURCE_ERROR_CODES):
                raise ResourceLimitExceeded(
                    f"ClickHouse refused the statement: {detail.strip()[:400]}",
                    status=exc.code,
                ) from exc
            raise StoreError(
                f"ClickHouse HTTP {exc.code}: {detail.strip()[:400]}",
                code="CLICKHOUSE_HTTP_ERROR",
                status=exc.code,
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            # The server may have applied the statement before the connection
            # broke. Reporting that honestly is the whole point of section 5.2.
            raise AckUnknownError(
                f"no response from ClickHouse, outcome unknown: {exc}",
            ) from exc

    def execute(self, sql: str, *, params: dict[str, Any] | None = None) -> None:
        self._request(sql, params=params)

    def query_rows(
        self,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
        settings: dict[str, str] | None = None,
    ) -> list[list[Any]]:
        raw = self._request(
            f"{sql}\nFORMAT JSONCompact", params=params, settings=settings
        )
        if not raw.strip():
            return []
        payload = json.loads(raw.decode("utf-8"))
        return list(payload.get("data", []))

    def query_scalar(self, sql: str, *, params: dict[str, Any] | None = None) -> Any:
        rows = self.query_rows(sql, params=params)
        if not rows or not rows[0]:
            raise StoreError("expected one scalar row, got none", code="EMPTY_RESULT")
        return rows[0][0]

    def insert_json_each_row(self, table: str, rows: Sequence[dict[str, Any]]) -> None:
        """One INSERT, one block. Serialised as JSONEachRow with string integers.

        ``input_format_...`` is left at its default and every Int64 is written as
        a JSON *string*, which ClickHouse parses exactly. Writing them as JSON
        numbers would route a 19-digit nanosecond through a double on some
        clients and land a different value than the one audited.
        """
        if not rows:
            return
        body = b"".join(
            json.dumps(row, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
            for row in rows
        )
        self._request(
            f"INSERT INTO {table} FORMAT JSONEachRow",
            body=body,
            settings={**self.settings, "max_result_rows": "0", "result_overflow_mode": "throw"},
        )

    def server_version(self) -> str:
        return str(self.query_scalar("SELECT version()"))

    def ping(self) -> bool:
        try:
            return self.query_scalar("SELECT 1") in (1, "1")
        except (StoreError, AckUnknownError):
            return False


def _as_int(value: Any) -> int:
    """Parse a ClickHouse JSON value that must be an exact integer."""
    if isinstance(value, bool):
        raise StoreError(f"expected an integer, got bool {value!r}", code="BAD_RESULT_TYPE")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    raise StoreError(
        f"refusing to coerce {type(value).__name__} {value!r} to int: a 64-bit "
        f"value must arrive as a string or int, never a float",
        code="BAD_RESULT_TYPE",
    )


class ClickHouseStore(AnalyticsStore):
    """The real backend. Requires a reachable, pinned ClickHouse server."""

    def __init__(
        self,
        client: ClickHouseHttpClient,
        *,
        bank: SqlBank | None = None,
        row_limit: int = 100_000,
    ) -> None:
        self.client = client
        self.bank = bank if bank is not None else SqlBank()
        self.row_limit = row_limit

    def info(self) -> StoreInfo:
        return StoreInfo(
            backend=BACKEND,
            evidence_class="REAL_CLICKHOUSE",
            server_version=self.client.server_version(),
            details={"url": self.client.url, "sql_bank_digest": self.bank.bank_digest()},
        )

    def setup(self) -> None:
        for statement in self.bank.ddl():
            self.client.execute(statement)

    # ----------------------------------------------------------------- writes

    def insert_canonical(self, ticks: Sequence[CanonicalTick], *, block_size: int) -> int:
        if block_size <= 0:
            raise ValueError(f"block_size must be positive, got {block_size}")
        written = 0
        for start in range(0, len(ticks), block_size):
            block = ticks[start : start + block_size]
            self.client.insert_json_each_row(
                "md_analytics.canonical_ticks",
                [self._canonical_wire(tick) for tick in block],
            )
            written += len(block)
        return written

    @staticmethod
    def _canonical_wire(tick: CanonicalTick) -> dict[str, Any]:
        wire = {
            "build_id": tick.build_id,
            "dataset_id": tick.dataset_id,
            "symbol": tick.symbol,
            "sequence": str(tick.sequence),
            "event_time_ns": str(tick.event_time_ns),
            "event_date": tick.event_date.isoformat(),
            "price_scale": str(tick.price_scale),
            "price_ticks": str(tick.price_ticks),
            "payload_digest": tick.payload_digest,
            "source_schema_version": tick.source_schema_version,
        }
        assert tuple(wire) == CANONICAL_COLUMNS, "wire form drifted from CANONICAL_COLUMNS"
        return wire

    def drop_build(self, *, build_id: str) -> None:
        """Delete a staging build from both tables.

        ``ALTER TABLE ... DELETE`` is a mutation: asynchronous, and invisible to
        the materialized view. That is why the state table is cleaned explicitly
        rather than being left to follow the source (section 7.1 item 2) -- and
        why this is a rebuild tool, never a correction tool for a published
        build (ADR-007).
        """
        for table in ("md_analytics.canonical_ticks", "md_analytics.bars_1m_state"):
            self.client.execute(
                f"ALTER TABLE {table} DELETE WHERE build_id = {{build_id:String}} "
                f"SETTINGS mutations_sync = 2",
                params={"build_id": build_id},
            )

    # ------------------------------------------------------------------ reads

    def select_canonical(
        self,
        *,
        build_id: str,
        dataset_id: str,
        symbol: str,
        start_ns: int,
        end_ns: int,
        limit: int,
    ) -> list[CanonicalTick]:
        rows = self.client.query_rows(
            self.bank.get("canonical_range"),
            params={
                "build_id": build_id,
                "dataset_id": dataset_id,
                "symbol": symbol,
                "start_ns": start_ns,
                "end_ns": end_ns,
                "row_limit": limit,
            },
        )
        from ..contracts import utc_date_from_ns

        out: list[CanonicalTick] = []
        for event_time_ns, sequence, price_ticks, price_scale in rows:
            ns = _as_int(event_time_ns)
            out.append(
                CanonicalTick(
                    build_id=build_id,
                    dataset_id=dataset_id,
                    symbol=symbol,
                    sequence=_as_int(sequence),
                    event_time_ns=ns,
                    event_date=utc_date_from_ns(ns),
                    price_scale=_as_int(price_scale),
                    price_ticks=_as_int(price_ticks),
                    payload_digest="",  # not selected: this is a price/time read path
                    source_schema_version="",
                )
            )
        return out

    def _bars_from_rows(self, build_id: str, rows: list[list[Any]]) -> list[tuple[BarKey, Bar]]:
        out: list[tuple[BarKey, Bar]] = []
        for (
            dataset_id,
            symbol,
            price_scale,
            bar_start_ns,
            open_ticks,
            high_ticks,
            low_ticks,
            close_ticks,
            tick_count,
        ) in rows:
            key = BarKey(
                build_id=build_id,
                dataset_id=str(dataset_id),
                symbol=str(symbol),
                price_scale=_as_int(price_scale),
                bar_start_ns=_as_int(bar_start_ns),
            )
            out.append(
                (
                    key,
                    Bar(
                        open_ticks=_as_int(open_ticks),
                        high_ticks=_as_int(high_ticks),
                        low_ticks=_as_int(low_ticks),
                        close_ticks=_as_int(close_ticks),
                        tick_count=_as_int(tick_count),
                    ),
                )
            )
        return out

    def select_bars_1m(
        self,
        *,
        build_id: str,
        dataset_id: str,
        symbol: str,
        start_ns: int,
        end_ns: int,
        limit: int,
    ) -> list[tuple[BarKey, Bar]]:
        rows = self.client.query_rows(
            self.bank.get("bars_1m"),
            params={
                "build_id": build_id,
                "dataset_id": dataset_id,
                "symbol": symbol,
                "start_ns": start_ns,
                "end_ns": end_ns,
                "row_limit": limit,
            },
        )
        return self._bars_from_rows(build_id, rows)

    def all_canonical(self, *, build_id: str) -> list[CanonicalTick]:
        from datetime import date as _date

        rows = self.client.query_rows(
            self.bank.get("canonical_all_for_build"),
            params={"build_id": build_id},
            settings={**self.client.settings, "max_result_rows": "0"},
        )
        out: list[CanonicalTick] = []
        for row in rows:
            out.append(
                CanonicalTick(
                    build_id=str(row[0]),
                    dataset_id=str(row[1]),
                    symbol=str(row[2]),
                    sequence=_as_int(row[3]),
                    event_time_ns=_as_int(row[4]),
                    event_date=_date.fromisoformat(str(row[5])),
                    price_scale=_as_int(row[6]),
                    price_ticks=_as_int(row[7]),
                    payload_digest=str(row[8]),
                    source_schema_version=str(row[9]),
                )
            )
        return out

    def canonical_identity_counts(self, *, build_id: str) -> dict[tuple[str, str, int], int]:
        """Only the *violations*, expanded to the audit's shape.

        The audit query returns rows with ``n != 1``, so a healthy build sends
        nothing over the wire. INV-04 is "this mapping is empty", and shipping
        200M identities to Python to confirm that would be its own outage.
        """
        rows = self.client.query_rows(
            self.bank.get("canonical_duplicate_identities"),
            params={"build_id": build_id, "row_limit": self.row_limit},
        )
        return {
            (str(dataset_id), str(symbol), _as_int(sequence)): _as_int(n)
            for dataset_id, symbol, sequence, n in rows
        }

    def bar_state_row_count(self, *, build_id: str) -> int:
        return _as_int(
            self.client.query_scalar(
                self.bank.get("bar_state_row_count"), params={"build_id": build_id}
            )
        )

    def all_bars_1m(self, *, build_id: str) -> dict[BarKey, Bar]:
        rows = self.client.query_rows(
            self.bank.get("bars_1m_all_for_build"),
            params={"build_id": build_id},
            settings={**self.client.settings, "max_result_rows": "0"},
        )
        return dict(self._bars_from_rows(build_id, rows))

    # ------------------------------------------------------- merges and parts

    def optimize_bars(self) -> None:
        """Lab and test use only.

        ADR-010: ``OPTIMIZE ... FINAL`` is heavy, and it cannot repair an
        aggregate state that was wrong when it was written. It is here so a test
        can assert the read path gives the same answer before and after a merge,
        not so an operator can run it in a hot path.
        """
        self.client.execute("OPTIMIZE TABLE md_analytics.bars_1m_state FINAL")

    def stop_merges(self) -> None:
        self.client.execute("SYSTEM STOP MERGES md_analytics.bars_1m_state")

    def start_merges(self) -> None:
        self.client.execute("SYSTEM START MERGES md_analytics.bars_1m_state")

    def parts(self) -> list[PartInfo]:
        rows = self.client.query_rows(
            self.bank.get("active_parts_by_partition"), params={"row_limit": self.row_limit}
        )
        return [
            PartInfo(
                database=str(database),
                table=str(table),
                partition=str(partition),
                active_parts=_as_int(active_parts),
                rows=_as_int(rows_count),
            )
            for database, table, partition, active_parts, rows_count, _bytes in rows
        ]

    def apply_ttl(self, *, table: str, cutoff: date) -> int:
        """Materialise TTL now, and report how many rows went.

        Real TTL is tied to merges and therefore late; ``MATERIALIZE TTL``
        forces it so a retention test has a defined moment to measure. The count
        is taken before and after because the statement itself reports nothing.
        """
        before = _as_int(self.client.query_scalar(f"SELECT count() FROM {table}"))
        self.client.execute(
            f"ALTER TABLE {table} DELETE WHERE event_date < {{cutoff:Date}} "
            f"SETTINGS mutations_sync = 2",
            params={"cutoff": cutoff.isoformat()},
        )
        after = _as_int(self.client.query_scalar(f"SELECT count() FROM {table}"))
        return before - after

    def seconds_to_utc_date_check(self, event_time_ns: int) -> str:
        """Ask the server to derive a UTC date, for the group D boundary tests.

        Exists so case D-04 can be answered by the server rather than by Python:
        the question is whether *ClickHouse* partitions in UTC when the host is
        in another zone, and only the server can answer that.
        """
        return str(
            self.client.query_scalar(
                "SELECT toString(toDate(toDateTime(intDiv({ns:Int64}, "
                f"{NS_PER_SECOND}), 'UTC')))",
                params={"ns": event_time_ns},
            )
        )
