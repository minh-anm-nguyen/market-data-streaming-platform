"""The ClickHouse sink: protocol, real driver adapter and error classification.

The protocol is tiny on purpose, because the interesting behaviour is in the
*classification* of failures, and that is what the tests need to drive.

Classification rules (spec section 5.2, and section 7 for what must not be
claimed):

  * a clean driver return is ``ACKED`` -- and means acknowledged under the
    configured sync mode, nothing more. It does not prove replication to other
    replicas, nor atomic materialized-view visibility.
  * a read timeout, a connection reset or a truncated response after the
    request was sent is ``AMBIGUOUS``. The server may have applied the insert.
    Treating this as "not written" is the single most dangerous mistake
    available here, so it has its own outcome and never collapses into a
    failure.
  * a refusal the server issued *before* applying anything -- 429, 503, connect
    refused -- is ``TRANSIENT``.
  * a structural refusal -- unknown column, type mismatch, permission denied --
    is ``PERMANENT``.

Where the driver does not let us distinguish "refused before" from "maybe
applied", the ambiguous classification wins. Being pessimistic costs a possible
duplicate; being optimistic costs a lost record.
"""

from __future__ import annotations

import time
from typing import Protocol, runtime_checkable

from .config import ClickHouseConfig
from .model import CH_INSERT_COLUMNS, SealedBatch, SinkOutcome

#: Substrings that identify a definitively permanent server-side refusal.
#: Matched case-insensitively against the exception text, because the Python
#: driver surfaces server errors as messages rather than typed codes.
_PERMANENT_MARKERS = (
    "unknown identifier",
    "no such column",
    "cannot parse",
    "type mismatch",
    "illegal type",
    "not enough privileges",
    "access_denied",
    "authentication failed",
    "unknown table",
    "unknown database",
    "syntax error",
    "cannot convert",
)

#: Substrings that identify a refusal issued before any data was applied.
_TRANSIENT_MARKERS = (
    "too many simultaneous queries",
    "service unavailable",
    "503",
    "429",
    "connection refused",
    "temporary failure in name resolution",
    "cannot allocate",
    "memory limit",
    "too many parts",
    "socket timeout while connecting",
)

#: Substrings that mean the request was in flight when contact was lost.
_AMBIGUOUS_MARKERS = (
    "timed out",
    "timeout",
    "connection reset",
    "connection aborted",
    "broken pipe",
    "remote end closed",
    "incompleteread",
    "eof occurred",
    "read timed out",
)


class SinkInsertResult:
    """Outcome of a single attempt, with the latency it took."""

    __slots__ = ("outcome", "latency_s", "detail", "rows")

    def __init__(
        self, outcome: SinkOutcome, latency_s: float, detail: str = "", rows: int | None = None
    ) -> None:
        self.outcome = outcome
        self.latency_s = latency_s
        self.detail = detail
        self.rows = rows

    def __repr__(self) -> str:  # pragma: no cover - diagnostics
        return f"SinkInsertResult({self.outcome}, {self.latency_s:.3f}s, {self.detail!r})"


@runtime_checkable
class ClickHouseSink(Protocol):
    """What the sink worker needs. Implemented for real and in the testkit."""

    def insert_batch(self, batch: SealedBatch) -> SinkInsertResult:
        """Send one immutable batch and classify the outcome. Must not raise."""
        ...

    def preflight(self) -> None:
        """Verify the destination exists and matches the expected schema."""
        ...

    def close(self) -> None: ...


def classify_exception(exc: BaseException) -> tuple[SinkOutcome, str]:
    """Map a driver exception onto an outcome.

    Order matters: ambiguity is checked before the transient markers, because a
    message can contain both ("socket timeout" during a read is ambiguous even
    though "timeout" appears in connect failures too). The only timeouts that
    are safely transient are those raised while *connecting*, which the driver
    labels distinctly.
    """
    text = f"{type(exc).__name__}: {exc}".lower()

    for marker in _PERMANENT_MARKERS:
        if marker in text:
            return SinkOutcome.PERMANENT, f"permanent: {exc}"

    # A pure connect failure never applied anything.
    if "socket timeout while connecting" in text or "connection refused" in text:
        return SinkOutcome.TRANSIENT, f"transient (pre-send): {exc}"

    for marker in _AMBIGUOUS_MARKERS:
        if marker in text:
            return (
                SinkOutcome.AMBIGUOUS,
                f"ambiguous (request may have been applied): {exc}",
            )

    for marker in _TRANSIENT_MARKERS:
        if marker in text:
            return SinkOutcome.TRANSIENT, f"transient: {exc}"

    # Unrecognised failure. Pessimistic by policy: an unknown error after the
    # request was dispatched might have been applied, so it must not be
    # reported as a definitive failure.
    return SinkOutcome.AMBIGUOUS, f"unclassified, treated as ambiguous: {exc}"


class ClickHouseConnectSink:
    """Real sink over ``clickhouse-connect`` HTTP.

    One instance per sink worker thread: the driver client is not shared across
    threads here, per spec section 2.3 (each worker owns its client rather than
    sharing one object loosely).
    """

    def __init__(self, cfg: ClickHouseConfig, *, monotonic=time.monotonic) -> None:
        self._cfg = cfg
        self._monotonic = monotonic
        self._client = None

    # ------------------------------------------------------------- connection

    def _settings(self) -> dict[str, object]:
        """Query settings sent with every insert.

        Pinned explicitly rather than relying on server defaults: spec section
        6.4 warns that ``async_insert`` may default to on in newer ClickHouse,
        which would make the ACK mean "buffered", not "stored".
        """
        settings: dict[str, object] = {
            "async_insert": 1 if self._cfg.async_insert else 0,
            "insert_deduplicate": 1,
        }
        if self._cfg.async_insert:
            settings["wait_for_async_insert"] = 1 if self._cfg.wait_for_async_insert else 0
        return settings

    def _ensure_client(self):
        if self._client is not None:
            return self._client
        import clickhouse_connect  # imported lazily so tests need no driver

        user, password = self._cfg.credentials()
        self._client = clickhouse_connect.get_client(
            host=self._cfg.host,
            port=self._cfg.port,
            database=self._cfg.database,
            username=user,
            password=password,
            secure=self._cfg.secure,
            connect_timeout=self._cfg.connect_timeout_s,
            send_receive_timeout=self._cfg.insert_timeout_s,
            settings=self._settings(),
        )
        return self._client

    # -------------------------------------------------------------- preflight

    def preflight(self) -> None:
        """Check the table exists and its columns match what we will send.

        Spec FR-18: a schema mismatch must stop startup rather than be
        discovered mid-run as a permanent insert error per batch.
        """
        from .errors import PreflightError

        client = self._ensure_client()
        try:
            rows = client.query(
                "SELECT name FROM system.columns "
                "WHERE database = {db:String} AND table = {tbl:String}",
                parameters={"db": self._cfg.database, "tbl": self._cfg.table},
            ).result_rows
        except Exception as exc:
            raise PreflightError(
                f"cannot read the schema of {self._cfg.table_fqn}: {exc}"
            ) from exc

        present = {r[0] for r in rows}
        if not present:
            raise PreflightError(
                f"{self._cfg.table_fqn} does not exist or is not readable; apply "
                "sql/001_create_raw_table.sql first (this program will not "
                "create or alter tables)"
            )
        missing = [c for c in CH_INSERT_COLUMNS if c not in present]
        if missing:
            raise PreflightError(
                f"{self._cfg.table_fqn} is missing column(s): {', '.join(missing)}"
            )

        effective = self._verify_effective_settings(client)
        if effective:
            raise PreflightError(effective)

    def _verify_effective_settings(self, client) -> str | None:
        """Refuse the unsafe async combination as the *server* sees it."""
        try:
            rows = client.query(
                "SELECT name, value FROM system.settings "
                "WHERE name IN ('async_insert', 'wait_for_async_insert')"
            ).result_rows
        except Exception:
            # Not fatal: the insert settings are sent per request anyway. The
            # startup gate in config.py already rejects the unsafe pairing.
            return None
        values = {name: str(value) for name, value in rows}
        async_on = values.get("async_insert", "0") not in ("0", "false")
        wait_on = values.get("wait_for_async_insert", "1") not in ("0", "false")
        if self._cfg.async_insert and async_on and not wait_on:
            return (
                "server effective settings have async_insert=1 with "
                "wait_for_async_insert=0; an ACK would only prove buffering "
                "(spec NFR-09)"
            )
        return None

    # ----------------------------------------------------------------- insert

    def insert_batch(self, batch: SealedBatch) -> SinkInsertResult:
        """Send ``batch``. Never raises: the outcome is the return value.

        The de-duplication token is the batch token, so a retry of this exact
        batch is eligible for ClickHouse block de-duplication. That is an
        optimisation, not a guarantee: the window is finite and a restart that
        regroups rows produces a different token (spec section 6.2).
        """
        started = self._monotonic()
        try:
            client = self._ensure_client()
            settings = dict(self._settings())
            settings["insert_deduplication_token"] = batch.insert_token
            client.insert(
                table=self._cfg.table,
                database=self._cfg.database,
                data=[row.to_ch_tuple() for row in batch.rows],
                column_names=list(CH_INSERT_COLUMNS),
                settings=settings,
            )
        except BaseException as exc:  # noqa: BLE001 - classification is the point
            outcome, detail = classify_exception(exc)
            return SinkInsertResult(outcome, self._monotonic() - started, detail)
        return SinkInsertResult(
            SinkOutcome.ACKED, self._monotonic() - started, "", rows=len(batch.rows)
        )

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:  # pragma: no cover - best effort
                pass
            self._client = None
