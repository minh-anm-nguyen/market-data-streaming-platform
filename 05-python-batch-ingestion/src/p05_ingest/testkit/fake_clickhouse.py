"""A deterministic ClickHouse stand-in built around ACK ambiguity.

The whole correctness argument in spec sections 5.3 and 7 turns on one fact:
the client cannot tell "the server refused" from "the server applied the insert
and the response was lost". A fake that only models success and failure cannot
test any of that, so this one models the three-way split explicitly:

  * ``ACK``                  -- applied, client told so;
  * ``WRITE_THEN_LOSE_ACK``  -- **applied**, client told nothing. This is the
    dangerous one, and the row count afterwards proves the write happened;
  * ``REFUSE_BEFORE_WRITE``  -- not applied, client told so.

It also models the finite insert de-duplication window, because the spec is
emphatic that block de-duplication is a bounded optimisation and not an
exactly-once guarantee: a token that has fallen out of the window no longer
suppresses a duplicate, which is exactly how a late retry produces a physical
duplicate (E2E-15).

Parts accounting is a crude proxy for MergeTree behaviour -- one part per
insert per ``event_date`` -- included so the batching-versus-parts trade-off in
spec 10.3 can be observed rather than guessed. It is not a merge simulator.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum

from ..clickhouse_sink import SinkInsertResult
from ..model import CH_INSERT_COLUMNS, SealedBatch, SinkOutcome


class Behaviour(StrEnum):
    """What the server does on one insert attempt."""

    ACK = "ack"
    #: Rows land, client sees a timeout. Retry is correct and may duplicate.
    WRITE_THEN_LOSE_ACK = "write_then_lose_ack"
    #: Nothing lands, client sees a retryable refusal.
    REFUSE_BEFORE_WRITE = "refuse_before_write"
    #: Nothing lands, client sees a permanent error.
    PERMANENT_ERROR = "permanent_error"
    #: Nothing lands, client sees an unclassifiable error.
    UNKNOWN_ERROR = "unknown_error"


@dataclass(slots=True)
class StoredRow:
    """One physical row, as the table would hold it."""

    values: dict[str, object]
    #: Which insert attempt wrote it; lets a test see the duplicate pair.
    insert_seq: int

    def __getitem__(self, column: str) -> object:
        return self.values[column]


@dataclass(slots=True)
class InsertAttempt:
    """Audit record of one attempt, for assertions and for the report."""

    insert_seq: int
    token: str
    rows: int
    behaviour: Behaviour
    applied: bool
    deduplicated: bool


class FakeClickHouse:
    """The table plus a scriptable failure plan.

    ``script`` is consumed one entry per *attempt*. When it runs out the
    ``default_behaviour`` applies, so a test only has to describe the
    interesting prefix.
    """

    def __init__(
        self,
        *,
        script: Sequence[Behaviour] = (),
        default_behaviour: Behaviour = Behaviour.ACK,
        dedup_window: int = 1000,
        latency_s: float = 0.001,
        monotonic: Callable[[], float] | None = None,
        evict_dedup_after_attempt: int | None = None,
    ) -> None:
        self._script = list(script)
        self._default = default_behaviour
        self._dedup_window = dedup_window
        self._latency_s = latency_s
        self._monotonic = monotonic
        #: Clear the de-duplication window immediately after this attempt
        #: number. Needed because a retry can happen in the same instant as
        #: the attempt it follows, leaving a test no moment in between to
        #: evict the window itself. Models a long delay or a server restart
        #: landing between two attempts (spec E2E-15).
        self.evict_dedup_after_attempt = evict_dedup_after_attempt

        self.rows: list[StoredRow] = []
        self.attempts: list[InsertAttempt] = []
        #: Tokens still inside the de-duplication window, oldest first.
        self._dedup_log: list[str] = []
        self._insert_seq = 0
        self.closed = False
        self.preflight_error: str | None = None
        #: Set of (date) keys ever written, as a stand-in for active parts.
        self._parts: dict[tuple[str, object], int] = {}

    # ------------------------------------------------------------- scripting

    def set_script(self, script: Sequence[Behaviour]) -> None:
        self._script = list(script)

    def _next_behaviour(self) -> Behaviour:
        if self._script:
            return self._script.pop(0)
        return self._default

    # -------------------------------------------------------------- dedup

    def _is_duplicate_token(self, token: str) -> bool:
        return token in self._dedup_log

    def _remember_token(self, token: str) -> None:
        self._dedup_log.append(token)
        if len(self._dedup_log) > self._dedup_window:
            # Eviction is what makes a late retry able to duplicate.
            del self._dedup_log[: len(self._dedup_log) - self._dedup_window]

    def evict_dedup_log(self) -> None:
        """Drop the whole window, simulating a long gap or a server restart."""
        self._dedup_log.clear()

    # ------------------------------------------------------------- the sink

    def insert_batch(self, batch: SealedBatch) -> SinkInsertResult:
        """Implements ``ClickHouseSink.insert_batch``. Never raises."""
        if self.closed:
            return SinkInsertResult(
                SinkOutcome.TRANSIENT, 0.0, "connection refused: sink closed"
            )
        self._insert_seq += 1
        seq = self._insert_seq
        behaviour = self._next_behaviour()
        token = batch.insert_token

        deduplicated = False
        applied = False

        if behaviour in (Behaviour.ACK, Behaviour.WRITE_THEN_LOSE_ACK):
            if self._is_duplicate_token(token):
                # Block de-duplication suppressed the write. The server still
                # reports success, which is why a de-duplicated retry is
                # indistinguishable from a first write at the client.
                deduplicated = True
            else:
                self._apply(batch, seq)
                self._remember_token(token)
                applied = True

        self.attempts.append(
            InsertAttempt(
                insert_seq=seq,
                token=token,
                rows=len(batch.rows),
                behaviour=behaviour,
                applied=applied,
                deduplicated=deduplicated,
            )
        )

        if self.evict_dedup_after_attempt == seq:
            self._dedup_log.clear()

        if behaviour is Behaviour.ACK:
            return SinkInsertResult(
                SinkOutcome.ACKED, self._latency_s, "", rows=len(batch.rows)
            )
        if behaviour is Behaviour.WRITE_THEN_LOSE_ACK:
            return SinkInsertResult(
                SinkOutcome.AMBIGUOUS,
                self._latency_s,
                "read timed out after the request was sent",
            )
        if behaviour is Behaviour.REFUSE_BEFORE_WRITE:
            return SinkInsertResult(
                SinkOutcome.TRANSIENT, self._latency_s, "503 service unavailable"
            )
        if behaviour is Behaviour.PERMANENT_ERROR:
            return SinkInsertResult(
                SinkOutcome.PERMANENT,
                self._latency_s,
                "unknown identifier: price_tick",
            )
        return SinkInsertResult(
            SinkOutcome.AMBIGUOUS, self._latency_s, "unclassified driver failure"
        )

    def _apply(self, batch: SealedBatch, seq: int) -> None:
        for row in batch.rows:
            values = dict(zip(CH_INSERT_COLUMNS, row.to_ch_tuple(), strict=True))
            self.rows.append(StoredRow(values=values, insert_seq=seq))
            key = ("market_events_raw", values["event_date"])
            self._parts[key] = self._parts.get(key, 0) + 1

    def preflight(self) -> None:
        from ..errors import PreflightError

        if self.preflight_error:
            raise PreflightError(self.preflight_error)

    def close(self) -> None:
        self.closed = True

    # ----------------------------------------------------------- inspection

    @property
    def row_count(self) -> int:
        return len(self.rows)

    @property
    def active_parts(self) -> int:
        """One part per (table, event_date) touched per insert, unmerged."""
        return len(self._parts)

    def column(self, name: str) -> list[object]:
        return [row[name] for row in self.rows]

    def identities(self) -> list[tuple[str, str, int]]:
        return [
            (row["dataset_id"], row["symbol"], row["sequence"]) for row in self.rows
        ]

    def digests_by_identity(self) -> dict[tuple[str, str, int], set[str]]:
        """Grouping used by the duplicate and conflict audit queries (6.3)."""
        out: dict[tuple[str, str, int], set[str]] = {}
        for row in self.rows:
            key = (row["dataset_id"], row["symbol"], row["sequence"])
            out.setdefault(key, set()).add(row["payload_digest"])
        return out

    def physical_counts_by_identity(self) -> dict[tuple[str, str, int], int]:
        out: dict[tuple[str, str, int], int] = {}
        for identity in self.identities():
            out[identity] = out.get(identity, 0) + 1
        return out

    def rows_for_run(self, ingest_run_id: str) -> list[StoredRow]:
        return [r for r in self.rows if r["ingest_run_id"] == ingest_run_id]
