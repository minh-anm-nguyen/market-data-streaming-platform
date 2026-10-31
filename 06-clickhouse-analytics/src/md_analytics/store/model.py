"""In-process model of the ClickHouse semantics this design depends on.

**Evidence class: MODEL.** This is a model of the *documented* behaviour of
``MergeTree``, ``AggregatingMergeTree`` and incremental materialized views, not
of any particular server. It exists so the correctness pipeline -- identity
reconciliation, canonical staging, the ``*Merge`` read path, publication gating,
TTL and crash handling -- can be exercised deterministically, on any machine,
with no container runtime. A result from here can support a claim about *our
pipeline*; it can never support a claim about ClickHouse. Section 4 is explicit
that SQL behaviour may change between versions, so claims of that kind need
:mod:`md_analytics.store.clickhouse` against a pinned server, and the evidence
record keeps the two apart.

What is modelled, because the design turns on it:

* **Parts.** Each INSERT block becomes a part inside its partition. Nothing
  merges until something asks, mirroring "background merges are asynchronous and
  carry no latency guarantee" (section 6).
* **MV as an insert trigger.** The view sees blocks, one at a time, and appends
  partial aggregate states. It never sees deletes, mutations or merges at the
  source (section 4.2, case K-group).
* **No de-duplication of any kind.** Inserting a canonical row twice doubles the
  count, permanently (case I-07).
* **Asynchronous TTL.** Expiry happens when a merge runs, not at the deadline
  (section 6, case O-01).

Faults are injected through explicit attributes rather than by monkey-patching,
so a chaos test can *prove* the stimulus fired -- an un-fired injector must make
the test INVALID, never PASS (section 17.4).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timezone
from typing import Final

from ..contracts import NS_PER_SECOND
from ..errors import AckUnknownError, ResourceLimitExceeded, StoreError
from ..oracle import Bar, BarKey
from ..rows import CanonicalTick
from .base import (
    AnalyticsStore,
    BarState,
    BarStateRow,
    PartInfo,
    StoreInfo,
    bar_states_from_block,
    merge_state_rows,
    states_to_bars,
)

CANONICAL_TABLE: Final = "md_analytics.canonical_ticks"
BARS_TABLE: Final = "md_analytics.bars_1m_state"

BACKEND: Final = "model"


def _partition_of_date(d: date) -> str:
    """``toYYYYMMDD(event_date)`` as a partition name."""
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


def _partition_of_bar(bar_start_ns: int) -> str:
    """``toYYYYMMDD(toDateTime(intDiv(bar_start_ns, 1e9), 'UTC'))``.

    Pinned to UTC, which is the point of case I-06: a state table partitioned in
    the server's local zone would split a UTC day across two partitions and the
    boundary audit would have nothing to compare against.
    """
    seconds = bar_start_ns // NS_PER_SECOND
    return _partition_of_date(datetime.fromtimestamp(seconds, tz=timezone.utc).date())


class _Part:
    """One immutable data part: the rows of a single INSERT block."""

    __slots__ = ("partition", "rows")

    def __init__(self, partition: str, rows: list) -> None:
        self.partition = partition
        self.rows = rows


class _Table:
    __slots__ = ("name", "parts")

    def __init__(self, name: str) -> None:
        self.name = name
        self.parts: list[_Part] = []

    def append_block(self, partition: str, rows: list) -> None:
        if rows:
            self.parts.append(_Part(partition, rows))

    def all_rows(self) -> list:
        return [row for part in self.parts for row in part.rows]

    def part_info(self) -> list[PartInfo]:
        by_partition: dict[str, list[int]] = {}
        for part in self.parts:
            counts = by_partition.setdefault(part.partition, [0, 0])
            counts[0] += 1
            counts[1] += len(part.rows)
        database, _, table = self.name.partition(".")
        return [
            PartInfo(database, table, partition, counts[0], counts[1])
            for partition, counts in sorted(by_partition.items())
        ]


class ModelStore(AnalyticsStore):
    """Deterministic model store. See the module docstring for its evidence class."""

    def __init__(self, *, max_result_rows: int = 1_000_000) -> None:
        self._canonical = _Table(CANONICAL_TABLE)
        self._bars = _Table(BARS_TABLE)
        self._merges_enabled = True
        self._mv_attached = False
        self._max_result_rows = max_result_rows

        # --- Fault injectors. Each counts down and records that it fired, so a
        # chaos test can assert the stimulus actually happened (section 17.4).
        self.fault_fail_before_insert = 0
        self.fault_ack_unknown_after_insert = 0
        self.fault_stop_after_blocks: int | None = None
        self.fault_mv_target_unavailable = False
        self.fault_disk_full = False
        self.fired: list[str] = []

    # ------------------------------------------------------------------ setup

    def info(self) -> StoreInfo:
        return StoreInfo(
            backend=BACKEND,
            evidence_class="MODEL",
            server_version=None,
            details={
                "note": "model of documented ClickHouse semantics; not a server",
                "mv_attached": str(self._mv_attached),
                "merges_enabled": str(self._merges_enabled),
            },
        )

    def setup(self) -> None:
        """Create tables and attach the MV.

        The MV is attached *before* any insert, never with ``POPULATE`` over a
        live source: case G-05 is about exactly the gap where rows written before
        the view existed are silently absent from the target.
        """
        self._mv_attached = True

    # ----------------------------------------------------------------- writes

    def insert_canonical(self, ticks: Sequence[CanonicalTick], *, block_size: int) -> int:
        if block_size <= 0:
            raise ValueError(f"block_size must be positive, got {block_size}")
        if self.fault_fail_before_insert > 0:
            self.fault_fail_before_insert -= 1
            self.fired.append("fail_before_insert")
            raise StoreError("injected failure before insert", code="INJECTED_INSERT_FAILURE")
        if self.fault_disk_full:
            self.fired.append("disk_full")
            raise StoreError("no space left on device", code="DISK_FULL")

        written = 0
        blocks = [ticks[i : i + block_size] for i in range(0, len(ticks), block_size)]
        for index, block in enumerate(blocks):
            if self.fault_stop_after_blocks is not None and index >= self.fault_stop_after_blocks:
                self.fired.append("stop_after_blocks")
                raise StoreError(
                    f"injected abort after {index} blocks",
                    code="INJECTED_PARTIAL_INSERT",
                    blocks_written=index,
                    rows_written=written,
                )
            self._write_block(list(block))
            written += len(block)

        if self.fault_ack_unknown_after_insert > 0:
            self.fault_ack_unknown_after_insert -= 1
            self.fired.append("ack_unknown_after_insert")
            # The rows are durable; only the acknowledgement was lost. This is the
            # shape of case G-02: a blind resubmit would double the MV states.
            raise AckUnknownError(
                "connection lost after INSERT; durability unknown to the client",
                rows_attempted=written,
            )
        return written

    def _write_block(self, block: list[CanonicalTick]) -> None:
        """One INSERT block: source part first, then the dependent view.

        The order matters and is not cosmetic. In ClickHouse the source insert
        completes before the view's target insert, so a failing target leaves the
        canonical rows behind while the client sees an error -- which is why case
        E2E-18 ends in a build that must not be READY rather than in a clean
        rollback.
        """
        by_partition: dict[str, list[CanonicalTick]] = {}
        for tick in block:
            by_partition.setdefault(_partition_of_date(tick.event_date), []).append(tick)
        for partition, rows in by_partition.items():
            self._canonical.append_block(partition, rows)

        if not self._mv_attached:
            return
        if self.fault_mv_target_unavailable:
            self.fired.append("mv_target_unavailable")
            raise StoreError(
                "materialized view target is read-only",
                code="MV_TARGET_UNAVAILABLE",
            )
        for state_row in bar_states_from_block(block):
            self._bars.append_block(_partition_of_bar(state_row.key.bar_start_ns), [state_row])

    def detach_mv(self) -> None:
        """Detach the materialized view, as ``DROP VIEW`` would.

        Canonical inserts keep working and silently stop producing bars, which
        is case K-02: the gap is invisible until the independent verifier
        compares the two tables.
        """
        self._mv_attached = False

    def drop_bar_state_rows(self, *, build_id: str, keep: int = 0) -> int:
        """Remove state rows for a build, keeping the first ``keep`` of them.

        Models a dependent insert that was throttled, lost, or partially written
        when the server restarted (case G-09, K-12). The canonical rows are left
        alone, so the mismatch is exactly the one an audit has to catch.
        """
        kept = 0
        removed = 0
        for part in self._bars.parts:
            survivors = []
            for row in part.rows:
                if row.key.build_id != build_id:
                    survivors.append(row)
                elif kept < keep:
                    kept += 1
                    survivors.append(row)
                else:
                    removed += 1
            part.rows = survivors
        self._bars.parts = [part for part in self._bars.parts if part.rows]
        return removed

    def drop_canonical_partition(self, *, partition: str) -> int:
        """``DROP PARTITION`` on the canonical table.

        The derived state rows are **not** touched, which is the hazard of
        section 7.1 item 2 and the point of case O-14: a fast whole-day delete
        removes the provenance while leaving bars that can no longer be audited.
        """
        removed = 0
        for part in self._canonical.parts:
            if part.partition == partition:
                removed += len(part.rows)
                part.rows = []
        self._canonical.parts = [part for part in self._canonical.parts if part.rows]
        return removed

    def drop_build(self, *, build_id: str) -> None:
        """Drop a build from both tables.

        Dropping canonical rows does **not** remove the states the MV already
        derived from them -- that is the whole hazard of section 7.1 item 2 --
        so the state rows are removed explicitly here, which is what the real
        rebuild path has to do too.
        """
        for table, matches in (
            (self._canonical, lambda row: row.build_id == build_id),
            (self._bars, lambda row: row.key.build_id == build_id),
        ):
            for part in table.parts:
                part.rows = [row for row in part.rows if not matches(row)]
            table.parts = [part for part in table.parts if part.rows]

    # ------------------------------------------------------------------ reads

    def _check_result_rows(self, n: int) -> None:
        if n > self._max_result_rows:
            raise ResourceLimitExceeded(
                f"result of {n} rows exceeds max_result_rows={self._max_result_rows}",
                rows=n,
                limit=self._max_result_rows,
            )

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
        rows = [
            row
            for row in self._canonical.all_rows()
            if row.build_id == build_id
            and row.dataset_id == dataset_id
            and row.symbol == symbol
            and start_ns <= row.event_time_ns < end_ns
        ]
        rows.sort(key=lambda row: (row.event_time_ns, row.sequence))
        self._check_result_rows(min(len(rows), limit))
        return rows[:limit]

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
        candidates = [
            row
            for row in self._bars.all_rows()
            if row.key.build_id == build_id
            and row.key.dataset_id == dataset_id
            and row.key.symbol == symbol
            and start_ns <= row.key.bar_start_ns < end_ns
        ]
        merged = states_to_bars(merge_state_rows(candidates))
        ordered = sorted(merged.items(), key=lambda item: (item[0].bar_start_ns, item[0].price_scale))
        self._check_result_rows(min(len(ordered), limit))
        return ordered[:limit]

    def all_canonical(self, *, build_id: str) -> list[CanonicalTick]:
        rows = [row for row in self._canonical.all_rows() if row.build_id == build_id]
        rows.sort(key=lambda row: (row.dataset_id, row.symbol, row.event_time_ns, row.sequence))
        return rows

    def canonical_identity_counts(self, *, build_id: str) -> dict[tuple[str, str, int], int]:
        counts: dict[tuple[str, str, int], int] = {}
        for row in self._canonical.all_rows():
            if row.build_id != build_id:
                continue
            key = (row.dataset_id, row.symbol, row.sequence)
            counts[key] = counts.get(key, 0) + 1
        return counts

    def bar_state_row_count(self, *, build_id: str) -> int:
        return sum(1 for row in self._bars.all_rows() if row.key.build_id == build_id)

    def all_bars_1m(self, *, build_id: str) -> dict[BarKey, Bar]:
        rows = [row for row in self._bars.all_rows() if row.key.build_id == build_id]
        return states_to_bars(merge_state_rows(rows))

    # ------------------------------------------------------- merges and parts

    def optimize_bars(self) -> None:
        """Collapse state parts per partition, as a background merge would.

        The merged result must be indistinguishable from the un-merged one at
        read time, because both go through :func:`merge_state_rows`. Tests assert
        that rather than trusting it (case I-09, I-12, E2E-20).
        """
        if not self._merges_enabled:
            return
        by_partition: dict[str, dict[BarKey, BarState]] = {}
        for part in self._bars.parts:
            target = by_partition.setdefault(part.partition, {})
            for row in part.rows:
                current = target.get(row.key)
                target[row.key] = row.state if current is None else current.merge(row.state)
        self._bars.parts = [
            _Part(partition, [BarStateRow(key, state) for key, state in sorted(states.items())])
            for partition, states in sorted(by_partition.items())
        ]

    def stop_merges(self) -> None:
        self._merges_enabled = False

    def start_merges(self) -> None:
        self._merges_enabled = True

    def parts(self) -> list[PartInfo]:
        return self._canonical.part_info() + self._bars.part_info()

    def apply_ttl(self, *, table: str, cutoff: date) -> int:
        """Delete rows whose event date is strictly before ``cutoff``.

        Only runs when called, which is the honest model of TTL: expiry is tied
        to merges, so data past its deadline can still be present, and data that
        arrives already older than the TTL can vanish almost immediately after
        one (section 6, case O-01, O-03).
        """
        if table != CANONICAL_TABLE:
            raise StoreError(f"no TTL modelled for {table}", code="TTL_TABLE_UNKNOWN")
        removed = 0
        for part in self._canonical.parts:
            keep = [row for row in part.rows if row.event_date >= cutoff]
            removed += len(part.rows) - len(keep)
            part.rows = keep
        self._canonical.parts = [part for part in self._canonical.parts if part.rows]
        return removed
