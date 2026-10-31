"""Aggregate-state algebra and the storage interface P06 reads and writes.

Section 4.2/4.3. The aggregate states are modelled explicitly because the one
thing the design keeps returning to is that ``AggregatingMergeTree`` holds
*partial* states: one INSERT block produces one state row per bar, background
merges combine them whenever they get round to it, and a reader that forgets
``*Merge`` sees several rows for one minute (case I-01, E2E-20).

The states form monoids -- associative, with an identity -- which is exactly why
a correct reader does not depend on merge timing. The tests exercise that
property directly rather than assuming it.

Deliberately *not* modelled: any form of automatic de-duplication. Inserting the
same canonical row twice doubles ``count_state``, and no merge will ever undo it
(section 4.2, case I-07). The builder's job is to make that impossible; the
store's job is to not paper over it.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date
from typing import NamedTuple, Protocol, runtime_checkable

from ..contracts import SourceOrder
from ..errors import ContractError
from ..oracle import Bar, BarKey
from ..rows import CanonicalTick


class ArgMinState(NamedTuple):
    """``AggregateFunction(argMin, Int64, Tuple(Int64, Int64))``."""

    order: SourceOrder
    value: int

    def merge(self, other: "ArgMinState") -> "ArgMinState":
        if self.order == other.order and self.value != other.value:
            raise ContractError(
                f"argMin state collision at order {tuple(self.order)}: "
                f"{self.value} vs {other.value}",
                code="ARGMIN_STATE_COLLISION",
            )
        return self if self.order <= other.order else other


class ArgMaxState(NamedTuple):
    """``AggregateFunction(argMax, Int64, Tuple(Int64, Int64))``."""

    order: SourceOrder
    value: int

    def merge(self, other: "ArgMaxState") -> "ArgMaxState":
        if self.order == other.order and self.value != other.value:
            raise ContractError(
                f"argMax state collision at order {tuple(self.order)}: "
                f"{self.value} vs {other.value}",
                code="ARGMAX_STATE_COLLISION",
            )
        return self if self.order >= other.order else other


class BarState(NamedTuple):
    """The five aggregate states of one bar, partial or merged."""

    open_state: ArgMinState
    high_state: int
    low_state: int
    close_state: ArgMaxState
    count_state: int

    def merge(self, other: "BarState") -> "BarState":
        return BarState(
            open_state=self.open_state.merge(other.open_state),
            high_state=max(self.high_state, other.high_state),
            low_state=min(self.low_state, other.low_state),
            close_state=self.close_state.merge(other.close_state),
            count_state=self.count_state + other.count_state,
        )

    def finalize(self) -> Bar:
        """``argMinMerge``/``maxMerge``/``minMerge``/``argMaxMerge``/``countMerge``."""
        return Bar(
            open_ticks=self.open_state.value,
            high_ticks=self.high_state,
            low_ticks=self.low_state,
            close_ticks=self.close_state.value,
            tick_count=self.count_state,
        )

    @classmethod
    def from_tick(cls, order: SourceOrder, price: int) -> "BarState":
        return cls(
            open_state=ArgMinState(order, price),
            high_state=price,
            low_state=price,
            close_state=ArgMaxState(order, price),
            count_state=1,
        )


class BarStateRow(NamedTuple):
    """One physical row of ``md_analytics.bars_1m_state``."""

    key: BarKey
    state: BarState


def merge_state_rows(rows: Iterable[BarStateRow]) -> dict[BarKey, BarState]:
    """Group partial state rows by bar key and merge them.

    This is the model of what ``GROUP BY ... argMinMerge(...)`` does at read
    time, and it is also what a background merge does at write time. Using the
    same function for both is the point: if the two differed, "same answer before
    and after merges" would be an accident rather than a property.
    """
    merged: dict[BarKey, BarState] = {}
    for row in rows:
        current = merged.get(row.key)
        merged[row.key] = row.state if current is None else current.merge(row.state)
    return merged


def states_to_bars(states: dict[BarKey, BarState]) -> dict[BarKey, Bar]:
    return {key: state.finalize() for key, state in states.items()}


def bar_states_from_block(ticks: Sequence[CanonicalTick]) -> list[BarStateRow]:
    """The materialized view's SELECT, applied to one INSERT block.

    One block yields at most one state row per bar key -- this is the ``GROUP BY``
    in the MV definition -- and a second block covering the same minute yields
    another row rather than updating the first. That is the whole reason readers
    must use ``*Merge`` (section 4.3).
    """
    from ..timebars import bar_start_ns  # local import keeps the module import-cycle free

    grouped: dict[BarKey, BarState] = {}
    for tick in ticks:
        key = BarKey(
            build_id=tick.build_id,
            dataset_id=tick.dataset_id,
            symbol=tick.symbol,
            price_scale=tick.price_scale,
            bar_start_ns=bar_start_ns(tick.event_time_ns),
        )
        state = BarState.from_tick(SourceOrder(tick.event_time_ns, tick.sequence), tick.price_ticks)
        current = grouped.get(key)
        grouped[key] = state if current is None else current.merge(state)
    return [BarStateRow(key, state) for key, state in grouped.items()]


class PartInfo(NamedTuple):
    """A row of ``system.parts`` reduced to what the budget checks need."""

    database: str
    table: str
    partition: str
    active_parts: int
    rows: int


class StoreInfo(NamedTuple):
    """Provenance of whatever produced a result.

    ``evidence_class`` travels with every measurement so a model run cannot later
    be quoted as a server run (section 4 note, section 22).
    """

    backend: str
    evidence_class: str
    server_version: str | None
    details: dict[str, str]


@runtime_checkable
class AnalyticsStore(Protocol):
    """What the builder, verifier and query service need from storage.

    Narrow on purpose: the pipeline must not be able to reach for ``ALTER``,
    ``OPTIMIZE FINAL`` in a hot path or a raw-state SELECT, because those are the
    exact moves sections 7.1 and 13 forbid.
    """

    def info(self) -> StoreInfo: ...

    def setup(self) -> None:
        """Create the database, canonical table, state table and MV."""

    def insert_canonical(self, ticks: Sequence[CanonicalTick], *, block_size: int) -> int:
        """Append canonical rows in blocks; the MV fires per block."""

    def select_canonical(
        self,
        *,
        build_id: str,
        dataset_id: str,
        symbol: str,
        start_ns: int,
        end_ns: int,
        limit: int,
    ) -> list[CanonicalTick]: ...

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
        """Finalized bars via ``*Merge``, correct whether or not merges have run."""

    def all_canonical(self, *, build_id: str) -> list[CanonicalTick]:
        """Every canonical row of a build. Audit path, not a serving path."""

    def canonical_identity_counts(self, *, build_id: str) -> dict[tuple[str, str, int], int]:
        """Rows per source identity. INV-04 requires every value to be 1."""

    def bar_state_row_count(self, *, build_id: str) -> int:
        """Physical state rows, pre-merge. Evidence that ``*Merge`` is load-bearing."""

    def all_bars_1m(self, *, build_id: str) -> dict[BarKey, Bar]:
        """Every finalized bar of a build, for the independent comparison."""

    def drop_build(self, *, build_id: str) -> None:
        """Remove a staging build completely, canonical rows and states alike."""

    def optimize_bars(self) -> None:
        """Force a merge of the state table. Lab/test use only (ADR-010)."""

    def stop_merges(self) -> None: ...

    def start_merges(self) -> None: ...

    def parts(self) -> list[PartInfo]: ...

    def apply_ttl(self, *, table: str, cutoff: date) -> int:
        """Run the TTL delete that a background merge would eventually run."""
