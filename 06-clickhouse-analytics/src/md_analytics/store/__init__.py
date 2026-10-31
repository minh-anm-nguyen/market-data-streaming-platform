"""Storage backends and the aggregate-state algebra they share.

:mod:`md_analytics.store.model` is an in-process model of the documented
ClickHouse semantics (evidence class ``MODEL``);
:mod:`md_analytics.store.clickhouse` talks to a real server (evidence class
``REAL_CLICKHOUSE``). Both satisfy :class:`~md_analytics.store.base.AnalyticsStore`,
and :class:`~md_analytics.store.base.StoreInfo` carries the distinction into
every evidence record so the two can never be quoted interchangeably.
"""

from __future__ import annotations

from .base import (
    AnalyticsStore,
    ArgMaxState,
    ArgMinState,
    BarState,
    BarStateRow,
    PartInfo,
    StoreInfo,
    bar_states_from_block,
    merge_state_rows,
    states_to_bars,
)

__all__ = [
    "AnalyticsStore",
    "ArgMaxState",
    "ArgMinState",
    "BarState",
    "BarStateRow",
    "PartInfo",
    "StoreInfo",
    "bar_states_from_block",
    "merge_state_rows",
    "states_to_bars",
]
