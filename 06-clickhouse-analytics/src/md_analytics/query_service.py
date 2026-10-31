"""The only read path clients get: scoped, bounded, pinned to a READY build.

Section 10 and matrix groups R/S/Y. Validation happens before the store is
touched, and every rejection is explicit:

* **A build must be READY.** ``DIRTY``, ``NOT_READY`` and revoked builds are
  errors. There is no fallback to the raw table, because a fallback silently
  turns "the verified snapshot" into "whatever happens to be in the landing
  table" (FR-09, section 10).
* **Scope is mandatory.** ``dataset_id`` and ``symbol`` are required, and symbols
  are checked against an allowlist, so a query cannot wander across tenants or
  inflate the ``LowCardinality`` dictionary with hostile values (case I-13).
* **Bar ranges must be minute-aligned.** An unaligned bound has no honest answer
  (section 4.3); the service demands alignment rather than guessing.
* **Nanoseconds cross the wire as strings.** ``1700000000000000001`` does not
  survive a JSON number in a 53-bit float client, in either direction, so
  requests accept decimal strings and responses emit them (case D-13, I-11).
* **Limits are server-side.** ``LIMIT`` caps the *result*, not the work: the
  guards that matter are ``max_execution_time`` and ``max_memory_usage`` on the
  server, which the store configures. The row cap here is the last line, not the
  first (section 10).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Final

from .build_catalog import BuildCatalog
from .errors import QueryRejected
from .oracle import Bar, BarKey
from .store.base import AnalyticsStore
from .timebars import BAR_INTERVAL_NS, validate_range

DEFAULT_MAX_BARS: Final = 10_000
DEFAULT_MAX_WINDOW_MINUTES: Final = 10_080  # 7 days
DEFAULT_MAX_CANONICAL_ROWS: Final = 100_000


@dataclass(frozen=True, slots=True)
class QueryLimits:
    """Bounds for one request class. Pinned per profile, not universal."""

    max_bars_returned: int = DEFAULT_MAX_BARS
    max_window_minutes: int = DEFAULT_MAX_WINDOW_MINUTES
    max_canonical_rows: int = DEFAULT_MAX_CANONICAL_ROWS
    require_ready_build: bool = True
    require_minute_alignment: bool = True


@dataclass(frozen=True, slots=True)
class BarRow:
    """One finalized bar in wire form.

    ``bar_start_ns`` and ``tick_count`` are strings on purpose. ``tick_count``
    is a ``UInt64`` server-side and a bar at scale can exceed what a JavaScript
    number holds exactly; emitting it as a string keeps the API honest about
    which values are integers (case I-11).
    """

    bar_start_ns: str
    bar_end_ns: str
    symbol: str
    dataset_id: str
    price_scale: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int
    tick_count: str

    @classmethod
    def from_bar(cls, key: BarKey, bar: Bar) -> "BarRow":
        return cls(
            bar_start_ns=str(key.bar_start_ns),
            bar_end_ns=str(key.bar_start_ns + BAR_INTERVAL_NS),
            symbol=key.symbol,
            dataset_id=key.dataset_id,
            price_scale=key.price_scale,
            open_ticks=bar.open_ticks,
            high_ticks=bar.high_ticks,
            low_ticks=bar.low_ticks,
            close_ticks=bar.close_ticks,
            tick_count=str(bar.tick_count),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "bar_start_ns": self.bar_start_ns,
            "bar_end_ns": self.bar_end_ns,
            "dataset_id": self.dataset_id,
            "symbol": self.symbol,
            "price_scale": self.price_scale,
            "open_ticks": self.open_ticks,
            "high_ticks": self.high_ticks,
            "low_ticks": self.low_ticks,
            "close_ticks": self.close_ticks,
            # Named tick_count, never "volume": P02 ticks carry no trade size
            # and a client must not be able to read one as the other (case H-06).
            "tick_count": self.tick_count,
        }


@dataclass(frozen=True, slots=True)
class BarResponse:
    build_id: str
    dataset_id: str
    symbol: str
    from_ns: str
    to_ns: str
    bars: tuple[BarRow, ...]
    truncated: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "build_id": self.build_id,
            "dataset_id": self.dataset_id,
            "symbol": self.symbol,
            "from_ns": self.from_ns,
            "to_ns": self.to_ns,
            "bar_interval_ns": str(BAR_INTERVAL_NS),
            "truncated": self.truncated,
            "bars": [bar.as_dict() for bar in self.bars],
        }


class QueryService:
    """Validates, scopes and bounds every client read."""

    def __init__(
        self,
        store: AnalyticsStore,
        catalog: BuildCatalog,
        *,
        limits: QueryLimits | None = None,
        symbol_allowlist: Iterable[str] | None = None,
        dataset_allowlist: Iterable[str] | None = None,
    ) -> None:
        self.store = store
        self.catalog = catalog
        self.limits = limits if limits is not None else QueryLimits()
        self.symbol_allowlist = frozenset(symbol_allowlist) if symbol_allowlist else None
        self.dataset_allowlist = frozenset(dataset_allowlist) if dataset_allowlist else None

    # ------------------------------------------------------------ validation

    def _require_scope(self, dataset_id: object, symbol: object) -> tuple[str, str]:
        if not isinstance(dataset_id, str) or not dataset_id:
            raise QueryRejected(
                "dataset_id is required: a query without dataset scope could "
                "read another tenant's snapshot",
                code="DATASET_SCOPE_REQUIRED",
            )
        if not isinstance(symbol, str) or not symbol:
            raise QueryRejected("symbol is required", code="SYMBOL_SCOPE_REQUIRED")
        if self.dataset_allowlist is not None and dataset_id not in self.dataset_allowlist:
            raise QueryRejected(
                f"dataset {dataset_id!r} is not authorized for this caller",
                code="DATASET_NOT_AUTHORIZED",
            )
        if self.symbol_allowlist is not None and symbol not in self.symbol_allowlist:
            raise QueryRejected(
                f"symbol {symbol!r} is not in the allowlist",
                code="SYMBOL_NOT_ALLOWED",
            )
        return dataset_id, symbol

    def _resolve_build(self, build_id: object) -> str:
        if not isinstance(build_id, str) or not build_id:
            raise QueryRejected("build_id is required", code="BUILD_SCOPE_REQUIRED")
        if self.limits.require_ready_build:
            manifest = self.catalog.require_ready(build_id)
            return manifest.build_id
        return build_id

    def _resolve_limit(self, requested: object, ceiling: int) -> int:
        if requested is None:
            return ceiling
        if isinstance(requested, bool) or not isinstance(requested, int):
            raise QueryRejected("limit must be an integer", code="LIMIT_NOT_INTEGER")
        if requested <= 0:
            raise QueryRejected("limit must be positive", code="LIMIT_NOT_POSITIVE")
        return min(requested, ceiling)

    # ----------------------------------------------------------------- reads

    def get_bars(
        self,
        *,
        build_id: object,
        dataset_id: object,
        symbol: object,
        from_ns: object,
        to_ns: object,
        limit: object = None,
    ) -> BarResponse:
        """``GET /bars`` (section 10), validated end to end."""
        resolved_build = self._resolve_build(build_id)
        resolved_dataset, resolved_symbol = self._require_scope(dataset_id, symbol)
        window = validate_range(
            from_ns,
            to_ns,
            require_alignment=self.limits.require_minute_alignment,
            max_window_minutes=self.limits.max_window_minutes,
        )
        row_limit = self._resolve_limit(limit, self.limits.max_bars_returned)

        # One row over the limit, so truncation is detectable rather than
        # indistinguishable from "that is all the data there is".
        rows = self.store.select_bars_1m(
            build_id=resolved_build,
            dataset_id=resolved_dataset,
            symbol=resolved_symbol,
            start_ns=window.start_ns,
            end_ns=window.end_ns,
            limit=row_limit + 1,
        )
        truncated = len(rows) > row_limit
        return BarResponse(
            build_id=resolved_build,
            dataset_id=resolved_dataset,
            symbol=resolved_symbol,
            from_ns=str(window.start_ns),
            to_ns=str(window.end_ns),
            bars=tuple(BarRow.from_bar(key, bar) for key, bar in rows[:row_limit]),
            truncated=truncated,
        )

    def get_canonical_range(
        self,
        *,
        build_id: object,
        dataset_id: object,
        symbol: object,
        from_ns: object,
        to_ns: object,
        limit: object = None,
    ) -> dict[str, Any]:
        """Raw tick range inside a READY build.

        Minute alignment is *not* required here: the predicate is on
        ``event_time_ns``, so a half-open window over ticks is exactly defined at
        any bound (case D-12). Bars are the thing that needs aligned edges.
        """
        resolved_build = self._resolve_build(build_id)
        resolved_dataset, resolved_symbol = self._require_scope(dataset_id, symbol)
        window = validate_range(
            from_ns,
            to_ns,
            require_alignment=False,
            max_window_minutes=self.limits.max_window_minutes,
        )
        row_limit = self._resolve_limit(limit, self.limits.max_canonical_rows)
        rows = self.store.select_canonical(
            build_id=resolved_build,
            dataset_id=resolved_dataset,
            symbol=resolved_symbol,
            start_ns=window.start_ns,
            end_ns=window.end_ns,
            limit=row_limit + 1,
        )
        truncated = len(rows) > row_limit
        return {
            "build_id": resolved_build,
            "dataset_id": resolved_dataset,
            "symbol": resolved_symbol,
            "from_ns": str(window.start_ns),
            "to_ns": str(window.end_ns),
            "truncated": truncated,
            "ticks": [
                {
                    "event_time_ns": str(row.event_time_ns),
                    "sequence": str(row.sequence),
                    "price_ticks": row.price_ticks,
                    "price_scale": row.price_scale,
                }
                for row in rows[:row_limit]
            ],
        }
