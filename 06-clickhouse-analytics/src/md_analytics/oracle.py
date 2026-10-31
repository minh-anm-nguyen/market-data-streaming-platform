"""Independent Python reference aggregator for 1m tick OHLC bars.

Section 7.2. This module is the oracle the SQL is judged against, so it is
written to share **nothing** with the materialized view: no SQL, no aggregate
states, no ClickHouse client. If both sides were derived from one helper, a
mistake in that helper would make them agree and the proof would be worthless.

Two implementations are provided on purpose:

* :func:`reference_bars` is a single-pass streaming fold, O(open bars) in memory,
  which is what a 200M-row run needs (section 7.2 forbids loading the corpus
  into RAM).
* :func:`reference_bars_sorted` is the literal sort-then-take-ends listing from
  the design document.

They must agree for every input. Group AD is about oracle integrity, and two
independently written implementations that agree is the cheapest real evidence
available that neither has a sign error at the ends.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Iterator
from typing import NamedTuple, Protocol

from .contracts import SourceOrder, require_price_scale
from .errors import ContractError
from .timebars import BAR_INTERVAL_NS, bar_start_ns


class TickLike(Protocol):
    """What the oracle needs from a tick: nothing about storage."""

    build_id: str
    dataset_id: str
    symbol: str
    sequence: int
    event_time_ns: int
    price_scale: int
    price_ticks: int


class BarKey(NamedTuple):
    """Bar scope (section 3.3).

    ``price_scale`` is part of the key, so two scale versions of one symbol can
    never blend into a bar whose unit is undefined (case C-03, H-08), and
    ``dataset_id`` is part of it so two datasets sharing a symbol stay separate
    (case H-07).
    """

    build_id: str
    dataset_id: str
    symbol: str
    price_scale: int
    bar_start_ns: int

    @property
    def bar_end_ns(self) -> int:
        return self.bar_start_ns + BAR_INTERVAL_NS


class Bar(NamedTuple):
    """A finalized bar. All prices are integer ticks.

    There is no ``volume`` field and there will not be one: P02 ticks carry no
    trade size, so ``tick_count`` counts *price updates* and labelling it volume
    would invent data (section 3.2, case H-06).
    """

    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int
    tick_count: int

    def check_bounds(self) -> None:
        """INV-07. Necessary, nowhere near sufficient -- see section 17.1."""
        if not (
            self.low_ticks <= self.open_ticks <= self.high_ticks
            and self.low_ticks <= self.close_ticks <= self.high_ticks
        ):
            raise ContractError(
                f"bar violates LOW <= OPEN/CLOSE <= HIGH: {self}",
                code="BAR_BOUNDS_VIOLATION",
            )


class _Accumulator:
    """Running state for one bar during the streaming fold."""

    __slots__ = ("open_order", "open_price", "close_order", "close_price", "high", "low", "count")

    def __init__(self, order: SourceOrder, price: int) -> None:
        self.open_order = self.close_order = order
        self.open_price = self.close_price = price
        self.high = self.low = price
        self.count = 1

    def add(self, order: SourceOrder, price: int) -> None:
        if order < self.open_order:
            self.open_order, self.open_price = order, price
        if order > self.close_order:
            self.close_order, self.close_price = order, price
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.count += 1

    def finalize(self) -> Bar:
        return Bar(self.open_price, self.high, self.low, self.close_price, self.count)


def _tick_key_and_order(tick: TickLike) -> tuple[BarKey, SourceOrder, int]:
    scale = require_price_scale(tick.price_scale)
    key = BarKey(
        build_id=tick.build_id,
        dataset_id=tick.dataset_id,
        symbol=tick.symbol,
        price_scale=scale,
        bar_start_ns=bar_start_ns(tick.event_time_ns),
    )
    return key, SourceOrder(tick.event_time_ns, tick.sequence), tick.price_ticks


def reference_bars(ticks: Iterable[TickLike]) -> dict[BarKey, Bar]:
    """Fold canonical ticks into bars in one streaming pass.

    Raises :class:`ContractError` if two ticks in the same bar share an ordering
    key ``(event_time_ns, sequence)``: inside one ``(dataset_id, symbol)`` the
    sequence is unique per identity, so a collision means the *input* is not a
    clean canonical set, and OPEN/CLOSE would stop being well-defined. Case I-04
    requires that to stop the build rather than resolve arbitrarily.
    """
    acc: dict[BarKey, _Accumulator] = {}
    seen: dict[BarKey, dict[SourceOrder, int]] = defaultdict(dict)
    for tick in ticks:
        key, order, price = _tick_key_and_order(tick)
        prior = seen[key].get(order)
        if prior is not None:
            raise ContractError(
                f"two canonical ticks share order {tuple(order)} in bar {key}: "
                f"prices {prior} and {price}",
                code="ORDER_KEY_COLLISION",
                bar_key=tuple(key),
                order=tuple(order),
            )
        seen[key][order] = price
        existing = acc.get(key)
        if existing is None:
            acc[key] = _Accumulator(order, price)
        else:
            existing.add(order, price)
    return {key: a.finalize() for key, a in acc.items()}


def reference_bars_sorted(ticks: Iterable[TickLike]) -> dict[BarKey, Bar]:
    """The literal section 7.2 listing: group, sort by order, take the ends.

    Kept as a second, deliberately naive oracle. It buffers every tick per bar,
    so it is for fixtures and property tests rather than for the 200M corpus.
    """
    grouped: dict[BarKey, list[tuple[SourceOrder, int]]] = defaultdict(list)
    for tick in ticks:
        key, order, price = _tick_key_and_order(tick)
        grouped[key].append((order, price))

    out: dict[BarKey, Bar] = {}
    for key, entries in grouped.items():
        entries.sort(key=lambda item: item[0])
        orders = [o for o, _ in entries]
        if len(set(orders)) != len(orders):
            raise ContractError(
                f"duplicate ordering key within bar {key}",
                code="ORDER_KEY_COLLISION",
                bar_key=tuple(key),
            )
        prices = [p for _, p in entries]
        out[key] = Bar(
            open_ticks=prices[0],
            high_ticks=max(prices),
            low_ticks=min(prices),
            close_ticks=prices[-1],
            tick_count=len(prices),
        )
    return out


def bars_in_range(
    bars: dict[BarKey, Bar],
    *,
    build_id: str,
    dataset_id: str,
    symbol: str,
    start_ns: int,
    end_ns: int,
) -> list[tuple[BarKey, Bar]]:
    """Scoped, ordered slice of a reference bar set, matching the read query.

    The predicate is on ``bar_start_ns``, exactly as the SQL in section 4.3 is,
    so a disagreement between the two is a real disagreement and not an artefact
    of the oracle filtering differently.
    """
    selected = [
        (key, bar)
        for key, bar in bars.items()
        if key.build_id == build_id
        and key.dataset_id == dataset_id
        and key.symbol == symbol
        and start_ns <= key.bar_start_ns < end_ns
    ]
    selected.sort(key=lambda item: (item[0].bar_start_ns, item[0].price_scale))
    return selected


def iter_sorted_bars(bars: dict[BarKey, Bar]) -> Iterator[tuple[BarKey, Bar]]:
    """Deterministic iteration order for evidence files and diffs."""
    yield from sorted(bars.items(), key=lambda item: item[0])


def total_tick_count(bars: dict[BarKey, Bar]) -> int:
    """INV-06 left-hand side: the sum of per-bar counts."""
    return sum(bar.tick_count for bar in bars.values())
