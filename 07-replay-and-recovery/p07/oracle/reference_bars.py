"""Reference 1-minute OHLC, computed in integers from the accepted tick set.

P06 section 4 fixes the arithmetic::

    bar_start_ns = floor(event_time_ns / 60_000_000_000) * 60_000_000_000   # UTC
    bar_key      = (dataset_id, symbol, price_scale, bar_start_ns)
    open         = price of the first tick by (event_time_ns, sequence)
    close        = price of the last  tick by (event_time_ns, sequence)
    high / low   = max / min price_ticks in the bucket
    tick_count   = number of accepted ticks in the bucket

Four things this oracle refuses to do, each of them a way OHLC gets quietly
wrong:

* **No volume.** A P01 tick is a price update with no size. ``tick_count`` is a
  count of updates and is never reported as traded volume (P06 section 3.1).
* **No quotes.** Bid/ask never enter a price bar; quote bars are a separate
  product that would need its own approved rounding policy.
* **No empty bars.** A minute with no ticks produces no row. Fill-forward is
  undefined in P06's MVP, and inventing a bar would invent a price.
* **No mixed ``price_scale``.** If one ``(dataset_id, symbol)`` shows two
  scales, the result is a conflict, not a conversion -- P06 section 3 says
  STOP/CONFLICT, and silently rescaling would corrupt every price in the bar.

Deduplication happens by :class:`~p07.contracts.SourceIdentity` before any
aggregation, which is what keeps a physical Kafka or ClickHouse retry from
inflating ``tick_count`` (``INV-09``, cases ``E2E-27`` and group ``G18``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, NamedTuple

from ..contracts import BAR_WIDTH_NS, EventKind, SemanticCore, SourceIdentity


class BarKey(NamedTuple):
    """Identity of one bar. ``price_scale`` is part of it, never averaged over."""

    dataset_id: str
    symbol: str
    price_scale: int
    bar_start_ns: int


class Bar(NamedTuple):
    """One 1-minute OHLC bar in integer ticks."""

    key: BarKey
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int
    tick_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.key.dataset_id,
            "symbol": self.key.symbol,
            "price_scale": self.key.price_scale,
            "bar_start_ns": self.key.bar_start_ns,
            "open_ticks": self.open_ticks,
            "high_ticks": self.high_ticks,
            "low_ticks": self.low_ticks,
            "close_ticks": self.close_ticks,
            "tick_count": self.tick_count,
        }


class PriceScaleConflict(Exception):
    """Two ``price_scale`` values for one ``(dataset_id, symbol)``."""

    def __init__(self, dataset_id: str, symbol: str, scales: tuple[int, ...]) -> None:
        super().__init__(f"{dataset_id}/{symbol} has price scales {scales}")
        self.dataset_id = dataset_id
        self.symbol = symbol
        self.scales = scales


@dataclass(slots=True)
class _Accumulator:
    """Running state for one bucket, ordered by ``(event_time_ns, sequence)``."""

    first_sort_key: tuple[int, int]
    last_sort_key: tuple[int, int]
    open_ticks: int
    close_ticks: int
    high_ticks: int
    low_ticks: int
    tick_count: int

    def add(self, sort_key: tuple[int, int], price: int) -> None:
        if sort_key < self.first_sort_key:
            self.first_sort_key = sort_key
            self.open_ticks = price
        if sort_key > self.last_sort_key:
            self.last_sort_key = sort_key
            self.close_ticks = price
        if price > self.high_ticks:
            self.high_ticks = price
        if price < self.low_ticks:
            self.low_ticks = price
        self.tick_count += 1


def bar_start_ns(event_time_ns: int) -> int:
    """UTC bucket start for ``event_time_ns``, flooring towards -inf."""
    return (event_time_ns // BAR_WIDTH_NS) * BAR_WIDTH_NS


def reference_bars(cores: Iterable[SemanticCore]) -> dict[BarKey, Bar]:
    """Aggregate accepted ticks into bars, keyed by :class:`BarKey`.

    Quotes in the input are ignored, so a caller can pass the whole accepted set
    without pre-filtering. Input order does not matter: the
    ``(event_time_ns, sequence)`` sort key decides open and close, which is what
    makes the result deterministic for ticks sharing a nanosecond
    (case ``E2E-26``).

    Single pass, O(buckets) in memory rather than sorting the input, because the
    capacity profiles run to hundreds of millions of ticks and a global sort
    would not fit.
    """
    scales: dict[tuple[str, str], int] = {}
    accumulators: dict[BarKey, _Accumulator] = {}

    for core in cores:
        if core.event_kind is not EventKind.TICK:
            continue
        price = core.price_ticks
        if price is None:
            # A tick without a present price is business-invalid and must never
            # have reached the accepted set. Refusing it here keeps the oracle
            # from inventing a zero.
            raise ValueError(f"accepted tick without price_ticks: {core.identity}")
        scale_key = (core.dataset_id, core.symbol)
        known = scales.setdefault(scale_key, core.price_scale)
        if known != core.price_scale:
            raise PriceScaleConflict(core.dataset_id, core.symbol, (known, core.price_scale))

        key = BarKey(core.dataset_id, core.symbol, core.price_scale, core.bar_start_ns)
        sort_key = (core.event_time_ns, core.sequence)
        existing = accumulators.get(key)
        if existing is None:
            accumulators[key] = _Accumulator(
                first_sort_key=sort_key,
                last_sort_key=sort_key,
                open_ticks=price,
                close_ticks=price,
                high_ticks=price,
                low_ticks=price,
                tick_count=1,
            )
        else:
            existing.add(sort_key, price)

    return {
        key: Bar(
            key=key,
            open_ticks=acc.open_ticks,
            high_ticks=acc.high_ticks,
            low_ticks=acc.low_ticks,
            close_ticks=acc.close_ticks,
            tick_count=acc.tick_count,
        )
        for key, acc in accumulators.items()
    }


def reference_bars_from_identity_map(
    cores_by_identity: Mapping[SourceIdentity, SemanticCore],
) -> dict[BarKey, Bar]:
    """Bars from an identity-keyed map, so deduplication is structural.

    This is the entry point the reconciler uses: taking a map keyed by identity
    makes "one logical event counted once" a property of the type rather than
    something the aggregation has to remember to do.
    """
    return reference_bars(cores_by_identity.values())


def diff_bars(
    expected: Mapping[BarKey, Bar], observed: Mapping[BarKey, Bar]
) -> dict[str, list[dict[str, Any]]]:
    """Exact bar-level diff: missing, extra and per-field mismatches.

    Returns the offending rows rather than counts, because an operator cannot
    act on "17 bars differ"; ``allow_sample_only_pass: false`` in the lab config
    is about proof, and a count is not one.
    """
    missing = [expected[key].as_dict() for key in sorted(set(expected) - set(observed))]
    extra = [observed[key].as_dict() for key in sorted(set(observed) - set(expected))]
    mismatched: list[dict[str, Any]] = []
    for key in sorted(set(expected) & set(observed)):
        want, got = expected[key], observed[key]
        if want == got:
            continue
        fields = [
            name
            for name in ("open_ticks", "high_ticks", "low_ticks", "close_ticks", "tick_count")
            if getattr(want, name) != getattr(got, name)
        ]
        mismatched.append(
            {
                "dataset_id": key.dataset_id,
                "symbol": key.symbol,
                "price_scale": key.price_scale,
                "bar_start_ns": key.bar_start_ns,
                "fields": fields,
                "expected": want.as_dict(),
                "observed": got.as_dict(),
            }
        )
    return {"missing": missing, "extra": extra, "mismatched": mismatched}
