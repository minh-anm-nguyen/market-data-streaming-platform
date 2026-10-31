"""Deterministic event generator and the normal-mode invariant checker.

Section 4.2 of SYSTEM_DESIGN. Determinism comes from three choices:

1. One :class:`~market_sim.prng.SplitMix64` **per symbol**, seeded by
   ``SHA-256(seed || symbol)`` -- never ``hash(symbol)``, which is randomised per
   process by ``PYTHONHASHSEED``.
2. Symbol selection by *smooth weighted round-robin*, which consumes no
   randomness at all, so changing the weights changes the interleaving in a way
   that is easy to reason about and easy to re-derive in C++ for Project 03.
3. Every price number is an ``int``. No floats anywhere on this path.

The invariant checker runs **before** the fault injector (invariant 8 of section
3.2): normal events are proven correct first, and only then is breakage injected
on purpose. That ordering is what lets us claim a gap in the output is an
intentional gap rather than a generator bug.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

from .clock import LogicalClock
from .config import SimConfig, SymbolConfig
from .errors import InvariantViolation
from .model import EVENT_QUOTE, EVENT_TICK, Event
from .prng import SplitMix64, derive_seed


def reflect_into(value: int, lo: int, hi: int) -> int:
    """Fold ``value`` into ``[lo, hi]`` by reflecting at the edges.

    A triangle wave, so a price that walks into the band edge turns around
    instead of sticking to it. Sticking would show up as a long run of identical
    prices, which is a less useful test feed.
    """
    if hi <= lo:
        return lo
    span = hi - lo
    period = 2 * span
    offset = (value - lo) % period
    return lo + offset if offset <= span else lo + period - offset


def clamp_into(value: int, lo: int, hi: int) -> int:
    """Fold ``value`` into ``[lo, hi]`` by saturating at the edges."""
    return lo if value < lo else (hi if value > hi else value)


class WeightedRoundRobin:
    """Smooth weighted round-robin selection over a fixed symbol order.

    Each step every symbol gains its weight; the highest running total wins and
    pays back the total weight. Ties break by position in ``[symbols].order``, so
    the sequence is fully determined by the configuration -- no randomness, and
    no dependence on dict iteration order.

    With weights 3/2/1 one cycle is EURUSD, EURUSD, GBPUSD, EURUSD, GBPUSD,
    USDJPY: the share is exact over each cycle, not merely on average.
    """

    __slots__ = ("_names", "_weights", "_current", "_total")

    def __init__(self, names: tuple[str, ...], weights: tuple[int, ...]) -> None:
        if not names:
            raise ValueError("WeightedRoundRobin needs at least one symbol")
        if len(names) != len(weights):
            raise ValueError("names and weights must be the same length")
        if any(w <= 0 for w in weights):
            raise ValueError("weights must be positive")
        self._names = names
        self._weights = weights
        self._current = [0] * len(names)
        self._total = sum(weights)

    def next_index(self) -> int:
        best = 0
        best_value: int | None = None
        for i, weight in enumerate(self._weights):
            self._current[i] += weight
            value = self._current[i]
            if best_value is None or value > best_value:
                best_value = value
                best = i
        self._current[best] -= self._total
        return best

    def next_symbol(self) -> str:
        return self._names[self.next_index()]


@dataclass(slots=True)
class SymbolState:
    """Mutable per-symbol generator state."""

    config: SymbolConfig
    rng: SplitMix64
    mid_ticks: int
    next_sequence: int = 1
    emitted: int = 0

    def snapshot(self) -> dict[str, int]:
        return {
            "mid_ticks": self.mid_ticks,
            "next_sequence": self.next_sequence,
            "emitted": self.emitted,
            "prng_draws": self.rng.draws,
        }


class Generator:
    """Produces one canonical :class:`~market_sim.model.Event` per logical slot.

    ``generate(slot_index)`` must be called with consecutive indices starting at
    0; this is a state machine, not a random-access function. The returned event
    is guaranteed to satisfy the normal-mode invariants of section 3.2 --
    :func:`check_normal_invariants` runs on every event, so a generator bug fails
    the run instead of landing in the golden dataset.
    """

    def __init__(self, config: SimConfig, *, check_invariants: bool = True) -> None:
        self._config = config
        self._dataset_id = config.dataset_id
        self._clock = LogicalClock(config.run.start_time_ns, config.run.rate_per_sec)
        self._quote_ratio = config.run.quote_ratio_pct
        self._check_invariants = check_invariants
        self._reflect = config.run.bound_policy == "reflect"
        # Resolved once: this is chosen per run, and re-deciding it per event was
        # a measurable branch plus attribute lookup on the hot path.
        self._fold = reflect_into if self._reflect else clamp_into
        self._selector = WeightedRoundRobin(
            config.symbol_order,
            tuple(config.symbols[s].weight for s in config.symbol_order),
        )
        self._states: dict[str, SymbolState] = {}
        for name in config.symbol_order:
            sym = config.symbols[name]
            self._states[name] = SymbolState(
                config=sym,
                rng=SplitMix64(derive_seed(config.run.seed, name)),
                mid_ticks=sym.initial_price_ticks,
            )
        self._next_slot = 0

    @property
    def clock(self) -> LogicalClock:
        return self._clock

    @property
    def dataset_id(self) -> str:
        return self._dataset_id

    def state_snapshot(self) -> dict[str, dict[str, int]]:
        """Per-symbol state, for the run report and determinism diagnostics."""
        return {name: st.snapshot() for name, st in self._states.items()}

    def generate(self, slot_index: int) -> Event:
        if slot_index != self._next_slot:
            raise InvariantViolation(
                "generator called out of order",
                expected_slot=self._next_slot,
                got_slot=slot_index,
            )
        self._next_slot += 1

        symbol = self._selector.next_symbol()
        state = self._states[symbol]
        sym = state.config
        rng = state.rng

        # Fixed draw order: type, step, spread. All three are always drawn, even
        # when the value goes unused, so the stream position depends only on the
        # slot index -- not on which branch was taken.
        type_roll = rng.below(100)
        step = (
            rng.range_inclusive(-sym.max_step_ticks, sym.max_step_ticks)
            if sym.max_step_ticks
            else 0
        )
        spread = rng.range_inclusive(sym.min_spread_ticks, sym.max_spread_ticks)

        mid = self._fold(
            state.mid_ticks + step, sym.min_price_ticks, sym.max_price_ticks
        )
        state.mid_ticks = mid

        sequence = state.next_sequence
        state.next_sequence += 1
        state.emitted += 1
        event_time_ns = self._clock.event_time_ns(slot_index)

        if type_roll < self._quote_ratio:
            # Keep the whole quote inside the price band by shifting the quote,
            # never the spread, so the configured spread distribution stays intact.
            half = spread // 2
            lo = sym.min_price_ticks + half
            hi = sym.max_price_ticks - (spread - half)
            quote_mid = clamp_into(mid, lo, hi)
            bid = quote_mid - half
            event = Event(
                dataset_id=self._dataset_id,
                event_type=EVENT_QUOTE,
                symbol=symbol,
                sequence=sequence,
                event_time_ns=event_time_ns,
                price_scale=sym.price_scale,
                bid_ticks=bid,
                ask_ticks=bid + spread,
            )
        else:
            event = Event(
                dataset_id=self._dataset_id,
                event_type=EVENT_TICK,
                symbol=symbol,
                sequence=sequence,
                event_time_ns=event_time_ns,
                price_scale=sym.price_scale,
                price_ticks=mid,
            )

        if self._check_invariants:
            check_normal_invariants(event, sym, dataset_id=self._dataset_id)
        return event

    def iter_slots(self, count: int) -> Iterator[Event]:
        """Convenience iterator over the next ``count`` slots. Used by tests."""
        for slot in range(self._next_slot, self._next_slot + count):
            yield self.generate(slot)


def check_normal_invariants(
    event: Event, sym: SymbolConfig, *, dataset_id: str | None = None
) -> None:
    """Assert the section 3.2 invariants for a **non-injected** event.

    Raises :class:`InvariantViolation`, which is a runtime failure rather than a
    data anomaly: in normal mode a bad event means the generator is broken, and
    the run must fail loudly rather than record it.
    """
    if dataset_id is not None and event.dataset_id != dataset_id:
        raise InvariantViolation(
            "dataset_id mismatch", expected=dataset_id, got=event.dataset_id
        )
    if event.symbol != sym.symbol:
        raise InvariantViolation("symbol mismatch", expected=sym.symbol, got=event.symbol)
    if event.price_scale != sym.price_scale:
        raise InvariantViolation(
            "price_scale mismatch",
            symbol=event.symbol,
            expected=sym.price_scale,
            got=event.price_scale,
        )
    if event.sequence < 1:
        raise InvariantViolation(
            "sequence must start at 1", symbol=event.symbol, sequence=event.sequence
        )
    if event.event_time_ns < 0:
        raise InvariantViolation(
            "event_time_ns must be >= 0",
            symbol=event.symbol,
            event_time_ns=event.event_time_ns,
        )

    if event.event_type == EVENT_QUOTE:
        bid, ask = event.bid_ticks, event.ask_ticks
        if bid is None or ask is None:
            raise InvariantViolation("quote missing bid/ask", symbol=event.symbol)
        if event.price_ticks is not None:
            raise InvariantViolation(
                "quote must not carry price_ticks", symbol=event.symbol
            )
        if bid <= 0:
            raise InvariantViolation("bid_ticks must be > 0", symbol=event.symbol, bid=bid)
        if ask < bid:
            raise InvariantViolation(
                "ask_ticks must be >= bid_ticks", symbol=event.symbol, bid=bid, ask=ask
            )
        spread = ask - bid
        if not (sym.min_spread_ticks <= spread <= sym.max_spread_ticks):
            raise InvariantViolation(
                "spread outside configured range",
                symbol=event.symbol,
                spread=spread,
                min=sym.min_spread_ticks,
                max=sym.max_spread_ticks,
            )
        if bid < sym.min_price_ticks or ask > sym.max_price_ticks:
            raise InvariantViolation(
                "quote outside configured price band",
                symbol=event.symbol,
                bid=bid,
                ask=ask,
                min=sym.min_price_ticks,
                max=sym.max_price_ticks,
            )
    elif event.event_type == EVENT_TICK:
        price = event.price_ticks
        if price is None:
            raise InvariantViolation("tick missing price_ticks", symbol=event.symbol)
        if event.bid_ticks is not None or event.ask_ticks is not None:
            raise InvariantViolation("tick must not carry bid/ask", symbol=event.symbol)
        if price <= 0:
            raise InvariantViolation(
                "price_ticks must be > 0", symbol=event.symbol, price=price
            )
        if not (sym.min_price_ticks <= price <= sym.max_price_ticks):
            raise InvariantViolation(
                "price outside configured band",
                symbol=event.symbol,
                price=price,
                min=sym.min_price_ticks,
                max=sym.max_price_ticks,
            )
    else:
        raise InvariantViolation("unknown event_type", event_type=event.event_type)
