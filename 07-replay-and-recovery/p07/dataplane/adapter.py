"""P03: the MDP1 framer, validator and sequence tracker, as the thing under test.

This is a **second, independent implementation** of the policy that
:mod:`p07.oracle.acceptance_oracle` also implements. The duplication is the
point and is not a refactoring opportunity: an oracle that called into the
adapter could not detect the adapter being wrong, which is the circular-oracle
trap design section 2.1 names explicitly. The two are written differently on
purpose -- the oracle is a batch pass over a sealed file with a dict-based
window, this is a byte-fed state machine with a ring buffer -- so a shared
misreading of the spec is less likely to cancel out.

The frame machine follows P03 section 4.1 and is byte-chunking agnostic:
``feed(b)`` may be called with one byte or a megabyte, and a frame split across
any number of calls decodes identically (case ``E2E-04``). A frame is parsed
only once complete; a truncated tail emits nothing, so killing the process
mid-frame can lose the frame but can never emit half an event (case ``E2E-07``).
"""

from __future__ import annotations

import struct
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Final, Iterable, Mapping

from ..contracts import EventKind, SemanticCore
from .codec import MDP1_MAGIC, MDP1_MAX_LENGTH, MDP1_MIN_LENGTH, Codec, CodecError, DEFAULT_CODEC

_LENGTH = struct.Struct(">I")

#: Mirrors the oracle's default window. Stated separately rather than imported,
#: so changing one does not silently change the other -- a divergence here is a
#: finding, not a refactor.
SEQUENCE_WINDOW: Final = 4096
EVENT_TIME_STALE_NS: Final = 2_000_000_000


class FrameState(Enum):
    PREFIX = "PREFIX"
    HEADER = "HEADER"
    BODY = "BODY"
    CLOSED = "CLOSED"


@dataclass(frozen=True, slots=True)
class AdapterOutcome:
    """What the adapter did with one decoded event."""

    core: SemanticCore | None
    published: bool
    reason: str | None
    flags: frozenset[str] = frozenset()


@dataclass(slots=True)
class _Key:
    """Tracker state for one ``(dataset_id, symbol)``.

    ``ring`` is a bounded deque of ``(sequence, core)`` plus a dict index. The
    deque is what enforces the bound: when it is full the oldest entry leaves
    the index too, so a long run cannot grow this without limit. The typed core
    is kept, not a short hash, because P03 section 5 forbids concluding
    ``DUPLICATE_EXACT`` from a hash that can collide.
    """

    highest: int | None = None
    ring: deque[int] = field(default_factory=lambda: deque(maxlen=SEQUENCE_WINDOW))
    index: dict[int, SemanticCore] = field(default_factory=dict)
    missing: set[int] = field(default_factory=set)
    quote_watermark_ns: int | None = None
    last_mid: int | None = None

    def remember(self, core: SemanticCore) -> None:
        if core.sequence in self.index:
            self.index[core.sequence] = core
            return
        if len(self.ring) == self.ring.maxlen:
            evicted = self.ring[0]
            self.index.pop(evicted, None)
            self.missing.discard(evicted)
        self.ring.append(core.sequence)
        self.index[core.sequence] = core

    def in_window(self, sequence: int) -> bool:
        return self.highest is not None and sequence > self.highest - SEQUENCE_WINDOW


@dataclass(slots=True)
class AdapterCounters:
    """Counters the adapter reports about itself.

    Design 0.1 is explicit that these are *not* ground truth: P07 compares the
    oracle's expected sets against independently scanned Kafka and ClickHouse,
    and uses these only to localise a discrepancy. They are here so a test can
    assert the adapter's self-report disagrees with reality when it should.
    """

    bytes_fed: int = 0
    frames_decoded: int = 0
    structural_rejects: int = 0
    business_rejects: int = 0
    policy_suppressed: int = 0
    published: int = 0
    gaps_detected: int = 0
    conflicts_detected: int = 0
    closed_reason: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "bytes_fed": self.bytes_fed,
            "frames_decoded": self.frames_decoded,
            "structural_rejects": self.structural_rejects,
            "business_rejects": self.business_rejects,
            "policy_suppressed": self.policy_suppressed,
            "published": self.published,
            "gaps_detected": self.gaps_detected,
            "conflicts_detected": self.conflicts_detected,
            "closed_reason": self.closed_reason,
        }


class SymbolLimits:
    """Business limits for one symbol, as the adapter knows them."""

    __slots__ = ("price_scale", "min_price", "max_price", "min_spread", "max_spread", "max_step")

    def __init__(
        self,
        price_scale: int,
        min_price: int,
        max_price: int,
        min_spread: int,
        max_spread: int,
        max_step: int,
    ) -> None:
        self.price_scale = price_scale
        self.min_price = min_price
        self.max_price = max_price
        self.min_spread = min_spread
        self.max_spread = max_spread
        self.max_step = max_step


class Adapter:
    """Byte-stream adapter: frame, decode, validate, classify, publish.

    ``sink`` receives every published :class:`~p07.contracts.SemanticCore` in
    publication order. ``on_outcome`` (optional) receives every outcome
    including rejects, which the harness records as the adapter's own account of
    events -- to be checked, not believed.
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        limits: Mapping[str, SymbolLimits],
        sink: Callable[[SemanticCore], None],
        codec: Codec = DEFAULT_CODEC,
        on_outcome: Callable[[AdapterOutcome], None] | None = None,
        fail_closed: bool = True,
    ) -> None:
        self._dataset_id = dataset_id
        self._limits = dict(limits)
        self._sink = sink
        self._codec = codec
        self._on_outcome = on_outcome
        self._fail_closed = fail_closed
        self._buffer = bytearray()
        self._state = FrameState.PREFIX
        self._body_length = 0
        self._keys: dict[str, _Key] = {}
        self.counters = AdapterCounters()

    # -- transport ------------------------------------------------------

    @property
    def state(self) -> FrameState:
        return self._state

    @property
    def pending_bytes(self) -> int:
        """Bytes held for an incomplete frame. Non-zero means data in flight."""
        return len(self._buffer)

    def feed(self, chunk: bytes) -> None:
        """Consume ``chunk``. Any number of complete frames may come out."""
        if self._state is FrameState.CLOSED:
            return
        self.counters.bytes_fed += len(chunk)
        self._buffer += chunk
        while self._step():
            pass

    def close(self, reason: str = "EOF") -> None:
        """End of stream. A partial frame in the buffer is *not* emitted."""
        if self._state is FrameState.CLOSED:
            return
        if self._buffer:
            self._state = FrameState.CLOSED
            self.counters.closed_reason = "TRUNCATED_BODY" if self._body_length else "TRUNCATED_PREFIX"
            return
        self._state = FrameState.CLOSED
        self.counters.closed_reason = reason

    def _close_bad(self, reason: str) -> None:
        self.counters.structural_rejects += 1
        self.counters.closed_reason = reason
        if self._fail_closed:
            self._state = FrameState.CLOSED
            self._buffer.clear()

    def _step(self) -> bool:
        """One state-machine transition. Returns whether progress was made."""
        if self._state is FrameState.CLOSED:
            return False
        if self._state is FrameState.PREFIX:
            if len(self._buffer) < 4:
                return False
            if bytes(self._buffer[:4]) != MDP1_MAGIC:
                # No resynchronisation scan: P03 rule 1 forbids hunting for the
                # next magic, because a payload byte sequence could spoof it.
                self._close_bad("BAD_MAGIC")
                return False
            del self._buffer[:4]
            self._state = FrameState.HEADER
            return True
        if self._state is FrameState.HEADER:
            if len(self._buffer) < 4:
                return False
            (length,) = _LENGTH.unpack(bytes(self._buffer[:4]))
            if not MDP1_MIN_LENGTH <= length <= MDP1_MAX_LENGTH:
                # Rejected *before* reserving anything, so a 0xFFFFFFFF length
                # cannot be turned into an allocation.
                self._close_bad("INVALID_LENGTH")
                return False
            del self._buffer[:4]
            self._body_length = length
            self._state = FrameState.BODY
            return True
        if len(self._buffer) < self._body_length:
            return False
        payload = bytes(self._buffer[: self._body_length])
        del self._buffer[: self._body_length]
        self._body_length = 0
        self._state = FrameState.PREFIX
        self._handle_payload(payload)
        return True

    # -- policy ---------------------------------------------------------

    def _emit(self, outcome: AdapterOutcome) -> None:
        if self._on_outcome is not None:
            self._on_outcome(outcome)

    def _handle_payload(self, payload: bytes) -> None:
        self.counters.frames_decoded += 1
        try:
            core = self._codec.decode(payload)
        except CodecError as exc:
            self.counters.structural_rejects += 1
            self._emit(AdapterOutcome(None, False, exc.reason))
            if self._fail_closed and exc.reason == "PROTO_DECODE_ERROR":
                self._state = FrameState.CLOSED
                self.counters.closed_reason = exc.reason
            return
        self._classify(core)

    def _classify(self, core: SemanticCore) -> None:
        # Stage 1 continued: identity scope.
        if core.dataset_id != self._dataset_id:
            self.counters.structural_rejects += 1
            self._emit(AdapterOutcome(core, False, "WRONG_DATASET"))
            return
        limits = self._limits.get(core.symbol)
        if limits is None:
            self.counters.business_rejects += 1
            self._emit(AdapterOutcome(core, False, "UNKNOWN_SYMBOL"))
            return

        # Stage 2: business. No tracker state is touched on a reject.
        reason = self._business_reason(core, limits)
        if reason is not None:
            self.counters.business_rejects += 1
            self._emit(AdapterOutcome(core, False, reason))
            return

        key = self._keys.setdefault(core.symbol, _Key())
        flags: set[str] = set()

        # Stage 3: sequence.
        if key.highest is None:
            pass
        elif core.sequence > key.highest:
            if core.sequence > key.highest + 1:
                floor = max(key.highest + 1, core.sequence - SEQUENCE_WINDOW)
                key.missing.update(range(floor, core.sequence))
                self.counters.gaps_detected += 1
                flags.add("SEQUENCE_GAP")
        elif key.in_window(core.sequence):
            previous = key.index.get(core.sequence)
            if previous is not None:
                if previous == core:
                    self.counters.policy_suppressed += 1
                    self._emit(AdapterOutcome(core, False, "DUPLICATE_EXACT", frozenset({"DUPLICATE_EXACT"})))
                else:
                    self.counters.conflicts_detected += 1
                    self.counters.business_rejects += 1
                    self._emit(
                        AdapterOutcome(core, False, "DUPLICATE_CONFLICT", frozenset({"DUPLICATE_CONFLICT"}))
                    )
                return
            if core.sequence in key.missing:
                key.remember(core)
                key.missing.discard(core.sequence)
                self.counters.policy_suppressed += 1
                self._emit(AdapterOutcome(core, False, "REORDER_LATE", frozenset({"REORDER_LATE"})))
                return
            self.counters.business_rejects += 1
            self._emit(
                AdapterOutcome(core, False, "TOO_OLD_UNVERIFIABLE", frozenset({"TOO_OLD_UNVERIFIABLE"}))
            )
            return
        else:
            self.counters.business_rejects += 1
            self._emit(
                AdapterOutcome(core, False, "TOO_OLD_UNVERIFIABLE", frozenset({"TOO_OLD_UNVERIFIABLE"}))
            )
            return

        # Stage 4: freshness. Quotes only; a tick is not judged by the quote
        # watermark and does not refresh it.
        if core.event_kind is EventKind.QUOTE and key.quote_watermark_ns is not None:
            if key.quote_watermark_ns - core.event_time_ns > EVENT_TIME_STALE_NS:
                self.counters.policy_suppressed += 1
                self._emit(AdapterOutcome(core, False, "EVENT_TIME_OLD", frozenset({"EVENT_TIME_OLD"})))
                return

        # Stage 5: publish.
        key.remember(core)
        if key.highest is None or core.sequence > key.highest:
            key.highest = core.sequence
        if core.event_kind is EventKind.QUOTE:
            bid, ask = core.bid_ticks, core.ask_ticks
            if bid is not None and ask is not None:
                key.quote_watermark_ns = max(key.quote_watermark_ns or 0, core.event_time_ns)
                key.last_mid = (bid + ask) // 2
        elif core.price_ticks is not None:
            key.last_mid = core.price_ticks

        self.counters.published += 1
        self._sink(core)
        self._emit(AdapterOutcome(core, True, None, frozenset(flags)))

    def _business_reason(self, core: SemanticCore, limits: SymbolLimits) -> str | None:
        if core.sequence <= 0:
            return "INVALID_SEQUENCE"
        if core.event_time_ns < 0:
            return "INVALID_EVENT_TIME"
        if core.price_scale <= 0 or core.price_scale != limits.price_scale:
            return "BAD_SCALE"
        if core.event_kind is EventKind.QUOTE:
            bid, ask = core.bid_ticks, core.ask_ticks
            if bid is None or ask is None:
                return "STRUCTURAL_REJECT"
            if bid <= 0:
                return "INVALID_BID"
            if ask < bid:
                return "INVALID_ASK"
            if not limits.min_spread <= ask - bid <= limits.max_spread:
                return "SPREAD_OUT_OF_RANGE"
            if not limits.min_price <= bid <= limits.max_price:
                return "PRICE_OUT_OF_RANGE"
            if not limits.min_price <= ask <= limits.max_price:
                return "PRICE_OUT_OF_RANGE"
            return None
        price = core.price_ticks
        if price is None:
            return "STRUCTURAL_REJECT"
        if price <= 0:
            return "INVALID_PRICE"
        if not limits.min_price <= price <= limits.max_price:
            return "PRICE_OUT_OF_RANGE"
        return None


def limits_from_sealed(symbols: Mapping[str, object]) -> dict[str, SymbolLimits]:
    """Build adapter limits from :class:`~p07.dataset_sealer.SymbolPolicy` values.

    The adapter is configured from the sealed manifest in the lab, the same way
    a deployed adapter is configured from its own symbol config. It never sees
    the fault manifest: P01 section 7 keeps the answer key out of the stream.
    """
    out: dict[str, SymbolLimits] = {}
    for name, policy in symbols.items():
        out[name] = SymbolLimits(
            price_scale=getattr(policy, "price_scale"),
            min_price=getattr(policy, "min_price_ticks"),
            max_price=getattr(policy, "max_price_ticks"),
            min_spread=getattr(policy, "min_spread_ticks"),
            max_spread=getattr(policy, "max_spread_ticks"),
            max_step=getattr(policy, "max_step_ticks"),
        )
    return out


def chunked(data: bytes, sizes: Iterable[int]) -> Iterable[bytes]:
    """Split ``data`` into chunks cycling through ``sizes``.

    Used by the fragmentation cases to prove framing is chunk-agnostic: the
    1-byte and 17-byte patterns in ``E2E-04`` both come from here.
    """
    sizes = list(sizes)
    if not sizes or any(size <= 0 for size in sizes):
        raise ValueError("sizes must be positive")
    position = 0
    index = 0
    while position < len(data):
        size = sizes[index % len(sizes)]
        yield data[position : position + size]
        position += size
        index += 1
