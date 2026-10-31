"""Portable deterministic PRNG.

ADR-010 (extends ADR-002 of SYSTEM_DESIGN section 4.2): the generator does **not**
use ``random.Random``.  ``random`` is a Mersenne Twister whose *consumption pattern*
per API call is a CPython implementation detail, so byte-identical output across
Python versions / other languages is not something we can promise.  SplitMix64 is
32 lines of integer arithmetic, is specified exactly, and produces identical
streams on any runtime with 64-bit integers -- which is what Project 03 (C++) will
need in order to reproduce a dataset.

Unbiased bounded draws use rejection sampling (not modulo), so the mapping from
raw stream to value is also implementation-independent.
"""

from __future__ import annotations

import hashlib

_MASK64 = (1 << 64) - 1
_GOLDEN_GAMMA = 0x9E3779B97F4A7C15
_MIX1 = 0xBF58476D1CE4E5B9
_MIX2 = 0x94D049BB133111EB

ALGORITHM = "splitmix64"


def derive_seed(seed: int, *parts: str) -> int:
    """Derive a 64-bit sub-seed from ``seed`` and string parts.

    Uses SHA-256 over an unambiguous encoding -- never Python's ``hash()``, which
    is randomised per process by PYTHONHASHSEED.
    """
    h = hashlib.sha256()
    h.update(str(int(seed)).encode("utf-8"))
    for part in parts:
        h.update(b"\x1f")
        h.update(part.encode("utf-8"))
    return int.from_bytes(h.digest()[:8], "big")


class SplitMix64:
    """Deterministic 64-bit generator. Not thread-safe by design (one per symbol)."""

    __slots__ = ("_state", "_draws", "_limits")

    def __init__(self, seed: int) -> None:
        self._state = seed & _MASK64
        self._draws = 0
        # Rejection limits per bound. The generator draws from only a handful of
        # fixed ranges per symbol, so this is a few entries that remove a
        # 128-bit divmod from every single draw.
        self._limits: dict[int, int] = {}

    @property
    def draws(self) -> int:
        """Number of raw 64-bit words consumed (for determinism diagnostics)."""
        return self._draws

    @property
    def state(self) -> int:
        return self._state

    def next_u64(self) -> int:
        self._draws += 1
        self._state = (self._state + _GOLDEN_GAMMA) & _MASK64
        z = self._state
        z = ((z ^ (z >> 30)) * _MIX1) & _MASK64
        z = ((z ^ (z >> 27)) * _MIX2) & _MASK64
        return z ^ (z >> 31)

    def below(self, n: int) -> int:
        """Uniform integer in ``[0, n)`` with no modulo bias."""
        if n <= 0:
            raise ValueError(f"below() requires n > 0, got {n}")
        if n == 1:
            # Still consume a word so the stream does not depend on the bound
            # collapsing to a single value (keeps datasets comparable).
            self.next_u64()
            return 0
        # Largest multiple of n that fits in 2**64; everything at or above it is
        # rejected so each residue class is equally likely.
        limit = self._limits.get(n)
        if limit is None:
            limit = (1 << 64) - ((1 << 64) % n)
            self._limits[n] = limit
        # The state update is inlined rather than calling next_u64(): this runs
        # three times per generated event, and the call overhead was measurable
        # in the profile. The arithmetic is identical, so the stream is identical.
        state = self._state
        draws = self._draws
        while True:
            state = (state + _GOLDEN_GAMMA) & _MASK64
            draws += 1
            z = state
            z = ((z ^ (z >> 30)) * _MIX1) & _MASK64
            z = ((z ^ (z >> 27)) * _MIX2) & _MASK64
            x = z ^ (z >> 31)
            if x < limit:
                self._state = state
                self._draws = draws
                return x % n

    def range_inclusive(self, lo: int, hi: int) -> int:
        """Uniform integer in ``[lo, hi]`` inclusive."""
        if hi < lo:
            raise ValueError(f"range_inclusive requires lo <= hi, got {lo}..{hi}")
        return lo + self.below(hi - lo + 1)
