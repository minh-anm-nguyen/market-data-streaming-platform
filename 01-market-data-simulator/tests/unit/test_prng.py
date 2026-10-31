"""The PRNG is the root of determinism, so it gets pinned test vectors.

ADR-010: SplitMix64 rather than ``random.Random``, because Project 03 has to be
able to reproduce a dataset in C++ and the per-call consumption pattern of
CPython's Mersenne Twister is an implementation detail.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from market_sim.prng import ALGORITHM, SplitMix64, derive_seed

#: Reference SplitMix64 output for seed 0, as published with the algorithm.
#: Pinned as hex so these can be eyeballed against any other implementation --
#: the point is to prove this *is* SplitMix64, not merely that it is stable.
#: If these ever change, every recorded dataset changes, so GENERATOR_VERSION
#: must be bumped in the same commit.
SEED_0_REFERENCE_HEX = [
    "E220A8397B1DCDAF",
    "6E789E6AA1B965F4",
    "06C45D188009454F",
    "F88BB8A8724C81EC",
    "1B39896A51A8749B",
    "53CB9F0C747EA2EA",
    "2C829ABE1F4532E1",
    "C584133AC916AB3C",
]

_MASK64 = (1 << 64) - 1


def reference_splitmix64(seed: int, count: int) -> list[int]:
    """Independent transcription of the published algorithm, used as an oracle.

    Written out longhand on purpose: if the production implementation and this
    one agree, an inlining or caching optimisation in the former did not quietly
    change the stream.
    """
    state = seed & _MASK64
    out = []
    for _ in range(count):
        state = (state + 0x9E3779B97F4A7C15) & _MASK64
        z = state
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _MASK64
        out.append(z ^ (z >> 31))
    return out


class TestAlgorithmIdentity:
    def test_algorithm_is_named_in_the_module(self):
        assert ALGORITHM == "splitmix64"

    def test_matches_the_published_reference_vector(self):
        rng = SplitMix64(0)
        produced = [rng.next_u64() for _ in range(len(SEED_0_REFERENCE_HEX))]
        assert [f"{v:016X}" for v in produced] == SEED_0_REFERENCE_HEX

    @pytest.mark.parametrize("seed", [0, 1, 20261030, 2**63 - 1, 2**64 - 1])
    def test_agrees_with_an_independent_implementation(self, seed):
        rng = SplitMix64(seed)
        assert [rng.next_u64() for _ in range(200)] == reference_splitmix64(seed, 200)

    def test_all_outputs_are_64_bit(self):
        rng = SplitMix64(0xDEADBEEF)
        assert all(0 <= rng.next_u64() < 2**64 for _ in range(1000))

    def test_same_seed_same_stream(self):
        a, b = SplitMix64(12345), SplitMix64(12345)
        assert [a.next_u64() for _ in range(100)] == [b.next_u64() for _ in range(100)]

    def test_adjacent_seeds_diverge_immediately(self):
        a, b = SplitMix64(12345), SplitMix64(12346)
        assert a.next_u64() != b.next_u64()


class TestSeedDerivation:
    def test_pinned_derivation(self):
        # SHA-256 based, so this is stable across runtimes and languages.
        assert derive_seed(20261030, "EURUSD") == 2794303133001796275

    def test_different_symbols_get_different_streams(self):
        seeds = {derive_seed(1, s) for s in ("EURUSD", "GBPUSD", "USDJPY")}
        assert len(seeds) == 3

    def test_separator_prevents_concatenation_collisions(self):
        # Without a delimiter, ("AB", "C") and ("A", "BC") would collide.
        assert derive_seed(1, "AB", "C") != derive_seed(1, "A", "BC")

    def test_is_not_affected_by_python_hash_randomisation(self):
        # The whole point of not using hash(symbol): PYTHONHASHSEED randomises it
        # per process, which would make datasets irreproducible across runs.
        code = (
            "import sys; sys.path.insert(0, %r);"
            "from market_sim.prng import derive_seed;"
            "print(derive_seed(20261030, 'EURUSD'))"
        ) % str((__import__("pathlib").Path(__file__).resolve().parents[2] / "src"))
        outputs = set()
        for hash_seed in ("0", "1", "random"):
            env = {**os.environ, "PYTHONHASHSEED": hash_seed}
            result = subprocess.run(
                [sys.executable, "-c", code], capture_output=True, text=True, env=env
            )
            assert result.returncode == 0, result.stderr
            outputs.add(result.stdout.strip())
        assert outputs == {"2794303133001796275"}


class TestBoundedDraws:
    @pytest.mark.parametrize("n", [1, 2, 3, 5, 100, 999, 2**32, 2**63])
    def test_below_stays_in_range(self, n):
        rng = SplitMix64(derive_seed(7, "range"))
        assert all(0 <= rng.below(n) < n for _ in range(500))

    @pytest.mark.parametrize("n", [0, -1, -100])
    def test_below_rejects_non_positive(self, n):
        with pytest.raises(ValueError):
            SplitMix64(1).below(n)

    def test_below_one_still_consumes_a_word(self):
        # A bound collapsing to one value must not shift the stream, or changing
        # a symbol's spread range to a single value would change everything after.
        rng = SplitMix64(1)
        rng.below(1)
        assert rng.draws == 1

    def test_range_inclusive_includes_both_ends(self):
        rng = SplitMix64(derive_seed(7, "incl"))
        seen = {rng.range_inclusive(-2, 2) for _ in range(500)}
        assert seen == {-2, -1, 0, 1, 2}

    def test_range_inclusive_single_value(self):
        rng = SplitMix64(1)
        assert rng.range_inclusive(5, 5) == 5

    def test_range_inclusive_rejects_inverted_bounds(self):
        with pytest.raises(ValueError):
            SplitMix64(1).range_inclusive(5, 4)

    def test_distribution_is_roughly_uniform(self):
        # Not a statistical proof -- a smoke check that rejection sampling did
        # not introduce a gross bias. 60k draws over 6 buckets: expect 10k each.
        rng = SplitMix64(derive_seed(99, "uniform"))
        counts = [0] * 6
        for _ in range(60_000):
            counts[rng.below(6)] += 1
        assert all(9_400 < c < 10_600 for c in counts), counts

    def test_draw_counter_tracks_consumption(self):
        rng = SplitMix64(1)
        for _ in range(10):
            rng.range_inclusive(0, 9)
        # At least one word per draw; more only when a value was rejected.
        assert rng.draws >= 10

    def test_limit_cache_does_not_change_the_stream(self):
        # The cache is a speed optimisation; cached and uncached must agree.
        a = SplitMix64(4242)
        b = SplitMix64(4242)
        mixed = [a.below(100) if i % 2 else a.below(7) for i in range(200)]
        plain = [b.below(100) if i % 2 else b.below(7) for i in range(200)]
        assert mixed == plain
