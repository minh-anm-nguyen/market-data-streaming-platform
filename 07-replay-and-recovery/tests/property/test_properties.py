"""Property-based checks for the invariants that must hold over *any* input.

Design 12.1 lists eighteen system invariants. The ones reachable without a real
broker are checked here over generated inputs rather than hand-picked examples,
because example-based tests can only assert what their author thought of.
"""

from __future__ import annotations

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from p07.contracts import EventKind, SemanticCore, SourceIdentity, partition_for_symbol
from p07.dataplane.adapter import Adapter, SymbolLimits, chunked
from p07.dataplane.codec import DEFAULT_CODEC, frame_mdp1
from p07.oracle.reference_bars import reference_bars
from p07.oracle.semantic_digest import digest_core, encode_core, reference_encode_core
from p07.reconcile.diff import count_physical, diff_identity_maps, shard_of, sharded_diff

SYMBOLS = ("EURUSD", "GBPUSD", "USDJPY")

LIMITS = {
    symbol: SymbolLimits(
        price_scale=100000,
        min_price=1,
        max_price=10**9,
        min_spread=0,
        max_spread=10**6,
        max_step=10**9,
    )
    for symbol in SYMBOLS
}

# Wide enough to exercise the int64 boundaries the digest encodes, but inside
# the signed range so a generated core is always encodable.
INT64 = st.integers(min_value=-(2**63), max_value=2**63 - 1)
PRICES = st.integers(min_value=1, max_value=10**6)


@st.composite
def cores(draw: st.DrawFn) -> SemanticCore:
    """Business-valid cores, for the components that validate them."""
    kind = draw(st.sampled_from(list(EventKind)))
    bid = draw(PRICES) if kind is EventKind.QUOTE else None
    spread = draw(st.integers(min_value=0, max_value=50)) if kind is EventKind.QUOTE else 0
    return SemanticCore(
        dataset_id=draw(st.sampled_from(["ds-1", "ds-2"])),
        symbol=draw(st.sampled_from(SYMBOLS)),
        sequence=draw(st.integers(min_value=1, max_value=10**6)),
        event_time_ns=draw(st.integers(min_value=0, max_value=10**15)),
        price_scale=100000,
        event_kind=kind,
        source_schema_version="sim.v1",
        bid_ticks=bid,
        ask_ticks=None if bid is None else bid + spread,
        price_ticks=draw(PRICES) if kind is EventKind.TICK else None,
    )


@st.composite
def presence_cores(draw: st.DrawFn) -> SemanticCore:
    """Cores including the absent/zero combinations the digest must separate."""
    return SemanticCore(
        dataset_id=draw(st.text(min_size=0, max_size=8)),
        symbol=draw(st.text(min_size=0, max_size=8)),
        sequence=draw(INT64),
        event_time_ns=draw(INT64),
        price_scale=draw(INT64),
        event_kind=draw(st.sampled_from(list(EventKind))),
        source_schema_version=draw(st.text(min_size=0, max_size=8)),
        bid_ticks=draw(st.one_of(st.none(), INT64)),
        ask_ticks=draw(st.one_of(st.none(), INT64)),
        price_ticks=draw(st.one_of(st.none(), INT64)),
    )


def tick_core(sequence: int, event_time_ns: int, price: int) -> SemanticCore:
    return SemanticCore(
        dataset_id="ds",
        symbol="EURUSD",
        sequence=sequence,
        event_time_ns=event_time_ns,
        price_scale=100000,
        event_kind=EventKind.TICK,
        source_schema_version="sim.v1",
        bid_ticks=None,
        ask_ticks=None,
        price_ticks=price,
    )


# -- the digest ------------------------------------------------------------


@given(presence_cores())
@settings(max_examples=200, deadline=None)
def test_the_two_digest_implementations_never_disagree(core: SemanticCore) -> None:
    assert encode_core(core) == reference_encode_core(core)


@given(presence_cores(), presence_cores())
@settings(max_examples=300, deadline=None)
def test_equal_digests_imply_equal_cores(left: SemanticCore, right: SemanticCore) -> None:
    """``INV-02``: the digest must not collapse two different events.

    Strictly this could fail on a SHA-256 collision, which is the point: the
    property asserts the *encoding* is injective, so any failure here is an
    encoding bug rather than a hash accident.
    """
    if digest_core(left) == digest_core(right):
        assert left == right


@given(presence_cores())
@settings(max_examples=100, deadline=None)
def test_the_digest_is_stable_and_fixed_width(core: SemanticCore) -> None:
    assert digest_core(core) == digest_core(core)
    assert len(digest_core(core)) == 64


# -- framing ---------------------------------------------------------------


@given(
    events=st.lists(cores(), min_size=1, max_size=12, unique_by=lambda core: core.identity),
    sizes=st.lists(st.integers(min_value=1, max_value=200), min_size=1, max_size=4),
)
@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_the_published_set_is_independent_of_segmentation(
    events: list[SemanticCore], sizes: list[int]
) -> None:
    """Any TCP chunking must yield the same accepted set (case ``E2E-04``).

    The baseline is the single-shot feed, so this compares two runs of the real
    adapter rather than against a hand-computed expectation.
    """
    dataset_id = events[0].dataset_id

    def run(chunk_sizes: list[int]) -> list[SemanticCore]:
        published: list[SemanticCore] = []
        adapter = Adapter(dataset_id=dataset_id, limits=LIMITS, sink=published.append)
        for core in events:
            if core.dataset_id != dataset_id:
                continue
            for chunk in chunked(frame_mdp1(DEFAULT_CODEC.encode(core)), chunk_sizes):
                adapter.feed(chunk)
        adapter.close()
        return published

    assert run(sizes) == run([4096])


@given(core=cores())
@settings(max_examples=100, deadline=None)
def test_the_codec_round_trips_every_core(core: SemanticCore) -> None:
    assert DEFAULT_CODEC.decode(DEFAULT_CODEC.encode(core)) == core


# -- bars ------------------------------------------------------------------


@given(
    ticks=st.lists(
        st.tuples(
            st.integers(min_value=1, max_value=50),
            st.integers(min_value=0, max_value=200_000_000_000),
            PRICES,
        ),
        min_size=1,
        max_size=30,
        unique_by=lambda item: item[0],
    ),
    rotation=st.integers(min_value=0, max_value=29),
)
@settings(max_examples=100, deadline=None)
def test_bars_do_not_depend_on_input_order(
    ticks: list[tuple[int, int, int]], rotation: int
) -> None:
    cores_list = [tick_core(*item) for item in ticks]
    split = rotation % len(cores_list)
    rotated = cores_list[split:] + cores_list[:split]
    assert reference_bars(cores_list) == reference_bars(rotated)


@given(
    ticks=st.lists(
        st.tuples(st.integers(1, 40), st.integers(0, 200_000_000_000), PRICES),
        min_size=1,
        max_size=40,
        unique_by=lambda item: item[0],
    )
)
@settings(max_examples=100, deadline=None)
def test_tick_counts_sum_to_the_input_size(ticks: list[tuple[int, int, int]]) -> None:
    """``INV-09``: every accepted tick counted exactly once, in exactly one bucket."""
    cores_list = [tick_core(*item) for item in ticks]
    bars = reference_bars(cores_list)
    assert sum(bar.tick_count for bar in bars.values()) == len(cores_list)
    for bar in bars.values():
        assert bar.low_ticks <= bar.open_ticks <= bar.high_ticks
        assert bar.low_ticks <= bar.close_ticks <= bar.high_ticks


# -- diffs -----------------------------------------------------------------


@given(
    expected=st.lists(cores(), min_size=0, max_size=25, unique_by=lambda core: core.identity),
    observed=st.lists(cores(), min_size=0, max_size=25, unique_by=lambda core: core.identity),
)
@settings(max_examples=150, deadline=None)
def test_the_sharded_and_in_memory_diffs_always_agree(
    expected: list[SemanticCore], observed: list[SemanticCore]
) -> None:
    """The bounded-memory path may not disagree with the simple one."""
    expected_map = {core.identity: core for core in expected}
    observed_map = {core.identity: core for core in observed}
    in_memory = diff_identity_maps(
        expected_map, observed_map, physical_counts=count_physical(observed_map)
    )
    sharded = sharded_diff(
        lambda: [(core.identity, core) for core in expected_map.values()],
        lambda: [(core.identity, core) for core in observed_map.values()],
        shards=5,
    )
    assert set(sharded.missing) == set(in_memory.missing)
    assert set(sharded.extra) == set(in_memory.extra)
    assert {identity for identity, _ in sharded.mismatched} == {
        identity for identity, _ in in_memory.mismatched
    }
    assert sharded.equal == in_memory.equal


@given(expected=st.lists(cores(), min_size=1, max_size=20, unique_by=lambda core: core.identity))
@settings(max_examples=100, deadline=None)
def test_a_map_compared_with_itself_is_always_equal(expected: list[SemanticCore]) -> None:
    expected_map = {core.identity: core for core in expected}
    assert diff_identity_maps(expected_map, dict(expected_map)).equal


@given(
    identity_parts=st.tuples(
        st.text(min_size=1, max_size=6), st.text(min_size=1, max_size=6), st.integers(0, 10**9)
    ),
    shards=st.integers(min_value=1, max_value=64),
)
@settings(max_examples=200, deadline=None)
def test_shard_assignment_is_in_range_and_deterministic(
    identity_parts: tuple[str, str, int], shards: int
) -> None:
    identity = SourceIdentity(*identity_parts)
    first = shard_of(identity, shards)
    assert 0 <= first < shards
    assert shard_of(identity, shards) == first


@given(symbol=st.text(min_size=1, max_size=12), count=st.integers(min_value=1, max_value=64))
@settings(max_examples=200, deadline=None)
def test_the_partitioner_is_total_and_stable(symbol: str, count: int) -> None:
    partition = partition_for_symbol(symbol, count)
    assert 0 <= partition < count
    assert partition_for_symbol(symbol, count) == partition
