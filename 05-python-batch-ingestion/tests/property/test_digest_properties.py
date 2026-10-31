"""Hypothesis properties of the canonical encoding."""

from __future__ import annotations

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from p05_ingest.canonical_digest import canonical_bytes, payload_digest
from p05_ingest.model import INT64_MAX, INT64_MIN, EventKind, LogicalProjection

pytestmark = pytest.mark.property

int64 = st.integers(min_value=INT64_MIN, max_value=INT64_MAX)
optional_int64 = st.one_of(st.none(), int64)
# Surrogates cannot be UTF-8 encoded, and the validator refuses them upstream.
text = st.text(max_size=40).filter(lambda s: _encodable(s))


def _encodable(value: str) -> bool:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


projections = st.builds(
    LogicalProjection,
    dataset_id=text,
    symbol=st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", max_size=12),
    sequence=int64,
    event_time_ns=int64,
    price_scale=int64,
    kind=st.sampled_from(list(EventKind)),
    source_schema_version=text,
    bid_ticks=optional_int64,
    ask_ticks=optional_int64,
    price_ticks=optional_int64,
)


@given(projections)
@settings(max_examples=300, deadline=None)
def test_digest_is_deterministic(projection):
    assert payload_digest(projection) == payload_digest(projection)


@given(projections)
@settings(max_examples=300, deadline=None)
def test_digest_is_always_64_lowercase_hex(projection):
    digest = payload_digest(projection)
    assert len(digest) == 64
    assert all(c in "0123456789abcdef" for c in digest)


@given(projections, projections)
@settings(max_examples=500, deadline=None)
def test_distinct_projections_give_distinct_digests(a, b):
    """No collisions over the generated space.

    A SHA-256 collision is not realistically findable, so a failure here means
    the *encoding* lost information -- two different projections serialising to
    the same bytes, which is the actual risk.
    """
    assume(a != b)
    assert payload_digest(a) != payload_digest(b)


@given(projections)
@settings(max_examples=300, deadline=None)
def test_equal_projections_give_equal_bytes(projection):
    """A separately constructed but equal projection encodes identically.

    Rebuilt field by field rather than reusing the object, so this exercises
    value equality rather than identity -- a replayed event arrives as a new
    object and must still digest the same.
    """
    from dataclasses import asdict

    rebuilt = LogicalProjection(**asdict(projection))
    assert rebuilt is not projection
    assert rebuilt == projection
    assert canonical_bytes(rebuilt) == canonical_bytes(projection)
    assert payload_digest(rebuilt) == payload_digest(projection)


@given(projections)
@settings(max_examples=200, deadline=None)
def test_canonical_bytes_are_self_delimiting(projection):
    """Every field boundary is recoverable, so no two fields can run together."""
    import struct

    raw = canonical_bytes(projection)
    cursor = len(b"P05-CORE-V1\x00")

    for expected in (
        projection.dataset_id,
        projection.symbol,
    ):
        (length,) = struct.unpack_from(">I", raw, cursor)
        cursor += 4
        assert raw[cursor : cursor + length] == expected.encode("utf-8")
        cursor += length
    for expected_int in (
        projection.sequence,
        projection.event_time_ns,
        projection.price_scale,
    ):
        (value,) = struct.unpack_from(">q", raw, cursor)
        assert value == expected_int
        cursor += 8
    (length,) = struct.unpack_from(">I", raw, cursor)
    cursor += 4
    assert raw[cursor : cursor + length] == projection.source_schema_version.encode()
    cursor += length
    assert raw[cursor] == int(projection.kind)
    cursor += 1
    for value in (projection.bid_ticks, projection.ask_ticks, projection.price_ticks):
        present = raw[cursor]
        cursor += 1
        if value is None:
            assert present == 0
        else:
            assert present == 1
            (decoded,) = struct.unpack_from(">q", raw, cursor)
            assert decoded == value
            cursor += 8
    assert cursor == len(raw)


@given(
    joined=st.text(alphabet="ABCXYZ", min_size=2, max_size=10),
    split_a=st.integers(min_value=0, max_value=10),
    split_b=st.integers(min_value=0, max_value=10),
)
@settings(max_examples=300, deadline=None)
def test_concatenation_ambiguity_is_impossible(joined, split_a, split_b):
    """``("ab","c")`` must not digest the same as ``("a","bc")``.

    Both pairs are built by splitting one string at two different points, so
    they always concatenate identically and the only difference is the field
    boundary. That is exactly what the length prefixes have to disambiguate.
    """
    cut_a = split_a % (len(joined) + 1)
    cut_b = split_b % (len(joined) + 1)
    assume(cut_a != cut_b)
    dataset_a, symbol_a = joined[:cut_a], joined[cut_a:]
    dataset_b, symbol_b = joined[:cut_b], joined[cut_b:]
    assert dataset_a + symbol_a == dataset_b + symbol_b

    def build(dataset_id, symbol):
        return LogicalProjection(
            dataset_id=dataset_id,
            symbol=symbol,
            sequence=1,
            event_time_ns=1,
            price_scale=1,
            kind=EventKind.TICK,
            source_schema_version="v",
            price_ticks=1,
        )

    assert payload_digest(build(dataset_a, symbol_a)) != payload_digest(
        build(dataset_b, symbol_b)
    )


@given(projections, st.integers(min_value=0, max_value=2**31))
@settings(max_examples=200, deadline=None)
def test_presence_never_collapses_to_a_value(projection, value):
    """An absent optional must never digest like the same field set to ``value``."""
    from dataclasses import replace

    for field in ("bid_ticks", "ask_ticks", "price_ticks"):
        absent = replace(projection, **{field: None})
        present = replace(projection, **{field: value})
        assert payload_digest(absent) != payload_digest(present)
