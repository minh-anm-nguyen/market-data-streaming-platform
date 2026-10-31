"""P05-CORE-V1 encoding: layout, distinctness and stability."""

from __future__ import annotations

import hashlib
import struct
import subprocess
import sys

import pytest

from p05_ingest.canonical_digest import (
    CANONICAL_ENCODING_VERSION,
    DOMAIN_SEPARATOR,
    batch_id,
    batch_insert_token,
    canonical_bytes,
    payload_digest,
)
from p05_ingest.errors import InvariantViolation
from p05_ingest.model import INT64_MAX, INT64_MIN, EventKind, LogicalProjection


def proj(**kwargs) -> LogicalProjection:
    base = {
        "dataset_id": "demo-a1b2c3",
        "symbol": "EURUSD",
        "sequence": 7,
        "event_time_ns": 1_704_171_845_123_456_789,
        "price_scale": 100_000,
        "kind": EventKind.TICK,
        "source_schema_version": "marketdata.v1",
        "price_ticks": 109_385,
    }
    base.update(kwargs)
    return LogicalProjection(**base)


# --------------------------------------------------------------------- layout


def test_encoding_matches_the_specified_byte_layout():
    """Decode the bytes by hand and check every field lands where the spec says.

    Asserting against an independently written decoder rather than a golden
    blob: a golden blob generated from this same encoder would agree with any
    bug it contains.
    """
    p = proj()
    raw = canonical_bytes(p)

    cursor = 0
    assert raw[: len(DOMAIN_SEPARATOR)] == DOMAIN_SEPARATOR
    cursor += len(DOMAIN_SEPARATOR)

    def read_string():
        nonlocal cursor
        (length,) = struct.unpack_from(">I", raw, cursor)
        cursor += 4
        value = raw[cursor : cursor + length].decode("utf-8")
        cursor += length
        return value

    def read_i64():
        nonlocal cursor
        (value,) = struct.unpack_from(">q", raw, cursor)
        cursor += 8
        return value

    def read_u8():
        nonlocal cursor
        (value,) = struct.unpack_from(">B", raw, cursor)
        cursor += 1
        return value

    assert read_string() == p.dataset_id
    assert read_string() == p.symbol
    assert read_i64() == p.sequence
    assert read_i64() == p.event_time_ns
    assert read_i64() == p.price_scale
    assert read_string() == p.source_schema_version
    assert read_u8() == int(EventKind.TICK) == 1

    # bid, ask, price -- in that fixed order, each with a presence byte.
    assert read_u8() == 0, "bid_ticks must be marked absent"
    assert read_u8() == 0, "ask_ticks must be marked absent"
    assert read_u8() == 1, "price_ticks must be marked present"
    assert read_i64() == p.price_ticks
    assert cursor == len(raw), "no trailing bytes"


def test_digest_is_lowercase_hex_sha256_of_canonical_bytes():
    p = proj()
    digest = payload_digest(p)
    assert digest == hashlib.sha256(canonical_bytes(p)).hexdigest()
    assert len(digest) == 64
    assert digest == digest.lower()
    assert all(c in "0123456789abcdef" for c in digest)


def test_domain_separator_is_versioned():
    assert DOMAIN_SEPARATOR == CANONICAL_ENCODING_VERSION.encode("ascii") + b"\x00"


# ---------------------------------------------------------------- distinctness


def test_explicit_zero_and_absent_field_differ():
    """The whole reason presence bytes exist (spec 3.2 golden corpus)."""
    present_zero = proj(kind=EventKind.QUOTE, price_ticks=None, bid_ticks=0, ask_ticks=0)
    absent_bid = proj(
        kind=EventKind.QUOTE, price_ticks=None, bid_ticks=None, ask_ticks=0
    )
    assert payload_digest(present_zero) != payload_digest(absent_bid)


def test_event_kind_changes_the_digest():
    """Same identity, different kind, is a conflict -- so it must not collide."""
    tick = proj(kind=EventKind.TICK, price_ticks=5)
    quote = proj(kind=EventKind.QUOTE, price_ticks=5)
    assert payload_digest(tick) != payload_digest(quote)


@pytest.mark.parametrize(
    "field,value",
    [
        ("dataset_id", "demo-other"),
        ("symbol", "GBPUSD"),
        ("sequence", 8),
        ("event_time_ns", 1_704_171_845_123_456_790),
        ("price_scale", 100_001),
        ("source_schema_version", "marketdata.v2"),
        ("price_ticks", 109_386),
    ],
)
def test_every_semantic_field_affects_the_digest(field, value):
    assert payload_digest(proj()) != payload_digest(proj(**{field: value}))


def test_length_prefix_prevents_field_boundary_ambiguity():
    """Without length prefixes, "ab"+"c" and "a"+"bc" would collide."""
    a = proj(dataset_id="ab", symbol="C")
    b = proj(dataset_id="a", symbol="BC")
    assert payload_digest(a) != payload_digest(b)


def test_multibyte_dataset_id_counts_bytes_not_characters():
    p = proj(dataset_id="dé中")
    raw = canonical_bytes(p)
    (length,) = struct.unpack_from(">I", raw, len(DOMAIN_SEPARATOR))
    assert length == len("dé中".encode("utf-8")) == 6


# --------------------------------------------------------------- transport


def test_transport_coordinates_do_not_affect_the_digest():
    """A replayed event at a new offset must digest identically (spec 3.2)."""
    from p05_ingest.model import EventRow, TransportCoordinate

    p = proj()
    row_a = EventRow(
        projection=p,
        coordinate=TransportCoordinate("c1", "gen-a", "md.ticks.v1", 0, 10),
        payload_digest=payload_digest(p),
        ingest_run_id="run-1",
    )
    row_b = EventRow(
        projection=p,
        coordinate=TransportCoordinate("c2", "gen-b", "md.ticks.v1", 3, 9999),
        payload_digest=payload_digest(p),
        ingest_run_id="run-2",
    )
    assert row_a.coordinate != row_b.coordinate
    assert row_a.ingest_run_id != row_b.ingest_run_id
    assert row_a.payload_digest == row_b.payload_digest


# --------------------------------------------------------------- range checks


@pytest.mark.parametrize("sequence", [INT64_MIN, INT64_MAX, 0, -1])
def test_int64_boundaries_encode(sequence):
    assert len(payload_digest(proj(sequence=sequence))) == 64


@pytest.mark.parametrize("sequence", [INT64_MAX + 1, INT64_MIN - 1, 2**70])
def test_values_outside_int64_are_refused_not_wrapped(sequence):
    with pytest.raises(InvariantViolation, match="outside int64 range"):
        canonical_bytes(proj(sequence=sequence))


def test_bool_is_refused_as_an_integer_field():
    """``True`` is an int in Python; encoding it silently would be a bug."""
    with pytest.raises(InvariantViolation, match="must be int"):
        canonical_bytes(proj(sequence=True))


def test_non_string_dataset_id_is_refused():
    with pytest.raises(InvariantViolation, match="must be str"):
        canonical_bytes(proj(dataset_id=12345))


# ----------------------------------------------------------------- stability


def test_digest_is_stable_across_processes_with_different_hash_seeds():
    """Guards against anyone reaching for ``hash()`` in the digest path.

    ``hash()`` of a str is salted per process by PYTHONHASHSEED, so a digest
    built on it would differ between runs. Two subprocesses with different
    seeds must still agree.
    """
    code = (
        "from p05_ingest.canonical_digest import payload_digest;"
        "from p05_ingest.model import EventKind, LogicalProjection;"
        "print(payload_digest(LogicalProjection("
        "dataset_id='demo-a1b2c3', symbol='EURUSD', sequence=7,"
        "event_time_ns=1704171845123456789, price_scale=100000,"
        "kind=EventKind.TICK, source_schema_version='marketdata.v1',"
        "price_ticks=109385)))"
    )
    import os

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    src = os.path.join(root, "src")
    digests = set()
    for seed in ("0", "1", "12345"):
        env = dict(os.environ, PYTHONHASHSEED=seed, PYTHONPATH=src)
        out = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, env=env, check=True
        )
        digests.add(out.stdout.strip())
    assert len(digests) == 1, f"digest varied across hash seeds: {digests}"
    assert digests.pop() == payload_digest(proj())


# ---------------------------------------------------------------- batch token


def _token(**kwargs) -> str:
    base = {
        "topic_generation": "gen-a",
        "topic": "md.ticks.v1",
        "partition": 0,
        "coordinates_and_digests": ((10, "a" * 64), (11, "b" * 64)),
        "table_fqn": "md_ingest.market_events_raw",
        "schema_version": "raw-v1",
        "insert_mode": "sync",
    }
    base.update(kwargs)
    return batch_insert_token(**base)


def test_batch_token_is_deterministic():
    assert _token() == _token()


@pytest.mark.parametrize(
    "field,value",
    [
        ("topic_generation", "gen-b"),
        ("topic", "md.quotes.v1"),
        ("partition", 1),
        ("table_fqn", "other.table"),
        ("schema_version", "raw-v2"),
        ("insert_mode", "async_wait"),
        ("coordinates_and_digests", ((10, "a" * 64), (12, "b" * 64))),
    ],
)
def test_batch_token_depends_on_every_component(field, value):
    assert _token() != _token(**{field: value})


def test_batch_token_depends_on_payload_not_only_offsets():
    """Spec 6.2: a token derived from offsets alone would let ClickHouse
    de-duplicate away a genuinely different batch covering the same range."""
    same_offsets_new_payload = _token(
        coordinates_and_digests=((10, "c" * 64), (11, "d" * 64))
    )
    assert _token() != same_offsets_new_payload


def test_batch_token_rejects_a_malformed_digest():
    with pytest.raises(InvariantViolation, match="64 hex chars"):
        _token(coordinates_and_digests=((10, "short"),))


def test_batch_id_is_readable_and_carries_the_token_prefix():
    token = _token()
    label = batch_id(topic="md.ticks.v1", partition=3, insert_token=token)
    assert label.startswith("md.ticks.v1:3:")
    assert token.startswith(label.split(":")[-1])
