"""The ``P05-CORE-V1`` encoding: two implementations, and what must change it."""

from __future__ import annotations

import pytest

from p07.contracts import EventKind, SemanticCore
from p07.oracle.semantic_digest import (
    DIGEST_VERSION,
    DigestError,
    digest_core,
    encode_core,
    reference_digest_core,
    reference_encode_core,
)


def tick(**overrides: object) -> SemanticCore:
    base = dict(
        dataset_id="ds-1",
        symbol="EURUSD",
        sequence=7,
        event_time_ns=1_700_000_000_000_000_000,
        price_scale=100000,
        event_kind=EventKind.TICK,
        source_schema_version="sim.v1",
        bid_ticks=None,
        ask_ticks=None,
        price_ticks=108347,
    )
    base.update(overrides)
    return SemanticCore(**base)  # type: ignore[arg-type]


def quote(**overrides: object) -> SemanticCore:
    fields: dict[str, object] = {
        "event_kind": EventKind.QUOTE,
        "bid_ticks": 108340,
        "ask_ticks": 108345,
        "price_ticks": None,
    }
    fields.update(overrides)
    return tick(**fields)


SHAPES = [
    tick(),
    quote(),
    tick(price_ticks=0),
    tick(price_ticks=-1),
    quote(bid_ticks=-5, ask_ticks=0),
    tick(sequence=1, event_time_ns=0, price_scale=1),
    tick(symbol="USDJPY", price_scale=1000, price_ticks=151234),
    tick(event_time_ns=-1),
    tick(price_ticks=2**62),
]


@pytest.mark.whatif("P07-G03-09")
@pytest.mark.parametrize("core", SHAPES, ids=lambda core: f"{core.event_kind.wire_name}-{core.sequence}")
def test_two_implementations_agree(core: SemanticCore) -> None:
    """The fast path and the from-the-spec reference must produce equal bytes.

    This is the only thing protecting the digest from a silent redefinition: if
    someone reorders a field in the fast encoder, every digest changes and no
    other test would notice, because both sides of every other comparison would
    change together.
    """
    assert encode_core(core) == reference_encode_core(core)
    assert digest_core(core) == reference_digest_core(core)


def test_encoding_starts_with_the_version_prefix() -> None:
    blob = encode_core(tick())
    assert blob.startswith(DIGEST_VERSION.encode("ascii") + b"\x00")


@pytest.mark.whatif("P07-G03-01", "P07-G03-10")
def test_presence_is_part_of_the_identity() -> None:
    """``price_ticks=0`` present and ``price_ticks`` absent are different events."""
    present = tick(price_ticks=0)
    absent = tick(price_ticks=None)
    assert present != absent
    assert digest_core(present) != digest_core(absent)
    # And the difference is visible in the encoding's presence byte, not just in
    # the hash -- so a reader can see *why* they differ.
    assert encode_core(present)[-9] == 1
    assert encode_core(absent)[-1] == 0


@pytest.mark.whatif("P07-G03-10")
def test_quote_presence_variants_differ() -> None:
    both = quote()
    bid_only = quote(ask_ticks=None)
    assert digest_core(both) != digest_core(bid_only)


def test_every_field_change_changes_the_digest() -> None:
    base = quote()
    variants = [
        base._replace(dataset_id="ds-2"),
        base._replace(symbol="GBPUSD"),
        base._replace(sequence=8),
        base._replace(event_time_ns=base.event_time_ns + 1),
        base._replace(price_scale=1000),
        base._replace(source_schema_version="sim.v2"),
        base._replace(bid_ticks=base.bid_ticks + 1),
        base._replace(ask_ticks=base.ask_ticks + 1),
    ]
    digests = {digest_core(base)} | {digest_core(variant) for variant in variants}
    assert len(digests) == len(variants) + 1


def test_boolean_is_not_an_integer() -> None:
    """``True`` must not encode as 1: a malformed record would look well-formed."""
    with pytest.raises(DigestError) as exc:
        encode_core(tick(sequence=True))
    assert exc.value.reason == "field_not_integer"


def test_out_of_range_integer_is_refused_not_wrapped() -> None:
    with pytest.raises(DigestError) as exc:
        encode_core(tick(price_ticks=2**63))
    assert exc.value.reason == "integer_out_of_range"
    # And the reference implementation agrees, so neither path silently wraps.
    with pytest.raises(DigestError):
        reference_encode_core(tick(price_ticks=2**63))


def test_non_string_dataset_id_is_refused() -> None:
    with pytest.raises(DigestError) as exc:
        encode_core(tick(dataset_id=123))
    assert exc.value.reason == "field_not_string"


def test_unicode_symbol_length_is_in_bytes_not_characters() -> None:
    """The length prefix counts UTF-8 bytes, so a multi-byte name cannot alias.

    P03 restricts symbols to ASCII, but the digest must not depend on that: if
    the prefix counted characters, two different names could share an encoding.
    """
    ascii_core = tick(symbol="ABCD")
    wide_core = tick(symbol="AéC")  # 4 UTF-8 bytes, 3 characters
    assert encode_core(ascii_core)[:40] != encode_core(wide_core)[:40]
    assert digest_core(ascii_core) != digest_core(wide_core)
