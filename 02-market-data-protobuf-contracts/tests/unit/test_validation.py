"""Business policy and fault passthrough: what-if groups C18 and F.

The single most important property in this file: an anomaly is either rejected
or preserved *exactly*. There is no third outcome where the codec repairs a
value and reports success. Several tests below assert the preserved value is
bit-identical rather than merely "still negative", because a clamp to -1 or a
sign flip would satisfy a weaker assertion.
"""

from __future__ import annotations

import pytest

from md_contract.codec import decode_payload, encode_event
from md_contract.domain import INT64_MAX, INT64_MIN, PAYLOAD_QUOTE, PAYLOAD_TICK, DomainEvent
from md_contract.errors import PolicyError, Reason
from md_contract.source import ANOMALY_TO_FAULT_KINDS, FaultIndex
from md_contract.validation import (
    ANOMALY_CROSSED_QUOTE,
    ANOMALY_NONPOSITIVE_PRICE,
    ANOMALY_NONPOSITIVE_SCALE,
    ANOMALY_NONPOSITIVE_SEQUENCE,
    ANOMALY_NONPOSITIVE_TIME,
    ValidationMode,
    find_anomalies,
    validate_business,
)

STRICT = ValidationMode.STRICT_NORMAL
PASSTHROUGH = ValidationMode.FAULT_PASSTHROUGH


def event(**overrides: object) -> DomainEvent:
    base = dict(
        dataset_id="ds-test",
        symbol="EURUSD",
        sequence=10,
        event_time_ns=1793322000000000000,
        price_scale=100000,
        source_schema_version="sim.v1",
        payload_case=PAYLOAD_QUOTE,
        bid_ticks=108345,
        ask_ticks=108348,
    )
    base.update(overrides)
    return DomainEvent(**base)  # type: ignore[arg-type]


def fault_index(kind: str, symbol: str = "EURUSD", sequence: int = 10) -> FaultIndex:
    return FaultIndex.from_lines(
        [
            '{"fault_id":"f0001","kind":"%s","applied":true,"channel":"payload",'
            '"target":{"symbol":"%s","sequence":%d},"affected_sequences":[%d],'
            '"output_impact":"test","planned":true}'
            % (kind, symbol, sequence, sequence)
        ]
    )


def reject_reason(ev: DomainEvent, **kwargs: object) -> Reason:
    with pytest.raises(PolicyError) as excinfo:
        validate_business(ev, **kwargs)  # type: ignore[arg-type]
    return excinfo.value.reason


# ---------------------------------------------------------------------------
# strict_normal
# ---------------------------------------------------------------------------


def test_a_normal_event_is_accepted_with_no_anomalies() -> None:
    verdict = validate_business(event(), mode=STRICT)
    assert verdict.accepted
    assert verdict.anomalies == ()
    assert verdict.preserved_faults == ()


@pytest.mark.parametrize(
    ("overrides", "anomaly"),
    [
        ({"bid_ticks": -1, "ask_ticks": -1}, ANOMALY_NONPOSITIVE_PRICE),
        ({"bid_ticks": 0, "ask_ticks": 0}, ANOMALY_NONPOSITIVE_PRICE),
        ({"bid_ticks": 200, "ask_ticks": 100}, ANOMALY_CROSSED_QUOTE),
        ({"sequence": 0}, ANOMALY_NONPOSITIVE_SEQUENCE),
        ({"sequence": -5}, ANOMALY_NONPOSITIVE_SEQUENCE),
        ({"event_time_ns": 0}, ANOMALY_NONPOSITIVE_TIME),
        ({"price_scale": 0}, ANOMALY_NONPOSITIVE_SCALE),
        ({"price_scale": -100}, ANOMALY_NONPOSITIVE_SCALE),
    ],
)
@pytest.mark.cases("B02", "B04", "F07", "F08")
def test_strict_mode_rejects_each_business_violation(
    overrides: dict[str, object], anomaly: str
) -> None:
    ev = event(**overrides)
    assert anomaly in {a.kind for a in find_anomalies(ev)}
    assert reject_reason(ev, mode=STRICT) is Reason.BUSINESS_VIOLATION


@pytest.mark.cases("F07")
def test_crossed_quote_is_detected_but_an_equal_bid_ask_is_not() -> None:
    # A zero spread is unusual but not a contract violation; only bid > ask is.
    assert find_anomalies(event(bid_ticks=100, ask_ticks=100)) == ()
    assert validate_business(event(bid_ticks=100, ask_ticks=100), mode=STRICT).accepted
    assert find_anomalies(event(bid_ticks=101, ask_ticks=100))


def test_all_violations_are_reported_not_just_the_first() -> None:
    # fault_passthrough has to confirm each anomaly separately, so the finder
    # must not stop at the first one it sees.
    ev = event(sequence=-1, price_scale=0, bid_ticks=-5, ask_ticks=-9)
    kinds = {a.kind for a in find_anomalies(ev)}
    assert kinds == {
        ANOMALY_NONPOSITIVE_SEQUENCE,
        ANOMALY_NONPOSITIVE_SCALE,
        ANOMALY_NONPOSITIVE_PRICE,
        ANOMALY_CROSSED_QUOTE,
    }


@pytest.mark.cases("F01", "F02", "F03")
def test_sequence_gaps_duplicates_and_reordering_are_not_per_event_violations() -> None:
    """Stream properties are Project 03's job, not this codec's.

    A codec that rejected a gap per-event could not forward an intentionally
    gapped dataset at all, and one that "filled in" the gap would be fabricating
    market data. So both modes pass these through untouched (section 3.3).
    """
    # A gap: sequence jumps 10 -> 20. Each event on its own is perfectly valid.
    for sequence in (10, 20):
        assert validate_business(event(sequence=sequence), mode=STRICT).accepted
    # A duplicate: the same (symbol, sequence) twice, identical payloads.
    first = event(sequence=10)
    duplicate = event(sequence=10)
    assert first.event_id == duplicate.event_id
    assert validate_business(duplicate, mode=STRICT).accepted
    # Out of order: 11 then 10.
    assert validate_business(event(sequence=11), mode=STRICT).accepted
    assert validate_business(event(sequence=10), mode=STRICT).accepted


@pytest.mark.cases("F05", "F06")
def test_a_stale_or_future_timestamp_is_not_judged_against_the_wall_clock() -> None:
    # Replaying a historical dataset is normal, and a codec that compared
    # event_time_ns against its own clock would reject every replay. Staleness
    # is a receiver-side policy with a logical reference, not a codec rule.
    long_past = event(event_time_ns=1)
    far_future = event(event_time_ns=INT64_MAX)
    assert validate_business(long_past, mode=STRICT).accepted
    assert validate_business(far_future, mode=STRICT).accepted


@pytest.mark.cases("F10", "F11")
def test_scale_and_dataset_differences_are_carried_not_normalised() -> None:
    # Two events of the same symbol with different scales: the codec preserves
    # both rather than rescaling either. Auto-scaling would silently change a
    # price by orders of magnitude.
    a = decode_payload(encode_event(event(price_scale=100000, bid_ticks=108345)))
    b = decode_payload(encode_event(event(price_scale=1000, bid_ticks=108345)))
    assert a.price_scale == 100000 and b.price_scale == 1000
    assert a.bid_ticks == b.bid_ticks == 108345
    # The decimal interpretation differs by 100x, which is exactly why changing
    # a scale is a forbidden schema change without a migration (case A12).
    assert a.display_price()["bid"] != b.display_price()["bid"]

    # A differing dataset_id keeps the identity tuples distinct, so two logical
    # datasets cannot be merged by accident.
    assert event(dataset_id="ds-a").event_id != event(dataset_id="ds-b").event_id


@pytest.mark.cases("F04")
def test_identity_includes_the_symbol_so_sequences_do_not_collide() -> None:
    eur = event(symbol="EURUSD", sequence=100)
    jpy = event(symbol="USDJPY", sequence=100)
    assert eur.event_id != jpy.event_id


# ---------------------------------------------------------------------------
# fault_passthrough
# ---------------------------------------------------------------------------


@pytest.mark.cases("C18", "F09")
def test_manifest_backed_negative_price_is_preserved_exactly() -> None:
    ev = event(bid_ticks=-1, ask_ticks=-1)
    verdict = validate_business(
        ev, mode=PASSTHROUGH, fault_index=fault_index("invalid_price")
    )
    assert verdict.accepted
    assert verdict.fault_ids == ("f0001",)

    # And it survives the full Protobuf round trip with the sign and magnitude
    # intact -- not clamped to 0, not wrapped into a large positive.
    decoded = decode_payload(encode_event(ev))
    assert decoded.bid_ticks == -1
    assert decoded.ask_ticks == -1


@pytest.mark.cases("F08")
def test_manifest_backed_zero_price_keeps_explicit_presence() -> None:
    ev = event(bid_ticks=0, ask_ticks=0)
    verdict = validate_business(
        ev, mode=PASSTHROUGH, fault_index=fault_index("invalid_price")
    )
    assert verdict.accepted
    decoded = decode_payload(encode_event(ev))
    assert decoded.bid_ticks == 0
    # Present-and-zero, not absent: the distinction the whole schema design
    # exists to preserve.
    assert "bid_ticks" in decoded.present_fields()


@pytest.mark.cases("F07")
def test_manifest_backed_crossed_quote_is_preserved() -> None:
    ev = event(bid_ticks=126567, ask_ticks=126564)
    verdict = validate_business(
        ev, mode=PASSTHROUGH, fault_index=fault_index("invalid_spread")
    )
    assert verdict.accepted
    decoded = decode_payload(encode_event(ev))
    assert decoded.bid_ticks > decoded.ask_ticks
    assert decoded.spread_ticks == -3


@pytest.mark.cases("F09")
def test_int64_min_price_is_preserved_under_passthrough() -> None:
    ev = event(bid_ticks=INT64_MIN, ask_ticks=INT64_MIN)
    verdict = validate_business(
        ev, mode=PASSTHROUGH, fault_index=fault_index("invalid_price")
    )
    assert verdict.accepted
    assert decode_payload(encode_event(ev)).bid_ticks == INT64_MIN


@pytest.mark.cases("L03")
def test_passthrough_without_a_manifest_refuses_rather_than_waving_through() -> None:
    ev = event(bid_ticks=-1, ask_ticks=-1)
    assert reject_reason(ev, mode=PASSTHROUGH, fault_index=None) is Reason.FAULT_NOT_IN_MANIFEST
    assert (
        reject_reason(ev, mode=PASSTHROUGH, fault_index=FaultIndex())
        is Reason.FAULT_NOT_IN_MANIFEST
    )


@pytest.mark.cases("L03")
def test_a_fault_of_the_wrong_kind_does_not_license_an_anomaly() -> None:
    # A real manifest entry exists for this event, but `stale_timestamp` cannot
    # produce a negative price. Accepting it would mean any manifest row
    # licenses any corruption on that event.
    ev = event(bid_ticks=-1, ask_ticks=-1)
    assert (
        reject_reason(ev, mode=PASSTHROUGH, fault_index=fault_index("stale_timestamp"))
        is Reason.FAULT_NOT_IN_MANIFEST
    )


def test_a_fault_at_a_different_sequence_does_not_license_this_event() -> None:
    ev = event(sequence=10, bid_ticks=-1, ask_ticks=-1)
    elsewhere = fault_index("invalid_price", sequence=999)
    assert (
        reject_reason(ev, mode=PASSTHROUGH, fault_index=elsewhere)
        is Reason.FAULT_NOT_IN_MANIFEST
    )


def test_a_fault_on_a_different_symbol_does_not_license_this_event() -> None:
    ev = event(symbol="EURUSD", bid_ticks=-1, ask_ticks=-1)
    other = fault_index("invalid_price", symbol="GBPUSD")
    assert (
        reject_reason(ev, mode=PASSTHROUGH, fault_index=other)
        is Reason.FAULT_NOT_IN_MANIFEST
    )


def test_a_planned_but_unapplied_fault_licenses_nothing() -> None:
    # The fault changed no data, so it cannot explain an anomaly in the data.
    unapplied = FaultIndex.from_lines(
        ['{"fault_id":"f0001","kind":"invalid_price","applied":false,'
         '"channel":"payload","target":{"symbol":"EURUSD","sequence":10},'
         '"affected_sequences":[10],"planned":true}']
    )
    ev = event(bid_ticks=-1, ask_ticks=-1)
    assert (
        reject_reason(ev, mode=PASSTHROUGH, fault_index=unapplied)
        is Reason.FAULT_NOT_IN_MANIFEST
    )


def test_a_transport_channel_fault_licenses_nothing_in_the_payload() -> None:
    # Transport faults corrupt Project 01's TCP bytes and never touch the
    # recorded file, so one cannot explain a bad value in that file.
    transport = FaultIndex.from_lines(
        ['{"fault_id":"f0001","kind":"invalid_price","applied":true,'
         '"channel":"transport","target":{"symbol":"EURUSD","sequence":10},'
         '"affected_sequences":[10],"planned":true}']
    )
    ev = event(bid_ticks=-1, ask_ticks=-1)
    assert (
        reject_reason(ev, mode=PASSTHROUGH, fault_index=transport)
        is Reason.FAULT_NOT_IN_MANIFEST
    )


def test_anomalies_project_01_cannot_produce_are_never_licensed() -> None:
    # Project 01 has no fault that yields a non-positive sequence or scale, so
    # the mapping is deliberately empty and no manifest row can excuse them.
    assert ANOMALY_TO_FAULT_KINDS[ANOMALY_NONPOSITIVE_SEQUENCE] == frozenset()
    assert ANOMALY_TO_FAULT_KINDS[ANOMALY_NONPOSITIVE_SCALE] == frozenset()
    for overrides in ({"sequence": -1}, {"price_scale": 0}):
        ev = event(**overrides)
        for kind in ("invalid_price", "invalid_spread", "stale_timestamp"):
            assert (
                reject_reason(ev, mode=PASSTHROUGH, fault_index=fault_index(kind))
                is Reason.FAULT_NOT_IN_MANIFEST
            )


def test_structural_safety_is_never_relaxed_by_the_mode() -> None:
    # fault_passthrough widens which *values* are acceptable. It cannot make
    # malformed bytes acceptable, and there is no code path that would let it.
    from md_contract.errors import CodecError

    with pytest.raises(CodecError):
        decode_payload(b"\x00")


@pytest.mark.cases("F08")
def test_a_tick_with_a_manifest_backed_zero_price_is_preserved() -> None:
    ev = event(payload_case=PAYLOAD_TICK, bid_ticks=None, ask_ticks=None, price_ticks=0)
    verdict = validate_business(
        ev, mode=PASSTHROUGH, fault_index=fault_index("invalid_price")
    )
    assert verdict.accepted
    decoded = decode_payload(encode_event(ev))
    assert decoded.price_ticks == 0
    assert "price_ticks" in decoded.present_fields()
