"""The independent P03 policy oracle: classification, stage order, and honesty.

These tests are the foundation of every other verdict in the harness. If the
expected set is wrong, every boundary comparison is wrong in the same direction
and nothing downstream can tell.
"""

from __future__ import annotations

from typing import Any, Callable

import pytest

from p07.dataset_sealer import seal_dataset
from p07.oracle.acceptance_oracle import (
    POLICY_VERSION,
    P03Reason,
    PolicyConfig,
    accepted_ticks,
    build_expectation,
)
from tools.source_factory import FaultOp, generate_events


def reasons(events: Any) -> set[str]:
    return {event.reason for event in events}


def first_quote_sequence(symbol: str, *, count: int, after: int = 0) -> int:
    """The first quote sequence for ``symbol`` above ``after``.

    The fixture generator decides tick-versus-quote from its seeded RNG, so a
    test that needs a quote has to ask which sequence is one. Hard-coding a
    number makes the test pass or fail depending on the walk, which is how a
    test ends up asserting the generator rather than the policy.
    """
    for event in generate_events(dataset_id="probe", count=count):
        if (
            event["symbol"] == symbol
            and event["event_type"] == "quote"
            and event["sequence"] > after
        ):
            return int(event["sequence"])
    raise AssertionError(f"no quote for {symbol} in {count} slots")


def test_clean_source_accepts_everything(expectation: Any, sealed: Any) -> None:
    assert expectation.counts["accepted"] == sealed.event_lines
    assert expectation.counts["rejected"] == 0
    assert expectation.counts["suppressed"] == 0
    assert expectation.source_identity_conflicts == ()
    assert expectation.untracked_anomalies == ()
    assert len(expectation.accepted) == sealed.event_lines


def test_receipt_carries_the_policy_version_and_config(expectation: Any) -> None:
    """An expected set is only comparable to another from the same policy."""
    receipt = expectation.receipt()
    assert receipt["policy_version"] == POLICY_VERSION
    assert receipt["config"]["sequence_window"] == PolicyConfig().sequence_window
    assert receipt["config"]["price_step_policy"] == "flag"


@pytest.mark.whatif("P07-G15-01", "P07-G05-03")
def test_exact_duplicate_is_suppressed_not_rejected(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    """A byte-identical repeat is business-valid and deliberately not published."""
    info = make_source(faults=(FaultOp("duplicate_exact", "EURUSD", 10),))
    expectation = build_expectation(seal_dataset(info["run_dir"]))
    assert P03Reason.DUPLICATE_EXACT in reasons(expectation.suppressed)
    assert expectation.counts["suppressed"] == 1
    # The identity is still expected exactly once.
    accepted_sequences = [
        identity.sequence for identity in expectation.accepted if identity.symbol == "EURUSD"
    ]
    assert accepted_sequences.count(10) == 1


@pytest.mark.whatif("P07-G16-01", "P07-G01-10")
def test_conflicting_duplicate_is_rejected_and_recorded_as_a_conflict(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    """Same identity, different semantics: no winner is chosen."""
    info = make_source(faults=(FaultOp("duplicate_conflict", "GBPUSD", 12),))
    expectation = build_expectation(seal_dataset(info["run_dir"]))
    assert P03Reason.DUPLICATE_CONFLICT in reasons(expectation.rejected)
    assert len(expectation.source_identity_conflicts) == 1
    identity, fields = expectation.source_identity_conflicts[0]
    assert identity.symbol == "GBPUSD" and identity.sequence == 12
    assert set(fields) & {"bid_ticks", "ask_ticks", "price_ticks"}


@pytest.mark.whatif("P07-G05-01")
def test_omission_produces_a_gap_flag_not_a_missing_event(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    """An intentional omission is a gap in the source, not transport loss.

    The omitted identity simply is not in the expected set, so downstream
    cannot be blamed for its absence -- which is ``INV-15``.
    """
    info = make_source(faults=(FaultOp("missing_sequence", "EURUSD", 15),))
    expectation = build_expectation(seal_dataset(info["run_dir"]))
    assert expectation.counts["gaps_detected"] == 1
    missing = [
        identity
        for identity in expectation.accepted
        if identity.symbol == "EURUSD" and identity.sequence == 15
    ]
    assert missing == []
    assert expectation.untracked_anomalies == ()


@pytest.mark.whatif("P07-G06-01")
def test_stale_quote_is_suppressed_and_does_not_move_the_watermark(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    # The fault has to land on a *quote*: a tick is never judged by the quote
    # watermark (P03 section 6), so staling one would correctly produce nothing.
    # The target sequence is therefore looked up rather than guessed.
    target = first_quote_sequence("EURUSD", count=400, after=5)
    info = make_source(
        count=400,
        faults=(FaultOp("stale_timestamp", "EURUSD", target, params={"back_ns": 10_000_000_000}),),
    )
    expectation = build_expectation(seal_dataset(info["run_dir"]))
    stale = [event for event in expectation.suppressed if event.reason == P03Reason.EVENT_TIME_OLD]
    assert len(stale) == 1
    assert stale[0].core is not None and stale[0].core.symbol == "EURUSD"
    # Later quotes are unaffected: the watermark never regressed.
    later = [
        identity
        for identity in expectation.accepted
        if identity.symbol == "EURUSD" and identity.sequence > 30
    ]
    assert later


@pytest.mark.whatif("P07-G30-02")
def test_business_rejects_are_classified_by_reason(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    info = make_source(
        count=400,
        faults=(
            FaultOp("invalid_spread", "EURUSD", 20),
            FaultOp("invalid_price", "GBPUSD", 18),
            FaultOp("bad_scale", "USDJPY", 9),
            FaultOp("unknown_symbol", "EURUSD", 25, params={"symbol": "ZZZZZZ"}),
        ),
    )
    expectation = build_expectation(seal_dataset(info["run_dir"]))
    found = reasons(expectation.rejected)
    assert P03Reason.BAD_SCALE in found
    assert P03Reason.UNKNOWN_SYMBOL in found
    assert found & {P03Reason.INVALID_ASK, P03Reason.SPREAD_OUT_OF_RANGE}
    assert found & {P03Reason.INVALID_PRICE, P03Reason.INVALID_BID, P03Reason.PRICE_OUT_OF_RANGE}


@pytest.mark.whatif("P07-G01-05")
def test_undeclared_anomaly_is_reported_as_untracked(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    """A fixture that claims to be clean and is not."""
    info = make_source(faults=(FaultOp("duplicate_conflict", "EURUSD", 11, declared=False),))
    expectation = build_expectation(seal_dataset(info["run_dir"]))
    assert len(expectation.untracked_anomalies) == 1
    assert expectation.untracked_anomalies[0].reason == P03Reason.DUPLICATE_CONFLICT


@pytest.mark.whatif("P07-G01-05")
def test_a_declared_fault_elsewhere_does_not_excuse_an_undeclared_one(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    """Declaration is matched per sequence, not per symbol.

    An earlier version matched at symbol level, so one declared fault made every
    anomaly on that symbol look accounted for. This is the regression test for
    that: both faults are on EURUSD, only one is declared.
    """
    info = make_source(
        count=400,
        faults=(
            FaultOp("duplicate_exact", "EURUSD", 10, declared=True),
            FaultOp("duplicate_conflict", "EURUSD", 30, declared=False),
        ),
    )
    expectation = build_expectation(seal_dataset(info["run_dir"]))
    untracked = expectation.untracked_anomalies
    assert len(untracked) == 1
    assert untracked[0].reason == P03Reason.DUPLICATE_CONFLICT
    assert untracked[0].identity is not None and untracked[0].identity.sequence == 30


@pytest.mark.whatif("P07-G01-06")
def test_declared_fault_that_never_applied_is_listed(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    info = make_source(faults=(FaultOp("missing_sequence", "EURUSD", 99_999),))
    expectation = build_expectation(seal_dataset(info["run_dir"]))
    assert len(expectation.unapplied_declared_faults) == 1


@pytest.mark.whatif("P07-G01-09")
def test_symbol_with_no_events_is_reported(make_source: Callable[..., dict[str, Any]]) -> None:
    """The manifest promises a symbol the file never mentions."""
    symbols = [
        {
            "symbol": "EURUSD",
            "price_scale": 100000,
            "initial_price_ticks": 108000,
            "min_price_ticks": 100000,
            "max_price_ticks": 120000,
            "min_spread_ticks": 1,
            "max_spread_ticks": 5,
            "max_step_ticks": 8,
            "weight": 1,
        },
        {
            "symbol": "NZDUSD",
            "price_scale": 100000,
            "initial_price_ticks": 60000,
            "min_price_ticks": 50000,
            "max_price_ticks": 70000,
            "min_spread_ticks": 1,
            "max_spread_ticks": 5,
            "max_step_ticks": 8,
            "weight": 0,
        },
    ]
    info = make_source(symbols=symbols, count=50)
    expectation = build_expectation(seal_dataset(info["run_dir"]))
    assert expectation.symbols_without_events == ("NZDUSD",)


def test_stage_order_business_reject_does_not_update_sequence_state(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    """A business-invalid event must not become ``seen_valid``.

    P03's implementation invariant: because seq 20 was rejected, seq 21 reports a
    gap. If the stages ran in the other order the gap would disappear and a real
    hole would be invisible.
    """
    info = make_source(count=400, faults=(FaultOp("bad_scale", "EURUSD", 20),))
    expectation = build_expectation(seal_dataset(info["run_dir"]))
    assert expectation.counts["gaps_detected"] >= 1
    assert expectation.counts["flagged_degraded"] >= 1


def test_price_step_policy_can_reject_instead_of_flagging(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    """The documented alternative policy is reachable and cannot be laxer.

    With ``flag`` (the default) no step violation is rejected; with ``reject``
    the oracle can only be stricter. The test asserts the direction rather than
    a count, because the count depends on the generated price walk.
    """
    info = make_source(count=400)
    dataset = seal_dataset(info["run_dir"])
    lenient = build_expectation(dataset, config=PolicyConfig(price_step_policy="flag"))
    strict = build_expectation(dataset, config=PolicyConfig(price_step_policy="reject"))
    assert strict.counts["accepted"] <= lenient.counts["accepted"]
    assert strict.config.price_step_policy == "reject"


def test_accepted_ticks_excludes_quotes(expectation: Any) -> None:
    ticks = list(accepted_ticks(expectation))
    assert ticks
    assert all(core.price_ticks is not None for core in ticks)
    assert all(core.bid_ticks is None and core.ask_ticks is None for core in ticks)


def test_invalid_policy_config_is_refused() -> None:
    with pytest.raises(ValueError):
        PolicyConfig(sequence_window=0)
    with pytest.raises(ValueError):
        PolicyConfig(price_step_policy="whatever")
    with pytest.raises(ValueError):
        PolicyConfig(event_time_stale_ns=-1)


def test_classification_is_deterministic(sealed: Any) -> None:
    """The same sealed source must produce the same expected set every time."""
    first = build_expectation(sealed)
    second = build_expectation(sealed)
    assert first.accepted == second.accepted
    assert first.accepted_digests == second.accepted_digests
    assert first.counts == second.counts
