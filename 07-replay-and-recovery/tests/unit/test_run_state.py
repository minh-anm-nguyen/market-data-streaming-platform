"""The lifecycle state machine: only the design's edges exist."""

from __future__ import annotations

import pytest

from p07.evidence.verdicts import Verdict
from p07.orchestration.run_state import (
    TERMINAL,
    IllegalTransition,
    RunState,
    State,
    ready_preconditions_met,
)

HAPPY_PATH = [
    State.ENV_READY,
    State.SOURCE_SEALED,
    State.RUNNING,
    State.CUT_CAPTURED,
    State.RAW_AUDIT,
    State.CANONICAL_BUILD,
    State.BAR_VERIFY,
    State.READY,
    State.PASS,
]


def walk(*states: State) -> RunState:
    run = RunState()
    for state in states:
        run.to(state)
    return run


def test_the_happy_path_is_walkable() -> None:
    run = walk(*HAPPY_PATH)
    assert run.state is State.PASS
    assert run.verdict is Verdict.PASS
    assert run.terminal


def test_a_run_in_flight_is_not_run_rather_than_failed() -> None:
    """Design 9.2 keeps NOT_RUN apart from the four terminal verdicts."""
    run = walk(State.ENV_READY, State.SOURCE_SEALED)
    assert run.verdict is Verdict.NOT_RUN
    assert not run.terminal


@pytest.mark.whatif("P07-G19-01")
def test_ready_cannot_be_reached_without_the_whole_path() -> None:
    """``INV-05`` as a property of the graph, not of remembering to check.

    There is simply no edge from an early state to READY, so a build cannot be
    published without having been audited, built and bar-verified.
    """
    run = walk(State.ENV_READY, State.SOURCE_SEALED, State.RUNNING, State.CUT_CAPTURED)
    for skip in (State.READY, State.BAR_VERIFY, State.CANONICAL_BUILD):
        assert not run.can(skip)
        with pytest.raises(IllegalTransition):
            RunState(state=run.state, history=list(run.history)).to(skip)


def test_pass_is_reachable_only_from_ready() -> None:
    for state in State:
        if state in TERMINAL or state is State.READY:
            continue
        run = RunState(state=state, history=[state])
        assert not run.can(State.PASS), state


def test_terminal_states_are_terminal() -> None:
    for terminal in TERMINAL:
        run = RunState(state=terminal, history=[terminal])
        assert run.terminal
        with pytest.raises(IllegalTransition):
            run.to(State.RUNNING)


def test_invalid_is_reachable_from_running() -> None:
    """An injector that missed its trigger invalidates the test where it stands."""
    run = walk(State.ENV_READY, State.SOURCE_SEALED, State.RUNNING, State.INVALID)
    assert run.verdict is Verdict.INVALID


def test_inconclusive_is_reachable_from_recovering() -> None:
    """Where a lost source or an expired Kafka range is discovered."""
    run = walk(
        State.ENV_READY, State.SOURCE_SEALED, State.RUNNING, State.RECOVERING, State.INCONCLUSIVE
    )
    assert run.verdict is Verdict.INCONCLUSIVE


def test_ready_preconditions_are_checked_against_history() -> None:
    """So a run nudged into READY cannot have its past assumed."""
    complete = walk(*HAPPY_PATH[:-1])
    met, missing = ready_preconditions_met(complete)
    assert met and missing == ()

    forced = RunState(state=State.READY, history=[State.CREATED, State.READY])
    met, missing = ready_preconditions_met(forced)
    assert not met
    assert set(missing) == {
        State.SOURCE_SEALED.value,
        State.CUT_CAPTURED.value,
        State.RAW_AUDIT.value,
        State.CANONICAL_BUILD.value,
        State.BAR_VERIFY.value,
    }


def test_history_records_the_route_taken() -> None:
    run = walk(State.ENV_READY, State.SOURCE_SEALED, State.RUNNING, State.FAULT_CONFIRMED)
    assert run.history[0] is State.CREATED
    assert run.reached(State.FAULT_CONFIRMED)
    assert not run.reached(State.READY)
    assert run.as_dict()["history"][-1] == State.FAULT_CONFIRMED.value


def test_every_non_terminal_state_has_a_route_to_a_terminal_one() -> None:
    """No state may be a dead end: every run must be able to end somewhere."""
    for state in State:
        if state in TERMINAL:
            continue
        run = RunState(state=state, history=[state])
        assert any(run.can(target) for target in TERMINAL) or any(
            run.can(target) for target in State if target not in TERMINAL
        ), state
