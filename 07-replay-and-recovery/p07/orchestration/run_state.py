"""The run lifecycle as an explicit state machine with legal transitions only.

Design section 5 gives the diagram; this encodes it, including the four terminal
states. Making the transitions explicit buys one specific guarantee: a run cannot
reach ``READY`` without having passed through ``CUT_CAPTURED``, ``RAW_AUDIT``,
``CANONICAL_BUILD`` and ``BAR_VERIFY``, because no edge exists that skips them.
That is ``INV-05`` enforced by the type rather than by remembering to check.

``PASS`` is reachable only from ``READY``, and ``READY`` only from
``BAR_VERIFY``. ``INVALID`` is reachable from ``RUNNING`` -- an injector that
missed its trigger invalidates the test wherever it was -- and ``INCONCLUSIVE``
from ``RECOVERING``, where a lost source or an expired Kafka range is
discovered.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Final, Mapping

from ..evidence.proof_ledger import Phase
from ..evidence.verdicts import Verdict


class State(str, Enum):
    """Lifecycle states, matching the ``stateDiagram`` in design section 5."""

    CREATED = "CREATED"
    ENV_READY = "ENV_READY"
    SOURCE_SEALED = "SOURCE_SEALED"
    RUNNING = "RUNNING"
    FAULT_CONFIRMED = "FAULT_CONFIRMED"
    RECOVERING = "RECOVERING"
    CUT_CAPTURED = "CUT_CAPTURED"
    RAW_AUDIT = "RAW_AUDIT"
    CANONICAL_BUILD = "CANONICAL_BUILD"
    BAR_VERIFY = "BAR_VERIFY"
    READY = "READY"
    PASS = "PASS"
    FAIL = "FAIL"
    INVALID = "INVALID"
    INCONCLUSIVE = "INCONCLUSIVE"


#: Terminal states. Nothing leaves these.
TERMINAL: Final[frozenset[State]] = frozenset(
    {State.PASS, State.FAIL, State.INVALID, State.INCONCLUSIVE}
)

#: Every legal edge. ``FAIL`` is reachable from anywhere a contradiction can be
#: proven; ``INVALID`` from anywhere the test can be found not to apply.
_TRANSITIONS: Final[Mapping[State, frozenset[State]]] = {
    State.CREATED: frozenset({State.ENV_READY, State.INVALID, State.FAIL}),
    State.ENV_READY: frozenset({State.SOURCE_SEALED, State.INVALID, State.INCONCLUSIVE, State.FAIL}),
    State.SOURCE_SEALED: frozenset({State.RUNNING, State.INVALID, State.INCONCLUSIVE, State.FAIL}),
    State.RUNNING: frozenset(
        {State.FAULT_CONFIRMED, State.RECOVERING, State.CUT_CAPTURED, State.INVALID, State.FAIL}
    ),
    State.FAULT_CONFIRMED: frozenset({State.RECOVERING, State.CUT_CAPTURED, State.FAIL, State.INVALID}),
    # RECOVERING -> RUNNING is the republish route: the sealed source is fed
    # again, into a fresh topic generation, before a new cut is taken.
    State.RECOVERING: frozenset(
        {State.RUNNING, State.CUT_CAPTURED, State.INCONCLUSIVE, State.FAIL, State.INVALID}
    ),
    # A captured cut can turn out unusable -- its range expired, its generation
    # is gone -- and the answer is then recovery, not a verdict. Design section
    # 5's diagram reaches CUT_CAPTURED *from* RECOVERING; this is the edge back,
    # which cases E2E-20 and E2E-21 both need.
    State.CUT_CAPTURED: frozenset(
        {State.RAW_AUDIT, State.RECOVERING, State.INCONCLUSIVE, State.FAIL}
    ),
    State.RAW_AUDIT: frozenset({State.CANONICAL_BUILD, State.FAIL, State.INCONCLUSIVE}),
    State.CANONICAL_BUILD: frozenset({State.BAR_VERIFY, State.FAIL, State.INCONCLUSIVE}),
    State.BAR_VERIFY: frozenset({State.READY, State.FAIL, State.INCONCLUSIVE}),
    State.READY: frozenset({State.PASS, State.FAIL, State.INCONCLUSIVE}),
}

#: Phase recorded in the journal for each state, so the two vocabularies stay
#: aligned instead of drifting apart.
_PHASE_FOR_STATE: Final[Mapping[State, Phase]] = {
    State.CREATED: Phase.CREATED,
    State.ENV_READY: Phase.ENV_READY,
    State.SOURCE_SEALED: Phase.SOURCE_SEALED,
    State.RUNNING: Phase.RUNNING,
    State.FAULT_CONFIRMED: Phase.FAULT_CONFIRMED,
    State.RECOVERING: Phase.RECOVERING,
    State.CUT_CAPTURED: Phase.CUT_CAPTURED,
    State.RAW_AUDIT: Phase.RAW_AUDIT,
    State.CANONICAL_BUILD: Phase.CANONICAL_BUILD,
    State.BAR_VERIFY: Phase.BAR_VERIFY,
    State.READY: Phase.READY,
    State.PASS: Phase.TERMINAL,
    State.FAIL: Phase.TERMINAL,
    State.INVALID: Phase.TERMINAL,
    State.INCONCLUSIVE: Phase.TERMINAL,
}

_VERDICT_FOR_STATE: Final[Mapping[State, Verdict]] = {
    State.PASS: Verdict.PASS,
    State.FAIL: Verdict.FAIL,
    State.INVALID: Verdict.INVALID,
    State.INCONCLUSIVE: Verdict.INCONCLUSIVE,
}


class IllegalTransition(Exception):
    """An attempt to move along an edge the design does not have."""

    def __init__(self, source: State, target: State) -> None:
        super().__init__(f"{source.value} -> {target.value} is not a legal transition")
        self.source = source
        self.target = target


@dataclass(slots=True)
class RunState:
    """Current state plus the path taken to reach it."""

    state: State = State.CREATED
    history: list[State] = field(default_factory=lambda: [State.CREATED])

    def can(self, target: State) -> bool:
        return target in _TRANSITIONS.get(self.state, frozenset())

    def to(self, target: State) -> State:
        """Move to ``target`` or raise :class:`IllegalTransition`."""
        if self.state in TERMINAL:
            raise IllegalTransition(self.state, target)
        if not self.can(target):
            raise IllegalTransition(self.state, target)
        self.state = target
        self.history.append(target)
        return target

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL

    @property
    def verdict(self) -> Verdict:
        """The verdict for a terminal state; ``NOT_RUN`` while still in flight.

        A run that never reached a terminal state is ``NOT_RUN``, not ``FAIL``.
        Design 9.2 keeps the two apart, and so does this.
        """
        return _VERDICT_FOR_STATE.get(self.state, Verdict.NOT_RUN)

    def phase(self) -> Phase:
        return _PHASE_FOR_STATE[self.state]

    def reached(self, state: State) -> bool:
        return state in self.history

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "terminal": self.terminal,
            "verdict": self.verdict.value,
            "history": [item.value for item in self.history],
        }


def ready_preconditions_met(run: RunState) -> tuple[bool, tuple[str, ...]]:
    """Whether the path to ``READY`` passed every required gate (``INV-05``).

    Checked against the *history*, not the current state, so a run cannot be
    nudged into ``READY`` and then have its past assumed.
    """
    required = (
        State.SOURCE_SEALED,
        State.CUT_CAPTURED,
        State.RAW_AUDIT,
        State.CANONICAL_BUILD,
        State.BAR_VERIFY,
    )
    missing = tuple(state.value for state in required if not run.reached(state))
    return (not missing), missing
