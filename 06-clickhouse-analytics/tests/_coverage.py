"""Record which what-if cases each test exercises, and how.

The matrix in section 15 has 420 entries and this repo cannot execute all of
them on a laptop: some need a real ClickHouse, some need Kafka and P05, some need
a replicated cluster or a disposable disk. The honest thing is to say which is
which, by name, and to let a test assert the totals -- so the claim
"N cases covered" is checked by the suite rather than asserted in a README.

:func:`covers` registers at *import* time, so the registry is complete once
pytest has finished collection and ``tests/registry`` can compare it against the
matrix extracted from the design document.

``kind`` records the strength of the coverage:

``automated``
    Runs here, every time, and asserts the required behaviour.
``model_only``
    Runs here against the in-process model of ClickHouse semantics. Evidence
    about this pipeline's logic; **not** evidence about ClickHouse.
``needs_clickhouse``
    Written, and skipped unless ``P06_CLICKHOUSE_URL`` points at a server.
``needs_infra``
    Needs Kafka, P05, a cluster, or hardware fault injection. Not executable in
    this repo; listed so it is visibly absent rather than quietly missing.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from typing import Any, Final, TypeVar

CASE_RE: Final = re.compile(r"^P06-[A-Z]{1,2}-\d{2}$")

KINDS: Final = ("automated", "model_only", "needs_clickhouse", "needs_infra")

#: case_id -> {kind -> [test names]}
REGISTRY: dict[str, dict[str, list[str]]] = {}

F = TypeVar("F", bound=Callable[..., Any])


def covers(*case_ids: str, kind: str = "automated") -> Callable[[F], F]:
    """Declare the matrix cases a test exercises."""
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}, got {kind!r}")
    for case_id in case_ids:
        if not CASE_RE.match(case_id):
            raise ValueError(f"{case_id!r} is not a P06-<GROUP>-<NN> case id")

    def decorate(func: F) -> F:
        name = f"{func.__module__}::{func.__qualname__}"
        for case_id in case_ids:
            REGISTRY.setdefault(case_id, {}).setdefault(kind, []).append(name)
        existing = getattr(func, "_p06_cases", ())
        func._p06_cases = (*existing, *case_ids)  # type: ignore[attr-defined]
        return func

    return decorate


def declare_unexecutable(case_ids: Iterable[str], *, kind: str, reason: str) -> None:
    """Register cases no test here executes, with the reason they cannot be.

    Called from ``tests/registry`` for the environment-bound groups. Keeping the
    reason next to the case id is what stops "not covered" from drifting into
    "forgotten".
    """
    if kind not in ("needs_clickhouse", "needs_infra"):
        raise ValueError(f"declare_unexecutable needs an environment kind, got {kind!r}")
    for case_id in case_ids:
        if not CASE_RE.match(case_id):
            raise ValueError(f"{case_id!r} is not a P06-<GROUP>-<NN> case id")
        REGISTRY.setdefault(case_id, {}).setdefault(kind, []).append(f"<unexecutable: {reason}>")


def covered_ids(*kinds: str) -> set[str]:
    selected = kinds or KINDS
    return {
        case_id
        for case_id, by_kind in REGISTRY.items()
        if any(kind in by_kind for kind in selected)
    }


def summary() -> dict[str, Any]:
    by_kind = {kind: sorted(covered_ids(kind)) for kind in KINDS}
    return {
        "total_registered": len(REGISTRY),
        "counts": {kind: len(ids) for kind, ids in by_kind.items()},
        "by_kind": by_kind,
    }
