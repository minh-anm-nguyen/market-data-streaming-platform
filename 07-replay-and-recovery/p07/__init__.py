"""P07 -- deterministic replay and failure-recovery harness.

Scope is a **test lab**. Nothing here is allowed to touch infrastructure it was
not explicitly pointed at, and no module in this package may claim a result it
did not independently observe. See ``README.md`` for the deployment profile the
test suite actually exercises, and :mod:`p07.evidence.verdicts` for the five
terminal verdicts (``PASS``/``FAIL``/``INCONCLUSIVE``/``INVALID``/``NOT_RUN``)
that every check has to resolve to.
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
