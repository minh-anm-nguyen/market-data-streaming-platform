"""Fault injectors. Every one produces a receipt, and a receipt can say "no".

Design 6.3 is the contract for this whole subpackage: an injector that cannot
*independently observe* that its fault took effect has not injected a fault, and
the test is ``INVALID`` -- never ``PASS``. So no injector here returns a bare
boolean. They all return a
:class:`~p07.evidence.proof_ledger.FaultReceipt` whose
:attr:`~p07.evidence.proof_ledger.FaultReceipt.confirmed` property is computed
from six separately observed facts.

Design 6.3 also rules out the shortcut that makes most chaos suites meaningless:
``sleep(2)`` is not a boundary proof. Every injector here fires on a *state*
condition -- a broker's live set, a partition's log start offset, a sink's insert
counter -- and reads that state back to confirm.

Safety: :class:`~p07.injectors.base.LabAllowlist` gates every target. A target
that is not on the list is refused with ``TARGET_NOT_ALLOWLISTED`` before
anything happens, which is the in-process form of the "exact container ID + test
label" rule in design 6.2. The simulation has no way to touch a real broker, but
the allowlist is still enforced so the discipline is tested, not assumed.
"""
