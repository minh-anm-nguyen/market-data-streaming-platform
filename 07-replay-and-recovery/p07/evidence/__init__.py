"""Evidence plane: verdicts, append-only receipts and the artifact store.

Design section 2 keeps this plane separate from the data plane for one reason:
when the thing being tested dies, the proof of what it did must not die with
it. Modules here never import from :mod:`p07.dataplane`.
"""
