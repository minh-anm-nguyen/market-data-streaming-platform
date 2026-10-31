"""Replay and read-only observation of the transport layer.

The scanner here belongs to the evidence plane even though it lives next to the
replayer: it only ever reads, and it is the component whose word -- not the
producer's ledger -- decides what Kafka actually holds.
"""
