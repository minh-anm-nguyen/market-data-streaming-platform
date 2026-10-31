"""The data plane under test: a simulation of P02 through P06.

**Read this before trusting any result from this package.**

P07's design describes five deployment profiles (section 2.2). This subpackage
*is* the ``UNIT / MODEL`` profile and nothing more: "deterministic simulation,
fake ACK/offset machines, no Kafka/CH. Chứng minh logic riêng, **không** chứng
minh real broker/DB behavior."

What that buys, and what it does not:

* It models the **state machines** where the interesting failures live -- a
  broker log with per-partition offsets, generations and retention; a producer
  whose ACK can be lost after the append; a consumer whose commit must trail a
  contiguous completed frontier; a sink whose INSERT can commit and then lose
  its reply; a catalog pointer moved by compare-and-swap. Faults at those
  boundaries are real faults in a real state machine.
* It does **not** model librdkafka, the Kafka replication protocol, ClickHouse
  ``MergeTree`` merges, or any of their timing. A ``PASS`` here is evidence
  about the *protocol logic*, and design 9.3 is explicit that it does not carry
  over to a real broker. :func:`p07.evidence.verdicts.Reason.PROFILE_NOT_VALIDATED`
  exists so reports say so out loud.

One rule holds throughout: modules here never import from :mod:`p07.oracle` or
:mod:`p07.reconcile`. The data plane must be able to be wrong, or the harness
proves nothing -- which is why, for example, :mod:`p07.dataplane.adapter`
re-implements the P03 policy instead of calling the oracle that checks it.
"""
