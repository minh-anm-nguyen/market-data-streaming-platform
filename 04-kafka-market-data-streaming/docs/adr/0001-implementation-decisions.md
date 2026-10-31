# ADR 0001 — decisions settled while implementing Project 04

The design document's section 13 holds ADR-01 through ADR-12, decided before any
code existed. This record covers the decisions that only became decisions once
the code met a real broker. Each one names what went wrong first, because a
decision without its counterfactual is just an assertion.

## ADR-13: poll while waiting, never block and wait

**Context.** Section 2.4 pins a single-owner input path and section 4.4 pins a
bounded handoff with a stall deadline. The obvious implementation of
backpressure is a condition variable: when the budget is full, wait until a
delivery report releases some.

**What went wrong.** The first run against a live Kafka broker published exactly
4096 events — the configured `max_records` — and then stopped with
`BACKPRESSURE_TIMEOUT`. The budget is released by a delivery report, delivery
reports are only served from `rd_kafka_poll`, and the only thread that polls was
the one blocked on the condition variable. Nothing could ever release the space
it was waiting for. The run reported `INCOMPLETE` honestly, which is why the
defect was visible rather than silent, but it was a deadlock, not backpressure.

**Decision.** `Publisher::PublishOnce` uses `TryReserve` and, on failure, enters
an explicit loop that calls `transport_->Poll(5ms)` and retries until the same
stall deadline expires. `BoundedHandoff::ReserveOrWait` is kept, with its unit
tests, for a deployment that runs a dedicated poll thread as section 2.4 also
allows; the single-owner path must not use it.

**Consequence.** The maximum observed app-queue depth for the same dataset fell
from 4096 (saturated) to about 1450, and the run completed with all 20000
records acknowledged.

## ADR-14: a terminal attempt owns no queue budget

**Context.** At the drain deadline, attempts with no delivery report become
`UNKNOWN` (NFR-10). Those attempts still hold a reservation in the bounded
handoff, because the release happens in the delivery-report callback that never
arrived.

**What went wrong.** `tests/unit/test_publisher_pipeline.cpp` asserted the
budget was empty after a drain and found one record still held. In a shutdown
path the leak is harmless because the process exits, but the final
`app_queue_records` in the report would be non-zero for records the same report
calls terminal. Two numbers in one document disagreeing is exactly the kind of
thing that makes a reader stop trusting the rest.

**Decision.** `FinalizePendingAsUnknown` returns the ids it converted and
`Publisher::Drain` releases each one. The invariant is now: a terminal attempt
owns no budget.

## ADR-15: an unmapped delivery error is UNKNOWN, never a definite failure

**Context.** Section 4.2 requires that an ambiguous outcome never be reported as
a proven non-write. librdkafka has well over a hundred error codes and gains
more with each release.

**Decision.** `RdKafkaProducer::MapDeliveryError` lists the codes that are
provably pre-append and maps everything else, including the `default` arm, to
`kUnknown`. A test sweeps the entire librdkafka error range and asserts that
every code outside the explicit definite list maps to `kUnknown`, so a client
upgrade cannot quietly introduce a false "not written".

**Consequence.** `NOT_ENOUGH_REPLICAS` is definite while
`NOT_ENOUGH_REPLICAS_AFTER_APPEND` is ambiguous; `__PURGE_QUEUE` is definite
while `__PURGE_INFLIGHT` is ambiguous. Those pairs differ by one word and by
whether the broker may already hold the record.

## ADR-16: the snapshot is bounded at both ends, and the lower bound is captured before the run

**Context.** Section 5.1 specifies a snapshot verifier that captures begin and
end offsets and reads exactly that range.

**What went wrong.** The first implementation used the earliest available offset
as the lower bound. Against a cluster that had already served two earlier runs,
the verifier reported 44096 records for 20000 expected and flagged all 20000
identities as duplicates. Every one of those findings was leftover data from a
previous run and none was a defect, but a verifier that cries wolf is worse than
no verifier.

**Decision.** `snapshot_verifier.py --capture-begin-offsets` records the current
end offsets before publishing; `--begin-offsets` uses them as the lower bound of
the scan. The bound is clamped upward by the broker's earliest available offset,
so a segment deleted by retention shows up as a missing identity rather than
being quietly replaced by a scan from the start (case C-12).

## ADR-17: a proxied broker needs its own advertised listener

**Context.** Network fault injection (E2E-08, group G) puts Toxiproxy between
the publisher and the broker.

**Decision.** The broker exposes a second listener, `PROXY`, advertised as the
proxy's address rather than its own. A client that bootstrapped through the
proxy and then received direct-path metadata would reconnect straight to the
broker, and the injected fault would never be exercised — producing a network
fault test that silently tests nothing. The compose file carries a comment
saying so, because the failure mode is invisible: the test passes either way.

## ADR-18: the upstream classifier is a named mock, and every report says so

**Context.** Project 03 does not exist. Something has to produce `OutputEvent`
values.

**Decision.** `MockP03Classifier` lives in `src/source/`, not in `src/policy/`,
so it cannot be mistaken for production logic; every run report carries
`mock_upstream_p03: true`; and the report's Markdown form states that nothing in
it is evidence about the real adapter. The *data* is not mocked: Project 01 is
implemented and its run directories supply a real dataset with a real digest and
a real COMPLETED marker, which is what makes the source oracle meaningful.
