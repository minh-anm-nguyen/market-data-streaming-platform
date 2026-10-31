# ADR-017 — I/O budgets use a real clock even during replay

**Status:** accepted · found by the end-to-end backpressure case

## Context

Design sections 6 and 10 require offline replay to run on an injected
`VirtualClock`, so classification is deterministic and a historical dataset
is never judged stale against today's wall clock. The runner originally used
that single injected clock for everything.

But the virtual clock only advances when something scripts it. During a real
offline replay nothing does, so it reads zero forever, and every duration
derived from it was always zero:

- the sink stall deadline could never expire, making
  `BACKPRESSURE_TIMEOUT` unreachable offline;
- frame deadlines could never expire;
- `wall_ns` and the processing-latency histogram reported zero.

A deadline measured on a clock that does not tick is not a conservative
deadline. It is no deadline at all.

## Decision

Two clocks, split by purpose:

| Purpose | Clock |
|---|---|
| Event-time watermark, quote inactivity, feed state | the injected `Clock` |
| Frame and connect deadlines, backpressure stall budget, reconnect backoff, `wall_ns` | a private `SteadyClock` in `AdapterRunner` |
| Processing-latency histogram | a private `SteadyClock` in `Pipeline` |

The timer *cadence* is driven by the real clock, but the value handed to
`FreshnessEngine::tick()` comes from the injected clock, so both sides of an
inactivity comparison always have the same source.

## Consequences

- Classification stays deterministic: the same input with the same scripted
  clock gives the same trace, which the replay-determinism property test
  asserts.
- Real-time budgets are enforced in every mode.
- Offline replay does **not** detect quote inactivity, because the injected
  clock does not advance while a file is read at disk speed. That is the
  honest outcome — receive-time inactivity has no meaning for a file — and
  it is stated in the README. Event-time staleness is still detected
  offline, and the inactivity engine is covered by unit tests with a
  scripted clock.
- Revisit if offline replay should simulate pacing. Advancing the virtual
  clock by a configured step per frame would make offline inactivity
  reproducible, but it invents a receive-time semantics the handoff contract
  does not define, so it was not done on our own authority.
