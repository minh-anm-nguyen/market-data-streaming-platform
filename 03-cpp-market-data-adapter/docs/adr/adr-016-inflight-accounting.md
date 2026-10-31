# ADR-016 — In-flight events count against the queue caps

**Status:** accepted · found by the end-to-end backpressure case

## Context

The sink writer originally pulled batches of up to 256 events out of the
bounded queue and wrote them one by one. The events left the deque
immediately, so the queue reported itself empty while up to 256 events were
still unwritten in a local vector.

The consequence was not a slow leak but the silent loss of the entire
backpressure mechanism. With `max_queue_events = 2` and a sink taking 300 ms
per event, the end-to-end case expected a stall. What actually happened was
that the writer drained both entries instantly, the producer always saw
room, and the run completed with the caps never once binding. The
`4096 events / 8 MiB` budget was describing deque occupancy, not memory the
process was holding.

This is the failure mode design section 7.2 warns about — "do not rely on
`std::queue::size` alone" — arriving by a different route.

## Decision

`BoundedQueue` tracks in-flight work explicitly:

- `pop_batch()` moves cost from *queued* to *in flight* rather than
  discharging it;
- the caller must call `release()` once the batch has reached the sink;
- `try_push()`, `should_pause()`, `should_resume()` and `outstanding()` all
  count queued **plus** in flight.

`pop()` keeps its fully-discharging semantics, for drains and tests where
the caller does no further work with the event.

## Consequences

- The configured caps now describe accepted-but-unwritten work, which is
  what an operator reasoning about memory and backpressure actually means.
- Batching is retained, so the writer still does not take the lock once per
  event.
- `release()` is a contract a caller can forget. It is called in exactly one
  production place — the writer, on both the success and the failure path —
  and `BoundedQueue.InFlightWorkCountsAgainstTheCap` pins the behaviour.
- Revisit if the writer gains more than one consumer thread; release would
  then need to be tied to the batch rather than to the loop iteration.
