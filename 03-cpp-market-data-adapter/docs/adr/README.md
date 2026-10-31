# Architecture decision records

## ADR-001 … ADR-014 — design-time decisions

These were decided before implementation and are recorded, with their
alternatives and trade-offs, in **section 16 of the design document**:
[`PROJECT_03_CPP_FX_CFD_Market_Data_Adapter_System_Design.md`](../../../docs/PROJECT_03_CPP_FX_CFD_Market_Data_Adapter_System_Design.md).

They are deliberately not copied here. A second copy would drift from the
first, and then neither would be authoritative.

| ADR | Decision | Implemented in |
|---|---|---|
| 001 | MDP1 protobuf only; no JSON fallback, no protocol sniffing | `mdp1_framer.cpp`, `config.cpp` (port 9009 rejected) |
| 002 | POSIX non-blocking sockets with an explicit loop | `mdp1_tcp_client.cpp`, `runner.cpp` |
| 003 | One owner thread for parsing and feed state | `pipeline.cpp`, `runner.cpp` |
| 004 | Typed integer payloads, no floating point | `domain_event.hpp`, `business_validator.cpp` |
| 005 | Bounded sequence window, no work proportional to a gap | `sequence_tracker.cpp` |
| 006 | Observed and published continuity tracked separately | `sequence_tracker.cpp` |
| 007 | Forward gapped events as DEGRADED; suppress duplicates and late arrivals | `classifier.cpp` |
| 008 | Event-time, monotonic inactivity and wall-age kept apart | `quote_freshness.cpp` |
| 009 | Bounded queue with pause-read; a stall fails the run | `bounded_queue.cpp`, `runner.cpp` |
| 010 | COMPLETE requires a verified manifest | `mdp1_file_source.cpp`, `runner.cpp` |
| 011 | Fail closed on corruption; never resynchronise | `mdp1_framer.cpp`, `pipeline.cpp` |
| 012 | No durable ACK or replay cursor in the MVP | reflected in `RunStatus::kPartialLive` |
| 013 | Pinned protobuf schema and toolchain | `cmake/Protobuf.cmake`, `protobuf_decoder.cpp` |
| 014 | P04 value is raw protobuf; quality on a side channel | `sink.cpp` (`P04StubSink`) |

Where the implementation had to refine one of these, the refinement has its
own record below and says which design section it changes.

## ADR-015 onwards — decisions forced by implementation

Each of these changed behaviour that the design document specified
differently, or resolved a conflict between two of its rules. Three of the
four were found by a test failing for the right reason.

| ADR | Decision |
|---|---|
| [015](adr-015-rollback-is-a-flag.md) | A sequence rollback is a flag on an honest class, not a class of its own |
| [016](adr-016-inflight-accounting.md) | In-flight events count against the queue caps |
| [017](adr-017-two-clocks-in-the-runner.md) | I/O budgets use a real clock even during replay |
| [018](adr-018-strict-toml-subset.md) | A strict TOML subset instead of a general parser |
