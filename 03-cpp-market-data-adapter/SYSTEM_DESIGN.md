# System design

The design for this component lives in one place, at the repository root:

**[`../docs/PROJECT_03_CPP_FX_CFD_Market_Data_Adapter_System_Design.md`](../docs/PROJECT_03_CPP_FX_CFD_Market_Data_Adapter_System_Design.md)**

It is not copied here. A second copy would drift from the first, and then
neither would be authoritative.

## Where the implementation diverges from it

Four decisions changed during implementation. Each has a record saying what
the design specified, why it could not stand, and what replaced it:

| Design section | What changed | Record |
|---|---|---|
| 4.3 vs 5 | A rollback at the sequence baseline is a flag on `TOO_OLD_UNVERIFIABLE`, not a class of its own | [ADR-015](docs/adr/adr-015-rollback-is-a-flag.md) |
| 7.2 | In-flight events count against the queue caps, not just queued ones | [ADR-016](docs/adr/adr-016-inflight-accounting.md) |
| 6 vs 4.1 | I/O and backpressure budgets use a real clock even during virtual-clock replay | [ADR-017](docs/adr/adr-017-two-clocks-in-the-runner.md) |
| 14 | Configuration is parsed by a strict TOML subset rather than a library | [ADR-018](docs/adr/adr-018-strict-toml-subset.md) |

Two further consequences worth knowing before reading the design document as
a description of this code:

- **Offline replay does not detect quote inactivity.** The design assumes a
  scripted virtual clock; a plain replay has none, and receive-time
  inactivity has no meaning for a file. Event-time staleness is still
  detected. See ADR-017.
- **The Project 02 schema is vendored.** `schemas/market_data.proto` is a
  frozen copy of the contract from design section 3.2, because Project 02
  is not implemented yet. It is a copy to be replaced, not a fork to be
  edited. See [docs/protocol_handoff.md](docs/protocol_handoff.md).

## Orientation

| Question | Start at |
|---|---|
| How do I build and run it? | [README.md](README.md) |
| What does this error mean? | [docs/runbook.md](docs/runbook.md) |
| What can it not detect? | [docs/failure-catalog.md](docs/failure-catalog.md) |
| Which of the 231 what-if cases run? | [docs/test-coverage.md](docs/test-coverage.md) |
| What do the neighbours owe it? | [docs/protocol_handoff.md](docs/protocol_handoff.md) |
| What has actually been measured? | [docs/benchmarks.md](docs/benchmarks.md) |
| Why is it built this way? | [docs/adr/](docs/adr/) |
