# ADR-015 — A sequence rollback is a flag on an honest class, not a class

**Status:** accepted · refines design section 4.3

## Context

Design section 4.3 says a sender that resets to sequence 1 inside one
`dataset_id`, with no new session contract, must be classified
`SEQUENCE_ROLLBACK`. Design section 5 says a sequence below the tracking
window must be classified `TOO_OLD_UNVERIFIABLE` and must *not* be assumed
to be a duplicate.

Both rules describe the same event, and the first implementation obeyed only
the first. That produced a worse answer than either rule alone: an isolated
re-send of sequence 1 — which happens whenever a feed loops a test fixture —
was reported as a sender restart, a far stronger claim than the evidence
supports.

At the moment sequence 1 arrives below the window, the adapter genuinely
cannot tell a restart from an ancient duplicate. Only the events that follow
would disambiguate it, and deciding later would mean holding a verdict open.

## Decision

The classification stays `TOO_OLD_UNVERIFIABLE`, because that is what is
actually known. When the sequence equals the key's own observed baseline,
the result additionally carries:

- the `SEQUENCE_ROLLBACK` quality flag,
- `Reason::kSequenceRollback` as the primary reason,
- `continuity_uncertain` on the key,
- an integrity alert from the classifier.

`SeqClass::kSequenceRollback` was removed. The concept lives on as a flag
and a reason, which is where the design document's own metrics dictionary
and quality-flag list already put it.

The baseline is the key's `first_seen`, not the literal value 1. A live feed
joined mid-stream has a baseline of, say, 50, and a re-send of 50 is the
same event as a re-send of 1 on a feed that started at 1.

## Consequences

- The event is refused, not published. A restart therefore stops the feed
  being published until an operator starts a new dataset epoch. This is
  documented as fail-closed behaviour in the README and the runbook; it is
  not a transient the system recovers from.
- Two signals now travel together — "cannot verify" and "looks like a
  restart" — which is more faithful than collapsing them into one verdict.
- Revisit if the handoff gains a session or epoch identifier. With an
  explicit epoch a restart becomes a verifiable fact rather than a
  suspicion, and it should then get its own class and its own state reset.
