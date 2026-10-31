# ADR-018 — A strict TOML subset instead of a general parser

**Status:** accepted

## Context

Design section 14 requires the configuration layer to reject unknown keys,
floats, NaN, negative budgets and several cross-field contradictions. A
general-purpose TOML library does close to the opposite: it accepts
everything the format allows and hands back a document, leaving every one of
those rejections to be re-implemented on top anyway.

It would also be the component's only third-party dependency beyond
protobuf, in a project whose stated goal includes a small, pinned toolchain.

## Decision

`TomlLite` implements exactly the surface the configuration contract uses:
tables, one level of sub-tables, double-quoted strings with four escapes,
decimal integers with `_` separators, booleans, and homogeneous arrays of
integers or strings. Everything else is an error carrying a line number —
including floats, hex and binary literals, `inf`, `nan`, array-of-tables,
quoted keys and duplicate keys.

Reading a key through a typed accessor marks it consumed; whatever is left
over is reported as an unknown configuration key. That inverts the usual
default: the configuration is rejected unless this build understands all of
it.

Command-line overrides are appended to the document text and re-parsed, so
they go through the same parser and the same validator. There is no second,
looser path for them.

## Consequences

- A typo in a threshold is a startup failure, not a silent fall back to a
  default.
- About 300 lines to own and test, covered by `test_toml_lite.cpp`,
  `test_config.cpp` and a libFuzzer target whose invariants are that a
  rejection always carries a reason and an acceptance always satisfies what
  the rest of the code assumes.
- The configuration file is not general TOML. Anything the contract does not
  need is refused, which is intended but can surprise someone pasting in a
  snippet.
- Revisit if the configuration grows nested structures or arrays of tables.
  At that point a real parser plus an explicit schema check is the better
  trade.
