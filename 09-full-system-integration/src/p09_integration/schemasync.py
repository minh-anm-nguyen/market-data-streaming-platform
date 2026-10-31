"""Make P02's schema the single source of truth, and keep it that way.

Finding P09-M0-02: four copies of ``marketdata.v1`` exist, they agree today, and
nothing in the build makes them keep agreeing. The audit can detect divergence,
because :mod:`p09_integration.protoparse` normalises them, but detection after
the fact is not prevention.

The fix is the ordinary one for vendored upstream files: the consumers' copies
become **generated artifacts**, byte-identical to P02's canonical schema, and CI
fails when one drifts. ``sync-schema --write`` regenerates them; ``--check``
compares and exits non-zero.

Why copies at all, rather than one file every project includes. Three reasons,
all about build systems rather than taste:

* P03 and P04 run ``protoc`` from their own trees against their own pinned
  protobuf prefixes (3.12.4 and 3.21.12). A shared include path would couple two
  toolchains that are deliberately independent.
* The descriptor digest each project pins is a function of the path ``protoc``
  was invoked with. Moving the file changes the digest and invalidates the pins
  ADR-0001 just made checkable.
* A copy CI proves identical carries the same guarantee as a symlink, and
  survives being checked out on Windows.

What this does **not** do is reformat anything. The content written is exactly
P02's bytes, so the comparison is a byte comparison with nothing to argue about.
The consumers' original formatting differed only in whitespace -- they already
normalised to the same contract -- so nothing of meaning is lost.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

#: P02 owns the schema. This is the only copy anyone should edit.
CANONICAL = (
    "02-market-data-protobuf-contracts/schemas/marketdata/v1/market_data.proto"
)

#: The consumers' copies, each at the path its own build already expects.
#: Keeping the paths is what keeps every pinned descriptor digest valid.
CONSUMER_COPIES: tuple[tuple[str, str], ...] = (
    ("P03", "03-cpp-market-data-adapter/schemas/market_data.proto"),
    ("P04", "04-kafka-market-data-streaming/schemas/market_data.proto"),
    ("P05", "05-python-batch-ingestion/schemas/market_data.proto"),
)


class SchemaSyncError(RuntimeError):
    """The canonical schema could not be read, so nothing may be synced."""


@dataclass(frozen=True)
class CopyState:
    project: str
    path: str
    exists: bool
    matches: bool
    sha256: str | None

    @property
    def status(self) -> str:
        if not self.exists:
            return "MISSING"
        return "IN_SYNC" if self.matches else "DRIFTED"


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def read_canonical(root: Path) -> str:
    path = root / CANONICAL
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SchemaSyncError("cannot read the canonical schema: %s" % exc) from exc


def check_copies(root: Path) -> tuple[list[CopyState], str]:
    """Compare every consumer copy against the canonical schema, byte for byte."""
    canonical = read_canonical(root)
    states: list[CopyState] = []
    for project, relative in CONSUMER_COPIES:
        path = root / relative
        if not path.exists():
            states.append(CopyState(project, relative, False, False, None))
            continue
        actual = path.read_text(encoding="utf-8")
        states.append(
            CopyState(project, relative, True, actual == canonical, _digest(actual))
        )
    return states, _digest(canonical)


def write_copies(root: Path) -> list[CopyState]:
    """Overwrite every consumer copy with the canonical bytes."""
    canonical = read_canonical(root)
    digest = _digest(canonical)
    written: list[CopyState] = []
    for project, relative in CONSUMER_COPIES:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        # newline="\n" so a checkout on Windows cannot fail the byte comparison
        # for a reason that has nothing to do with the contract.
        path.write_text(canonical, encoding="utf-8", newline="\n")
        written.append(CopyState(project, relative, True, True, digest))
    return written
