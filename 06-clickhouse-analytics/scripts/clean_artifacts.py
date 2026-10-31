"""Remove generated artefacts under ``.artifacts/``, and nothing else.

A ``make clean`` implemented as ``rm -rf`` is one typo away from deleting the
repository, so the deletion is written here with an explicit allowlist: it
refuses any path outside ``.artifacts/`` and refuses to follow symlinks out of
it. The published build catalog lives under that directory, and section 9 is
clear that deleting the only copy of something you cannot rebuild is not a
cleanup step -- so this prints what it removed.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
ARTIFACTS = REPO / ".artifacts"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--yes",
        action="store_true",
        help="required when the catalog holds published builds",
    )
    args = parser.parse_args(argv)

    if not ARTIFACTS.exists():
        print(f"nothing to clean: {ARTIFACTS} does not exist")
        return 0

    resolved = ARTIFACTS.resolve()
    if not resolved.is_relative_to(REPO.resolve()):
        print(f"refusing to delete {resolved}: outside the repository", file=sys.stderr)
        return 2

    builds = sorted((resolved / "catalog" / "builds").glob("*.json"))
    if builds and not args.yes:
        print(
            f"{len(builds)} published build manifest(s) under {resolved / 'catalog'}.\n"
            f"These are the only record of what was served. Re-run with --yes to "
            f"delete them.",
            file=sys.stderr,
        )
        return 3

    removed = [p.name for p in sorted(resolved.iterdir())]
    shutil.rmtree(resolved)
    print(f"removed {resolved} ({', '.join(removed) if removed else 'empty'})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
