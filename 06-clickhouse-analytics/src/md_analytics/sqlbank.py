"""Load the SQL in ``sql/`` and hash it, so evidence names exact bytes.

Every benchmark and every build manifest has to record which SQL produced it
(NFR-12, section 14.2). That is only meaningful if the text in the evidence and
the text sent to the server are the same bytes, which is why queries live in
``.sql`` files and are looked up by name instead of being pasted into Python
strings that drift.

Named blocks are delimited by ``-- name: <id>`` and run to the next marker or
end of file.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Final

from .errors import ConfigError

_NAME_RE: Final = re.compile(r"^--\s*name:\s*([a-z0-9_]+)\s*$", re.MULTILINE)

#: DDL files, in apply order. Numbered so the order is visible on disk too.
DDL_FILES: Final = (
    "001_database.sql",
    "002_canonical_ticks.sql",
    "003_bars_1m_state.sql",
    "004_mv_canonical_to_bars.sql",
)
QUERY_FILES: Final = ("005_read_queries.sql", "006_audit_queries.sql")


def default_sql_dir() -> Path:
    """``sql/`` next to the installed package, resolved from this file.

    Walks up looking for the directory rather than assuming a fixed depth, so an
    editable install and a test run from a different working directory both find
    it, and a missing directory fails loudly instead of yielding empty SQL.
    """
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "sql"
        if (candidate / "002_canonical_ticks.sql").is_file():
            return candidate
    raise ConfigError(
        f"could not locate the sql/ directory above {here}", code="SQL_DIR_NOT_FOUND"
    )


def _split_statements(text: str) -> list[str]:
    """Split a DDL file on ``;`` at statement level.

    The DDL in this project has no string literals containing semicolons, and
    keeping the splitter this simple is preferable to a half-written SQL parser
    that would be wrong in a more interesting way. :func:`SqlBank.ddl` asserts
    the result is non-empty, so a file this mis-handles cannot pass silently.
    """
    # Comments come out BEFORE the split, not after. That ordering is the whole
    # correctness of this function: a `--` comment may contain a semicolon, and
    # splitting first cuts such a comment in half, strips the leading fragment
    # because it still starts with `--`, and hands the trailing prose to the
    # server as a statement. 002_canonical_ticks.sql has exactly that shape --
    # it cites "006_audit_queries.sql; this DDL only makes the proof cheap to
    # run." -- and ClickHouse answered:
    #
    #   Code: 62. DB::Exception: Syntax error: failed at position 1 (this)
    #
    # The model backend never parsed SQL, so this surfaced only the first time
    # the DDL met a real server.
    without_comments = "\n".join(
        line for line in text.splitlines() if not line.strip().startswith("--")
    )
    return [
        statement
        for statement in (chunk.strip() for chunk in without_comments.split(";"))
        if statement
    ]


class SqlBank:
    """Named queries and DDL, with per-item and whole-bank digests."""

    def __init__(self, sql_dir: Path | None = None) -> None:
        self.sql_dir = sql_dir if sql_dir is not None else default_sql_dir()
        self._queries: dict[str, str] = {}
        self._sources: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        for filename in DDL_FILES + QUERY_FILES:
            path = self.sql_dir / filename
            if not path.is_file():
                raise ConfigError(f"missing SQL file {path}", code="SQL_FILE_MISSING")
            text = path.read_text(encoding="utf-8")
            self._sources[filename] = text
            if filename in QUERY_FILES:
                self._load_named(filename, text)

    def _load_named(self, filename: str, text: str) -> None:
        markers = list(_NAME_RE.finditer(text))
        if not markers:
            raise ConfigError(
                f"{filename} declares no '-- name:' blocks", code="SQL_NO_NAMED_BLOCKS"
            )
        for index, match in enumerate(markers):
            name = match.group(1)
            end = markers[index + 1].start() if index + 1 < len(markers) else len(text)
            body = text[match.end() : end].strip().rstrip(";").strip()
            if not body:
                raise ConfigError(f"{filename}:{name} is empty", code="SQL_EMPTY_BLOCK")
            if name in self._queries:
                raise ConfigError(
                    f"duplicate SQL block name {name!r}", code="SQL_DUPLICATE_NAME"
                )
            self._queries[name] = body

    def __contains__(self, name: object) -> bool:
        return name in self._queries

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._queries))

    def get(self, name: str) -> str:
        try:
            return self._queries[name]
        except KeyError:
            raise ConfigError(
                f"unknown SQL block {name!r}; known: {', '.join(self.names())}",
                code="SQL_UNKNOWN_NAME",
            ) from None

    def sha256(self, name: str) -> str:
        return hashlib.sha256(self.get(name).encode("utf-8")).hexdigest()

    def source(self, filename: str) -> str:
        """Raw text of one SQL file, comments included."""
        try:
            return self._sources[filename]
        except KeyError:
            raise ConfigError(f"unknown SQL file {filename!r}", code="SQL_UNKNOWN_FILE") from None

    def statements(self, filename: str) -> list[str]:
        """Comment-free statements of one DDL file.

        Checks that a DDL does *not* contain something -- ``TTL``, ``POPULATE``,
        ``any()`` -- have to run against this rather than the raw source, or the
        comment explaining why it is absent trips the check.
        """
        return _split_statements(self.source(filename))

    def ddl(self) -> list[str]:
        statements: list[str] = []
        for filename in DDL_FILES:
            parts = _split_statements(self._sources[filename])
            if not parts:
                raise ConfigError(
                    f"{filename} produced no statements", code="SQL_NO_STATEMENTS"
                )
            statements.extend(parts)
        return statements

    def file_sha256(self, filename: str) -> str:
        try:
            text = self._sources[filename]
        except KeyError:
            raise ConfigError(f"unknown SQL file {filename!r}", code="SQL_UNKNOWN_FILE") from None
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def bank_digest(self) -> str:
        """One hash over every file, in a fixed order: the ``sql_sha`` of a manifest."""
        h = hashlib.sha256()
        for filename in DDL_FILES + QUERY_FILES:
            h.update(filename.encode("utf-8"))
            h.update(b"\0")
            h.update(self._sources[filename].encode("utf-8"))
            h.update(b"\0")
        return h.hexdigest()

    def digests(self) -> dict[str, str]:
        """Per-file digests plus the bank digest, for a manifest or report."""
        out = {name: self.file_sha256(name) for name in DDL_FILES + QUERY_FILES}
        out["bank"] = self.bank_digest()
        return out
