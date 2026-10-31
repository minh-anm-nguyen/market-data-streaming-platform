"""A strict, tiny YAML subset loader -- deliberately not a YAML implementation.

Scenario and lab files are YAML in the design (section 14.1), and design 2.1
requires the loader to refuse to "parse và thực thi arbitrary shell từ untrusted
config". The safest way to honour that is to support only what a scenario needs
and reject the rest *loudly*:

**Supported.** Block mappings, block sequences, nesting by indentation, inline
flow sequences of scalars (``[a, b, 3]``), comments, and the scalars
``int``, ``float``, ``true``/``false``, ``null``/``~`` and strings (plain,
single- or double-quoted).

**Rejected with an error, not ignored.** Anchors and aliases (``&``/``*``),
explicit tags (``!!python/object``, any ``!tag``), multiple documents (``---``),
block scalars (``|``, ``>``), flow mappings (``{...}``), and tabs used for
indentation. Each of those is a line in a config file that could change what the
harness does, so silently skipping one would be worse than failing.

The trade-off, stated plainly: this is a reimplementation of part of a standard
format, which is normally a bad idea. It is taken here because the alternative is
a parser that accepts constructs the harness has no meaning for, in a file that
controls destructive operations -- and because the subset is small enough to be
covered exhaustively by ``tests/unit/test_yamlite.py``, including every
rejection. A ``.json`` scenario is also accepted everywhere a ``.yaml`` one is,
for anyone who would rather not rely on this at all.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

_COMMENT = re.compile(r"(?:^|\s+)#.*$")
_KEY = re.compile(r"^(?P<key>'[^']*'|\"[^\"]*\"|[^:#]+?)\s*:(?P<rest>\s.*|)$")

_FORBIDDEN_PREFIXES = {
    "---": "multiple documents are not supported",
    "...": "document end markers are not supported",
    "%": "directives are not supported",
}


class YamlLiteError(ValueError):
    """A construct outside the supported subset, or a malformed document."""

    def __init__(self, line_number: int, detail: str) -> None:
        super().__init__(f"line {line_number}: {detail}")
        self.line_number = line_number
        self.detail = detail


def _strip_comment(text: str) -> str:
    # Only strip a ``#`` that starts a token, so a value like ``colour: #fff``
    # inside quotes survives and an unquoted one is still treated as a comment
    # exactly as YAML would.
    in_single = in_double = False
    for index, char in enumerate(text):
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif char == "#" and not in_single and not in_double:
            if index == 0 or text[index - 1].isspace():
                return text[:index]
    return text


def _scalar(raw: str, line_number: int) -> Any:
    text = raw.strip()
    if not text:
        return None
    first = text[0]
    if first in "&*":
        raise YamlLiteError(line_number, "anchors and aliases are not supported")
    if first == "!":
        raise YamlLiteError(line_number, "explicit tags are not supported")
    if first in "|>":
        raise YamlLiteError(line_number, "block scalars are not supported")
    if first == "{":
        raise YamlLiteError(line_number, "flow mappings are not supported; use a block mapping")
    if first == "[":
        if not text.endswith("]"):
            raise YamlLiteError(line_number, "unterminated flow sequence")
        inner = text[1:-1].strip()
        if not inner:
            return []
        return [_scalar(part, line_number) for part in _split_flow(inner, line_number)]
    if len(text) >= 2 and first == text[-1] and first in "'\"":
        return text[1:-1]
    lowered = text.lower()
    if lowered in {"true", "yes", "on"}:
        return True
    if lowered in {"false", "no", "off"}:
        return False
    if lowered in {"null", "~"}:
        return None
    try:
        return int(text, 10)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        pass
    return text


def _split_flow(inner: str, line_number: int) -> list[str]:
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    in_single = in_double = False
    for char in inner:
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        if char == "[" and not in_single and not in_double:
            depth += 1
        elif char == "]" and not in_single and not in_double:
            depth -= 1
        if char == "," and depth == 0 and not in_single and not in_double:
            parts.append("".join(current))
            current = []
            continue
        current.append(char)
    if in_single or in_double:
        raise YamlLiteError(line_number, "unterminated quoted scalar")
    parts.append("".join(current))
    return [part for part in parts]


def _prepared_lines(text: str) -> list[tuple[int, int, str]]:
    """``(line_number, indent, content)`` for every meaningful line."""
    out: list[tuple[int, int, str]] = []
    for number, raw in enumerate(text.splitlines(), start=1):
        if "\t" in raw[: len(raw) - len(raw.lstrip())]:
            raise YamlLiteError(number, "tab used for indentation")
        content = _strip_comment(raw).rstrip()
        if not content.strip():
            continue
        stripped = content.lstrip(" ")
        for prefix, detail in _FORBIDDEN_PREFIXES.items():
            if stripped.startswith(prefix):
                raise YamlLiteError(number, detail)
        out.append((number, len(content) - len(stripped), stripped))
    return out


def _parse_block(lines: list[tuple[int, int, str]], index: int, indent: int) -> tuple[Any, int]:
    if index >= len(lines):
        return None, index
    _, first_indent, first_content = lines[index]
    if first_indent != indent:
        raise YamlLiteError(lines[index][0], f"unexpected indent {first_indent}, expected {indent}")
    if first_content.startswith("- ") or first_content == "-":
        return _parse_sequence(lines, index, indent)
    return _parse_mapping(lines, index, indent)


def _parse_mapping(lines: list[tuple[int, int, str]], index: int, indent: int) -> tuple[Any, int]:
    mapping: dict[str, Any] = {}
    while index < len(lines):
        number, line_indent, content = lines[index]
        if line_indent < indent:
            break
        if line_indent > indent:
            raise YamlLiteError(number, f"unexpected indent {line_indent}, expected {indent}")
        if content.startswith("- "):
            raise YamlLiteError(number, "sequence item inside a mapping at the same indent")
        match = _KEY.match(content)
        if match is None:
            raise YamlLiteError(number, f"expected 'key: value', got {content!r}")
        key = match.group("key").strip()
        if len(key) >= 2 and key[0] == key[-1] and key[0] in "'\"":
            key = key[1:-1]
        if key in mapping:
            # A duplicate key in YAML silently wins; in a config that controls
            # destructive operations, silently losing a setting is unacceptable.
            raise YamlLiteError(number, f"duplicate key {key!r}")
        rest = match.group("rest").strip()
        index += 1
        if rest:
            mapping[key] = _scalar(rest, number)
            continue
        if index < len(lines) and lines[index][1] > indent:
            value, index = _parse_block(lines, index, lines[index][1])
            mapping[key] = value
        else:
            mapping[key] = None
    return mapping, index


def _parse_sequence(lines: list[tuple[int, int, str]], index: int, indent: int) -> tuple[Any, int]:
    items: list[Any] = []
    while index < len(lines):
        number, line_indent, content = lines[index]
        if line_indent < indent:
            break
        if line_indent > indent:
            raise YamlLiteError(number, f"unexpected indent {line_indent}, expected {indent}")
        if not (content.startswith("- ") or content == "-"):
            break
        rest = content[2:].strip() if content.startswith("- ") else ""
        index += 1
        if not rest:
            if index < len(lines) and lines[index][1] > indent:
                value, index = _parse_block(lines, index, lines[index][1])
                items.append(value)
            else:
                items.append(None)
            continue
        if _KEY.match(rest):
            # ``- key: value`` starts a mapping whose columns begin after the
            # dash; the following lines belong to it if they line up there.
            inner_indent = indent + 2
            virtual: list[tuple[int, int, str]] = [(number, inner_indent, rest)]
            while index < len(lines) and lines[index][1] >= inner_indent:
                virtual.append(lines[index])
                index += 1
            value, consumed = _parse_mapping(virtual, 0, inner_indent)
            if consumed != len(virtual):
                raise YamlLiteError(virtual[consumed][0], "could not parse sequence item mapping")
            items.append(value)
            continue
        items.append(_scalar(rest, number))
    return items, index


def safe_load(text: str) -> Any:
    """Parse ``text`` as the supported subset. Raises :class:`YamlLiteError`."""
    lines = _prepared_lines(text)
    if not lines:
        return None
    value, index = _parse_block(lines, 0, lines[0][1])
    if index != len(lines):
        raise YamlLiteError(lines[index][0], "trailing content could not be parsed")
    return value


def load_path(path: str | Path) -> Any:
    """Load a ``.yaml``/``.yml`` file with :func:`safe_load`, or a ``.json`` file."""
    path = Path(path)
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(text)
    return safe_load(text)
