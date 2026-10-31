"""A restricted YAML writer and reader, so the lockfile needs no dependency.

The subset is deliberately small: nested mappings, lists of scalars, lists of
mappings, and the scalar types ``str``, ``int``, ``bool`` and ``None``. Anything
outside it raises rather than being guessed at, because a lockfile that loads
with a silently different meaning than it was written with is worse than one
that fails to load (P09 FR-01).

P07 made the same call in ``p07/yamlite.py`` for the same reason. This is a
separate implementation rather than an import: P09 must be able to audit P07,
and an auditor that shares a parser with the thing it audits cannot find a
parser bug.
"""

from __future__ import annotations

from typing import Any

_INDENT = "  "
_RESERVED_FIRST = "-?:,[]{}#&*!|>'\"%@`"
_RESERVED_WORDS = ("true", "false", "null", "yes", "no", "~")


class YamlSubsetError(ValueError):
    """The document, or the object being written, is outside the subset."""


# --------------------------------------------------------------------------
# writing
# --------------------------------------------------------------------------

def _needs_quoting(s: str) -> bool:
    if s == "" or s.strip() != s:
        return True
    if s[0] in _RESERVED_FIRST:
        return True
    if ": " in s or " #" in s:
        return True
    if s in _RESERVED_WORDS:
        return True
    try:
        int(s)
    except ValueError:
        return False
    # A bare scalar that would read back as a number must keep its quotes.
    return True


def _scalar(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        if _needs_quoting(value):
            escaped = value.replace("\\", "\\\\").replace('"', '\\"')
            return '"' + escaped + '"'
        return value
    raise YamlSubsetError("unsupported scalar type " + type(value).__name__)


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (bool, int, str))


def _emit_mapping(value: dict[str, Any], depth: int, out: list[str]) -> None:
    pad = _INDENT * depth
    for key, item in value.items():
        if not isinstance(key, str):
            raise YamlSubsetError("mapping key must be str, got " + type(key).__name__)
        if _is_scalar(item):
            out.append(pad + key + ": " + _scalar(item))
        elif isinstance(item, (dict, list)):
            out.append(pad + key + ":")
            _emit(item, depth + 1, out)
        else:
            raise YamlSubsetError("unsupported value type " + type(item).__name__)


def _emit_list(value: list[Any], depth: int, out: list[str]) -> None:
    pad = _INDENT * depth
    for item in value:
        if _is_scalar(item):
            out.append(pad + "- " + _scalar(item))
            continue
        if not isinstance(item, dict):
            raise YamlSubsetError("unsupported list item " + type(item).__name__)
        if not item:
            out.append(pad + "- {}")
            continue
        for index, (key, sub) in enumerate(item.items()):
            lead = pad + "- " if index == 0 else pad + _INDENT
            if _is_scalar(sub):
                out.append(lead + key + ": " + _scalar(sub))
            else:
                out.append(lead + key + ":")
                _emit(sub, depth + 2, out)


def _emit(value: Any, depth: int, out: list[str]) -> None:
    if isinstance(value, dict):
        if not value:
            out[-1] = out[-1] + " {}"
            return
        _emit_mapping(value, depth, out)
        return
    if isinstance(value, list):
        if not value:
            out[-1] = out[-1] + " []"
            return
        _emit_list(value, depth, out)
        return
    raise YamlSubsetError("unsupported document node " + type(value).__name__)


def dumps(document: dict[str, Any], header: list[str] | None = None) -> str:
    """Render ``document`` as YAML-subset text, ending in a newline."""
    if not isinstance(document, dict):
        raise YamlSubsetError("document root must be a mapping")
    out: list[str] = []
    for line in header or []:
        out.append("# " + line if line else "#")
    if header:
        out.append("")
    _emit_mapping(document, 0, out)
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------
# reading
# --------------------------------------------------------------------------

def _unescape(body: str) -> str:
    out: list[str] = []
    i = 0
    while i < len(body):
        ch = body[i]
        if ch != "\\":
            out.append(ch)
            i += 1
            continue
        if i + 1 >= len(body):
            raise YamlSubsetError("trailing escape in quoted scalar")
        nxt = body[i + 1]
        if nxt not in ('"', "\\"):
            raise YamlSubsetError("unsupported escape in quoted scalar: " + nxt)
        out.append(nxt)
        i += 2
    return "".join(out)


def _parse_scalar(text: str) -> Any:
    text = text.strip()
    if text in ("null", "~", ""):
        return None
    if text == "true":
        return True
    if text == "false":
        return False
    if text == "{}":
        return {}
    if text == "[]":
        return []
    if text.startswith('"'):
        if len(text) < 2 or not text.endswith('"'):
            raise YamlSubsetError("unterminated quoted scalar: " + text)
        return _unescape(text[1:-1])
    try:
        return int(text)
    except ValueError:
        return text


class _Line:
    __slots__ = ("indent", "text", "number")

    def __init__(self, indent: int, text: str, number: int) -> None:
        self.indent = indent
        self.text = text
        self.number = number


def _tokenize(source: str) -> list[_Line]:
    lines: list[_Line] = []
    for number, raw in enumerate(source.splitlines(), start=1):
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if raw.startswith("\t"):
            raise YamlSubsetError("line %d: tab indentation is not allowed" % number)
        indent = len(raw) - len(raw.lstrip(" "))
        if indent % 2:
            raise YamlSubsetError(
                "line %d: indent %d is not a multiple of 2" % (number, indent)
            )
        lines.append(_Line(indent // 2, stripped, number))
    return lines


def _quoted_key_end(text: str, number: int) -> int:
    end = 1
    while end < len(text):
        if text[end] == "\\":
            end += 2
            continue
        if text[end] == '"':
            return end
        end += 1
    raise YamlSubsetError("line %d: unterminated quoted key" % number)


def _split_key(text: str, number: int) -> tuple[str, str]:
    if text.startswith('"'):
        end = _quoted_key_end(text, number)
        key = _unescape(text[1:end])
        rest = text[end + 1 :]
        if not rest.startswith(":"):
            raise YamlSubsetError("line %d: expected ':' after quoted key" % number)
        return key, rest[1:].strip()
    if ":" not in text:
        raise YamlSubsetError("line %d: expected 'key: value'" % number)
    key, _, rest = text.partition(":")
    key = key.strip()
    if not key:
        raise YamlSubsetError("line %d: empty mapping key" % number)
    return key, rest.strip()


def _looks_like_mapping(body: str) -> bool:
    if ":" not in body:
        return False
    if body.startswith('"'):
        return True
    head = body.partition(":")[0].strip()
    return bool(head) and " " not in head


def _parse_block(lines: list[_Line], pos: int, depth: int) -> tuple[Any, int]:
    if pos >= len(lines):
        return None, pos
    if lines[pos].text.startswith("- "):
        return _parse_list(lines, pos, depth)
    return _parse_map(lines, pos, depth)


def _parse_map(lines: list[_Line], pos: int, depth: int) -> tuple[dict[str, Any], int]:
    result: dict[str, Any] = {}
    while pos < len(lines) and lines[pos].indent == depth:
        line = lines[pos]
        if line.text.startswith("- "):
            raise YamlSubsetError("line %d: list item inside a mapping" % line.number)
        key, rest = _split_key(line.text, line.number)
        if key in result:
            raise YamlSubsetError("line %d: duplicate key %r" % (line.number, key))
        pos += 1
        if rest:
            result[key] = _parse_scalar(rest)
            continue
        if pos < len(lines) and lines[pos].indent > depth:
            result[key], pos = _parse_block(lines, pos, lines[pos].indent)
        else:
            result[key] = None
    return result, pos


def _parse_list(lines: list[_Line], pos: int, depth: int) -> tuple[list[Any], int]:
    result: list[Any] = []
    while (
        pos < len(lines)
        and lines[pos].indent == depth
        and lines[pos].text.startswith("- ")
    ):
        line = lines[pos]
        body = line.text[2:].strip()
        pos += 1
        if body == "{}":
            result.append({})
            continue
        if not _looks_like_mapping(body):
            result.append(_parse_scalar(body))
            continue
        key, rest = _split_key(body, line.number)
        item: dict[str, Any] = {}
        if rest:
            item[key] = _parse_scalar(rest)
        elif pos < len(lines) and lines[pos].indent > depth + 1:
            item[key], pos = _parse_block(lines, pos, lines[pos].indent)
        else:
            item[key] = None
        # Continuation keys of this item sit one level in from the dash.
        while (
            pos < len(lines)
            and lines[pos].indent == depth + 1
            and not lines[pos].text.startswith("- ")
        ):
            cont = lines[pos]
            ckey, crest = _split_key(cont.text, cont.number)
            if ckey in item:
                raise YamlSubsetError("line %d: duplicate key %r" % (cont.number, ckey))
            pos += 1
            if crest:
                item[ckey] = _parse_scalar(crest)
            elif pos < len(lines) and lines[pos].indent > depth + 1:
                item[ckey], pos = _parse_block(lines, pos, lines[pos].indent)
            else:
                item[ckey] = None
        result.append(item)
    return result, pos


def loads(source: str) -> dict[str, Any]:
    """Parse YAML-subset text. Raises :class:`YamlSubsetError` outside the subset."""
    lines = _tokenize(source)
    if not lines:
        return {}
    if lines[0].indent != 0:
        raise YamlSubsetError("document must start at indent 0")
    document, pos = _parse_block(lines, 0, 0)
    if pos != len(lines):
        raise YamlSubsetError("line %d: unexpected indentation" % lines[pos].number)
    if not isinstance(document, dict):
        raise YamlSubsetError("document root must be a mapping")
    return document
