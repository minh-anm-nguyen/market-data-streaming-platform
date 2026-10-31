"""Normalise a ``.proto`` file down to the part that is actually a contract.

Four projects each keep their own copy of ``marketdata.v1``. Comparing the files
by SHA-256 says they differ, which is true and useless: the difference is
whitespace. Comparing them by descriptor digest also says they differ, and that
is true for a third reason again (see ``audit.DESCRIPTOR_DIGEST_NOTE``).

What a consumer actually depends on is narrower: the package, the message names,
and for each field its number, type, label and oneof membership. That is what is
extracted here, canonically ordered, and hashed. Two copies with the same
normalised digest are the same wire contract however they are formatted.

This is a text parser, not ``protoc``. It covers the subset the platform's
schema uses and **raises on anything it does not recognise**, so an unparsed
construct can never be silently dropped from the comparison. A normalised digest
is therefore evidence about a file this parser fully understood, and it refuses
to be evidence about one it did not.

A runtime descriptor is deliberately not used here. The descriptors are the
thing under audit, so deriving the comparison from one would reproduce the very
divergence this module exists to detect.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path

_SCALARS = frozenset(
    {
        "double", "float", "int32", "int64", "uint32", "uint64", "sint32",
        "sint64", "fixed32", "fixed64", "sfixed32", "sfixed64", "bool",
        "string", "bytes",
    }
)
_LABELS = ("optional", "repeated", "required")

_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
_TYPE = r"\.?" + _IDENT + r"(?:\." + _IDENT + r")*"

_RE_SYNTAX = re.compile(r'^syntax\s*=\s*"(proto2|proto3)"$')
_RE_PACKAGE = re.compile(r"^package\s+(" + _TYPE + r")$")
_RE_IMPORT = re.compile(r'^import\s+(?:public\s+|weak\s+)?"([^"]+)"$')
_RE_OPTION = re.compile(r"^option\s+.+$")
_RE_MESSAGE = re.compile(r"^message\s+(" + _IDENT + r")\s*\{$")
_RE_ENUM = re.compile(r"^enum\s+(" + _IDENT + r")\s*\{$")
_RE_ONEOF = re.compile(r"^oneof\s+(" + _IDENT + r")\s*\{$")
_RE_RESERVED = re.compile(r"^reserved\s+(.+)$")
_RE_ENUM_VALUE = re.compile(r"^(" + _IDENT + r")\s*=\s*(-?\d+)(?:\s*\[[^\]]*\])?$")
_RE_FIELD = re.compile(
    r"^(?:(" + "|".join(_LABELS) + r")\s+)?"
    r"(" + _TYPE + r")\s+"
    r"(" + _IDENT + r")\s*=\s*(\d+)"
    r"(?:\s*\[([^\]]*)\])?$"
)


class ProtoParseError(ValueError):
    """The file used a construct this parser will not guess at."""


@dataclass(frozen=True)
class Field:
    name: str
    number: int
    type: str
    label: str          # optional | repeated | required | singular | oneof
    oneof: str | None
    options: str        # normalised field options, "" when absent

    def canonical(self) -> str:
        return "field %d %s %s %s oneof=%s opts=%s" % (
            self.number, self.type, self.name, self.label,
            self.oneof or "-", self.options or "-",
        )


@dataclass
class Message:
    name: str
    fields: list[Field] = field(default_factory=list)
    oneofs: list[str] = field(default_factory=list)
    reserved: list[str] = field(default_factory=list)
    nested: list[str] = field(default_factory=list)

    def canonical_lines(self) -> list[str]:
        lines = ["message " + self.name]
        lines.extend("  oneof " + name for name in sorted(self.oneofs))
        lines.extend("  reserved " + item for item in sorted(self.reserved))
        lines.extend(
            "  " + f.canonical() for f in sorted(self.fields, key=lambda x: x.number)
        )
        return lines


@dataclass
class Enum:
    name: str
    values: list[tuple[str, int]] = field(default_factory=list)

    def canonical_lines(self) -> list[str]:
        lines = ["enum " + self.name]
        lines.extend(
            "  value %d %s" % (number, name)
            for name, number in sorted(self.values, key=lambda x: (x[1], x[0]))
        )
        return lines


@dataclass
class ProtoFile:
    """The contract-bearing content of one ``.proto`` file."""

    path: str
    syntax: str
    package: str
    imports: list[str]
    messages: dict[str, Message]
    enums: dict[str, Enum]
    file_sha256: str

    def canonical_text(self) -> str:
        """The normal form: formatting, comments and file path all removed."""
        lines = ["syntax " + self.syntax, "package " + self.package]
        lines.extend("import " + name for name in sorted(self.imports))
        for name in sorted(self.messages):
            lines.extend(self.messages[name].canonical_lines())
        for name in sorted(self.enums):
            lines.extend(self.enums[name].canonical_lines())
        return "\n".join(lines) + "\n"

    def normalized_sha256(self) -> str:
        return hashlib.sha256(self.canonical_text().encode("utf-8")).hexdigest()


def _strip_comments(source: str) -> str:
    """Remove ``//`` and ``/* */`` comments without touching string literals."""
    out: list[str] = []
    i = 0
    n = len(source)
    quote = ""
    while i < n:
        ch = source[i]
        if quote:
            out.append(ch)
            if ch == "\\" and i + 1 < n:
                out.append(source[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = ""
            i += 1
            continue
        if ch in ('"', "'"):
            quote = ch
            out.append(ch)
            i += 1
            continue
        if ch == "/" and i + 1 < n and source[i + 1] == "/":
            while i < n and source[i] != "\n":
                i += 1
            continue
        if ch == "/" and i + 1 < n and source[i + 1] == "*":
            end = source.find("*/", i + 2)
            if end == -1:
                raise ProtoParseError("unterminated block comment")
            # Keep the newlines so reported line numbers stay usable.
            out.append("\n" * source.count("\n", i, end))
            i = end + 2
            continue
        out.append(ch)
        i += 1
    if quote:
        raise ProtoParseError("unterminated string literal")
    return "".join(out)


def _statements(source: str) -> list[tuple[str, int]]:
    """Split into statements tagged with a 1-based line number.

    ``;`` and the braces are the only separators the subset uses.
    """
    statements: list[tuple[str, int]] = []
    buffer: list[str] = []
    line = 1
    start_line = 1
    for ch in source:
        if ch == "\n":
            line += 1
        if ch in ";{}":
            text = " ".join("".join(buffer).split())
            if ch == "{":
                if not text:
                    raise ProtoParseError("line %d: '{' with no declaration" % line)
                statements.append((text + " {", start_line))
            elif ch == "}":
                if text:
                    statements.append((text, start_line))
                statements.append(("}", line))
            elif text:
                statements.append((text, start_line))
            buffer = []
            start_line = line
            continue
        buffer.append(ch)
    tail = " ".join("".join(buffer).split())
    if tail:
        raise ProtoParseError(
            "line %d: trailing text without ';': %r" % (start_line, tail)
        )
    return statements


def _normalise_options(raw: str | None) -> str:
    if not raw:
        return ""
    parts = [" ".join(p.split()) for p in raw.split(",")]
    return ",".join(sorted(p for p in parts if p))


def _normalise_reserved(raw: str) -> list[str]:
    return sorted(" ".join(p.split()) for p in raw.split(",") if p.strip())


def _parse_field(text: str, line: int, syntax: str, oneof: str | None) -> Field:
    match = _RE_FIELD.match(text)
    if not match:
        raise ProtoParseError(
            "line %d: unrecognised field declaration: %r" % (line, text)
        )
    label, ftype, name, number, options = match.groups()
    if label == "required" and syntax == "proto3":
        raise ProtoParseError("line %d: 'required' is not valid in proto3" % line)
    if oneof is not None and label:
        raise ProtoParseError(
            "line %d: a oneof field may not carry label %r" % (line, label)
        )
    return Field(
        name=name,
        number=int(number),
        type=ftype[1:] if ftype.startswith(".") else ftype,
        label=label or ("singular" if oneof is None else "oneof"),
        oneof=oneof,
        options=_normalise_options(options),
    )


class _Parser:
    """Recursive descent over the statement list, with an explicit scope stack."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.syntax = ""
        self.package = ""
        self.imports: list[str] = []
        self.messages: dict[str, Message] = {}
        self.enums: dict[str, Enum] = {}

    def parse(self, statements: list[tuple[str, int]]) -> None:
        pos = 0
        while pos < len(statements):
            text, line = statements[pos]
            pos += 1
            if text == "}":
                raise ProtoParseError("line %d: unmatched '}'" % line)
            if match := _RE_SYNTAX.match(text):
                if self.syntax:
                    raise ProtoParseError("line %d: duplicate syntax statement" % line)
                self.syntax = match.group(1)
                continue
            if match := _RE_PACKAGE.match(text):
                if self.package:
                    raise ProtoParseError("line %d: duplicate package statement" % line)
                self.package = match.group(1)
                continue
            if match := _RE_IMPORT.match(text):
                self.imports.append(match.group(1))
                continue
            if _RE_OPTION.match(text):
                # File options do not change the wire contract.
                continue
            if match := _RE_MESSAGE.match(text):
                pos = self._parse_message(statements, pos, match.group(1), prefix="")
                continue
            if match := _RE_ENUM.match(text):
                pos = self._parse_enum(statements, pos, match.group(1), prefix="")
                continue
            raise ProtoParseError(
                "line %d: unsupported top-level statement: %r" % (line, text)
            )
        if not self.syntax:
            raise ProtoParseError(
                "no syntax statement; the proto2 default is too risky to assume"
            )

    def _parse_message(
        self, statements: list[tuple[str, int]], pos: int, name: str, prefix: str
    ) -> int:
        qualified = prefix + name
        if qualified in self.messages:
            raise ProtoParseError("duplicate message %s" % qualified)
        message = Message(name=qualified)
        self.messages[qualified] = message
        oneof: str | None = None
        while pos < len(statements):
            text, line = statements[pos]
            pos += 1
            if text == "}":
                if oneof is not None:
                    oneof = None
                    continue
                return pos
            if match := _RE_ONEOF.match(text):
                if oneof is not None:
                    raise ProtoParseError("line %d: nested oneof is not supported" % line)
                oneof = match.group(1)
                message.oneofs.append(oneof)
                continue
            if match := _RE_MESSAGE.match(text):
                if oneof is not None:
                    raise ProtoParseError("line %d: message inside a oneof" % line)
                child = match.group(1)
                message.nested.append(qualified + "." + child)
                pos = self._parse_message(statements, pos, child, prefix=qualified + ".")
                continue
            if match := _RE_ENUM.match(text):
                if oneof is not None:
                    raise ProtoParseError("line %d: enum inside a oneof" % line)
                pos = self._parse_enum(statements, pos, match.group(1), prefix=qualified + ".")
                continue
            if match := _RE_RESERVED.match(text):
                message.reserved.extend(_normalise_reserved(match.group(1)))
                continue
            if _RE_OPTION.match(text):
                continue
            message.fields.append(_parse_field(text, line, self.syntax, oneof))
        raise ProtoParseError("unterminated message %s" % qualified)

    def _parse_enum(
        self, statements: list[tuple[str, int]], pos: int, name: str, prefix: str
    ) -> int:
        qualified = prefix + name
        if qualified in self.enums:
            raise ProtoParseError("duplicate enum %s" % qualified)
        enum = Enum(name=qualified)
        self.enums[qualified] = enum
        while pos < len(statements):
            text, line = statements[pos]
            pos += 1
            if text == "}":
                return pos
            if _RE_OPTION.match(text) or _RE_RESERVED.match(text):
                continue
            if match := _RE_ENUM_VALUE.match(text):
                enum.values.append((match.group(1), int(match.group(2))))
                continue
            raise ProtoParseError("line %d: unrecognised enum entry: %r" % (line, text))
        raise ProtoParseError("unterminated enum %s" % qualified)


def _resolvable(type_name: str, parser: _Parser) -> bool:
    if type_name in parser.messages or type_name in parser.enums:
        return True
    if parser.package and type_name.startswith(parser.package + "."):
        bare = type_name[len(parser.package) + 1 :]
        if bare in parser.messages or bare in parser.enums:
            return True
    suffix = "." + type_name
    if any(known.endswith(suffix) for known in (*parser.messages, *parser.enums)):
        return True
    # An import could supply it; without reading the imported file this parser
    # cannot tell, and says so by accepting rather than claiming a mismatch.
    return bool(parser.imports)


def _check_field_numbers(parser: _Parser) -> None:
    for name, message in parser.messages.items():
        seen: dict[int, str] = {}
        for f in message.fields:
            if f.number in seen:
                raise ProtoParseError(
                    "message %s reuses field number %d (%s and %s)"
                    % (name, f.number, seen[f.number], f.name)
                )
            if not 1 <= f.number <= 536_870_911:
                raise ProtoParseError(
                    "message %s field %s has out-of-range number %d"
                    % (name, f.name, f.number)
                )
            if 19_000 <= f.number <= 19_999:
                raise ProtoParseError(
                    "message %s field %s uses a reserved number %d"
                    % (name, f.name, f.number)
                )
            seen[f.number] = f.name
            if f.type not in _SCALARS and not _resolvable(f.type, parser):
                raise ProtoParseError(
                    "message %s field %s has unresolved type %r"
                    % (name, f.name, f.type)
                )


def parse_text(source: str, path: str = "<memory>") -> ProtoFile:
    """Parse ``.proto`` text into its contract-bearing normal form."""
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
    parser = _Parser(path)
    parser.parse(_statements(_strip_comments(source)))
    _check_field_numbers(parser)
    return ProtoFile(
        path=path,
        syntax=parser.syntax,
        package=parser.package,
        imports=parser.imports,
        messages=parser.messages,
        enums=parser.enums,
        file_sha256=digest,
    )


def parse_file(path: Path) -> ProtoFile:
    """Parse a ``.proto`` from disk, reading it as UTF-8."""
    return parse_text(path.read_text(encoding="utf-8"), path=path.as_posix())


def diff_contracts(left: ProtoFile, right: ProtoFile) -> list[str]:
    """Differences that change the wire or the presence contract."""
    notes: list[str] = []
    if left.syntax != right.syntax:
        notes.append("syntax: %s vs %s" % (left.syntax, right.syntax))
    if left.package != right.package:
        notes.append("package: %s vs %s" % (left.package, right.package))

    lmsg, rmsg = set(left.messages), set(right.messages)
    for name in sorted(lmsg - rmsg):
        notes.append("message %s only in %s" % (name, left.path))
    for name in sorted(rmsg - lmsg):
        notes.append("message %s only in %s" % (name, right.path))
    for name in sorted(lmsg & rmsg):
        lf = {f.number: f for f in left.messages[name].fields}
        rf = {f.number: f for f in right.messages[name].fields}
        for number in sorted(set(lf) - set(rf)):
            notes.append(
                "%s field %d (%s) only in %s" % (name, number, lf[number].name, left.path)
            )
        for number in sorted(set(rf) - set(lf)):
            notes.append(
                "%s field %d (%s) only in %s" % (name, number, rf[number].name, right.path)
            )
        for number in sorted(set(lf) & set(rf)):
            if lf[number].canonical() != rf[number].canonical():
                notes.append(
                    "%s field %d differs: %s vs %s"
                    % (name, number, lf[number].canonical(), rf[number].canonical())
                )
        if set(left.messages[name].oneofs) != set(right.messages[name].oneofs):
            notes.append(
                "%s oneofs differ: %s vs %s"
                % (
                    name,
                    sorted(left.messages[name].oneofs),
                    sorted(right.messages[name].oneofs),
                )
            )

    lenum, renum = set(left.enums), set(right.enums)
    for name in sorted(lenum ^ renum):
        notes.append(
            "enum %s only in %s" % (name, left.path if name in lenum else right.path)
        )
    for name in sorted(lenum & renum):
        if sorted(left.enums[name].values) != sorted(right.enums[name].values):
            notes.append("enum %s values differ" % name)
    return notes
