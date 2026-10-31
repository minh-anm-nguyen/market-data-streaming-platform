"""Schema evolution gate: descriptor diffing against a frozen baseline.

This module exists because ``protoc`` does not protect the contract. Earlier in
this project's own setup, six deliberately breaking schema variants were
compiled: renumbering ``sequence``, changing ``bid_ticks`` from ``int64`` to
``double``, making ``sequence`` unsigned, deleting ``price_scale``. Five of the
six compiled without a word of complaint. Only the one using a field number in
the 19000-19999 implementation-reserved range was refused. ``protoc`` validates
that a schema is *well-formed*, not that it is *compatible with what already
shipped* -- that is what this gate is for.

The rules implement section 5.3's allow/conditional/forbid table. The important
distinction it draws is between wire compatibility and *semantic* compatibility:

* ``int64`` to ``uint64`` is wire-compatible for non-negative values, and would
  silently reinterpret Project 01's deliberate negative fault values as huge
  positive numbers. Forbidden (case A07).
* Renaming a field keeps the wire format intact and breaks generated APIs and
  ProtoJSON. Flagged as conditional, needing review rather than a block (A09).
* Deleting a field is wire-safe if the tag is reserved, but deleting one the v1
  business contract requires is forbidden regardless (A04, A05).

A gate cannot prove semantic compatibility in general -- changing the meaning of
``price_scale`` while keeping its type is invisible here -- so the design pairs
this with ADR review and integration oracles (section 7).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Final

from google.protobuf import descriptor_pb2

from .errors import ContractError, Reason

#: Field numbers the v1 business contract requires. Removing any of these breaks
#: readers even if the tag is properly reserved (case A05).
CORE_REQUIRED_FIELDS: Final[dict[str, frozenset[str]]] = {
    "marketdata.v1.MarketDataEvent": frozenset(
        {
            "dataset_id",
            "symbol",
            "sequence",
            "event_time_ns",
            "price_scale",
            "source_schema_version",
        }
    ),
    "marketdata.v1.Quote": frozenset({"bid_ticks", "ask_ticks"}),
    "marketdata.v1.Tick": frozenset({"price_ticks"}),
}

#: Types forbidden for the integer fields of this contract. ``double`` loses
#: precision above 2^53, and the unsigned and zigzag variants change how a
#: negative value is read (cases A06, A07, A13).
FORBIDDEN_PRICE_TYPES: Final = frozenset(
    {"TYPE_DOUBLE", "TYPE_FLOAT", "TYPE_UINT64", "TYPE_UINT32", "TYPE_SINT64", "TYPE_STRING"}
)


class Severity(str, Enum):
    FORBIDDEN = "FORBIDDEN"
    CONDITIONAL = "CONDITIONAL"
    ALLOWED = "ALLOWED"


@dataclass(frozen=True)
class Finding:
    severity: Severity
    rule: str
    message: str
    #: What-if case id, so a gate failure points at a documented scenario.
    case: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "severity": self.severity.value,
            "rule": self.rule,
            "message": self.message,
            "case": self.case,
        }


@dataclass(frozen=True)
class FieldInfo:
    name: str
    number: int
    type_name: str
    label: str
    oneof_index: int | None
    type_ref: str
    #: Whether the field belongs to a oneof the *schema author* declared.
    #:
    #: This cannot be read off ``oneof_index`` alone. proto3 implements explicit
    #: `optional` presence by putting each optional field in its own synthetic
    #: oneof (named ``_feed_label`` and so on), so ``oneof_index`` is set for
    #: every one of this contract's fields. Without separating the two, adding a
    #: plain optional field would be misreported as adding a oneof variant --
    #: which inverts the gate's verdict on the single most common safe change.
    #: ``proto3_optional`` is the flag that tells them apart.
    in_real_oneof: bool


@dataclass(frozen=True)
class MessageInfo:
    full_name: str
    fields_by_number: dict[int, FieldInfo]
    fields_by_name: dict[str, FieldInfo]
    reserved_numbers: frozenset[int]
    reserved_names: frozenset[str]
    oneof_names: tuple[str, ...]


_TYPE_NAMES: Final = {
    value: name
    for name, value in descriptor_pb2.FieldDescriptorProto.Type.items()
}
_LABEL_NAMES: Final = {
    value: name
    for name, value in descriptor_pb2.FieldDescriptorProto.Label.items()
}


def load_descriptor_set(path: str | Path) -> descriptor_pb2.FileDescriptorSet:
    data = Path(path).read_bytes()
    descriptor_set = descriptor_pb2.FileDescriptorSet()
    descriptor_set.ParseFromString(data)
    return descriptor_set


def index_messages(
    descriptor_set: descriptor_pb2.FileDescriptorSet,
) -> dict[str, MessageInfo]:
    """Flatten a descriptor set into ``{full_name: MessageInfo}``."""
    out: dict[str, MessageInfo] = {}
    for file in descriptor_set.file:
        prefix = f"{file.package}." if file.package else ""
        for message in file.message_type:
            _index_message(message, prefix, out)
    return out


def _index_message(
    message: descriptor_pb2.DescriptorProto, prefix: str, out: dict[str, MessageInfo]
) -> None:
    full_name = f"{prefix}{message.name}"
    by_number: dict[int, FieldInfo] = {}
    by_name: dict[str, FieldInfo] = {}
    for field in message.field:
        info = FieldInfo(
            name=field.name,
            number=field.number,
            type_name=_TYPE_NAMES.get(field.type, str(field.type)),
            label=_LABEL_NAMES.get(field.label, str(field.label)),
            oneof_index=field.oneof_index if field.HasField("oneof_index") else None,
            type_ref=field.type_name,
            in_real_oneof=field.HasField("oneof_index") and not field.proto3_optional,
        )
        by_number[field.number] = info
        by_name[field.name] = info

    reserved_numbers: set[int] = set()
    for span in message.reserved_range:
        reserved_numbers.update(range(span.start, span.end))

    out[full_name] = MessageInfo(
        full_name=full_name,
        fields_by_number=by_number,
        fields_by_name=by_name,
        reserved_numbers=frozenset(reserved_numbers),
        reserved_names=frozenset(message.reserved_name),
        oneof_names=tuple(
            decl.name for decl in message.oneof_decl if not decl.name.startswith("_")
        ),
    )
    for nested in message.nested_type:
        _index_message(nested, f"{full_name}.", out)


def compare_descriptors(
    baseline: descriptor_pb2.FileDescriptorSet,
    candidate: descriptor_pb2.FileDescriptorSet,
) -> list[Finding]:
    """Findings for a candidate schema against the released baseline."""
    findings: list[Finding] = []
    base_messages = index_messages(baseline)
    cand_messages = index_messages(candidate)

    for name, base in base_messages.items():
        cand = cand_messages.get(name)
        if cand is None:
            findings.append(
                Finding(
                    Severity.FORBIDDEN,
                    "message_removed",
                    f"{name} exists in the baseline but not in the candidate",
                    "A10",
                )
            )
            continue
        findings.extend(_compare_message(base, cand))

    for name in sorted(set(cand_messages) - set(base_messages)):
        findings.append(
            Finding(
                Severity.ALLOWED,
                "message_added",
                f"{name} is new; adding a message is wire-safe on its own, but a "
                "new oneof variant referencing it is not (see A11)",
                "A11",
            )
        )
    return findings


def _compare_message(base: MessageInfo, cand: MessageInfo) -> list[Finding]:
    findings: list[Finding] = []
    name = base.full_name
    required = CORE_REQUIRED_FIELDS.get(name, frozenset())

    for number, base_field in sorted(base.fields_by_number.items()):
        cand_field = cand.fields_by_number.get(number)

        if cand_field is None:
            # The tag is gone. Which of three different problems this is depends
            # on whether the field was business-critical and whether the tag was
            # reserved on the way out.
            if base_field.name in required:
                findings.append(
                    Finding(
                        Severity.FORBIDDEN,
                        "core_field_removed",
                        f"{name}.{base_field.name} (tag {number}) is required by the "
                        "v1 business contract and cannot be removed, reserved or not",
                        "A05",
                    )
                )
            elif number in cand.reserved_numbers and base_field.name in cand.reserved_names:
                findings.append(
                    Finding(
                        Severity.CONDITIONAL,
                        "field_removed_and_reserved",
                        f"{name}.{base_field.name} (tag {number}) was removed with both "
                        "the tag and the name reserved; allowed only if no v1 reader "
                        "needs it",
                        "A04",
                    )
                )
            else:
                missing = []
                if number not in cand.reserved_numbers:
                    missing.append(f"tag {number}")
                if base_field.name not in cand.reserved_names:
                    missing.append(f"name {base_field.name!r}")
                findings.append(
                    Finding(
                        Severity.FORBIDDEN,
                        "field_removed_without_reserve",
                        f"{name}.{base_field.name} was removed without reserving "
                        f"{' and '.join(missing)}; the tag could be reused later and "
                        "old bytes would then be misread",
                        "A04",
                    )
                )
            continue

        if cand_field.name != base_field.name:
            # The wire format does not carry names, so this parses. It breaks
            # generated accessors and ProtoJSON field names instead.
            findings.append(
                Finding(
                    Severity.CONDITIONAL,
                    "field_renamed",
                    f"{name} tag {number} was renamed {base_field.name!r} -> "
                    f"{cand_field.name!r}; binary stays compatible but generated "
                    "APIs and ProtoJSON do not",
                    "A09",
                )
            )

        if cand_field.type_name != base_field.type_name:
            severity = Severity.FORBIDDEN
            note = ""
            if cand_field.type_name in FORBIDDEN_PRICE_TYPES:
                note = (
                    " -- this type is forbidden for v1 integer fields: double "
                    "loses precision above 2^53, and the unsigned and zigzag "
                    "variants reinterpret the deliberate negative fault values"
                )
            findings.append(
                Finding(
                    severity,
                    "field_type_changed",
                    f"{name}.{base_field.name} (tag {number}) changed type "
                    f"{base_field.type_name} -> {cand_field.type_name}{note}",
                    "A06",
                )
            )

        if cand_field.label != base_field.label:
            findings.append(
                Finding(
                    Severity.FORBIDDEN,
                    "field_label_changed",
                    f"{name}.{base_field.name} (tag {number}) changed label "
                    f"{base_field.label} -> {cand_field.label}, which changes "
                    "presence semantics",
                    "A12",
                )
            )

        if cand_field.in_real_oneof != base_field.in_real_oneof:
            findings.append(
                Finding(
                    Severity.FORBIDDEN,
                    "oneof_membership_changed",
                    f"{name}.{base_field.name} (tag {number}) moved into or out of a "
                    "oneof; this is not a wire-safe migration",
                    "A10",
                )
            )

    # A field that exists in the baseline under a different tag is a renumber,
    # which the per-tag loop above sees only as "removed" plus "added". Naming it
    # explicitly makes the gate output say what actually happened.
    for field_name, base_field in sorted(base.fields_by_name.items()):
        cand_field = cand.fields_by_name.get(field_name)
        if cand_field is not None and cand_field.number != base_field.number:
            findings.append(
                Finding(
                    Severity.FORBIDDEN,
                    "field_renumbered",
                    f"{name}.{field_name} moved from tag {base_field.number} to "
                    f"{cand_field.number}; previously serialised bytes would be "
                    "read as a different field",
                    "A01",
                )
            )

    for number, cand_field in sorted(cand.fields_by_number.items()):
        if number in base.fields_by_number:
            continue
        if number in base.reserved_numbers:
            findings.append(
                Finding(
                    Severity.FORBIDDEN,
                    "reserved_tag_reused",
                    f"{name}.{cand_field.name} uses tag {number}, which the baseline "
                    "reserved; historical bytes carry a different meaning for it",
                    "A02",
                )
            )
        elif cand_field.in_real_oneof:
            findings.append(
                Finding(
                    Severity.CONDITIONAL,
                    "oneof_variant_added",
                    f"{name}.{cand_field.name} (tag {number}) is a new oneof variant; "
                    "a v1 reader cannot interpret it and must fail closed with "
                    "UNSUPPORTED_EVENT_KIND rather than treat it as quote or tick",
                    "A11",
                )
            )
        else:
            findings.append(
                Finding(
                    Severity.ALLOWED,
                    "optional_field_added",
                    f"{name}.{cand_field.name} (tag {number}) is a new optional field; "
                    "wire-safe provided the v1 core projection is unchanged",
                    "A03",
                )
            )

    return findings


def blocking(findings: list[Finding]) -> list[Finding]:
    return [f for f in findings if f.severity is Severity.FORBIDDEN]


def gate(
    baseline_path: str | Path, candidate_path: str | Path
) -> tuple[bool, list[Finding]]:
    """Run the gate. Returns ``(passed, findings)``; conditional does not block."""
    findings = compare_descriptors(
        load_descriptor_set(baseline_path), load_descriptor_set(candidate_path)
    )
    return not blocking(findings), findings


def require_gate(baseline_path: str | Path, candidate_path: str | Path) -> list[Finding]:
    """Raise if the candidate schema breaks the contract."""
    passed, findings = gate(baseline_path, candidate_path)
    if not passed:
        raise ContractError(
            Reason.SCHEMA_GATE_REJECT,
            "; ".join(f.message for f in blocking(findings)),
            findings=[f.as_dict() for f in findings],
        )
    return findings
