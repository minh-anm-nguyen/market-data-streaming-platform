"""Make the descriptor digests comparable, by deriving each one from one schema.

The first version of this audit concluded that the four descriptor pins "can
never agree" and cited protobuf's lack of a canonical serialisation. That is
true in general and it was the wrong diagnosis here. Building P03 and P04 and
reproducing both digests in Python established what actually happens:

* protobuf **3.12.4 (C++)**, **3.21.12 (C++)** and **7.35.1 (Python)** serialize
  byte-identical ``FileDescriptorProto`` for this schema, ``proto3_optional``
  included. The runtimes agree.
* The pins differ for exactly three **representational** reasons:

  1. the wrapper: a ``FileDescriptorSet`` (P02, P03) or the bare
     ``FileDescriptorProto`` (P04, P05);
  2. ``name``, which carries the path ``protoc`` was invoked with:
     ``marketdata/v1/market_data.proto`` for P02, ``market_data.proto``
     everywhere else;
  3. ``json_name``, which ``protoc --descriptor_set_out`` populates and a
     runtime ``CopyTo`` omits because it is derivable.

So the digests were never measuring different schemas. They were measuring
different encodings of one schema, and comparing them directly is the bug.

What this module does about it. Rather than ask four projects to change how they
hash, which would invalidate their own pinned self-checks, it **derives** what
each project's pin must be from one canonical descriptor by applying that
project's representation. The audit then asks each project the question it can
actually answer:

    does your pin equal the digest this schema produces in your encoding?

That is a cross-project provenance check that can pass, and that still fails if
any project's schema changes. The weaker, runtime-independent identity (the
normalised digest in :mod:`p09_integration.protoparse`) stays the primary one in
the lock; this is the sharper second check, for the projects that can produce it.

Verified values at the time of writing, all derived from the one P02 baseline:

===== ================================= ==============
proj  representation                    digest
===== ================================= ==============
P02   file_descriptor_set_protoc        ``0e6fc2b2...``
P03   file_descriptor_set_runtime       ``3363eeed...``
P04   file_descriptor_proto_runtime     ``e17bf37b...``
P05   file_descriptor_proto_runtime     ``e17bf37b...``
===== ================================= ==============
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

try:
    from google.protobuf import descriptor_pb2
except ImportError:  # pragma: no cover - reported, never guessed around
    descriptor_pb2 = None  # type: ignore[assignment]

#: Where P02 keeps the baseline this module derives everything from.
CANONICAL_BASELINE = (
    "02-market-data-protobuf-contracts/toolchain/descriptor-baselines/"
    "marketdata-v1.desc"
)

#: The path ``protoc`` was invoked with when P02 cut its baseline. It lands in
#: ``FileDescriptorProto.name`` and therefore in the digest.
PROTOC_FILE_NAME = "marketdata/v1/market_data.proto"

#: What every consumer's generated code carries instead: the bare file name.
RUNTIME_FILE_NAME = "market_data.proto"


class DescriptorError(RuntimeError):
    """The canonical descriptor could not be read, or protobuf is unavailable."""


@dataclass(frozen=True)
class Representation:
    """One way of encoding the schema descriptor into the bytes a project hashes."""

    name: str
    wrapper: str          # "set" | "proto"
    file_name: str
    keep_json_name: bool
    note: str

    def describe(self) -> str:
        return "%s(name=%s, json_name=%s)" % (
            "FileDescriptorSet" if self.wrapper == "set" else "FileDescriptorProto",
            self.file_name,
            "kept" if self.keep_json_name else "cleared",
        )


REPRESENTATIONS: dict[str, Representation] = {
    "file_descriptor_set_protoc": Representation(
        name="file_descriptor_set_protoc",
        wrapper="set",
        file_name=PROTOC_FILE_NAME,
        keep_json_name=True,
        note="protoc --descriptor_set_out: wraps in a set, keeps the invocation "
        "path in name, and populates json_name",
    ),
    "file_descriptor_set_runtime": Representation(
        name="file_descriptor_set_runtime",
        wrapper="set",
        file_name=RUNTIME_FILE_NAME,
        keep_json_name=False,
        note="descriptor()->file()->CopyTo(set.add_file()): a runtime copy "
        "wrapped in a set, so json_name is absent",
    ),
    "file_descriptor_proto_runtime": Representation(
        name="file_descriptor_proto_runtime",
        wrapper="proto",
        file_name=RUNTIME_FILE_NAME,
        keep_json_name=False,
        note="the bare runtime FileDescriptorProto; identical bytes out of "
        "C++ 3.21.12 and Python 7.35.1",
    ),
}

#: Which representation each project's own pin is expressed in. Read out of the
#: projects' source rather than assumed:
#: P02 ``scripts/generate_proto.sh`` (protoc --descriptor_set_out),
#: P03 ``src/protobuf_decoder.cpp`` (CopyTo(set.add_file())),
#: P04 ``src/serialization/marketdata_codec.cpp`` (file_proto.SerializeToString),
#: P05 ``schemas/descriptor.sha256`` (serialized FileDescriptorProto).
PROJECT_REPRESENTATION: dict[str, str] = {
    "P02": "file_descriptor_set_protoc",
    "P03": "file_descriptor_set_runtime",
    "P04": "file_descriptor_proto_runtime",
    "P05": "file_descriptor_proto_runtime",
}

#: The representation chosen as the cross-project descriptor identity: the
#: narrowest encoding, and the one two independent runtimes already agree on.
IDENTITY_REPRESENTATION = "file_descriptor_proto_runtime"


def available() -> bool:
    """Whether the protobuf runtime needed to derive digests is importable."""
    return descriptor_pb2 is not None


def load_canonical(root: Path):
    """Read P02's baseline and return the single ``FileDescriptorProto`` in it."""
    if descriptor_pb2 is None:
        raise DescriptorError(
            "the protobuf runtime is not installed, so descriptor digests cannot "
            "be derived; this is reported as INCONCLUSIVE, never as a pass"
        )
    path = root / CANONICAL_BASELINE
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise DescriptorError("cannot read %s: %s" % (CANONICAL_BASELINE, exc)) from exc

    file_set = descriptor_pb2.FileDescriptorSet()
    try:
        file_set.ParseFromString(raw)
    except Exception as exc:  # protobuf raises its own DecodeError type
        raise DescriptorError(
            "%s is not a FileDescriptorSet: %s" % (CANONICAL_BASELINE, exc)
        ) from exc
    if len(file_set.file) != 1:
        raise DescriptorError(
            "expected exactly one file in the baseline, found %d" % len(file_set.file)
        )
    return file_set.file[0]


def _encode(canonical, representation: Representation) -> bytes:
    """Re-encode the canonical descriptor the way one project would hash it."""
    if descriptor_pb2 is None:  # pragma: no cover - guarded by load_canonical
        raise DescriptorError("protobuf runtime unavailable")

    proto = descriptor_pb2.FileDescriptorProto()
    proto.CopyFrom(canonical)
    proto.name = representation.file_name
    # Never part of any project's pin, and present only when protoc is asked for
    # it, so it is always dropped.
    proto.ClearField("source_code_info")
    if not representation.keep_json_name:
        for message in proto.message_type:
            for field in message.field:
                field.ClearField("json_name")

    if representation.wrapper == "proto":
        return proto.SerializeToString()
    if representation.wrapper == "set":
        file_set = descriptor_pb2.FileDescriptorSet()
        file_set.file.add().CopyFrom(proto)
        return file_set.SerializeToString()
    raise DescriptorError("unknown wrapper: " + representation.wrapper)


def digest_for(canonical, representation_name: str) -> str:
    """The sha256 this schema yields in the named representation."""
    representation = REPRESENTATIONS.get(representation_name)
    if representation is None:
        raise DescriptorError("unknown representation: " + representation_name)
    return hashlib.sha256(_encode(canonical, representation)).hexdigest()


def canonical_identity(canonical) -> str:
    """The cross-project descriptor identity: the narrowest encoding."""
    return digest_for(canonical, IDENTITY_REPRESENTATION)


def expected_pins(root: Path) -> dict[str, str]:
    """What each project's pin must be, derived from the one canonical schema."""
    canonical = load_canonical(root)
    return {
        project: digest_for(canonical, representation)
        for project, representation in PROJECT_REPRESENTATION.items()
    }
