"""P02: the ``marketdata.v1`` Protobuf schema, built at runtime, plus MDP1 framing.

The schema is assembled from a ``FileDescriptorProto`` rather than shipped as a
generated ``_pb2`` module, for two reasons. It keeps the repository free of
generated artefacts that would drift from the design, and it lets a test build a
*deliberately skewed* descriptor (a retyped field, an extra field, a renumbered
one) to exercise the version-skew cases in group ``G03`` without a second
toolchain.

This is the real ``protobuf`` runtime, not a hand-rolled encoder, which matters
for the one property those cases hinge on: **explicit field presence**. Every
scalar is declared ``optional`` in proto3, so ``price_ticks = 0`` present and
``price_ticks`` absent are distinguishable on the wire and after a round trip
(case ``P07-G03-01``).

Framing lives here too. MDP1 is P03's *transport* framing -- ``b"MDP1"`` then a
big-endian ``uint32`` length then the payload -- and it must never appear in a
Kafka record value. :func:`strip_mdp1_if_present` exists so the Kafka scanner can
*detect* that mistake rather than quietly parse around it (case ``P07-G03-12``).
"""

from __future__ import annotations

import hashlib
import struct
from typing import Any, Final

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.message import DecodeError, Message

from ..contracts import PROTOBUF_PACKAGE, EventKind, SemanticCore

MDP1_MAGIC: Final = b"MDP1"
_MDP1_LENGTH = struct.Struct(">I")

#: P03 rule 1: the frame length must be in ``[1, 4096]`` *before* any allocation.
MDP1_MIN_LENGTH: Final = 1
MDP1_MAX_LENGTH: Final = 4096

#: The scalar field names, in declaration order. Named once so both the
#: descriptor builder and :meth:`Codec.encode` use the same list.
_SCALAR_NAMES: Final = (
    "dataset_id",
    "symbol",
    "sequence",
    "event_time_ns",
    "price_scale",
    "source_schema_version",
)

_SCALARS: Final = (
    (1, "dataset_id", descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
    (2, "symbol", descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
    (3, "sequence", descriptor_pb2.FieldDescriptorProto.TYPE_INT64),
    (4, "event_time_ns", descriptor_pb2.FieldDescriptorProto.TYPE_INT64),
    (5, "price_scale", descriptor_pb2.FieldDescriptorProto.TYPE_INT64),
    (6, "source_schema_version", descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
)


class CodecError(ValueError):
    """Decode or framing failure, with a machine-readable reason."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


def _optional_scalar(
    message: descriptor_pb2.DescriptorProto, number: int, name: str, field_type: int
) -> None:
    """Add a proto3 ``optional`` scalar, i.e. one with explicit presence.

    proto3 spells optional presence as a synthetic single-member oneof, so each
    field gets its own ``_name`` oneof. Without this, a zero value would be
    indistinguishable from an absent field and the presence tests become
    meaningless.
    """
    field = message.field.add()
    field.name = name
    field.number = number
    field.type = field_type
    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    field.proto3_optional = True
    field.oneof_index = len(message.oneof_decl)
    message.oneof_decl.add().name = "_" + name


def build_file_descriptor(
    *,
    package: str = PROTOBUF_PACKAGE,
    sequence_type: int = descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
    extra_event_field: tuple[int, str, int] | None = None,
    drop_fields: frozenset[str] = frozenset(),
) -> descriptor_pb2.FileDescriptorProto:
    """The ``marketdata.v1`` file descriptor, with hooks for skew tests.

    ``sequence_type``
        Retype ``sequence`` to produce an incompatible schema
        (case ``P07-G03-11``).
    ``extra_event_field``
        ``(number, name, type)`` added to ``MarketDataEvent``, so an encoder can
        emit a field this version's decoder keeps only as unknown
        (cases ``P07-G03-04``, ``P07-G03-14``).
    ``drop_fields``
        Names omitted entirely, for a decoder that is missing a field the
        encoder sent.
    """
    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name = "marketdata/v1/market_data.proto"
    fdp.package = package
    fdp.syntax = "proto3"

    quote = fdp.message_type.add()
    quote.name = "Quote"
    _optional_scalar(quote, 1, "bid_ticks", descriptor_pb2.FieldDescriptorProto.TYPE_INT64)
    _optional_scalar(quote, 2, "ask_ticks", descriptor_pb2.FieldDescriptorProto.TYPE_INT64)

    tick = fdp.message_type.add()
    tick.name = "Tick"
    _optional_scalar(tick, 1, "price_ticks", descriptor_pb2.FieldDescriptorProto.TYPE_INT64)

    event = fdp.message_type.add()
    event.name = "MarketDataEvent"
    # The payload oneof must be index 0 so the tick/quote fields can point at it
    # before the synthetic presence oneofs are appended.
    event.oneof_decl.add().name = "payload"
    for number, name, field_type in _SCALARS:
        if name in drop_fields:
            continue
        actual_type = sequence_type if name == "sequence" else field_type
        _optional_scalar(event, number, name, actual_type)
    for number, name, type_name in ((10, "quote", ".Quote"), (11, "tick", ".Tick")):
        field = event.field.add()
        field.name = name
        field.number = number
        field.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        field.type_name = f".{package}{type_name}"
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        field.oneof_index = 0
    if extra_event_field is not None:
        number, name, field_type = extra_event_field
        _optional_scalar(event, number, name, field_type)
    return fdp


class Codec:
    """Encode and decode ``MarketDataEvent`` for one pinned descriptor.

    Each instance owns a private :class:`~google.protobuf.descriptor_pool.DescriptorPool`,
    so two codecs with different descriptors can coexist in one process -- which
    is what a skew test needs, and what a shared default pool would refuse by
    raising on the duplicate symbol.
    """

    def __init__(self, descriptor: descriptor_pb2.FileDescriptorProto | None = None) -> None:
        self._fdp = descriptor or build_file_descriptor()
        self._pool = descriptor_pool.DescriptorPool()
        self._pool.Add(self._fdp)
        full_name = f"{self._fdp.package}.MarketDataEvent"
        self._message_class = message_factory.GetMessageClass(
            self._pool.FindMessageTypeByName(full_name)
        )
        self.descriptor_sha256 = hashlib.sha256(self._fdp.SerializeToString()).hexdigest()
        self.package = self._fdp.package
        #: Resolved once. Reading it from ``DESCRIPTOR.fields`` per field per
        #: event made :meth:`encode` 75x slower than :meth:`decode`, which is
        #: enough to dominate a whole scenario run.
        self._field_names = frozenset(field.name for field in self._message_class.DESCRIPTOR.fields)
        self._scalar_names = tuple(
            name for name in _SCALAR_NAMES if name in self._field_names
        )

    @property
    def message_class(self) -> type[Message]:
        return self._message_class

    def encode(self, core: SemanticCore, *, extra: dict[str, Any] | None = None) -> bytes:
        """Serialise ``core``. ``extra`` sets fields only a skewed schema has."""
        message = self._message_class()
        for name in self._scalar_names:
            setattr(message, name, getattr(core, name))
        if core.event_kind is EventKind.QUOTE:
            if core.bid_ticks is not None:
                message.quote.bid_ticks = core.bid_ticks
            if core.ask_ticks is not None:
                message.quote.ask_ticks = core.ask_ticks
            if core.bid_ticks is None and core.ask_ticks is None:
                # Still select the oneof, so "quote with no prices" stays
                # distinguishable from "no payload at all".
                message.quote.SetInParent()
        else:
            if core.price_ticks is not None:
                message.tick.price_ticks = core.price_ticks
            else:
                message.tick.SetInParent()
        for name, value in (extra or {}).items():
            setattr(message, name, value)
        return message.SerializeToString()

    def decode(self, payload: bytes) -> SemanticCore:
        """Decode to a :class:`~p07.contracts.SemanticCore`, presence preserved.

        Raises :class:`CodecError` rather than returning a partially filled core:
        a decode failure must produce *no* event, per P03 rule 2.
        """
        if len(payload) > MDP1_MAX_LENGTH:
            raise CodecError("PAYLOAD_TOO_LARGE", str(len(payload)))
        message = self._message_class()
        try:
            message.ParseFromString(payload)
        except (DecodeError, ValueError) as exc:
            raise CodecError("PROTO_DECODE_ERROR", str(exc)) from exc

        for name in self._scalar_names:
            if not message.HasField(name):
                raise CodecError("STRUCTURAL_REJECT", f"missing {name}")
        which = message.WhichOneof("payload")
        if which is None:
            raise CodecError("STRUCTURAL_REJECT", "no payload oneof")

        bid = ask = price = None
        if which == "quote":
            kind = EventKind.QUOTE
            if message.quote.HasField("bid_ticks"):
                bid = message.quote.bid_ticks
            if message.quote.HasField("ask_ticks"):
                ask = message.quote.ask_ticks
        else:
            kind = EventKind.TICK
            if message.tick.HasField("price_ticks"):
                price = message.tick.price_ticks

        return SemanticCore(
            dataset_id=message.dataset_id,
            symbol=message.symbol,
            sequence=message.sequence,
            event_time_ns=message.event_time_ns,
            price_scale=message.price_scale,
            event_kind=kind,
            source_schema_version=message.source_schema_version,
            bid_ticks=bid,
            ask_ticks=ask,
            price_ticks=price,
        )

    def unknown_field_bytes(self, payload: bytes) -> int:
        """How many bytes this descriptor could not interpret.

        Reported in the skew findings. It is deliberately *not* folded into the
        semantic digest: a digest that changed when an unknown field appeared
        would make two semantically identical events look like a conflict
        (case ``P07-G03-04``).
        """
        message = self._message_class()
        try:
            message.ParseFromString(payload)
        except (DecodeError, ValueError) as exc:
            raise CodecError("PROTO_DECODE_ERROR", str(exc)) from exc
        return len(message.UnknownFields().__str__().encode()) if message.UnknownFields() else 0


#: The pinned production codec. Tests that need skew build their own.
DEFAULT_CODEC = Codec()


def frame_mdp1(payload: bytes) -> bytes:
    """Wrap ``payload`` in one MDP1 frame."""
    if not MDP1_MIN_LENGTH <= len(payload) <= MDP1_MAX_LENGTH:
        raise CodecError("INVALID_LENGTH", str(len(payload)))
    return MDP1_MAGIC + _MDP1_LENGTH.pack(len(payload)) + payload


def looks_like_mdp1(value: bytes) -> bool:
    """Whether ``value`` starts with the MDP1 transport prefix."""
    return value[:4] == MDP1_MAGIC


def strip_mdp1_if_present(value: bytes) -> tuple[bytes, bool]:
    """``(payload, had_prefix)``.

    The caller is expected to *fail* on ``had_prefix`` for a Kafka record rather
    than use the stripped payload; the stripping exists so the error message can
    show what was wrong.
    """
    if not looks_like_mdp1(value):
        return value, False
    if len(value) < 8:
        raise CodecError("TRUNCATED_HEADER", str(len(value)))
    (length,) = _MDP1_LENGTH.unpack(value[4:8])
    return value[8 : 8 + length], True
