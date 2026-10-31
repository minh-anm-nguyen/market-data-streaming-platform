"""Strict validation of a Kafka record against the P02/P04 input contract.

Implements the rules table in spec section 3.1. Two principles shape the code:

  * **cheap checks first.** Byte caps on the value, the key and the headers are
    enforced *before* the protobuf parser ever sees the bytes, so a hostile
    oversized or malformed record cannot cost a decode (spec NFR-03).
  * **never repair.** A lowercase symbol is not upper-cased, a missing field is
    not defaulted, a float-looking price is not rounded. The validator's only
    outputs are "accepted exactly as sent" and "refused with a stable reason
    code" -- per the spec module table, it must not clean malformed records.

Reason codes are stable strings. Fixtures and the what-if matrix assert on the
code, so renaming one is a contract change.
"""

from __future__ import annotations

from dataclasses import dataclass

from .config import ContractsConfig, ProjectConfig
from .model import (
    INT64_MAX,
    INT64_MIN,
    NS_PER_DAY,
    ConsumedRecord,
    EventKind,
    LogicalProjection,
)
from .proto_schema import MarketDataEvent, decode_value, unknown_field_numbers

#: Header names P04 may attach. Anything else is refused rather than ignored:
#: an unexpected header may carry routing or policy meaning that this build
#: does not understand (spec section 3.1, header row).
KNOWN_HEADERS = frozenset(
    {
        "md-schema",
        "md-descriptor-sha256",
        "md-dataset-id",
        "md-run-id",
        "md-publish-policy",
    }
)

#: event_time_ns must land inside the ClickHouse Date range, because
#: event_date is a non-nullable partition key derived from it.
MIN_EVENT_TIME_NS = 0
MAX_EVENT_TIME_NS = 65533 * NS_PER_DAY  # 2149-06-06, the ClickHouse Date ceiling

_LOWER_A = ord("a")
_LOWER_Z = ord("z")
_UPPER_A = ord("A")
_UPPER_Z = ord("Z")
_DIGIT_0 = ord("0")
_DIGIT_9 = ord("9")
_SYMBOL_PUNCT = frozenset(ord(c) for c in "._-/")


@dataclass(frozen=True, slots=True)
class ValidationOk:
    projection: LogicalProjection
    #: Unknown protobuf field numbers seen. Empty under a matching schema.
    unknown_fields: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class ValidationReject:
    code: str
    detail: str = ""


ValidationResult = ValidationOk | ValidationReject


class ContractValidator:
    """Stateless validator. One instance per run; safe to share read-only."""

    def __init__(self, contracts: ContractsConfig, project: ProjectConfig) -> None:
        self._c = contracts
        self._project = project
        self._topic_to_kind = contracts.topic_to_kind

    # ------------------------------------------------------------------ entry

    def validate(self, record: ConsumedRecord) -> ValidationResult:
        """Validate one record. Never raises for bad *data*; it returns."""
        pre = self._check_transport(record)
        if pre is not None:
            return pre

        try:
            event = decode_value(record.value)
        except Exception as exc:  # ContractViolation carries the code
            code = getattr(exc, "code", "PROTO_DECODE_ERROR")
            return ValidationReject(code, getattr(exc, "detail", str(exc)))

        unknown = unknown_field_numbers(event)
        if unknown:
            # Spec section 3.1: do not infer a new message type from unknown
            # fields. Under strict validation this is a compatibility stop.
            return ValidationReject(
                "SCHEMA_UNKNOWN_FIELDS",
                f"unknown field numbers {list(unknown)}; the producer schema "
                "may be newer, or a known field arrived with the wrong wire type",
            )

        return self._check_semantics(record, event)

    # -------------------------------------------------------- transport layer

    def _check_transport(self, record: ConsumedRecord) -> ValidationReject | None:
        """Topic allowlist, byte caps and key shape. Runs before decoding."""
        if record.tp.topic not in self._topic_to_kind:
            return ValidationReject(
                "TOPIC_NOT_ALLOWED",
                f"topic {record.tp.topic!r} is outside the contract allowlist",
            )

        if record.value is None:
            return ValidationReject("VALUE_NULL", "record value is null (tombstone?)")
        if not isinstance(record.value, (bytes, bytearray)):
            return ValidationReject(
                "VALUE_NOT_BYTES", f"value is {type(record.value).__name__}"
            )
        if len(record.value) == 0:
            return ValidationReject("VALUE_EMPTY", "record value is zero bytes")
        if len(record.value) > self._c.max_record_value_bytes:
            return ValidationReject(
                "VALUE_TOO_LARGE",
                f"{len(record.value)} bytes exceeds cap "
                f"{self._c.max_record_value_bytes}",
            )

        if record.key is None:
            return ValidationReject(
                "KEY_NULL", "record key is null; P04 must key by symbol"
            )
        if len(record.key) == 0:
            return ValidationReject("KEY_EMPTY", "record key is zero bytes")
        if len(record.key) > self._c.max_key_bytes:
            return ValidationReject(
                "KEY_TOO_LARGE",
                f"{len(record.key)} bytes exceeds cap {self._c.max_key_bytes}",
            )

        return self._check_headers(record)

    def _check_headers(self, record: ConsumedRecord) -> ValidationReject | None:
        total = 0
        seen: set[str] = set()
        for name, value in record.headers or ():
            raw_value = value or b""
            total += len(name.encode("utf-8")) + len(raw_value)
            if name not in KNOWN_HEADERS:
                return ValidationReject("HEADER_UNKNOWN", f"unexpected header {name!r}")
            if name in seen:
                return ValidationReject(
                    "HEADER_DUPLICATE",
                    f"header {name!r} appears more than once; which value "
                    "applies is undefined",
                )
            seen.add(name)
        if total > self._c.max_headers_total_bytes:
            return ValidationReject(
                "HEADERS_TOO_LARGE",
                f"{total} bytes exceeds cap {self._c.max_headers_total_bytes}",
            )
        return None

    # --------------------------------------------------------- semantic layer

    def _check_semantics(
        self, record: ConsumedRecord, event: MarketDataEvent
    ) -> ValidationResult:
        # Presence. proto3 explicit presence makes HasField meaningful: an
        # explicitly-set zero is present, while an absent field is not.
        for field_name, code in (
            ("dataset_id", "DATASET_ID_ABSENT"),
            ("symbol", "SYMBOL_ABSENT"),
            ("sequence", "SEQUENCE_ABSENT"),
            ("event_time_ns", "EVENT_TIME_ABSENT"),
            ("price_scale", "PRICE_SCALE_ABSENT"),
        ):
            if not event.HasField(field_name):
                return ValidationReject(code, f"{field_name} is not present")

        # The oneof must be set, and must match the topic it arrived on.
        variant = event.WhichOneof("payload")
        if variant is None:
            return ValidationReject(
                "PAYLOAD_ONEOF_ABSENT", "neither tick nor quote is set"
            )
        expected_variant = self._topic_to_kind[record.tp.topic]
        if variant != expected_variant:
            return ValidationReject(
                "TOPIC_PAYLOAD_MISMATCH",
                f"topic {record.tp.topic!r} requires payload {expected_variant!r}, "
                f"got {variant!r}",
            )
        kind = EventKind.TICK if variant == "tick" else EventKind.QUOTE

        dataset_id = event.dataset_id
        if dataset_id == "":
            return ValidationReject("DATASET_ID_EMPTY", "dataset_id is empty")
        dataset_bytes = len(dataset_id.encode("utf-8"))
        if dataset_bytes > self._c.max_dataset_id_bytes:
            return ValidationReject(
                "DATASET_ID_TOO_LARGE",
                f"{dataset_bytes} UTF-8 bytes exceeds cap "
                f"{self._c.max_dataset_id_bytes}",
            )

        # Symbol, and the key-to-symbol equality that makes partitioning
        # meaningful. Compared as exact bytes: no case folding, no stripping.
        symbol = event.symbol
        reject = self._check_symbol(symbol)
        if reject is not None:
            return reject
        if record.key != symbol.encode("ascii"):
            return ValidationReject(
                "KEY_SYMBOL_MISMATCH",
                f"key {record.key!r} != symbol {symbol!r} encoded as ASCII; the "
                "record sits in a partition that does not match its symbol",
            )

        reject = self._check_header_consistency(record, dataset_id)
        if reject is not None:
            return reject

        if not INT64_MIN <= event.sequence <= INT64_MAX:  # pragma: no cover
            return ValidationReject("SEQUENCE_OUT_OF_RANGE", str(event.sequence))
        if self._c.strict_market_validation and event.sequence < 0:
            return ValidationReject(
                "SEQUENCE_NEGATIVE",
                f"sequence={event.sequence}; P01 sequences increase from zero",
            )

        event_time_ns = event.event_time_ns
        if not MIN_EVENT_TIME_NS <= event_time_ns <= MAX_EVENT_TIME_NS:
            return ValidationReject(
                "EVENT_TIME_OUT_OF_RANGE",
                f"event_time_ns={event_time_ns} falls outside the supported "
                f"ClickHouse Date range [{MIN_EVENT_TIME_NS}, {MAX_EVENT_TIME_NS}]",
            )

        price_scale = event.price_scale
        if price_scale <= 0:
            return ValidationReject(
                "PRICE_SCALE_NOT_POSITIVE",
                f"price_scale={price_scale}; a non-positive scale cannot convert "
                "ticks to a price",
            )

        schema_version = event.source_schema_version
        allowed = self._c.allowed_source_schema_versions
        if allowed and schema_version not in allowed:
            return ValidationReject(
                "SCHEMA_VERSION_NOT_ALLOWED",
                f"source_schema_version={schema_version!r} not in {list(allowed)}",
            )

        if kind is EventKind.QUOTE:
            return self._build_quote(event, dataset_id, symbol, schema_version)
        return self._build_tick(event, dataset_id, symbol, schema_version)

    def _check_symbol(self, symbol: str) -> ValidationReject | None:
        if symbol == "":
            return ValidationReject("SYMBOL_EMPTY", "symbol is empty")
        try:
            raw = symbol.encode("ascii")
        except UnicodeEncodeError:
            return ValidationReject(
                "SYMBOL_NOT_ASCII", f"symbol {symbol!r} is not ASCII"
            )
        if len(raw) > self._c.max_key_bytes:
            return ValidationReject(
                "SYMBOL_TOO_LONG",
                f"{len(raw)} bytes exceeds the {self._c.max_key_bytes}-byte cap",
            )
        if self._c.strict_symbol_ascii and any(
            _LOWER_A <= ch <= _LOWER_Z for ch in raw
        ):
            return ValidationReject(
                "SYMBOL_NOT_UPPERCASE",
                f"symbol {symbol!r} contains lowercase; P04 keys are uppercase "
                "ASCII and must not be silently folded",
            )
        for ch in raw:
            if not (
                _UPPER_A <= ch <= _UPPER_Z
                or _DIGIT_0 <= ch <= _DIGIT_9
                or ch in _SYMBOL_PUNCT
            ):
                return ValidationReject(
                    "SYMBOL_CHARSET",
                    f"symbol {symbol!r} contains a character outside [A-Z0-9._-/]",
                )
        return None

    def _check_header_consistency(
        self, record: ConsumedRecord, dataset_id: str
    ) -> ValidationReject | None:
        """A header that restates a payload field must not contradict it."""
        for name, value in record.headers or ():
            raw = value or b""
            if name == "md-dataset-id":
                try:
                    header_dataset = raw.decode("utf-8")
                except UnicodeDecodeError:
                    return ValidationReject(
                        "HEADER_DATASET_ID_NOT_UTF8", "md-dataset-id is not UTF-8"
                    )
                if header_dataset != dataset_id:
                    return ValidationReject(
                        "HEADER_DATASET_ID_MISMATCH",
                        f"header md-dataset-id={header_dataset!r} contradicts "
                        f"payload dataset_id={dataset_id!r}",
                    )
            elif name == "md-schema":
                try:
                    header_schema = raw.decode("utf-8")
                except UnicodeDecodeError:
                    return ValidationReject(
                        "HEADER_SCHEMA_NOT_UTF8", "md-schema is not UTF-8"
                    )
                if header_schema and header_schema != self._c.protobuf_package:
                    return ValidationReject(
                        "HEADER_SCHEMA_MISMATCH",
                        f"header md-schema={header_schema!r} is not the pinned "
                        f"package {self._c.protobuf_package!r}",
                    )
            elif name == "md-descriptor-sha256":
                try:
                    header_hash = raw.decode("ascii").lower()
                except UnicodeDecodeError:
                    return ValidationReject(
                        "HEADER_DESCRIPTOR_NOT_ASCII",
                        "md-descriptor-sha256 is not ASCII",
                    )
                if header_hash and header_hash != self._c.protobuf_descriptor_sha256:
                    return ValidationReject(
                        "HEADER_DESCRIPTOR_MISMATCH",
                        "the producer descriptor hash differs from the pinned "
                        "one; the two sides disagree about the schema",
                    )
        return None

    # ----------------------------------------------------------- constructors

    def _build_quote(
        self,
        event: MarketDataEvent,
        dataset_id: str,
        symbol: str,
        schema_version: str,
    ) -> ValidationResult:
        quote = event.quote
        if not quote.HasField("bid_ticks"):
            return ValidationReject("QUOTE_BID_ABSENT", "quote.bid_ticks absent")
        if not quote.HasField("ask_ticks"):
            return ValidationReject("QUOTE_ASK_ABSENT", "quote.ask_ticks absent")
        bid, ask = quote.bid_ticks, quote.ask_ticks

        if self._c.strict_market_validation:
            if bid <= 0:
                return ValidationReject("QUOTE_BID_NOT_POSITIVE", f"bid_ticks={bid}")
            if ask <= 0:
                return ValidationReject("QUOTE_ASK_NOT_POSITIVE", f"ask_ticks={ask}")
            if bid > ask:
                return ValidationReject(
                    "QUOTE_CROSSED",
                    f"bid_ticks={bid} > ask_ticks={ask}; a crossed quote is not a "
                    "market-valid event in this lane",
                )

        return ValidationOk(
            LogicalProjection(
                dataset_id=dataset_id,
                symbol=symbol,
                sequence=event.sequence,
                event_time_ns=event.event_time_ns,
                price_scale=event.price_scale,
                kind=EventKind.QUOTE,
                source_schema_version=schema_version,
                bid_ticks=bid,
                ask_ticks=ask,
                price_ticks=None,
            )
        )

    def _build_tick(
        self,
        event: MarketDataEvent,
        dataset_id: str,
        symbol: str,
        schema_version: str,
    ) -> ValidationResult:
        tick = event.tick
        if not tick.HasField("price_ticks"):
            return ValidationReject("TICK_PRICE_ABSENT", "tick.price_ticks absent")
        price = tick.price_ticks
        if self._c.strict_market_validation and price <= 0:
            return ValidationReject("TICK_PRICE_NOT_POSITIVE", f"price_ticks={price}")
        return ValidationOk(
            LogicalProjection(
                dataset_id=dataset_id,
                symbol=symbol,
                sequence=event.sequence,
                event_time_ns=event.event_time_ns,
                price_scale=event.price_scale,
                kind=EventKind.TICK,
                source_schema_version=schema_version,
                bid_ticks=None,
                ask_ticks=None,
                price_ticks=price,
            )
        )
