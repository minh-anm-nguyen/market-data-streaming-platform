"""Encode/decode with exactly ONE schema variant, in its own process.

The compatibility matrix has to be run by genuinely separate artifacts
(section 5.4), and for Python that is not a style preference -- it is forced.
Every generated module calls ``AddSerializedFile`` on the *default* descriptor
pool, and all variants here declare the same file name
``marketdata/v1/market_data.proto`` and the same message full names. Importing
two of them in one interpreter raises::

    TypeError: Couldn't build proto file into descriptor pool:
    duplicate file name marketdata/v1/market_data.proto

So a v1 reader and a v1.1 writer cannot coexist in one process, and this module
is the subprocess entry point that loads one variant and nothing else. It
deliberately does **not** import :mod:`md_contract.codec`, because that would
pull in the v1 generated module and defeat the whole arrangement.

    python -m md_contract.variant_runner --variant v1_1 --op encode \\
        --input events.jsonl --output v1_1.pbstream --feed-label prod-a

    python -m md_contract.variant_runner --variant v1 --op decode \\
        --input v1_1.pbstream --output core.jsonl
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any

from .domain import (
    ENVELOPE_FIELDS,
    PAYLOAD_NONE,
    PAYLOAD_QUOTE,
    PAYLOAD_TICK,
    PAYLOAD_UNKNOWN,
    DomainEvent,
    projection_line,
)
from .errors import ContractError
from .framing import FileSource, FrameReader, PbStreamWriter
from .strict_json import parse_sim_v1_line

VARIANTS = ("v1", "v1_1", "v2_probe")

#: Tags 10-19 are the payload oneof range (see the schema contract).
PAYLOAD_TAG_RANGE = range(10, 20)


def load_variant(name: str) -> Any:
    if name not in VARIANTS:
        raise SystemExit(f"unknown variant {name!r}; expected one of {VARIANTS}")
    return importlib.import_module(
        f"md_contract.generated.{name}.marketdata.v1.market_data_pb2"
    )


def encode(pb: Any, event: DomainEvent, feed_label: str | None) -> bytes:
    msg = pb.MarketDataEvent()
    for field_name in ENVELOPE_FIELDS:
        value = getattr(event, field_name)
        if value is not None:
            setattr(msg, field_name, value)
    if event.payload_case == PAYLOAD_QUOTE:
        # SetInParent() for the same reason as in codec.py: reading msg.quote
        # does not set the oneof case in Python, only mutating it does.
        quote = msg.quote
        quote.SetInParent()
        if event.bid_ticks is not None:
            quote.bid_ticks = event.bid_ticks
        if event.ask_ticks is not None:
            quote.ask_ticks = event.ask_ticks
    elif event.payload_case == PAYLOAD_TICK:
        tick = msg.tick
        tick.SetInParent()
        if event.price_ticks is not None:
            tick.price_ticks = event.price_ticks
    # Only v1.1 and later have feed_label; asking for it against v1 is a
    # configuration error rather than something to ignore.
    if feed_label is not None:
        if not hasattr(msg, "feed_label"):
            raise SystemExit("this variant has no feed_label field")
        msg.feed_label = feed_label
    return msg.SerializeToString()


def encode_depth_update(pb: Any, event: DomainEvent, level: int, size_ticks: int) -> bytes:
    """A payload using the v2-only oneof variant, for the fail-closed tests.

    This is how a newer writer's event actually looks on the wire, which is the
    only honest way to test what a v1 reader does with one (cases A11, D20, G06).
    """
    msg = pb.MarketDataEvent()
    for field_name in ENVELOPE_FIELDS:
        value = getattr(event, field_name)
        if value is not None:
            setattr(msg, field_name, value)
    if not hasattr(msg, "depth_update"):
        raise SystemExit("this variant has no depth_update variant")
    msg.depth_update.level = level
    msg.depth_update.size_ticks = size_ticks
    return msg.SerializeToString()


def decode(pb: Any, payload: bytes) -> tuple[DomainEvent, dict[str, Any]]:
    """Decode with this variant, reporting what it could and could not read."""
    from google.protobuf import unknown_fields

    msg = pb.MarketDataEvent()
    msg.ParseFromString(payload)

    values: dict[str, Any] = {}
    for field_name in ENVELOPE_FIELDS:
        if msg.HasField(field_name):
            values[field_name] = getattr(msg, field_name)

    case = msg.WhichOneof("payload")
    unknown = tuple(
        sorted(field.field_number for field in unknown_fields.UnknownFieldSet(msg))
    )
    extra: dict[str, Any] = {"unknown_field_numbers": list(unknown)}
    # feed_label's presence is reported separately from the core projection: the
    # matrix asserts the core is unchanged by it, and asserts its presence as a
    # distinct property, because core equality and lossless forwarding are
    # different guarantees (sections 5.2, 5.5).
    if hasattr(msg, "feed_label"):
        extra["has_feed_label"] = msg.HasField("feed_label")
        extra["feed_label"] = msg.feed_label if msg.HasField("feed_label") else None

    if case == PAYLOAD_QUOTE:
        event = DomainEvent(
            **values,
            payload_case=PAYLOAD_QUOTE,
            bid_ticks=msg.quote.bid_ticks if msg.quote.HasField("bid_ticks") else None,
            ask_ticks=msg.quote.ask_ticks if msg.quote.HasField("ask_ticks") else None,
        )
    elif case == PAYLOAD_TICK:
        event = DomainEvent(
            **values,
            payload_case=PAYLOAD_TICK,
            price_ticks=(
                msg.tick.price_ticks if msg.tick.HasField("price_ticks") else None
            ),
        )
    elif case is not None:
        # A variant this build knows but the v1 projection has no shape for
        # (depth_update, in the v2 probe build).
        event = DomainEvent(**values, payload_case=PAYLOAD_UNKNOWN)
        extra["known_variant"] = case
    else:
        has_unknown_payload = any(n in PAYLOAD_TAG_RANGE for n in unknown)
        event = DomainEvent(
            **values,
            payload_case=PAYLOAD_UNKNOWN if has_unknown_payload else PAYLOAD_NONE,
        )
    return event, extra


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="md_contract.variant_runner")
    parser.add_argument("--variant", required=True, choices=VARIANTS)
    parser.add_argument("--op", required=True, choices=("encode", "decode", "describe"))
    parser.add_argument("--input")
    parser.add_argument("--output")
    parser.add_argument("--feed-label")
    parser.add_argument(
        "--payload",
        default="source",
        choices=("source", "depth_update"),
        help="'depth_update' emits the v2-only oneof variant (needs --variant v2_probe)",
    )
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args(argv)

    pb = load_variant(args.variant)
    report: dict[str, Any] = {
        "variant": args.variant,
        "op": args.op,
        "python_protobuf_runtime_version": _runtime_version(),
    }

    if args.op == "describe":
        msg = pb.MarketDataEvent()
        report["fields"] = sorted(f.name for f in msg.DESCRIPTOR.fields)
        report["payload_variants"] = sorted(
            f.name for f in msg.DESCRIPTOR.oneofs_by_name["payload"].fields
        )
        print(json.dumps(report, sort_keys=True))
        return 0

    if args.op == "encode":
        written = rejected = 0
        with PbStreamWriter(Path(args.output)) as writer:
            with open(args.input, "rb") as handle:
                for raw in handle:
                    if args.limit and written >= args.limit:
                        break
                    try:
                        event = parse_sim_v1_line(raw)
                    except ContractError as exc:
                        rejected += 1
                        report.setdefault("first_error", exc.reason.value)
                        continue
                    payload = (
                        encode_depth_update(pb, event, level=1, size_ticks=250_000)
                        if args.payload == "depth_update"
                        else encode(pb, event, args.feed_label)
                    )
                    writer.write_frame(payload)
                    written += 1
        report["events_encoded"] = written
        report["rejected"] = rejected
        report["committed"] = True
        print(json.dumps(report, sort_keys=True))
        return 0 if rejected == 0 else 1

    # decode
    accepted = 0
    unknown_kind = 0
    decode_errors = 0
    extras: list[dict[str, Any]] = []
    out_path = Path(args.output) if args.output else None
    out = out_path.open("wb") if out_path else None
    reader: FrameReader | None = None
    try:
        with open(args.input, "rb") as handle:
            reader = FrameReader(FileSource(handle), deadline_s=None)
            while True:
                try:
                    payload = reader.read_frame()
                except ContractError as exc:
                    report["framing_error"] = exc.reason.value
                    break
                if payload is None:
                    break
                try:
                    event, extra = decode(pb, payload)
                except Exception as exc:  # noqa: BLE001
                    # Counted and reported, never silently skipped: a swallowed
                    # parse failure inflates throughput and loses events
                    # (section 10.3).
                    decode_errors += 1
                    report.setdefault("first_decode_error", type(exc).__name__)
                    continue
                if event.payload_case == PAYLOAD_UNKNOWN:
                    unknown_kind += 1
                    # Fails closed: counted and not written out, never coerced
                    # into a quote or a tick (case G06).
                    continue
                accepted += 1
                if out is not None:
                    out.write(projection_line(event))
                if len(extras) < 3:
                    extras.append(extra)
    finally:
        if out is not None:
            out.close()

    report["events_accepted"] = accepted
    report["unsupported_event_kind"] = unknown_kind
    report["decode_errors"] = decode_errors
    report["frames_read"] = reader.frames_read if reader else 0
    report["stream_end"] = reader.end.value if reader and reader.end else None
    report["sample_extras"] = extras
    print(json.dumps(report, sort_keys=True))
    return 0 if decode_errors == 0 else 1


def _runtime_version() -> str:
    import google.protobuf

    return google.protobuf.__version__


if __name__ == "__main__":
    sys.exit(main())
