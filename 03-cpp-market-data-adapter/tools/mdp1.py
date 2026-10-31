"""MDP1 wire helpers and the marketdata.v1 message type, for test tooling.

This module is deliberately independent of the C++ build: it loads the
protobuf contract from a descriptor set and uses Google's *Python* protobuf
runtime to encode and decode. A bug shared by the encoder and the decoder
under test is the classic way a golden fixture proves nothing, so the two
sides must not share an implementation.
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import struct
import subprocess
import tempfile
from dataclasses import dataclass
from typing import Iterable, Iterator

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

MAGIC = b"MDP1"
MAX_PAYLOAD = 4096

_REPO = pathlib.Path(__file__).resolve().parent.parent
_PROTO = _REPO / "schemas" / "market_data.proto"


def _build_descriptor_set(protoc: str) -> bytes:
    """Runs protoc to produce a FileDescriptorSet for the pinned schema."""
    with tempfile.TemporaryDirectory() as td:
        out = pathlib.Path(td) / "market_data.desc"
        cmd = [
            protoc,
            "--experimental_allow_proto3_optional",
            "-I",
            str(_PROTO.parent),
            f"--descriptor_set_out={out}",
            str(_PROTO),
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        return out.read_bytes()


_CACHE: dict[str, type] = {}


def message_class(protoc: str = "protoc") -> type:
    """Returns the MarketDataEvent class, building the descriptor on demand."""
    if "MarketDataEvent" not in _CACHE:
        fds = descriptor_pb2.FileDescriptorSet.FromString(
            _build_descriptor_set(protoc)
        )
        pool = descriptor_pool.DescriptorPool()
        for f in fds.file:
            pool.Add(f)
        _CACHE["MarketDataEvent"] = message_factory.GetMessageClass(
            pool.FindMessageTypeByName("marketdata.v1.MarketDataEvent")
        )
    return _CACHE["MarketDataEvent"]


# --------------------------------------------------------------------------
# Event construction
# --------------------------------------------------------------------------


@dataclass
class Event:
    """A test event. `None` means the field is left absent, not zeroed --
    the difference is the whole point of proto3 explicit presence."""

    dataset_id: str | None = "ds-test"
    symbol: str | None = "EURUSD"
    sequence: int | None = 1
    event_time_ns: int | None = 0
    price_scale: int | None = 100000
    source_schema_version: str | None = "sim.v1"
    kind: str | None = "quote"  # quote | tick | None (no oneof set)
    bid_ticks: int | None = 109000
    ask_ticks: int | None = 109010
    price_ticks: int | None = 109000

    def encode(self, protoc: str = "protoc") -> bytes:
        m = message_class(protoc)()
        if self.dataset_id is not None:
            m.dataset_id = self.dataset_id
        if self.symbol is not None:
            m.symbol = self.symbol
        if self.sequence is not None:
            m.sequence = self.sequence
        if self.event_time_ns is not None:
            m.event_time_ns = self.event_time_ns
        if self.price_scale is not None:
            m.price_scale = self.price_scale
        if self.source_schema_version is not None:
            m.source_schema_version = self.source_schema_version
        if self.kind == "quote":
            if self.bid_ticks is not None:
                m.quote.bid_ticks = self.bid_ticks
            if self.ask_ticks is not None:
                m.quote.ask_ticks = self.ask_ticks
            else:
                m.quote.SetInParent()
        elif self.kind == "tick":
            if self.price_ticks is not None:
                m.tick.price_ticks = self.price_ticks
            else:
                m.tick.SetInParent()
        return m.SerializeToString()


# --------------------------------------------------------------------------
# Framing
# --------------------------------------------------------------------------


def frame(payload: bytes) -> bytes:
    """One length-delimited MDP1 frame (no magic)."""
    return struct.pack(">I", len(payload)) + payload


def stream(payloads: Iterable[bytes]) -> bytes:
    """A complete MDP1 stream: magic once, then framed payloads."""
    out = bytearray(MAGIC)
    for p in payloads:
        out += frame(p)
    return bytes(out)


def unframe(data: bytes) -> Iterator[bytes]:
    """Decodes a well-formed MDP1 stream. Raises on anything malformed, so a
    test fixture that is accidentally corrupt fails loudly at generation."""
    if not data.startswith(MAGIC):
        raise ValueError("stream does not start with the MDP1 magic")
    i = len(MAGIC)
    while i < len(data):
        if i + 4 > len(data):
            raise ValueError("truncated length header")
        (n,) = struct.unpack(">I", data[i : i + 4])
        i += 4
        if n == 0 or n > MAX_PAYLOAD:
            raise ValueError(f"length {n} outside 1..{MAX_PAYLOAD}")
        if i + n > len(data):
            raise ValueError("truncated payload")
        yield data[i : i + n]
        i += n


# --------------------------------------------------------------------------
# Artifacts
# --------------------------------------------------------------------------


def write_pbstream(
    path: pathlib.Path,
    payloads: list[bytes],
    *,
    dataset_id: str = "ds-test",
    manifest: bool = True,
    status: str = "COMPLETED",
    descriptor_sha256: str = "",
    frames_override: int | None = None,
) -> bytes:
    """Writes the stream and, by default, the sibling manifest the adapter
    requires before it will call a replay COMPLETE."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = stream(payloads)
    path.write_bytes(data)
    if manifest:
        meta = {
            "dataset_id": dataset_id,
            "sha256": hashlib.sha256(data).hexdigest(),
            "status": status,
            "frames": len(payloads) if frames_override is None else frames_override,
            "bytes": len(data),
            "schema_descriptor_sha256": descriptor_sha256,
            "producer": "p03-test-tooling",
        }
        path.with_suffix(path.suffix + ".manifest.json").write_text(
            json.dumps(meta, indent=2, sort_keys=True) + "\n"
        )
    return data


def write_raw(path: pathlib.Path, data: bytes) -> None:
    """Writes bytes verbatim with no manifest -- for malformed fixtures."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
