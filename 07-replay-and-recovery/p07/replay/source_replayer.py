"""Replay a sealed source through the P02 bridge into P03's byte stream.

This is the ``source_replay`` mode of design 4.2: read the sealed JSONL, encode
each record as ``marketdata.v1`` Protobuf, frame it as MDP1, and hand the bytes
to the adapter in whatever chunk pattern the scenario asks for. Fields are never
altered -- a replay that edits a value to make a test pass is not a replay.

The transport faults live here rather than in the adapter because that is where
they live in reality: truncating a frame, closing mid-frame, corrupting bytes and
re-sending a frame are all things that happen to a socket, not decisions the
adapter makes. Design 6.2 also keeps them off the recorded dataset: the sealed
file stays well-formed and only the bytes in flight are damaged.

The honesty note that belongs with this module: ``send()`` returning is not proof
the receiver processed anything (design 4.2, ``network_replay``). What
:func:`replay_source` reports is what it *fed*; what the adapter did with it is
the adapter's counters, and what actually reached Kafka is the scanner's
business.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from ..contracts import EventKind, SemanticCore
from ..dataplane.adapter import Adapter, chunked
from ..dataplane.codec import Codec, CodecError, DEFAULT_CODEC, frame_mdp1
from ..dataset_sealer import SealedDataset


@dataclass(frozen=True, slots=True)
class TransportFaults:
    """Byte-level damage to apply while replaying. All indices are 0-based.

    ``truncate_frame_at``
        Send only the first half of that frame's body, then stop. Models a
        process killed mid-frame (case ``E2E-07``).
    ``close_after_records``
        Close the stream after this many frames, cleanly.
    ``malformed_frame_at``
        Replace that frame's body with bytes that are not valid Protobuf.
    ``duplicate_frame_at``
        Send that frame twice, byte-identical. A physical retry on the wire.
    ``bad_magic_at``
        Corrupt the 4-byte magic of that frame, so the framer must fail closed
        rather than resynchronise.
    """

    truncate_frame_at: int | None = None
    close_after_records: int | None = None
    malformed_frame_at: int | None = None
    duplicate_frame_at: int | None = None
    bad_magic_at: int | None = None


@dataclass(frozen=True, slots=True)
class ReplayReport:
    """What the replayer fed, and what it could not encode."""

    source_lines: int
    frames_built: int
    frames_sent: int
    bytes_sent: int
    bridge_rejects: tuple[tuple[int, str], ...]
    transport_faults_applied: tuple[str, ...]
    stream_closed_reason: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_lines": self.source_lines,
            "frames_built": self.frames_built,
            "frames_sent": self.frames_sent,
            "bytes_sent": self.bytes_sent,
            "bridge_reject_count": len(self.bridge_rejects),
            "bridge_rejects_sample": [
                {"line": line, "reason": reason} for line, reason in self.bridge_rejects[:20]
            ],
            "transport_faults_applied": list(self.transport_faults_applied),
            "stream_closed_reason": self.stream_closed_reason,
            "note": "Bytes fed is not proof of processing; see the adapter and scanner reports.",
        }


def _core_from_obj(obj: Mapping[str, Any]) -> SemanticCore | None:
    """Project a parsed JSONL object onto a core, or ``None`` if impossible.

    Tolerant on purpose: this is the P02 bridge, and its job is to carry what the
    source said -- including values a validator will later reject. It returns
    ``None`` only when there is no well-typed core to build at all (a missing
    identity field, a non-integer where the schema needs an integer), because
    there is then nothing to encode.
    """
    try:
        kind = EventKind.from_wire_name(obj.get("event_type"))
    except ValueError:
        return None
    for name in ("dataset_id", "symbol", "schema_version"):
        if not isinstance(obj.get(name), str):
            return None
    for name in ("sequence", "event_time_ns", "price_scale"):
        value = obj.get(name)
        if not isinstance(value, int) or isinstance(value, bool):
            return None
    optional: dict[str, int | None] = {}
    for name in ("bid_ticks", "ask_ticks", "price_ticks"):
        value = obj.get(name)
        if value is None:
            optional[name] = None
        elif isinstance(value, int) and not isinstance(value, bool):
            optional[name] = value
        else:
            return None
    return SemanticCore(
        dataset_id=obj["dataset_id"],
        symbol=obj["symbol"],
        sequence=obj["sequence"],
        event_time_ns=obj["event_time_ns"],
        price_scale=obj["price_scale"],
        event_kind=kind,
        source_schema_version=obj["schema_version"],
        bid_ticks=optional["bid_ticks"],
        ask_ticks=optional["ask_ticks"],
        price_ticks=optional["price_ticks"],
    )


def replay_source(
    dataset: SealedDataset,
    adapter: Adapter,
    *,
    codec: Codec = DEFAULT_CODEC,
    chunk_sizes: Sequence[int] = (4096,),
    faults: TransportFaults | None = None,
) -> ReplayReport:
    """Feed the sealed dataset to ``adapter`` as an MDP1 byte stream.

    ``chunk_sizes`` cycles, so ``(1,)`` delivers one byte at a time and
    ``(1, 17)`` alternates -- the two patterns case ``E2E-04`` calls for. The
    adapter must produce the same accepted set for every pattern.
    """
    faults = faults or TransportFaults()
    applied: list[str] = []
    rejects: list[tuple[int, str]] = []
    frames_built = frames_sent = bytes_sent = 0
    source_lines = 0
    stopped = False

    for line_number, text in dataset.iter_raw_lines():
        source_lines += 1
        if stopped:
            continue
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            rejects.append((line_number, "BRIDGE_INVALID_JSON"))
            continue
        if not isinstance(obj, dict):
            rejects.append((line_number, "BRIDGE_NOT_AN_OBJECT"))
            continue
        core = _core_from_obj(obj)
        if core is None:
            rejects.append((line_number, "BRIDGE_UNENCODABLE"))
            continue
        try:
            payload = codec.encode(core)
        except CodecError as exc:
            rejects.append((line_number, f"BRIDGE_{exc.reason}"))
            continue

        index = frames_built
        frames_built += 1

        if faults.malformed_frame_at == index:
            payload = b"\xff\xff\xff\xff\xff\xff"
            applied.append(f"malformed_frame_at={index}")
        frame = frame_mdp1(payload)
        if faults.bad_magic_at == index:
            frame = b"XXXX" + frame[4:]
            applied.append(f"bad_magic_at={index}")

        if faults.truncate_frame_at == index:
            # Half a body and then nothing: the adapter must hold the partial
            # frame and emit no event when the stream ends.
            cut = 8 + max(1, len(payload) // 2)
            for chunk in chunked(frame[:cut], chunk_sizes):
                adapter.feed(chunk)
                bytes_sent += len(chunk)
            applied.append(f"truncate_frame_at={index}")
            stopped = True
            break

        repeats = 2 if faults.duplicate_frame_at == index else 1
        if repeats == 2:
            applied.append(f"duplicate_frame_at={index}")
        for _ in range(repeats):
            for chunk in chunked(frame, chunk_sizes):
                adapter.feed(chunk)
                bytes_sent += len(chunk)
            frames_sent += 1

        if faults.close_after_records is not None and frames_sent >= faults.close_after_records:
            applied.append(f"close_after_records={faults.close_after_records}")
            stopped = True
            break

    adapter.close("EOF" if not stopped else "INJECTED_CLOSE")
    return ReplayReport(
        source_lines=source_lines,
        frames_built=frames_built,
        frames_sent=frames_sent,
        bytes_sent=bytes_sent,
        bridge_rejects=tuple(rejects),
        transport_faults_applied=tuple(applied),
        stream_closed_reason=adapter.counters.closed_reason,
    )


def replay_cores(
    cores: Iterable[SemanticCore],
    adapter: Adapter,
    *,
    codec: Codec = DEFAULT_CODEC,
    chunk_sizes: Sequence[int] = (4096,),
) -> int:
    """Feed an explicit sequence of cores. Returns frames sent.

    Used by property tests and by the small shrunken counter-examples design
    12.4 asks for, where building a whole P01 fixture would be overkill.
    """
    sent = 0
    for core in cores:
        for chunk in chunked(frame_mdp1(codec.encode(core)), chunk_sizes):
            adapter.feed(chunk)
        sent += 1
    adapter.close()
    return sent
