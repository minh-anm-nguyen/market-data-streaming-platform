"""MDP1 framing: Protobuf does not delimit messages on a stream (section 4).

A serialised Protobuf message is not self-delimiting. Concatenate two of them
and the result parses as a *single* message with the later fields overwriting
the earlier ones -- silently, with no error. So a stream needs an explicit
length prefix, and that framing is a protocol in its own right:

    MDP1                     once, at the start of a connection or file
    [uint32 BE len][payload] repeated, 1 <= len <= 4096

Three decisions in here matter more than they look:

**Length is validated before allocation.** A hostile 4-byte header claiming
0xffffffff must not cause a 4 GiB reserve. The check happens on the integer, not
after a read (cases D08, D09, J03).

**Deadlines are absolute, not per-read.** A client that sends one byte just
before each timeout expires would hold a connection open forever if the deadline
reset on every byte. Every multi-byte read here shares one deadline computed
once from a monotonic clock (cases D12, I14).

**A corrupted stream is not resynchronised.** On a bad magic, length or body the
reader stops at that offset and reports it. Scanning forward for the next
plausible ``MDP1`` would silently skip an unknown amount of data and present the
remainder as if nothing had been lost (ADR-007). The offline corpus runner gets
isolation a different way: it runs each frame as its own case.
"""

from __future__ import annotations

import os
import socket
import struct
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import BinaryIO, Final, Iterator, Protocol

from .errors import FramingError, Reason

#: One-time stream prefix. Versions the *framing*, not the ``.proto`` contract:
#: an incompatible framing change becomes MDP2 on a different port, never a
#: sniffed variant of this one (section 4.1, ADR-004).
MAGIC: Final = b"MDP1"
MAGIC_LEN: Final = 4
HEADER_LEN: Final = 4

MAX_PAYLOAD_BYTES: Final = 4096
#: Header plus the largest legal payload. Reported in budgets so a queue's byte
#: accounting includes framing overhead rather than payload alone (NFR-04).
MAX_FRAME_BYTES: Final = HEADER_LEN + MAX_PAYLOAD_BYTES

DEFAULT_DEADLINE_S: Final = 2.0

#: Marker files for a ``.pbstream`` treated as a committed artifact (I10).
PBSTREAM_COMPLETED_SUFFIX: Final = ".COMPLETED"

#: The sibling manifest Project 03 reads to establish source completeness. Its
#: own docstring explains why EOF is not enough: a truncated file also ends.
PBSTREAM_MANIFEST_SUFFIX: Final = ".manifest.json"

_BE32 = struct.Struct(">I")


class StreamEnd(str, Enum):
    """How a frame stream finished."""

    #: Clean EOF exactly on a frame boundary.
    COMPLETED = "COMPLETED"
    #: EOF or a deadline part way through a prefix, header or body.
    INCOMPLETE = "INCOMPLETE"
    #: A structural violation: bad magic, illegal length, undecodable frame.
    FAILED = "FAILED"


class ByteSource(Protocol):
    """Minimal reader interface, so files and sockets share one framing path."""

    def read_some(self, size: int, deadline: float | None) -> bytes:
        """Return 1..size bytes, or ``b""`` at EOF. May return fewer than asked."""


@dataclass
class BytesSource:
    """In-memory source. Used by the fixture corpus and the fuzz harness."""

    data: bytes
    pos: int = 0
    #: Forces fragmentation, so the reassembly path is exercised with the same
    #: code that serves sockets (cases D16, D17).
    chunk_size: int | None = None

    def read_some(self, size: int, deadline: float | None) -> bytes:
        if self.chunk_size is not None:
            size = min(size, self.chunk_size)
        out = self.data[self.pos : self.pos + size]
        self.pos += len(out)
        return out


@dataclass
class FileSource:
    handle: BinaryIO

    def read_some(self, size: int, deadline: float | None) -> bytes:
        return self.handle.read(size)


@dataclass
class SocketSource:
    """Socket source honouring an absolute deadline.

    The per-call ``settimeout`` is derived from the time *remaining* against the
    shared deadline, which is what makes a trickle of bytes unable to extend the
    read indefinitely.
    """

    sock: socket.socket

    def read_some(self, size: int, deadline: float | None) -> bytes:
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise FramingError(Reason.READ_DEADLINE_EXCEEDED, "deadline passed")
            self.sock.settimeout(remaining)
        try:
            return self.sock.recv(size)
        except socket.timeout as exc:
            raise FramingError(Reason.READ_DEADLINE_EXCEEDED, "recv timed out") from exc


def read_exact(
    source: ByteSource,
    size: int,
    *,
    deadline: float | None,
    truncated_reason: Reason,
    offset: int,
) -> bytes:
    """Read exactly ``size`` bytes or raise.

    Returning short is never acceptable for a header or a body: a 3-byte read of
    a 4-byte length field is not "a small header", it is a truncated stream. The
    distinction between "nothing at all" and "some but not all" is made by the
    caller, because at a frame boundary zero bytes is a clean end of stream.
    """
    chunks: list[bytes] = []
    got = 0
    while got < size:
        if deadline is not None and time.monotonic() > deadline:
            raise FramingError(
                Reason.READ_DEADLINE_EXCEEDED,
                f"after {got} of {size} bytes",
                offset=offset + got,
            )
        block = source.read_some(size - got, deadline)
        if not block:
            raise FramingError(
                truncated_reason,
                f"EOF after {got} of {size} bytes",
                offset=offset + got,
                bytes_read=got,
            )
        chunks.append(block)
        got += len(block)
    return b"".join(chunks)


@dataclass
class FrameReader:
    """Reads an MDP1 stream frame by frame.

    Stateful on purpose: the magic is consumed exactly once, and ``offset``
    tracks the byte position so a rejection can name where the stream went wrong
    (case I13 needs the offset to be exact).
    """

    source: ByteSource
    max_payload_bytes: int = MAX_PAYLOAD_BYTES
    deadline_s: float | None = DEFAULT_DEADLINE_S
    offset: int = 0
    magic_read: bool = False
    frames_read: int = 0
    end: StreamEnd | None = None

    def _deadline(self) -> float | None:
        return None if self.deadline_s is None else time.monotonic() + self.deadline_s

    def read_magic(self) -> None:
        """Consume the one-time prefix. Raises on mismatch or truncation."""
        if self.magic_read:
            return
        try:
            prefix = read_exact(
                self.source,
                MAGIC_LEN,
                deadline=self._deadline(),
                truncated_reason=Reason.TRUNCATED_PREFIX,
                offset=self.offset,
            )
        except FramingError:
            self.end = StreamEnd.INCOMPLETE
            raise
        self.offset += MAGIC_LEN
        if prefix != MAGIC:
            # A JSONL line arriving on this port lands here. Fail closed; there
            # is deliberately no fallback to a JSON parser, because accepting
            # two protocols on one port is how payloads get misinterpreted
            # (cases D01, D02, I08).
            self.end = StreamEnd.FAILED
            raise FramingError(
                Reason.BAD_MAGIC,
                f"expected {MAGIC!r}, got {prefix!r}",
                offset=0,
            )
        self.magic_read = True

    def read_frame(self) -> bytes | None:
        """Next payload, or ``None`` on a clean end of stream at a frame boundary."""
        self.read_magic()
        deadline = self._deadline()

        # Zero bytes here is the one legitimate stopping point; anything between
        # 1 and 3 bytes is a truncated header.
        header = b""
        while len(header) < HEADER_LEN:
            block = self.source.read_some(HEADER_LEN - len(header), deadline)
            if not block:
                if not header:
                    self.end = StreamEnd.COMPLETED
                    return None
                self.end = StreamEnd.INCOMPLETE
                raise FramingError(
                    Reason.TRUNCATED_HEADER,
                    f"EOF after {len(header)} of {HEADER_LEN} header bytes",
                    offset=self.offset + len(header),
                )
            if deadline is not None and time.monotonic() > deadline:
                self.end = StreamEnd.INCOMPLETE
                raise FramingError(
                    Reason.READ_DEADLINE_EXCEEDED,
                    "while reading header",
                    offset=self.offset + len(header),
                )
            header += block

        header_offset = self.offset
        self.offset += HEADER_LEN
        (length,) = _BE32.unpack(header)

        if length == 0 or length > self.max_payload_bytes:
            # Validated as an integer, before a single byte of payload is read
            # and before anything is allocated for it. A little-endian sender's
            # swapped header lands here too, as an absurd length, and is failed
            # closed rather than guessed at (cases D07, D08, D09, D10, J03).
            self.end = StreamEnd.FAILED
            raise FramingError(
                Reason.INVALID_LENGTH,
                f"length {length} outside 1..{self.max_payload_bytes}",
                offset=header_offset,
                advertised_length=length,
            )

        try:
            payload = read_exact(
                self.source,
                length,
                deadline=deadline,
                truncated_reason=Reason.TRUNCATED_BODY,
                offset=self.offset,
            )
        except FramingError:
            self.end = StreamEnd.INCOMPLETE
            raise
        self.offset += length
        self.frames_read += 1
        return payload

    def __iter__(self) -> Iterator[bytes]:
        while (payload := self.read_frame()) is not None:
            yield payload


@dataclass
class FrameWriter:
    """Writes an MDP1 stream. The magic goes out exactly once."""

    handle: BinaryIO
    max_payload_bytes: int = MAX_PAYLOAD_BYTES
    magic_written: bool = False
    frames_written: int = 0
    bytes_written: int = 0

    def write_magic(self) -> None:
        if self.magic_written:
            return
        self.handle.write(MAGIC)
        self.bytes_written += MAGIC_LEN
        self.magic_written = True

    def write_frame(self, payload: bytes) -> int:
        """Frame and write one payload; returns the bytes written for this frame."""
        if not payload:
            raise FramingError(Reason.INVALID_LENGTH, "refusing to write a 0-length frame")
        if len(payload) > self.max_payload_bytes:
            raise FramingError(
                Reason.INVALID_LENGTH,
                f"payload {len(payload)} > {self.max_payload_bytes}",
            )
        self.write_magic()
        self.handle.write(_BE32.pack(len(payload)))
        self.handle.write(payload)
        written = HEADER_LEN + len(payload)
        self.bytes_written += written
        self.frames_written += 1
        return written


@dataclass
class PbStreamWriter:
    """Writes a ``.pbstream`` as a committed artifact.

    Mirrors Project 01's durability discipline: write ``.part``, flush, fsync,
    rename, then write the ``COMPLETED`` marker carrying the count and hash. A
    process killed at any point leaves either no marker or a marker that does
    not match, and ``verify`` reports INCOMPLETE rather than success.

    ``close()`` is explicitly **not** treated as proof of commit: a successful
    ``close`` only means the file descriptor is gone, not that the data reached
    stable storage (cases I10, I11, I12).
    """

    path: Path
    max_payload_bytes: int = MAX_PAYLOAD_BYTES
    fsync: bool = True
    #: The dataset this stream carries. Written into the source manifest that
    #: Project 03 reads, because an artifact that cannot say which dataset it
    #: belongs to cannot be reconciled against a source (P03 exit code 3).
    #: ``None`` still produces a manifest, with the field absent, so a consumer
    #: that requires it fails loudly rather than matching the wrong dataset.
    dataset_id: str | None = None
    producer: str = "p02-encoder"
    _handle: BinaryIO | None = field(default=None, init=False, repr=False)
    _writer: FrameWriter | None = field(default=None, init=False, repr=False)
    committed: bool = field(default=False, init=False)

    @property
    def part_path(self) -> Path:
        return self.path.with_suffix(self.path.suffix + ".part")

    @property
    def completed_path(self) -> Path:
        return Path(str(self.path) + PBSTREAM_COMPLETED_SUFFIX)

    @property
    def manifest_path(self) -> Path:
        """The sibling manifest Project 03 reads.

        A separate file from ``.COMPLETED`` rather than a rename, because
        ``.COMPLETED`` is this project's own durability marker and its tests and
        tooling read it. The manifest is the *consumer's* contract, documented in
        ``03-cpp-market-data-adapter/docs/protocol_handoff.md``, and it needs a
        ``status`` field and a ``dataset_id`` the marker never carried. Writing
        both costs one small file and removes the reason P09 had to shim between
        them (finding P09-M0-19).
        """
        return Path(str(self.path) + PBSTREAM_MANIFEST_SUFFIX)

    def __enter__(self) -> "PbStreamWriter":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.part_path.open("wb")
        self._writer = FrameWriter(self._handle, max_payload_bytes=self.max_payload_bytes)
        self._writer.write_magic()
        return self

    def write_frame(self, payload: bytes) -> int:
        assert self._writer is not None, "writer used outside its context manager"
        return self._writer.write_frame(payload)

    @property
    def frames_written(self) -> int:
        return self._writer.frames_written if self._writer else 0

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        assert self._handle is not None
        self._handle.flush()
        if self.fsync:
            os.fsync(self._handle.fileno())
        self._handle.close()
        self._handle = None
        if exc_type is not None:
            # Leave the .part in place and write no marker. An operator can see
            # a failed run's output; they cannot mistake it for a complete one.
            return
        os.replace(self.part_path, self.path)
        import hashlib
        import json

        digest = hashlib.sha256(self.path.read_bytes()).hexdigest()
        size = self.path.stat().st_size

        # This project's own durability marker. Unchanged.
        self.completed_path.write_text(
            json.dumps(
                {
                    "frames": self.frames_written,
                    "bytes": size,
                    "sha256": digest,
                    "frame_protocol": MAGIC.decode("ascii"),
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        # The consumer's contract. Same facts, the shape Project 03 specified:
        # it checks `status == "COMPLETED"`, that `bytes` equals the real file
        # size and that `sha256` equals the real digest, and it needs a
        # `dataset_id` to reconcile against a source. Written after the marker
        # so a crash between the two leaves the weaker claim, never the stronger.
        manifest: dict[str, object] = {
            "sha256": digest,
            "status": "COMPLETED",
            "frames": self.frames_written,
            "bytes": size,
            "producer": self.producer,
        }
        if self.dataset_id is not None:
            manifest["dataset_id"] = self.dataset_id
        self.manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        self.committed = True


def iter_frames_from_file(
    path: str | Path,
    *,
    max_payload_bytes: int = MAX_PAYLOAD_BYTES,
) -> Iterator[bytes]:
    """Stream frames out of a ``.pbstream``. No deadline: a file is not slowloris."""
    with open(path, "rb") as handle:
        reader = FrameReader(
            FileSource(handle), max_payload_bytes=max_payload_bytes, deadline_s=None
        )
        yield from reader


def frame_bytes(payload: bytes) -> bytes:
    """One framed record without the stream prefix. For crafting fixtures."""
    return _BE32.pack(len(payload)) + payload


def encode_stream(payloads: list[bytes]) -> bytes:
    """A complete in-memory MDP1 stream. For fixtures and round-trip tests."""
    return MAGIC + b"".join(frame_bytes(p) for p in payloads)
