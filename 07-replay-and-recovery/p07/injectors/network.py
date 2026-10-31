"""Transport faults on the P01-to-P03 byte stream, and a counting proxy.

The byte-level damage itself is applied by
:class:`~p07.replay.source_replayer.TransportFaults`, because that is where the
bytes are produced. This module wraps it so the damage comes with a receipt, and
adds :class:`CountingProxy` -- a stand-in for the user-space proxy design 6.2
lists, whose counters are what independently observes that the fault reached the
wire.

The honest limit, from design 4.2: a ``send()`` that returns proves nothing
about the receiver. The proxy counts bytes *offered*; whether the adapter
processed them is the adapter's counters, and whether anything reached Kafka is
the scanner's.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..evidence.proof_ledger import FaultReceipt
from ..replay.source_replayer import TransportFaults
from .base import DEFAULT_ALLOWLIST, LabAllowlist, inject


@dataclass(slots=True)
class CountingProxy:
    """Counts and optionally damages the bytes passing through.

    ``drop_after_bytes`` stops forwarding once the budget is spent, which models
    a connection that goes dead mid-stream without a clean close -- distinct
    from a graceful close, because the adapter's framer must treat a partial
    frame as a partial frame either way.
    """

    forward: Callable[[bytes], None]
    drop_after_bytes: int | None = None
    bytes_offered: int = 0
    bytes_forwarded: int = 0
    chunks_offered: int = 0
    dropped: bool = False
    reset_requested: bool = False

    def feed(self, chunk: bytes) -> None:
        self.chunks_offered += 1
        self.bytes_offered += len(chunk)
        if self.dropped:
            return
        if self.drop_after_bytes is not None and self.bytes_forwarded >= self.drop_after_bytes:
            self.dropped = True
            return
        self.forward(chunk)
        self.bytes_forwarded += len(chunk)

    def as_dict(self) -> dict[str, Any]:
        return {
            "bytes_offered": self.bytes_offered,
            "bytes_forwarded": self.bytes_forwarded,
            "chunks_offered": self.chunks_offered,
            "dropped": self.dropped,
            "note": "bytes forwarded is not proof the receiver processed them",
        }


def arm_transport_fault(
    faults: TransportFaults,
    *,
    description: str,
    applied_check: Callable[[], bool],
    fault_id: str = "network-transport-fault",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Receipt for a byte-stream fault whose application is checked afterwards.

    ``applied_check`` is evaluated after the replay and must read the replay
    report -- the list of faults the replayer says it actually applied -- rather
    than the plan. A plan that targeted frame 500 of a 300-frame stream applied
    nothing, and this is where that gets caught.
    """
    return inject(
        fault_id=fault_id,
        intended_fault=description,
        target="network_proxy",
        allowlist=allowlist,
        precondition=lambda: any(
            value is not None
            for value in (
                faults.truncate_frame_at,
                faults.close_after_records,
                faults.malformed_frame_at,
                faults.duplicate_frame_at,
                faults.bad_magic_at,
            )
        ),
        action=lambda: description,
        observe=lambda: (applied_check(), description),
        detail={
            "truncate_frame_at": faults.truncate_frame_at,
            "close_after_records": faults.close_after_records,
            "malformed_frame_at": faults.malformed_frame_at,
            "duplicate_frame_at": faults.duplicate_frame_at,
            "bad_magic_at": faults.bad_magic_at,
        },
    )


def cut_connection(
    proxy: CountingProxy,
    *,
    after_bytes: int,
    fault_id: str = "network-cut",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Arm the proxy to stop forwarding after ``after_bytes``."""
    return inject(
        fault_id=fault_id,
        intended_fault="connection_cut",
        target="network_proxy",
        allowlist=allowlist,
        precondition=lambda: not proxy.dropped,
        action=lambda: setattr(proxy, "drop_after_bytes", after_bytes),
        observe=lambda: (proxy.drop_after_bytes == after_bytes, "cut armed"),
        detail={"after_bytes": after_bytes, "armed_only": True},
    )
