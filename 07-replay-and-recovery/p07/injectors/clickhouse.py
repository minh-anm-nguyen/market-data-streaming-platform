"""ClickHouse-side faults: ambiguous INSERT ACK, pre-commit failure, disk, TTL.

The ambiguous ACK is the one worth reading carefully. Design 6.1 uses it as the
example of an injector that can *lie*: a TCP proxy cannot see "server committed"
without server-side instrumentation, so a proxy that drops the reply has not
necessarily hit the boundary it claims. This model lab can hit it exactly --
:meth:`~p07.dataplane.clickhouse_fake.FakeClickHouse.insert_raw` appends the rows
and then raises -- and the receipt says so in ``detail["boundary_precision"]``.
A real lab would have to downgrade that claim, and the field is there so a
report cannot quietly inherit the model's precision.
"""

from __future__ import annotations

from ..dataplane.clickhouse_fake import FakeClickHouse
from ..evidence.proof_ledger import FaultReceipt
from .base import DEFAULT_ALLOWLIST, LabAllowlist, inject


def drop_insert_ack(
    sink: FakeClickHouse,
    *,
    fault_id: str = "ch-ack-ambiguity",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Arm "commit, then lose the reply" on the next insert."""
    return inject(
        fault_id=fault_id,
        intended_fault="insert_ack_lost_after_commit",
        target="clickhouse_ingest",
        allowlist=allowlist,
        precondition=lambda: not sink.ack_loss_once and sink.available,
        action=lambda: setattr(sink, "ack_loss_once", True),
        observe=lambda: (sink.ack_loss_once, "ack loss armed at the post-commit boundary"),
        detail={
            "armed_only": True,
            "boundary_precision": (
                "exact in the model lab: rows are appended before the raise. A real lab using a "
                "TCP proxy cannot prove the server committed first and must report INVALID unless "
                "it has server-side instrumentation (design 6.1)."
            ),
        },
    )


def fail_before_commit(
    sink: FakeClickHouse,
    *,
    fault_id: str = "ch-fail-before-commit",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Arm a failure that happens *before* any row lands.

    The counterpart to :func:`drop_insert_ack`: same client-visible error, no
    side effect. Running both and getting the same verdict would mean the
    harness cannot tell them apart, which is why both exist.
    """
    return inject(
        fault_id=fault_id,
        intended_fault="insert_failed_before_commit",
        target="clickhouse_ingest",
        allowlist=allowlist,
        precondition=lambda: not sink.fail_before_commit_once and sink.available,
        action=lambda: setattr(sink, "fail_before_commit_once", True),
        observe=lambda: (sink.fail_before_commit_once, "pre-commit failure armed"),
        detail={"armed_only": True},
    )


def fill_disk(
    sink: FakeClickHouse,
    *,
    free_bytes: int = 0,
    fault_id: str = "ch-disk-full",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Shrink free space so the next insert cannot fit.

    The lab's disk, not the host's. Design 6.2 is explicit: "never fill host
    root FS". In this model the number is a field on the fake server, which is
    the strongest possible form of that containment.
    """
    before = sink.disk_free_bytes
    return inject(
        fault_id=fault_id,
        intended_fault="disk_full",
        target="clickhouse_server",
        allowlist=allowlist,
        precondition=lambda: sink.disk_free_bytes > free_bytes,
        action=lambda: sink.set_disk_free_bytes(free_bytes),
        observe=lambda: (
            sink.disk_free_bytes <= free_bytes,
            f"disk_free {before} -> {sink.disk_free_bytes}",
        ),
        cleanup=None,
        detail={"before_bytes": before, "after_bytes": free_bytes},
    )


def take_server_down(
    sink: FakeClickHouse,
    *,
    fault_id: str = "ch-server-down",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> FaultReceipt:
    """Make every query and insert fail until the server is brought back."""
    return inject(
        fault_id=fault_id,
        intended_fault="server_unavailable",
        target="clickhouse_server",
        allowlist=allowlist,
        precondition=lambda: sink.available,
        action=lambda: setattr(sink, "available", False),
        observe=lambda: (not sink.available, "server marked unavailable"),
        detail={},
    )


def expire_raw_ttl(
    sink: FakeClickHouse,
    *,
    before_ingested_at_ns: int,
    fault_id: str = "ch-raw-ttl",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
    confirm: bool = False,
) -> FaultReceipt:
    """Delete raw rows older than the cutoff. Destructive, needs ``confirm``.

    An audit that runs after this must report ``RAW_ROWS_EXPIRED`` and stop. A
    build that proceeds on the surviving rows would publish a snapshot whose
    completeness nobody can check (case ``E2E-33``).
    """
    before = len(sink.raw_rows)
    return inject(
        fault_id=fault_id,
        intended_fault="raw_ttl_expiry",
        target="clickhouse_server",
        allowlist=allowlist,
        destructive=True,
        confirm_destructive=confirm,
        precondition=lambda: before > 0,
        action=lambda: sink.ttl_expire_raw(before_ingested_at_ns=before_ingested_at_ns),
        observe=lambda: (
            len(sink.raw_rows) < before,
            f"raw rows {before} -> {len(sink.raw_rows)}",
        ),
        detail={"before_ingested_at_ns": before_ingested_at_ns, "rows_before": before},
    )


def drop_volume(
    sink: FakeClickHouse,
    *,
    fault_id: str = "ch-volume-loss",
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
    confirm: bool = False,
) -> FaultReceipt:
    """Lose everything. Models disaster; recovery needs a restored backup."""
    return inject(
        fault_id=fault_id,
        intended_fault="volume_loss",
        target="clickhouse_server",
        allowlist=allowlist,
        destructive=True,
        confirm_destructive=confirm,
        precondition=lambda: bool(sink.raw_rows) or sink.ready_build is not None,
        action=sink.drop_everything,
        observe=lambda: (
            not sink.raw_rows and sink.ready_build is None,
            "raw and catalog empty",
        ),
        detail={},
    )
