"""Global and per-partition byte budget with pause/resume hysteresis.

Spec I-05 and section 5.4. Two separate jobs that are easy to confuse:

  * **admission** -- ``try_reserve`` refuses a row that would push the tracked
    application buffer past ``app_buffer_budget_bytes``. A refusal is not an
    error; it tells the caller to pause that partition and retry later.
  * **flow control** -- ``partitions_to_pause`` / ``partitions_to_resume``
    report which partitions should be paused at the consumer. Hysteresis is
    mandatory: pausing at 80% and resuming at 50% stops the pause/resume
    flapping that a single threshold produces, which otherwise shows up as a
    rebalance storm rather than as backpressure.

Honest accounting, stated plainly: this tracks only bytes *this module was
told about*. The librdkafka fetch queue, the ClickHouse driver buffers, the
allocator free lists and fragmentation are all outside it. A run that stays
inside this budget may still exceed its RSS ceiling, so ``tests/performance``
asserts on ``psutil`` RSS and this class never claims to bound memory.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .model import TopicPartition


@dataclass(slots=True)
class BudgetSnapshot:
    """Point-in-time view, for metrics and assertions."""

    reserved_bytes: int
    budget_bytes: int
    utilisation: float
    paused: frozenset[TopicPartition]

    @property
    def available_bytes(self) -> int:
        return self.budget_bytes - self.reserved_bytes


@dataclass(slots=True)
class BudgetManager:
    budget_bytes: int
    high_watermark: float = 0.80
    low_watermark: float = 0.50

    _reserved: int = 0
    _per_tp: dict[TopicPartition, int] = field(default_factory=dict)
    _paused: set[TopicPartition] = field(default_factory=set)

    def __post_init__(self) -> None:
        if self.budget_bytes <= 0:
            raise ValueError("budget_bytes must be positive")
        if not 0.0 < self.low_watermark < self.high_watermark < 1.0:
            raise ValueError(
                "watermarks must satisfy 0 < low < high < 1 to give hysteresis"
            )

    # ------------------------------------------------------------- accounting

    @property
    def reserved_bytes(self) -> int:
        return self._reserved

    def reserved_for(self, tp: TopicPartition) -> int:
        return self._per_tp.get(tp, 0)

    @property
    def utilisation(self) -> float:
        return self._reserved / self.budget_bytes

    def try_reserve(self, tp: TopicPartition, nbytes: int) -> bool:
        """Charge ``nbytes`` to ``tp``, or return False if it would overflow.

        A single row larger than the whole budget can never be admitted. The
        caller must fail that record rather than raise the cap (spec 5.4),
        otherwise the bound is not a bound.
        """
        if nbytes < 0:
            raise ValueError("nbytes must not be negative")
        if self._reserved + nbytes > self.budget_bytes:
            return False
        self._reserved += nbytes
        self._per_tp[tp] = self._per_tp.get(tp, 0) + nbytes
        return True

    def release(self, tp: TopicPartition, nbytes: int) -> None:
        """Return bytes to the pool once a batch is acknowledged or dropped."""
        if nbytes < 0:
            raise ValueError("nbytes must not be negative")
        held = self._per_tp.get(tp, 0)
        if nbytes > held:
            # Releasing more than was reserved would silently create budget out
            # of nothing and defeat the cap, so clamp and keep the totals
            # consistent rather than drift.
            nbytes = held
        self._per_tp[tp] = held - nbytes
        if self._per_tp[tp] == 0:
            del self._per_tp[tp]
        self._reserved -= nbytes
        if self._reserved < 0:  # pragma: no cover - defensive
            self._reserved = 0

    def release_all(self, tp: TopicPartition) -> int:
        """Drop everything charged to one partition (revoke / lost path)."""
        held = self._per_tp.pop(tp, 0)
        self._reserved -= held
        if self._reserved < 0:  # pragma: no cover - defensive
            self._reserved = 0
        self._paused.discard(tp)
        return held

    # ----------------------------------------------------------- flow control

    def should_pause(self) -> bool:
        return self.utilisation >= self.high_watermark

    def should_resume(self) -> bool:
        return self.utilisation <= self.low_watermark

    def partitions_to_pause(
        self, assigned: frozenset[TopicPartition]
    ) -> list[TopicPartition]:
        """Partitions to pause now, hottest first.

        Pausing by descending reserved bytes targets the partition actually
        causing the pressure, rather than penalising an idle one. A partition
        holding nothing is never paused: it cannot relieve pressure and pausing
        it only delays its own progress.
        """
        if not self.should_pause():
            return []
        candidates = [
            tp
            for tp in assigned
            if tp not in self._paused and self._per_tp.get(tp, 0) > 0
        ]
        candidates.sort(key=lambda tp: (-self._per_tp.get(tp, 0), tp))
        self._paused.update(candidates)
        return candidates

    def partitions_to_resume(self) -> list[TopicPartition]:
        """Partitions to resume once utilisation falls back to the low mark."""
        if not self._paused or not self.should_resume():
            return []
        resumed = sorted(self._paused)
        self._paused.clear()
        return resumed

    @property
    def paused(self) -> frozenset[TopicPartition]:
        return frozenset(self._paused)

    def forget(self, tp: TopicPartition) -> None:
        self._paused.discard(tp)

    def snapshot(self) -> BudgetSnapshot:
        return BudgetSnapshot(
            reserved_bytes=self._reserved,
            budget_bytes=self.budget_bytes,
            utilisation=self.utilisation,
            paused=frozenset(self._paused),
        )
