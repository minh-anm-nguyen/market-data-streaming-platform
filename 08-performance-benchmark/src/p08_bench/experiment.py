"""A/B optimization experiments (sections 4.4, 12; NFR-11; cases E2E-45, E2E-47).

Section 12 is an experiment sheet, and every row of it is a way an optimization
claim goes wrong. This module encodes the ones that can be checked mechanically:

* **One variable.** Two knobs moved at once and the cause is unknown. The arms
  declare their config, and :meth:`PairedExperiment.confounds` lists every field
  that differs besides the declared variable -- case E2E-45, where only the B arm
  got a different CPU governor.
* **Paired, order-randomised runs.** ABBA or BAAB, not AAABBB, so a drift in the
  host across the session does not land entirely on one arm.
* **Run-level confidence.** The interval is bootstrapped over per-run values.
  Section 4.4: the millions of requests inside one run share a host and a
  minute of wall-clock and are not independent samples.
* **Correctness and durability are preconditions, not trade-offs.** Case E2E-47
  is ``acks=all`` swapped for ``acks=0``; the speedup is real and the claim is
  void. A faster arm that loses the durability contract is
  :attr:`Outcome.REJECTED_UNSAFE` regardless of its numbers.
* **Operational, not just statistical, significance.** Section 19.2: +3% median
  with a 50% p99 regression is not an improvement.

``NFR-11``'s 5% median threshold is a configurable project default, recorded per
experiment, not a scientific constant.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Sequence

from .errors import MetrologyError
from .percentiles import ConfidenceInterval, bootstrap_ci, median

#: NFR-11: the median improvement an optimization must show. A project default.
DEFAULT_MIN_MEDIAN_IMPROVEMENT = 0.05

#: How much tail regression is tolerated before the win is refused, even when
#: the median improved. Section 19.2 gives +3%/-50% as the shape of the problem.
DEFAULT_MAX_TAIL_REGRESSION = 0.10

#: Minimum paired repeats (section 4.4 point 2).
DEFAULT_MIN_PAIRS = 5


class Arm(str, Enum):
    A = "A"
    B = "B"


class Outcome(str, Enum):
    OPTIMIZED = "OPTIMIZED"
    NO_EFFECT = "NO_EFFECT"
    REGRESSED = "REGRESSED"
    INCONCLUSIVE = "INCONCLUSIVE"
    #: More than one variable differs between the arms.
    CONFOUNDED = "CONFOUNDED"
    #: The faster arm weakened durability, validation or verification.
    REJECTED_UNSAFE = "REJECTED_UNSAFE"


@dataclass(frozen=True)
class RunSample:
    """One run of one arm.

    ``higher_is_better`` lives on the experiment, not here, so a throughput
    experiment and a latency experiment use the same sample type.
    """

    arm: Arm
    order_index: int
    primary: float
    tail: float | None = None
    correctness_clean: bool | None = None
    resource_peak: float | None = None
    config_digest: str = ""
    host_profile_digest: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "arm": self.arm.value,
            "order_index": self.order_index,
            "primary": self.primary,
            "tail": self.tail,
            "correctness_clean": self.correctness_clean,
            "resource_peak": self.resource_peak,
            "config_digest": self.config_digest,
            "host_profile_digest": self.host_profile_digest,
        }


@dataclass(frozen=True)
class Hypothesis:
    """The pre-registration of section 4.4 point 1.

    Recorded before the runs so the threshold cannot move after the numbers are
    in. Section 16: "the definition of PASS does not change with the result".
    """

    statement: str
    single_variable: str
    expected_direction: str
    min_median_improvement: float = DEFAULT_MIN_MEDIAN_IMPROVEMENT
    max_tail_regression: float = DEFAULT_MAX_TAIL_REGRESSION
    min_pairs: int = DEFAULT_MIN_PAIRS
    higher_is_better: bool = True

    def __post_init__(self) -> None:
        if not self.single_variable.strip():
            raise MetrologyError("an experiment must name its single variable")
        if self.min_median_improvement < 0:
            raise MetrologyError("min_median_improvement may not be negative")
        if self.min_pairs < 2:
            raise MetrologyError("a paired experiment needs at least 2 pairs")

    def as_dict(self) -> dict[str, Any]:
        return {
            "statement": self.statement,
            "single_variable": self.single_variable,
            "expected_direction": self.expected_direction,
            "min_median_improvement": self.min_median_improvement,
            "max_tail_regression": self.max_tail_regression,
            "min_pairs": self.min_pairs,
            "higher_is_better": self.higher_is_better,
        }


def is_paired_order(order: Sequence[Arm]) -> bool:
    """Whether a run order alternates in ABBA-style blocks.

    Accepts any sequence where no arm runs three times consecutively and both
    arms appear equally often. ``AAABBB`` fails; ``ABBA`` and ``ABAB`` pass.
    Blocking matters because host drift over a session otherwise lands entirely
    on whichever arm ran last.
    """
    if not order or len(order) % 2:
        return False
    if order.count(Arm.A) != order.count(Arm.B):
        return False
    run_length = 1
    for previous, current in zip(order, order[1:]):
        run_length = run_length + 1 if current is previous else 1
        if run_length >= 3:
            return False
    return True


@dataclass
class PairedExperiment:
    """Two arms, paired repeats, and the verdict they justify."""

    hypothesis: Hypothesis
    samples: list[RunSample] = field(default_factory=list)
    #: Config fields that differ between arms *by design*: the single variable.
    declared_differences: tuple[str, ...] = ()
    #: Arbitrary per-arm metadata, compared to find confounds.
    arm_metadata: dict[Arm, Mapping[str, Any]] = field(default_factory=dict)
    #: Case E2E-47: whether each arm preserves the durability contract.
    arm_durability_intact: dict[Arm, bool] = field(default_factory=dict)

    def add(self, sample: RunSample) -> None:
        self.samples.append(sample)

    # -- populations ------------------------------------------------------
    def arm_samples(self, arm: Arm) -> list[RunSample]:
        return [s for s in self.samples if s.arm is arm]

    @property
    def order(self) -> list[Arm]:
        return [s.arm for s in sorted(self.samples, key=lambda s: s.order_index)]

    @property
    def pairs(self) -> int:
        return min(len(self.arm_samples(Arm.A)), len(self.arm_samples(Arm.B)))

    # -- checks -----------------------------------------------------------
    def confounds(self) -> list[str]:
        """Metadata fields that differ between arms and were not declared.

        Case E2E-45: the governor changed for B only, so the comparison is not
        about the knob under test.
        """
        meta_a = self.arm_metadata.get(Arm.A, {})
        meta_b = self.arm_metadata.get(Arm.B, {})
        out: list[str] = []
        for key in sorted(set(meta_a) | set(meta_b)):
            if key in self.declared_differences:
                continue
            if meta_a.get(key) != meta_b.get(key):
                out.append(
                    f"{key} differs between arms ({meta_a.get(key)!r} vs "
                    f"{meta_b.get(key)!r}) and was not declared as the variable"
                )
        digests_a = {s.host_profile_digest for s in self.arm_samples(Arm.A) if s.host_profile_digest}
        digests_b = {s.host_profile_digest for s in self.arm_samples(Arm.B) if s.host_profile_digest}
        if digests_a and digests_b and digests_a != digests_b:
            out.append("host profile digests differ between arms; the pair is confounded")
        return out

    def safety_rejections(self) -> list[str]:
        """Reasons the faster arm cannot be released however fast it is."""
        out: list[str] = []
        for arm, intact in sorted(self.arm_durability_intact.items()):
            if not intact:
                out.append(
                    f"arm {arm.value} weakens the durability contract; a speedup "
                    "bought that way is not an optimization (case E2E-47)"
                )
        for arm in (Arm.A, Arm.B):
            for sample in self.arm_samples(arm):
                if sample.correctness_clean is False:
                    out.append(
                        f"arm {arm.value} run {sample.order_index} did not reconcile "
                        "cleanly; correctness is a precondition, not a trade-off"
                    )
                    break
        return out

    def evidence_gaps(self) -> list[str]:
        out: list[str] = []
        if self.pairs < self.hypothesis.min_pairs:
            out.append(
                f"{self.pairs} paired repeat(s) against a pre-registered minimum of "
                f"{self.hypothesis.min_pairs}"
            )
        if not is_paired_order(self.order):
            out.append(
                "run order is not alternating/ABBA; host drift across the session "
                "cannot be separated from the effect"
            )
        unchecked = [
            s for s in self.samples if s.correctness_clean is None
        ]
        if unchecked:
            out.append(
                f"{len(unchecked)} run(s) have no correctness result; an unverified "
                "arm cannot be declared an optimization"
            )
        return out

    # -- effect -----------------------------------------------------------
    def _medians(self) -> tuple[float, float]:
        a = [s.primary for s in self.arm_samples(Arm.A)]
        b = [s.primary for s in self.arm_samples(Arm.B)]
        if not a or not b:
            raise MetrologyError("both arms need at least one run")
        return median(a), median(b)

    def relative_effect(self) -> float:
        """Signed improvement of B over A, normalised and direction-aware.

        Positive always means "B is better", whether the metric is throughput
        (higher is better) or latency (lower is better).
        """
        median_a, median_b = self._medians()
        if median_a == 0:
            raise MetrologyError("baseline median is zero; a relative effect is undefined")
        raw = (median_b - median_a) / abs(median_a)
        return raw if self.hypothesis.higher_is_better else -raw

    def tail_regression(self) -> float | None:
        """How much worse B's tail is than A's, as a fraction. ``None`` if unmeasured.

        A tail is always worse when it is larger, regardless of the primary
        metric direction, because every tail here is a latency.
        """
        tails_a = [s.tail for s in self.arm_samples(Arm.A) if s.tail is not None]
        tails_b = [s.tail for s in self.arm_samples(Arm.B) if s.tail is not None]
        if not tails_a or not tails_b:
            return None
        median_a, median_b = median(tails_a), median(tails_b)
        if median_a == 0:
            return None
        return (median_b - median_a) / abs(median_a)

    def confidence_interval(self, *, seed: int = 20261030, iterations: int = 2000) -> ConfidenceInterval | None:
        """Bootstrap interval over the *per-run* effect, or ``None`` if too few runs."""
        a = [s.primary for s in sorted(self.arm_samples(Arm.A), key=lambda s: s.order_index)]
        b = [s.primary for s in sorted(self.arm_samples(Arm.B), key=lambda s: s.order_index)]
        n = min(len(a), len(b))
        if n < 2:
            return None
        sign = 1.0 if self.hypothesis.higher_is_better else -1.0
        deltas = [
            sign * (b[i] - a[i]) / abs(a[i]) if a[i] else 0.0 for i in range(n)
        ]
        return bootstrap_ci(deltas, seed=seed, iterations=iterations)

    # -- verdict ----------------------------------------------------------
    def outcome(self, *, seed: int = 20261030) -> tuple[Outcome, list[str]]:
        """Classify the experiment, with the reasons behind the classification.

        Order of precedence: unsafe, then confounded, then insufficient
        evidence, then the effect itself. A confounded experiment has no effect
        worth reporting, so the effect is not computed for it.
        """
        reasons: list[str] = []

        unsafe = self.safety_rejections()
        if unsafe:
            return Outcome.REJECTED_UNSAFE, unsafe

        confounds = self.confounds()
        if confounds:
            return Outcome.CONFOUNDED, confounds

        gaps = self.evidence_gaps()
        if gaps:
            return Outcome.INCONCLUSIVE, gaps

        try:
            effect = self.relative_effect()
        except MetrologyError as exc:
            # A baseline that produced nothing is a real measurement, and the
            # ratio against it is undefined rather than infinite. The report
            # still has to render, so this is INCONCLUSIVE, not an exception.
            return Outcome.INCONCLUSIVE, [f"relative effect is undefined: {exc.message}"]
        interval = self.confidence_interval(seed=seed)
        reasons.append(f"median effect {effect:+.4f}")
        if interval is not None:
            reasons.append(
                f"run-level 95% interval [{interval.low:+.4f}, {interval.high:+.4f}] "
                f"over {interval.n_runs} pairs"
            )

        tail = self.tail_regression()
        if tail is not None and tail > self.hypothesis.max_tail_regression:
            reasons.append(
                f"tail regressed by {tail:+.4f}, above the {self.hypothesis.max_tail_regression} "
                "allowance; statistical significance does not override operational "
                "significance"
            )
            return Outcome.REGRESSED, reasons

        if effect <= -self.hypothesis.min_median_improvement:
            reasons.append("the optimized arm is worse than the baseline")
            return Outcome.REGRESSED, reasons

        if effect < self.hypothesis.min_median_improvement:
            reasons.append(
                f"effect is below the pre-registered threshold of "
                f"{self.hypothesis.min_median_improvement}"
            )
            return Outcome.NO_EFFECT, reasons

        if interval is not None and interval.low <= 0:
            reasons.append(
                "the confidence interval includes zero; the effect is not "
                "distinguishable from measurement noise"
            )
            return Outcome.INCONCLUSIVE, reasons

        return Outcome.OPTIMIZED, reasons

    def as_dict(self, *, seed: int = 20261030) -> dict[str, Any]:
        outcome, reasons = self.outcome(seed=seed)
        interval = None
        try:
            interval = self.confidence_interval(seed=seed)
        except MetrologyError:
            interval = None
        effect: float | None
        try:
            effect = self.relative_effect()
        except MetrologyError:
            effect = None
        return {
            "hypothesis": self.hypothesis.as_dict(),
            "outcome": outcome.value,
            "reasons": reasons,
            "pairs": self.pairs,
            "order": [arm.value for arm in self.order],
            "order_is_paired": is_paired_order(self.order),
            "relative_effect": effect,
            "tail_regression": self.tail_regression(),
            "confidence_interval": interval.as_dict() if interval else None,
            "confounds": self.confounds(),
            "safety_rejections": self.safety_rejections(),
            "evidence_gaps": self.evidence_gaps(),
            "samples": [s.as_dict() for s in self.samples],
        }


def reversal_holds(
    baseline: Sequence[float], optimized: Sequence[float], restored: Sequence[float], *, tolerance: float = 0.05
) -> bool:
    """Whether restoring the baseline config restores the baseline numbers.

    Section 12 asks for a reversal step, and case RB-09 is the reason: an effect
    that does not reverse was probably environmental drift rather than the knob.
    """
    if not baseline or not optimized or not restored:
        raise MetrologyError("reversal needs samples from all three phases")
    base, back = median(baseline), median(restored)
    if base == 0:
        raise MetrologyError("baseline median is zero; reversal is undefined")
    return abs(back - base) / abs(base) <= tolerance
