// Counters, conservation equations and the latency histogram. Group N.
#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <vector>

#include "md_adapter/metrics.hpp"

using md::LatencyHistogram;
using md::Metrics;
using md::Reason;

namespace {

// A balanced book, used as the starting point for the violation tests.
Metrics balanced() {
  Metrics m;
  m.frames_complete = 100;
  m.frames_decode_error = 5;
  m.frames_decoded = 95;
  m.structural_reject = 10;
  m.structural_ok = 85;
  m.business_reject = 15;
  m.business_ok = 70;
  m.policy_suppressed = 20;
  m.classified_output_attempted = 50;
  m.output_success = 48;
  m.output_failed_or_pending = 2;
  for (int i = 0; i < 10; ++i) m.structural_reject_by_reason[
      static_cast<std::size_t>(Reason::kMissingSymbol)] += 1;
  for (int i = 0; i < 15; ++i) m.business_reject_by_reason[
      static_cast<std::size_t>(Reason::kScaleMismatch)] += 1;
  for (int i = 0; i < 20; ++i) m.policy_suppressed_by_reason[
      static_cast<std::size_t>(Reason::kDuplicateExact)] += 1;
  return m;
}

}  // namespace

TEST(Metrics, BalancedBookPasses) {
  const auto c = balanced().check_conservation();
  EXPECT_TRUE(c.ok);
  EXPECT_TRUE(c.violations.empty());
}

TEST(Metrics, EachEquationIsChecked) {
  {
    Metrics m = balanced();
    m.frames_decoded += 1;  // breaks the first and second equations
    const auto c = m.check_conservation();
    EXPECT_FALSE(c.ok);
    EXPECT_GE(c.violations.size(), 1u);
  }
  {
    Metrics m = balanced();
    m.output_success += 1;
    const auto c = m.check_conservation();
    EXPECT_FALSE(c.ok);
  }
  {
    Metrics m = balanced();
    m.business_ok += 1;
    EXPECT_FALSE(m.check_conservation().ok);
  }
}

TEST(Metrics, PerReasonBreakdownsMustSumToTheirTotals) {
  Metrics m = balanced();
  m.structural_reject_by_reason[static_cast<std::size_t>(Reason::kInvalidUtf8)] +=
      1;
  const auto c = m.check_conservation();
  EXPECT_FALSE(c.ok);
  EXPECT_NE(std::find_if(c.violations.begin(), c.violations.end(),
                         [](const std::string& s) {
                           return s.find("structural_reject_by_reason") !=
                                  std::string::npos;
                         }),
            c.violations.end());
}

TEST(Metrics, CountersAndBreakdownsMoveTogether) {
  Metrics m;
  m.count_structural_reject(Reason::kMissingSymbol);
  m.count_business_reject(Reason::kQuoteCrossed);
  m.count_policy_suppressed(Reason::kReorderLate);

  EXPECT_EQ(m.structural_reject, 1u);
  EXPECT_EQ(
      m.structural_reject_by_reason[static_cast<std::size_t>(Reason::kMissingSymbol)],
      1u);
  EXPECT_EQ(m.business_reject, 1u);
  EXPECT_EQ(m.policy_suppressed, 1u);
}

// Diagnostics overlap by design -- one event can be both gapped and stale --
// so they must never be folded into a partition equation.
TEST(Metrics, DiagnosticCountersDoNotAffectConservation) {
  Metrics m = balanced();
  m.gap_detected = 999;
  m.duplicate_exact = 999;
  m.stale_event_time = 999;
  m.sequence_missing_slots_observed = 1 << 30;
  EXPECT_TRUE(m.check_conservation().ok);
}

// --- histogram --------------------------------------------------------------

TEST(Histogram, EmptyReportsZero) {
  LatencyHistogram h;
  EXPECT_EQ(h.count(), 0u);
  EXPECT_EQ(h.quantile(0.5), 0);
  EXPECT_EQ(h.max(), 0);
  EXPECT_EQ(h.mean(), 0.0);
}

TEST(Histogram, SmallValuesAreExact) {
  LatencyHistogram h;
  for (int i = 0; i < 16; ++i) h.record(i);
  EXPECT_EQ(h.count(), 16u);
  EXPECT_EQ(h.max(), 15);
  EXPECT_EQ(h.min(), 0);
}

TEST(Histogram, QuantilesAreWithinTheDocumentedError) {
  LatencyHistogram h;
  for (int i = 1; i <= 10000; ++i) h.record(i);

  // 16 sub-buckets per octave gives roughly 6% relative error, and the
  // reported value is the bucket's lower bound.
  const auto within = [](std::int64_t got, std::int64_t want) {
    const double err = static_cast<double>(want - got) / static_cast<double>(want);
    return err >= -0.001 && err <= 0.07;
  };
  EXPECT_TRUE(within(h.quantile(0.50), 5000)) << h.quantile(0.50);
  EXPECT_TRUE(within(h.quantile(0.95), 9500)) << h.quantile(0.95);
  EXPECT_TRUE(within(h.quantile(0.99), 9900)) << h.quantile(0.99);
  EXPECT_EQ(h.max(), 10000);
}

TEST(Histogram, HandlesWideDynamicRange) {
  LatencyHistogram h;
  h.record(1);
  h.record(1'000);
  h.record(1'000'000);
  h.record(1'000'000'000);
  EXPECT_EQ(h.count(), 4u);
  EXPECT_EQ(h.max(), 1'000'000'000);
  EXPECT_GE(h.quantile(1.0), 900'000'000);
}

TEST(Histogram, NegativeSamplesAreClampedNotDropped) {
  LatencyHistogram h;
  h.record(-5);
  EXPECT_EQ(h.count(), 1u);
  EXPECT_EQ(h.min(), 0);
}

TEST(Histogram, Reset) {
  LatencyHistogram h;
  for (int i = 0; i < 100; ++i) h.record(i * 7);
  h.reset();
  EXPECT_EQ(h.count(), 0u);
  EXPECT_EQ(h.quantile(0.99), 0);
}

// --- process observations ---------------------------------------------------

TEST(Resources, ProcSelfObservationsAreAvailableOnLinux) {
  EXPECT_GT(md::current_rss_bytes(), 0u);
  EXPECT_GE(md::peak_rss_bytes(), md::current_rss_bytes());
  EXPECT_GT(md::open_fd_count(), 0u);
}
