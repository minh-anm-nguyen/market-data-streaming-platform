// Event-time staleness vs feed inactivity. Group G of the what-if matrix.
//
// Every test here drives a virtual clock. Nothing sleeps, and nothing
// consults wall-clock time -- which is also the property under test.
#include <gtest/gtest.h>

#include "md_adapter/clock.hpp"
#include "md_adapter/quote_freshness.hpp"
#include "test_support.hpp"

using namespace mdtest;
using md::FeedState;
using md::FreshnessEngine;
using md::FreshnessLimits;

namespace {

FreshnessLimits limits() {
  FreshnessLimits l;
  l.event_time_stale_threshold_ns = 2'000'000'000;   // 2 s
  l.quote_inactivity_threshold_ns = 3'000'000'000;   // 3 s
  return l;
}

bool has(std::uint32_t flags, std::uint32_t bit) { return (flags & bit) != 0; }

constexpr std::int64_t kSec = 1'000'000'000;

}  // namespace

// --- event-time regression --------------------------------------------------

TEST(Freshness, FirstEventHasNoWatermarkSoItIsNeverStale) {
  FreshnessEngine f(limits());
  // An event timestamped years in the past, replayed today.
  const auto r = f.observe(quote_event(1, 1'000'000), 0);
  EXPECT_FALSE(r.event_time_old);
  EXPECT_EQ(r.quality_flags, md::quality::kNone);
}

// E2E-04 / design 6: a 2025 dataset replayed in 2026 is not stale. The
// comparison is against the feed's own watermark, never against wall time.
TEST(Freshness, HistoricalReplayIsNotStale) {
  FreshnessEngine f(limits());
  // Event times from a year ago, increasing normally.
  const std::int64_t base = 1'700'000'000LL * kSec;
  for (int i = 1; i <= 10; ++i) {
    const auto r = f.observe(quote_event(i, base + i * kSec), i * kSec);
    EXPECT_FALSE(r.event_time_old) << "seq=" << i;
  }
}

TEST(Freshness, EventOlderThanTheWatermarkByMoreThanTheThresholdIsStale) {
  FreshnessEngine f(limits());
  f.observe(quote_event(1, 10 * kSec), 0);
  f.observe(quote_event(2, 10 * kSec + 100'000'000), 0);

  // 1 s old: inside the 2 s threshold.
  EXPECT_FALSE(f.observe(quote_event(3, 9 * kSec + 100'000'000), 0).event_time_old);
  // 9.1 s old: outside it.
  const auto r = f.observe(quote_event(4, 1 * kSec), 0);
  EXPECT_TRUE(r.event_time_old);
  EXPECT_EQ(r.reason, md::Reason::kEventTimeOld);
  EXPECT_TRUE(has(r.quality_flags, md::quality::kEventTimeOld));
  EXPECT_TRUE(has(r.quality_flags, md::quality::kDegraded));
}

TEST(Freshness, StaleQuoteDoesNotAdvanceTheWatermark) {
  FreshnessEngine f(limits());
  f.observe(quote_event(1, 10 * kSec), 0);
  f.observe(quote_event(2, 1 * kSec), 0);  // stale

  // If the stale event had moved the watermark backwards, this would now be
  // judged fresh. It must still be stale.
  EXPECT_TRUE(f.observe(quote_event(3, 1 * kSec), 0).event_time_old);
}

TEST(Freshness, WatermarkIsPerKey) {
  FreshnessEngine f(limits());
  f.observe(quote_event(1, 100 * kSec, 109000, 109010, "EURUSD"), 0);
  // A low timestamp on a different symbol is that symbol's first event.
  EXPECT_FALSE(
      f.observe(quote_event(1, 1 * kSec, 109000, 109010, "GBPUSD"), 0)
          .event_time_old);
}

// --- feed inactivity --------------------------------------------------------

TEST(Freshness, NoTransitionWhileQuotesKeepArriving) {
  FreshnessEngine f(limits());
  std::vector<FreshnessEngine::Transition> tr;
  std::int64_t now = 0;
  for (int i = 1; i <= 10; ++i) {
    now += kSec;
    f.observe(quote_event(i, i * kSec), now);
    f.tick(now, tr);
  }
  EXPECT_TRUE(tr.empty());
  EXPECT_EQ(f.state_of("ds-test", "EURUSD"), FeedState::kHealthy);
}

// E2E-04: ticks are price updates, not evidence that quoting is alive.
TEST(Freshness, TicksDoNotRefreshTheQuoteInactivityTimer) {
  FreshnessEngine f(limits());
  std::vector<FreshnessEngine::Transition> tr;

  f.observe(quote_event(1, 1 * kSec), 0);
  // Four seconds of ticks only, arriving steadily.
  std::int64_t now = 0;
  for (int i = 2; i <= 9; ++i) {
    now += 500'000'000;  // every 500 ms
    f.observe(tick_event(i, i * kSec), now);
    f.tick(now, tr);
  }

  ASSERT_EQ(tr.size(), 1u) << "exactly one active->inactive edge";
  EXPECT_EQ(tr[0].to, FeedState::kInactive);
  EXPECT_EQ(tr[0].symbol, "EURUSD");
  EXPECT_EQ(f.inactive_transitions(), 1u);
}

TEST(Freshness, InactiveTransitionFiresOnceNotPerTick) {
  FreshnessEngine f(limits());
  std::vector<FreshnessEngine::Transition> tr;
  f.observe(quote_event(1, 1 * kSec), 0);

  for (std::int64_t now = 100'000'000; now <= 10 * kSec; now += 100'000'000)
    f.tick(now, tr);

  EXPECT_EQ(tr.size(), 1u);
  EXPECT_EQ(f.inactive_transitions(), 1u);
}

// NFR-09: detection must land within one timer tick of the threshold.
TEST(Freshness, DetectionLandsWithinOneTick) {
  FreshnessEngine f(limits());
  std::vector<FreshnessEngine::Transition> tr;
  f.observe(quote_event(1, 1 * kSec), 0);

  constexpr std::int64_t kTick = 100'000'000;  // 100 ms
  std::int64_t fired_at = -1;
  for (std::int64_t now = kTick; now <= 6 * kSec; now += kTick) {
    const std::size_t before = tr.size();
    f.tick(now, tr);
    if (tr.size() > before) {
      fired_at = now;
      break;
    }
  }
  ASSERT_GE(fired_at, 0);
  EXPECT_GT(fired_at, 3 * kSec);
  EXPECT_LE(fired_at, 3 * kSec + kTick);
}

TEST(Freshness, RecoveryAfterInactivity) {
  FreshnessEngine f(limits());
  std::vector<FreshnessEngine::Transition> tr;
  f.observe(quote_event(1, 1 * kSec), 0);
  f.tick(4 * kSec, tr);
  ASSERT_EQ(f.state_of("ds-test", "EURUSD"), FeedState::kInactive);

  const auto r = f.observe(quote_event(2, 5 * kSec), 5 * kSec);
  EXPECT_TRUE(r.recovered);
  EXPECT_TRUE(has(r.quality_flags, md::quality::kRecovered));
  EXPECT_EQ(f.state_of("ds-test", "EURUSD"), FeedState::kRecovering);

  // A second good quote completes the recovery.
  f.observe(quote_event(3, 6 * kSec), 6 * kSec);
  EXPECT_EQ(f.state_of("ds-test", "EURUSD"), FeedState::kHealthy);
}

TEST(Freshness, StaleQuoteDoesNotCountAsLiveness) {
  FreshnessEngine f(limits());
  std::vector<FreshnessEngine::Transition> tr;
  f.observe(quote_event(1, 100 * kSec), 0);

  // A quote arrives every second, but each carries an ancient timestamp.
  std::int64_t now = 0;
  for (int i = 2; i <= 6; ++i) {
    now += kSec;
    const auto r = f.observe(quote_event(i, 1 * kSec), now);
    ASSERT_TRUE(r.event_time_old);
    f.tick(now, tr);
  }

  EXPECT_EQ(tr.size(), 1u)
      << "stale quotes must not keep the feed looking alive";
  EXPECT_EQ(f.state_of("ds-test", "EURUSD"), FeedState::kInactive);
}

// --- feed state -------------------------------------------------------------

// Design 13.2: DEGRADED is sticky; it does not expire on a timer.
TEST(Freshness, DegradedIsStickyAcrossRecovery) {
  FreshnessEngine f(limits());
  std::vector<FreshnessEngine::Transition> tr;
  f.observe(quote_event(1, 1 * kSec), 0);
  f.mark_degraded("ds-test", "EURUSD");
  EXPECT_EQ(f.state_of("ds-test", "EURUSD"), FeedState::kDegraded);

  f.tick(5 * kSec, tr);
  EXPECT_EQ(f.state_of("ds-test", "EURUSD"), FeedState::kInactive);

  f.observe(quote_event(2, 6 * kSec), 6 * kSec);
  f.observe(quote_event(3, 7 * kSec), 7 * kSec);
  // Liveness recovered, but the quality problem has not been forgotten.
  EXPECT_EQ(f.state_of("ds-test", "EURUSD"), FeedState::kDegraded);
}

TEST(Freshness, UnknownKeyIsInitial) {
  FreshnessEngine f(limits());
  EXPECT_EQ(f.state_of("nope", "EURUSD"), FeedState::kInitial);
}

TEST(Freshness, SnapshotReportsPerKeyState) {
  FreshnessEngine f(limits());
  f.observe(quote_event(1, 5 * kSec, 109000, 109010, "EURUSD"), 0);
  f.observe(quote_event(1, 7 * kSec, 109000, 109010, "GBPUSD"), 0);

  const auto snap = f.snapshot();
  ASSERT_EQ(snap.size(), 2u);
  for (const auto& k : snap) {
    EXPECT_TRUE(k.has_watermark);
    EXPECT_NE(k.key.find('/'), std::string::npos);
  }
}
