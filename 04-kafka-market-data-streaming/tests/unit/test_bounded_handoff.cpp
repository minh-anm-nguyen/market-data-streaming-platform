// The two-tier bounded budget (NFR-03, section 4.4, cases A-07, A-08, A-14,
// I-01 to I-14).
//
// The invariant under test is the one from section 10.4:
//
//     app_queue_records <= 4096 && app_queue_bytes <= 8 MiB
//
// held under every sequence of reserves and releases, with nothing ever dropped
// to make room and no unbounded wait.
#include "queue/bounded_handoff.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

namespace p04 {
namespace {

HandoffConfig MakeConfig(std::size_t records = 4096,
                         std::size_t bytes = 8u * 1024u * 1024u) {
  HandoffConfig config;
  config.max_records = records;
  config.max_bytes = bytes;
  config.high_watermark_percent = 80;
  config.low_watermark_percent = 40;
  config.stall_timeout_ms = 50;  // short, so the stall tests stay fast
  return config;
}

TEST(BoundedHandoff, AcceptsUpToTheRecordCapThenRefuses) {
  BoundedHandoff handoff(MakeConfig(4, 1024 * 1024));
  for (std::uint64_t id = 1; id <= 4; ++id) {
    EXPECT_EQ(handoff.TryReserve(id, 10), ReserveResult::kAccepted) << id;
  }
  EXPECT_EQ(handoff.TryReserve(5, 10), ReserveResult::kRejectedFull);
  const HandoffStats stats = handoff.Stats();
  EXPECT_EQ(stats.records, 4u);
  EXPECT_EQ(stats.reserve_rejected_full, 1u);
}

// NFR-03: whichever cap is hit first wins. A records-only cap would let a few
// large items hold far more memory than the byte budget allows.
TEST(BoundedHandoff, ByteCapCanBindBeforeTheRecordCap) {
  BoundedHandoff handoff(MakeConfig(4096, 1000));
  EXPECT_EQ(handoff.TryReserve(1, 600), ReserveResult::kAccepted);
  EXPECT_EQ(handoff.TryReserve(2, 600), ReserveResult::kRejectedFull)
      << "the byte budget must bind even though the record cap has room";
  EXPECT_EQ(handoff.Stats().bytes, 600u);
}

// An item larger than the whole budget is a contract violation, not
// backpressure: reporting it as full would send the upstream into a pause loop
// waiting for space that can never be enough.
TEST(BoundedHandoff, DistinguishesTooLargeFromFull) {
  BoundedHandoff handoff(MakeConfig(4096, 1000));
  EXPECT_EQ(handoff.TryReserve(1, 1001), ReserveResult::kRejectedTooLarge);
  EXPECT_EQ(handoff.Stats().reserve_rejected_too_large, 1u);
  EXPECT_EQ(handoff.Stats().records, 0u);
}

TEST(BoundedHandoff, ReleaseRestoresBothBudgets) {
  BoundedHandoff handoff(MakeConfig(2, 1000));
  ASSERT_EQ(handoff.TryReserve(1, 400), ReserveResult::kAccepted);
  ASSERT_EQ(handoff.TryReserve(2, 400), ReserveResult::kAccepted);
  EXPECT_EQ(handoff.TryReserve(3, 100), ReserveResult::kRejectedFull);

  EXPECT_TRUE(handoff.Release(1));
  EXPECT_EQ(handoff.Stats().records, 1u);
  EXPECT_EQ(handoff.Stats().bytes, 400u);
  EXPECT_EQ(handoff.TryReserve(3, 400), ReserveResult::kAccepted);
}

// The budget remembers the byte count from reserve time, so a caller cannot
// corrupt it by passing a different number at release.
TEST(BoundedHandoff, ReleaseOfUnknownAttemptIsRefusedNotApplied) {
  BoundedHandoff handoff(MakeConfig(4, 1000));
  ASSERT_EQ(handoff.TryReserve(1, 400), ReserveResult::kAccepted);
  EXPECT_FALSE(handoff.Release(999)) << "an unknown attempt must be refused";
  EXPECT_EQ(handoff.Stats().bytes, 400u) << "the budget must not have moved";
  EXPECT_TRUE(handoff.Release(1));
  EXPECT_EQ(handoff.Stats().bytes, 0u);
  EXPECT_FALSE(handoff.Release(1)) << "a double release must be refused";
  EXPECT_EQ(handoff.Stats().records, 0u);
}

// Case A-07: hysteresis. Pause latches at 80% and clears only at 40%, so the
// upstream does not toggle once per event.
TEST(BoundedHandoff, HysteresisLatchesAtHighAndClearsAtLow) {
  BoundedHandoff handoff(MakeConfig(10, 1024 * 1024));
  for (std::uint64_t id = 1; id <= 7; ++id) {
    ASSERT_EQ(handoff.TryReserve(id, 1), ReserveResult::kAccepted);
  }
  EXPECT_FALSE(handoff.ShouldPauseUpstream()) << "7 of 10 is below the 80% mark";

  ASSERT_EQ(handoff.TryReserve(8, 1), ReserveResult::kAccepted);
  EXPECT_TRUE(handoff.ShouldPauseUpstream()) << "8 of 10 reaches the high watermark";

  // Draining to 5 is still above the 40% low watermark, so the pause holds.
  // This is the whole point of the second threshold: the queue has to come
  // substantially down, not just back under the pause level.
  handoff.Release(1);
  handoff.Release(2);
  handoff.Release(3);
  EXPECT_TRUE(handoff.ShouldPauseUpstream()) << "5 of 10 is above the low watermark";

  // The boundary is inclusive: reaching the low watermark clears the pause.
  handoff.Release(4);
  EXPECT_FALSE(handoff.ShouldPauseUpstream())
      << "4 of 10 is the low watermark, which clears the pause";

  const HandoffStats stats = handoff.Stats();
  EXPECT_EQ(stats.pause_transitions, 1u);
  EXPECT_EQ(stats.resume_transitions, 1u);
}

TEST(BoundedHandoff, PauseDoesNotFlapWhileOscillatingNearTheHighWatermark) {
  BoundedHandoff handoff(MakeConfig(10, 1024 * 1024));
  for (std::uint64_t id = 1; id <= 8; ++id) {
    ASSERT_EQ(handoff.TryReserve(id, 1), ReserveResult::kAccepted);
  }
  ASSERT_TRUE(handoff.ShouldPauseUpstream());
  // Release and re-reserve the same slot 50 times. A single-threshold design
  // would toggle 100 times; hysteresis must produce no further transitions.
  for (int i = 0; i < 50; ++i) {
    handoff.Release(8);
    ASSERT_EQ(handoff.TryReserve(8, 1), ReserveResult::kAccepted);
  }
  const HandoffStats stats = handoff.Stats();
  EXPECT_EQ(stats.pause_transitions, 1u);
  EXPECT_EQ(stats.resume_transitions, 0u);
}

// Section 4.4: a blocking reserve has a deadline. An unbounded wait would turn
// a slow broker into a hang instead of an INCOMPLETE run.
TEST(BoundedHandoff, BlockingReserveTimesOutRatherThanWaitingForever) {
  HandoffConfig config = MakeConfig(1, 1000);
  config.stall_timeout_ms = 60;
  BoundedHandoff handoff(config);
  ASSERT_EQ(handoff.TryReserve(1, 100), ReserveResult::kAccepted);

  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(handoff.ReserveOrWait(2, 100), ReserveResult::kStallTimeout);
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start).count();
  EXPECT_GE(elapsed, 50) << "it returned before the deadline";
  EXPECT_LT(elapsed, 2000) << "it waited far past the deadline";
  EXPECT_EQ(handoff.Stats().reserve_stall_timeout, 1u);
}

TEST(BoundedHandoff, BlockingReserveSucceedsOnceSpaceAppears) {
  HandoffConfig config = MakeConfig(1, 1000);
  config.stall_timeout_ms = 2000;
  BoundedHandoff handoff(config);
  ASSERT_EQ(handoff.TryReserve(1, 100), ReserveResult::kAccepted);

  std::thread releaser([&handoff]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    handoff.Release(1);
  });
  EXPECT_EQ(handoff.ReserveOrWait(2, 100), ReserveResult::kAccepted);
  releaser.join();
}

// A spurious wakeup must not restart the deadline. Driven by a thread that
// notifies waiters without actually freeing room.
TEST(BoundedHandoff, SpuriousWakeupsDoNotExtendTheDeadline) {
  HandoffConfig config = MakeConfig(1, 1000);
  config.stall_timeout_ms = 120;
  BoundedHandoff handoff(config);
  ASSERT_EQ(handoff.TryReserve(1, 100), ReserveResult::kAccepted);

  std::atomic<bool> stop{false};
  std::thread noisy([&handoff, &stop]() {
    while (!stop.load()) {
      handoff.Release(999);  // unknown id: notifies, frees nothing
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  });
  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(handoff.ReserveOrWait(2, 100), ReserveResult::kStallTimeout);
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start).count();
  EXPECT_LT(elapsed, 1500) << "the deadline was restarted by spurious wakeups";
  stop.store(true);
  noisy.join();
}

TEST(BoundedHandoff, CloseRefusesNewWorkButAllowsDraining) {
  BoundedHandoff handoff(MakeConfig(4, 1000));
  ASSERT_EQ(handoff.TryReserve(1, 100), ReserveResult::kAccepted);
  handoff.Close();
  EXPECT_TRUE(handoff.closed());
  EXPECT_EQ(handoff.TryReserve(2, 100), ReserveResult::kClosed);
  EXPECT_EQ(handoff.ReserveOrWait(3, 100), ReserveResult::kClosed);
  // The outstanding reservation can still be released, which is what lets a
  // drain finish after SIGTERM.
  EXPECT_TRUE(handoff.Release(1));
  EXPECT_EQ(handoff.Stats().records, 0u);
}

TEST(BoundedHandoff, TracksOldestAgeAndHighWaterMarks) {
  BoundedHandoff handoff(MakeConfig(8, 10000));
  ASSERT_EQ(handoff.TryReserve(1, 100), ReserveResult::kAccepted);
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  ASSERT_EQ(handoff.TryReserve(2, 100), ReserveResult::kAccepted);

  HandoffStats stats = handoff.Stats();
  EXPECT_GT(stats.oldest_age_seconds, 0.02) << "oldest age should reflect attempt 1";
  EXPECT_EQ(stats.max_records_observed, 2u);
  EXPECT_EQ(stats.max_bytes_observed, 200u);

  // After releasing the oldest, the age must drop to the next one.
  handoff.Release(1);
  stats = handoff.Stats();
  EXPECT_LT(stats.oldest_age_seconds, 0.02);
  EXPECT_EQ(stats.max_records_observed, 2u) << "high-water marks do not decrease";
}

// Case A-14 and the section 10.4 invariant: a randomised reserve/release storm
// must never breach either cap and must never lose accounting.
TEST(BoundedHandoff, RandomisedStormNeverBreachesEitherCap) {
  constexpr std::size_t kMaxRecords = 64;
  constexpr std::size_t kMaxBytes = 4096;
  BoundedHandoff handoff(MakeConfig(kMaxRecords, kMaxBytes));

  std::mt19937 rng(20261030);
  std::uniform_int_distribution<int> size_dist(1, 200);
  std::uniform_int_distribution<int> action_dist(0, 2);
  std::vector<std::uint64_t> held;
  std::uint64_t next_id = 1;

  for (int step = 0; step < 20000; ++step) {
    const HandoffStats stats = handoff.Stats();
    ASSERT_LE(stats.records, kMaxRecords) << "step " << step;
    ASSERT_LE(stats.bytes, kMaxBytes) << "step " << step;
    ASSERT_EQ(stats.records, held.size()) << "step " << step;

    if (action_dist(rng) == 0 && !held.empty()) {
      const auto index = static_cast<std::size_t>(rng() % held.size());
      ASSERT_TRUE(handoff.Release(held[index]));
      held.erase(held.begin() + static_cast<std::ptrdiff_t>(index));
    } else {
      const auto bytes = static_cast<std::size_t>(size_dist(rng));
      const std::uint64_t id = next_id++;
      if (handoff.TryReserve(id, bytes) == ReserveResult::kAccepted) {
        held.push_back(id);
      }
    }
  }
  // Drain and confirm the budget returns to exactly zero: a leak shows up here
  // as a non-zero residue.
  for (const std::uint64_t id : held) ASSERT_TRUE(handoff.Release(id));
  const HandoffStats final_stats = handoff.Stats();
  EXPECT_EQ(final_stats.records, 0u);
  EXPECT_EQ(final_stats.bytes, 0u);
}

// Release from a second thread, as a delivery-report callback would, while the
// producer thread reserves. The caps must still hold.
TEST(BoundedHandoff, ConcurrentReserveAndReleaseRespectsCaps) {
  constexpr std::size_t kMaxRecords = 32;
  BoundedHandoff handoff(MakeConfig(kMaxRecords, 1u << 20));
  std::atomic<bool> stop{false};
  std::atomic<std::uint64_t> next_id{1};
  std::atomic<std::size_t> breaches{0};
  std::mutex held_mutex;
  std::vector<std::uint64_t> held;

  std::thread producer([&]() {
    while (!stop.load()) {
      const std::uint64_t id = next_id.fetch_add(1);
      if (handoff.TryReserve(id, 64) == ReserveResult::kAccepted) {
        std::lock_guard<std::mutex> lock(held_mutex);
        held.push_back(id);
      }
      if (handoff.Stats().records > kMaxRecords) breaches.fetch_add(1);
    }
  });
  std::thread consumer([&]() {
    while (!stop.load()) {
      std::uint64_t id = 0;
      {
        std::lock_guard<std::mutex> lock(held_mutex);
        if (!held.empty()) {
          id = held.front();
          held.erase(held.begin());
        }
      }
      if (id != 0) handoff.Release(id);
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  stop.store(true);
  producer.join();
  consumer.join();
  EXPECT_EQ(breaches.load(), 0u);
  EXPECT_LE(handoff.Stats().records, kMaxRecords);
}

TEST(BoundedHandoff, ClampsWatermarksThatWouldDisableHysteresis) {
  // A config with low above high would make the pause clear on the same event
  // that set it. The constructor clamps low to high-1 rather than accepting a
  // configuration that silently does nothing, so the pause still latches and
  // still needs at least one release to clear.
  HandoffConfig config = MakeConfig(10, 1000);
  config.high_watermark_percent = 50;  // high = 5 records
  config.low_watermark_percent = 90;   // would be 9; clamped to 4
  BoundedHandoff handoff(config);
  for (std::uint64_t id = 1; id <= 5; ++id) {
    ASSERT_EQ(handoff.TryReserve(id, 1), ReserveResult::kAccepted);
  }
  EXPECT_TRUE(handoff.ShouldPauseUpstream())
      << "reaching the high watermark must still latch the pause";
  handoff.Release(1);
  EXPECT_FALSE(handoff.ShouldPauseUpstream())
      << "the clamped low watermark sits one record below high, so one release "
         "clears it; what matters is that it latched at all";
  EXPECT_EQ(handoff.Stats().pause_transitions, 1u);
  EXPECT_EQ(handoff.Stats().resume_transitions, 1u);
}

TEST(BoundedHandoff, EveryReserveResultHasAName) {
  for (const auto result : {ReserveResult::kAccepted, ReserveResult::kRejectedFull,
                            ReserveResult::kRejectedTooLarge,
                            ReserveResult::kStallTimeout, ReserveResult::kClosed}) {
    EXPECT_STRNE(ToString(result), "UNMAPPED");
  }
}

}  // namespace
}  // namespace p04
