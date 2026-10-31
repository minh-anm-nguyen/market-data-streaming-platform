// Bounded hand-off queue. Group I of the what-if matrix.
//
// The invariant under test throughout: the queue never discards anything. A
// full queue is reported to the producer, which pauses reading.
#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "md_adapter/bounded_queue.hpp"
#include "test_support.hpp"

using namespace mdtest;
using md::BoundedQueue;
using md::ClassifiedEvent;
using md::QueueLimits;

namespace {

ClassifiedEvent ev(std::int64_t seq, std::size_t raw_bytes = 0) {
  ClassifiedEvent e;
  e.source = quote_event(seq, seq * 1000);
  e.arrival_index = static_cast<std::uint64_t>(seq);
  if (raw_bytes != 0) e.raw_wire.assign(raw_bytes, 'r');
  return e;
}

}  // namespace

TEST(BoundedQueue, FifoOrder) {
  BoundedQueue q({100, 1 << 20, 80, 40});
  for (int i = 1; i <= 5; ++i)
    ASSERT_EQ(q.try_push(ev(i)), BoundedQueue::PushResult::kAccepted);

  for (int i = 1; i <= 5; ++i) {
    auto e = q.pop(1'000'000);
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->source.sequence, i);
  }
}

TEST(BoundedQueue, EnforcesTheEventCountCap) {
  BoundedQueue q({3, 1 << 20, 80, 40});
  for (int i = 1; i <= 3; ++i)
    ASSERT_EQ(q.try_push(ev(i)), BoundedQueue::PushResult::kAccepted);

  EXPECT_EQ(q.try_push(ev(4)), BoundedQueue::PushResult::kRejectedFull);
  EXPECT_EQ(q.size(), 3u) << "a rejected push must not displace anything";
}

// Many small events can blow a memory budget while staying far below any
// entry-count cap, so both are enforced.
TEST(BoundedQueue, EnforcesTheByteCapIndependently) {
  const std::size_t one = md::estimate_event_bytes(ev(1, 1024));
  BoundedQueue q({100000, one * 3 + 1, 80, 40});

  int accepted = 0;
  for (int i = 1; i <= 20; ++i)
    if (q.try_push(ev(i, 1024)) == BoundedQueue::PushResult::kAccepted)
      ++accepted;

  EXPECT_EQ(accepted, 3);
  EXPECT_LE(q.bytes(), one * 3 + 1);
  EXPECT_LT(q.size(), 100000u);
}

TEST(BoundedQueue, RejectedPushLeavesTheEventWithTheCaller) {
  BoundedQueue q({1, 1 << 20, 80, 40});
  ASSERT_EQ(q.try_push(ev(1)), BoundedQueue::PushResult::kAccepted);

  ClassifiedEvent held = ev(2);
  ASSERT_EQ(q.try_push(std::move(held)), BoundedQueue::PushResult::kRejectedFull);
  // try_push only moves out of its argument when it accepts, so the caller
  // can retry with the same object instead of losing the event.
  EXPECT_EQ(held.source.sequence, 2);
  EXPECT_EQ(held.source.symbol, "EURUSD");

  (void)q.pop(1'000'000);
  EXPECT_EQ(q.try_push(std::move(held)), BoundedQueue::PushResult::kAccepted);
}

TEST(BoundedQueue, BytesAreReturnedOnPop) {
  BoundedQueue q({100, 1 << 20, 80, 40});
  for (int i = 1; i <= 10; ++i) q.try_push(ev(i, 512));
  const std::size_t full = q.bytes();
  ASSERT_GT(full, 0u);

  while (q.pop(1'000).has_value()) {
  }
  EXPECT_EQ(q.bytes(), 0u);
  EXPECT_EQ(q.size(), 0u);
  EXPECT_GE(q.peak_bytes(), full);
  EXPECT_EQ(q.peak_events(), 10u);
}

TEST(BoundedQueue, WatermarkHysteresis) {
  BoundedQueue q({10, 1 << 20, 80, 40});
  EXPECT_FALSE(q.should_pause());
  EXPECT_TRUE(q.should_resume());

  for (int i = 1; i <= 8; ++i) q.try_push(ev(i));
  EXPECT_TRUE(q.should_pause());
  EXPECT_FALSE(q.should_resume());

  // Draining to 5/10 is below the pause point but still above resume: the
  // gap is what stops the reader toggling on every event.
  for (int i = 0; i < 3; ++i) (void)q.pop(1000);
  EXPECT_FALSE(q.should_pause());
  EXPECT_FALSE(q.should_resume());

  for (int i = 0; i < 2; ++i) (void)q.pop(1000);
  EXPECT_TRUE(q.should_resume());
}

TEST(BoundedQueue, PopBatch) {
  BoundedQueue q({100, 1 << 20, 80, 40});
  for (int i = 1; i <= 10; ++i) q.try_push(ev(i));

  std::vector<ClassifiedEvent> out;
  EXPECT_EQ(q.pop_batch(4, out), 4u);
  EXPECT_EQ(out.size(), 4u);
  EXPECT_EQ(out[0].source.sequence, 1);
  EXPECT_EQ(q.size(), 6u);
  EXPECT_EQ(q.outstanding(), 10u) << "popped-but-unwritten work still counts";
  q.release(out);
  EXPECT_EQ(q.outstanding(), 6u);

  out.clear();
  EXPECT_EQ(q.pop_batch(100, out), 6u);
  q.release(out);
  out.clear();
  EXPECT_EQ(q.pop_batch(10, out), 0u);
  EXPECT_EQ(q.outstanding(), 0u);
}

// The cap has to describe unwritten work, not just deque occupancy. A writer
// that pulled a batch out and then stalled must still produce backpressure;
// otherwise the budget stops meaning anything.
TEST(BoundedQueue, InFlightWorkCountsAgainstTheCap) {
  BoundedQueue q({4, 1 << 20, 80, 40});
  for (int i = 1; i <= 4; ++i)
    ASSERT_EQ(q.try_push(ev(i)), BoundedQueue::PushResult::kAccepted);

  std::vector<ClassifiedEvent> batch;
  ASSERT_EQ(q.pop_batch(4, batch), 4u);
  EXPECT_EQ(q.size(), 0u) << "the deque is empty...";
  EXPECT_EQ(q.outstanding(), 4u) << "...but the work is not done";

  EXPECT_EQ(q.try_push(ev(5)), BoundedQueue::PushResult::kRejectedFull);
  EXPECT_TRUE(q.should_pause());

  q.release(batch);
  EXPECT_EQ(q.outstanding(), 0u);
  EXPECT_TRUE(q.should_resume());
  EXPECT_EQ(q.try_push(ev(5)), BoundedQueue::PushResult::kAccepted);
}

TEST(BoundedQueue, CloseRejectsNewWorkButLetsTheRestDrain) {
  BoundedQueue q({100, 1 << 20, 80, 40});
  q.try_push(ev(1));
  q.try_push(ev(2));
  q.close();

  EXPECT_EQ(q.try_push(ev(3)), BoundedQueue::PushResult::kClosed);
  EXPECT_TRUE(q.closed());
  EXPECT_TRUE(q.pop(1'000'000).has_value());
  EXPECT_TRUE(q.pop(1'000'000).has_value());
  EXPECT_FALSE(q.pop(1'000'000).has_value());
}

TEST(BoundedQueue, PopTimesOutWhenEmpty) {
  BoundedQueue q({100, 1 << 20, 80, 40});
  const auto start = std::chrono::steady_clock::now();
  EXPECT_FALSE(q.pop(20'000'000).has_value());  // 20 ms
  const auto waited = std::chrono::steady_clock::now() - start;
  EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(waited).count(),
            15);
}

// Exercised under ThreadSanitizer in the tsan build.
TEST(BoundedQueue, SingleProducerSingleConsumerLosesNothing) {
  BoundedQueue q({64, 1 << 20, 80, 40});
  constexpr int kTotal = 20000;
  std::atomic<int> consumed{0};
  std::atomic<std::int64_t> last{0};
  std::atomic<bool> ordered{true};

  std::thread consumer([&] {
    while (consumed.load() < kTotal) {
      std::vector<ClassifiedEvent> batch;
      if (q.pop_batch(32, batch) == 0) {
        auto e = q.pop(1'000'000);
        if (!e.has_value()) continue;
        batch.push_back(std::move(*e));
      }
      for (const auto& e : batch) {
        if (e.source.sequence != last.load() + 1) ordered.store(false);
        last.store(e.source.sequence);
        consumed.fetch_add(1);
      }
      q.release(batch);
    }
  });

  for (int i = 1; i <= kTotal; ++i) {
    while (q.try_push(ev(i)) != BoundedQueue::PushResult::kAccepted)
      std::this_thread::yield();
  }
  consumer.join();

  EXPECT_EQ(consumed.load(), kTotal);
  EXPECT_TRUE(ordered.load());
  EXPECT_LE(q.peak_events(), 64u);
}
