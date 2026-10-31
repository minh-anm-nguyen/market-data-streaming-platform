// Property 7: bounded state under adversarial input.
//
// The attacker model here is a peer that is syntactically perfect and
// semantically hostile: unbounded symbol cardinality, absurd sequence jumps,
// and extreme integers. None of it may translate into unbounded memory or
// into work proportional to a wire value.
#include <gtest/gtest.h>

#include <limits>
#include <random>

#include "md_adapter/pipeline.hpp"
#include "test_support.hpp"

using namespace mdtest;

namespace {

md::SequenceLimits tight_limits() {
  md::SequenceLimits l;
  l.window = 256;
  l.max_symbols_per_dataset = 16;
  l.max_datasets = 2;
  l.max_state_bytes = 512 * 1024;
  l.live_baseline_unverified = false;
  return l;
}

}  // namespace

TEST(StateBounds, SymbolCardinalityAttackStaysInsideTheBudget) {
  md::SequenceTracker t(tight_limits());
  for (int i = 0; i < 100000; ++i) {
    t.observe(quote_event(1, 1000, 109000, 109010,
                          "S" + std::to_string(i), "D" + std::to_string(i % 7)));
  }
  EXPECT_LE(t.state_bytes(), tight_limits().max_state_bytes);
  EXPECT_LE(t.dataset_count(), tight_limits().max_datasets);
  EXPECT_LE(t.key_count(),
            tight_limits().max_datasets * tight_limits().max_symbols_per_dataset);
  EXPECT_GT(t.state_cap_rejections(), 0u);
}

TEST(StateBounds, AlternatingHugeJumpsDoNotGrowState) {
  md::SequenceTracker t(tight_limits());
  constexpr std::int64_t kMax = std::numeric_limits<std::int64_t>::max();

  t.observe(quote_event(1, 1000));
  const std::uint64_t after_first = t.state_bytes();

  std::mt19937_64 rng(31337);
  for (int i = 0; i < 5000; ++i) {
    const std::int64_t seq =
        1 + static_cast<std::int64_t>(rng() % static_cast<std::uint64_t>(kMax));
    t.observe(quote_event(seq, 1000 + i));
  }
  // One key, one ring. Nothing about the sequence values can change that.
  EXPECT_EQ(t.state_bytes(), after_first);
  EXPECT_EQ(t.key_count(), 1u);
}

TEST(StateBounds, ExtremeSequenceValuesNeverOverflow) {
  md::SequenceTracker t(tight_limits());
  constexpr std::int64_t kMax = std::numeric_limits<std::int64_t>::max();
  const std::int64_t values[] = {1, kMax, 2, kMax - 1, kMax / 2, 3, kMax};

  for (std::int64_t v : values) {
    const auto r = t.observe(quote_event(v, 1000));
    // Whatever the classification, it must be a defined one and the missing
    // slot count must stay non-negative.
    EXPECT_GE(r.missing_slots_observed, 0);
  }
  const auto* s = t.stats("ds-test", "EURUSD");
  ASSERT_NE(s, nullptr);
  EXPECT_EQ(s->highest_seen_valid, kMax);
}

// The whole pipeline, not just the tracker: hostile input must not make the
// queue, the state table or the metrics grow without limit.
TEST(StateBounds, PipelineStaysBoundedUnderAHostileCorpus) {
  md::Config cfg = base_config();
  cfg.identity.max_state_bytes = 4 * 1024 * 1024;
  cfg.identity.max_symbols = 8;
  cfg.validation.sequence_window = 256;

  md::VirtualClock clock(0);
  md::BoundedQueue queue({256, 256 * 1024, 80, 40});
  md::Pipeline pipeline(cfg, clock, queue, false, false);

  md::FramerLimits fl;
  fl.prefix_deadline_ns = 0;
  fl.header_partial_deadline_ns = 0;
  fl.body_deadline_ns = 0;
  md::Mdp1Framer framer(fl);
  framer.reset(0);

  std::mt19937_64 rng(5150);
  static const char* kSyms[] = {"EURUSD", "GBPUSD", "USDJPY"};

  // Feed in batches, draining the queue each time, so the test measures
  // state growth rather than queue backlog.
  std::string carry;
  bool first_batch = true;
  for (int batch = 0; batch < 40; ++batch) {
    std::vector<std::string> payloads;
    for (int i = 0; i < 200; ++i) {
      EventSpec e;
      const int s = static_cast<int>(rng() % 3);
      e.symbol = kSyms[s];
      e.price_scale = (s == 2) ? 1000 : 100000;
      e.sequence = 1 + static_cast<std::int64_t>(rng() % 1'000'000'000);
      e.event_time_ns = static_cast<std::int64_t>(rng() % 1'000'000'000'000);
      e.bid = (s == 2) ? 15000 : 109000;
      e.ask = e.bid + static_cast<std::int64_t>(rng() % 100);
      payloads.push_back(encode(e));
    }
    std::string wire;
    if (first_batch) {
      wire = stream(payloads);
      first_batch = false;
    } else {
      for (const auto& p : payloads) wire += frame(p);
    }
    carry += wire;

    while (!carry.empty()) {
      const auto fr = framer.feed(as_bytes(carry), clock.now_mono_ns(), pipeline);
      carry.erase(0, fr.bytes_consumed);
      clock.advance_ns(1000);
      std::vector<md::ClassifiedEvent> drained;
      queue.pop_batch(4096, drained);
      queue.release(drained);
      if (fr.paused) {
        if (!pipeline.flush_pending()) break;
        continue;
      }
      if (fr.fatal) {
        carry.clear();
        break;
      }
      if (fr.bytes_consumed == 0) break;
    }
  }

  EXPECT_LE(pipeline.tracker().state_bytes(), cfg.identity.max_state_bytes);
  EXPECT_LE(queue.peak_events(), 256u);
  EXPECT_LE(queue.peak_bytes(), 256u * 1024u);
  EXPECT_LE(pipeline.tracker().key_count(), 8u);
}

// Property 6: process counters never decrease.
TEST(StateBounds, CountersAreMonotonic) {
  md::Config cfg = base_config();
  md::VirtualClock clock(0);
  md::BoundedQueue queue({4096, 8u << 20, 80, 40});
  md::Pipeline pipeline(cfg, clock, queue, false, false);

  md::FramerLimits fl;
  fl.prefix_deadline_ns = 0;
  fl.header_partial_deadline_ns = 0;
  fl.body_deadline_ns = 0;
  md::Mdp1Framer framer(fl);
  framer.reset(0);

  std::uint64_t prev_frames = 0;
  std::uint64_t prev_ok = 0;
  std::string wire = "MDP1";
  for (int i = 1; i <= 200; ++i) {
    EventSpec e;
    e.sequence = i;
    e.event_time_ns = i * 1'000'000;
    wire += frame(encode(e));

    framer.feed(as_bytes(wire), clock.now_mono_ns(), pipeline);
    wire.clear();
    clock.advance_ns(1000);

    const auto& m = pipeline.metrics();
    EXPECT_GE(m.frames_complete, prev_frames);
    EXPECT_GE(m.business_ok, prev_ok);
    prev_frames = m.frames_complete;
    prev_ok = m.business_ok;

    std::vector<md::ClassifiedEvent> drained;
    queue.pop_batch(4096, drained);
    queue.release(drained);
  }
  EXPECT_EQ(prev_frames, 200u);
}
