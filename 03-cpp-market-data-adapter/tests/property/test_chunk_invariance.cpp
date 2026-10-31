// Metamorphic properties from design 10.1.
//
// These run the whole pipeline, not just the framer, because the property
// that matters is about the *classification trace*, not about byte counts.
#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

#include "md_adapter/pipeline.hpp"
#include "test_support.hpp"

using namespace mdtest;

namespace {

// One line per published event, in order. This is the observable the
// properties are stated over.
std::vector<std::string> run_trace(const std::string& wire,
                                   const std::vector<std::size_t>& chunks,
                                   bool* fatal_out = nullptr) {
  const md::Config cfg = base_config();
  md::VirtualClock clock(0);
  md::MetricsOnlySink sink;
  md::BoundedQueue queue({4096, 8u << 20, 80, 40});
  md::Pipeline pipeline(cfg, clock, queue, false, false);

  md::FramerLimits fl;
  fl.prefix_deadline_ns = 0;
  fl.header_partial_deadline_ns = 0;
  fl.body_deadline_ns = 0;
  fl.max_frames_per_iteration = 256;
  md::Mdp1Framer framer(fl);
  framer.reset(0);

  std::string buffered;
  std::size_t next = 0;
  std::size_t ci = 0;
  bool fatal = false;
  while (next < wire.size() || !buffered.empty()) {
    if (next < wire.size()) {
      const std::size_t n =
          std::min(chunks[ci++ % chunks.size()], wire.size() - next);
      buffered.append(wire, next, n);
      next += n;
    }
    const auto fr = framer.feed(as_bytes(buffered), clock.now_mono_ns(), pipeline);
    buffered.erase(0, fr.bytes_consumed);
    clock.advance_ns(1000);
    if (fr.fatal) {
      fatal = true;
      break;
    }
    if (fr.paused) {
      // metrics_only never pauses in these sizes, but drain anyway so the
      // property does not depend on queue capacity.
      std::vector<md::ClassifiedEvent> drained;
      queue.pop_batch(4096, drained);
      queue.release(drained);
      if (!pipeline.flush_pending()) break;
    }
    if (fr.bytes_consumed == 0 && next >= wire.size()) break;
  }
  if (fatal_out != nullptr) *fatal_out = fatal;

  std::vector<md::ClassifiedEvent> out;
  queue.pop_batch(1 << 20, out);
  std::vector<std::string> trace;
  trace.reserve(out.size());
  for (const auto& e : out) {
    std::string line = e.source.dataset_id + "/" + e.source.symbol + "#" +
                       std::to_string(e.source.sequence) + " t=" +
                       std::to_string(e.source.event_time_ns) + " flags=";
    md::append_quality_flags(e.quality_flags, line);
    line += " reason=";
    line += md::to_string(e.primary_reason);
    trace.push_back(std::move(line));
  }
  return trace;
}

std::string build_wire(const std::vector<EventSpec>& specs) {
  std::vector<std::string> payloads;
  payloads.reserve(specs.size());
  for (const auto& s : specs) payloads.push_back(encode(s));
  return stream(payloads);
}

std::vector<EventSpec> mixed_corpus(int count, std::mt19937& rng) {
  static const char* kSyms[] = {"EURUSD", "GBPUSD", "USDJPY"};
  std::vector<EventSpec> out;
  std::int64_t seq[3] = {0, 0, 0};
  for (int i = 0; i < count; ++i) {
    const int s = static_cast<int>(rng() % 3);
    EventSpec e;
    e.symbol = kSyms[s];
    e.price_scale = (s == 2) ? 1000 : 100000;
    // Occasionally skip a sequence to create a genuine gap.
    seq[s] += (rng() % 10 == 0) ? 2 : 1;
    e.sequence = seq[s];
    e.event_time_ns = static_cast<std::int64_t>(i) * 1'000'000;
    if (rng() % 5 == 0) {
      e.kind = "tick";
      e.price = 109000 + static_cast<std::int64_t>(rng() % 500);
    } else {
      e.bid = 109000 + static_cast<std::int64_t>(rng() % 500);
      e.ask = e.bid + static_cast<std::int64_t>(rng() % 50);
      if (s == 2) {
        e.bid = 15000 + static_cast<std::int64_t>(rng() % 500);
        e.ask = e.bid + static_cast<std::int64_t>(rng() % 50);
      }
    }
    if (s == 2 && e.kind == "tick") e.price = 15000;
    out.push_back(e);
  }
  return out;
}

}  // namespace

// Property 1: decode(concat(chunks)) must equal decode(stream) for every
// partition of the bytes.
TEST(Property, ChunkPartitioningDoesNotChangeTheTrace) {
  std::mt19937 rng(424242);
  const auto specs = mixed_corpus(400, rng);
  const std::string wire = build_wire(specs);

  const auto reference = run_trace(wire, {wire.size()});
  ASSERT_FALSE(reference.empty());

  const std::vector<std::vector<std::size_t>> partitions = {
      {1}, {2}, {3}, {4}, {5}, {7}, {13}, {64}, {4096}, {65536},
      {1, 7, 3, 100, 2}, {4, 4, 4}, {1, 1, 1, 1000},
  };
  for (const auto& p : partitions) {
    const auto got = run_trace(wire, p);
    ASSERT_EQ(got, reference) << "partition starting with " << p[0];
  }
}

TEST(Property, RandomChunkPartitionsAgree) {
  std::mt19937 rng(97531);
  const auto specs = mixed_corpus(250, rng);
  const std::string wire = build_wire(specs);
  const auto reference = run_trace(wire, {wire.size()});

  for (int trial = 0; trial < 25; ++trial) {
    std::vector<std::size_t> chunks;
    for (int i = 0; i < 8; ++i) chunks.push_back(1 + (rng() % 500));
    ASSERT_EQ(run_trace(wire, chunks), reference) << "trial=" << trial;
  }
}

// Property 3: the same ordered input with the same logical clock must
// classify identically however many times it is replayed.
TEST(Property, ReplayIsDeterministic) {
  std::mt19937 rng(135790);
  const auto specs = mixed_corpus(300, rng);
  const std::string wire = build_wire(specs);

  const auto first = run_trace(wire, {997});
  for (int i = 0; i < 5; ++i) ASSERT_EQ(run_trace(wire, {997}), first);
}

// Property 2: interleaving another symbol must not change a symbol's own
// trace.
TEST(Property, SymbolTracesAreIndependentOfInterleaving) {
  std::vector<EventSpec> only_eur;
  for (int i = 1; i <= 50; ++i) {
    EventSpec e;
    e.symbol = "EURUSD";
    e.sequence = i;
    e.event_time_ns = i * 1'000'000;
    e.bid = 109000 + i;
    e.ask = e.bid + 5;
    only_eur.push_back(e);
  }

  std::vector<EventSpec> interleaved;
  int g = 1;
  for (const auto& e : only_eur) {
    interleaved.push_back(e);
    EventSpec other;
    other.symbol = "GBPUSD";
    other.sequence = g++;
    other.event_time_ns = e.event_time_ns;
    other.bid = 125000;
    other.ask = 125005;
    interleaved.push_back(other);
  }

  const auto solo = run_trace(build_wire(only_eur), {64});
  auto mixed = run_trace(build_wire(interleaved), {64});
  mixed.erase(std::remove_if(mixed.begin(), mixed.end(),
                             [](const std::string& s) {
                               return s.find("GBPUSD") != std::string::npos;
                             }),
              mixed.end());
  EXPECT_EQ(mixed, solo);
}

// Property 5: nothing may be accepted before a valid magic appears.
TEST(Property, NothingIsAcceptedBeforeAValidMagic) {
  std::mt19937 rng(2468);
  const auto specs = mixed_corpus(20, rng);
  const std::string good = build_wire(specs);

  const std::vector<std::string> prefixes = {
      std::string("JSON"), std::string("MDP2"),
      std::string("\x00\x00\x00\x00", 4)};
  for (const std::string& prefix : prefixes) {
    bool fatal = false;
    const auto trace = run_trace(prefix + good, {37}, &fatal);
    EXPECT_TRUE(fatal);
    EXPECT_TRUE(trace.empty()) << "prefix=" << prefix;
  }
}

// Property 4: no normalisation. Core fields must survive byte-exact.
TEST(Property, CoreFieldsAreNeverRewritten) {
  std::vector<EventSpec> specs;
  const std::int64_t times[] = {0, 1, 999999999999, 4611686018427387903};
  for (int i = 0; i < 4; ++i) {
    EventSpec e;
    e.sequence = i + 1;
    e.event_time_ns = times[i];
    e.bid = 1 + i;
    e.ask = 1 + i;
    specs.push_back(e);
  }
  const auto trace = run_trace(build_wire(specs), {11});
  ASSERT_EQ(trace.size(), 4u);
  for (int i = 0; i < 4; ++i)
    EXPECT_NE(trace[static_cast<std::size_t>(i)].find(
                  "t=" + std::to_string(times[i])),
              std::string::npos)
        << trace[static_cast<std::size_t>(i)];
}
