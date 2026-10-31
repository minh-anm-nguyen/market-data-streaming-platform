// The property and metamorphic suite from section 10.4 and 10.5.
//
// Where the unit tests pin specific cases, these generate a constrained corpus
// and assert the invariants hold over all of it. The generator is seeded, so a
// failure is reproducible from the seed printed in the failure message rather
// than being a story about a run nobody can repeat.
//
// The metamorphic tests are the interesting half: they change something that
// must not matter (pacing, batching, the order events are offered in across
// symbols) and assert the semantic result is identical, while allowing the
// things that legitimately differ (offsets, wall-clock timing).
#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "app/publisher.h"
#include "ops/metrics.h"
#include "producer/fake_transport.h"
#include "routing/partition_policy.h"
#include "serialization/marketdata_codec.h"

namespace p04 {
namespace {

constexpr unsigned kSeed = 20261030;

struct GeneratedEvent {
  OutputEvent event;
};

// A constrained corpus: valid symbols, valid prices, both event kinds, and a
// sprinkling of classifications the gate must suppress. Deliberately not
// adversarial -- the hostile inputs live in the unit tests, and mixing them in
// here would make the conservation assertions ambiguous.
std::vector<OutputEvent> GenerateCorpus(std::size_t count, unsigned seed) {
  const std::vector<std::string> symbols = {"EURUSD", "GBPUSD", "USDJPY",
                                            "AUDUSD", "XAUUSD", "US500"};
  std::mt19937 rng(seed);
  std::uniform_int_distribution<std::size_t> symbol_pick(0, symbols.size() - 1);
  std::uniform_int_distribution<int> kind_pick(0, 2);
  std::uniform_int_distribution<int> classification_pick(0, 19);
  std::uniform_int_distribution<std::int64_t> price_pick(1, 1000000);

  std::map<std::string, std::int64_t> next_sequence;
  std::vector<OutputEvent> corpus;
  corpus.reserve(count);

  for (std::size_t i = 0; i < count; ++i) {
    const std::string& symbol = symbols[symbol_pick(rng)];
    OutputEvent out;
    out.feed_state = FeedState::kLive;
    // One in twenty is a classification the gate must refuse, so the
    // conservation equations have something to account for.
    const int roll = classification_pick(rng);
    out.classification = (roll == 0)   ? Classification::kBusinessReject
                         : (roll == 1) ? Classification::kDuplicateSuppressed
                                       : Classification::kMarketValid;
    out.event.dataset_id = "ds-property";
    out.event.symbol = symbol;
    out.event.sequence = ++next_sequence[symbol];
    out.event.event_time_ns = 1793322000000000000LL + static_cast<std::int64_t>(i);
    out.event.price_scale = 100000;
    out.event.source_schema_version = "sim.v1";
    if (kind_pick(rng) == 0) {
      out.event.kind = EventKind::kTick;
      out.event.tick.price_ticks = price_pick(rng);
    } else {
      out.event.kind = EventKind::kQuote;
      const std::int64_t bid = price_pick(rng);
      out.event.quote.bid_ticks = bid;
      out.event.quote.ask_ticks = bid + 1 + (static_cast<std::int64_t>(i) % 7);
    }
    corpus.push_back(std::move(out));
  }
  return corpus;
}

RouteConfig MakeRouteConfig() {
  RouteConfig config;
  config.ticks_topic = "md.ticks.v1";
  config.quotes_topic = "md.quotes.v1";
  config.expected_source_schema = "sim.v1";
  config.partition_count = 8;
  config.run_id = "property";
  return config;
}

// The semantic projection of what ended up on the fake broker, in a form two
// runs can be compared by. Offsets are deliberately excluded: they are allowed
// to differ between runs, and including them would make every metamorphic test
// fail for the wrong reason.
struct ObservedRecord {
  std::string topic;
  std::int32_t partition;
  std::string key;
  TypedEvent core;

  bool operator<(const ObservedRecord& other) const {
    if (topic != other.topic) return topic < other.topic;
    if (key != other.key) return key < other.key;
    const std::int64_t left = core.sequence.value_or(-1);
    const std::int64_t right = other.core.sequence.value_or(-1);
    return left < right;
  }
};

std::vector<ObservedRecord> Project(const FakeTransport& transport) {
  std::vector<ObservedRecord> out;
  for (const auto& record : transport.log()) {
    ObservedRecord projected;
    projected.topic = record.topic;
    projected.partition = record.partition;
    projected.key = record.key;
    CodecError error = CodecError::kNone;
    EXPECT_TRUE(Decode(record.value, &projected.core, &error)) << ToString(error);
    out.push_back(std::move(projected));
  }
  std::sort(out.begin(), out.end());
  return out;
}

struct RunResult {
  std::vector<ObservedRecord> records;
  LedgerCounters counters;
  std::size_t max_queue_records = 0;
  std::size_t max_queue_bytes = 0;
};

// Publishes a corpus through a fresh pipeline. poll_every controls how often
// delivery reports are served, which is the knob the metamorphic tests vary: it
// changes how deep the queue gets and how records batch, and must not change
// the semantic result.
RunResult RunCorpus(const std::vector<OutputEvent>& corpus, std::size_t poll_every,
                    std::size_t queue_records) {
  FakeTransportConfig transport_config;
  transport_config.partition_count = 8;
  auto transport = std::make_unique<FakeTransport>(transport_config);

  RouteTable routes(MakeRouteConfig());
  PublishGate gate(PublishGateConfig{});
  HandoffConfig handoff_config;
  handoff_config.max_records = queue_records;
  handoff_config.stall_timeout_ms = 200;
  BoundedHandoff handoff(handoff_config);
  PublishLedger ledger;
  Metrics metrics("md.ticks.v1", "md.quotes.v1", 8);
  Publisher publisher(transport.get(), &routes, &gate, &handoff, &ledger, &metrics,
                      PublisherConfig{});
  publisher.Install();

  RunResult result;
  std::size_t since_poll = 0;
  for (const OutputEvent& event : corpus) {
    publisher.Offer(event);
    const HandoffStats stats = handoff.Stats();
    result.max_queue_records = std::max(result.max_queue_records, stats.records);
    result.max_queue_bytes = std::max(result.max_queue_bytes, stats.bytes);
    if (++since_poll >= poll_every) {
      publisher.Poll(0);
      since_poll = 0;
    }
  }
  publisher.Drain(10);
  result.records = Project(*transport);
  result.counters = ledger.Counters();
  return result;
}

// ---------------------------------------------------------------------------
// Properties over the record contract (section 10.4, first block).
// ---------------------------------------------------------------------------

TEST(Property, EveryPublishedRecordSatisfiesTheRecordContract) {
  const auto corpus = GenerateCorpus(3000, kSeed);
  const RunResult run = RunCorpus(corpus, 16, 4096);
  ASSERT_FALSE(run.records.empty());

  for (const ObservedRecord& record : run.records) {
    // route(e).topic in {md.ticks.v1, md.quotes.v1}
    ASSERT_TRUE(record.topic == "md.ticks.v1" || record.topic == "md.quotes.v1")
        << record.topic;
    // kafka.key == encode_ascii(e.symbol) and the key is never empty
    ASSERT_FALSE(record.key.empty());
    ASSERT_TRUE(record.core.symbol.has_value());
    ASSERT_EQ(*record.core.symbol, record.key);
    // the oneof kind agrees with the topic
    ASSERT_TRUE(record.core.kind.has_value());
    const std::string expected_topic =
        (*record.core.kind == EventKind::kQuote) ? "md.quotes.v1" : "md.ticks.v1";
    ASSERT_EQ(record.topic, expected_topic);
    // the partition is the one the pinned partitioner chooses
    ASSERT_EQ(record.partition, SelectPartition(record.key, 8));
    // every identity field is present
    ASSERT_TRUE(record.core.dataset_id.has_value());
    ASSERT_TRUE(record.core.sequence.has_value());
    ASSERT_TRUE(record.core.event_time_ns.has_value());
    ASSERT_TRUE(record.core.price_scale.has_value());
  }
}

TEST(Property, ValueSizeNeverExceedsTheCap) {
  const auto corpus = GenerateCorpus(2000, kSeed + 1);
  for (const OutputEvent& event : corpus) {
    std::string encoded;
    CodecError error = CodecError::kNone;
    ASSERT_TRUE(Encode(event.event, &encoded, &error));
    ASSERT_LE(encoded.size(), 4096u);
  }
}

// decode(encode(x)).core == x.core over the whole corpus.
TEST(Property, CodecRoundTripIsSemanticallyLossless) {
  const auto corpus = GenerateCorpus(5000, kSeed + 2);
  for (const OutputEvent& event : corpus) {
    std::string encoded;
    CodecError error = CodecError::kNone;
    ASSERT_TRUE(Encode(event.event, &encoded, &error)) << ToString(error);
    TypedEvent decoded;
    ASSERT_TRUE(Decode(encoded, &decoded, &error)) << ToString(error);
    ASSERT_TRUE(SemanticEqual(event.event, decoded))
        << DescribeCore(event.event) << " became " << DescribeCore(decoded);
  }
}

TEST(Property, RoutingIsDeterministic) {
  const RouteTable table(MakeRouteConfig());
  const auto corpus = GenerateCorpus(1000, kSeed + 3);
  for (const OutputEvent& event : corpus) {
    std::string encoded;
    CodecError error = CodecError::kNone;
    ASSERT_TRUE(Encode(event.event, &encoded, &error));
    RouteReject reason = RouteReject::kNone;
    std::string detail;
    const auto first = table.Build(event, encoded, &reason, &detail);
    const auto second = table.Build(event, encoded, &reason, &detail);
    ASSERT_EQ(first.has_value(), second.has_value());
    if (!first.has_value()) continue;
    EXPECT_EQ(first->topic, second->topic);
    EXPECT_EQ(first->key, second->key);
    EXPECT_EQ(first->expected_partition, second->expected_partition);
    EXPECT_EQ(first->value, second->value);
  }
}

// ---------------------------------------------------------------------------
// Properties over the counters and the queue (section 10.4, second and fifth
// blocks).
// ---------------------------------------------------------------------------

TEST(Property, ConservationEquationsHoldAndQueueCapsBind) {
  for (const std::size_t queue_records : {8u, 64u, 512u, 4096u}) {
    const auto corpus = GenerateCorpus(2000, kSeed + 4);
    const RunResult run = RunCorpus(corpus, 32, queue_records);

    const LedgerCounters& c = run.counters;
    EXPECT_EQ(c.upstream_accepted, 2000u) << "queue=" << queue_records;
    EXPECT_EQ(c.upstream_accepted,
              c.policy_suppressed + c.route_rejected + c.app_reserve_rejected + c.queued_app)
        << "queue=" << queue_records;
    EXPECT_EQ(c.queued_app, c.enqueue_failed + c.queued_lib + c.pending_in_app)
        << "queue=" << queue_records;
    EXPECT_EQ(c.queued_lib, c.acked + c.failed_definite + c.unknown + c.pending_in_library)
        << "queue=" << queue_records;
    EXPECT_LE(run.max_queue_records, queue_records) << "the cap did not bind";
    EXPECT_EQ(c.double_finalize_refused, 0u);
  }
}

// A smaller queue must not change what ends up published, only how often the
// pipeline has to wait. This is the property that makes the queue a flow-control
// mechanism rather than a filter.
TEST(Metamorphic, QueueDepthDoesNotChangeTheSemanticResult) {
  const auto corpus = GenerateCorpus(1500, kSeed + 5);
  const RunResult deep = RunCorpus(corpus, 8, 4096);
  const RunResult shallow = RunCorpus(corpus, 8, 16);

  ASSERT_EQ(deep.records.size(), shallow.records.size());
  for (std::size_t i = 0; i < deep.records.size(); ++i) {
    EXPECT_EQ(deep.records[i].topic, shallow.records[i].topic) << "index " << i;
    EXPECT_EQ(deep.records[i].key, shallow.records[i].key) << "index " << i;
    EXPECT_EQ(deep.records[i].partition, shallow.records[i].partition) << "index " << i;
    EXPECT_TRUE(SemanticEqual(deep.records[i].core, shallow.records[i].core))
        << "index " << i;
  }
  EXPECT_EQ(deep.counters.acked, shallow.counters.acked);
  EXPECT_LE(shallow.max_queue_records, 16u);
}

// Section 10.4 metamorphic test (a): changing the pacing, here expressed as how
// often delivery reports are served, must give the same semantic identities.
TEST(Metamorphic, PollCadenceDoesNotChangeTheSemanticResult) {
  const auto corpus = GenerateCorpus(1200, kSeed + 6);
  const RunResult eager = RunCorpus(corpus, 1, 4096);     // poll after every event
  const RunResult lazy = RunCorpus(corpus, 256, 4096);    // poll rarely

  ASSERT_EQ(eager.records.size(), lazy.records.size());
  for (std::size_t i = 0; i < eager.records.size(); ++i) {
    EXPECT_EQ(eager.records[i].key, lazy.records[i].key) << "index " << i;
    EXPECT_TRUE(SemanticEqual(eager.records[i].core, lazy.records[i].core))
        << "index " << i;
  }
  EXPECT_EQ(eager.counters.acked, lazy.counters.acked);
  // What legitimately differs: the queue gets deeper when reports are served
  // less often. Asserting it, so the test is not accidentally comparing two
  // identical runs.
  EXPECT_GT(lazy.max_queue_records, eager.max_queue_records)
      << "the two runs did not actually differ in queue behaviour";
}

// ---------------------------------------------------------------------------
// Ordering (section 10.4, fourth block).
// ---------------------------------------------------------------------------

TEST(Property, SequencesIncreaseStrictlyWithinOneTopicPartitionAndKey) {
  const auto corpus = GenerateCorpus(4000, kSeed + 7);
  const RunResult run = RunCorpus(corpus, 16, 4096);

  std::map<std::tuple<std::string, std::int32_t, std::string>, std::int64_t> last;
  // run.records is sorted by (topic, key, sequence), so walking it in order and
  // requiring strict increase per group checks exactly the per-key claim; the
  // publish order within a partition is checked separately by the pipeline test
  // against the fake log offsets.
  for (const ObservedRecord& record : run.records) {
    const auto group = std::make_tuple(record.topic, record.partition, record.key);
    const std::int64_t sequence = record.core.sequence.value_or(-1);
    const auto it = last.find(group);
    if (it != last.end()) {
      ASSERT_GT(sequence, it->second)
          << record.topic << "/" << record.partition << " " << record.key;
    }
    last[group] = sequence;
  }
  EXPECT_FALSE(last.empty());
}

// The design is explicit that there is no cross-topic ordering claim. The
// property asserted here is that the two topics genuinely interleave in source
// order, so any consumer assuming a global order over both would be wrong --
// which is why no oracle in this project asserts one.
TEST(Property, TheTwoTopicsInterleaveSoNoGlobalOrderExists) {
  const auto corpus = GenerateCorpus(2000, kSeed + 8);
  const RunResult run = RunCorpus(corpus, 16, 4096);

  std::set<std::string> topics;
  for (const ObservedRecord& record : run.records) topics.insert(record.topic);
  ASSERT_EQ(topics.size(), 2u) << "the corpus must exercise both topics";

  // For one symbol, the sequences present on each topic must both be
  // non-contiguous: each topic is missing the events that went to the other.
  // A reader seeing a gap in md.ticks.v1 is therefore seeing normal operation,
  // not loss (case B-10).
  bool found_gap = false;
  for (const std::string& topic : topics) {
    std::vector<std::int64_t> sequences;
    for (const ObservedRecord& record : run.records) {
      if (record.topic == topic && record.key == "EURUSD") {
        sequences.push_back(record.core.sequence.value_or(-1));
      }
    }
    std::sort(sequences.begin(), sequences.end());
    for (std::size_t i = 1; i < sequences.size(); ++i) {
      if (sequences[i] != sequences[i - 1] + 1) found_gap = true;
    }
  }
  EXPECT_TRUE(found_gap)
      << "splitting event types across topics must leave sequence gaps in each; "
         "if it did not, this corpus cannot demonstrate case B-10";
}

// ---------------------------------------------------------------------------
// Partitioner properties.
// ---------------------------------------------------------------------------

TEST(Property, PartitionerIsStableAndSpreadsKeys) {
  std::vector<std::string> keys;
  for (int i = 0; i < 2000; ++i) keys.push_back("SYM" + std::to_string(i));

  std::map<std::int32_t, int> histogram;
  for (const std::string& key : keys) {
    const std::int32_t partition = SelectPartition(key, 8);
    ASSERT_GE(partition, 0);
    ASSERT_LT(partition, 8);
    ASSERT_EQ(partition, SelectPartition(key, 8)) << "not stable for " << key;
    ++histogram[partition];
  }
  EXPECT_EQ(histogram.size(), 8u) << "every partition should receive some keys";
  // A loose bound only: murmur2 is not a balanced allocator and asserting a
  // tight spread would make this test flaky for no benefit. What matters is
  // that no partition is empty and none swallows nearly everything.
  for (const auto& [partition, count] : histogram) {
    EXPECT_GT(count, 2000 / 8 / 4) << "partition " << partition << " is starved";
    EXPECT_LT(count, 2000 / 2) << "partition " << partition << " dominates";
  }
}

}  // namespace
}  // namespace p04
