// The market-versus-fault gate (cases A-01, A-02, A-03, A-06, A-09, A-10,
// section 3.5, fixture F11).
//
// The gate is one-way: it can suppress, never promote. These tests pin that
// property from both directions -- every non-market classification is
// suppressed with its own reason, and no configuration makes an unverified
// fault-test event publishable.
#include "policy/publish_gate.h"

#include <gtest/gtest.h>

#include <string>

namespace p04 {
namespace {

OutputEvent MakeEvent(Classification classification, const std::string& dataset,
                      const std::string& symbol, std::int64_t sequence) {
  OutputEvent out;
  out.classification = classification;
  out.feed_state = FeedState::kLive;
  out.event.dataset_id = dataset;
  out.event.symbol = symbol;
  out.event.sequence = sequence;
  out.event.event_time_ns = 1793322000000000000LL + sequence;
  out.event.price_scale = 100000;
  out.event.source_schema_version = "sim.v1";
  out.event.kind = EventKind::kQuote;
  out.event.quote.bid_ticks = 108345;
  out.event.quote.ask_ticks = 108348;
  return out;
}

PublishGateConfig MakeConfig() {
  PublishGateConfig config;
  config.single_active_dataset = true;
  config.max_active_symbols = 256;
  config.max_active_datasets = 4;
  return config;
}

TEST(PublishGate, AcceptsMarketValidLiveEvent) {
  PublishGate gate(MakeConfig());
  const auto decision = gate.Decide(MakeEvent(Classification::kMarketValid, "ds-a", "EURUSD", 1));
  EXPECT_TRUE(decision.market_valid());
  EXPECT_EQ(decision.lane, PublishLane::kMarketTopic);
  EXPECT_EQ(decision.reason, SuppressReason::kNone);
  EXPECT_EQ(gate.market_valid_count(), 1u);
}

// Case A-01: a business-rejected quote never reaches a market topic.
TEST(PublishGate, SuppressesBusinessReject) {
  PublishGate gate(MakeConfig());
  const auto decision =
      gate.Decide(MakeEvent(Classification::kBusinessReject, "ds-a", "EURUSD", 1));
  EXPECT_FALSE(decision.market_valid());
  EXPECT_EQ(decision.reason, SuppressReason::kBusinessReject);
  EXPECT_EQ(gate.market_valid_count(), 0u);
  EXPECT_EQ(gate.suppressed_count(SuppressReason::kBusinessReject), 1u);
}

// Case A-02: an upstream duplicate suppression is honoured, not re-decided. The
// fixture is sequences 1, 2, 2 and the expectation is two published identities,
// not three.
TEST(PublishGate, HonoursUpstreamDuplicateSuppression) {
  PublishGate gate(MakeConfig());
  EXPECT_TRUE(gate.Decide(MakeEvent(Classification::kMarketValid, "ds-a", "EURUSD", 1)).market_valid());
  EXPECT_TRUE(gate.Decide(MakeEvent(Classification::kMarketValid, "ds-a", "EURUSD", 2)).market_valid());
  const auto third =
      gate.Decide(MakeEvent(Classification::kDuplicateSuppressed, "ds-a", "EURUSD", 2));
  EXPECT_FALSE(third.market_valid());
  EXPECT_EQ(third.reason, SuppressReason::kDuplicateSuppressed);
  EXPECT_EQ(gate.market_valid_count(), 2u);
}

TEST(PublishGate, SuppressesEveryNonMarketClassificationWithItsOwnReason) {
  const std::vector<std::pair<Classification, SuppressReason>> expectations = {
      {Classification::kStructuralInvalid, SuppressReason::kStructuralInvalid},
      {Classification::kBusinessReject, SuppressReason::kBusinessReject},
      {Classification::kDuplicateSuppressed, SuppressReason::kDuplicateSuppressed},
      {Classification::kStaleSuppressed, SuppressReason::kStaleSuppressed},
      {Classification::kUnclassified, SuppressReason::kUnclassified},
  };
  for (const auto& [classification, reason] : expectations) {
    PublishGate gate(MakeConfig());
    const auto decision = gate.Decide(MakeEvent(classification, "ds-a", "EURUSD", 1));
    EXPECT_FALSE(decision.market_valid()) << ToString(classification);
    EXPECT_EQ(decision.reason, reason) << ToString(classification);
  }
}

// Case A-03, the important one. Without a verified fault manifest, a
// FAULT_TEST_ONLY event is refused even when the config says fault-test events
// may enter market topics. Fail closed in both dimensions.
TEST(PublishGate, FaultTestRequiresAVerifiedManifestEvenWhenAllowed) {
  PublishGateConfig config = MakeConfig();
  config.allow_fault_test_into_market_topics = true;
  config.fault_manifest_verified = false;
  PublishGate gate(config);
  const auto decision =
      gate.Decide(MakeEvent(Classification::kFaultTestOnly, "ds-a", "EURUSD", 1));
  EXPECT_FALSE(decision.market_valid());
  EXPECT_EQ(decision.reason, SuppressReason::kFaultManifestRequired);
  EXPECT_STREQ(ToString(decision.reason), "FAULT_MANIFEST_REQUIRED");
}

TEST(PublishGate, FaultTestIsRefusedFromMarketTopicsByDefault) {
  PublishGateConfig config = MakeConfig();
  config.fault_manifest_verified = true;
  config.allow_fault_test_into_market_topics = false;  // the strict default
  PublishGate gate(config);
  const auto decision =
      gate.Decide(MakeEvent(Classification::kFaultTestOnly, "ds-a", "EURUSD", 1));
  EXPECT_FALSE(decision.market_valid());
  EXPECT_EQ(decision.reason, SuppressReason::kFaultTestNotAllowedInMarket);
}

// With a verified manifest and an isolated anomaly topic configured, the event
// goes to the audit lane. Reaching the audit lane is not a market publish and
// is counted separately.
TEST(PublishGate, FaultTestGoesToIsolatedAuditLaneWhenConfigured) {
  PublishGateConfig config = MakeConfig();
  config.fault_manifest_verified = true;
  config.anomaly_topic = "md.anomalies.v1";
  PublishGate gate(config);
  const auto decision =
      gate.Decide(MakeEvent(Classification::kFaultTestOnly, "ds-a", "EURUSD", 1));
  EXPECT_FALSE(decision.market_valid());
  EXPECT_EQ(decision.lane, PublishLane::kAuditLane);
  EXPECT_EQ(gate.audit_lane_count(), 1u);
  EXPECT_EQ(gate.market_valid_count(), 0u);
}

TEST(PublishGate, FaultTestEntersMarketOnlyWithManifestAndExplicitOptIn) {
  PublishGateConfig config = MakeConfig();
  config.fault_manifest_verified = true;
  config.allow_fault_test_into_market_topics = true;
  PublishGate gate(config);
  const auto decision =
      gate.Decide(MakeEvent(Classification::kFaultTestOnly, "ds-a", "EURUSD", 1));
  EXPECT_TRUE(decision.market_valid());
  EXPECT_EQ(decision.detail, "fault_test_allowed_by_config")
      << "the run report must be able to show this was a deliberate test run";
}

// Case A-06: a market-valid event on a feed the adapter could not vouch for is
// suppressed with its own reason, not folded into "unclassified".
TEST(PublishGate, SuppressesNonLiveFeedStateWithItsOwnReason) {
  for (const FeedState state : {FeedState::kUnknown, FeedState::kWarmup,
                                FeedState::kStale, FeedState::kDisconnected}) {
    PublishGate gate(MakeConfig());
    OutputEvent event = MakeEvent(Classification::kMarketValid, "ds-a", "EURUSD", 1);
    event.feed_state = state;
    const auto decision = gate.Decide(event);
    EXPECT_FALSE(decision.market_valid()) << ToString(state);
    EXPECT_EQ(decision.reason, SuppressReason::kFeedStateNotLive) << ToString(state);
    EXPECT_NE(decision.detail.find(ToString(state)), std::string::npos) << decision.detail;
  }
}

TEST(PublishGate, FeedStateCheckCanBeRelaxed) {
  PublishGateConfig config = MakeConfig();
  config.require_live_feed_state = false;
  PublishGate gate(config);
  OutputEvent event = MakeEvent(Classification::kMarketValid, "ds-a", "EURUSD", 1);
  event.feed_state = FeedState::kWarmup;
  EXPECT_TRUE(gate.Decide(event).market_valid());
}

// Case A-09: two datasets reusing sequence 1 for the same symbol are distinct
// identities. With single_active_dataset off, both are publishable.
TEST(PublishGate, DatasetResetDoesNotMakeEventsDuplicates) {
  PublishGateConfig config = MakeConfig();
  config.single_active_dataset = false;
  PublishGate gate(config);
  EXPECT_TRUE(gate.Decide(MakeEvent(Classification::kMarketValid, "ds-a", "EURUSD", 1)).market_valid());
  EXPECT_TRUE(gate.Decide(MakeEvent(Classification::kMarketValid, "ds-b", "EURUSD", 1)).market_valid());
  EXPECT_EQ(gate.market_valid_count(), 2u);
  EXPECT_EQ(gate.observed_datasets().size(), 2u);
}

// Case A-10: with the single-dataset policy on, the second dataset is refused,
// and refused consistently against the first dataset admitted rather than
// against whatever arrived last.
TEST(PublishGate, RejectsDatasetMixingConsistently) {
  PublishGate gate(MakeConfig());
  EXPECT_TRUE(gate.Decide(MakeEvent(Classification::kMarketValid, "ds-a", "EURUSD", 1)).market_valid());
  for (int i = 0; i < 5; ++i) {
    const auto b = gate.Decide(MakeEvent(Classification::kMarketValid, "ds-b", "EURUSD", i));
    EXPECT_FALSE(b.market_valid());
    EXPECT_EQ(b.reason, SuppressReason::kDatasetMixRejected);
    // The interleaved ds-a event must still pass, which is what "consistently"
    // means: the policy does not flap to whichever dataset was last seen.
    EXPECT_TRUE(gate.Decide(MakeEvent(Classification::kMarketValid, "ds-a", "EURUSD", 10 + i)).market_valid());
  }
  EXPECT_EQ(gate.suppressed_count(SuppressReason::kDatasetMixRejected), 5u);
}

TEST(PublishGate, EnforcesDatasetAllowlist) {
  PublishGateConfig config = MakeConfig();
  config.single_active_dataset = false;
  config.dataset_allowlist = {"ds-a"};
  PublishGate gate(config);
  EXPECT_TRUE(gate.Decide(MakeEvent(Classification::kMarketValid, "ds-a", "EURUSD", 1)).market_valid());
  const auto rejected = gate.Decide(MakeEvent(Classification::kMarketValid, "ds-x", "EURUSD", 1));
  EXPECT_FALSE(rejected.market_valid());
  EXPECT_EQ(rejected.reason, SuppressReason::kDatasetNotAllowlisted);
}

// Fixture F11 and NFR-13: the active symbol set is bounded, and the bound is
// enforced before any per-symbol state is allocated for the offending event.
TEST(PublishGate, BoundsTheActiveSymbolSet) {
  PublishGateConfig config = MakeConfig();
  config.max_active_symbols = 16;
  PublishGate gate(config);
  for (int i = 0; i < 16; ++i) {
    const std::string symbol = "SYM" + std::to_string(i);
    EXPECT_TRUE(gate.Decide(MakeEvent(Classification::kMarketValid, "ds-a", symbol, 1)).market_valid())
        << symbol;
  }
  EXPECT_EQ(gate.observed_symbols().size(), 16u);

  // The 17th distinct symbol, and 10000 more after it, must all be refused
  // without the observed set growing. This is the property that keeps the
  // metrics exporter bounded under a hostile source.
  for (int i = 16; i < 10016; ++i) {
    const std::string symbol = "SYM" + std::to_string(i);
    const auto decision = gate.Decide(MakeEvent(Classification::kMarketValid, "ds-a", symbol, 1));
    ASSERT_FALSE(decision.market_valid()) << symbol;
    ASSERT_EQ(decision.reason, SuppressReason::kSymbolBudgetExceeded) << symbol;
  }
  EXPECT_EQ(gate.observed_symbols().size(), 16u)
      << "the observed set grew past the cap, so the cap is not a real bound";

  // An already-known symbol still works: the cap limits distinct symbols, not
  // throughput.
  EXPECT_TRUE(gate.Decide(MakeEvent(Classification::kMarketValid, "ds-a", "SYM0", 2)).market_valid());
}

TEST(PublishGate, BoundsTheActiveDatasetSet) {
  PublishGateConfig config = MakeConfig();
  config.single_active_dataset = false;
  config.max_active_datasets = 2;
  PublishGate gate(config);
  EXPECT_TRUE(gate.Decide(MakeEvent(Classification::kMarketValid, "ds-1", "EURUSD", 1)).market_valid());
  EXPECT_TRUE(gate.Decide(MakeEvent(Classification::kMarketValid, "ds-2", "EURUSD", 1)).market_valid());
  const auto third = gate.Decide(MakeEvent(Classification::kMarketValid, "ds-3", "EURUSD", 1));
  EXPECT_FALSE(third.market_valid());
  EXPECT_EQ(third.reason, SuppressReason::kDatasetBudgetExceeded);
  EXPECT_EQ(gate.observed_datasets().size(), 2u);
}

TEST(PublishGate, SuppressionReasonsAllHaveNames) {
  for (int i = 0; i <= static_cast<int>(SuppressReason::kDatasetBudgetExceeded); ++i) {
    EXPECT_STRNE(ToString(static_cast<SuppressReason>(i)), "UNMAPPED") << i;
  }
}

}  // namespace
}  // namespace p04
