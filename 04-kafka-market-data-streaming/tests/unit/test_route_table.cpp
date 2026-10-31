// Routing, key construction and the record-shape caps (cases A-04, B-01 to
// B-04, D-03, D-04, D-05, D-10, D-11, FR-02, FR-03, NFR-05).
//
// Every one of these is a negative test with a named reason. A single generic
// "invalid" verdict would make B-02, B-03, B-04 and D-03 indistinguishable in a
// report, which is why RouteReject has one member per failure mode and these
// tests assert on the specific member.
#include "routing/route_table.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <vector>

#include "routing/partition_policy.h"
#include "serialization/marketdata_codec.h"

namespace p04 {
namespace {

RouteConfig MakeConfig() {
  RouteConfig config;
  config.ticks_topic = "md.ticks.v1";
  config.quotes_topic = "md.quotes.v1";
  config.expected_source_schema = "sim.v1";
  config.partition_count = 8;
  config.run_id = "p04-test";
  config.descriptor_sha256 = std::string(64, 'a');
  return config;
}

OutputEvent MakeMarketValidQuote() {
  OutputEvent out;
  out.classification = Classification::kMarketValid;
  out.feed_state = FeedState::kLive;
  out.event.dataset_id = "ds-1234";
  out.event.symbol = "EURUSD";
  out.event.sequence = 7;
  out.event.event_time_ns = 1793322000000000000LL;
  out.event.price_scale = 100000;
  out.event.source_schema_version = "sim.v1";
  out.event.kind = EventKind::kQuote;
  out.event.quote.bid_ticks = 108345;
  out.event.quote.ask_ticks = 108348;
  return out;
}

OutputEvent MakeMarketValidTick() {
  OutputEvent out = MakeMarketValidQuote();
  out.event.kind = EventKind::kTick;
  out.event.quote = QuoteBody{};
  out.event.tick.price_ticks = 108347;
  return out;
}

std::string EncodeOf(const OutputEvent& event) {
  std::string encoded;
  CodecError error = CodecError::kNone;
  EXPECT_TRUE(Encode(event.event, &encoded, &error));
  return encoded;
}

struct BuildOutcome {
  std::optional<Route> route;
  RouteReject reason = RouteReject::kNone;
  std::string detail;
};

BuildOutcome Build(const RouteTable& table, const OutputEvent& event) {
  BuildOutcome outcome;
  outcome.route = table.Build(event, EncodeOf(event), &outcome.reason, &outcome.detail);
  return outcome;
}

// FR-02: the topic comes from the oneof arm, never from input text.
TEST(RouteTable, QuoteGoesToQuotesTopicAndTickToTicksTopic) {
  const RouteTable table(MakeConfig());
  const auto quote = Build(table, MakeMarketValidQuote());
  ASSERT_TRUE(quote.route.has_value()) << ToString(quote.reason);
  EXPECT_EQ(quote.route->topic, "md.quotes.v1");
  EXPECT_EQ(quote.route->kind, EventKind::kQuote);

  const auto tick = Build(table, MakeMarketValidTick());
  ASSERT_TRUE(tick.route.has_value()) << ToString(tick.reason);
  EXPECT_EQ(tick.route->topic, "md.ticks.v1");
  EXPECT_EQ(tick.route->kind, EventKind::kTick);
}

// FR-03: the key is the symbol bytes and the value parses as a bare
// MarketDataEvent with no framing.
TEST(RouteTable, KeyIsSymbolBytesAndValueIsBareProtobuf) {
  const RouteTable table(MakeConfig());
  const auto built = Build(table, MakeMarketValidQuote());
  ASSERT_TRUE(built.route.has_value());
  EXPECT_EQ(built.route->key, "EURUSD");
  EXPECT_FALSE(LooksLikeFramedPayload(built.route->value));

  TypedEvent decoded;
  CodecError error = CodecError::kNone;
  ASSERT_TRUE(Decode(built.route->value, &decoded, &error)) << ToString(error);
  ASSERT_TRUE(decoded.symbol.has_value());
  EXPECT_EQ(*decoded.symbol, built.route->key)
      << "key and value.symbol must agree, or reconciliation joins on a lie";
}

// FR-05: the expected partition is precomputed with the pinned partitioner so
// the delivery report can be audited against it.
TEST(RouteTable, ExpectedPartitionMatchesThePinnedPartitioner) {
  const RouteTable table(MakeConfig());
  const auto built = Build(table, MakeMarketValidQuote());
  ASSERT_TRUE(built.route.has_value());
  EXPECT_EQ(built.route->expected_partition, SelectPartition("EURUSD", 8));
  EXPECT_EQ(built.route->expected_partition, 5);
}

// Case D-04: an unset oneof has no route and must not fall back to a topic.
TEST(RouteTable, RejectsUnsetOneof) {
  const RouteTable table(MakeConfig());
  OutputEvent event = MakeMarketValidQuote();
  event.event.kind.reset();
  const auto built = Build(table, event);
  EXPECT_FALSE(built.route.has_value());
  EXPECT_EQ(built.reason, RouteReject::kPayloadKindUnset);
  EXPECT_STREQ(ToString(built.reason), "MISSING_PAYLOAD");
}

// Case D-03: an absent sequence is refused, not defaulted to zero.
TEST(RouteTable, RejectsAbsentSequence) {
  const RouteTable table(MakeConfig());
  OutputEvent event = MakeMarketValidQuote();
  event.event.sequence.reset();
  const auto built = Build(table, event);
  EXPECT_FALSE(built.route.has_value());
  EXPECT_EQ(built.reason, RouteReject::kSequenceAbsent);
}

TEST(RouteTable, RejectsEveryAbsentCoreField) {
  const RouteTable table(MakeConfig());
  {
    OutputEvent event = MakeMarketValidQuote();
    event.event.dataset_id.reset();
    EXPECT_EQ(Build(table, event).reason, RouteReject::kDatasetIdAbsent);
  }
  {
    OutputEvent event = MakeMarketValidQuote();
    event.event.symbol.reset();
    EXPECT_EQ(Build(table, event).reason, RouteReject::kSymbolAbsent);
  }
  {
    OutputEvent event = MakeMarketValidQuote();
    event.event.event_time_ns.reset();
    EXPECT_EQ(Build(table, event).reason, RouteReject::kEventTimeAbsent);
  }
  {
    OutputEvent event = MakeMarketValidQuote();
    event.event.price_scale.reset();
    EXPECT_EQ(Build(table, event).reason, RouteReject::kPriceScaleAbsent);
  }
  {
    OutputEvent event = MakeMarketValidQuote();
    event.event.source_schema_version.reset();
    EXPECT_EQ(Build(table, event).reason, RouteReject::kSchemaVersionAbsent);
  }
}

TEST(RouteTable, RejectsUnexpectedSchemaVersion) {
  const RouteTable table(MakeConfig());
  OutputEvent event = MakeMarketValidQuote();
  event.event.source_schema_version = "sim.v2";
  const auto built = Build(table, event);
  EXPECT_EQ(built.reason, RouteReject::kSchemaVersionUnexpected);
  EXPECT_NE(built.detail.find("sim.v2"), std::string::npos) << built.detail;
}

// Case B-02: an empty key is refused before the producer, so librdkafka never
// gets the chance to pick a random partition for it.
TEST(RouteTable, RejectsEmptySymbol) {
  const RouteTable table(MakeConfig());
  OutputEvent event = MakeMarketValidQuote();
  event.event.symbol = "";
  const auto built = Build(table, event);
  EXPECT_FALSE(built.route.has_value());
  EXPECT_EQ(built.reason, RouteReject::kSymbolEmpty);
  EXPECT_STREQ(ToString(built.reason), "KEY_INVALID_EMPTY");
}

// Case B-03: a lowercase symbol is refused, with a reason that says "not
// canonical" rather than "bad charset", because the fix upstream differs.
TEST(RouteTable, RefusesLowercaseSymbolRatherThanNormalisingIt) {
  const RouteTable table(MakeConfig());
  OutputEvent event = MakeMarketValidQuote();
  event.event.symbol = "eurusd";
  const auto built = Build(table, event);
  EXPECT_FALSE(built.route.has_value());
  EXPECT_EQ(built.reason, RouteReject::kSymbolNotCanonicalCase);

  OutputEvent mixed = MakeMarketValidQuote();
  mixed.event.symbol = "EurUsd";
  EXPECT_EQ(Build(table, mixed).reason, RouteReject::kSymbolNotCanonicalCase);
}

// Case B-04: NUL bytes, control characters and non-ASCII confusables.
TEST(RouteTable, RejectsHostileSymbolBytes) {
  const RouteTable table(MakeConfig());
  const std::vector<std::string> hostile = {
      std::string("EUR\0USD", 7),   // embedded NUL
      "EUR USD",                    // space
      "EUR\tUSD",                   // tab
      "EUR/USD",                    // slash
      "EUR-USD",                    // dash is not in the P01 alphabet
      "\xEF\xBC\xA5URUSD",          // fullwidth E, a Unicode confusable
      "EUR\nUSD",
      "EUR;DROP",
  };
  for (const std::string& symbol : hostile) {
    OutputEvent event = MakeMarketValidQuote();
    event.event.symbol = symbol;
    const auto built = Build(table, event);
    EXPECT_FALSE(built.route.has_value()) << "accepted a hostile symbol";
    EXPECT_EQ(built.reason, RouteReject::kSymbolCharsetInvalid)
        << "symbol byte count: " << symbol.size();
  }
}

// NFR-05: the key cap. A 33-byte symbol is refused before anything is queued.
TEST(RouteTable, EnforcesKeyLengthCap) {
  RouteConfig config = MakeConfig();
  config.max_key_bytes = 32;
  const RouteTable table(config);

  OutputEvent ok_event = MakeMarketValidQuote();
  ok_event.event.symbol = std::string(32, 'A');
  EXPECT_TRUE(Build(table, ok_event).route.has_value());

  OutputEvent too_long = MakeMarketValidQuote();
  too_long.event.symbol = std::string(33, 'A');
  EXPECT_EQ(Build(table, too_long).reason, RouteReject::kSymbolTooLong);

  OutputEvent too_short = MakeMarketValidQuote();
  too_short.event.symbol = "AB";
  EXPECT_EQ(Build(table, too_short).reason, RouteReject::kSymbolTooShort);
}

TEST(RouteTable, RejectsIncompletePayloadBodies) {
  const RouteTable table(MakeConfig());
  {
    OutputEvent event = MakeMarketValidQuote();
    event.event.quote.ask_ticks.reset();
    const auto built = Build(table, event);
    EXPECT_EQ(built.reason, RouteReject::kQuoteBodyIncomplete);
    EXPECT_NE(built.detail.find("ask present=0"), std::string::npos) << built.detail;
  }
  {
    OutputEvent event = MakeMarketValidQuote();
    event.event.quote.bid_ticks.reset();
    EXPECT_EQ(Build(table, event).reason, RouteReject::kQuoteBodyIncomplete);
  }
  {
    OutputEvent event = MakeMarketValidTick();
    event.event.tick.price_ticks.reset();
    EXPECT_EQ(Build(table, event).reason, RouteReject::kTickBodyIncomplete);
  }
}

TEST(RouteTable, RejectsNegativeSequenceAndNonPositiveScale) {
  const RouteTable table(MakeConfig());
  {
    OutputEvent event = MakeMarketValidQuote();
    event.event.sequence = -1;
    EXPECT_EQ(Build(table, event).reason, RouteReject::kSequenceNegative);
  }
  {
    OutputEvent event = MakeMarketValidQuote();
    event.event.price_scale = 0;
    EXPECT_EQ(Build(table, event).reason, RouteReject::kPriceScaleNotPositive);
  }
  {
    OutputEvent event = MakeMarketValidQuote();
    event.event.price_scale = -100;
    EXPECT_EQ(Build(table, event).reason, RouteReject::kPriceScaleNotPositive);
  }
}

// Case D-10: 4096 bytes is allowed, 4097 is refused, and the refusal happens
// before any queue reservation. A hand-made value is passed in rather than a
// real encoding, which is why Build takes the encoded value as a parameter: the
// size guard has to be testable independently of the encoder.
TEST(RouteTable, EnforcesValueSizeCap) {
  RouteConfig config = MakeConfig();
  config.max_value_bytes = 4096;
  const RouteTable table(config);
  const OutputEvent event = MakeMarketValidQuote();

  const std::string at_cap(4096, '\x01');
  RouteReject reason = RouteReject::kNone;
  std::string detail;
  EXPECT_TRUE(table.Build(event, at_cap, &reason, &detail).has_value())
      << ToString(reason);

  const std::string over_cap(4097, '\x01');
  EXPECT_FALSE(table.Build(event, over_cap, &reason, &detail).has_value());
  EXPECT_EQ(reason, RouteReject::kValueTooLarge);
  EXPECT_NE(detail.find("4097"), std::string::npos) << detail;
}

// Case D-11: the header budget. A dataset_id long enough to blow the 512-byte
// total is refused before anything is queued.
TEST(RouteTable, EnforcesHeaderByteCap) {
  RouteConfig config = MakeConfig();
  config.max_headers_total_bytes = 512;
  const RouteTable table(config);

  OutputEvent event = MakeMarketValidQuote();
  event.event.dataset_id = std::string(600, 'd');
  const auto built = Build(table, event);
  EXPECT_FALSE(built.route.has_value());
  EXPECT_EQ(built.reason, RouteReject::kHeaderTooLarge);
}

// Section 3.4: headers are bounded and every value derives from an already
// validated field. md-dataset-id must mirror the value (case D-05).
TEST(RouteTable, EmitsBoundedHeadersThatMirrorTheValue) {
  const RouteTable table(MakeConfig());
  const auto built = Build(table, MakeMarketValidQuote());
  ASSERT_TRUE(built.route.has_value());

  std::size_t total = 0;
  bool saw_dataset = false;
  bool saw_schema = false;
  bool saw_policy = false;
  for (const auto& header : built.route->headers) {
    total += header.key.size() + header.value.size();
    if (header.key == "md-dataset-id") {
      saw_dataset = true;
      EXPECT_EQ(header.value, "ds-1234");
    }
    if (header.key == "md-schema") {
      saw_schema = true;
      EXPECT_EQ(header.value, "marketdata.v1");
    }
    if (header.key == "md-publish-policy") {
      saw_policy = true;
      EXPECT_EQ(header.value, "market_valid");
    }
    EXPECT_LE(header.key.size(), 64u) << header.key;
  }
  EXPECT_TRUE(saw_dataset);
  EXPECT_TRUE(saw_schema);
  EXPECT_TRUE(saw_policy);
  EXPECT_LE(total, 512u);
}

TEST(RouteTable, HeadersCanBeDisabled) {
  RouteConfig config = MakeConfig();
  config.emit_headers = false;
  const RouteTable table(config);
  const auto built = Build(table, MakeMarketValidQuote());
  ASSERT_TRUE(built.route.has_value());
  EXPECT_TRUE(built.route->headers.empty());
}

TEST(RouteTable, EstimatedBytesCoversKeyValueAndHeaders) {
  const RouteTable table(MakeConfig());
  const auto built = Build(table, MakeMarketValidQuote());
  ASSERT_TRUE(built.route.has_value());
  std::size_t expected = built.route->key.size() + built.route->value.size();
  for (const auto& header : built.route->headers) {
    expected += header.key.size() + header.value.size();
  }
  EXPECT_EQ(built.route->estimated_bytes, expected)
      << "the byte budget must account for everything the record carries, or "
         "the 8 MiB cap is not a real cap";
}

TEST(RouteTable, IsMarketTopicRecognisesOnlyTheConfiguredPair) {
  const RouteTable table(MakeConfig());
  EXPECT_TRUE(table.IsMarketTopic("md.ticks.v1"));
  EXPECT_TRUE(table.IsMarketTopic("md.quotes.v1"));
  EXPECT_FALSE(table.IsMarketTopic("md.anomalies.v1"));
  EXPECT_FALSE(table.IsMarketTopic("md.ticks.v2"));
  EXPECT_FALSE(table.IsMarketTopic(""));
}

TEST(RouteTable, ValidateSymbolAcceptsTheProjectOneAlphabet) {
  const RouteTable table(MakeConfig());
  const std::vector<std::string> valid = {"EURUSD", "USDJPY", "US500", "A1.B2_C3", "XAU"};
  for (const std::string& symbol : valid) {
    EXPECT_EQ(table.ValidateSymbol(symbol), RouteReject::kNone) << symbol;
  }
}

TEST(RouteTable, EveryRejectReasonHasAName) {
  for (int i = 0; i <= static_cast<int>(RouteReject::kTopicNotConfigured); ++i) {
    EXPECT_STRNE(ToString(static_cast<RouteReject>(i)), "UNMAPPED") << i;
  }
}

}  // namespace
}  // namespace p04
