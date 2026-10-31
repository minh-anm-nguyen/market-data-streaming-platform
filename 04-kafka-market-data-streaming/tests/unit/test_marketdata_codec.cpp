// The Kafka record value contract (cases D-01, D-02, D-03, D-04, D-06, D-07,
// D-12, D-14, NFR-01, NFR-05).
//
// The theme: presence and framing. proto3 optional carries presence all the way
// through, so "absent" and "zero" stay different; and a Kafka value is a bare
// MarketDataEvent, so anything carrying MDP1 or a length prefix is refused
// before it can be mistaken for market data.
#include "serialization/marketdata_codec.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace p04 {
namespace {

TypedEvent MakeQuote() {
  TypedEvent event;
  event.dataset_id = "ds-1234";
  event.symbol = "EURUSD";
  event.sequence = 42;
  event.event_time_ns = 1793322000000000000LL;
  event.price_scale = 100000;
  event.source_schema_version = "sim.v1";
  event.kind = EventKind::kQuote;
  event.quote.bid_ticks = 108345;
  event.quote.ask_ticks = 108348;
  return event;
}

TypedEvent MakeTick() {
  TypedEvent event = MakeQuote();
  event.kind = EventKind::kTick;
  event.quote = QuoteBody{};
  event.tick.price_ticks = 108347;
  return event;
}

TEST(Codec, RoundTripPreservesEverySemanticField) {
  const TypedEvent original = MakeQuote();
  std::string encoded;
  CodecError error = CodecError::kParseFailed;
  ASSERT_TRUE(Encode(original, &encoded, &error)) << ToString(error);
  EXPECT_EQ(error, CodecError::kNone);

  TypedEvent decoded;
  ASSERT_TRUE(Decode(encoded, &decoded, &error)) << ToString(error);
  EXPECT_TRUE(SemanticEqual(original, decoded))
      << DescribeCore(original) << " vs " << DescribeCore(decoded);
}

TEST(Codec, RoundTripTick) {
  const TypedEvent original = MakeTick();
  std::string encoded;
  CodecError error = CodecError::kNone;
  ASSERT_TRUE(Encode(original, &encoded, &error));
  TypedEvent decoded;
  ASSERT_TRUE(Decode(encoded, &decoded, &error));
  EXPECT_TRUE(SemanticEqual(original, decoded));
  ASSERT_TRUE(decoded.kind.has_value());
  EXPECT_EQ(*decoded.kind, EventKind::kTick);
}

// Case D-03. An absent sequence must survive the round trip as absent, so the
// route validator can refuse it. Without proto3 presence this would come back
// as sequence 0 and be published as a real event.
TEST(Codec, AbsentSequenceStaysAbsentAndIsNotZero) {
  TypedEvent event = MakeQuote();
  event.sequence.reset();
  std::string encoded;
  CodecError error = CodecError::kNone;
  ASSERT_TRUE(Encode(event, &encoded, &error));
  TypedEvent decoded;
  ASSERT_TRUE(Decode(encoded, &decoded, &error));
  EXPECT_FALSE(decoded.sequence.has_value());

  TypedEvent zero = MakeQuote();
  zero.sequence = 0;
  EXPECT_FALSE(SemanticEqual(event, zero))
      << "absent and zero must not compare equal, or D-03 is untestable";
}

TEST(Codec, PresentZeroStaysPresent) {
  TypedEvent event = MakeQuote();
  event.sequence = 0;
  std::string encoded;
  CodecError error = CodecError::kNone;
  ASSERT_TRUE(Encode(event, &encoded, &error));
  TypedEvent decoded;
  ASSERT_TRUE(Decode(encoded, &decoded, &error));
  ASSERT_TRUE(decoded.sequence.has_value());
  EXPECT_EQ(*decoded.sequence, 0);
}

// Case D-04. An unset oneof decodes with no kind. The codec does not reject it,
// deliberately: a verifier must be able to decode such a record in order to
// report it, and RouteTable is what refuses to publish one.
TEST(Codec, UnsetOneofDecodesWithNoKind) {
  TypedEvent event = MakeQuote();
  event.kind.reset();
  std::string encoded;
  CodecError error = CodecError::kNone;
  ASSERT_TRUE(Encode(event, &encoded, &error));
  TypedEvent decoded;
  ASSERT_TRUE(Decode(encoded, &decoded, &error));
  EXPECT_FALSE(decoded.kind.has_value());
}

// Cases D-06 and D-07: int64 edges must survive exactly. A float anywhere in
// the path would quietly lose the low digits of a price near the int64 limit.
TEST(Codec, Int64BoundariesSurviveExactly) {
  const std::vector<std::int64_t> edges = {
      std::numeric_limits<std::int64_t>::min(),
      std::numeric_limits<std::int64_t>::min() + 1,
      -1,
      0,
      1,
      4503599627370497LL,  // 2^52 + 1: the first integer a double cannot hold
      9007199254740993LL,  // 2^53 + 1: well beyond double precision
      std::numeric_limits<std::int64_t>::max() - 1,
      std::numeric_limits<std::int64_t>::max(),
  };
  for (const std::int64_t value : edges) {
    TypedEvent event = MakeTick();
    event.tick.price_ticks = value;
    event.event_time_ns = value;
    event.sequence = value;
    std::string encoded;
    CodecError error = CodecError::kNone;
    ASSERT_TRUE(Encode(event, &encoded, &error)) << value;
    TypedEvent decoded;
    ASSERT_TRUE(Decode(encoded, &decoded, &error)) << value;
    ASSERT_TRUE(decoded.tick.price_ticks.has_value());
    EXPECT_EQ(*decoded.tick.price_ticks, value);
    EXPECT_EQ(*decoded.event_time_ns, value);
    EXPECT_EQ(*decoded.sequence, value);
  }
}

// Case D-01. An MDP1-framed buffer is refused rather than parsed into whatever
// the magic bytes happen to look like.
TEST(Codec, RejectsMdp1FramedValue) {
  std::string encoded;
  CodecError error = CodecError::kNone;
  ASSERT_TRUE(Encode(MakeQuote(), &encoded, &error));
  const std::string framed = "MDP1" + encoded;
  EXPECT_TRUE(LooksLikeFramedPayload(framed));
  TypedEvent decoded;
  EXPECT_FALSE(Decode(framed, &decoded, &error));
  EXPECT_EQ(error, CodecError::kFramedPayload);
}

// Case D-02. A big-endian length prefix, which is what a .pbstream record looks
// like once the magic is stripped.
TEST(Codec, RejectsBigEndianLengthPrefixedValue) {
  std::string encoded;
  CodecError error = CodecError::kNone;
  ASSERT_TRUE(Encode(MakeQuote(), &encoded, &error));
  const auto length = static_cast<std::uint32_t>(encoded.size());
  std::string prefixed;
  prefixed.push_back(static_cast<char>((length >> 24) & 0xff));
  prefixed.push_back(static_cast<char>((length >> 16) & 0xff));
  prefixed.push_back(static_cast<char>((length >> 8) & 0xff));
  prefixed.push_back(static_cast<char>(length & 0xff));
  prefixed += encoded;

  EXPECT_TRUE(LooksLikeFramedPayload(prefixed));
  TypedEvent decoded;
  EXPECT_FALSE(Decode(prefixed, &decoded, &error));
  EXPECT_EQ(error, CodecError::kFramedPayload);
}

// A legitimately encoded event must not trip the framing heuristic, or the
// guard would reject real data. Checked over many shapes, not just one.
TEST(Codec, ValidEncodingsAreNotMistakenForFraming) {
  for (int i = 0; i < 200; ++i) {
    TypedEvent event = (i % 2 == 0) ? MakeQuote() : MakeTick();
    event.sequence = i;
    event.symbol = std::string("SYM") + std::to_string(i);
    std::string encoded;
    CodecError error = CodecError::kNone;
    ASSERT_TRUE(Encode(event, &encoded, &error));
    EXPECT_FALSE(LooksLikeFramedPayload(encoded))
        << "iteration " << i << " size " << encoded.size();
  }
}

TEST(Codec, RejectsEmptyValue) {
  TypedEvent decoded;
  CodecError error = CodecError::kNone;
  EXPECT_FALSE(Decode("", &decoded, &error));
  EXPECT_EQ(error, CodecError::kEmptyPayload);
}

// Case D-12: bytes injected by something other than this publisher must fail to
// decode rather than produce a plausible event.
TEST(Codec, RejectsGarbage) {
  CodecError error = CodecError::kNone;
  TypedEvent decoded;
  EXPECT_FALSE(Decode(std::string("\xff\xff\xff\xff", 4), &decoded, &error));
  EXPECT_FALSE(Decode(std::string("\x08", 1), &decoded, &error));
}

// Case D-14: byte equality is not the oracle. Two encoders may legally produce
// different bytes, so the projection is what is compared.
TEST(Codec, SemanticEqualityIsTheOracleNotBytes) {
  const TypedEvent event = MakeQuote();
  std::string first;
  std::string second;
  CodecError error = CodecError::kNone;
  ASSERT_TRUE(Encode(event, &first, &error));
  ASSERT_TRUE(Encode(event, &second, &error));
  TypedEvent a;
  TypedEvent b;
  ASSERT_TRUE(Decode(first, &a, &error));
  ASSERT_TRUE(Decode(second, &b, &error));
  EXPECT_TRUE(SemanticEqual(a, b));
}

TEST(Codec, SemanticEqualDetectsEveryFieldDifference) {
  const TypedEvent base = MakeQuote();
  {
    TypedEvent other = base; other.dataset_id = "ds-other";
    EXPECT_FALSE(SemanticEqual(base, other));
  }
  {
    TypedEvent other = base; other.symbol = "GBPUSD";
    EXPECT_FALSE(SemanticEqual(base, other));
  }
  {
    TypedEvent other = base; other.sequence = 43;
    EXPECT_FALSE(SemanticEqual(base, other));
  }
  {
    TypedEvent other = base; other.event_time_ns = 1;
    EXPECT_FALSE(SemanticEqual(base, other));
  }
  {
    TypedEvent other = base; other.price_scale = 1000;
    EXPECT_FALSE(SemanticEqual(base, other));
  }
  {
    TypedEvent other = base; other.source_schema_version = "sim.v2";
    EXPECT_FALSE(SemanticEqual(base, other));
  }
  {
    TypedEvent other = base; other.quote.bid_ticks = 108346;
    EXPECT_FALSE(SemanticEqual(base, other));
  }
  {
    TypedEvent other = base; other.quote.ask_ticks.reset();
    EXPECT_FALSE(SemanticEqual(base, other));
  }
  {
    // A quote and a tick are never equal even with identical identity fields:
    // they belong on different topics.
    const TypedEvent other = MakeTick();
    EXPECT_FALSE(SemanticEqual(base, other));
  }
}

TEST(Codec, DescriptorPinIsStableAndNamed) {
  const std::string digest = PinnedDescriptorSha256();
  EXPECT_EQ(digest.size(), 64u) << "expected a hex SHA-256";
  EXPECT_EQ(digest, PinnedDescriptorSha256());
  EXPECT_EQ(PinnedMessageFullName(), "marketdata.v1.MarketDataEvent");
}

// NFR-05: a well-formed event must sit far below the 4096-byte value cap, or
// the cap would be rejecting ordinary data.
TEST(Codec, TypicalEventIsFarBelowTheValueCap) {
  std::string encoded;
  CodecError error = CodecError::kNone;
  ASSERT_TRUE(Encode(MakeQuote(), &encoded, &error));
  EXPECT_LT(encoded.size(), 100u) << "encoded size " << encoded.size();
}

}  // namespace
}  // namespace p04
