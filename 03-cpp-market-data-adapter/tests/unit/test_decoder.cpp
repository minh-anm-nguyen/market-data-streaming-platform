// Protobuf decode stage. Group D of the what-if matrix.
#include <gtest/gtest.h>

#include <cstring>

#include "md_adapter/decoder.hpp"
#include "md_adapter/validators.hpp"
#include "test_support.hpp"

using namespace mdtest;
using md::ProtobufDecoder;
using md::Reason;

TEST(Decoder, DecodesAValidMessage) {
  ProtobufDecoder d;
  const std::string bytes = encode(EventSpec{});
  const auto r = d.decode(as_bytes(bytes));
  ASSERT_TRUE(r.ok);
  EXPECT_FALSE(r.unknown_fields);
  EXPECT_EQ(d.message().symbol(), "EURUSD");
}

// Parse success is not validity: proto3 accepts a message with nothing set.
TEST(Decoder, EmptyPayloadParsesButCarriesNoFields) {
  ProtobufDecoder d;
  const std::string empty;
  const auto r = d.decode(as_bytes(empty));
  EXPECT_TRUE(r.ok);
  EXPECT_FALSE(d.message().has_symbol());
  EXPECT_FALSE(d.message().has_sequence());
}

TEST(Decoder, RejectsMalformedWireData) {
  ProtobufDecoder d;
  const char* bad[] = {
      "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",  // runaway varint
      "\x0a\xff",                                   // length exceeds the buffer
      "\x08",                                       // tag with no value
      "\x0f",                                       // reserved wire type 7
  };
  for (const char* b : bad) {
    const std::string s(b, std::strlen(b));
    const auto r = d.decode(as_bytes(s));
    EXPECT_FALSE(r.ok) << "bytes=" << s.size();
    EXPECT_EQ(r.reason, Reason::kProtoDecodeError);
  }
}

// E2E-06: a failed parse must never leave the previous message visible.
TEST(Decoder, FailedParseDoesNotLeakThePreviousMessage) {
  ProtobufDecoder d;
  EventSpec s;
  s.symbol = "GBPUSD";
  s.sequence = 42;
  ASSERT_TRUE(d.decode(as_bytes(encode(s))).ok);
  ASSERT_EQ(d.message().sequence(), 42);

  const std::string garbage("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", 10);
  ASSERT_FALSE(d.decode(as_bytes(garbage)).ok);

  EXPECT_FALSE(d.message().has_sequence());
  EXPECT_FALSE(d.message().has_symbol());
}

TEST(Decoder, DetectsUnknownFields) {
  // Field 99, varint. A v1 consumer must notice it without guessing meaning.
  std::string bytes = encode(EventSpec{});
  bytes += std::string("\xd8\x06\x2a", 3);  // tag 99 varint, value 42

  ProtobufDecoder d;
  const auto r = d.decode(as_bytes(bytes));
  ASSERT_TRUE(r.ok);
  EXPECT_TRUE(r.unknown_fields);
}

// Property 8: two encoders may order fields differently on the wire; the
// decoded typed projection must be identical either way.
TEST(Decoder, FieldOrderOnTheWireDoesNotChangeTheProjection) {
  const std::string canonical = encode(EventSpec{});

  // Re-serialise through a second message object; protobuf makes no
  // canonical-bytes promise, so compare the decoded values, not the bytes.
  marketdata::v1::MarketDataEvent m;
  ASSERT_TRUE(m.ParseFromString(canonical));
  std::string again;
  ASSERT_TRUE(m.SerializeToString(&again));

  ProtobufDecoder d1;
  ProtobufDecoder d2;
  ASSERT_TRUE(d1.decode(as_bytes(canonical)).ok);
  md::DomainEvent e1;
  md::StructuralLimits lim;
  ASSERT_EQ(md::validate_structural(d1.message(), lim, false, e1),
            Reason::kNone);

  ASSERT_TRUE(d2.decode(as_bytes(again)).ok);
  md::DomainEvent e2;
  ASSERT_EQ(md::validate_structural(d2.message(), lim, false, e2),
            Reason::kNone);

  EXPECT_EQ(md::core_of(e1), md::core_of(e2));
  EXPECT_EQ(e1.symbol, e2.symbol);
  EXPECT_EQ(e1.sequence, e2.sequence);
}

TEST(Decoder, DescriptorDigestIsStableAndPinnable) {
  const std::string a = ProtobufDecoder::descriptor_sha256();
  const std::string b = ProtobufDecoder::descriptor_sha256();
  EXPECT_EQ(a, b);
  EXPECT_EQ(a.size(), 64u);

  const auto unpinned = ProtobufDecoder::check_descriptor("UNPINNED");
  EXPECT_FALSE(unpinned.pinned);
  EXPECT_TRUE(unpinned.matches);

  const auto good = ProtobufDecoder::check_descriptor(a);
  EXPECT_TRUE(good.pinned);
  EXPECT_TRUE(good.matches);

  const auto bad = ProtobufDecoder::check_descriptor(std::string(64, '0'));
  EXPECT_TRUE(bad.pinned);
  EXPECT_FALSE(bad.matches);
}
