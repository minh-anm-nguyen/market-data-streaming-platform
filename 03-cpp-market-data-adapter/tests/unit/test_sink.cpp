// Sinks: artifact shape and, more importantly, what "complete" means.
//
// The rule under test is that a written file is not a finished artifact. Only
// commit() -- fsync, rename, marker -- may produce one.
#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <filesystem>

#include "md_adapter/sha256.hpp"
#include "md_adapter/sink.hpp"
#include "test_support.hpp"

using namespace mdtest;
using md::ClassifiedEvent;
using md::Reason;

namespace {

ClassifiedEvent ev(std::int64_t seq, std::uint32_t flags = 0,
                   const std::string& raw = "") {
  ClassifiedEvent e;
  e.source = quote_event(seq, seq * 1000);
  e.arrival_index = static_cast<std::uint64_t>(seq) - 1;
  e.quality_flags = flags;
  e.feed_state = md::FeedState::kHealthy;
  e.primary_reason = md::Reason::kNone;
  e.raw_wire = raw;
  return e;
}

std::size_t count_lines(const std::string& s) {
  return static_cast<std::size_t>(std::count(s.begin(), s.end(), '\n'));
}

}  // namespace

TEST(JsonEscape, EscapesControlAndNonAsciiBytes) {
  std::string out;
  md::json_escape("a\"b\\c", out);
  EXPECT_EQ(out, "\"a\\\"b\\\\c\"");

  out.clear();
  md::json_escape(std::string("x\x01y", 3), out);
  EXPECT_EQ(out, "\"x\\u0001y\"");

  // Field content can come from an untrusted feed, so nothing raw reaches a
  // file a human will open.
  out.clear();
  md::json_escape("caf\xc3\xa9", out);
  EXPECT_EQ(out, "\"caf\\u00c3\\u00a9\"");
}

TEST(MetricsOnlySink, CountsWithoutWriting) {
  md::MetricsOnlySink s;
  EXPECT_EQ(s.write(ev(1)), Reason::kNone);
  EXPECT_EQ(s.write(ev(2)), Reason::kNone);
  EXPECT_EQ(s.commit(), Reason::kNone);
  EXPECT_EQ(s.events_written(), 2u);
  EXPECT_TRUE(s.artifact_path().empty());
}

TEST(NdjsonSink, WritesToPartUntilCommitted) {
  TempDir dir;
  const std::string out = dir.file("events.ndjson");
  md::NdjsonTestSink s(out);
  ASSERT_EQ(s.open(), Reason::kNone);
  ASSERT_EQ(s.write(ev(1)), Reason::kNone);

  // Before commit the final name must not exist.
  EXPECT_FALSE(std::filesystem::exists(out));
  EXPECT_TRUE(std::filesystem::exists(out + ".part"));

  ASSERT_EQ(s.commit(), Reason::kNone);
  EXPECT_TRUE(std::filesystem::exists(out));
  EXPECT_FALSE(std::filesystem::exists(out + ".part"));
  EXPECT_TRUE(std::filesystem::exists(out + ".complete"));
}

// E2E-07 / I-group: an aborted run must leave visible evidence that it did
// not finish, and must never be mistaken for a complete artifact.
TEST(NdjsonSink, AbortLeavesThePartFileAndNoMarker) {
  TempDir dir;
  const std::string out = dir.file("events.ndjson");
  md::NdjsonTestSink s(out);
  ASSERT_EQ(s.open(), Reason::kNone);
  ASSERT_EQ(s.write(ev(1)), Reason::kNone);
  s.abort();

  EXPECT_TRUE(std::filesystem::exists(out + ".part"));
  EXPECT_FALSE(std::filesystem::exists(out));
  EXPECT_FALSE(std::filesystem::exists(out + ".complete"));
}

TEST(NdjsonSink, CompletionMarkerRecordsCountAndDigest) {
  TempDir dir;
  const std::string out = dir.file("events.ndjson");
  md::NdjsonTestSink s(out);
  ASSERT_EQ(s.open(), Reason::kNone);
  for (int i = 1; i <= 5; ++i) ASSERT_EQ(s.write(ev(i)), Reason::kNone);
  ASSERT_EQ(s.commit(), Reason::kNone);

  const std::string marker = read_file(out + ".complete");
  EXPECT_NE(marker.find("\"events\":5"), std::string::npos) << marker;
  // The digest in the marker must describe the artifact that is on disk.
  EXPECT_NE(marker.find(md::sha256_file(out)), std::string::npos);
  EXPECT_EQ(s.artifact_sha256(), md::sha256_file(out));
  EXPECT_EQ(count_lines(read_file(out)), 5u);
}

TEST(NdjsonSink, RecordShapeIsStableAndIncludesQuality) {
  TempDir dir;
  const std::string out = dir.file("events.ndjson");
  md::NdjsonTestSink s(out);
  ASSERT_EQ(s.open(), Reason::kNone);
  ASSERT_EQ(s.write(ev(7, md::quality::kGap | md::quality::kDegraded)),
            Reason::kNone);
  ASSERT_EQ(s.commit(), Reason::kNone);

  const std::string line = read_file(out);
  EXPECT_NE(line.find("\"sequence\":7"), std::string::npos) << line;
  EXPECT_NE(line.find("\"symbol\":\"EURUSD\""), std::string::npos);
  EXPECT_NE(line.find("\"kind\":\"quote\""), std::string::npos);
  EXPECT_NE(line.find("\"bid_ticks\":109000"), std::string::npos);
  EXPECT_NE(line.find("\"quality_flags\":[\"GAP\",\"DEGRADED\"]"),
            std::string::npos)
      << line;
  EXPECT_NE(line.find("\"feed_state\":\"HEALTHY\""), std::string::npos);
}

TEST(NdjsonSink, TickRecordsCarryPriceNotBidAsk) {
  TempDir dir;
  const std::string out = dir.file("t.ndjson");
  md::NdjsonTestSink s(out);
  ASSERT_EQ(s.open(), Reason::kNone);
  ClassifiedEvent e;
  e.source = tick_event(3, 3000, 123456);
  ASSERT_EQ(s.write(e), Reason::kNone);
  ASSERT_EQ(s.commit(), Reason::kNone);

  const std::string line = read_file(out);
  EXPECT_NE(line.find("\"kind\":\"tick\""), std::string::npos);
  EXPECT_NE(line.find("\"price_ticks\":123456"), std::string::npos);
  EXPECT_EQ(line.find("bid_ticks"), std::string::npos);
}

TEST(NdjsonSink, RepeatedRunsProduceIdenticalBytes) {
  TempDir dir;
  std::string first;
  for (int run = 0; run < 2; ++run) {
    const std::string out = dir.file("run" + std::to_string(run) + ".ndjson");
    md::NdjsonTestSink s(out);
    ASSERT_EQ(s.open(), Reason::kNone);
    for (int i = 1; i <= 20; ++i) ASSERT_EQ(s.write(ev(i)), Reason::kNone);
    ASSERT_EQ(s.commit(), Reason::kNone);
    if (run == 0) {
      first = read_file(out);
    } else {
      EXPECT_EQ(read_file(out), first);
    }
  }
}

TEST(NdjsonSink, OpenFailsOnAnUnwritablePath) {
  md::NdjsonTestSink s("/proc/definitely-not-writable/x.ndjson");
  EXPECT_EQ(s.open(), Reason::kSinkIoError);
  EXPECT_EQ(s.write(ev(1)), Reason::kSinkIoError);
}

// ADR-14 / E2E-09: record.value must be the untouched P02 wire message --
// no MDP1 magic, no length prefix -- with quality on a side channel.
TEST(P04StubSink, ValueIsRawProtobufAndQualityIsSeparate) {
  TempDir dir;
  const std::string out = dir.file("p04.ndjson");
  const std::string raw = encode(EventSpec{});

  md::P04StubSink s(out);
  ASSERT_EQ(s.open(), Reason::kNone);
  EXPECT_TRUE(s.needs_raw_wire());
  ASSERT_EQ(s.write(ev(1, md::quality::kGap, raw)), Reason::kNone);
  ASSERT_EQ(s.commit(), Reason::kNone);

  const std::string line = read_file(out);
  EXPECT_NE(line.find("\"key\":\"EURUSD\""), std::string::npos) << line;
  EXPECT_NE(line.find("\"value_bytes\":" + std::to_string(raw.size())),
            std::string::npos);

  // Extract the hex value and confirm it is byte-identical to the input.
  const std::size_t at = line.find("\"value_hex\":\"");
  ASSERT_NE(at, std::string::npos);
  const std::size_t start = at + std::strlen("\"value_hex\":\"");
  const std::size_t end = line.find('"', start);
  const std::string hex = line.substr(start, end - start);
  ASSERT_EQ(hex.size(), raw.size() * 2);

  std::string decoded;
  for (std::size_t i = 0; i < hex.size(); i += 2)
    decoded += static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16));
  EXPECT_EQ(decoded, raw);
  // The MDP1 magic and the BE32 length belong to the transport, not to the
  // Kafka record value.
  EXPECT_NE(decoded.substr(0, 4), "MDP1");

  EXPECT_NE(line.find("\"quality\":{\"flags\":[\"GAP\"]"), std::string::npos)
      << line;
}

TEST(P04StubSink, CommitProducesAMarkerToo) {
  TempDir dir;
  const std::string out = dir.file("p04.ndjson");
  md::P04StubSink s(out);
  ASSERT_EQ(s.open(), Reason::kNone);
  ASSERT_EQ(s.write(ev(1, 0, encode(EventSpec{}))), Reason::kNone);
  ASSERT_EQ(s.commit(), Reason::kNone);
  EXPECT_TRUE(std::filesystem::exists(out + ".complete"));
}
