// Tests for the real Project 03 upstream (ADR-0002).
//
// The reader's whole value is that it refuses things, so most of what follows
// feeds a deliberately wrong record and asserts the run stops. A reader that
// accepted these would hand the publisher a half-built event, and the
// conservation equations downstream would balance against the wrong number.

#include <cstdio>
#include <fstream>
#include <unistd.h>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "serialization/marketdata_codec.h"
#include "source/p03_source.h"

namespace p04 {
namespace {

std::string ToHex(const std::string& bytes) {
  static const char* kDigits = "0123456789abcdef";
  std::string out;
  out.reserve(bytes.size() * 2);
  for (char raw : bytes) {
    const auto c = static_cast<unsigned char>(raw);
    out.push_back(kDigits[c >> 4]);
    out.push_back(kDigits[c & 0x0f]);
  }
  return out;
}

TypedEvent SampleTick() {
  TypedEvent event;
  event.dataset_id = "ds-test";
  event.symbol = "EURUSD";
  event.sequence = 7;
  event.event_time_ns = 1793322000000000000LL;
  event.price_scale = 100000;
  event.source_schema_version = "sim.v1";
  event.kind = EventKind::kTick;
  event.tick.price_ticks = 108347;
  return event;
}

std::string EncodeSample(const TypedEvent& event) {
  std::string bytes;
  CodecError error = {};
  EXPECT_TRUE(Encode(event, &bytes, &error));
  return bytes;
}

// Builds one p04_stub record in exactly the shape 03-*/src/sink.cpp writes.
std::string Record(const std::string& key, const std::string& value_hex,
                   std::size_t value_bytes, const std::string& flags,
                   const std::string& feed_state,
                   const std::string& primary_reason) {
  return "{\"key\":\"" + key + "\",\"value_hex\":\"" + value_hex +
         "\",\"value_bytes\":" + std::to_string(value_bytes) +
         ",\"quality\":{\"flags\":" + flags + ",\"feed_state\":\"" + feed_state +
         "\",\"primary_reason\":\"" + primary_reason +
         "\",\"arrival_index\":3,\"event_id\":{\"dataset_id\":\"ds-test\"," +
         "\"symbol\":\"" + key + "\",\"sequence\":7}}}";
}

class TempFile {
 public:
  explicit TempFile(const std::string& contents) {
    char name[] = "/tmp/p04-p03-handoff-XXXXXX";
    const int fd = mkstemp(name);
    if (fd >= 0) close(fd);
    path_ = name;
    std::ofstream out(path_, std::ios::binary);
    out << contents;
  }
  ~TempFile() { std::remove(path_.c_str()); }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

P03HandoffConfig Config() {
  P03HandoffConfig config;
  config.run_id = "unit";
  config.descriptor_sha256 = PinnedDescriptorSha256();
  config.expected_dataset_id = "ds-test";
  config.expected_schema_version = "sim.v1";
  return config;
}

struct ReadResult {
  bool ok = false;
  std::vector<OutputEvent> events;
  P03HandoffStats stats;
  std::vector<SourceParseError> errors;
  std::string error;
};

ReadResult Read(const std::string& contents,
                const P03HandoffConfig& config = Config()) {
  TempFile file(contents);
  ReadResult result;
  result.ok = ReadP03Handoff(
      file.path(), config,
      [&](const OutputEvent& event) {
        result.events.push_back(event);
        return true;
      },
      &result.stats, &result.errors, &result.error);
  return result;
}

// --------------------------------------------------------------------------

TEST(P03Source, ReadsACleanRecordAndPreservesTheWireBytes) {
  const TypedEvent event = SampleTick();
  const std::string bytes = EncodeSample(event);
  const ReadResult result = Read(
      Record("EURUSD", ToHex(bytes), bytes.size(), "[]", "HEALTHY", "none") + "\n");

  ASSERT_TRUE(result.ok) << result.error;
  ASSERT_EQ(result.events.size(), 1u);
  const OutputEvent& out = result.events[0];
  EXPECT_EQ(out.classification, Classification::kMarketValid);
  EXPECT_EQ(out.feed_state, FeedState::kLive);
  EXPECT_EQ(out.quality_flags, kQualityNone);
  EXPECT_TRUE(SemanticEqual(out.event, event));
  EXPECT_EQ(out.provenance.arrival_index, 3u);
  EXPECT_EQ(result.stats.market_valid, 1u);
  EXPECT_EQ(result.stats.offered, 1u);
}

// P03 prints Reason::kNone as lowercase "none", alone among its reasons. This
// is the case that failed closed on the first real run.
TEST(P03Source, AcceptsLowercaseNoneAsACleanForward) {
  Classification classification = Classification::kUnclassified;
  std::uint32_t flags = 0;
  ASSERT_TRUE(ClassifyFromP03Reason("none", {}, &classification, &flags));
  EXPECT_EQ(classification, Classification::kMarketValid);
  EXPECT_EQ(flags, kQualityNone);
}

TEST(P03Source, StartUnverifiedIsWarmupNotLive) {
  // The state before any sequence can be checked is not a liveness claim.
  EXPECT_EQ(FeedStateFromP03("START_UNVERIFIED"), FeedState::kWarmup);
  EXPECT_EQ(FeedStateFromP03("INITIAL"), FeedState::kWarmup);
  EXPECT_EQ(FeedStateFromP03("HEALTHY"), FeedState::kLive);
  // DEGRADED is live data with a discontinuity, not stale data.
  EXPECT_EQ(FeedStateFromP03("DEGRADED"), FeedState::kLive);
  EXPECT_EQ(FeedStateFromP03("INACTIVE"), FeedState::kStale);
  EXPECT_EQ(FeedStateFromP03("INVALID"), FeedState::kUnknown);
}

TEST(P03Source, AGapIsForwardedAsValidWithAFlag) {
  // P03's ADR-07: a discontinuity is a property of the series, not a defect in
  // the event, so it stays publishable and the gap travels in the flags.
  Classification classification = Classification::kUnclassified;
  std::uint32_t flags = 0;
  ASSERT_TRUE(
      ClassifyFromP03Reason("SEQUENCE_GAP", {"GAP"}, &classification, &flags));
  EXPECT_EQ(classification, Classification::kMarketValid);
  EXPECT_TRUE(flags & kQualitySequenceGapBefore);
}

TEST(P03Source, SuppressionReasonsMapAwayFromMarketValid) {
  const struct {
    const char* reason;
    Classification expected;
  } cases[] = {
      {"DUPLICATE_EXACT", Classification::kDuplicateSuppressed},
      {"DUPLICATE_CONFLICT", Classification::kDuplicateSuppressed},
      {"EVENT_TIME_OLD", Classification::kStaleSuppressed},
      {"QUOTE_CROSSED", Classification::kBusinessReject},
      {"PROTO_DECODE_ERROR", Classification::kStructuralInvalid},
      {"FAULT_TEST_ONLY", Classification::kFaultTestOnly},
  };
  for (const auto& test_case : cases) {
    Classification classification = Classification::kUnclassified;
    std::uint32_t flags = 0;
    ASSERT_TRUE(
        ClassifyFromP03Reason(test_case.reason, {}, &classification, &flags))
        << test_case.reason;
    EXPECT_EQ(classification, test_case.expected) << test_case.reason;
  }
}

TEST(P03Source, AnUnknownReasonIsUnclassifiedAndStopsTheRun) {
  Classification classification = Classification::kMarketValid;
  std::uint32_t flags = 0;
  EXPECT_FALSE(ClassifyFromP03Reason("SOMETHING_P03_ADDED_LATER", {},
                                     &classification, &flags));
  EXPECT_EQ(classification, Classification::kUnclassified);

  const std::string bytes = EncodeSample(SampleTick());
  const ReadResult result =
      Read(Record("EURUSD", ToHex(bytes), bytes.size(), "[]", "HEALTHY",
                  "WHO_KNOWS") +
           "\n");
  EXPECT_FALSE(result.ok);
  ASSERT_FALSE(result.errors.empty());
  EXPECT_EQ(result.errors.back().reason, "P03_REASON_UNRECOGNISED");
}

TEST(P03Source, AFramedPayloadIsRejectedNotStripped) {
  // A Kafka value carries its own boundary, so MDP1 magic here is the upstream
  // packaging bug of case L07. Stripping it would hide it.
  const std::string framed = "MDP1" + EncodeSample(SampleTick());
  const ReadResult result = Read(
      Record("EURUSD", ToHex(framed), framed.size(), "[]", "HEALTHY", "none") + "\n");
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.stats.framing_violations, 1u);
  ASSERT_FALSE(result.errors.empty());
  EXPECT_EQ(result.errors.back().reason, "P03_FRAMED_PAYLOAD");
}

TEST(P03Source, AKeyThatDisagreesWithThePayloadSymbolIsRejected) {
  // The key picks the partition and the payload carries the meaning (case L08).
  const std::string bytes = EncodeSample(SampleTick());
  const ReadResult result = Read(
      Record("GBPUSD", ToHex(bytes), bytes.size(), "[]", "HEALTHY", "none") + "\n");
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.stats.key_symbol_mismatches, 1u);
  ASSERT_FALSE(result.errors.empty());
  EXPECT_EQ(result.errors.back().reason, "P03_KEY_SYMBOL_MISMATCH");
}

TEST(P03Source, ADeclaredLengthThatDisagreesWithTheHexIsRejected) {
  const std::string bytes = EncodeSample(SampleTick());
  const ReadResult result =
      Read(Record("EURUSD", ToHex(bytes), bytes.size() + 1, "[]", "HEALTHY",
                  "none") +
           "\n");
  EXPECT_FALSE(result.ok);
  ASSERT_FALSE(result.errors.empty());
  EXPECT_EQ(result.errors.back().reason, "P03_VALUE_LENGTH_MISMATCH");
}

TEST(P03Source, OddLengthHexIsRejected) {
  const ReadResult result =
      Read(Record("EURUSD", "0a1", 1, "[]", "HEALTHY", "none") + "\n");
  EXPECT_FALSE(result.ok);
  ASSERT_FALSE(result.errors.empty());
  EXPECT_EQ(result.errors.back().reason, "P03_VALUE_HEX_INVALID");
}

TEST(P03Source, AMismatchedDatasetIsRejected) {
  P03HandoffConfig config = Config();
  config.expected_dataset_id = "ds-some-other-run";
  const std::string bytes = EncodeSample(SampleTick());
  const ReadResult result =
      Read(Record("EURUSD", ToHex(bytes), bytes.size(), "[]", "HEALTHY", "none") +
               "\n",
           config);
  EXPECT_FALSE(result.ok);
  ASSERT_FALSE(result.errors.empty());
  EXPECT_EQ(result.errors.back().reason, "P03_DATASET_MISMATCH");
}

TEST(P03Source, MalformedJsonIsRejectedRatherThanPartiallyRead) {
  for (const char* line : {"not json", "{\"key\":\"EURUSD\"", "{}",
                           "{\"key\":\"EURUSD\",\"value_hex\":\"0a\"}"}) {
    const ReadResult result = Read(std::string(line) + "\n");
    EXPECT_FALSE(result.ok) << line;
  }
}

TEST(P03Source, BlankLinesAreSkippedAndNotCounted) {
  const std::string bytes = EncodeSample(SampleTick());
  const std::string record =
      Record("EURUSD", ToHex(bytes), bytes.size(), "[]", "HEALTHY", "none");
  const ReadResult result = Read("\n" + record + "\n\n" + record + "\n");
  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_EQ(result.stats.records_read, 2u);
  EXPECT_EQ(result.events.size(), 2u);
}

TEST(P03Source, AMissingFileIsAnErrorNotAnEmptyDataset) {
  P03HandoffStats stats;
  std::vector<SourceParseError> errors;
  std::string error;
  EXPECT_FALSE(ReadP03Handoff(
      "/nonexistent/p03-handoff.ndjson", Config(),
      [](const OutputEvent&) { return true; }, &stats, &errors, &error));
  EXPECT_FALSE(error.empty());
  EXPECT_EQ(stats.offered, 0u);
}

TEST(P03Source, TheCallbackCanStopEarlyWithoutFailingTheRun) {
  const std::string bytes = EncodeSample(SampleTick());
  const std::string record =
      Record("EURUSD", ToHex(bytes), bytes.size(), "[]", "HEALTHY", "none");
  TempFile file(record + "\n" + record + "\n" + record + "\n");

  P03HandoffStats stats;
  std::vector<SourceParseError> errors;
  std::string error;
  int seen = 0;
  const bool ok = ReadP03Handoff(
      file.path(), Config(),
      [&](const OutputEvent&) {
        ++seen;
        return seen < 2;  // stop after the second
      },
      &stats, &errors, &error);
  EXPECT_TRUE(ok) << error;
  EXPECT_EQ(seen, 2);
}

}  // namespace
}  // namespace p04
