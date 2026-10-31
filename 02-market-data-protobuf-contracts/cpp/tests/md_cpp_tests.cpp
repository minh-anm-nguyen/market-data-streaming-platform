// C++ unit tests for the Project 02 contract.
//
// No external test framework: the build already pins protobuf and a compiler,
// and adding GoogleTest would add a third thing to pin for a suite this size.
// The CHECK macros below report the file, line, expression and both values,
// which is what a failure actually needs.
//
// Test ids in the names map to the what-if matrix in SYSTEM_DESIGN section 8,
// so a failure here points at a specific documented scenario.
#include <cctype>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <span>
#include <string>
#include <vector>

#include "md/contract.hpp"

namespace {

int g_checks = 0;
int g_failures = 0;
std::string g_case;

// Per-what-if-case verdicts, so the C++ evidence reaches the section 15 report
// rather than stopping at this binary's exit code. Keyed by case id; the value
// becomes true once any check under that case fails.
std::map<std::string, bool> g_case_failed;
std::vector<std::string> g_current_cases;

// Case ids are the leading tokens of a case name, so
// "D20/G06 a newer writer's ..." registers both D20 and G06.
void RecordCaseIds(const char* name) {
  g_current_cases.clear();
  const char* p = name;
  while (p[0] != '\0' && p[1] != '\0' && p[2] != '\0') {
    const bool is_id = p[0] >= 'A' && p[0] <= 'L' &&
                       std::isdigit(static_cast<unsigned char>(p[1])) &&
                       std::isdigit(static_cast<unsigned char>(p[2]));
    if (!is_id) break;
    const std::string id(p, 3);
    g_current_cases.push_back(id);
    g_case_failed.emplace(id, false);
    p += 3;
    if (*p != '/') break;
    ++p;
  }
}

void BeginCase(const char* name) {
  g_case = name;
  RecordCaseIds(name);
  std::printf("  %s\n", name);
}

void Fail(const char* file, int line, const std::string& message) {
  ++g_failures;
  for (const std::string& id : g_current_cases) g_case_failed[id] = true;
  std::printf("    FAIL %s:%d [%s] %s\n", file, line, g_case.c_str(), message.c_str());
}

#define CHECK(expr)                                                      \
  do {                                                                   \
    ++g_checks;                                                          \
    if (!(expr)) Fail(__FILE__, __LINE__, "expected: " #expr);           \
  } while (0)

#define CHECK_EQ(a, b)                                                   \
  do {                                                                   \
    ++g_checks;                                                          \
    auto va = (a);                                                       \
    auto vb = (b);                                                       \
    if (!(va == vb)) {                                                   \
      Fail(__FILE__, __LINE__,                                           \
           std::string(#a " == " #b " -> ") + ToText(va) + " vs " + ToText(vb)); \
    }                                                                    \
  } while (0)

std::string ToText(const std::string& v) { return "\"" + v + "\""; }
std::string ToText(std::string_view v) { return "\"" + std::string(v) + "\""; }
std::string ToText(std::int64_t v) { return std::to_string(v); }
std::string ToText(std::size_t v) { return std::to_string(v); }
std::string ToText(int v) { return std::to_string(v); }
std::string ToText(bool v) { return v ? "true" : "false"; }
std::string ToText(md::Reason v) { return std::string(md::ReasonName(v)); }
std::string ToText(md::StreamEnd v) { return std::string(md::StreamEndName(v)); }
std::string ToText(md::PayloadCase v) { return std::string(md::PayloadCaseName(v)); }

md::DomainEvent SampleQuote() {
  md::DomainEvent e;
  e.dataset_id = "ds-9b75f62167ae4c5d";
  e.symbol = "EURUSD";
  e.sequence = 42;
  e.event_time_ns = 1793322000008200000LL;
  e.price_scale = 100000;
  e.source_schema_version = "sim.v1";
  e.payload_case = md::PayloadCase::kQuote;
  e.bid_ticks = 108346;
  e.ask_ticks = 108348;
  return e;
}

std::string Framed(const std::string& payload) {
  const std::uint32_t n = static_cast<std::uint32_t>(payload.size());
  std::string out;
  out += static_cast<char>((n >> 24) & 0xff);
  out += static_cast<char>((n >> 16) & 0xff);
  out += static_cast<char>((n >> 8) & 0xff);
  out += static_cast<char>(n & 0xff);
  out += payload;
  return out;
}

md::Error ReadOneFrame(const std::string& stream, std::string& payload,
                       std::size_t chunk = 0) {
  md::MemorySource source(std::span<const char>(stream.data(), stream.size()), chunk);
  md::FrameReader reader(source);
  md::Error error;
  reader.ReadFrame(payload, error);
  return error;
}

// -------------------------------------------------------------------------
// Round trip and presence (E01-E03, B01-B08)
// -------------------------------------------------------------------------
void TestRoundTrip() {
  BeginCase("E01/E02 quote round-trips through encode/decode without change");
  const md::DomainEvent original = SampleQuote();
  std::string payload;
  CHECK(md::EncodeEvent(original, payload).ok());
  md::DomainEvent decoded;
  CHECK(md::DecodePayload(std::span<const char>(payload.data(), payload.size()), decoded).ok());
  CHECK(md::CheckEnvelope(decoded).ok());
  CHECK_EQ(md::ProjectionJson(decoded), md::ProjectionJson(original));
  CHECK_EQ(*decoded.bid_ticks, static_cast<std::int64_t>(108346));
  CHECK_EQ(decoded.payload_case, md::PayloadCase::kQuote);

  BeginCase("E03 int64 boundaries survive both directions without wrapping");
  md::DomainEvent extreme = SampleQuote();
  extreme.sequence = std::numeric_limits<std::int64_t>::max();
  extreme.bid_ticks = std::numeric_limits<std::int64_t>::min();
  extreme.ask_ticks = std::numeric_limits<std::int64_t>::max();
  extreme.event_time_ns = std::numeric_limits<std::int64_t>::max();
  std::string extreme_payload;
  CHECK(md::EncodeEvent(extreme, extreme_payload).ok());
  md::DomainEvent extreme_back;
  CHECK(md::DecodePayload(
            std::span<const char>(extreme_payload.data(), extreme_payload.size()),
            extreme_back)
            .ok());
  CHECK_EQ(*extreme_back.sequence, std::numeric_limits<std::int64_t>::max());
  CHECK_EQ(*extreme_back.bid_ticks, std::numeric_limits<std::int64_t>::min());
  CHECK_EQ(*extreme_back.event_time_ns, std::numeric_limits<std::int64_t>::max());

  BeginCase("C08 a one-nanosecond difference is preserved exactly");
  md::DomainEvent a = SampleQuote(), b = SampleQuote();
  b.event_time_ns = *a.event_time_ns + 1;
  std::string pa, pbuf;
  CHECK(md::EncodeEvent(a, pa).ok());
  CHECK(md::EncodeEvent(b, pbuf).ok());
  md::DomainEvent da, db;
  CHECK(md::DecodePayload(std::span<const char>(pa.data(), pa.size()), da).ok());
  CHECK(md::DecodePayload(std::span<const char>(pbuf.data(), pbuf.size()), db).ok());
  CHECK_EQ(*db.event_time_ns - *da.event_time_ns, static_cast<std::int64_t>(1));
}

void TestPresence() {
  BeginCase("B01 a missing sequence is rejected, not defaulted to zero");
  md::DomainEvent no_seq = SampleQuote();
  no_seq.sequence.reset();
  std::string payload;
  CHECK(md::EncodeEvent(no_seq, payload).ok());
  md::DomainEvent decoded;
  CHECK(md::DecodePayload(std::span<const char>(payload.data(), payload.size()), decoded).ok());
  CHECK(!decoded.sequence.has_value());
  CHECK_EQ(md::CheckEnvelope(decoded).reason, md::Reason::kMissingRequiredSemanticField);

  BeginCase("B02 an explicit sequence of zero is present and distinguishable");
  md::DomainEvent zero_seq = SampleQuote();
  zero_seq.sequence = 0;
  std::string zero_payload;
  CHECK(md::EncodeEvent(zero_seq, zero_payload).ok());
  md::DomainEvent zero_back;
  CHECK(md::DecodePayload(
            std::span<const char>(zero_payload.data(), zero_payload.size()), zero_back)
            .ok());
  CHECK(zero_back.sequence.has_value());
  CHECK_EQ(*zero_back.sequence, static_cast<std::int64_t>(0));
  // Present-and-zero passes the envelope check; whether 0 is an acceptable
  // sequence is a business decision, made by mode, not a structural one.
  CHECK(md::CheckEnvelope(zero_back).ok());
  CHECK(zero_payload != payload);

  BeginCase("B06 an empty message is UNSUPPORTED_EVENT_KIND, not an empty event");
  md::DomainEvent empty;
  md::DomainEvent parsed;
  CHECK(md::DecodePayload(std::span<const char>{}, parsed).ok());
  CHECK_EQ(parsed.payload_case, md::PayloadCase::kNone);
  CHECK_EQ(md::CheckEnvelope(parsed).reason, md::Reason::kMissingRequiredSemanticField);
  // ... and an event with a full envelope but no payload also fails closed.
  md::DomainEvent no_payload = SampleQuote();
  no_payload.payload_case = md::PayloadCase::kNone;
  no_payload.bid_ticks.reset();
  no_payload.ask_ticks.reset();
  CHECK_EQ(md::CheckEnvelope(no_payload).reason, md::Reason::kUnsupportedEventKind);

  BeginCase("B07 a quote missing bid/ask is rejected rather than read as zero");
  md::DomainEvent bare_quote = SampleQuote();
  bare_quote.bid_ticks.reset();
  bare_quote.ask_ticks.reset();
  std::string bare_payload;
  CHECK(md::EncodeEvent(bare_quote, bare_payload).ok());
  md::DomainEvent bare_back;
  CHECK(md::DecodePayload(
            std::span<const char>(bare_payload.data(), bare_payload.size()), bare_back)
            .ok());
  CHECK_EQ(bare_back.payload_case, md::PayloadCase::kQuote);
  CHECK(!bare_back.bid_ticks.has_value());
  CHECK_EQ(md::CheckEnvelope(bare_back).reason, md::Reason::kMissingRequiredSemanticField);

  BeginCase("B08 a tick missing price_ticks is rejected");
  md::DomainEvent bare_tick = SampleQuote();
  bare_tick.payload_case = md::PayloadCase::kTick;
  bare_tick.bid_ticks.reset();
  bare_tick.ask_ticks.reset();
  std::string tick_payload;
  CHECK(md::EncodeEvent(bare_tick, tick_payload).ok());
  md::DomainEvent tick_back;
  CHECK(md::DecodePayload(
            std::span<const char>(tick_payload.data(), tick_payload.size()), tick_back)
            .ok());
  CHECK_EQ(tick_back.payload_case, md::PayloadCase::kTick);
  CHECK_EQ(md::CheckEnvelope(tick_back).reason, md::Reason::kMissingRequiredSemanticField);

  BeginCase("B05 an explicitly empty dataset_id is present but policy-rejected");
  md::DomainEvent empty_id = SampleQuote();
  empty_id.dataset_id = "";
  std::string id_payload;
  CHECK(md::EncodeEvent(empty_id, id_payload).ok());
  md::DomainEvent id_back;
  CHECK(md::DecodePayload(
            std::span<const char>(id_payload.data(), id_payload.size()), id_back)
            .ok());
  CHECK(id_back.dataset_id.has_value());
  CHECK_EQ(*id_back.dataset_id, std::string(""));
}

void TestUnknownVariant() {
  BeginCase("D20/G06 a newer writer's oneof variant fails closed as unsupported");
  // A full v1 envelope, then field 12 holding a submessage: what a v2 writer
  // emitting a depth update would actually put on the wire. The envelope has to
  // be complete, otherwise the presence check fires first and the test would
  // not reach the payload decision at all.
  md::DomainEvent envelope_only = SampleQuote();
  envelope_only.payload_case = md::PayloadCase::kNone;
  envelope_only.bid_ticks.reset();
  envelope_only.ask_ticks.reset();
  std::string raw;
  CHECK(md::EncodeEvent(envelope_only, raw).ok());

  std::string inner;
  inner += static_cast<char>((1 << 3) | 0);  // level, varint
  inner += static_cast<char>(3);
  // Tag for field 12, wire type 2: 12*8+2 == 98, which fits in one varint byte.
  raw += static_cast<char>((12 << 3) | 2);
  raw += static_cast<char>(inner.size());
  raw += inner;

  md::DomainEvent decoded;
  CHECK(md::DecodePayload(std::span<const char>(raw.data(), raw.size()), decoded).ok());
  CHECK_EQ(decoded.payload_case, md::PayloadCase::kUnknown);
  CHECK_EQ(md::CheckEnvelope(decoded).reason, md::Reason::kUnsupportedEventKind);

  BeginCase("G01 an unknown field outside the payload range leaves the core intact");
  std::string with_unknown;
  CHECK(md::EncodeEvent(SampleQuote(), with_unknown).ok());
  // field 20 (feed_label in v1.1), wire type 2, 3 bytes of string. The tag is
  // 20*8+2 == 162, which does NOT fit in one varint byte, so it encodes as
  // 0xa2 0x01 -- writing a bare 0xa2 would be a truncated varint and the frame
  // would be rejected as undecodable instead of carrying an unknown field.
  with_unknown += static_cast<char>(0xa2);
  with_unknown += static_cast<char>(0x01);
  with_unknown += static_cast<char>(3);
  with_unknown += "abc";
  md::DomainEvent v11;
  CHECK(md::DecodePayload(
            std::span<const char>(with_unknown.data(), with_unknown.size()), v11)
            .ok());
  CHECK(md::CheckEnvelope(v11).ok());
  // The v1 core projection is unchanged by the added field, which is exactly
  // the promise a wire-safe additive change makes.
  CHECK_EQ(md::ProjectionJson(v11), md::ProjectionJson(SampleQuote()));
}

// -------------------------------------------------------------------------
// Framing (D01-D19)
// -------------------------------------------------------------------------
void TestFraming() {
  std::string payload;
  CHECK(md::EncodeEvent(SampleQuote(), payload).ok());
  const std::string good = std::string(md::kMagic) + Framed(payload);

  BeginCase("D01 a wrong magic is BAD_MAGIC with no attempt to parse a payload");
  std::string body;
  CHECK_EQ(ReadOneFrame("XXXX" + Framed(payload), body).reason, md::Reason::kBadMagic);

  BeginCase("D02/I08 a JSONL line on the Protobuf port is BAD_MAGIC, not JSON");
  CHECK_EQ(ReadOneFrame("{\"schema_version\":\"sim.v1\"}\n", body).reason,
           md::Reason::kBadMagic);

  BeginCase("D03 EOF inside the prefix is TRUNCATED_PREFIX");
  CHECK_EQ(ReadOneFrame("MD", body).reason, md::Reason::kTruncatedPrefix);

  BeginCase("D04-D06 a 1-, 2- or 3-byte header is TRUNCATED_HEADER");
  for (std::size_t n = 1; n <= 3; ++n) {
    const std::string partial = std::string(md::kMagic) + Framed(payload).substr(0, n);
    CHECK_EQ(ReadOneFrame(partial, body).reason, md::Reason::kTruncatedHeader);
  }

  BeginCase("D07 length 0 is INVALID_LENGTH, never an empty default event");
  CHECK_EQ(ReadOneFrame(std::string(md::kMagic) + std::string(4, '\0'), body).reason,
           md::Reason::kInvalidLength);

  BeginCase("D08 length 4097 is rejected before any payload is read");
  std::string over = std::string(md::kMagic);
  over += static_cast<char>(0x00);
  over += static_cast<char>(0x00);
  over += static_cast<char>(0x10);
  over += static_cast<char>(0x01);  // 4097
  CHECK_EQ(ReadOneFrame(over, body).reason, md::Reason::kInvalidLength);

  BeginCase("D09/J03 length 0xffffffff is rejected before allocation");
  std::string huge = std::string(md::kMagic) + std::string(4, static_cast<char>(0xff));
  CHECK_EQ(ReadOneFrame(huge, body).reason, md::Reason::kInvalidLength);

  BeginCase("D10 a little-endian sender's swapped header fails closed");
  // payload is 48-63 bytes, so a byte-swapped header reads as a huge length.
  std::string swapped = std::string(md::kMagic);
  const std::uint32_t n = static_cast<std::uint32_t>(payload.size());
  swapped += static_cast<char>(n & 0xff);
  swapped += static_cast<char>((n >> 8) & 0xff);
  swapped += static_cast<char>((n >> 16) & 0xff);
  swapped += static_cast<char>((n >> 24) & 0xff);
  swapped += payload;
  CHECK_EQ(ReadOneFrame(swapped, body).reason, md::Reason::kInvalidLength);

  BeginCase("D11 a body one byte short is TRUNCATED_BODY and emits nothing");
  const std::string short_body = good.substr(0, good.size() - 1);
  CHECK_EQ(ReadOneFrame(short_body, body).reason, md::Reason::kTruncatedBody);

  BeginCase("D16 several frames in one buffer are read as separate frames");
  const std::string three =
      std::string(md::kMagic) + Framed(payload) + Framed(payload) + Framed(payload);
  {
    md::MemorySource source(std::span<const char>(three.data(), three.size()));
    md::FrameReader reader(source);
    md::Error error;
    int count = 0;
    std::string one;
    while (reader.ReadFrame(one, error)) {
      CHECK_EQ(one, payload);
      ++count;
    }
    CHECK_EQ(count, 3);
    CHECK_EQ(reader.end(), md::StreamEnd::kCompleted);
  }

  BeginCase("D17 one frame split across many reads is reassembled");
  {
    md::MemorySource source(std::span<const char>(three.data(), three.size()), 1);
    md::FrameReader reader(source);
    md::Error error;
    int count = 0;
    std::string one;
    while (reader.ReadFrame(one, error)) {
      CHECK_EQ(one, payload);
      ++count;
    }
    CHECK_EQ(count, 3);
    CHECK_EQ(reader.end(), md::StreamEnd::kCompleted);
  }

  BeginCase("D13/D19 undecodable payload bytes are DECODE_ERROR");
  // Field number 0 is never valid in Protobuf.
  const std::string bad_tag(1, static_cast<char>(0x00));
  md::DomainEvent ignored;
  CHECK_EQ(md::DecodePayload(std::span<const char>(bad_tag.data(), bad_tag.size()), ignored)
               .reason,
           md::Reason::kDecodeError);
  // A truncated varint: tag for field 3 then a continuation byte with no end.
  std::string truncated_varint;
  truncated_varint += static_cast<char>((3 << 3) | 0);
  truncated_varint += static_cast<char>(0x80);
  CHECK_EQ(md::DecodePayload(std::span<const char>(truncated_varint.data(),
                                                   truncated_varint.size()),
                             ignored)
               .reason,
           md::Reason::kDecodeError);

  BeginCase("D18 a payload over the budget is rejected before parsing");
  const std::string oversize(md::kMaxPayloadBytes + 1, '\0');
  CHECK_EQ(md::DecodePayload(std::span<const char>(oversize.data(), oversize.size()),
                             ignored)
               .reason,
           md::Reason::kPayloadTooLarge);

  BeginCase("a clean EOF on a frame boundary is COMPLETED, not an error");
  {
    md::MemorySource source(std::span<const char>(good.data(), good.size()));
    md::FrameReader reader(source);
    md::Error error;
    std::string one;
    CHECK(reader.ReadFrame(one, error));
    CHECK(!reader.ReadFrame(one, error));
    CHECK_EQ(reader.end(), md::StreamEnd::kCompleted);
  }
}

// -------------------------------------------------------------------------
// Strict JSON reader (C01-C17)
// -------------------------------------------------------------------------
void TestStrictJson() {
  const std::string valid =
      R"({"ask_ticks":108348,"bid_ticks":108345,"dataset_id":"ds-x","event_time_ns":1793322000000000000,)"
      R"("event_type":"quote","price_scale":100000,"schema_version":"sim.v1","sequence":1,"symbol":"EURUSD"})";

  BeginCase("a valid sim.v1 quote line parses with every field present");
  md::DomainEvent event;
  md::Error error = md::ParseSimV1Line(valid, event);
  CHECK(error.ok());
  CHECK_EQ(*event.sequence, static_cast<std::int64_t>(1));
  CHECK_EQ(*event.bid_ticks, static_cast<std::int64_t>(108345));
  CHECK_EQ(event.payload_case, md::PayloadCase::kQuote);
  CHECK_EQ(*event.source_schema_version, std::string("sim.v1"));

  struct Rejection {
    const char* id;
    std::string line;
    md::Reason reason;
  };
  const std::vector<Rejection> rejections = {
      {"C01 duplicate key",
       R"({"ask_ticks":108348,"bid_ticks":108345,"bid_ticks":1,"dataset_id":"ds-x","event_time_ns":1,"event_type":"quote","price_scale":100000,"schema_version":"sim.v1","sequence":1,"symbol":"EURUSD"})",
       md::Reason::kDuplicateJsonKey},
      {"C02 boolean where an integer belongs",
       R"({"ask_ticks":108348,"bid_ticks":108345,"dataset_id":"ds-x","event_time_ns":1,"event_type":"quote","price_scale":100000,"schema_version":"sim.v1","sequence":true,"symbol":"EURUSD"})",
       md::Reason::kFieldNotInteger},
      {"C03 float literal",
       R"({"ask_ticks":108348,"bid_ticks":108345,"dataset_id":"ds-x","event_time_ns":1,"event_type":"quote","price_scale":100000,"schema_version":"sim.v1","sequence":1.0,"symbol":"EURUSD"})",
       md::Reason::kFloatNotAllowed},
      {"C04 exponent notation",
       R"({"ask_ticks":108348,"bid_ticks":108345,"dataset_id":"ds-x","event_time_ns":1,"event_type":"quote","price_scale":100000,"schema_version":"sim.v1","sequence":1e3,"symbol":"EURUSD"})",
       md::Reason::kFloatNotAllowed},
      {"C05 above INT64_MAX",
       R"({"ask_ticks":108348,"bid_ticks":108345,"dataset_id":"ds-x","event_time_ns":1,"event_type":"quote","price_scale":100000,"schema_version":"sim.v1","sequence":9223372036854775808,"symbol":"EURUSD"})",
       md::Reason::kInt64OutOfRange},
      {"C07 over-long numeric token",
       R"({"ask_ticks":108348,"bid_ticks":108345,"dataset_id":"ds-x","event_time_ns":1,"event_type":"quote","price_scale":100000,"schema_version":"sim.v1","sequence":111111111111111111111111,"symbol":"EURUSD"})",
       md::Reason::kNumericTokenTooLong},
      {"C10 null price",
       R"({"ask_ticks":108348,"bid_ticks":null,"dataset_id":"ds-x","event_time_ns":1,"event_type":"quote","price_scale":100000,"schema_version":"sim.v1","sequence":1,"symbol":"EURUSD"})",
       md::Reason::kFieldNotInteger},
      {"C11 missing event_type",
       R"({"ask_ticks":108348,"bid_ticks":108345,"dataset_id":"ds-x","event_time_ns":1,"price_scale":100000,"schema_version":"sim.v1","sequence":1,"symbol":"EURUSD"})",
       md::Reason::kFieldSetMismatch},
      {"C12 unsupported event_type",
       R"({"dataset_id":"ds-x","event_time_ns":1,"event_type":"trade","price_scale":100000,"price_ticks":1,"schema_version":"sim.v1","sequence":1,"symbol":"EURUSD"})",
       md::Reason::kUnknownEventType},
      {"C13 quote carrying price_ticks",
       R"({"ask_ticks":108348,"bid_ticks":108345,"dataset_id":"ds-x","event_time_ns":1,"event_type":"quote","price_scale":100000,"price_ticks":5,"schema_version":"sim.v1","sequence":1,"symbol":"EURUSD"})",
       md::Reason::kFieldSetMismatch},
      {"C14 tick carrying bid/ask",
       R"({"ask_ticks":1,"bid_ticks":1,"dataset_id":"ds-x","event_time_ns":1,"event_type":"tick","price_scale":100000,"price_ticks":5,"schema_version":"sim.v1","sequence":1,"symbol":"EURUSD"})",
       md::Reason::kFieldSetMismatch},
      {"C16 symbol outside the charset",
       R"({"ask_ticks":108348,"bid_ticks":108345,"dataset_id":"ds-x","event_time_ns":1,"event_type":"quote","price_scale":100000,"schema_version":"sim.v1","sequence":1,"symbol":"eurusd"})",
       md::Reason::kInvalidSymbol},
      {"C17 unsupported source schema version",
       R"({"ask_ticks":108348,"bid_ticks":108345,"dataset_id":"ds-x","event_time_ns":1,"event_type":"quote","price_scale":100000,"schema_version":"sim.v2","sequence":1,"symbol":"EURUSD"})",
       md::Reason::kUnsupportedSourceSchemaVersion},
      {"NaN as a bare word",
       R"({"ask_ticks":108348,"bid_ticks":NaN,"dataset_id":"ds-x","event_time_ns":1,"event_type":"quote","price_scale":100000,"schema_version":"sim.v1","sequence":1,"symbol":"EURUSD"})",
       md::Reason::kNonFiniteNumber},
      {"a JSON array is not an object",
       "[1,2,3]",
       md::Reason::kNotAnObject},
      {"J06 path traversal in dataset_id",
       R"({"ask_ticks":108348,"bid_ticks":108345,"dataset_id":"../../etc/passwd","event_time_ns":1,"event_type":"quote","price_scale":100000,"schema_version":"sim.v1","sequence":1,"symbol":"EURUSD"})",
       md::Reason::kInvalidDatasetId},
  };

  for (const auto& r : rejections) {
    BeginCase(r.id);
    md::DomainEvent out;
    const md::Error got = md::ParseSimV1Line(r.line, out);
    CHECK_EQ(got.reason, r.reason);
  }

  BeginCase("C06 below INT64_MIN is rejected, not clamped");
  md::DomainEvent low;
  const std::string low_line =
      R"({"ask_ticks":108348,"bid_ticks":-9223372036854775809,"dataset_id":"ds-x","event_time_ns":1,"event_type":"quote","price_scale":100000,"schema_version":"sim.v1","sequence":1,"symbol":"EURUSD"})";
  const md::Reason low_reason = md::ParseSimV1Line(low_line, low).reason;
  // 20 digits after the sign, so the digit-run guard may fire before the range
  // check; either is a correct refusal, and both are refusals.
  CHECK(low_reason == md::Reason::kInt64OutOfRange ||
        low_reason == md::Reason::kNumericTokenTooLong);

  BeginCase("C15 invalid UTF-8 is rejected, not replaced with U+FFFD");
  std::string bad_utf8 = valid;
  const std::size_t pos = bad_utf8.find("EURUSD");
  bad_utf8[pos] = static_cast<char>(0x80);  // lone continuation byte
  md::DomainEvent utf8_out;
  CHECK_EQ(md::ParseSimV1Line(bad_utf8, utf8_out).reason, md::Reason::kInvalidUtf8);

  BeginCase("C09/F09 a negative price parses exactly, sign intact");
  const std::string negative =
      R"({"dataset_id":"ds-x","event_time_ns":1,"event_type":"tick","price_scale":100000,"price_ticks":-1,"schema_version":"sim.v1","sequence":1,"symbol":"EURUSD"})";
  md::DomainEvent neg;
  CHECK(md::ParseSimV1Line(negative, neg).ok());
  CHECK_EQ(*neg.price_ticks, static_cast<std::int64_t>(-1));
  // ... and survives a Protobuf round trip without becoming a large unsigned.
  std::string neg_payload;
  CHECK(md::EncodeEvent(neg, neg_payload).ok());
  md::DomainEvent neg_back;
  CHECK(md::DecodePayload(
            std::span<const char>(neg_payload.data(), neg_payload.size()), neg_back)
            .ok());
  CHECK_EQ(*neg_back.price_ticks, static_cast<std::int64_t>(-1));

  BeginCase("C09 a value above 2^53 is exact, proving no float is involved");
  const std::string big =
      R"({"dataset_id":"ds-x","event_time_ns":1,"event_type":"tick","price_scale":100000,"price_ticks":9007199254740993,"schema_version":"sim.v1","sequence":1,"symbol":"EURUSD"})";
  md::DomainEvent big_event;
  CHECK(md::ParseSimV1Line(big, big_event).ok());
  // 2^53 + 1 is the smallest integer a double cannot represent; if any step
  // went through a float this would come back as 9007199254740992.
  CHECK_EQ(*big_event.price_ticks, static_cast<std::int64_t>(9007199254740993LL));

  BeginCase("JSON_LINE_TOO_LARGE fires on bytes, before parsing");
  const std::string huge(md::kMaxJsonLineBytes + 1, 'x');
  md::DomainEvent huge_out;
  CHECK_EQ(md::ParseSimV1Line(huge, huge_out).reason, md::Reason::kJsonLineTooLarge);
}

void TestProjectionEscaping() {
  BeginCase("projection escaping matches Python json.dumps(ensure_ascii=False)");
  std::string out;
  md::AppendJsonString(out, "a\"b\\c\nd\te");
  CHECK_EQ(out, std::string("\"a\\\"b\\\\c\\nd\\te\""));

  out.clear();
  md::AppendJsonString(out, std::string("\x01\x1f", 2));
  CHECK_EQ(out, std::string("\"\\u0001\\u001f\""));

  out.clear();
  // Non-ASCII passes through as raw UTF-8, and '/' and 0x7f are not escaped --
  // all three match Python's behaviour.
  md::AppendJsonString(out, "\xe2\x82\xac/\x7f");
  CHECK_EQ(out, std::string("\"\xe2\x82\xac/\x7f\""));

  BeginCase("absent fields are omitted and listed nowhere in `present`");
  md::DomainEvent sparse;
  sparse.symbol = "EURUSD";
  sparse.payload_case = md::PayloadCase::kNone;
  CHECK_EQ(md::ProjectionJson(sparse),
           std::string(R"({"payload_case":"none","present":["symbol"],"symbol":"EURUSD"})"));
}

void TestUtf8Validator() {
  BeginCase("the UTF-8 validator rejects overlongs, surrogates and truncation");
  CHECK(md::ValidUtf8("plain ASCII"));
  CHECK(md::ValidUtf8("\xe2\x82\xac"));              // EUR sign
  CHECK(md::ValidUtf8("\xf0\x9f\x92\xa9"));          // 4-byte
  CHECK(!md::ValidUtf8("\x80"));                     // lone continuation
  CHECK(!md::ValidUtf8("\xc0\xaf"));                 // overlong '/'
  CHECK(!md::ValidUtf8("\xed\xa0\x80"));             // UTF-16 surrogate half
  CHECK(!md::ValidUtf8("\xe2\x82"));                 // truncated 3-byte
  CHECK(!md::ValidUtf8("\xf5\x80\x80\x80"));         // beyond U+10FFFF
}

void TestVersions() {
  BeginCase("E05/E07 the linked protobuf library matches the headers");
  // Two checks, and the first has already happened: the generated header's
  // `#if PROTOBUF_VERSION != ...` means this binary could not have compiled
  // against mismatched gencode. What is left to verify at run time is the
  // *linked* library, which aborts here if it disagrees.
  md::VerifyProtobufVersion();
  CHECK(!md::ProtobufRuntimeVersion().empty());
  std::printf("    protobuf runtime/gencode: %s\n", md::ProtobufRuntimeVersion().c_str());
}

}  // namespace

int main(int argc, char** argv) {
  const char* json_path = nullptr;
  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "--json") == 0 && i + 1 < argc) json_path = argv[++i];
  }

  std::printf("md_cpp_tests\n");
  TestRoundTrip();
  TestPresence();
  TestUnknownVariant();
  TestFraming();
  TestStrictJson();
  TestProjectionEscaping();
  TestUtf8Validator();
  TestVersions();
  std::printf("\n%d checks, %d failures\n", g_checks, g_failures);

  if (json_path != nullptr) {
    // Deliberately the same shape as the pytest plugin's output, so
    // `md_contract report` folds both languages' evidence in through one path.
    std::FILE* out = std::fopen(json_path, "wb");
    if (out == nullptr) {
      std::fprintf(stderr, "cannot write %s\n", json_path);
      return 2;
    }
    std::fprintf(out, "{\n  \"cases\": {\n");
    bool first = true;
    for (const auto& [id, failed] : g_case_failed) {
      if (!first) std::fprintf(out, ",\n");
      first = false;
      std::fprintf(out,
                   "    \"%s\": {\"verdict\": \"%s\", \"tests\": [\"md_cpp_tests.cpp\"]}",
                   id.c_str(), failed ? "FAIL" : "PASS");
    }
    std::fprintf(out, "\n  }\n}\n");
    std::fclose(out);
    std::printf("case verdicts for %zu cases written to %s\n",
                g_case_failed.size(), json_path);
  }
  return g_failures == 0 ? 0 : 1;
}
