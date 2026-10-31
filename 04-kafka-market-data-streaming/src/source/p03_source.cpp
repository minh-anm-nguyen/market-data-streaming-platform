#include "source/p03_source.h"

#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

#include "serialization/marketdata_codec.h"

namespace p04 {
namespace {

// A reader for exactly the shape P04StubSink writes, and nothing else.
//
// Not a general JSON parser, on purpose. The handoff stream is machine-written
// with a known value grammar; a permissive parser would accept a malformed
// record and hand the publisher a half-built event, which is the failure mode
// this project is organised against. Every helper below returns false rather
// than a default on anything unexpected.

bool SkipWhitespace(std::string_view text, std::size_t* pos) {
  while (*pos < text.size()) {
    const char c = text[*pos];
    if (c != ' ' && c != '\t' && c != '\r' && c != '\n') return true;
    ++*pos;
  }
  return false;
}

bool Expect(std::string_view text, std::size_t* pos, char expected) {
  if (!SkipWhitespace(text, pos)) return false;
  if (text[*pos] != expected) return false;
  ++*pos;
  return true;
}

// Reads a JSON string, supporting the escapes P03's json_escape emits.
bool ReadString(std::string_view text, std::size_t* pos, std::string* out) {
  if (!Expect(text, pos, '"')) return false;
  out->clear();
  while (*pos < text.size()) {
    const char c = text[*pos];
    if (c == '"') {
      ++*pos;
      return true;
    }
    if (c != '\\') {
      out->push_back(c);
      ++*pos;
      continue;
    }
    if (*pos + 1 >= text.size()) return false;
    const char esc = text[*pos + 1];
    switch (esc) {
      case '"':  out->push_back('"');  break;
      case '\\': out->push_back('\\'); break;
      case '/':  out->push_back('/');  break;
      case 'b':  out->push_back('\b'); break;
      case 'f':  out->push_back('\f'); break;
      case 'n':  out->push_back('\n'); break;
      case 'r':  out->push_back('\r'); break;
      case 't':  out->push_back('\t'); break;
      case 'u': {
        // P03 escapes control characters as \u00XX and nothing else.
        if (*pos + 5 >= text.size()) return false;
        const char hex[5] = {text[*pos + 2], text[*pos + 3], text[*pos + 4],
                             text[*pos + 5], '\0'};
        char* end = nullptr;
        const long value = std::strtol(hex, &end, 16);
        if (end != hex + 4 || value < 0 || value > 0x7f) return false;
        out->push_back(static_cast<char>(value));
        *pos += 4;
        break;
      }
      default:
        return false;
    }
    *pos += 2;
  }
  return false;
}

bool ReadUnsigned(std::string_view text, std::size_t* pos, std::uint64_t* out) {
  if (!SkipWhitespace(text, pos)) return false;
  const std::size_t start = *pos;
  while (*pos < text.size() && text[*pos] >= '0' && text[*pos] <= '9') ++*pos;
  if (*pos == start) return false;
  const std::string digits(text.substr(start, *pos - start));
  errno = 0;
  char* end = nullptr;
  const unsigned long long value = std::strtoull(digits.c_str(), &end, 10);
  if (errno != 0 || end != digits.c_str() + digits.size()) return false;
  *out = static_cast<std::uint64_t>(value);
  return true;
}

bool ReadStringArray(std::string_view text, std::size_t* pos,
                     std::vector<std::string>* out) {
  if (!Expect(text, pos, '[')) return false;
  out->clear();
  if (!SkipWhitespace(text, pos)) return false;
  if (text[*pos] == ']') {
    ++*pos;
    return true;
  }
  for (;;) {
    std::string item;
    if (!ReadString(text, pos, &item)) return false;
    out->push_back(item);
    if (!SkipWhitespace(text, pos)) return false;
    if (text[*pos] == ',') {
      ++*pos;
      continue;
    }
    if (text[*pos] == ']') {
      ++*pos;
      return true;
    }
    return false;
  }
}

// Skips one JSON value of any shape. Used for quality.event_id: P03's copy of
// the core fields is informational, because the authoritative values are the
// ones decoded from the wire bytes.
bool SkipValue(std::string_view text, std::size_t* pos) {
  if (!SkipWhitespace(text, pos)) return false;
  const char c = text[*pos];
  if (c == '"') {
    std::string ignored;
    return ReadString(text, pos, &ignored);
  }
  if (c == '{' || c == '[') {
    const char open = c;
    const char close = (c == '{') ? '}' : ']';
    int depth = 0;
    bool in_string = false;
    while (*pos < text.size()) {
      const char ch = text[*pos];
      if (in_string) {
        if (ch == '\\') {
          *pos += 2;
          continue;
        }
        if (ch == '"') in_string = false;
        ++*pos;
        continue;
      }
      if (ch == '"') {
        in_string = true;
        ++*pos;
        continue;
      }
      if (ch == open) ++depth;
      if (ch == close) {
        --depth;
        if (depth == 0) {
          ++*pos;
          return true;
        }
      }
      ++*pos;
    }
    return false;
  }
  // A bare literal: number, true, false or null.
  const std::size_t start = *pos;
  while (*pos < text.size()) {
    const char ch = text[*pos];
    if (ch == ',' || ch == '}' || ch == ']') break;
    ++*pos;
  }
  return *pos > start;
}

bool HexNibble(char c, unsigned char* out) {
  if (c >= '0' && c <= '9') {
    *out = static_cast<unsigned char>(c - '0');
    return true;
  }
  if (c >= 'a' && c <= 'f') {
    *out = static_cast<unsigned char>(c - 'a' + 10);
    return true;
  }
  if (c >= 'A' && c <= 'F') {
    *out = static_cast<unsigned char>(c - 'A' + 10);
    return true;
  }
  return false;
}

bool HexDecode(const std::string& hex, std::string* out) {
  if (hex.size() % 2 != 0) return false;
  out->clear();
  out->reserve(hex.size() / 2);
  for (std::size_t i = 0; i < hex.size(); i += 2) {
    unsigned char hi = 0;
    unsigned char lo = 0;
    if (!HexNibble(hex[i], &hi) || !HexNibble(hex[i + 1], &lo)) return false;
    out->push_back(static_cast<char>((hi << 4) | lo));
  }
  return true;
}

bool Contains(const std::vector<std::string>& flags, const char* needle) {
  for (const std::string& flag : flags) {
    if (flag == needle) return true;
  }
  return false;
}

struct Record {
  std::string key;
  std::string value_hex;
  std::uint64_t value_bytes = 0;
  std::vector<std::string> flags;
  std::string feed_state;
  std::string primary_reason;
  std::uint64_t arrival_index = 0;
};

bool ParseQuality(std::string_view line, std::size_t* pos, Record* out,
                  std::string* why) {
  if (!Expect(line, pos, '{')) {
    *why = "quality is not an object";
    return false;
  }
  for (;;) {
    std::string sub;
    if (!ReadString(line, pos, &sub)) {
      *why = "expected a field name inside quality";
      return false;
    }
    if (!Expect(line, pos, ':')) {
      *why = "expected ':' after quality." + sub;
      return false;
    }
    if (sub == "flags") {
      if (!ReadStringArray(line, pos, &out->flags)) {
        *why = "quality.flags is not an array of strings";
        return false;
      }
    } else if (sub == "feed_state") {
      if (!ReadString(line, pos, &out->feed_state)) {
        *why = "quality.feed_state is not a string";
        return false;
      }
    } else if (sub == "primary_reason") {
      if (!ReadString(line, pos, &out->primary_reason)) {
        *why = "quality.primary_reason is not a string";
        return false;
      }
    } else if (sub == "arrival_index") {
      if (!ReadUnsigned(line, pos, &out->arrival_index)) {
        *why = "quality.arrival_index is not an unsigned integer";
        return false;
      }
    } else if (!SkipValue(line, pos)) {
      *why = "unreadable value for quality." + sub;
      return false;
    }
    if (!SkipWhitespace(line, pos)) {
      *why = "truncated inside quality";
      return false;
    }
    if (line[*pos] == ',') {
      ++*pos;
      continue;
    }
    if (line[*pos] == '}') {
      ++*pos;
      return true;
    }
    *why = "expected ',' or '}' inside quality";
    return false;
  }
}

// Fields are read by name rather than by position, so a field added in the
// middle later does not silently shift every value after it.
bool ParseRecord(std::string_view line, Record* out, std::string* why) {
  std::size_t pos = 0;
  if (!Expect(line, &pos, '{')) {
    *why = "record does not start with '{'";
    return false;
  }
  bool seen_key = false;
  bool seen_value = false;
  bool seen_quality = false;
  for (;;) {
    std::string field;
    if (!ReadString(line, &pos, &field)) {
      *why = "expected a field name";
      return false;
    }
    if (!Expect(line, &pos, ':')) {
      *why = "expected ':' after field " + field;
      return false;
    }
    if (field == "key") {
      if (!ReadString(line, &pos, &out->key)) {
        *why = "key is not a string";
        return false;
      }
      seen_key = true;
    } else if (field == "value_hex") {
      if (!ReadString(line, &pos, &out->value_hex)) {
        *why = "value_hex is not a string";
        return false;
      }
      seen_value = true;
    } else if (field == "value_bytes") {
      if (!ReadUnsigned(line, &pos, &out->value_bytes)) {
        *why = "value_bytes is not an unsigned integer";
        return false;
      }
    } else if (field == "quality") {
      if (!ParseQuality(line, &pos, out, why)) return false;
      seen_quality = true;
    } else if (!SkipValue(line, &pos)) {
      *why = "unreadable value for field " + field;
      return false;
    }

    if (!SkipWhitespace(line, &pos)) {
      *why = "truncated record";
      return false;
    }
    if (line[pos] == ',') {
      ++pos;
      continue;
    }
    if (line[pos] == '}') {
      ++pos;
      break;
    }
    *why = "expected ',' or '}' after field " + field;
    return false;
  }
  if (!seen_key || !seen_value || !seen_quality) {
    *why = "record is missing key, value_hex or quality";
    return false;
  }
  return true;
}

}  // namespace

FeedState FeedStateFromP03(const std::string& feed_state) {
  // P03's six states, exactly as 03-*/src/status.cpp spells them, plus the
  // "INVALID" it returns for an out-of-range enum. Listing only the values that
  // really exist matters: a mapping that also accepted invented names would
  // silently absorb a renamed state instead of reporting it.
  //
  // The collapse into P04's narrower set is deliberate. DEGRADED is live data
  // whose series is not continuous, so it stays kLive and the discontinuity
  // travels in quality_flags; calling it kStale would say the data is old,
  // which it is not.
  if (feed_state == "HEALTHY" || feed_state == "DEGRADED") {
    return FeedState::kLive;
  }
  // START_UNVERIFIED is the state before the first sequence can be checked
  // against anything, so it is a warm-up rather than a live claim.
  if (feed_state == "INITIAL" || feed_state == "START_UNVERIFIED" ||
      feed_state == "RECOVERING") {
    return FeedState::kWarmup;
  }
  if (feed_state == "INACTIVE") {
    return FeedState::kStale;
  }
  return FeedState::kUnknown;
}

bool ClassifyFromP03Reason(const std::string& primary_reason,
                           const std::vector<std::string>& flags,
                           Classification* classification,
                           std::uint32_t* quality_flags) {
  *quality_flags = kQualityNone;
  if (Contains(flags, "GAP") || Contains(flags, "SEQUENCE_GAP")) {
    *quality_flags |= kQualitySequenceGapBefore;
  }
  if (Contains(flags, "NON_MONOTONIC") || Contains(flags, "REORDER")) {
    *quality_flags |= kQualityTimestampNonMonotonic;
  }
  if (Contains(flags, "FAULT") || Contains(flags, "FAULT_INJECTED")) {
    *quality_flags |= kQualityFaultInjected;
  }
  if (Contains(flags, "REPLAYED")) {
    *quality_flags |= kQualityReplayed;
  }

  // `Reason::kNone` prints as lowercase "none" in 03-*/src/status.cpp, alone
  // among the reasons. The uppercase spellings are accepted too so a future
  // normalisation there does not break this reader; nothing else is guessed.
  if (primary_reason.empty() || primary_reason == "none" ||
      primary_reason == "NONE" || primary_reason == "HEALTHY") {
    *classification = Classification::kMarketValid;
    return true;
  }

  // A discontinuity is a property of the series, not a defect in this event.
  // P03 forwards these deliberately rather than dropping the feed (its ADR-07),
  // so they stay publishable and carry the flag.
  if (primary_reason == "SEQUENCE_GAP" || primary_reason == "HUGE_GAP" ||
      primary_reason == "INITIAL_GAP" || primary_reason == "CONTINUITY_LOST" ||
      primary_reason == "SEQUENCE_ROLLBACK" || primary_reason == "DEGRADED" ||
      primary_reason == "RECOVERED" || primary_reason == "RECOVERING") {
    *classification = Classification::kMarketValid;
    *quality_flags |= kQualitySequenceGapBefore;
    return true;
  }

  if (primary_reason == "FAULT_TEST_ONLY" ||
      primary_reason == "UNEXPECTED_FAULT") {
    *classification = Classification::kFaultTestOnly;
    *quality_flags |= kQualityFaultInjected;
    return true;
  }

  if (primary_reason == "DUPLICATE_EXACT" ||
      primary_reason == "DUPLICATE_CONFLICT") {
    *classification = Classification::kDuplicateSuppressed;
    return true;
  }

  if (primary_reason == "EVENT_TIME_OLD" || primary_reason == "REORDER_LATE" ||
      primary_reason == "TOO_OLD_UNVERIFIABLE" ||
      primary_reason == "QUOTE_INACTIVE" || primary_reason == "FEED_INACTIVE" ||
      primary_reason == "INACTIVE") {
    *classification = Classification::kStaleSuppressed;
    *quality_flags |= kQualityTimestampNonMonotonic;
    return true;
  }

  if (primary_reason == "QUOTE_CROSSED" ||
      primary_reason == "QUOTE_BID_NON_POSITIVE" ||
      primary_reason == "TICK_NON_POSITIVE" ||
      primary_reason == "PRICE_OUT_OF_RANGE" ||
      primary_reason == "SPREAD_OUT_OF_RANGE" ||
      primary_reason == "SCALE_MISMATCH" ||
      primary_reason == "INVALID_PRICE_SCALE") {
    *classification = Classification::kBusinessReject;
    return true;
  }

  if (primary_reason == "BAD_MAGIC" || primary_reason == "INVALID_LENGTH" ||
      primary_reason == "TRUNCATED_PREFIX" ||
      primary_reason == "TRUNCATED_HEADER" ||
      primary_reason == "TRUNCATED_BODY" ||
      primary_reason == "PROTO_DECODE_ERROR" ||
      primary_reason == "UNKNOWN_FIELDS" ||
      primary_reason == "UNKNOWN_FIELD_PRESENT" ||
      primary_reason == "MISSING_DATASET_ID" ||
      primary_reason == "MISSING_SYMBOL" || primary_reason == "MISSING_SEQUENCE" ||
      primary_reason == "MISSING_EVENT_TIME" ||
      primary_reason == "MISSING_PRICE_SCALE" ||
      primary_reason == "MISSING_SOURCE_VERSION" ||
      primary_reason == "MISSING_PAYLOAD_ONEOF" ||
      primary_reason == "MISSING_QUOTE_FIELD" ||
      primary_reason == "MISSING_TICK_FIELD" ||
      primary_reason == "INVALID_SEQUENCE" ||
      primary_reason == "NEGATIVE_EVENT_TIME" ||
      primary_reason == "ARITHMETIC_OVERFLOW" ||
      primary_reason == "FIELD_TOO_LONG" || primary_reason == "SYMBOL_CHARSET" ||
      primary_reason == "SYMBOL_NOT_ALLOWED" ||
      primary_reason == "UNSUPPORTED_SCHEMA_VERSION" ||
      primary_reason == "DESCRIPTOR_MISMATCH") {
    *classification = Classification::kStructuralInvalid;
    return true;
  }

  // Unknown reason. kUnclassified is not publishable, so the run stops rather
  // than guessing an unfamiliar reason was benign.
  *classification = Classification::kUnclassified;
  return false;
}

bool ReadP03Handoff(const std::string& path, const P03HandoffConfig& config,
                    const std::function<bool(const OutputEvent&)>& on_event,
                    P03HandoffStats* stats,
                    std::vector<SourceParseError>* errors, std::string* error) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    *error = "cannot open P03 handoff stream: " + path;
    return false;
  }

  std::string line;
  std::uint64_t line_number = 0;
  while (std::getline(in, line)) {
    ++line_number;
    if (!line.empty() && line.back() == '\r') line.pop_back();
    if (line.empty()) continue;
    ++stats->records_read;

    Record record;
    std::string why;
    if (!ParseRecord(line, &record, &why)) {
      errors->push_back({line_number, "P03_RECORD_MALFORMED", why});
      *error = "malformed P03 handoff record at line " + std::to_string(line_number);
      return false;
    }

    std::string raw;
    if (!HexDecode(record.value_hex, &raw)) {
      errors->push_back({line_number, "P03_VALUE_HEX_INVALID",
                         "value_hex is not an even-length hex string"});
      *error = "invalid value_hex at line " + std::to_string(line_number);
      return false;
    }
    if (record.value_bytes != raw.size()) {
      errors->push_back(
          {line_number, "P03_VALUE_LENGTH_MISMATCH",
           "value_bytes=" + std::to_string(record.value_bytes) +
               " but value_hex decodes to " + std::to_string(raw.size())});
      *error = "value length mismatch at line " + std::to_string(line_number);
      return false;
    }

    // A Kafka record value carries its own boundary. A framing prefix here is
    // the upstream packaging bug of case L07, and stripping it would hide it.
    if (LooksLikeFramedPayload(raw)) {
      ++stats->framing_violations;
      errors->push_back({line_number, "P03_FRAMED_PAYLOAD",
                         "value carries MDP1 magic or a length prefix; a Kafka "
                         "value must be a bare MarketDataEvent"});
      *error = "framed payload at line " + std::to_string(line_number);
      return false;
    }

    TypedEvent event;
    CodecError codec_error = {};
    if (!Decode(raw, &event, &codec_error)) {
      errors->push_back({line_number, "P03_PAYLOAD_DECODE_FAILED",
                         std::string("codec: ") + ToString(codec_error)});
      *error = "payload decode failed at line " + std::to_string(line_number);
      return false;
    }

    // P04 serializes the value it publishes, so prove the round trip is exact
    // rather than assuming P03's bytes survive it.
    std::string reencoded;
    CodecError encode_error = {};
    if (!Encode(event, &reencoded, &encode_error) || reencoded != raw) {
      ++stats->reencode_mismatches;
      errors->push_back({line_number, "P03_REENCODE_MISMATCH",
                         "re-encoding the decoded event does not reproduce P03's "
                         "bytes, so publishing would change the payload"});
      *error = "re-encode mismatch at line " + std::to_string(line_number);
      return false;
    }

    if (config.require_key_matches_symbol) {
      const std::string symbol = event.symbol.value_or(std::string());
      if (symbol != record.key) {
        ++stats->key_symbol_mismatches;
        errors->push_back({line_number, "P03_KEY_SYMBOL_MISMATCH",
                           "record.key=" + record.key + " but payload symbol=" +
                               symbol});
        *error = "key/symbol mismatch at line " + std::to_string(line_number);
        return false;
      }
    }

    if (!config.expected_dataset_id.empty()) {
      const std::string dataset = event.dataset_id.value_or(std::string());
      if (dataset != config.expected_dataset_id) {
        errors->push_back({line_number, "P03_DATASET_MISMATCH",
                           "dataset_id=" + dataset + " expected " +
                               config.expected_dataset_id});
        *error = "dataset mismatch at line " + std::to_string(line_number);
        return false;
      }
    }

    if (!config.expected_schema_version.empty()) {
      const std::string version =
          event.source_schema_version.value_or(std::string());
      if (version != config.expected_schema_version) {
        errors->push_back({line_number, "P03_SCHEMA_VERSION_MISMATCH",
                           "source_schema_version=" + version + " expected " +
                               config.expected_schema_version});
        *error = "schema version mismatch at line " + std::to_string(line_number);
        return false;
      }
    }

    OutputEvent output;
    output.event = event;
    output.feed_state = FeedStateFromP03(record.feed_state);
    if (!ClassifyFromP03Reason(record.primary_reason, record.flags,
                               &output.classification, &output.quality_flags)) {
      errors->push_back({line_number, "P03_REASON_UNRECOGNISED",
                         "primary_reason=" + record.primary_reason +
                             " has no mapping; refusing to publish it as valid"});
      *error = "unrecognised P03 reason at line " + std::to_string(line_number);
      return false;
    }

    if (output.classification == Classification::kMarketValid) {
      if (output.quality_flags == kQualityNone) {
        ++stats->market_valid;
      } else {
        ++stats->degraded_forwarded;
      }
    } else if (output.classification == Classification::kFaultTestOnly) {
      ++stats->fault_test_only;
    } else {
      ++stats->other_classified;
    }

    output.provenance.source_dataset_id = event.dataset_id.value_or(std::string());
    output.provenance.source_schema_version =
        event.source_schema_version.value_or(std::string());
    output.provenance.run_id = config.run_id;
    output.provenance.descriptor_sha256 = config.descriptor_sha256;
    output.provenance.arrival_index = record.arrival_index;
    // The raw P03 record, kept for the evidence bundle. It is never published.
    output.raw_source_payload = line;

    ++stats->offered;
    if (!on_event(output)) return true;
  }

  if (in.bad()) {
    *error = "I/O error reading P03 handoff stream after line " +
             std::to_string(line_number);
    return false;
  }
  return true;
}

}  // namespace p04
