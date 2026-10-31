// Canonical projection output, byte-identical to the Python side.
//
// The projection is the cross-language oracle, so "byte-identical" is a hard
// requirement rather than a nicety: the conformance suite compares the two
// languages' output with ==. That forces three details to match Python's
// json.dumps(..., sort_keys=True, separators=(",", ":"), ensure_ascii=False):
// key order, escaping, and the omission of absent fields.
#include "md/contract.hpp"

#include <algorithm>
#include <array>
#include <charconv>

namespace md {

std::string_view ReasonName(Reason reason) {
  switch (reason) {
    case Reason::kOk: return "OK";
    case Reason::kBadMagic: return "BAD_MAGIC";
    case Reason::kTruncatedPrefix: return "TRUNCATED_PREFIX";
    case Reason::kTruncatedHeader: return "TRUNCATED_HEADER";
    case Reason::kInvalidLength: return "INVALID_LENGTH";
    case Reason::kTruncatedBody: return "TRUNCATED_BODY";
    case Reason::kReadDeadlineExceeded: return "READ_DEADLINE_EXCEEDED";
    case Reason::kDecodeError: return "DECODE_ERROR";
    case Reason::kPayloadTooLarge: return "PAYLOAD_TOO_LARGE";
    case Reason::kMissingRequiredSemanticField: return "MISSING_REQUIRED_SEMANTIC_FIELD";
    case Reason::kUnsupportedEventKind: return "UNSUPPORTED_EVENT_KIND";
    case Reason::kStringTooLong: return "STRING_TOO_LONG";
    case Reason::kInvalidUtf8: return "INVALID_UTF8";
    case Reason::kInvalidSymbol: return "INVALID_SYMBOL";
    case Reason::kInvalidDatasetId: return "INVALID_DATASET_ID";
    case Reason::kUnsupportedSourceSchemaVersion: return "UNSUPPORTED_SOURCE_SCHEMA_VERSION";
    case Reason::kJsonLineTooLarge: return "JSON_LINE_TOO_LARGE";
    case Reason::kInvalidJson: return "INVALID_JSON";
    case Reason::kNotAnObject: return "NOT_AN_OBJECT";
    case Reason::kDuplicateJsonKey: return "DUPLICATE_JSON_KEY";
    case Reason::kFloatNotAllowed: return "FLOAT_NOT_ALLOWED";
    case Reason::kNonFiniteNumber: return "NON_FINITE_NUMBER";
    case Reason::kFieldNotInteger: return "FIELD_NOT_INTEGER";
    case Reason::kInt64OutOfRange: return "INT64_OUT_OF_RANGE";
    case Reason::kNumericTokenTooLong: return "NUMERIC_TOKEN_TOO_LONG";
    case Reason::kFieldSetMismatch: return "FIELD_SET_MISMATCH";
    case Reason::kUnknownEventType: return "UNKNOWN_EVENT_TYPE";
    case Reason::kBusinessViolation: return "BUSINESS_VIOLATION";
  }
  return "UNKNOWN_REASON";
}

std::string Error::ToString() const {
  std::string out(ReasonName(reason));
  if (!detail.empty()) {
    out += ": ";
    out += detail;
  }
  if (offset >= 0) {
    out += " @";
    std::array<char, 24> buf{};
    auto res = std::to_chars(buf.data(), buf.data() + buf.size(), offset);
    out.append(buf.data(), res.ptr);
  }
  return out;
}

std::string_view PayloadCaseName(PayloadCase payload_case) {
  switch (payload_case) {
    case PayloadCase::kNone: return "none";
    case PayloadCase::kQuote: return "quote";
    case PayloadCase::kTick: return "tick";
    case PayloadCase::kUnknown: return "unknown";
  }
  return "none";
}

std::string_view StreamEndName(StreamEnd end) {
  switch (end) {
    case StreamEnd::kCompleted: return "COMPLETED";
    case StreamEnd::kIncomplete: return "INCOMPLETE";
    case StreamEnd::kFailed: return "FAILED";
  }
  return "INCOMPLETE";
}

void AppendJsonString(std::string& out, std::string_view value) {
  out += '"';
  for (unsigned char ch : value) {
    switch (ch) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\b': out += "\\b"; break;
      case '\f': out += "\\f"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (ch < 0x20) {
          // Python emits lowercase hex here; \u00xx is the only form it uses
          // for the remaining C0 controls.
          static const char* kHex = "0123456789abcdef";
          out += "\\u00";
          out += kHex[(ch >> 4) & 0xf];
          out += kHex[ch & 0xf];
        } else {
          // Everything else, including every byte of a multi-byte UTF-8
          // sequence, passes through unescaped. That is what
          // ensure_ascii=False does, and note that Python does not escape
          // '/' or 0x7f either.
          out += static_cast<char>(ch);
        }
    }
  }
  out += '"';
}

namespace {

void AppendInt(std::string& out, std::int64_t value) {
  std::array<char, 24> buf{};
  auto res = std::to_chars(buf.data(), buf.data() + buf.size(), value);
  out.append(buf.data(), res.ptr);
}

void AppendKey(std::string& out, std::string_view key, bool& first) {
  if (!first) out += ',';
  first = false;
  AppendJsonString(out, key);
  out += ':';
}

}  // namespace

std::string ProjectionJson(const DomainEvent& event) {
  // The sorted order of the keys that can appear. Hard-coded rather than
  // computed, because it is part of the oracle's definition and a comment can
  // state why this exact order is correct:
  //   ask_ticks < bid_ticks < dataset_id < event_time_ns < payload_case
  //   < present < price_scale < price_ticks < sequence
  //   < source_schema_version < symbol
  // ('price_scale' before 'price_ticks' because 's' < 't'.)
  std::string out;
  out.reserve(320);
  out += '{';
  bool first = true;

  if (event.ask_ticks) { AppendKey(out, "ask_ticks", first); AppendInt(out, *event.ask_ticks); }
  if (event.bid_ticks) { AppendKey(out, "bid_ticks", first); AppendInt(out, *event.bid_ticks); }
  if (event.dataset_id) {
    AppendKey(out, "dataset_id", first);
    AppendJsonString(out, *event.dataset_id);
  }
  if (event.event_time_ns) {
    AppendKey(out, "event_time_ns", first);
    AppendInt(out, *event.event_time_ns);
  }

  AppendKey(out, "payload_case", first);
  AppendJsonString(out, PayloadCaseName(event.payload_case));

  // The `present` array states which fields existed on the wire, which is what
  // makes "absent", "present and zero" and "present and non-zero" three
  // distinct outcomes in the oracle itself (cases B01-B05).
  std::vector<std::string_view> present;
  present.reserve(9);
  if (event.dataset_id) present.push_back("dataset_id");
  if (event.symbol) present.push_back("symbol");
  if (event.sequence) present.push_back("sequence");
  if (event.event_time_ns) present.push_back("event_time_ns");
  if (event.price_scale) present.push_back("price_scale");
  if (event.source_schema_version) present.push_back("source_schema_version");
  if (event.bid_ticks) present.push_back("bid_ticks");
  if (event.ask_ticks) present.push_back("ask_ticks");
  if (event.price_ticks) present.push_back("price_ticks");
  std::sort(present.begin(), present.end());

  AppendKey(out, "present", first);
  out += '[';
  for (std::size_t i = 0; i < present.size(); ++i) {
    if (i != 0) out += ',';
    AppendJsonString(out, present[i]);
  }
  out += ']';

  if (event.price_scale) {
    AppendKey(out, "price_scale", first);
    AppendInt(out, *event.price_scale);
  }
  if (event.price_ticks) {
    AppendKey(out, "price_ticks", first);
    AppendInt(out, *event.price_ticks);
  }
  if (event.sequence) { AppendKey(out, "sequence", first); AppendInt(out, *event.sequence); }
  if (event.source_schema_version) {
    AppendKey(out, "source_schema_version", first);
    AppendJsonString(out, *event.source_schema_version);
  }
  if (event.symbol) { AppendKey(out, "symbol", first); AppendJsonString(out, *event.symbol); }

  out += '}';
  return out;
}

}  // namespace md
