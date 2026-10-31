// Strict sim.v1 reader, written independently of the Python one.
//
// A hand-rolled parser rather than a JSON library, for two reasons. First, the
// contract is a *flat object of strings and integers*, so the grammar is small
// enough to implement exactly. Second, the rules this has to enforce are the
// ones general-purpose parsers deliberately relax: duplicate keys must be
// rejected rather than last-wins, `1.0` and `1e3` must be rejected as integers,
// and an over-long numeric token must be refused before any conversion. Most
// libraries would need to be fought on each of those.
//
// Independence from the Python implementation is the point: the differential
// suite compares the two, and a shared library would hide a shared bug.
#include "md/contract.hpp"

#include <charconv>
#include <cstdlib>
#include <string>
#include <vector>

namespace md {

bool ValidUtf8(std::string_view text) {
  std::size_t i = 0;
  const std::size_t n = text.size();
  while (i < n) {
    const unsigned char c = static_cast<unsigned char>(text[i]);
    std::size_t extra = 0;
    std::uint32_t code = 0;
    if (c < 0x80) { ++i; continue; }
    else if ((c & 0xe0) == 0xc0) { extra = 1; code = c & 0x1fu; }
    else if ((c & 0xf0) == 0xe0) { extra = 2; code = c & 0x0fu; }
    else if ((c & 0xf8) == 0xf0) { extra = 3; code = c & 0x07u; }
    else return false;  // continuation byte or 5/6-byte form
    if (i + extra >= n) return false;
    for (std::size_t k = 1; k <= extra; ++k) {
      const unsigned char cc = static_cast<unsigned char>(text[i + k]);
      if ((cc & 0xc0) != 0x80) return false;
      code = (code << 6) | (cc & 0x3fu);
    }
    // Reject overlong encodings, surrogates and out-of-range code points. A
    // permissive decoder here would let the same symbol have two byte forms,
    // which would break the identity tuple (case C15).
    if (extra == 1 && code < 0x80) return false;
    if (extra == 2 && code < 0x800) return false;
    if (extra == 3 && code < 0x10000) return false;
    if (code > 0x10FFFF) return false;
    if (code >= 0xD800 && code <= 0xDFFF) return false;
    i += extra + 1;
  }
  return true;
}

namespace {

struct Scanner {
  std::string_view text;
  std::size_t pos = 0;

  void SkipWs() {
    while (pos < text.size()) {
      const char c = text[pos];
      if (c == ' ' || c == '\t' || c == '\n' || c == '\r') ++pos;
      else break;
    }
  }
  bool Eof() const { return pos >= text.size(); }
  char Peek() const { return pos < text.size() ? text[pos] : '\0'; }
};

// A parsed JSON value, restricted to what sim.v1 may contain.
enum class ValueKind { kString, kInt };

struct Value {
  ValueKind kind = ValueKind::kInt;
  std::string str;
  std::int64_t num = 0;
};

Error ParseString(Scanner& s, std::string& out) {
  if (s.Peek() != '"') return {Reason::kInvalidJson, "expected string"};
  ++s.pos;
  out.clear();
  while (true) {
    if (s.Eof()) return {Reason::kInvalidJson, "unterminated string"};
    const char c = s.text[s.pos++];
    if (c == '"') return Error::Ok();
    if (c == '\\') {
      if (s.Eof()) return {Reason::kInvalidJson, "unterminated escape"};
      const char e = s.text[s.pos++];
      switch (e) {
        case '"': out += '"'; break;
        case '\\': out += '\\'; break;
        case '/': out += '/'; break;
        case 'b': out += '\b'; break;
        case 'f': out += '\f'; break;
        case 'n': out += '\n'; break;
        case 'r': out += '\r'; break;
        case 't': out += '\t'; break;
        case 'u': {
          if (s.pos + 4 > s.text.size()) {
            return {Reason::kInvalidJson, "short \\u escape"};
          }
          std::uint32_t code = 0;
          for (int k = 0; k < 4; ++k) {
            const char h = s.text[s.pos + k];
            code <<= 4;
            if (h >= '0' && h <= '9') code |= static_cast<std::uint32_t>(h - '0');
            else if (h >= 'a' && h <= 'f') code |= static_cast<std::uint32_t>(h - 'a' + 10);
            else if (h >= 'A' && h <= 'F') code |= static_cast<std::uint32_t>(h - 'A' + 10);
            else return {Reason::kInvalidJson, "bad hex in \\u escape"};
          }
          s.pos += 4;
          // Surrogate halves are refused rather than paired up. Project 01
          // never emits them, and accepting them would mean this parser and the
          // Python one could disagree on the resulting string.
          if (code >= 0xD800 && code <= 0xDFFF) {
            return {Reason::kInvalidJson, "surrogate escape not supported"};
          }
          if (code < 0x80) {
            out += static_cast<char>(code);
          } else if (code < 0x800) {
            out += static_cast<char>(0xC0 | (code >> 6));
            out += static_cast<char>(0x80 | (code & 0x3F));
          } else {
            out += static_cast<char>(0xE0 | (code >> 12));
            out += static_cast<char>(0x80 | ((code >> 6) & 0x3F));
            out += static_cast<char>(0x80 | (code & 0x3F));
          }
          break;
        }
        default:
          return {Reason::kInvalidJson, "unknown escape"};
      }
      continue;
    }
    if (static_cast<unsigned char>(c) < 0x20) {
      return {Reason::kInvalidJson, "raw control character in string"};
    }
    out += c;
  }
}

Error ParseNumberOrLiteral(Scanner& s, Value& out) {
  const std::size_t start = s.pos;
  if (s.Peek() == '-') ++s.pos;
  std::size_t digits = 0;
  while (!s.Eof()) {
    const char c = s.Peek();
    if (c >= '0' && c <= '9') { ++digits; ++s.pos; }
    else break;
  }
  if (digits == 0) {
    // Not a number at all: `true`, `false`, `null`, `NaN`, `Infinity`.
    const std::string_view rest = s.text.substr(start);
    if (rest.starts_with("true") || rest.starts_with("false")) {
      return {Reason::kFieldNotInteger, "boolean where an integer is required"};
    }
    if (rest.starts_with("null")) {
      return {Reason::kFieldNotInteger, "null where an integer is required"};
    }
    if (rest.starts_with("NaN") || rest.starts_with("Infinity") ||
        rest.starts_with("-Infinity")) {
      return {Reason::kNonFiniteNumber, std::string(rest.substr(0, 9))};
    }
    return {Reason::kInvalidJson, "expected a value"};
  }
  if (digits > static_cast<std::size_t>(kMaxNumericDigits)) {
    // Refused before conversion, so a 4000-digit token cannot buy conversion
    // time even though the line byte cap already bounds it (case C07).
    return {Reason::kNumericTokenTooLong,
            std::to_string(digits) + " digits > " + std::to_string(kMaxNumericDigits)};
  }
  // A fraction or exponent means this was never an integer literal. Rejected
  // rather than rounded: `1.0` and `1e3` are contract violations, and rounding
  // them would put a derived value into a price field (cases C03, C04).
  if (!s.Eof() && (s.Peek() == '.' || s.Peek() == 'e' || s.Peek() == 'E')) {
    return {Reason::kFloatNotAllowed, std::string(s.text.substr(start, s.pos - start + 1))};
  }

  const std::string_view token = s.text.substr(start, s.pos - start);
  std::int64_t value = 0;
  const auto res = std::from_chars(token.data(), token.data() + token.size(), value);
  if (res.ec == std::errc::result_out_of_range) {
    return {Reason::kInt64OutOfRange, std::string(token)};
  }
  if (res.ec != std::errc() || res.ptr != token.data() + token.size()) {
    return {Reason::kInvalidJson, "malformed integer"};
  }
  out.kind = ValueKind::kInt;
  out.num = value;
  return Error::Ok();
}

bool ValidSymbol(std::string_view symbol) {
  if (symbol.size() < 3 || symbol.size() > 16) return false;
  for (char c : symbol) {
    const bool ok = (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.';
    if (!ok) return false;
  }
  return true;
}

bool ValidDatasetId(std::string_view id) {
  if (id.empty() || id.size() > kMaxDatasetIdBytes) return false;
  for (char c : id) {
    const unsigned char u = static_cast<unsigned char>(c);
    if (u < 0x20 || u == 0x7f || c == '/' || c == '\\') return false;
  }
  return true;
}

}  // namespace

Error ParseSimV1Line(std::string_view line, DomainEvent& out) {
  // Tolerate the terminator without counting it, matching the Python reader.
  if (line.ends_with("\r\n")) line.remove_suffix(2);
  else if (line.ends_with("\n")) line.remove_suffix(1);

  if (line.size() > kMaxJsonLineBytes) {
    return {Reason::kJsonLineTooLarge,
            std::to_string(line.size()) + " > " + std::to_string(kMaxJsonLineBytes)};
  }
  if (!ValidUtf8(line)) return {Reason::kInvalidUtf8, "line is not valid UTF-8"};

  Scanner s{line, 0};
  s.SkipWs();
  if (s.Peek() != '{') return {Reason::kNotAnObject, "input is not a JSON object"};
  ++s.pos;

  std::vector<std::pair<std::string, Value>> fields;
  fields.reserve(9);

  s.SkipWs();
  if (s.Peek() == '}') { ++s.pos; }
  else {
    while (true) {
      s.SkipWs();
      std::string key;
      if (Error e = ParseString(s, key); !e.ok()) return e;
      for (const auto& [existing, _] : fields) {
        if (existing == key) {
          // Rejected, not last-wins. Two values for one field means the input
          // is ambiguous, and picking either would be a guess (case C01).
          return {Reason::kDuplicateJsonKey, key};
        }
      }
      s.SkipWs();
      if (s.Peek() != ':') return {Reason::kInvalidJson, "expected ':'"};
      ++s.pos;
      s.SkipWs();

      Value value;
      if (s.Peek() == '"') {
        value.kind = ValueKind::kString;
        if (Error e = ParseString(s, value.str); !e.ok()) return e;
      } else if (s.Peek() == '{' || s.Peek() == '[') {
        return {Reason::kFieldSetMismatch, "nested value in a flat contract"};
      } else {
        if (Error e = ParseNumberOrLiteral(s, value); !e.ok()) return e;
      }
      fields.emplace_back(std::move(key), std::move(value));

      s.SkipWs();
      if (s.Peek() == ',') { ++s.pos; continue; }
      if (s.Peek() == '}') { ++s.pos; break; }
      return {Reason::kInvalidJson, "expected ',' or '}'"};
    }
  }
  s.SkipWs();
  if (!s.Eof()) return {Reason::kInvalidJson, "trailing content after object"};

  auto find = [&fields](std::string_view name) -> const Value* {
    for (const auto& [key, value] : fields) {
      if (key == name) return &value;
    }
    return nullptr;
  };
  auto want_int = [&](std::string_view name, std::int64_t& dst) -> Error {
    const Value* v = find(name);
    if (v == nullptr) return {Reason::kFieldSetMismatch, std::string("missing ") + std::string(name)};
    if (v->kind != ValueKind::kInt) {
      return {Reason::kFieldNotInteger, std::string(name)};
    }
    dst = v->num;
    return Error::Ok();
  };
  auto want_str = [&](std::string_view name, std::string& dst) -> Error {
    const Value* v = find(name);
    if (v == nullptr) return {Reason::kFieldSetMismatch, std::string("missing ") + std::string(name)};
    if (v->kind != ValueKind::kString) {
      return {Reason::kFieldNotInteger, std::string(name) + " is not a string"};
    }
    dst = v->str;
    return Error::Ok();
  };

  std::string schema_version;
  if (Error e = want_str("schema_version", schema_version); !e.ok()) return e;
  if (schema_version != kSourceSchemaVersion) {
    return {Reason::kUnsupportedSourceSchemaVersion, schema_version};
  }

  std::string event_type;
  if (Error e = want_str("event_type", event_type); !e.ok()) return e;
  const bool is_quote = event_type == "quote";
  if (!is_quote && event_type != "tick") {
    return {Reason::kUnknownEventType, event_type};
  }

  // Exact field set, per event type: both a missing and an unknown key are
  // rejections, so a quote carrying price_ticks is a contradiction rather than
  // an extra field to ignore (cases C13, C14).
  static const char* kQuoteKeys[] = {"ask_ticks", "bid_ticks", "dataset_id",
                                     "event_time_ns", "event_type", "price_scale",
                                     "schema_version", "sequence", "symbol"};
  static const char* kTickKeys[] = {"dataset_id", "event_time_ns", "event_type",
                                    "price_scale", "price_ticks", "schema_version",
                                    "sequence", "symbol"};
  const char* const* expected = is_quote ? kQuoteKeys : kTickKeys;
  const std::size_t expected_n = is_quote ? 9 : 8;
  if (fields.size() != expected_n) {
    return {Reason::kFieldSetMismatch,
            std::to_string(fields.size()) + " fields, expected " + std::to_string(expected_n)};
  }
  for (std::size_t i = 0; i < expected_n; ++i) {
    if (find(expected[i]) == nullptr) {
      return {Reason::kFieldSetMismatch, std::string("missing ") + expected[i]};
    }
  }

  std::string symbol;
  if (Error e = want_str("symbol", symbol); !e.ok()) return e;
  if (symbol.size() > kMaxSymbolBytes) return {Reason::kStringTooLong, "symbol"};
  if (!ValidSymbol(symbol)) return {Reason::kInvalidSymbol, symbol};

  std::string dataset_id;
  if (Error e = want_str("dataset_id", dataset_id); !e.ok()) return e;
  if (!ValidDatasetId(dataset_id)) return {Reason::kInvalidDatasetId, dataset_id};

  std::int64_t sequence = 0, event_time_ns = 0, price_scale = 0;
  if (Error e = want_int("sequence", sequence); !e.ok()) return e;
  if (Error e = want_int("event_time_ns", event_time_ns); !e.ok()) return e;
  if (Error e = want_int("price_scale", price_scale); !e.ok()) return e;

  out = DomainEvent{};
  out.dataset_id = dataset_id;
  out.symbol = symbol;
  out.sequence = sequence;
  out.event_time_ns = event_time_ns;
  out.price_scale = price_scale;
  out.source_schema_version = std::string(kSourceSchemaVersion);

  if (is_quote) {
    std::int64_t bid = 0, ask = 0;
    if (Error e = want_int("bid_ticks", bid); !e.ok()) return e;
    if (Error e = want_int("ask_ticks", ask); !e.ok()) return e;
    out.payload_case = PayloadCase::kQuote;
    out.bid_ticks = bid;
    out.ask_ticks = ask;
  } else {
    std::int64_t price = 0;
    if (Error e = want_int("price_ticks", price); !e.ok()) return e;
    out.payload_case = PayloadCase::kTick;
    out.price_ticks = price;
  }
  return Error::Ok();
}

}  // namespace md
