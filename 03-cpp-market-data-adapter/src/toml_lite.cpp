#include "md_adapter/toml_lite.hpp"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdlib>

namespace md {
namespace {

std::string_view trim(std::string_view s) {
  std::size_t b = 0;
  std::size_t e = s.size();
  while (b < e && (s[b] == ' ' || s[b] == '\t' || s[b] == '\r')) ++b;
  while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t' || s[e - 1] == '\r')) --e;
  return s.substr(b, e - b);
}

// Strips an unquoted trailing comment. Quoted strings may contain '#'.
std::string_view strip_comment(std::string_view s) {
  bool in_str = false;
  for (std::size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '"') {
      in_str = !in_str;
    } else if (s[i] == '#' && !in_str) {
      return s.substr(0, i);
    }
  }
  return s;
}

bool is_bare_key_char(char c) {
  return (std::isalnum(static_cast<unsigned char>(c)) != 0) || c == '_' ||
         c == '-' || c == '.';
}

// Basic double-quoted string with the escapes the config actually needs.
bool parse_string(std::string_view in, std::string& out, std::string& err) {
  if (in.size() < 2 || in.front() != '"' || in.back() != '"') {
    err = "not a quoted string";
    return false;
  }
  const std::string_view body = in.substr(1, in.size() - 2);
  out.clear();
  for (std::size_t i = 0; i < body.size(); ++i) {
    if (body[i] != '\\') {
      if (body[i] == '"') {
        err = "unescaped quote inside string";
        return false;
      }
      out += body[i];
      continue;
    }
    if (++i >= body.size()) {
      err = "dangling escape";
      return false;
    }
    switch (body[i]) {
      case '"': out += '"'; break;
      case '\\': out += '\\'; break;
      case 'n': out += '\n'; break;
      case 't': out += '\t'; break;
      default: err = "unsupported escape"; return false;
    }
  }
  return true;
}

// Decimal integers only, optional sign, optional '_' separators. Floats,
// hex/oct/bin literals, inf and nan are rejected on purpose.
bool parse_int(std::string_view in, std::int64_t& out, std::string& err) {
  if (in.empty()) {
    err = "empty integer";
    return false;
  }
  std::string cleaned;
  cleaned.reserve(in.size());
  for (std::size_t i = 0; i < in.size(); ++i) {
    const char c = in[i];
    if (c == '_') {
      if (i == 0 || i + 1 == in.size()) {
        err = "misplaced underscore in integer";
        return false;
      }
      continue;
    }
    if (c == '+' || c == '-') {
      if (i != 0) {
        err = "misplaced sign";
        return false;
      }
      cleaned += c;
      continue;
    }
    if (std::isdigit(static_cast<unsigned char>(c)) == 0) {
      err = "not a decimal integer (floats/inf/nan/hex are rejected)";
      return false;
    }
    cleaned += c;
  }
  if (cleaned.empty() || cleaned == "+" || cleaned == "-") {
    err = "empty integer";
    return false;
  }
  errno = 0;
  char* end = nullptr;
  const long long v = std::strtoll(cleaned.c_str(), &end, 10);
  if (errno == ERANGE || end == nullptr || *end != '\0') {
    err = "integer out of int64 range";
    return false;
  }
  out = static_cast<std::int64_t>(v);
  return true;
}

// Splits a single-line array body on top-level commas.
bool split_array(std::string_view in, std::vector<std::string_view>& out,
                 std::string& err) {
  if (in.size() < 2 || in.front() != '[' || in.back() != ']') {
    err = "malformed array";
    return false;
  }
  const std::string_view body = trim(in.substr(1, in.size() - 2));
  if (body.empty()) return true;
  bool in_str = false;
  std::size_t start = 0;
  for (std::size_t i = 0; i < body.size(); ++i) {
    if (body[i] == '"') {
      in_str = !in_str;
    } else if (body[i] == ',' && !in_str) {
      out.push_back(trim(body.substr(start, i - start)));
      start = i + 1;
    }
  }
  out.push_back(trim(body.substr(start)));
  for (auto s : out) {
    if (s.empty()) {
      err = "empty array element";
      return false;
    }
  }
  return true;
}

}  // namespace

bool TomlLite::parse(std::string_view text, std::vector<std::string>& errors) {
  const std::size_t before = errors.size();
  std::string table;
  std::size_t lineno = 0;

  std::size_t pos = 0;
  while (pos <= text.size()) {
    const std::size_t nl = text.find('\n', pos);
    const std::string_view raw = text.substr(
        pos, nl == std::string_view::npos ? text.size() - pos : nl - pos);
    pos = (nl == std::string_view::npos) ? text.size() + 1 : nl + 1;
    ++lineno;

    const std::string_view line = trim(strip_comment(raw));
    if (line.empty()) continue;

    const std::string loc = "line " + std::to_string(lineno) + ": ";

    if (line.front() == '[') {
      if (line.back() != ']' || line.size() < 3) {
        errors.push_back(loc + "malformed table header");
        continue;
      }
      if (line[1] == '[') {
        errors.push_back(loc + "array-of-tables is not supported");
        continue;
      }
      const std::string_view name = trim(line.substr(1, line.size() - 2));
      bool ok = !name.empty();
      for (char c : name) ok = ok && is_bare_key_char(c);
      if (!ok) {
        errors.push_back(loc + "invalid table name");
        continue;
      }
      table.assign(name);
      continue;
    }

    const std::size_t eq = line.find('=');
    if (eq == std::string_view::npos) {
      errors.push_back(loc + "expected 'key = value'");
      continue;
    }
    const std::string_view key = trim(line.substr(0, eq));
    const std::string_view val = trim(line.substr(eq + 1));
    if (key.empty() || val.empty()) {
      errors.push_back(loc + "empty key or value");
      continue;
    }
    bool key_ok = true;
    for (char c : key) key_ok = key_ok && is_bare_key_char(c);
    if (!key_ok) {
      errors.push_back(loc + "invalid key (quoted keys are not supported)");
      continue;
    }

    const std::string full =
        table.empty() ? std::string(key) : table + "." + std::string(key);
    if (entries_.count(full) != 0) {
      errors.push_back(loc + "duplicate key: " + full);
      continue;
    }

    std::string perr;
    if (val.front() == '"') {
      std::string s;
      if (!parse_string(val, s, perr)) {
        errors.push_back(loc + "key " + full + ": " + perr);
        continue;
      }
      entries_.emplace(full, Value(std::move(s)));
    } else if (val == "true" || val == "false") {
      entries_.emplace(full, Value(val == "true"));
    } else if (val.front() == '[') {
      std::vector<std::string_view> parts;
      if (!split_array(val, parts, perr)) {
        errors.push_back(loc + "key " + full + ": " + perr);
        continue;
      }
      if (!parts.empty() && parts.front().front() == '"') {
        std::vector<std::string> xs;
        bool ok = true;
        for (auto p : parts) {
          std::string s;
          if (!parse_string(p, s, perr)) {
            errors.push_back(loc + "key " + full + ": " + perr);
            ok = false;
            break;
          }
          xs.push_back(std::move(s));
        }
        if (ok) entries_.emplace(full, Value(std::move(xs)));
      } else {
        std::vector<std::int64_t> xs;
        bool ok = true;
        for (auto p : parts) {
          std::int64_t v = 0;
          if (!parse_int(p, v, perr)) {
            errors.push_back(loc + "key " + full + ": " + perr);
            ok = false;
            break;
          }
          xs.push_back(v);
        }
        if (ok) entries_.emplace(full, Value(std::move(xs)));
      }
    } else {
      std::int64_t v = 0;
      if (!parse_int(val, v, perr)) {
        errors.push_back(loc + "key " + full + ": " + perr);
        continue;
      }
      entries_.emplace(full, Value(v));
    }
  }
  return errors.size() == before;
}

bool TomlLite::contains(std::string_view k) const {
  return entries_.find(std::string(k)) != entries_.end();
}

namespace {
template <class T>
std::optional<T> fetch(std::map<std::string, TomlLite::Value>& entries,
                       std::set<std::string>& consumed, std::string_view key,
                       const char* type_name, std::vector<std::string>& errors) {
  const std::string k(key);
  consumed.insert(k);
  const auto it = entries.find(k);
  if (it == entries.end()) return std::nullopt;
  if (const T* v = std::get_if<T>(&it->second)) return *v;
  errors.push_back("key " + k + " is not " + type_name);
  return std::nullopt;
}
}  // namespace

std::optional<std::string> TomlLite::get_string(std::string_view k,
                                                std::vector<std::string>& e) {
  return fetch<std::string>(entries_, consumed_, k, "a string", e);
}
std::optional<std::int64_t> TomlLite::get_int(std::string_view k,
                                              std::vector<std::string>& e) {
  return fetch<std::int64_t>(entries_, consumed_, k, "an integer", e);
}
std::optional<bool> TomlLite::get_bool(std::string_view k,
                                       std::vector<std::string>& e) {
  return fetch<bool>(entries_, consumed_, k, "a boolean", e);
}
std::optional<std::vector<std::int64_t>> TomlLite::get_int_array(
    std::string_view k, std::vector<std::string>& e) {
  return fetch<std::vector<std::int64_t>>(entries_, consumed_, k,
                                          "an integer array", e);
}
std::optional<std::vector<std::string>> TomlLite::get_string_array(
    std::string_view k, std::vector<std::string>& e) {
  return fetch<std::vector<std::string>>(entries_, consumed_, k,
                                         "a string array", e);
}

std::vector<std::string> TomlLite::subtables_of(std::string_view prefix) const {
  std::vector<std::string> out;
  const std::string p = std::string(prefix) + ".";
  for (const auto& kv : entries_) {
    const std::string& k = kv.first;
    if (k.rfind(p, 0) != 0) continue;
    const std::string rest = k.substr(p.size());
    const std::size_t dot = rest.find('.');
    if (dot == std::string::npos) continue;  // direct key, not a sub-table
    std::string name = rest.substr(0, dot);
    if (std::find(out.begin(), out.end(), name) == out.end())
      out.push_back(std::move(name));
  }
  return out;
}

std::vector<std::string> TomlLite::unconsumed_keys() const {
  std::vector<std::string> out;
  for (const auto& kv : entries_)
    if (consumed_.find(kv.first) == consumed_.end()) out.push_back(kv.first);
  return out;
}

}  // namespace md
