// A deliberately small, strict TOML subset.
//
// Rationale: the adapter must reject unknown keys, floats, NaN and negative
// budgets (design section 14). A general-purpose TOML library accepts far more
// than the config contract allows, so the surface is kept to exactly what the
// contract uses: tables, integers, booleans, quoted strings, and homogeneous
// arrays of integers or strings. Anything else is a config error, not a
// best-effort coercion.
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace md {

class TomlLite {
 public:
  using Value = std::variant<std::string, std::int64_t, bool,
                             std::vector<std::int64_t>, std::vector<std::string>>;

  // Parses `text`. Appends human-readable problems to `errors` and returns
  // false when anything in the document failed to parse.
  bool parse(std::string_view text, std::vector<std::string>& errors);

  bool contains(std::string_view dotted_key) const;

  // Typed accessors. Each marks the key as consumed so unconsumed_keys() can
  // report unknown configuration. A type mismatch appends an error.
  std::optional<std::string> get_string(std::string_view key,
                                        std::vector<std::string>& errors);
  std::optional<std::int64_t> get_int(std::string_view key,
                                      std::vector<std::string>& errors);
  std::optional<bool> get_bool(std::string_view key,
                               std::vector<std::string>& errors);
  std::optional<std::vector<std::int64_t>> get_int_array(
      std::string_view key, std::vector<std::string>& errors);
  std::optional<std::vector<std::string>> get_string_array(
      std::string_view key, std::vector<std::string>& errors);

  // Names of the direct sub-tables of `prefix` (e.g. "prices" -> EURUSD...).
  std::vector<std::string> subtables_of(std::string_view prefix) const;

  // Every key never touched by a typed accessor. Non-empty means the config
  // contains something this build does not understand.
  std::vector<std::string> unconsumed_keys() const;

  const std::map<std::string, Value>& entries() const { return entries_; }

 private:
  std::map<std::string, Value> entries_;
  std::set<std::string> consumed_;
};

}  // namespace md
