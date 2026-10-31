// The strict TOML subset. What it refuses matters as much as what it parses.
#include <gtest/gtest.h>

#include "md_adapter/toml_lite.hpp"

using md::TomlLite;

namespace {

TomlLite parse_ok(const std::string& text) {
  TomlLite t;
  std::vector<std::string> errs;
  EXPECT_TRUE(t.parse(text, errs)) << (errs.empty() ? "" : errs[0]);
  return t;
}

std::vector<std::string> parse_errors(const std::string& text) {
  TomlLite t;
  std::vector<std::string> errs;
  t.parse(text, errs);
  return errs;
}

}  // namespace

TEST(TomlLite, ParsesTablesScalarsAndArrays) {
  TomlLite t = parse_ok(
      "# a comment\n"
      "[a]\n"
      "s = \"hello\"\n"
      "n = 42\n"
      "neg = -7\n"
      "sep = 1_000_000\n"
      "b = true\n"
      "ints = [1, 2, 3]\n"
      "strs = [\"x\", \"y\"]\n");
  std::vector<std::string> e;
  EXPECT_EQ(t.get_string("a.s", e).value(), "hello");
  EXPECT_EQ(t.get_int("a.n", e).value(), 42);
  EXPECT_EQ(t.get_int("a.neg", e).value(), -7);
  EXPECT_EQ(t.get_int("a.sep", e).value(), 1000000);
  EXPECT_TRUE(t.get_bool("a.b", e).value());
  EXPECT_EQ(t.get_int_array("a.ints", e).value().size(), 3u);
  EXPECT_EQ(t.get_string_array("a.strs", e).value()[1], "y");
  EXPECT_TRUE(e.empty());
}

TEST(TomlLite, HashInsideAStringIsNotAComment) {
  TomlLite t = parse_ok("[a]\ns = \"not # a comment\"\n");
  std::vector<std::string> e;
  EXPECT_EQ(t.get_string("a.s", e).value(), "not # a comment");
}

TEST(TomlLite, RejectsFloatsHexAndSpecials) {
  for (const char* v : {"1.5", "0x10", "1e9", "nan", "inf", "-inf", "0b1"}) {
    const auto errs = parse_errors(std::string("[a]\nx = ") + v + "\n");
    EXPECT_FALSE(errs.empty()) << v;
  }
}

TEST(TomlLite, RejectsDuplicateKeys) {
  const auto errs = parse_errors("[a]\nx = 1\nx = 2\n");
  ASSERT_FALSE(errs.empty());
  EXPECT_NE(errs[0].find("duplicate key"), std::string::npos);
}

TEST(TomlLite, RejectsUnsupportedSyntax) {
  EXPECT_FALSE(parse_errors("[[a]]\nx = 1\n").empty());       // array of tables
  EXPECT_FALSE(parse_errors("[a]\n\"q\" = 1\n").empty());     // quoted key
  EXPECT_FALSE(parse_errors("[a]\nx\n").empty());             // no '='
  EXPECT_FALSE(parse_errors("[a\nx = 1\n").empty());          // bad header
  EXPECT_FALSE(parse_errors("[a]\nx = \n").empty());          // empty value
}

TEST(TomlLite, ReportsIntegerOverflow) {
  const auto errs = parse_errors("[a]\nx = 99999999999999999999\n");
  ASSERT_FALSE(errs.empty());
  EXPECT_NE(errs[0].find("out of int64 range"), std::string::npos);
}

TEST(TomlLite, TypeMismatchIsAnError) {
  TomlLite t = parse_ok("[a]\nx = 1\n");
  std::vector<std::string> e;
  EXPECT_FALSE(t.get_string("a.x", e).has_value());
  EXPECT_FALSE(e.empty());
}

TEST(TomlLite, MissingKeyIsNotAnError) {
  TomlLite t = parse_ok("[a]\nx = 1\n");
  std::vector<std::string> e;
  EXPECT_FALSE(t.get_int("a.absent", e).has_value());
  EXPECT_TRUE(e.empty());
}

// Reading a key is what marks it known; anything left over is configuration
// this build does not understand.
TEST(TomlLite, UnconsumedKeysAreReported) {
  TomlLite t = parse_ok("[a]\nknown = 1\nmystery = 2\n");
  std::vector<std::string> e;
  (void)t.get_int("a.known", e);
  const auto left = t.unconsumed_keys();
  ASSERT_EQ(left.size(), 1u);
  EXPECT_EQ(left[0], "a.mystery");
}

TEST(TomlLite, FindsSubTables) {
  TomlLite t = parse_ok(
      "[prices.EURUSD]\nscale = 1\n"
      "[prices.USDJPY]\nscale = 2\n"
      "[prices]\ntop = 3\n");
  const auto subs = t.subtables_of("prices");
  ASSERT_EQ(subs.size(), 2u);
  EXPECT_NE(std::find(subs.begin(), subs.end(), "EURUSD"), subs.end());
  EXPECT_NE(std::find(subs.begin(), subs.end(), "USDJPY"), subs.end());
}

TEST(TomlLite, StringEscapes) {
  TomlLite t = parse_ok("[a]\ns = \"line\\nnext\\ttab\\\\slash\\\"quote\"\n");
  std::vector<std::string> e;
  EXPECT_EQ(t.get_string("a.s", e).value(), "line\nnext\ttab\\slash\"quote");
  EXPECT_FALSE(parse_errors("[a]\ns = \"bad\\qescape\"\n").empty());
}

TEST(TomlLite, EmptyDocumentIsValid) {
  TomlLite t = parse_ok("");
  EXPECT_TRUE(t.unconsumed_keys().empty());
}
