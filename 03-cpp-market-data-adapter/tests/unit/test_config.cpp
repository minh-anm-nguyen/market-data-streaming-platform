// Configuration validation. The negative cases are the point: a
// configuration that cannot be honoured must stop the process at startup
// rather than fail halfway through a run.
#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "md_adapter/config.hpp"
#include "test_support.hpp"

using namespace mdtest;
using md::Config;

namespace {

struct Override {
  std::string section;
  std::string key;
  std::string value;
};

// Rewrites keys in place inside the baseline document. Appending a second
// [section] carrying the same key would trip the parser's duplicate-key rule,
// which is itself under test, so overrides must replace rather than append.
std::string toml_with(const std::vector<Override>& overrides) {
  std::string toml = base_config_toml();
  for (const auto& o : overrides) {
    const std::string header = "[" + o.section + "]";
    const std::size_t sec = toml.find(header);
    if (sec == std::string::npos) continue;
    const std::size_t body = sec + header.size();
    const std::size_t next_sec = toml.find("\n[", body);
    const std::size_t limit =
        (next_sec == std::string::npos) ? toml.size() : next_sec;
    const std::size_t at = toml.find("\n" + o.key + " = ", body);
    if (at != std::string::npos && at < limit) {
      const std::size_t eol = toml.find('\n', at + 1);
      toml.replace(at + 1, eol - at - 1, o.key + " = " + o.value);
    } else {
      toml.insert(body + 1, o.key + " = " + o.value + "\n");
    }
  }
  return toml;
}

std::vector<std::string> errors_for(const std::vector<Override>& overrides) {
  Config c;
  std::vector<std::string> errs;
  Config::load_from_string(toml_with(overrides), "<test>", c, errs);
  return errs;
}

std::vector<std::string> errors_with(const std::string& section,
                                     const std::string& key,
                                     const std::string& value) {
  return errors_for({{section, key, value}});
}

bool mentions(const std::vector<std::string>& errs, const std::string& frag) {
  return std::any_of(errs.begin(), errs.end(), [&](const std::string& e) {
    return e.find(frag) != std::string::npos;
  });
}

}  // namespace

TEST(Config, BaselineIsValid) {
  Config c;
  std::vector<std::string> errs;
  EXPECT_TRUE(Config::load_from_string(base_config_toml(), "<test>", c, errs))
      << (errs.empty() ? std::string() : errs[0]);
  EXPECT_EQ(c.identity.allowed_symbols.size(), 3u);
  EXPECT_EQ(c.prices.size(), 3u);
  EXPECT_FALSE(c.source_sha256.empty());
}

TEST(Config, TheOverrideHelperItselfProducesAValidDocument) {
  // Guards every negative test below: if the helper corrupted the document,
  // the tests would "pass" for the wrong reason.
  EXPECT_TRUE(errors_for({{"input", "read_chunk_bytes", "4096"}}).empty());
}

TEST(Config, UnknownKeysAreRejected) {
  const auto errs = errors_with("identity", "typo_threshold", "5");
  EXPECT_TRUE(
      mentions(errs, "unknown configuration key: identity.typo_threshold"));
}

TEST(Config, FloatsAndNaNAreRejected) {
  EXPECT_TRUE(mentions(errors_with("freshness", "timer_tick_ms", "10.5"),
                       "not a decimal integer"));
  EXPECT_TRUE(mentions(errors_with("freshness", "timer_tick_ms", "nan"),
                       "not a decimal integer"));
  EXPECT_TRUE(mentions(errors_with("freshness", "timer_tick_ms", "inf"),
                       "not a decimal integer"));
  EXPECT_TRUE(mentions(errors_with("freshness", "timer_tick_ms", "0x64"),
                       "not a decimal integer"));
}

TEST(Config, NegativeAndZeroBudgetsAreRejected) {
  EXPECT_TRUE(
      mentions(errors_with("input", "max_payload_bytes", "0"), "must be > 0"));
  EXPECT_TRUE(
      mentions(errors_with("validation", "sequence_window", "-1"), "must be > 0"));
  EXPECT_TRUE(
      mentions(errors_with("identity", "max_state_bytes", "0"), "must be > 0"));
}

// A zero partial-frame deadline would let a slowloris sender hold a
// connection open indefinitely.
TEST(Config, PartialFrameDeadlinesMustBePositive) {
  EXPECT_TRUE(mentions(errors_with("input", "body_deadline_ms", "0"),
                       "input.body_deadline_ms must be > 0"));
  EXPECT_TRUE(mentions(errors_with("input", "prefix_deadline_ms", "0"),
                       "input.prefix_deadline_ms must be > 0"));
  EXPECT_TRUE(mentions(errors_with("input", "header_partial_deadline_ms", "0"),
                       "input.header_partial_deadline_ms must be > 0"));
}

TEST(Config, PayloadCapCannotExceedTheProtocolLimit) {
  EXPECT_TRUE(mentions(errors_with("input", "max_payload_bytes", "4097"),
                       "exceeds the MDP1 cap"));
}

// A-02: the Project 01 JSONL port is a configuration error, not something to
// sniff at runtime.
TEST(Config, Port9009IsRejectedForTcpInput) {
  const auto errs =
      errors_for({{"input", "mode", "\"tcp\""},
                  {"input", "port", "9009"},
                  {"freshness", "clock_mode", "\"monotonic_live\""}});
  EXPECT_TRUE(mentions(errs, "9009"));
}

TEST(Config, PortRangeIsChecked) {
  const auto errs =
      errors_for({{"input", "mode", "\"tcp\""},
                  {"input", "port", "70000"},
                  {"freshness", "clock_mode", "\"monotonic_live\""}});
  EXPECT_TRUE(mentions(errs, "outside 1..65535"));
}

// A-06 / M-group: a non-loopback endpoint needs an explicit opt-in.
TEST(Config, NonLoopbackHostNeedsAnExplicitOptIn) {
  const std::vector<Override> live = {
      {"input", "mode", "\"tcp\""},
      {"input", "host", "\"203.0.113.9\""},
      {"freshness", "clock_mode", "\"monotonic_live\""}};
  EXPECT_TRUE(mentions(errors_for(live), "not loopback"));

  std::vector<Override> opted_in = live;
  opted_in.push_back({"input", "allow_non_loopback", "true"});
  EXPECT_FALSE(mentions(errors_for(opted_in), "not loopback"));
}

TEST(Config, VirtualClockIsRejectedForLiveInput) {
  // The baseline uses clock_mode = virtual_for_replay.
  EXPECT_TRUE(mentions(errors_with("input", "mode", "\"tcp\""),
                       "virtual_for_replay is offline-only"));
}

TEST(Config, WatermarkHysteresisMustBeOrdered) {
  EXPECT_TRUE(
      mentions(errors_with("output", "resume_read_low_watermark_pct", "90"),
               "strictly below the high watermark"));
  EXPECT_TRUE(
      mentions(errors_with("output", "resume_read_low_watermark_pct", "80"),
               "strictly below the high watermark"));
}

TEST(Config, QueueMustBeAbleToHoldOneMaximumFrame) {
  EXPECT_TRUE(mentions(errors_with("output", "max_queue_bytes", "100"),
                       "cannot hold a single maximum-size frame"));
}

TEST(Config, TimerTickCannotBeCoarserThanTheInactivityThreshold) {
  EXPECT_TRUE(mentions(errors_with("freshness", "timer_tick_ms", "9000"),
                       "coarser than the inactivity threshold"));
}

TEST(Config, FaultObserveRequiresAManifest) {
  EXPECT_TRUE(mentions(errors_with("validation", "mode", "\"fault_observe\""),
                       "requires validation.fault_manifest"));
}

TEST(Config, FaultObserveMayNotFeedTheProductionLikeSink) {
  const auto errs =
      errors_for({{"validation", "mode", "\"fault_observe\""},
                  {"validation", "fault_manifest", "\"plan.json\""},
                  {"output", "sink", "\"p04_stub\""}});
  EXPECT_TRUE(mentions(errs, "must not feed the production-like P04 sink"));
}

TEST(Config, UnknownEnumValuesAreRejected) {
  EXPECT_TRUE(mentions(errors_with("validation", "mode", "\"lenient\""),
                       "unknown value"));
  EXPECT_TRUE(
      mentions(errors_with("output", "sink", "\"kafka\""), "unknown value"));
  EXPECT_TRUE(
      mentions(errors_with("input", "mode", "\"jsonl\""), "unknown value"));
}

TEST(Config, EverySymbolNeedsPriceRulesAndViceVersa) {
  std::string toml = base_config_toml();
  toml += "\n[prices.AUDCAD]\nprice_scale = 100000\nmax_price_ticks = 1\n";
  Config c;
  std::vector<std::string> errs;
  Config::load_from_string(toml, "<test>", c, errs);
  EXPECT_TRUE(mentions(errs, "missing from allowed_symbols"));

  // ... and an allowed symbol with no rules is equally a configuration error.
  const auto missing =
      errors_with("identity", "allowed_symbols",
                  "[\"EURUSD\", \"GBPUSD\", \"USDJPY\", \"XAUUSD\"]");
  EXPECT_TRUE(mentions(missing, "no [prices.XAUUSD] rules"));
}

TEST(Config, RejectsInvertedPriceAndSpreadBounds) {
  EXPECT_TRUE(mentions(errors_with("prices.EURUSD", "max_price_ticks", "0"),
                       "max_price_ticks < min_price_ticks"));
  EXPECT_TRUE(mentions(errors_with("prices.EURUSD", "max_spread_ticks", "-1"),
                       "max_spread_ticks < min_spread_ticks"));
  EXPECT_TRUE(mentions(errors_with("prices.EURUSD", "price_scale", "0"),
                       "price_scale must be > 0"));
}

TEST(Config, UnsafeOutputPathIsRejected) {
  EXPECT_TRUE(mentions(errors_for({{"output", "sink", "\"ndjson_test\""},
                                   {"output", "file", "\"../../etc/passwd\""}}),
                       "unsafe path"));
  EXPECT_TRUE(
      mentions(errors_for({{"output", "sink", "\"ndjson_test\""},
                           {"output", "file", "\"out/$(whoami).ndjson\""}}),
               "unsafe path"));
}

TEST(Config, ProtocolAndPackageArePinned) {
  EXPECT_TRUE(
      mentions(errors_with("identity", "protocol", "\"MDP2\""), "must be MDP1"));
  EXPECT_TRUE(mentions(errors_with("identity", "proto_package", "\"other.v1\""),
                       "must be marketdata.v1"));
}

TEST(Config, MalformedFrameHandlingCannotBeLoosened) {
  // There is no resynchronisation scan in this build, so anything other than
  // fail_closed would be a promise the code cannot keep.
  EXPECT_TRUE(
      mentions(errors_with("validation", "on_malformed_frame", "\"resync\""),
               "must be fail_closed"));
}

TEST(Config, AllowedSymbolsMustRespectTheCharsetAndCaps) {
  EXPECT_TRUE(mentions(errors_with("identity", "allowed_symbols", "[\"eur usd\"]"),
                       "ASCII allowlist"));
  EXPECT_TRUE(mentions(errors_with("identity", "max_symbols", "1"),
                       "smaller than allowed_symbols"));
}

TEST(Config, StubDelayOnlyAppliesToTheP04Sink) {
  EXPECT_TRUE(mentions(errors_with("output", "stub_delay_us", "100"),
                       "only applies to the p04_stub sink"));
  EXPECT_FALSE(mentions(errors_for({{"output", "sink", "\"p04_stub\""},
                                    {"output", "file", "\"out/p04.ndjson\""},
                                    {"output", "stub_delay_us", "100"}}),
                        "only applies to the p04_stub sink"));
}

TEST(Config, PbstreamModeRequiresAnInputFile) {
  EXPECT_TRUE(mentions(errors_with("input", "file", "\"\""),
                       "input.file is required for input.mode=pbstream"));
}

TEST(Config, LookupHelpers) {
  const auto c = base_config();
  EXPECT_TRUE(c.symbol_allowed("EURUSD"));
  EXPECT_FALSE(c.symbol_allowed("AUDCAD"));
  ASSERT_NE(c.rules_for("USDJPY"), nullptr);
  EXPECT_EQ(c.rules_for("USDJPY")->price_scale, 1000);
  EXPECT_EQ(c.rules_for("NOPE"), nullptr);
}
