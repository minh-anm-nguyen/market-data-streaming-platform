// Structural and business validation. Groups D and E of the what-if matrix.
#include <gtest/gtest.h>

#include <limits>

#include "md_adapter/validators.hpp"
#include "test_support.hpp"

using namespace mdtest;
using md::DomainEvent;
using md::Reason;

namespace {

md::StructuralLimits slimits() {
  md::StructuralLimits l;
  l.expected_source_schema_version = "sim.v1";
  return l;
}

Reason structural(const EventSpec& s, bool unknown = false) {
  DomainEvent out;
  return md::validate_structural(make_message(s), slimits(), unknown, out);
}

}  // namespace

// --- structural: presence ---------------------------------------------------

TEST(Structural, AcceptsAWellFormedQuote) {
  DomainEvent e;
  EXPECT_EQ(md::validate_structural(make_message(EventSpec{}), slimits(), false, e),
            Reason::kNone);
  EXPECT_EQ(e.symbol, "EURUSD");
  EXPECT_EQ(e.sequence, 1);
  ASSERT_EQ(md::kind_of(e.payload), md::PayloadKind::kQuote);
  EXPECT_EQ(std::get<md::QuotePrice>(e.payload).bid, 109000);
}

// D-group: every required field must be *present*, not merely defaulted.
TEST(Structural, RejectsEachMissingField) {
  {
    EventSpec s;
    s.has_dataset_id = false;
    EXPECT_EQ(structural(s), Reason::kMissingDatasetId);
  }
  {
    EventSpec s;
    s.has_symbol = false;
    EXPECT_EQ(structural(s), Reason::kMissingSymbol);
  }
  {
    EventSpec s;
    s.has_sequence = false;
    EXPECT_EQ(structural(s), Reason::kMissingSequence);
  }
  {
    EventSpec s;
    s.has_event_time = false;
    EXPECT_EQ(structural(s), Reason::kMissingEventTime);
  }
  {
    EventSpec s;
    s.has_price_scale = false;
    EXPECT_EQ(structural(s), Reason::kMissingPriceScale);
  }
  {
    EventSpec s;
    s.has_version = false;
    EXPECT_EQ(structural(s), Reason::kMissingSourceVersion);
  }
}

// The heart of explicit presence: an explicit zero is present, and absence is
// not the same thing. Treating 0 as "unset" would silently accept a message
// that never carried a sequence.
TEST(Structural, ExplicitZeroIsPresentAndDistinctFromAbsent) {
  EventSpec present;
  present.sequence = 0;
  DomainEvent e;
  // Structurally fine; it is the business validator that rejects seq 0.
  EXPECT_EQ(md::validate_structural(make_message(present), slimits(), false, e),
            Reason::kNone);
  EXPECT_EQ(e.sequence, 0);

  EventSpec absent;
  absent.has_sequence = false;
  EXPECT_EQ(structural(absent), Reason::kMissingSequence);
}

TEST(Structural, RejectsMissingOneof) {
  EventSpec s;
  s.kind = "";
  EXPECT_EQ(structural(s), Reason::kMissingPayloadOneof);
}

TEST(Structural, RejectsIncompletePayloadInsideTheOneof) {
  {
    EventSpec s;
    s.has_bid = false;
    EXPECT_EQ(structural(s), Reason::kMissingQuoteField);
  }
  {
    EventSpec s;
    s.has_ask = false;
    EXPECT_EQ(structural(s), Reason::kMissingQuoteField);
  }
  {
    EventSpec s;
    s.kind = "tick";
    s.has_price = false;
    EXPECT_EQ(structural(s), Reason::kMissingTickField);
  }
}

// --- structural: bounds and charset -----------------------------------------

TEST(Structural, EnforcesByteCaps) {
  {
    EventSpec s;
    s.dataset_id = std::string(129, 'd');
    EXPECT_EQ(structural(s), Reason::kFieldTooLong);
  }
  {
    EventSpec s;
    s.symbol = std::string(33, 'A');
    EXPECT_EQ(structural(s), Reason::kFieldTooLong);
  }
  {
    EventSpec s;
    s.dataset_id = "";
    EXPECT_EQ(structural(s), Reason::kFieldTooLong);
  }
}

TEST(Structural, SymbolCharsetIsAnAllowlistAndNothingIsNormalised) {
  for (const char* bad : {"eurusd", "EUR USD", "EUR/USD", "EUR\tUSD", "EUR+USD"}) {
    EventSpec s;
    s.symbol = bad;
    EXPECT_EQ(structural(s), Reason::kSymbolCharset) << bad;
  }
  for (const char* good : {"EURUSD", "XAU.USD", "US_500", "BRENT-1"}) {
    EventSpec s;
    s.symbol = good;
    EXPECT_EQ(structural(s), Reason::kNone) << good;
  }
}

TEST(Structural, RejectsUnsupportedSchemaVersion) {
  EventSpec s;
  s.version = "sim.v2";
  EXPECT_EQ(structural(s), Reason::kUnsupportedSchemaVersion);
}

TEST(Structural, UnknownFieldsAreRecordedAndOptionallyRejected) {
  EventSpec s;
  EXPECT_EQ(structural(s, /*unknown=*/true), Reason::kNone);

  md::StructuralLimits l = slimits();
  l.reject_unknown_fields = true;
  DomainEvent e;
  EXPECT_EQ(md::validate_structural(make_message(s), l, true, e),
            Reason::kUnknownFieldPresent);
}

TEST(Structural, NegativeIntegersSurviveStructuralValidation) {
  // Signed values must reach the business validator unchanged, not be
  // clamped or rejected as "impossible" by the structural stage.
  EventSpec s;
  s.sequence = -5;
  s.bid = -1;
  s.ask = -1;
  DomainEvent e;
  EXPECT_EQ(md::validate_structural(make_message(s), slimits(), false, e),
            Reason::kNone);
  EXPECT_EQ(e.sequence, -5);
  EXPECT_EQ(std::get<md::QuotePrice>(e.payload).bid, -1);
}

// --- UTF-8 ------------------------------------------------------------------

TEST(Utf8, AcceptsValidSequences) {
  EXPECT_TRUE(md::is_valid_utf8(""));
  EXPECT_TRUE(md::is_valid_utf8("ascii"));
  EXPECT_TRUE(md::is_valid_utf8("\xC3\xA9"));              // e-acute
  EXPECT_TRUE(md::is_valid_utf8("\xE2\x82\xAC"));          // euro sign
  EXPECT_TRUE(md::is_valid_utf8("\xF0\x9F\x92\xB9"));      // U+1F4B9
}

TEST(Utf8, RejectsMalformedSequences) {
  EXPECT_FALSE(md::is_valid_utf8("\x80"));                  // lone continuation
  EXPECT_FALSE(md::is_valid_utf8("\xC3"));                  // truncated 2-byte
  EXPECT_FALSE(md::is_valid_utf8("\xE2\x82"));              // truncated 3-byte
  EXPECT_FALSE(md::is_valid_utf8("\xC0\xAF"));              // overlong '/'
  EXPECT_FALSE(md::is_valid_utf8("\xE0\x80\xAF"));          // overlong
  EXPECT_FALSE(md::is_valid_utf8("\xED\xA0\x80"));          // UTF-16 surrogate
  EXPECT_FALSE(md::is_valid_utf8("\xF5\x80\x80\x80"));      // > U+10FFFF
  EXPECT_FALSE(md::is_valid_utf8("\xFE"));                  // invalid lead
}

// --- business ---------------------------------------------------------------

TEST(Business, AcceptsAPlausibleQuote) {
  const auto cfg = base_config();
  md::BusinessValidator v(cfg);
  EXPECT_EQ(v.validate(quote_event(1, 1000, 109000, 109010)), Reason::kNone);
}

TEST(Business, RejectsNonPositiveSequence) {
  const auto cfg = base_config();
  md::BusinessValidator v(cfg);
  EXPECT_EQ(v.validate(quote_event(0, 1000)), Reason::kInvalidSequence);
  EXPECT_EQ(v.validate(quote_event(-1, 1000)), Reason::kInvalidSequence);
}

TEST(Business, RejectsNegativeEventTime) {
  const auto cfg = base_config();
  md::BusinessValidator v(cfg);
  EXPECT_EQ(v.validate(quote_event(1, -1)), Reason::kNegativeEventTime);
}

// E-group: a wrong scale silently rescales every price downstream, so it is
// a rejection and never a conversion.
TEST(Business, RejectsScaleMismatchWithoutConverting) {
  const auto cfg = base_config();
  md::BusinessValidator v(cfg);
  auto e = quote_event(1, 1000);
  e.price_scale = 1000;  // USDJPY's scale on a EURUSD event
  EXPECT_EQ(v.validate(e), Reason::kScaleMismatch);

  e.price_scale = 0;
  EXPECT_EQ(v.validate(e), Reason::kInvalidPriceScale);
  e.price_scale = -100000;
  EXPECT_EQ(v.validate(e), Reason::kInvalidPriceScale);
}

TEST(Business, RejectsCrossedAndNonPositiveQuotes) {
  const auto cfg = base_config();
  md::BusinessValidator v(cfg);
  EXPECT_EQ(v.validate(quote_event(1, 1000, 0, 10)),
            Reason::kQuoteBidNonPositive);
  EXPECT_EQ(v.validate(quote_event(1, 1000, -5, 10)),
            Reason::kQuoteBidNonPositive);
  EXPECT_EQ(v.validate(quote_event(1, 1000, 109010, 109000)),
            Reason::kQuoteCrossed);
}

TEST(Business, EnforcesSpreadBounds) {
  const auto cfg = base_config();
  md::BusinessValidator v(cfg);
  EXPECT_EQ(v.validate(quote_event(1, 1000, 109000, 109200)), Reason::kNone);
  EXPECT_EQ(v.validate(quote_event(1, 1000, 109000, 109201)),
            Reason::kSpreadOutOfRange);
  // Zero spread is allowed by this configuration.
  EXPECT_EQ(v.validate(quote_event(1, 1000, 109000, 109000)), Reason::kNone);
}

TEST(Business, EnforcesPriceRange) {
  const auto cfg = base_config();
  md::BusinessValidator v(cfg);
  EXPECT_EQ(v.validate(quote_event(1, 1000, 500001, 500001)),
            Reason::kPriceOutOfRange);
  EXPECT_EQ(v.validate(tick_event(1, 1000, 500001)), Reason::kPriceOutOfRange);
  EXPECT_EQ(v.validate(tick_event(1, 1000, 0)), Reason::kTickNonPositive);
}

// M/E-group: adversarial int64 values must not produce undefined behaviour.
TEST(Business, ExtremeIntegersDoNotOverflow) {
  const auto cfg = base_config();
  md::BusinessValidator v(cfg);
  constexpr std::int64_t kMin = std::numeric_limits<std::int64_t>::min();
  constexpr std::int64_t kMax = std::numeric_limits<std::int64_t>::max();

  // ask - bid would overflow if computed naively.
  const auto r = v.validate(quote_event(1, 1000, kMin + 1, kMax));
  EXPECT_NE(r, Reason::kNone);
  EXPECT_EQ(v.validate(quote_event(1, kMax, kMax, kMax)),
            Reason::kPriceOutOfRange);
  EXPECT_EQ(v.validate(quote_event(kMax, 1000, 109000, 109010)), Reason::kNone);
}

TEST(Business, RejectsSymbolsOutsideTheAllowlist) {
  const auto cfg = base_config();
  md::BusinessValidator v(cfg);
  auto e = quote_event(1, 1000, 109000, 109010, "AUDCAD");
  EXPECT_EQ(v.validate(e), Reason::kSymbolNotAllowed);
}

TEST(Business, PerSymbolScaleIsRespected) {
  const auto cfg = base_config();
  md::BusinessValidator v(cfg);
  // quote_event() picks 1000 for USDJPY, matching the configured scale.
  EXPECT_EQ(v.validate(quote_event(1, 1000, 15000, 15010, "USDJPY")),
            Reason::kNone);
  auto wrong = quote_event(1, 1000, 15000, 15010, "USDJPY");
  wrong.price_scale = 100000;
  EXPECT_EQ(v.validate(wrong), Reason::kScaleMismatch);
}
