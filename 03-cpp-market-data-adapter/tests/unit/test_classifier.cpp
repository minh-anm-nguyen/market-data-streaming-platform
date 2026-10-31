// Output policy per validation mode. ADR-06 / ADR-07.
#include <gtest/gtest.h>

#include "md_adapter/classifier.hpp"
#include "test_support.hpp"

using namespace mdtest;
using md::Classifier;
using md::FreshnessEngine;
using md::Reason;
using md::SeqClass;
using md::SeqResult;
using md::ValidationMode;

namespace {

SeqResult seq(SeqClass k, std::uint32_t flags = 0, Reason r = Reason::kNone) {
  SeqResult s;
  s.klass = k;
  s.quality_flags = flags;
  s.reason = r;
  return s;
}

FreshnessEngine::Result fresh_ok() { return {}; }

FreshnessEngine::Result fresh_stale() {
  FreshnessEngine::Result f;
  f.event_time_old = true;
  f.reason = Reason::kEventTimeOld;
  f.quality_flags = md::quality::kEventTimeOld | md::quality::kDegraded;
  return f;
}

bool has(std::uint32_t flags, std::uint32_t bit) { return (flags & bit) != 0; }

}  // namespace

// --- strict_monitor ---------------------------------------------------------

TEST(Classifier, StrictPublishesInOrderEvents) {
  Classifier c(ValidationMode::kStrictMonitor);
  const auto d = c.classify(quote_event(1, 1000), seq(SeqClass::kOk), fresh_ok());
  EXPECT_TRUE(d.publish);
  EXPECT_EQ(d.primary_reason, Reason::kNone);
}

// ADR-07: a gapped-but-new event is still forwarded, carrying DEGRADED, so
// analytics keeps seeing fresh data and also knows continuity is broken.
TEST(Classifier, StrictPublishesGappedEventsAsDegraded) {
  Classifier c(ValidationMode::kStrictMonitor);
  const auto d = c.classify(
      quote_event(4, 4000),
      seq(SeqClass::kGap, md::quality::kGap | md::quality::kDegraded,
          Reason::kSequenceGap),
      fresh_ok());
  EXPECT_TRUE(d.publish);
  EXPECT_EQ(d.primary_reason, Reason::kSequenceGap);
  EXPECT_TRUE(has(d.quality_flags, md::quality::kDegraded));
}

TEST(Classifier, StrictSuppressesDuplicatesLateAndTooOld) {
  Classifier c(ValidationMode::kStrictMonitor);

  const auto dup = c.classify(quote_event(2, 2000),
                              seq(SeqClass::kDuplicateExact), fresh_ok());
  EXPECT_FALSE(dup.publish);
  EXPECT_EQ(dup.suppressed_reason, Reason::kDuplicateExact);
  EXPECT_FALSE(dup.integrity_alert);

  const auto conflict = c.classify(quote_event(2, 2000),
                                   seq(SeqClass::kDuplicateConflict), fresh_ok());
  EXPECT_FALSE(conflict.publish);
  EXPECT_EQ(conflict.suppressed_reason, Reason::kDuplicateConflict);
  EXPECT_TRUE(conflict.integrity_alert)
      << "the same ID carrying different data is an integrity problem";

  const auto late = c.classify(quote_event(3, 3000),
                               seq(SeqClass::kReorderLate), fresh_ok());
  EXPECT_FALSE(late.publish);
  EXPECT_EQ(late.suppressed_reason, Reason::kReorderLate);

  const auto old = c.classify(quote_event(1, 1000),
                              seq(SeqClass::kTooOldUnverifiable), fresh_ok());
  EXPECT_FALSE(old.publish);
  EXPECT_EQ(old.suppressed_reason, Reason::kTooOldUnverifiable);
}

TEST(Classifier, StrictSuppressesAStaleQuoteEvenWhenTheSequenceIsFine) {
  Classifier c(ValidationMode::kStrictMonitor);
  const auto d =
      c.classify(quote_event(5, 1000), seq(SeqClass::kOk), fresh_stale());
  EXPECT_FALSE(d.publish) << "an old quote is not a current quote";
  EXPECT_EQ(d.suppressed_reason, Reason::kEventTimeOld);
  EXPECT_TRUE(has(d.quality_flags, md::quality::kEventTimeOld));
}

TEST(Classifier, StateCapExceededIsNeverPublished) {
  Classifier c(ValidationMode::kStrictMonitor);
  const auto d = c.classify(quote_event(1, 1000),
                            seq(SeqClass::kStateCapExceeded), fresh_ok());
  EXPECT_FALSE(d.publish);
  EXPECT_EQ(d.primary_reason, Reason::kStateCapExceeded);
}

// A sender restarting at the baseline is refused like any other
// unverifiable old event, but it raises an integrity alert: the operator has
// to start a new verified dataset epoch, the adapter cannot do it alone.
TEST(Classifier, RollbackIsRefusedAndAlerted) {
  Classifier c(ValidationMode::kStrictMonitor);
  const auto d = c.classify(
      quote_event(1, 1000),
      seq(SeqClass::kTooOldUnverifiable,
          md::quality::kTooOldUnverifiable | md::quality::kSequenceRollback),
      fresh_ok());
  EXPECT_FALSE(d.publish);
  EXPECT_TRUE(d.integrity_alert);
  EXPECT_EQ(d.primary_reason, Reason::kSequenceRollback);
}

// --- fault_observe ----------------------------------------------------------

// Design 5: the fault lane preserves arrival order and every anomaly, so a
// detector can be scored. It is a test-only sink.
TEST(Classifier, FaultObservePublishesEverythingWithATestOnlyFlag) {
  Classifier c(ValidationMode::kFaultObserve);
  for (auto k : {SeqClass::kOk, SeqClass::kGap, SeqClass::kDuplicateExact,
                 SeqClass::kDuplicateConflict, SeqClass::kReorderLate,
                 SeqClass::kTooOldUnverifiable}) {
    const auto d = c.classify(quote_event(1, 1000), seq(k), fresh_ok());
    EXPECT_TRUE(d.publish) << static_cast<int>(k);
    EXPECT_TRUE(has(d.quality_flags, md::quality::kFaultTestOnly))
        << static_cast<int>(k);
  }
  // Even a stale quote is preserved for the oracle.
  const auto stale =
      c.classify(quote_event(1, 1000), seq(SeqClass::kOk), fresh_stale());
  EXPECT_TRUE(stale.publish);
  EXPECT_TRUE(has(stale.quality_flags, md::quality::kEventTimeOld));
}

TEST(Classifier, FaultObserveStillRefusesWhenNoStateCouldBeAllocated) {
  Classifier c(ValidationMode::kFaultObserve);
  const auto d = c.classify(quote_event(1, 1000),
                            seq(SeqClass::kStateCapExceeded), fresh_ok());
  EXPECT_FALSE(d.publish);
}

// --- offline_verify ---------------------------------------------------------

TEST(Classifier, OfflineVerifyUsesTheStrictPublicationPolicy) {
  Classifier strict(ValidationMode::kStrictMonitor);
  Classifier offline(ValidationMode::kOfflineVerify);
  for (auto k : {SeqClass::kOk, SeqClass::kGap, SeqClass::kDuplicateExact,
                 SeqClass::kReorderLate}) {
    EXPECT_EQ(strict.classify(quote_event(1, 1000), seq(k), fresh_ok()).publish,
              offline.classify(quote_event(1, 1000), seq(k), fresh_ok()).publish)
        << static_cast<int>(k);
  }
}

// --- flags ------------------------------------------------------------------

TEST(Classifier, UnknownFieldsAndUncertainContinuityAreSurfaced) {
  Classifier c(ValidationMode::kStrictMonitor);
  auto e = quote_event(1, 1000);
  e.unknown_fields = true;
  auto s = seq(SeqClass::kOk);
  s.continuity_uncertain = true;

  const auto d = c.classify(e, s, fresh_ok());
  EXPECT_TRUE(has(d.quality_flags, md::quality::kUnknownFields));
  EXPECT_TRUE(has(d.quality_flags, md::quality::kContinuityLost));
}
