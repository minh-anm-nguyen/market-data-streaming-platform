// Per-key sequence classification. Group F of the what-if matrix.
//
// Expectations here are written from the design document by hand. None of
// them is derived from the tracker's own output.
#include <gtest/gtest.h>

#include <limits>

#include "md_adapter/sequence_tracker.hpp"
#include "test_support.hpp"

using namespace mdtest;
using md::SeqClass;
using md::SequenceLimits;
using md::SequenceTracker;

namespace {

SequenceLimits offline_limits(std::uint32_t window = 4096) {
  SequenceLimits l;
  l.window = window;
  l.live_baseline_unverified = false;
  l.expect_start_at_one = false;
  return l;
}

bool has(std::uint32_t flags, std::uint32_t bit) { return (flags & bit) != 0; }

}  // namespace

// --- ordering ---------------------------------------------------------------

TEST(SequenceTracker, ContiguousStreamIsAllOk) {
  SequenceTracker t(offline_limits());
  for (int i = 1; i <= 4; ++i) {
    const auto r = t.observe(quote_event(i, i * 1000));
    EXPECT_EQ(r.klass, SeqClass::kOk) << "seq=" << i;
  }
  const auto* s = t.stats("ds-test", "EURUSD");
  ASSERT_NE(s, nullptr);
  EXPECT_EQ(s->highest_seen_valid, 4);
  EXPECT_EQ(s->contiguous_observed_valid, 4);
  EXPECT_EQ(s->gap_incidents, 0u);
}

// Design 5.1 trace: 1,2,4 -> seq 4 reports a gap over slot 3.
TEST(SequenceTracker, GapIsDetectedAndCounted) {
  SequenceTracker t(offline_limits());
  t.observe(quote_event(1, 1000));
  t.observe(quote_event(2, 2000));
  const auto r = t.observe(quote_event(4, 4000));

  EXPECT_EQ(r.klass, SeqClass::kGap);
  EXPECT_EQ(r.missing_slots_observed, 1);
  EXPECT_TRUE(has(r.quality_flags, md::quality::kGap));
  EXPECT_TRUE(has(r.quality_flags, md::quality::kDegraded));

  const auto* s = t.stats("ds-test", "EURUSD");
  EXPECT_EQ(s->gap_incidents, 1u);
  EXPECT_EQ(s->missing_slots_observed, 1u);
  // Observation continuity stops before the hole.
  EXPECT_EQ(s->contiguous_observed_valid, 2);
  EXPECT_EQ(s->highest_seen_valid, 4);
}

// E2E-03: 1,2,4,3,2,2*,5
TEST(SequenceTracker, DesignTraceE2E03) {
  SequenceTracker t(offline_limits());

  EXPECT_EQ(t.observe(quote_event(1, 1000)).klass, SeqClass::kOk);
  EXPECT_EQ(t.observe(quote_event(2, 2000)).klass, SeqClass::kOk);
  EXPECT_EQ(t.observe(quote_event(4, 4000)).klass, SeqClass::kGap);

  const auto late = t.observe(quote_event(3, 3000));
  EXPECT_EQ(late.klass, SeqClass::kReorderLate);

  // Byte-identical repeat of seq 2.
  const auto dup = t.observe(quote_event(2, 2000));
  EXPECT_EQ(dup.klass, SeqClass::kDuplicateExact);

  // Same sequence, different price: an integrity problem, not a duplicate.
  const auto conflict = t.observe(quote_event(2, 2000, 109500, 109510));
  EXPECT_EQ(conflict.klass, SeqClass::kDuplicateConflict);

  EXPECT_EQ(t.observe(quote_event(5, 5000)).klass, SeqClass::kOk);

  const auto* s = t.stats("ds-test", "EURUSD");
  // The late 3 repaired *observed* continuity up to 5.
  EXPECT_EQ(s->contiguous_observed_valid, 5);
  EXPECT_EQ(s->duplicate_exact, 1u);
  EXPECT_EQ(s->duplicate_conflict, 1u);
  EXPECT_EQ(s->reorder_late, 1u);
}

TEST(SequenceTracker, ConflictingDuplicateDoesNotOverwriteStoredValue) {
  SequenceTracker t(offline_limits());
  t.observe(quote_event(1, 1000, 109000, 109010));
  t.observe(quote_event(2, 2000, 109000, 109010));
  t.observe(quote_event(2, 2000, 777777, 777787));  // conflict

  // The original value must still be the one on record: a later sender
  // cannot rewrite history by resending.
  const auto again = t.observe(quote_event(2, 2000, 109000, 109010));
  EXPECT_EQ(again.klass, SeqClass::kDuplicateExact);
}

TEST(SequenceTracker, DuplicateComparesTheFullTypedCore) {
  SequenceTracker t(offline_limits());
  t.observe(quote_event(1, 1000, 109000, 109010));

  // Same prices, different event time -> still a conflict.
  EXPECT_EQ(t.observe(quote_event(1, 9999, 109000, 109010)).klass,
            SeqClass::kDuplicateConflict);

  SequenceTracker t2(offline_limits());
  t2.observe(quote_event(1, 1000, 109000, 109010));
  // A tick with the same numbers is a different payload kind.
  EXPECT_EQ(t2.observe(tick_event(1, 1000, 109000)).klass,
            SeqClass::kDuplicateConflict);
}

// --- per-key isolation ------------------------------------------------------

// F-group / property 2: symbols must not share continuity.
TEST(SequenceTracker, SymbolsAreIndependent) {
  SequenceTracker t(offline_limits());
  t.observe(quote_event(1, 1000, 109000, 109010, "EURUSD"));
  t.observe(quote_event(1, 1000, 109000, 109010, "GBPUSD"));
  t.observe(quote_event(2, 2000, 109000, 109010, "EURUSD"));

  // GBPUSD jumping to 2 is its own first gap-free step, not affected by
  // EURUSD traffic in between.
  EXPECT_EQ(t.observe(quote_event(2, 2000, 109000, 109010, "GBPUSD")).klass,
            SeqClass::kOk);
  EXPECT_EQ(t.stats("ds-test", "EURUSD")->highest_seen_valid, 2);
  EXPECT_EQ(t.stats("ds-test", "GBPUSD")->highest_seen_valid, 2);
}

TEST(SequenceTracker, DatasetsAreIndependent) {
  SequenceTracker t(offline_limits());
  t.observe(quote_event(5, 1000, 109000, 109010, "EURUSD", "ds-A"));
  // A different dataset starts its own state; this is not a rollback.
  const auto r = t.observe(quote_event(1, 1000, 109000, 109010, "EURUSD", "ds-B"));
  EXPECT_EQ(r.klass, SeqClass::kOk);
  EXPECT_TRUE(r.first_event_for_key);
  EXPECT_EQ(t.key_count(), 2u);
  EXPECT_EQ(t.dataset_count(), 2u);
}

// --- window edges -----------------------------------------------------------

TEST(SequenceTracker, OutsideTheWindowIsUnverifiableNotDuplicate) {
  SequenceTracker t(offline_limits(8));
  for (int i = 1; i <= 20; ++i) t.observe(quote_event(i, i * 1000));

  // Slot 1 is long gone from an 8-wide window. We saw it, but we can no
  // longer prove that, so claiming "duplicate" would be a guess.
  const auto r = t.observe(quote_event(1, 1000));
  EXPECT_EQ(r.klass, SeqClass::kTooOldUnverifiable);
  EXPECT_TRUE(has(r.quality_flags, md::quality::kTooOldUnverifiable));
  EXPECT_FALSE(has(r.quality_flags, md::quality::kDuplicateExact));
}

TEST(SequenceTracker, DuplicateIsDetectedAtTheExactWindowEdge) {
  SequenceTracker t(offline_limits(8));
  for (int i = 1; i <= 8; ++i) t.observe(quote_event(i, i * 1000));
  // Window is [1..8]; sequence 1 is still inside it.
  EXPECT_EQ(t.observe(quote_event(1, 1000)).klass, SeqClass::kDuplicateExact);

  t.observe(quote_event(9, 9000));
  // Window is now [2..9]; sequence 1 has just fallen out.
  EXPECT_EQ(t.observe(quote_event(1, 1000)).klass, SeqClass::kTooOldUnverifiable);
}

// F/J-group: a jump to INT64_MAX must not loop or allocate per missing slot.
TEST(SequenceTracker, HugeGapIsBoundedWork) {
  SequenceTracker t(offline_limits());
  t.observe(quote_event(1, 1000));

  const std::uint64_t before = t.state_bytes();
  const auto r = t.observe(quote_event(std::numeric_limits<std::int64_t>::max(), 2000));

  EXPECT_EQ(r.klass, SeqClass::kGap);
  EXPECT_TRUE(has(r.quality_flags, md::quality::kHugeGap));
  EXPECT_EQ(r.missing_slots_observed,
            std::numeric_limits<std::int64_t>::max() - 2);
  // No state growth proportional to the gap.
  EXPECT_EQ(t.state_bytes(), before);

  const auto* s = t.stats("ds-test", "EURUSD");
  EXPECT_TRUE(s->continuity_uncertain);
  // And the stream keeps working afterwards.
  EXPECT_EQ(t.observe(quote_event(std::numeric_limits<std::int64_t>::max(), 2000))
                .klass,
            SeqClass::kDuplicateExact);
}

TEST(SequenceTracker, SequenceAtInt64MaxDoesNotOverflow) {
  SequenceTracker t(offline_limits());
  const std::int64_t kMax = std::numeric_limits<std::int64_t>::max();
  t.observe(quote_event(kMax - 1, 1000));
  // highest + 1 must be computed without signed overflow.
  const auto r = t.observe(quote_event(kMax, 2000));
  EXPECT_EQ(r.klass, SeqClass::kOk);
}

// --- rollback ---------------------------------------------------------------

// Design 4.3: a restart to the baseline inside one dataset_id must not be
// read as a new session. But below the window we also cannot prove what the
// slot held, so the honest classification stays TOO_OLD_UNVERIFIABLE and the
// restart travels as a flag on top of it.
TEST(SequenceTracker, RestartAtBaselineIsFlaggedOnTopOfTooOld) {
  SequenceTracker t(offline_limits(8));
  for (int i = 1; i <= 30; ++i) t.observe(quote_event(i, i * 1000));

  const auto r = t.observe(quote_event(1, 1000));
  EXPECT_EQ(r.klass, SeqClass::kTooOldUnverifiable);
  EXPECT_EQ(r.reason, md::Reason::kSequenceRollback);
  EXPECT_TRUE(has(r.quality_flags, md::quality::kSequenceRollback));
  EXPECT_TRUE(has(r.quality_flags, md::quality::kTooOldUnverifiable));
  EXPECT_TRUE(t.stats("ds-test", "EURUSD")->continuity_uncertain);
}

// A feed that legitimately started mid-stream has a baseline above 1, and an
// ancient event that is not that baseline is only "too old".
TEST(SequenceTracker, OldEventThatIsNotTheBaselineIsNotFlaggedAsARollback) {
  SequenceLimits l = offline_limits(8);
  l.live_baseline_unverified = true;
  SequenceTracker t(l);
  for (int i = 50; i <= 80; ++i) t.observe(quote_event(i, i * 1000));

  const auto r = t.observe(quote_event(51, 51000));
  EXPECT_EQ(r.klass, SeqClass::kTooOldUnverifiable);
  EXPECT_FALSE(has(r.quality_flags, md::quality::kSequenceRollback));

  const auto baseline = t.observe(quote_event(50, 50000));
  EXPECT_TRUE(has(baseline.quality_flags, md::quality::kSequenceRollback));
}

// --- first event ------------------------------------------------------------

TEST(SequenceTracker, LiveFirstEventIsStartUnverified) {
  SequenceLimits l = offline_limits();
  l.live_baseline_unverified = true;
  SequenceTracker t(l);

  const auto r = t.observe(quote_event(57, 1000));
  EXPECT_TRUE(r.first_event_for_key);
  EXPECT_TRUE(has(r.quality_flags, md::quality::kStartUnverified));
  // Starting mid-stream on a live feed is normal; it is not a gap.
  EXPECT_EQ(r.klass, SeqClass::kOk);
}

TEST(SequenceTracker, VerifiedOfflineFixtureMustStartAtOne) {
  SequenceLimits l = offline_limits();
  l.live_baseline_unverified = false;
  l.expect_start_at_one = true;
  SequenceTracker t(l);

  const auto r = t.observe(quote_event(5, 1000));
  EXPECT_EQ(r.klass, SeqClass::kGap);
  EXPECT_TRUE(has(r.quality_flags, md::quality::kInitialGap));
  EXPECT_EQ(r.missing_slots_observed, 4);
}

// --- published continuity ---------------------------------------------------

// ADR-06: a late arrival repairs what we observed, never what we published.
TEST(SequenceTracker, PublishedContinuityIsSeparateFromObserved) {
  SequenceTracker t(offline_limits());
  const auto r1 = t.observe(quote_event(1, 1000));
  t.note_published(r1.handle, 1);
  const auto r2 = t.observe(quote_event(2, 2000));
  t.note_published(r2.handle, 2);
  const auto r4 = t.observe(quote_event(4, 4000));
  t.note_published(r4.handle, 4);  // forwarded, but out of order
  t.observe(quote_event(3, 3000));  // late; strict mode does not publish it

  const auto* s = t.stats("ds-test", "EURUSD");
  EXPECT_EQ(s->contiguous_observed_valid, 4);
  EXPECT_EQ(s->contiguous_published, 2)
      << "published continuity must not heal from an unpublished late event";
}

// --- resource caps ----------------------------------------------------------

TEST(SequenceTracker, RejectsNewKeysOnceTheByteBudgetIsSpent) {
  SequenceLimits l;
  l.window = 64;
  l.max_symbols_per_dataset = 1000;
  l.max_datasets = 1000;
  SequenceTracker probe(l);
  probe.observe(quote_event(1, 1000, 109000, 109010, "EURUSD", "ds-0"));
  const std::uint64_t per_key = probe.bytes_per_key();

  l.max_state_bytes = per_key * 3 + 1;  // room for exactly three keys
  SequenceTracker t(l);
  for (int i = 0; i < 3; ++i) {
    const auto r =
        t.observe(quote_event(1, 1000, 109000, 109010, "EURUSD",
                              "ds-" + std::to_string(i)));
    EXPECT_NE(r.klass, SeqClass::kStateCapExceeded) << "key " << i;
  }
  const auto r =
      t.observe(quote_event(1, 1000, 109000, 109010, "EURUSD", "ds-overflow"));

  EXPECT_EQ(r.klass, SeqClass::kStateCapExceeded);
  EXPECT_EQ(r.reason, md::Reason::kStateCapExceeded);
  EXPECT_EQ(t.key_count(), 3u) << "an active key must never be evicted";
  EXPECT_EQ(t.state_cap_rejections(), 1u);
  EXPECT_LE(t.state_bytes(), l.max_state_bytes);
}

TEST(SequenceTracker, RejectsBeyondMaxSymbolsPerDataset) {
  SequenceLimits l = offline_limits(16);
  l.max_symbols_per_dataset = 2;
  l.max_state_bytes = 1ull << 30;
  SequenceTracker t(l);

  t.observe(quote_event(1, 1000, 109000, 109010, "EURUSD"));
  t.observe(quote_event(1, 1000, 109000, 109010, "GBPUSD"));
  const auto r = t.observe(quote_event(1, 1000, 109000, 109010, "USDJPY"));

  EXPECT_EQ(r.klass, SeqClass::kStateCapExceeded);
}

TEST(SequenceTracker, RejectsBeyondMaxDatasets) {
  SequenceLimits l = offline_limits(16);
  l.max_datasets = 2;
  l.max_state_bytes = 1ull << 30;
  SequenceTracker t(l);

  t.observe(quote_event(1, 1000, 109000, 109010, "EURUSD", "a"));
  t.observe(quote_event(1, 1000, 109000, 109010, "EURUSD", "b"));
  const auto r = t.observe(quote_event(1, 1000, 109000, 109010, "EURUSD", "c"));

  EXPECT_EQ(r.klass, SeqClass::kStateCapExceeded);
}

// J-group: cardinality attacks must not grow state without bound.
TEST(SequenceTracker, StateStaysBoundedUnderACardinalityAttack) {
  SequenceLimits l;
  l.window = 64;
  l.max_symbols_per_dataset = 256;
  l.max_datasets = 4;
  l.max_state_bytes = 1 << 20;  // 1 MiB
  SequenceTracker t(l);

  for (int d = 0; d < 50; ++d) {
    for (int s = 0; s < 500; ++s) {
      t.observe(quote_event(1, 1000, 109000, 109010,
                            "S" + std::to_string(s), "D" + std::to_string(d)));
    }
  }
  EXPECT_LE(t.state_bytes(), l.max_state_bytes);
  EXPECT_LE(t.dataset_count(), l.max_datasets);
  EXPECT_GT(t.state_cap_rejections(), 0u);
}
