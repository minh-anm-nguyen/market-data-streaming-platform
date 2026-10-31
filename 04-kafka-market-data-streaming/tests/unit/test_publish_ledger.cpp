// The publish attempt ledger (FR-07, FR-08, FR-16, section 5.2, section 10.4,
// cases E-07, E-08, F-01 to F-05, Q-*).
//
// Three properties carry the weight:
//
//   * The conservation equations from section 10.4 balance at every quiesced
//     cut, so no event can go missing from the accounting.
//   * An attempt cannot be finalised twice. A second delivery report must be
//     refused and counted, never applied, or an UNKNOWN could be overwritten
//     with an ACKED and fabricate durability.
//   * One identity can own several attempts. The resend ordinal is what turns
//     "this run might have duplicated something" into a number.
#include "ledger/publish_status.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <unistd.h>
#include <string>
#include <vector>

namespace p04 {
namespace {

// mkstemp rather than tmpnam: tmpnam races between the name it returns and the
// open, and the linker warns about it. These tests write real files, so the
// unique-and-already-created guarantee matters.
std::string MakeTempPath(const std::string& suffix) {
  char pattern[] = "/tmp/p04_test_XXXXXX";
  const int fd = mkstemp(pattern);
  if (fd >= 0) close(fd);
  return std::string(pattern) + suffix;
}

EventIdentity Id(const std::string& dataset, const std::string& symbol,
                 std::int64_t sequence) {
  EventIdentity identity;
  identity.dataset_id = dataset;
  identity.symbol = symbol;
  identity.sequence = sequence;
  return identity;
}

std::uint64_t Enqueue(PublishLedger* ledger, const EventIdentity& identity,
                      std::int32_t expected_partition = 5) {
  ledger->RecordUpstreamAccepted();
  const std::uint64_t attempt = ledger->CreateAttempt(
      identity, EventKind::kQuote, "md.quotes.v1", identity.symbol,
      expected_partition, 64, 80);
  ledger->MarkQueuedLibrary(attempt);
  return attempt;
}

TEST(PublishLedger, FreshLedgerConserves) {
  PublishLedger ledger;
  std::string error;
  EXPECT_TRUE(ledger.CheckConservation(&error)) << error;
}

TEST(PublishLedger, AckedAttemptRecordsBrokerMetadata) {
  PublishLedger ledger;
  const std::uint64_t attempt = Enqueue(&ledger, Id("ds-a", "EURUSD", 1));
  ledger.MarkAcked(attempt, 5, 1234);

  AttemptRecord record;
  ASSERT_TRUE(ledger.Lookup(attempt, &record));
  EXPECT_EQ(record.state, AttemptState::kAcked);
  EXPECT_EQ(record.broker_partition, 5);
  EXPECT_EQ(record.broker_offset, 1234);
  EXPECT_GE(record.ack_latency_seconds, 0.0);
  EXPECT_TRUE(record.terminal());

  std::string error;
  EXPECT_TRUE(ledger.CheckConservation(&error)) << error;
  EXPECT_EQ(ledger.Counters().acked, 1u);
}

// Section 4.1: queued in the library is not delivered. A ledger that reported
// otherwise would make FR-07 unverifiable.
TEST(PublishLedger, QueuedInLibraryIsNotTerminalAndNotAcked) {
  PublishLedger ledger;
  const std::uint64_t attempt = Enqueue(&ledger, Id("ds-a", "EURUSD", 1));
  AttemptRecord record;
  ASSERT_TRUE(ledger.Lookup(attempt, &record));
  EXPECT_EQ(record.state, AttemptState::kQueuedLib);
  EXPECT_FALSE(record.terminal());
  EXPECT_FALSE(IsBrokerConfirmed(record.state));
  EXPECT_EQ(record.broker_offset, -1) << "no offset exists without an append";

  const LedgerCounters counters = ledger.Counters();
  EXPECT_EQ(counters.queued_lib, 1u);
  EXPECT_EQ(counters.acked, 0u);
  EXPECT_EQ(counters.pending_in_library, 1u);
  std::string error;
  EXPECT_TRUE(ledger.CheckConservation(&error)) << error;
}

// The double-finalize refusal. A second report is counted and discarded.
TEST(PublishLedger, RefusesASecondTerminalReport) {
  PublishLedger ledger;
  const std::uint64_t attempt = Enqueue(&ledger, Id("ds-a", "EURUSD", 1));
  ledger.MarkUnknown(attempt, ErrorClass::kDeliveryUnknown, "timeout");
  ledger.MarkAcked(attempt, 5, 999);  // must not be applied

  AttemptRecord record;
  ASSERT_TRUE(ledger.Lookup(attempt, &record));
  EXPECT_EQ(record.state, AttemptState::kUnknown)
      << "an UNKNOWN was overwritten with an ACKED, fabricating durability";
  EXPECT_EQ(record.broker_offset, -1);

  const LedgerCounters counters = ledger.Counters();
  EXPECT_EQ(counters.unknown, 1u);
  EXPECT_EQ(counters.acked, 0u);
  EXPECT_EQ(counters.double_finalize_refused, 1u);
}

TEST(PublishLedger, RefusesReportsForUnknownAttemptIds) {
  PublishLedger ledger;
  ledger.MarkAcked(4242, 1, 1);
  ledger.MarkUnknown(4243, ErrorClass::kDeliveryUnknown, "x");
  ledger.MarkQueuedLibrary(4244);
  const LedgerCounters counters = ledger.Counters();
  EXPECT_EQ(counters.unknown_attempt_reports, 3u);
  EXPECT_EQ(counters.acked, 0u);
  EXPECT_EQ(counters.unknown, 0u);
}

TEST(PublishLedger, MarkQueuedLibraryTwiceIsRefused) {
  PublishLedger ledger;
  ledger.RecordUpstreamAccepted();
  const std::uint64_t attempt = ledger.CreateAttempt(
      Id("ds-a", "EURUSD", 1), EventKind::kTick, "md.ticks.v1", "EURUSD", 1, 10, 20);
  ledger.MarkQueuedLibrary(attempt);
  ledger.MarkQueuedLibrary(attempt);
  EXPECT_EQ(ledger.Counters().queued_lib, 1u)
      << "counting the same attempt twice would break the conservation equations";
  EXPECT_EQ(ledger.Counters().double_finalize_refused, 1u);
}

// Cases E-07 and E-08: an application-level resend creates a second attempt for
// the same identity, and the ledger says so.
TEST(PublishLedger, TracksResendOrdinalsPerIdentity) {
  PublishLedger ledger;
  const EventIdentity identity = Id("ds-a", "EURUSD", 1);
  const std::uint64_t first = Enqueue(&ledger, identity);
  const std::uint64_t second = Enqueue(&ledger, identity);
  const std::uint64_t other = Enqueue(&ledger, Id("ds-a", "EURUSD", 2));

  AttemptRecord record;
  ASSERT_TRUE(ledger.Lookup(first, &record));
  EXPECT_EQ(record.resend_ordinal, 0u);
  ASSERT_TRUE(ledger.Lookup(second, &record));
  EXPECT_EQ(record.resend_ordinal, 1u);
  ASSERT_TRUE(ledger.Lookup(other, &record));
  EXPECT_EQ(record.resend_ordinal, 0u);

  EXPECT_EQ(ledger.Counters().app_level_resends, 1u);
  const auto resent = ledger.ResentIdentities();
  ASSERT_EQ(resent.size(), 1u);
  EXPECT_EQ(resent.front(), identity);
  EXPECT_NE(first, second) << "attempt ids must be unique, not keyed by identity";
}

// Case A-09: a dataset reset reuses sequence numbers, and that must not look
// like a resend.
TEST(PublishLedger, DatasetResetIsNotAResend) {
  PublishLedger ledger;
  Enqueue(&ledger, Id("ds-a", "EURUSD", 1));
  Enqueue(&ledger, Id("ds-b", "EURUSD", 1));
  EXPECT_EQ(ledger.Counters().app_level_resends, 0u);
  EXPECT_TRUE(ledger.ResentIdentities().empty());
}

// Cases B-05 and B-07: the broker reporting a different partition than the
// pinned partitioner chose is counted, because any per-key ordering claim would
// then be false.
TEST(PublishLedger, CountsPartitionMismatch) {
  PublishLedger ledger;
  const std::uint64_t attempt = Enqueue(&ledger, Id("ds-a", "EURUSD", 1), 5);
  ledger.MarkAcked(attempt, 7, 10);  // broker says 7, we expected 5
  EXPECT_EQ(ledger.Counters().partition_mismatch, 1u);

  PublishLedger agreeing;
  const std::uint64_t ok = Enqueue(&agreeing, Id("ds-a", "EURUSD", 1), 5);
  agreeing.MarkAcked(ok, 5, 10);
  EXPECT_EQ(agreeing.Counters().partition_mismatch, 0u);
}

// NFR-10: at the drain deadline, pending attempts become UNKNOWN, never ACKED
// and never a silent disappearance.
TEST(PublishLedger, FinalizesPendingAsUnknownAtTheDeadline) {
  PublishLedger ledger;
  Enqueue(&ledger, Id("ds-a", "EURUSD", 1));
  Enqueue(&ledger, Id("ds-a", "EURUSD", 2));
  ledger.RecordUpstreamAccepted();
  ledger.CreateAttempt(Id("ds-a", "EURUSD", 3), EventKind::kQuote, "md.quotes.v1",
                       "EURUSD", 5, 64, 80);

  EXPECT_EQ(ledger.Counters().pending_in_library, 2u);
  EXPECT_EQ(ledger.Counters().pending_in_app, 1u);

  std::vector<std::uint64_t> converted_ids;
  const std::uint64_t converted =
      ledger.FinalizePendingAsUnknown("deadline", &converted_ids);
  EXPECT_EQ(converted_ids.size(), converted)
      << "the caller needs every converted id so it can release the budget "
         "those attempts still hold";
  EXPECT_EQ(converted, 3u);
  const LedgerCounters counters = ledger.Counters();
  EXPECT_EQ(counters.unknown, 3u);
  EXPECT_EQ(counters.pending_in_library, 0u);
  EXPECT_EQ(counters.pending_in_app, 0u);
  EXPECT_EQ(counters.acked, 0u);
}

// The section 10.4 equations, exercised over a mixed run.
TEST(PublishLedger, ConservationHoldsAcrossAMixedRun) {
  PublishLedger ledger;
  for (int i = 0; i < 10; ++i) {
    ledger.RecordUpstreamAccepted();
    ledger.RecordPolicySuppressed();
  }
  for (int i = 0; i < 5; ++i) {
    ledger.RecordUpstreamAccepted();
    ledger.RecordRouteRejected();
  }
  for (int i = 0; i < 3; ++i) {
    ledger.RecordUpstreamAccepted();
    ledger.RecordAppReserveRejected();
  }
  std::vector<std::uint64_t> attempts;
  for (int i = 0; i < 20; ++i) {
    attempts.push_back(Enqueue(&ledger, Id("ds-a", "EURUSD", 100 + i)));
  }
  for (int i = 0; i < 12; ++i) {
    ledger.MarkAcked(attempts[static_cast<std::size_t>(i)], 5, i);
  }
  for (int i = 12; i < 15; ++i) {
    ledger.MarkFailedDefinite(attempts[static_cast<std::size_t>(i)],
                              ErrorClass::kContractError, "too large");
  }
  for (int i = 15; i < 18; ++i) {
    ledger.MarkUnknown(attempts[static_cast<std::size_t>(i)],
                       ErrorClass::kDeliveryUnknown, "timeout");
  }
  // Two attempts are left pending on purpose.

  const LedgerCounters counters = ledger.Counters();
  EXPECT_EQ(counters.upstream_accepted, 38u);
  EXPECT_EQ(counters.policy_suppressed, 10u);
  EXPECT_EQ(counters.route_rejected, 5u);
  EXPECT_EQ(counters.app_reserve_rejected, 3u);
  EXPECT_EQ(counters.queued_app, 20u);
  EXPECT_EQ(counters.queued_lib, 20u);
  EXPECT_EQ(counters.acked, 12u);
  EXPECT_EQ(counters.failed_definite, 3u);
  EXPECT_EQ(counters.unknown, 3u);
  EXPECT_EQ(counters.pending_in_library, 2u);

  std::string error;
  EXPECT_TRUE(ledger.CheckConservation(&error)) << error;
}

// The equations must actually be able to fail, or the check proves nothing. An
// offer counted without any corresponding disposition breaks equation one.
TEST(PublishLedger, ConservationDetectsAnUnaccountedOffer) {
  PublishLedger ledger;
  ledger.RecordUpstreamAccepted();
  std::string error;
  EXPECT_FALSE(ledger.CheckConservation(&error));
  EXPECT_NE(error.find("upstream_accepted"), std::string::npos) << error;
}

TEST(PublishLedger, EnqueueFailureIsAccountedUnderQueuedApp) {
  PublishLedger ledger;
  ledger.RecordUpstreamAccepted();
  const std::uint64_t attempt = ledger.CreateAttempt(
      Id("ds-a", "EURUSD", 1), EventKind::kTick, "md.ticks.v1", "EURUSD", 1, 10, 20);
  ledger.MarkEnqueueFailed(attempt, ErrorClass::kBackpressure, "queue full");

  const LedgerCounters counters = ledger.Counters();
  EXPECT_EQ(counters.queued_app, 1u);
  EXPECT_EQ(counters.enqueue_failed, 1u);
  EXPECT_EQ(counters.queued_lib, 0u);
  std::string error;
  EXPECT_TRUE(ledger.CheckConservation(&error)) << error;
}

TEST(PublishLedger, WritesOneJsonObjectPerAttempt) {
  PublishLedger ledger;
  const std::uint64_t acked = Enqueue(&ledger, Id("ds-a", "EURUSD", 1));
  ledger.MarkAcked(acked, 5, 77);
  const std::uint64_t unknown = Enqueue(&ledger, Id("ds-a", "EURUSD", 2));
  ledger.MarkUnknown(unknown, ErrorClass::kDeliveryUnknown, "timeout \"quoted\"");

  const std::string path = MakeTempPath(".jsonl");
  ASSERT_TRUE(ledger.WriteJsonl(path));

  std::ifstream in(path);
  std::string line;
  int lines = 0;
  bool saw_acked = false;
  bool saw_unknown = false;
  while (std::getline(in, line)) {
    ++lines;
    if (line.find("\"state\":\"ACKED\"") != std::string::npos) {
      saw_acked = true;
      EXPECT_NE(line.find("\"broker_offset\":77"), std::string::npos) << line;
      EXPECT_NE(line.find("\"expected_partition\":5"), std::string::npos) << line;
    }
    if (line.find("\"state\":\"UNKNOWN\"") != std::string::npos) {
      saw_unknown = true;
      // The free-text detail came from the client and must be escaped, or the
      // reconciliation tool cannot parse the file at all.
      EXPECT_NE(line.find("\\\"quoted\\\""), std::string::npos) << line;
      EXPECT_NE(line.find("\"broker_offset\":-1"), std::string::npos) << line;
    }
  }
  in.close();
  std::remove(path.c_str());
  EXPECT_EQ(lines, 2);
  EXPECT_TRUE(saw_acked);
  EXPECT_TRUE(saw_unknown);
}

TEST(PublishLedger, AttemptIdsAreUniqueAndMonotonic) {
  PublishLedger ledger;
  std::uint64_t previous = 0;
  for (int i = 0; i < 1000; ++i) {
    const std::uint64_t id = Enqueue(&ledger, Id("ds-a", "EURUSD", i));
    EXPECT_GT(id, previous) << "attempt ids must increase; the handoff keys on it";
    previous = id;
  }
  EXPECT_EQ(ledger.attempt_count(), 1000u);
}

}  // namespace
}  // namespace p04
