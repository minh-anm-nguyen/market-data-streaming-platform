// The publish attempt ledger: the record of what Project 04 actually knows
// about every event it accepted (FR-07, FR-08, FR-16, section 4.1, section 5.2,
// section 10.4).
//
// This is the artifact that makes the difference between an honest report and a
// hopeful one. Four facts have to stay separable for the whole run:
//
//   accepted_by_adapter, queued_in_p04, queued_in_librdkafka,
//   broker_acknowledged
//
// and a fifth, consumer_observed, deliberately lives outside this class because
// only an independent verifier can establish it (section 5.1).
//
// Design decisions that the test matrix depends on:
//
//   * An attempt id is unique per enqueue attempt, never per identity
//     (section 10.1, oracle rule 3). One logical event that is resent after an
//     ambiguous timeout owns two attempts, and the ledger reports two. If the
//     identity were the key, case E-07 would be unobservable.
//   * Finalising an attempt twice is refused and counted, not applied. The
//     model-based fuzz suite in section 10.5 asserts "no double-finalize", and
//     a ledger that let the second write through would hide a librdkafka
//     callback bug or a shutdown race.
//   * A delivery timeout finalises as kUnknown, never kFailedDefinite.
//     Section 4.2: the broker may have appended and the acknowledgement may
//     have been lost. kFailedDefinite is reserved for errors that are provably
//     pre-append.
//
// The ledger is in memory. Section 6 is explicit that this means a SIGKILL
// loses it and the run becomes UNVERIFIED, which is why it is dumped to JSONL
// during an orderly shutdown and why recovery proofs rely on an offline
// replayable source rather than on this file surviving.
#ifndef P04_LEDGER_PUBLISH_STATUS_H_
#define P04_LEDGER_PUBLISH_STATUS_H_

#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "handoff/output_event.h"
#include "ops/error_class.h"

namespace p04 {

struct AttemptRecord {
  std::uint64_t attempt_id = 0;
  EventIdentity identity;
  EventKind kind = EventKind::kTick;
  std::string topic;
  std::string key;
  std::int32_t expected_partition = -1;
  std::size_t value_bytes = 0;
  std::size_t reserved_bytes = 0;

  AttemptState state = AttemptState::kCreated;
  ErrorClass error_class = ErrorClass::kNone;
  std::string error_detail;

  // Populated only on a successful delivery report. -1 means the broker never
  // told us, which is a different fact from partition 0 offset 0.
  std::int32_t broker_partition = -1;
  std::int64_t broker_offset = -1;

  // Which resend of this identity this attempt is: 0 for the first, 1 for the
  // first application-level retry, and so on. Any value above 0 means the run
  // can produce duplicates on Kafka and cannot claim exactly-once (E-07, E-08).
  std::uint32_t resend_ordinal = 0;

  std::chrono::steady_clock::time_point created_at{};
  std::chrono::steady_clock::time_point queued_lib_at{};
  std::chrono::steady_clock::time_point terminal_at{};

  // Seconds from librdkafka enqueue to the delivery report, measured on the
  // steady clock. Section 7.1 forbids deriving this from event_time_ns, which
  // in a replay is a historical timestamp and would produce nonsense latencies.
  double ack_latency_seconds = -1.0;

  bool terminal() const { return IsTerminal(state); }
};

// Every counter the run report and the conservation equations need. Grouped in
// one struct so a report cannot accidentally mix counters from two snapshots.
struct LedgerCounters {
  std::uint64_t upstream_accepted = 0;       // offered to P04 by the adapter
  std::uint64_t policy_suppressed = 0;       // gate refused it
  std::uint64_t route_rejected = 0;          // contract/route validation refused it
  std::uint64_t app_reserve_rejected = 0;    // bounded handoff refused it
  std::uint64_t queued_app = 0;              // budget reserved, attempt created
  std::uint64_t enqueue_failed = 0;          // produce call refused it
  std::uint64_t queued_lib = 0;              // handed to librdkafka
  std::uint64_t acked = 0;
  std::uint64_t failed_definite = 0;
  std::uint64_t unknown = 0;
  std::uint64_t audit_lane_attempts = 0;

  std::uint64_t double_finalize_refused = 0;
  std::uint64_t unknown_attempt_reports = 0;  // delivery report for an unknown id
  std::uint64_t partition_mismatch = 0;       // broker partition != expected
  std::uint64_t app_level_resends = 0;        // attempts with resend_ordinal > 0

  // Derived, not stored: attempts handed to the library with no terminal state
  // yet. This is the number that makes a run INCOMPLETE at the drain deadline.
  std::uint64_t pending_in_library = 0;
  std::uint64_t pending_in_app = 0;
};

class PublishLedger {
 public:
  PublishLedger();

  // Counting offers. Called once per event the adapter hands over, before any
  // decision, so the conservation equations have a left-hand side.
  void RecordUpstreamAccepted();
  void RecordPolicySuppressed();
  void RecordRouteRejected();
  void RecordAppReserveRejected();
  void RecordAuditLaneAttempt();

  // Creates an attempt in state kQueuedApp: the budget is already reserved by
  // the time this is called, which is what makes "queued in P04" true.
  std::uint64_t CreateAttempt(const EventIdentity& identity, EventKind kind,
                              const std::string& topic, const std::string& key,
                              std::int32_t expected_partition,
                              std::size_t value_bytes, std::size_t reserved_bytes);

  void MarkQueuedLibrary(std::uint64_t attempt_id);

  // Terminal transitions. Each refuses to act on an already-terminal attempt
  // and counts the refusal instead.
  void MarkEnqueueFailed(std::uint64_t attempt_id, ErrorClass error_class,
                         const std::string& detail);
  void MarkAcked(std::uint64_t attempt_id, std::int32_t partition, std::int64_t offset);
  void MarkFailedDefinite(std::uint64_t attempt_id, ErrorClass error_class,
                          const std::string& detail);
  void MarkUnknown(std::uint64_t attempt_id, ErrorClass error_class,
                   const std::string& detail);

  // At the drain deadline, everything still pending becomes kUnknown with
  // DELIVERY_UNKNOWN. Returns how many attempts were converted and, in
  // converted_ids, which ones.
  //
  // This is the honest ending for NFR-10: an attempt with no delivery report is
  // not a success and not a proven failure, so the run is INCOMPLETE and the
  // exit code is non-zero.
  //
  // The caller needs the ids because a converted attempt still holds a
  // reservation in the bounded handoff: its delivery report never arrived, so
  // nothing released it. Leaving them held would break the invariant that a
  // terminal attempt owns no budget, and the final app_queue_records in the
  // report would be non-zero for records that are no longer pending.
  std::uint64_t FinalizePendingAsUnknown(const std::string& reason,
                                         std::vector<std::uint64_t>* converted_ids);

  LedgerCounters Counters() const;

  // Section 10.4 conservation equations. Returns false and fills error with the
  // first equation that does not balance.
  //
  // Checked at a quiesced cut only: calling it mid-flight will legitimately
  // show pending counts, which the equations account for, but a concurrent
  // mutation while it reads would not be meaningful.
  bool CheckConservation(std::string* error) const;

  // Attempts whose identity was seen more than once. Non-empty means this run
  // can have produced duplicate records and must not claim exactly-once.
  std::vector<EventIdentity> ResentIdentities() const;

  // Writes one JSON object per attempt, for the reconciliation step. Returns
  // false if the file cannot be written, because a report that silently lacks
  // its ledger is worse than a failed run.
  bool WriteJsonl(const std::string& path) const;

  // Snapshot of one attempt, for tests and diagnostics.
  bool Lookup(std::uint64_t attempt_id, AttemptRecord* out) const;

  std::size_t attempt_count() const;

 private:
  // Returns nullptr when the id is unknown or already terminal, incrementing
  // the matching counter. Caller must hold mutex_.
  AttemptRecord* MutableForFinalizeLocked(std::uint64_t attempt_id);

  mutable std::mutex mutex_;
  std::uint64_t next_attempt_id_ = 1;
  // Ordered so the JSONL dump is in attempt order, which makes two runs of the
  // same fixture diffable.
  std::map<std::uint64_t, AttemptRecord> attempts_;
  std::unordered_map<EventIdentity, std::uint32_t, EventIdentityHash> identity_seen_;
  LedgerCounters counters_;
};

}  // namespace p04

#endif  // P04_LEDGER_PUBLISH_STATUS_H_
