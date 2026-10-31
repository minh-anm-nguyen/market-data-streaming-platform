// A deterministic, fault-injectable stand-in for librdkafka plus a broker.
//
// It exists so the unit and property suites can drive every state transition in
// section 4.1 and every invariant in section 10.4 with no Docker, no network
// and no timing flakiness. It is not a Kafka implementation and it never claims
// to be: a test that passes here and is not also run against a real broker is
// reported as such, never as a PASS for a case whose oracle requires a broker.
//
// What it models faithfully, because the tests would be worthless otherwise:
//
//   * Delivery is asynchronous. Produce never reports delivery; reports are
//     only served from Poll or Flush. A fake that called the callback inline
//     would hide the single most common real bug, treating produce success as
//     an acknowledgement.
//   * Partition assignment uses the same pinned partitioner as production, so
//     per-key ordering assertions mean the same thing here and on a broker.
//   * Offsets are per topic-partition and strictly increasing in append order.
//   * The ambiguous outcome is reachable: kAckDroppedAfterAppend appends the
//     record to the fake log and still reports kUnknown, which is the only way
//     to test that the reconciliation path treats UNKNOWN as "look at the log"
//     rather than "lost" (cases E-06, F-06).
//   * Idempotence is scoped the way Kafka scopes it: the fake appends once per
//     Produce call, so an application-level resend produces two records, which
//     is what cases E-07 and E-08 require to be visible.
#ifndef P04_PRODUCER_FAKE_TRANSPORT_H_
#define P04_PRODUCER_FAKE_TRANSPORT_H_

#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <string>
#include <vector>

#include "producer/kafka_transport.h"

namespace p04 {

// How the fake should treat one particular produce call. Selected by the
// 1-based ordinal of the call, so a fault plan is reproducible without any
// randomness: "fail the 7th record" means the same thing on every run.
enum class FakeFault {
  kNone,
  kQueueFull,             // client queue refuses it
  kMessageTooLarge,
  kUnknownTopic,
  kFailBeforeAppend,      // provably not appended: kFailedDefinite
  kAckDroppedAfterAppend, // appended, acknowledgement lost: kUnknown
  kTimeoutNoAppend,       // timed out, nothing appended, still kUnknown
  kFatal,                 // producer becomes unusable
  kNeverReport,           // no delivery report at all, to test drain deadlines
};

struct FakeTransportConfig {
  std::int32_t partition_count = 8;
  std::size_t queue_max_records = 100000;
  std::size_t queue_max_bytes = 64u * 1024u * 1024u;

  // The pinned producer policy, mirrored so ValidateEffectiveConfig can refuse
  // the same combinations the real client refuses (cases E-01 to E-04).
  bool enable_idempotence = true;
  std::string acks = "all";
  int max_in_flight_requests_per_connection = 5;
  long retries = 2147483647;
  int message_timeout_ms = 30000;

  std::size_t max_message_bytes = 1048576;

  // Fault plan, keyed by the 1-based produce ordinal.
  std::map<std::uint64_t, FakeFault> faults;

  // When true, Produce refuses everything with kQueueFull. Used to drive the
  // backpressure path for as long as a test wants, which a per-ordinal plan
  // cannot express.
  bool stall_all = false;
};

struct FakeStoredRecord {
  std::string topic;
  std::int32_t partition = -1;
  std::int64_t offset = -1;
  std::string key;
  std::string value;
  std::vector<RecordHeader> headers;
  std::uint64_t attempt_id = 0;
  // True when the append happened but the producer was told kUnknown. The
  // honest reconciliation story depends on these existing: the ledger says
  // UNKNOWN, the log says present, and only an independent read can tell.
  bool ack_withheld = false;
};

class FakeTransport : public KafkaTransport {
 public:
  explicit FakeTransport(FakeTransportConfig config);

  bool ValidateEffectiveConfig(std::string* error) override;
  EnqueueOutcome Produce(const Route& route, std::uint64_t attempt_id,
                         std::string* error_detail) override;
  int Poll(int timeout_ms) override;
  int Flush(int timeout_ms) override;
  void SetDeliveryCallback(DeliveryCallback callback) override;
  TransportStats Stats() const override;
  std::string ClientVersion() const override;

  // Inspection for tests. The fake log is the in-process equivalent of reading
  // the topic back, and the snapshot verifier logic is tested against it before
  // it is pointed at a real cluster.
  const std::vector<FakeStoredRecord>& log() const { return log_; }
  std::vector<FakeStoredRecord> RecordsFor(const std::string& topic,
                                           std::int32_t partition) const;
  std::size_t pending_reports() const { return pending_.size(); }
  std::uint64_t produce_calls() const { return produce_calls_; }

  void InjectFault(std::uint64_t ordinal, FakeFault fault);
  void SetStallAll(bool stall);

 private:
  FakeFault FaultFor(std::uint64_t ordinal) const;
  void Append(const Route& route, std::uint64_t attempt_id, bool ack_withheld,
              std::int32_t* partition_out, std::int64_t* offset_out);

  FakeTransportConfig config_;
  DeliveryCallback callback_;
  std::uint64_t produce_calls_ = 0;
  std::size_t queue_records_ = 0;
  std::size_t queue_bytes_ = 0;
  bool fatal_ = false;
  std::string fatal_detail_;

  std::vector<FakeStoredRecord> log_;
  std::map<std::pair<std::string, std::int32_t>, std::int64_t> next_offset_;

  // Reports waiting for a Poll, with the bytes to release so queue accounting
  // mirrors the real client, where a record occupies the queue until its report
  // is served.
  struct PendingReport {
    DeliveryReport report;
    std::size_t bytes = 0;
  };
  std::deque<PendingReport> pending_;

  TransportStats stats_{};
};

}  // namespace p04

#endif  // P04_PRODUCER_FAKE_TRANSPORT_H_
