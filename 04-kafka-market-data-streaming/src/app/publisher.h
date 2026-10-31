// The publish pipeline: the executable form of the pseudo-flow in section 4.3.
//
// Offer() is the whole contract in one function:
//
//   gate -> validate and route -> serialize -> reserve budget -> produce
//
// in that order, with a ledger entry created only once the budget is reserved,
// so "queued in P04" is true at the moment the ledger says so. Poll() serves
// delivery reports and is the only place an attempt becomes ACKED. Drain()
// bounds the wait and converts whatever is left into UNKNOWN rather than
// hoping.
//
// The ordering of the steps is not arbitrary. Serializing before reserving
// means the reservation can use the real byte count instead of an estimate,
// which is what makes the 8 MiB cap in NFR-03 a real cap. Reserving before
// producing means a full queue is refused before librdkafka is asked to hold
// anything, which is what keeps the two queue tiers independent (NFR-04).
#ifndef P04_APP_PUBLISHER_H_
#define P04_APP_PUBLISHER_H_

#include <cstdint>
#include <string>

#include "ledger/publish_status.h"
#include "ops/metrics.h"
#include "policy/publish_gate.h"
#include "producer/kafka_transport.h"
#include "queue/bounded_handoff.h"
#include "routing/route_table.h"

namespace p04 {

// What happened to one offered event. Distinct values rather than a bool,
// because the caller behaves differently for each: backpressure means pause,
// a contract failure means stop if configured to, and a suppression means
// carry on.
enum class OfferResult {
  kAcceptedForAsyncDelivery,
  kPolicySuppressed,
  kRoutedToAuditLane,
  kRouteRejected,
  kBackpressured,     // queue full now; retry after draining
  kBackpressureTimeout,  // waited past the stall deadline: run is INCOMPLETE
  kEnqueueFailed,
  kFatal,             // producer unusable; stop the run
};

const char* ToString(OfferResult result);

struct PublisherConfig {
  // Blocking reserve, so a single-owner input path stalls instead of spinning.
  // A test that wants to observe kBackpressured rather than a stall sets this
  // to false.
  bool block_on_backpressure = true;
  // Publish each accepted event twice at the application level. Off by default;
  // when on, the run report says so, because such a run produces duplicates by
  // construction and can never claim exactly-once (case E-07).
  bool duplicate_every_event = false;
  bool stop_on_contract_mismatch = true;
};

struct DrainResult {
  int outstanding_in_library = 0;
  std::uint64_t finalized_as_unknown = 0;
  bool complete = false;
};

class Publisher {
 public:
  // None of the pointers may be null and all must outlive the publisher. Passed
  // as raw pointers rather than owned, because the run report, the metrics and
  // the ledger are also read by the shutdown path and by the harness.
  Publisher(KafkaTransport* transport, RouteTable* routes, PublishGate* gate,
            BoundedHandoff* handoff, PublishLedger* ledger, Metrics* metrics,
            PublisherConfig config);

  // Installs the delivery callback on the transport. Must be called before the
  // first Offer; kept separate from the constructor so a test can wrap the
  // callback.
  void Install();

  OfferResult Offer(const OutputEvent& event);

  int Poll(int timeout_ms);

  // Flushes and then finalises anything still pending as UNKNOWN. complete is
  // true only when nothing was left over, which is the only case that may
  // produce a zero exit code (NFR-10).
  DrainResult Drain(int deadline_ms);

  // Last contract-level error, for the run report. Non-empty means a P0 was
  // observed, such as the broker reporting a partition the pinned partitioner
  // did not choose.
  const std::string& contract_error() const { return contract_error_; }
  bool contract_violated() const { return !contract_error_.empty(); }

 private:
  void OnDelivery(const DeliveryReport& report);
  OfferResult PublishOnce(const OutputEvent& event, const Route& route);

  KafkaTransport* transport_;
  RouteTable* routes_;
  PublishGate* gate_;
  BoundedHandoff* handoff_;
  PublishLedger* ledger_;
  Metrics* metrics_;
  PublisherConfig config_;
  std::string contract_error_;
};

}  // namespace p04

#endif  // P04_APP_PUBLISHER_H_
