#include "app/publisher.h"

#include <chrono>

#include "serialization/marketdata_codec.h"

namespace {
// How long one poll inside the backpressure wait blocks for. Short enough that
// the stall deadline stays accurate, long enough that the loop is not a spin.
constexpr int kBackpressurePollMs = 5;
}  // namespace

namespace p04 {

const char* ToString(OfferResult result) {
  switch (result) {
    case OfferResult::kAcceptedForAsyncDelivery: return "ACCEPTED_FOR_ASYNC_DELIVERY";
    case OfferResult::kPolicySuppressed:         return "POLICY_SUPPRESSED";
    case OfferResult::kRoutedToAuditLane:        return "ROUTED_TO_AUDIT_LANE";
    case OfferResult::kRouteRejected:            return "ROUTE_REJECTED";
    case OfferResult::kBackpressured:            return "BACKPRESSURED";
    case OfferResult::kBackpressureTimeout:      return "BACKPRESSURE_TIMEOUT";
    case OfferResult::kEnqueueFailed:            return "ENQUEUE_FAILED";
    case OfferResult::kFatal:                    return "FATAL";
  }
  return "UNMAPPED";
}

Publisher::Publisher(KafkaTransport* transport, RouteTable* routes,
                     PublishGate* gate, BoundedHandoff* handoff,
                     PublishLedger* ledger, Metrics* metrics,
                     PublisherConfig config)
    : transport_(transport),
      routes_(routes),
      gate_(gate),
      handoff_(handoff),
      ledger_(ledger),
      metrics_(metrics),
      config_(config) {}

void Publisher::Install() {
  transport_->SetDeliveryCallback([this](const DeliveryReport& report) {
    OnDelivery(report);
  });
}

void Publisher::OnDelivery(const DeliveryReport& report) {
  // This runs on whichever thread called Poll. It must stay short: no file IO,
  // no blocking, no lock held across a callback into user code (section 4.3).
  switch (report.status) {
    case DeliveryStatus::kAcked: {
      ledger_->MarkAcked(report.attempt_id, report.partition, report.offset);
      AttemptRecord record;
      double latency = -1.0;
      std::int32_t expected = -1;
      if (ledger_->Lookup(report.attempt_id, &record)) {
        latency = record.ack_latency_seconds;
        expected = record.expected_partition;
      }
      metrics_->DeliveryAcked(report.topic, report.partition, latency);
      if (expected >= 0 && report.partition >= 0 && expected != report.partition) {
        // The broker put the record somewhere the pinned partitioner did not
        // choose. Any per-key ordering statement in the report would now be
        // false, so this is recorded as a contract violation rather than a
        // curiosity (cases B-05, B-07).
        metrics_->PartitionMappingMismatch();
        if (contract_error_.empty()) {
          contract_error_ = "partition mapping mismatch for key " + record.key +
                            ": expected " + std::to_string(expected) +
                            " broker reported " + std::to_string(report.partition);
        }
      }
      break;
    }
    case DeliveryStatus::kFailedDefinite:
      ledger_->MarkFailedDefinite(report.attempt_id, report.error_class,
                                  report.error_detail);
      metrics_->DeliveryFailed(report.error_class);
      break;
    case DeliveryStatus::kUnknown:
      ledger_->MarkUnknown(report.attempt_id, report.error_class, report.error_detail);
      metrics_->DeliveryUnknown(report.error_class);
      break;
  }

  if (!handoff_->Release(report.attempt_id) && contract_error_.empty()) {
    contract_error_ = "delivery report for attempt " +
                      std::to_string(report.attempt_id) +
                      " had no outstanding queue reservation";
  }

  const HandoffStats stats = handoff_->Stats();
  metrics_->SetAppQueue(stats.records, stats.bytes, stats.oldest_age_seconds);
  metrics_->AddBackpressureSeconds(stats.backpressure_seconds_total);
}

OfferResult Publisher::PublishOnce(const OutputEvent& event, const Route& route) {
  // The attempt id has to exist before the reservation, because the handoff
  // keys its outstanding set by it in order to report the age of the oldest
  // held record. So the ledger allocates the id first and a failed reservation
  // finalises that attempt immediately rather than leaving it dangling.
  const std::uint64_t attempt_id = ledger_->CreateAttempt(
      event.Identity(), route.kind, route.topic, route.key,
      route.expected_partition, route.value.size(), route.estimated_bytes);

  ReserveResult reserved = handoff_->TryReserve(attempt_id, route.estimated_bytes);
  if (reserved == ReserveResult::kRejectedFull && config_.block_on_backpressure) {
    // Poll while waiting, never block and wait.
    //
    // The budget is only released by a delivery report, and delivery reports
    // are only served from Poll. In a single-owner design, blocking on a
    // condition variable here would stop anyone from polling, so the report
    // that would free the space can never arrive: the wait always runs to the
    // stall deadline and the run stops at exactly max_records events. That is
    // not a broker problem or a backpressure signal, it is a deadlock, and it
    // is what a first integration run against a real broker surfaces
    // immediately.
    //
    // So the wait is an explicit poll loop with the same deadline the blocking
    // reserve would have used. BoundedHandoff::ReserveOrWait still exists for a
    // deployment with a dedicated poll thread (section 2.4 allows either), but
    // this path must not use it.
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(handoff_->config().stall_timeout_ms);
    while (reserved == ReserveResult::kRejectedFull &&
           std::chrono::steady_clock::now() < deadline) {
      transport_->Poll(kBackpressurePollMs);
      reserved = handoff_->TryReserve(attempt_id, route.estimated_bytes);
    }
    if (reserved == ReserveResult::kRejectedFull) {
      reserved = ReserveResult::kStallTimeout;
    }
  }

  if (reserved != ReserveResult::kAccepted) {
    // The attempt never held budget, so it is finalised as an enqueue failure
    // with class BACKPRESSURE. It stays counted under queued_app because the
    // attempt exists; RecordAppReserveRejected is deliberately not called here,
    // or the event would be counted twice on the left-hand side of the first
    // conservation equation. That counter is for offers refused before any
    // attempt was created.
    ledger_->MarkEnqueueFailed(attempt_id, ErrorClass::kBackpressure,
                               ToString(reserved));
    metrics_->ProduceEnqueueFailed(ErrorClass::kBackpressure);
    if (reserved == ReserveResult::kStallTimeout) {
      metrics_->BackpressureTimeout();
      return OfferResult::kBackpressureTimeout;
    }
    return OfferResult::kBackpressured;
  }

  std::string detail;
  const EnqueueOutcome outcome = transport_->Produce(route, attempt_id, &detail);
  if (outcome != EnqueueOutcome::kQueued) {
    handoff_->Release(attempt_id);
    const ErrorClass error_class = ErrorClassFor(outcome);
    ledger_->MarkEnqueueFailed(attempt_id, error_class,
                               std::string(ToString(outcome)) + ": " + detail);
    metrics_->ProduceEnqueueFailed(error_class);
    if (outcome == EnqueueOutcome::kFatalError) {
      metrics_->CaughtFatalError(error_class);
      if (contract_error_.empty()) contract_error_ = "producer fatal error: " + detail;
      return OfferResult::kFatal;
    }
    if (outcome == EnqueueOutcome::kQueueFull) return OfferResult::kBackpressured;
    return OfferResult::kEnqueueFailed;
  }

  // Queued in the client. Explicitly NOT delivered: the ledger state says
  // QUEUED_LIB and only a delivery report can move it on (FR-07).
  ledger_->MarkQueuedLibrary(attempt_id);
  metrics_->ProduceEnqueued(route.topic);

  const HandoffStats stats = handoff_->Stats();
  metrics_->SetAppQueue(stats.records, stats.bytes, stats.oldest_age_seconds);
  const TransportStats transport_stats = transport_->Stats();
  metrics_->SetLibQueue(transport_stats.queue_records, transport_stats.queue_bytes);

  return OfferResult::kAcceptedForAsyncDelivery;
}

OfferResult Publisher::Offer(const OutputEvent& event) {
  ledger_->RecordUpstreamAccepted();
  metrics_->UpstreamAccepted(event.event.kind.has_value() &&
                             *event.event.kind == EventKind::kQuote);

  const GateDecision decision = gate_->Decide(event);
  if (decision.lane == PublishLane::kDropped) {
    ledger_->RecordPolicySuppressed();
    metrics_->PolicySuppressed(static_cast<std::size_t>(decision.reason));
    return OfferResult::kPolicySuppressed;
  }
  if (decision.lane == PublishLane::kAuditLane) {
    // The audit lane is a separate contract with its own schema (section 3.5).
    // Project 04 counts the routing decision and does not pretend to have
    // written anything: writing two topics is not atomic, and claiming an audit
    // commit off the back of a market ACK is exactly what section 3.5 forbids.
    ledger_->RecordPolicySuppressed();
    ledger_->RecordAuditLaneAttempt();
    metrics_->PolicySuppressed(static_cast<std::size_t>(decision.reason));
    return OfferResult::kRoutedToAuditLane;
  }

  std::string encoded;
  CodecError codec_error = CodecError::kNone;
  if (!Encode(event.event, &encoded, &codec_error)) {
    ledger_->RecordRouteRejected();
    metrics_->RouteInvalid(static_cast<std::size_t>(RouteReject::kNone));
    if (config_.stop_on_contract_mismatch && contract_error_.empty()) {
      contract_error_ = std::string("serialization failed: ") + ToString(codec_error);
    }
    return OfferResult::kRouteRejected;
  }

  RouteReject reason = RouteReject::kNone;
  std::string detail;
  const auto route = routes_->Build(event, encoded, &reason, &detail);
  if (!route.has_value()) {
    ledger_->RecordRouteRejected();
    metrics_->RouteInvalid(static_cast<std::size_t>(reason));
    return OfferResult::kRouteRejected;
  }

  const OfferResult first = PublishOnce(event, *route);
  if (first != OfferResult::kAcceptedForAsyncDelivery) return first;

  if (config_.duplicate_every_event) {
    // A second, independent attempt for the same identity. The ledger records
    // resend_ordinal 1, which is the signal that this run can contain
    // duplicates on Kafka and must not claim exactly-once (cases E-07, E-08).
    const OfferResult second = PublishOnce(event, *route);
    if (second == OfferResult::kFatal) return second;
  }
  return OfferResult::kAcceptedForAsyncDelivery;
}

int Publisher::Poll(int timeout_ms) { return transport_->Poll(timeout_ms); }

DrainResult Publisher::Drain(int deadline_ms) {
  DrainResult result;
  result.outstanding_in_library = transport_->Flush(deadline_ms);
  // Flush may return with records still held, which is the normal outcome when
  // the broker is unreachable. Serve whatever reports did arrive before
  // finalising the rest.
  transport_->Poll(0);

  const LedgerCounters before = ledger_->Counters();
  if (before.pending_in_library > 0 || before.pending_in_app > 0 ||
      result.outstanding_in_library > 0) {
    std::vector<std::uint64_t> converted;
    result.finalized_as_unknown = ledger_->FinalizePendingAsUnknown(
        "drain deadline expired with no terminal delivery report", &converted);
    // A converted attempt is terminal in the ledger but its delivery report
    // never arrived, so nothing released its queue reservation. Releasing them
    // here keeps the invariant that a terminal attempt owns no budget, which is
    // also what makes the final app_queue_records in the report meaningful.
    for (const std::uint64_t attempt_id : converted) handoff_->Release(attempt_id);
  }
  const LedgerCounters after = ledger_->Counters();
  result.complete = after.pending_in_library == 0 && after.pending_in_app == 0 &&
                    result.finalized_as_unknown == 0 &&
                    result.outstanding_in_library == 0;
  return result;
}

}  // namespace p04
