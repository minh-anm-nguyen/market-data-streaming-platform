#include "producer/fake_transport.h"

#include "routing/partition_policy.h"

namespace p04 {

FakeTransport::FakeTransport(FakeTransportConfig config)
    : config_(std::move(config)) {}

bool FakeTransport::ValidateEffectiveConfig(std::string* error) {
  // Mirrors the policy the real producer enforces (FR-06). The point of
  // duplicating it is that cases E-01 to E-04 are negative tests: they assert
  // the process refuses to start. If only the real client checked, those tests
  // would need a broker, and a unit suite could not cover them at all.
  const auto fail = [&](const std::string& message) {
    if (error != nullptr) *error = message;
    return false;
  };
  if (!config_.enable_idempotence) {
    return fail("enable.idempotence=false violates the pinned producer policy");
  }
  if (config_.acks != "all" && config_.acks != "-1") {
    return fail("acks=" + config_.acks + " is incompatible with required idempotence");
  }
  if (config_.max_in_flight_requests_per_connection > 5 ||
      config_.max_in_flight_requests_per_connection < 1) {
    return fail("max.in.flight.requests.per.connection=" +
                std::to_string(config_.max_in_flight_requests_per_connection) +
                " must be between 1 and 5 with idempotence enabled");
  }
  if (config_.retries <= 0) {
    return fail("retries=" + std::to_string(config_.retries) +
                " violates the pinned producer policy");
  }
  if (config_.message_timeout_ms <= 0) {
    return fail("message.timeout.ms must be positive and finite (NFR-11)");
  }
  if (config_.partition_count <= 0) {
    return fail("partition_count must be positive");
  }
  if (error != nullptr) error->clear();
  return true;
}

FakeFault FakeTransport::FaultFor(std::uint64_t ordinal) const {
  const auto it = config_.faults.find(ordinal);
  return it == config_.faults.end() ? FakeFault::kNone : it->second;
}

void FakeTransport::InjectFault(std::uint64_t ordinal, FakeFault fault) {
  config_.faults[ordinal] = fault;
}

void FakeTransport::SetStallAll(bool stall) { config_.stall_all = stall; }

void FakeTransport::Append(const Route& route, std::uint64_t attempt_id,
                           bool ack_withheld, std::int32_t* partition_out,
                           std::int64_t* offset_out) {
  // The partition comes from the same pinned partitioner the publisher used, so
  // the fake agrees with production about where a key belongs. Picking a
  // partition some other way would make every ordering test here vacuous.
  const std::int32_t partition = SelectPartition(route.key, config_.partition_count);
  const auto key = std::make_pair(route.topic, partition);
  std::int64_t& next = next_offset_[key];
  FakeStoredRecord record;
  record.topic = route.topic;
  record.partition = partition;
  record.offset = next++;
  record.key = route.key;
  record.value = route.value;
  record.headers = route.headers;
  record.attempt_id = attempt_id;
  record.ack_withheld = ack_withheld;
  log_.push_back(std::move(record));
  if (partition_out != nullptr) *partition_out = partition;
  if (offset_out != nullptr) *offset_out = next - 1;
}

EnqueueOutcome FakeTransport::Produce(const Route& route, std::uint64_t attempt_id,
                                     std::string* error_detail) {
  const auto detail = [&](const std::string& message) {
    if (error_detail != nullptr) *error_detail = message;
  };
  if (fatal_) {
    detail("producer is in a fatal state: " + fatal_detail_);
    return EnqueueOutcome::kFatalError;
  }

  const std::uint64_t ordinal = ++produce_calls_;
  const FakeFault fault = config_.stall_all ? FakeFault::kQueueFull : FaultFor(ordinal);

  if (fault == FakeFault::kQueueFull) {
    // Note the ordinal is consumed even when the call is refused, matching the
    // real client where a rejected produce still happened. A fault plan is
    // therefore written against produce attempts, not against successes.
    detail("client queue full");
    return EnqueueOutcome::kQueueFull;
  }
  if (fault == FakeFault::kMessageTooLarge ||
      route.value.size() > config_.max_message_bytes) {
    detail("message of " + std::to_string(route.value.size()) +
           " bytes exceeds max_message_bytes");
    return EnqueueOutcome::kMessageTooLarge;
  }
  if (fault == FakeFault::kUnknownTopic) {
    detail("unknown topic " + route.topic);
    return EnqueueOutcome::kUnknownTopic;
  }
  if (fault == FakeFault::kFatal) {
    fatal_ = true;
    fatal_detail_ = "injected fatal error";
    stats_.fatal_error = true;
    stats_.fatal_detail = fatal_detail_;
    detail(fatal_detail_);
    return EnqueueOutcome::kFatalError;
  }

  const std::size_t bytes = route.key.size() + route.value.size();
  if (queue_records_ + 1 > config_.queue_max_records ||
      queue_bytes_ + bytes > config_.queue_max_bytes) {
    detail("client queue cap reached");
    return EnqueueOutcome::kQueueFull;
  }
  queue_records_ += 1;
  queue_bytes_ += bytes;

  PendingReport pending;
  pending.bytes = bytes;
  pending.report.attempt_id = attempt_id;
  pending.report.topic = route.topic;

  switch (fault) {
    case FakeFault::kFailBeforeAppend:
      // Nothing is appended, and the failure is provable. This is the only
      // delivery failure shape that may be reported as definite.
      pending.report.status = DeliveryStatus::kFailedDefinite;
      pending.report.error_class = ErrorClass::kDurabilityUnavailable;
      pending.report.error_detail = "injected pre-append failure";
      ++stats_.transmission_errors;
      break;

    case FakeFault::kAckDroppedAfterAppend: {
      // Appended, acknowledgement lost. The log holds the record while the
      // producer is told UNKNOWN, which is precisely the ambiguity section 4.2
      // forbids reporting as NOT_WRITTEN.
      std::int32_t partition = -1;
      std::int64_t offset = -1;
      Append(route, attempt_id, /*ack_withheld=*/true, &partition, &offset);
      pending.report.status = DeliveryStatus::kUnknown;
      pending.report.error_class = ErrorClass::kDeliveryUnknown;
      pending.report.error_detail = "injected ack loss after append";
      // Deliberately not filling partition/offset: the producer did not learn
      // them. Reporting them here would let a test cheat its way to a
      // completeness proof it has not earned.
      ++stats_.transmitted_records;
      break;
    }

    case FakeFault::kTimeoutNoAppend:
      pending.report.status = DeliveryStatus::kUnknown;
      pending.report.error_class = ErrorClass::kDeliveryUnknown;
      pending.report.error_detail = "injected delivery timeout";
      ++stats_.transmission_errors;
      break;

    case FakeFault::kNeverReport:
      // Queue budget is consumed and no report is ever queued, which is what a
      // producer holding records against an unreachable broker looks like. The
      // drain deadline is what resolves it (NFR-10).
      return EnqueueOutcome::kQueued;

    case FakeFault::kNone: {
      std::int32_t partition = -1;
      std::int64_t offset = -1;
      Append(route, attempt_id, /*ack_withheld=*/false, &partition, &offset);
      pending.report.status = DeliveryStatus::kAcked;
      pending.report.partition = partition;
      pending.report.offset = offset;
      ++stats_.transmitted_records;
      break;
    }

    default:
      pending.report.status = DeliveryStatus::kUnknown;
      pending.report.error_class = ErrorClass::kDeliveryUnknown;
      pending.report.error_detail = "unhandled injected fault";
      break;
  }

  pending_.push_back(std::move(pending));
  return EnqueueOutcome::kQueued;
}

int FakeTransport::Poll(int /*timeout_ms*/) {
  // Serves every queued report. The timeout is ignored because the fake has no
  // real waiting to do; tests that need to observe a pending report simply do
  // not call Poll yet, which is a stronger and more deterministic control than
  // a sleep.
  int served = 0;
  while (!pending_.empty()) {
    const PendingReport pending = pending_.front();
    pending_.pop_front();
    queue_records_ = queue_records_ > 0 ? queue_records_ - 1 : 0;
    queue_bytes_ = pending.bytes >= queue_bytes_ ? 0 : queue_bytes_ - pending.bytes;
    if (callback_) callback_(pending.report);
    ++served;
  }
  return served;
}

int FakeTransport::Flush(int timeout_ms) {
  Poll(timeout_ms);
  // Whatever is left is a record with kNeverReport injected: outstanding, with
  // no report coming. Returning the count is how the caller learns the drain
  // was incomplete.
  return static_cast<int>(queue_records_);
}

void FakeTransport::SetDeliveryCallback(DeliveryCallback callback) {
  callback_ = std::move(callback);
}

TransportStats FakeTransport::Stats() const {
  TransportStats out = stats_;
  out.queue_records = static_cast<std::int64_t>(queue_records_);
  out.queue_bytes = static_cast<std::int64_t>(queue_bytes_);
  return out;
}

std::string FakeTransport::ClientVersion() const {
  // Named so no report can mistake a fake run for a real one.
  return "fake-transport/in-process";
}

std::vector<FakeStoredRecord> FakeTransport::RecordsFor(const std::string& topic,
                                                       std::int32_t partition) const {
  std::vector<FakeStoredRecord> out;
  for (const auto& record : log_) {
    if (record.topic == topic && record.partition == partition) out.push_back(record);
  }
  return out;
}

}  // namespace p04
