#include "ledger/publish_status.h"

#include <fstream>
#include <sstream>

namespace p04 {
namespace {

// Minimal JSON string escaping. Symbols and dataset ids are already restricted
// to a safe alphabet by the route validator, but error details carry free text
// from librdkafka, and an unescaped quote there would produce a ledger file the
// reconciliation tool cannot parse.
std::string JsonEscape(const std::string& in) {
  std::string out;
  out.reserve(in.size() + 8);
  for (const char c : in) {
    switch (c) {
      case '"':  out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          static const char* kHex = "0123456789abcdef";
          out += "\\u00";
          out += kHex[(static_cast<unsigned char>(c) >> 4) & 0x0f];
          out += kHex[static_cast<unsigned char>(c) & 0x0f];
        } else {
          out += c;
        }
    }
  }
  return out;
}

}  // namespace

PublishLedger::PublishLedger() = default;

void PublishLedger::RecordUpstreamAccepted() {
  std::lock_guard<std::mutex> lock(mutex_);
  ++counters_.upstream_accepted;
}

void PublishLedger::RecordPolicySuppressed() {
  std::lock_guard<std::mutex> lock(mutex_);
  ++counters_.policy_suppressed;
}

void PublishLedger::RecordRouteRejected() {
  std::lock_guard<std::mutex> lock(mutex_);
  ++counters_.route_rejected;
}

void PublishLedger::RecordAppReserveRejected() {
  std::lock_guard<std::mutex> lock(mutex_);
  ++counters_.app_reserve_rejected;
}

void PublishLedger::RecordAuditLaneAttempt() {
  std::lock_guard<std::mutex> lock(mutex_);
  ++counters_.audit_lane_attempts;
}

std::uint64_t PublishLedger::CreateAttempt(const EventIdentity& identity,
                                           EventKind kind, const std::string& topic,
                                           const std::string& key,
                                           std::int32_t expected_partition,
                                           std::size_t value_bytes,
                                           std::size_t reserved_bytes) {
  std::lock_guard<std::mutex> lock(mutex_);
  AttemptRecord record;
  record.attempt_id = next_attempt_id_++;
  record.identity = identity;
  record.kind = kind;
  record.topic = topic;
  record.key = key;
  record.expected_partition = expected_partition;
  record.value_bytes = value_bytes;
  record.reserved_bytes = reserved_bytes;
  record.state = AttemptState::kQueuedApp;
  record.created_at = std::chrono::steady_clock::now();

  // The resend ordinal is what turns "we might have duplicated something" into
  // a number. It counts how many previous attempts carried this identity, so
  // the first attempt is 0 and anything above 0 is a flag on the whole run.
  auto& seen = identity_seen_[identity];
  record.resend_ordinal = seen;
  ++seen;
  if (record.resend_ordinal > 0) ++counters_.app_level_resends;

  ++counters_.queued_app;
  const std::uint64_t id = record.attempt_id;
  attempts_.emplace(id, std::move(record));
  return id;
}

void PublishLedger::MarkQueuedLibrary(std::uint64_t attempt_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = attempts_.find(attempt_id);
  if (it == attempts_.end()) {
    ++counters_.unknown_attempt_reports;
    return;
  }
  if (it->second.state != AttemptState::kQueuedApp) {
    // Only an app-queued attempt can become library-queued. Anything else means
    // the producer called this out of order, which would corrupt the counters.
    ++counters_.double_finalize_refused;
    return;
  }
  it->second.state = AttemptState::kQueuedLib;
  it->second.queued_lib_at = std::chrono::steady_clock::now();
  ++counters_.queued_lib;
}

AttemptRecord* PublishLedger::MutableForFinalizeLocked(std::uint64_t attempt_id) {
  const auto it = attempts_.find(attempt_id);
  if (it == attempts_.end()) {
    ++counters_.unknown_attempt_reports;
    return nullptr;
  }
  if (it->second.terminal()) {
    // Refusing rather than overwriting. A second delivery report for the same
    // attempt is a client or shutdown bug; letting it overwrite would turn an
    // UNKNOWN into an ACKED and fabricate durability.
    ++counters_.double_finalize_refused;
    return nullptr;
  }
  return &it->second;
}

void PublishLedger::MarkEnqueueFailed(std::uint64_t attempt_id,
                                      ErrorClass error_class,
                                      const std::string& detail) {
  std::lock_guard<std::mutex> lock(mutex_);
  AttemptRecord* record = MutableForFinalizeLocked(attempt_id);
  if (record == nullptr) return;
  record->state = AttemptState::kEnqueueFailed;
  record->error_class = error_class;
  record->error_detail = detail;
  record->terminal_at = std::chrono::steady_clock::now();
  ++counters_.enqueue_failed;
}

void PublishLedger::MarkAcked(std::uint64_t attempt_id, std::int32_t partition,
                              std::int64_t offset) {
  std::lock_guard<std::mutex> lock(mutex_);
  AttemptRecord* record = MutableForFinalizeLocked(attempt_id);
  if (record == nullptr) return;
  record->state = AttemptState::kAcked;
  record->broker_partition = partition;
  record->broker_offset = offset;
  record->terminal_at = std::chrono::steady_clock::now();
  if (record->queued_lib_at.time_since_epoch().count() != 0) {
    record->ack_latency_seconds =
        std::chrono::duration<double>(record->terminal_at - record->queued_lib_at).count();
  }
  // The broker is the authority on where the record landed. If it disagrees with
  // the pinned partitioner, the per-key ordering claim in the report would be
  // false, so the mismatch is counted here and promoted to a P0 by the caller.
  if (record->expected_partition >= 0 && partition >= 0 &&
      record->expected_partition != partition) {
    ++counters_.partition_mismatch;
  }
  ++counters_.acked;
}

void PublishLedger::MarkFailedDefinite(std::uint64_t attempt_id,
                                       ErrorClass error_class,
                                       const std::string& detail) {
  std::lock_guard<std::mutex> lock(mutex_);
  AttemptRecord* record = MutableForFinalizeLocked(attempt_id);
  if (record == nullptr) return;
  record->state = AttemptState::kFailedDefinite;
  record->error_class = error_class;
  record->error_detail = detail;
  record->terminal_at = std::chrono::steady_clock::now();
  ++counters_.failed_definite;
}

void PublishLedger::MarkUnknown(std::uint64_t attempt_id, ErrorClass error_class,
                                const std::string& detail) {
  std::lock_guard<std::mutex> lock(mutex_);
  AttemptRecord* record = MutableForFinalizeLocked(attempt_id);
  if (record == nullptr) return;
  record->state = AttemptState::kUnknown;
  record->error_class = error_class;
  record->error_detail = detail;
  record->terminal_at = std::chrono::steady_clock::now();
  ++counters_.unknown;
}

std::uint64_t PublishLedger::FinalizePendingAsUnknown(
    const std::string& reason, std::vector<std::uint64_t>* converted_ids) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::uint64_t converted = 0;
  const auto now = std::chrono::steady_clock::now();
  for (auto& [id, record] : attempts_) {
    if (record.terminal()) continue;
    record.state = AttemptState::kUnknown;
    record.error_class = ErrorClass::kDeliveryUnknown;
    record.error_detail = reason;
    record.terminal_at = now;
    ++counters_.unknown;
    ++converted;
    if (converted_ids != nullptr) converted_ids->push_back(id);
  }
  return converted;
}

LedgerCounters PublishLedger::Counters() const {
  std::lock_guard<std::mutex> lock(mutex_);
  LedgerCounters out = counters_;
  out.pending_in_library = 0;
  out.pending_in_app = 0;
  for (const auto& [id, record] : attempts_) {
    if (record.state == AttemptState::kQueuedLib) ++out.pending_in_library;
    if (record.state == AttemptState::kQueuedApp) ++out.pending_in_app;
  }
  return out;
}

bool PublishLedger::CheckConservation(std::string* error) const {
  const LedgerCounters c = Counters();
  const auto fail = [&](const std::string& message) {
    if (error != nullptr) *error = message;
    return false;
  };

  // accepted_offer = policy_suppressed + app_enqueue_rejected + app_queue_enqueued
  //
  // route_rejected is part of "enqueue rejected" here: the event was offered,
  // refused before the queue, and therefore never queued. Keeping it as its own
  // counter rather than folding it into policy_suppressed matters because the
  // two have different owners, the gate and the route validator.
  const std::uint64_t offered_accounted = c.policy_suppressed + c.route_rejected +
                                          c.app_reserve_rejected + c.queued_app;
  if (c.upstream_accepted != offered_accounted) {
    return fail("upstream_accepted(" + std::to_string(c.upstream_accepted) +
                ") != policy_suppressed + route_rejected + app_reserve_rejected + queued_app(" +
                std::to_string(offered_accounted) + ")");
  }

  // app_queue_enqueued = lib_queue_failed + lib_queue_enqueued + still_in_app_queue
  const std::uint64_t app_accounted =
      c.enqueue_failed + c.queued_lib + c.pending_in_app;
  if (c.queued_app != app_accounted) {
    return fail("queued_app(" + std::to_string(c.queued_app) +
                ") != enqueue_failed + queued_lib + pending_in_app(" +
                std::to_string(app_accounted) + ")");
  }

  // lib_queue_enqueued = ack + error_or_unknown + still_in_lib_queue
  const std::uint64_t lib_accounted =
      c.acked + c.failed_definite + c.unknown + c.pending_in_library;
  if (c.queued_lib != lib_accounted) {
    return fail("queued_lib(" + std::to_string(c.queued_lib) +
                ") != acked + failed_definite + unknown + pending_in_library(" +
                std::to_string(lib_accounted) + ")");
  }

  if (error != nullptr) error->clear();
  return true;
}

std::vector<EventIdentity> PublishLedger::ResentIdentities() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<EventIdentity> out;
  for (const auto& [identity, count] : identity_seen_) {
    if (count > 1) out.push_back(identity);
  }
  return out;
}

bool PublishLedger::Lookup(std::uint64_t attempt_id, AttemptRecord* out) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = attempts_.find(attempt_id);
  if (it == attempts_.end()) return false;
  *out = it->second;
  return true;
}

std::size_t PublishLedger::attempt_count() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return attempts_.size();
}

bool PublishLedger::WriteJsonl(const std::string& path) const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) return false;
  for (const auto& [id, r] : attempts_) {
    std::ostringstream line;
    line << "{\"attempt_id\":" << r.attempt_id
         << ",\"dataset_id\":\"" << JsonEscape(r.identity.dataset_id) << "\""
         << ",\"symbol\":\"" << JsonEscape(r.identity.symbol) << "\""
         << ",\"sequence\":" << r.identity.sequence
         << ",\"kind\":\"" << p04::ToString(r.kind) << "\""
         << ",\"topic\":\"" << JsonEscape(r.topic) << "\""
         << ",\"key\":\"" << JsonEscape(r.key) << "\""
         << ",\"expected_partition\":" << r.expected_partition
         << ",\"value_bytes\":" << r.value_bytes
         << ",\"state\":\"" << p04::ToString(r.state) << "\""
         << ",\"error_class\":\"" << p04::ToString(r.error_class) << "\""
         << ",\"error_detail\":\"" << JsonEscape(r.error_detail) << "\""
         << ",\"broker_partition\":" << r.broker_partition
         << ",\"broker_offset\":" << r.broker_offset
         << ",\"resend_ordinal\":" << r.resend_ordinal
         << ",\"ack_latency_seconds\":" << r.ack_latency_seconds
         << "}\n";
    out << line.str();
    if (!out) return false;
  }
  out.flush();
  return static_cast<bool>(out);
}

}  // namespace p04
