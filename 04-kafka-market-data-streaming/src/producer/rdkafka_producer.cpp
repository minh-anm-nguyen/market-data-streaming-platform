#include "producer/rdkafka_producer.h"

#include <librdkafka/rdkafka.h>

#include <cstring>
#include <sstream>

namespace p04 {
namespace {

static_assert(sizeof(void*) >= sizeof(std::uint64_t),
              "the attempt id is carried in the librdkafka message opaque "
              "pointer, which requires a 64-bit pointer");

bool SetConf(rd_kafka_conf_t* conf, const char* name, const std::string& value,
             std::string* error) {
  char errstr[512];
  if (rd_kafka_conf_set(conf, name, value.c_str(), errstr, sizeof(errstr)) !=
      RD_KAFKA_CONF_OK) {
    *error = std::string("rd_kafka_conf_set(") + name + "=" + value + "): " + errstr;
    return false;
  }
  return true;
}

// Reads back what the client actually holds. rd_kafka_conf_get on a live
// handle's config is not available, so the check is done against a config
// object queried before rd_kafka_new consumes it, plus the fact that
// rd_kafka_new itself fails on an incompatible combination.
bool GetConf(rd_kafka_conf_t* conf, const char* name, std::string* out) {
  char buffer[512];
  size_t size = sizeof(buffer);
  if (rd_kafka_conf_get(conf, name, buffer, &size) != RD_KAFKA_CONF_OK) return false;
  out->assign(buffer, size > 0 ? size - 1 : 0);
  return true;
}

// Pulls one integer field out of the librdkafka statistics JSON without
// bringing in a JSON parser. The statistics document is large and this needs
// three numbers from it; a full parse on every stats callback would be work on
// a callback that should stay short (section 4.3).
bool ExtractJsonInt(const char* json, std::size_t length, const char* key,
                    std::int64_t* out) {
  const std::string needle = std::string("\"") + key + "\":";
  const std::string_view haystack(json, length);
  const auto pos = haystack.find(needle);
  if (pos == std::string_view::npos) return false;
  std::size_t i = pos + needle.size();
  while (i < length && (json[i] == ' ' || json[i] == '\t')) ++i;
  const std::size_t start = i;
  if (i < length && json[i] == '-') ++i;
  while (i < length && json[i] >= '0' && json[i] <= '9') ++i;
  if (i == start) return false;
  *out = std::strtoll(std::string(haystack.substr(start, i - start)).c_str(), nullptr, 10);
  return true;
}

}  // namespace

DeliveryStatus RdKafkaProducer::MapDeliveryError(int err, ErrorClass* out_class) {
  const auto set = [&](ErrorClass c) { if (out_class != nullptr) *out_class = c; };
  const auto code = static_cast<rd_kafka_resp_err_t>(err);

  switch (code) {
    case RD_KAFKA_RESP_ERR_NO_ERROR:
      set(ErrorClass::kNone);
      return DeliveryStatus::kAcked;

    // ---- Provably pre-append. The record never reached a log. ----
    case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
    case RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE:
    case RD_KAFKA_RESP_ERR_INVALID_RECORD:
    case RD_KAFKA_RESP_ERR__INVALID_ARG:
    case RD_KAFKA_RESP_ERR__KEY_SERIALIZATION:
    case RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION:
      set(ErrorClass::kContractError);
      return DeliveryStatus::kFailedDefinite;

    case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
    case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
    case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
      set(ErrorClass::kRoutingError);
      return DeliveryStatus::kFailedDefinite;

    case RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED:
    case RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED:
    case RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED:
    case RD_KAFKA_RESP_ERR__AUTHENTICATION:
      set(ErrorClass::kSecurityError);
      return DeliveryStatus::kFailedDefinite;

    // The broker refused the write because the in-sync replica set was below
    // min.insync.replicas. It says so *before* appending, which is what makes
    // this definite, unlike the AFTER_APPEND variant below.
    case RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS:
      set(ErrorClass::kDurabilityUnavailable);
      return DeliveryStatus::kFailedDefinite;

    // Purged from the queue before transmission: the client can prove it never
    // went out.
    case RD_KAFKA_RESP_ERR__PURGE_QUEUE:
      set(ErrorClass::kBackpressure);
      return DeliveryStatus::kFailedDefinite;

    // ---- Ambiguous. The broker may hold the record. ----
    // The name says it: the append happened, the replication did not complete.
    // Reporting this as a failure would be a lie in the other direction.
    case RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND:
      set(ErrorClass::kDurabilityUnavailable);
      return DeliveryStatus::kUnknown;

    case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
    case RD_KAFKA_RESP_ERR__TIMED_OUT:
    case RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE:
    case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
      set(ErrorClass::kDeliveryUnknown);
      return DeliveryStatus::kUnknown;

    case RD_KAFKA_RESP_ERR__TRANSPORT:
    case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
    case RD_KAFKA_RESP_ERR__DESTROY:
      set(ErrorClass::kTransientTransport);
      return DeliveryStatus::kUnknown;

    // In flight when the queue was purged: it may have been appended.
    case RD_KAFKA_RESP_ERR__PURGE_INFLIGHT:
      set(ErrorClass::kDeliveryUnknown);
      return DeliveryStatus::kUnknown;

    // Idempotence bookkeeping problems. The producer cannot tell what the
    // broker holds, so neither can this report.
    case RD_KAFKA_RESP_ERR_OUT_OF_ORDER_SEQUENCE_NUMBER:
    case RD_KAFKA_RESP_ERR_DUPLICATE_SEQUENCE_NUMBER:
    case RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH:
    case RD_KAFKA_RESP_ERR_UNKNOWN_PRODUCER_ID:
    case RD_KAFKA_RESP_ERR__FENCED:
      set(ErrorClass::kDeliveryUnknown);
      return DeliveryStatus::kUnknown;

    case RD_KAFKA_RESP_ERR__FATAL:
      set(ErrorClass::kDurabilityUnavailable);
      return DeliveryStatus::kUnknown;

    default:
      // The conservative default, and the reason this function exists. A code
      // this build does not know about is ambiguous, never a definite failure.
      // An unmapped code that defaulted to kFailedDefinite would silently
      // under-report possible duplicates for every future librdkafka release.
      set(ErrorClass::kDeliveryUnknown);
      return DeliveryStatus::kUnknown;
  }
}

void RdKafkaProducer::DeliveryReportTrampoline(rd_kafka_s* /*rk*/,
                                               const void* message, void* opaque) {
  auto* self = static_cast<RdKafkaProducer*>(opaque);
  if (self != nullptr) self->OnDeliveryReport(message);
}

void RdKafkaProducer::ErrorTrampoline(rd_kafka_s* /*rk*/, int err,
                                      const char* reason, void* opaque) {
  auto* self = static_cast<RdKafkaProducer*>(opaque);
  if (self != nullptr) self->OnError(err, reason);
}

int RdKafkaProducer::StatsTrampoline(rd_kafka_s* /*rk*/, char* json,
                                     size_t json_len, void* opaque) {
  auto* self = static_cast<RdKafkaProducer*>(opaque);
  if (self != nullptr) self->OnStats(json, json_len);
  return 0;  // librdkafka frees the buffer
}

void RdKafkaProducer::OnDeliveryReport(const void* message) {
  const auto* msg = static_cast<const rd_kafka_message_t*>(message);
  DeliveryReport report;
  report.attempt_id = static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(msg->_private));
  report.status = MapDeliveryError(static_cast<int>(msg->err), &report.error_class);
  if (msg->rkt != nullptr) report.topic = rd_kafka_topic_name(msg->rkt);
  if (report.status == DeliveryStatus::kAcked) {
    report.partition = msg->partition;
    report.offset = msg->offset;
  } else {
    report.error_detail = rd_kafka_err2str(msg->err);
    // Partition is recorded even on failure when the client knows it, because
    // a failure concentrated on one partition is a different diagnosis from one
    // spread across all of them. Offset is not: there is no offset without an
    // append.
    report.partition = msg->partition;
  }
  if (callback_) callback_(report);
}

void RdKafkaProducer::OnError(int err, const char* reason) {
  const auto code = static_cast<rd_kafka_resp_err_t>(err);
  std::lock_guard<std::mutex> lock(stats_mutex_);
  if (code == RD_KAFKA_RESP_ERR__FATAL) {
    char errstr[512];
    rd_kafka_fatal_error(handle_, errstr, sizeof(errstr));
    fatal_.store(true);
    fatal_detail_ = errstr;
    stats_.fatal_error = true;
    stats_.fatal_detail = fatal_detail_;
    return;
  }
  // Transport-level errors are counted but do not stop the run: librdkafka
  // retries them internally, and the delivery report is what decides each
  // record's fate. Treating an error callback as a per-record failure would
  // double-count, which case E-10 specifically warns about.
  if (code == RD_KAFKA_RESP_ERR__TRANSPORT ||
      code == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
    ++stats_.broker_disconnects;
  }
  if (reason != nullptr && stats_.fatal_detail.empty()) {
    // Keep the most recent non-fatal reason for the report, without letting it
    // masquerade as a fatal state.
  }
}

void RdKafkaProducer::OnStats(const char* json, std::size_t length) {
  std::int64_t msg_cnt = 0;
  std::int64_t msg_size = 0;
  std::int64_t txmsgs = 0;
  std::int64_t tx_errs = 0;
  std::lock_guard<std::mutex> lock(stats_mutex_);
  if (ExtractJsonInt(json, length, "msg_cnt", &msg_cnt)) stats_.queue_records = msg_cnt;
  if (ExtractJsonInt(json, length, "msg_size", &msg_size)) stats_.queue_bytes = msg_size;
  if (ExtractJsonInt(json, length, "txmsgs", &txmsgs)) stats_.transmitted_records = txmsgs;
  if (ExtractJsonInt(json, length, "tx_errs", &tx_errs)) stats_.transmission_errors = tx_errs;
}

std::unique_ptr<RdKafkaProducer> RdKafkaProducer::Create(
    const RdKafkaProducerConfig& config, std::string* error) {
  rd_kafka_conf_t* conf = rd_kafka_conf_new();
  if (conf == nullptr) {
    *error = "rd_kafka_conf_new failed";
    return nullptr;
  }

  const auto cleanup_and_fail = [&]() -> std::unique_ptr<RdKafkaProducer> {
    rd_kafka_conf_destroy(conf);
    return nullptr;
  };

  struct Pair { const char* name; std::string value; };
  const std::vector<Pair> settings = {
      {"bootstrap.servers", config.bootstrap_servers},
      {"client.id", config.client_id},
      {"security.protocol", config.security_protocol},
      {"enable.idempotence", config.enable_idempotence ? "true" : "false"},
      {"acks", config.acks},
      {"max.in.flight.requests.per.connection",
       std::to_string(config.max_in_flight_requests_per_connection)},
      {"retries", config.retries},
      {"message.timeout.ms", std::to_string(config.message_timeout_ms)},
      {"linger.ms", std::to_string(config.linger_ms)},
      {"batch.size", std::to_string(config.batch_size_bytes)},
      {"compression.type", config.compression_type},
      {"queue.buffering.max.messages", std::to_string(config.queue_buffering_max_messages)},
      {"queue.buffering.max.kbytes", std::to_string(config.queue_buffering_max_kbytes)},
      {"partitioner", config.partitioner},
      {"statistics.interval.ms", std::to_string(config.statistics_interval_ms)},
      {"socket.timeout.ms", std::to_string(config.socket_timeout_ms)},
      {"topic.metadata.refresh.interval.ms", std::to_string(config.metadata_max_age_ms)},
      {"reconnect.backoff.ms", std::to_string(config.reconnect_backoff_ms)},
      {"reconnect.backoff.max.ms", std::to_string(config.reconnect_backoff_max_ms)},
      // Topic auto-creation is refused at the client as well as at the broker.
      // FR-13 requires provisioning through the Admin path; relying on only one
      // of the two switches leaves the other as a silent fallback.
      {"allow.auto.create.topics", "false"},
  };
  for (const auto& setting : settings) {
    if (!SetConf(conf, setting.name, setting.value, error)) return cleanup_and_fail();
  }
  for (const auto& [name, value] : config.extra) {
    if (!SetConf(conf, name.c_str(), value, error)) return cleanup_and_fail();
  }

  auto producer = std::unique_ptr<RdKafkaProducer>(new RdKafkaProducer());
  producer->config_ = config;

  // Record the effective values while the config object is still ours to query;
  // rd_kafka_new takes ownership of it.
  std::string effective_idempotence;
  std::string effective_acks;
  std::string effective_in_flight;
  std::string effective_partitioner;
  GetConf(conf, "enable.idempotence", &effective_idempotence);
  GetConf(conf, "acks", &effective_acks);
  GetConf(conf, "max.in.flight.requests.per.connection", &effective_in_flight);
  GetConf(conf, "partitioner", &effective_partitioner);

  rd_kafka_conf_set_opaque(conf, producer.get());
  rd_kafka_conf_set_dr_msg_cb(
      conf, reinterpret_cast<void (*)(rd_kafka_t*, const rd_kafka_message_t*, void*)>(
                &RdKafkaProducer::DeliveryReportTrampoline));
  rd_kafka_conf_set_error_cb(
      conf, reinterpret_cast<void (*)(rd_kafka_t*, int, const char*, void*)>(
                &RdKafkaProducer::ErrorTrampoline));
  rd_kafka_conf_set_stats_cb(
      conf, reinterpret_cast<int (*)(rd_kafka_t*, char*, size_t, void*)>(
                &RdKafkaProducer::StatsTrampoline));

  char errstr[512];
  rd_kafka_t* handle = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if (handle == nullptr) {
    // conf is NOT consumed when rd_kafka_new fails, so it must be destroyed
    // here. This is the documented contract and getting it wrong leaks on every
    // negative config test, which is exactly the path cases E-01 to E-04 take.
    *error = std::string("rd_kafka_new failed: ") + errstr;
    rd_kafka_conf_destroy(conf);
    return nullptr;
  }
  producer->handle_ = handle;

  // Stash the effective values for the report. Pulled from the config object
  // rather than from our own struct so that a librdkafka-side adjustment is
  // what gets recorded.
  producer->config_.acks = effective_acks.empty() ? config.acks : effective_acks;
  producer->config_.enable_idempotence = effective_idempotence != "false";
  if (!effective_in_flight.empty()) {
    producer->config_.max_in_flight_requests_per_connection =
        static_cast<int>(std::strtol(effective_in_flight.c_str(), nullptr, 10));
  }
  if (!effective_partitioner.empty()) producer->config_.partitioner = effective_partitioner;

  return producer;
}

RdKafkaProducer::~RdKafkaProducer() {
  if (handle_ != nullptr) {
    // Purge anything still queued so the delivery callbacks fire with
    // PURGE_QUEUE or PURGE_INFLIGHT instead of being dropped silently. A
    // destructor that discarded them would leave attempts pending forever in
    // the ledger with no explanation.
    rd_kafka_purge(handle_, RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT);
    rd_kafka_flush(handle_, 2000);
    rd_kafka_destroy(handle_);
    handle_ = nullptr;
  }
}

bool RdKafkaProducer::ValidateEffectiveConfig(std::string* error) {
  const auto fail = [&](const std::string& message) {
    if (error != nullptr) *error = message;
    return false;
  };
  if (handle_ == nullptr) return fail("producer handle is null");

  // rd_kafka_new already refuses an incompatible idempotence combination, so
  // reaching here with idempotence off means it was configured off on purpose.
  if (!config_.enable_idempotence) {
    return fail("effective enable.idempotence is false; the pinned policy requires true");
  }
  if (config_.acks != "all" && config_.acks != "-1") {
    return fail("effective acks=" + config_.acks + "; the pinned policy requires all");
  }
  if (config_.max_in_flight_requests_per_connection < 1 ||
      config_.max_in_flight_requests_per_connection > 5) {
    return fail("effective max.in.flight.requests.per.connection=" +
                std::to_string(config_.max_in_flight_requests_per_connection) +
                "; idempotence requires 1..5");
  }
  if (config_.partitioner != "murmur2_random") {
    return fail("effective partitioner=" + config_.partitioner +
                "; the pinned mapping requires murmur2_random (ADR-02)");
  }
  if (config_.message_timeout_ms <= 0) {
    return fail("message.timeout.ms must be positive and finite (NFR-11)");
  }
  if (error != nullptr) error->clear();
  return true;
}

EnqueueOutcome RdKafkaProducer::Produce(const Route& route, std::uint64_t attempt_id,
                                       std::string* error_detail) {
  const auto detail = [&](const std::string& message) {
    if (error_detail != nullptr) *error_detail = message;
  };
  if (fatal_.load()) {
    detail("producer is in a fatal state: " + fatal_detail_);
    return EnqueueOutcome::kFatalError;
  }

  // Headers are built per record and ownership passes to librdkafka on success.
  // On failure this function must destroy them, which is why the error paths
  // below are explicit rather than falling through to a single return.
  rd_kafka_headers_t* headers = nullptr;
  if (!route.headers.empty()) {
    headers = rd_kafka_headers_new(route.headers.size());
    for (const auto& header : route.headers) {
      rd_kafka_header_add(headers, header.key.c_str(), static_cast<ssize_t>(header.key.size()),
                          header.value.data(), static_cast<ssize_t>(header.value.size()));
    }
  }

  // The two calls differ only in whether the headers vtype is present. They
  // cannot be merged with a conditional expression: RD_KAFKA_V_HEADERS and
  // RD_KAFKA_V_END expand to argument sequences of different types, so a
  // ternary between them does not type-check and, more importantly, would not
  // pass the right number of variadic arguments.
  rd_kafka_resp_err_t err;
  if (headers != nullptr) {
    err = rd_kafka_producev(
        handle_,
        RD_KAFKA_V_TOPIC(route.topic.c_str()),
        RD_KAFKA_V_KEY(route.key.data(), route.key.size()),
        RD_KAFKA_V_VALUE(const_cast<char*>(route.value.data()), route.value.size()),
        // F_COPY: librdkafka copies the payload, so route may be destroyed as
        // soon as this returns. See the class comment for why the copy is worth
        // it.
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_OPAQUE(reinterpret_cast<void*>(static_cast<std::uintptr_t>(attempt_id))),
        RD_KAFKA_V_HEADERS(headers),
        RD_KAFKA_V_END);
  } else {
    err = rd_kafka_producev(
        handle_,
        RD_KAFKA_V_TOPIC(route.topic.c_str()),
        RD_KAFKA_V_KEY(route.key.data(), route.key.size()),
        RD_KAFKA_V_VALUE(const_cast<char*>(route.value.data()), route.value.size()),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_OPAQUE(reinterpret_cast<void*>(static_cast<std::uintptr_t>(attempt_id))),
        RD_KAFKA_V_END);
  }

  if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
    return EnqueueOutcome::kQueued;
  }

  // producev only takes ownership of the headers when it succeeds.
  if (headers != nullptr) rd_kafka_headers_destroy(headers);
  detail(rd_kafka_err2str(err));

  switch (err) {
    case RD_KAFKA_RESP_ERR__QUEUE_FULL:
      return EnqueueOutcome::kQueueFull;
    case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
      return EnqueueOutcome::kMessageTooLarge;
    case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
    case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
      return EnqueueOutcome::kUnknownTopic;
    case RD_KAFKA_RESP_ERR__INVALID_ARG:
      return EnqueueOutcome::kInvalidArgument;
    case RD_KAFKA_RESP_ERR__FATAL:
      fatal_.store(true);
      return EnqueueOutcome::kFatalError;
    default:
      return EnqueueOutcome::kTransportError;
  }
}

int RdKafkaProducer::Poll(int timeout_ms) {
  return rd_kafka_poll(handle_, timeout_ms);
}

int RdKafkaProducer::Flush(int timeout_ms) {
  rd_kafka_flush(handle_, timeout_ms);
  // rd_kafka_flush returns TIMED_OUT without saying how much is left, so the
  // outstanding count is queried separately. The number, not the error, is what
  // the run report needs.
  return rd_kafka_outq_len(handle_);
}

void RdKafkaProducer::SetDeliveryCallback(DeliveryCallback callback) {
  callback_ = std::move(callback);
}

TransportStats RdKafkaProducer::Stats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  TransportStats out = stats_;
  if (handle_ != nullptr) {
    // outq_len is authoritative and cheap, unlike the statistics callback which
    // only updates on its interval.
    out.queue_records = rd_kafka_outq_len(handle_);
  }
  return out;
}

std::string RdKafkaProducer::ClientVersion() const {
  return std::string("librdkafka/") + rd_kafka_version_str();
}

int RdKafkaProducer::OutstandingCount() const {
  return handle_ != nullptr ? rd_kafka_outq_len(handle_) : 0;
}

bool RdKafkaProducer::HasFatalError(std::string* detail) const {
  if (!fatal_.load()) return false;
  if (detail != nullptr) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    *detail = fatal_detail_;
  }
  return true;
}

}  // namespace p04
