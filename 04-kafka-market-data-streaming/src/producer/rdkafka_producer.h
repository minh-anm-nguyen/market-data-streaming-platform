// The real producer: a RAII wrapper over the librdkafka C API
// (FR-06, FR-07, NFR-04, NFR-11, section 4.2, section 4.3).
//
// The C API is used rather than the C++ one because the C++ wrapper hides the
// headers API and the delivery-report opaque behind its own lifetime rules,
// and section 4.3 calls out payload and opaque lifetime as a genuine hazard.
// Owning it explicitly here is less code than reasoning about it indirectly.
//
// Three things in this class are load-bearing:
//
//   1. Configuration is pinned and then read back. Setting a value is not the
//      same as having it: librdkafka may reject a combination, and a default
//      may differ between releases. FR-06 wants fail-fast on the effective
//      config, so ValidateEffectiveConfig queries the client rather than
//      trusting what we passed in.
//
//   2. The delivery-report error mapping defaults to kUnknown. A new or
//      unrecognised error code must never become kFailedDefinite, because
//      claiming a record was not written when it may have been is the exact
//      falsehood section 4.2 forbids. Only codes that are provably pre-append
//      are mapped to definite failure, and they are listed one by one.
//
//   3. RD_KAFKA_MSG_F_COPY is used for the payload. Zero-copy would be faster
//      and would require the buffer to outlive the delivery report; at the
//      rates in section 7.2 the copy is not the bottleneck, and the class of
//      use-after-free bug it removes is the kind that only shows up under load.
#ifndef P04_PRODUCER_RDKAFKA_PRODUCER_H_
#define P04_PRODUCER_RDKAFKA_PRODUCER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "producer/kafka_transport.h"

struct rd_kafka_s;
struct rd_kafka_conf_s;

namespace p04 {

struct RdKafkaProducerConfig {
  std::string bootstrap_servers = "127.0.0.1:9092";
  std::string client_id = "p04-publisher-01";
  std::string security_protocol = "PLAINTEXT";

  // The pinned producer policy from section 11.2. Every one of these is passed
  // to librdkafka explicitly; none is left to a client default, because a
  // default that changes between releases would change the durability story
  // without anything in the repository changing.
  bool enable_idempotence = true;
  std::string acks = "all";
  int max_in_flight_requests_per_connection = 5;
  std::string retries = "2147483647";
  int message_timeout_ms = 30000;
  int linger_ms = 5;
  int batch_size_bytes = 65536;
  std::string compression_type = "lz4";
  int queue_buffering_max_messages = 100000;
  int queue_buffering_max_kbytes = 65536;  // KiB, so roughly 64 MiB
  std::string partitioner = "murmur2_random";
  int statistics_interval_ms = 1000;
  int socket_timeout_ms = 60000;
  int metadata_max_age_ms = 30000;
  // Bounded reconnect behaviour, so a blackholed broker produces timeouts
  // rather than an endless silent retry (NFR-11).
  int reconnect_backoff_ms = 100;
  int reconnect_backoff_max_ms = 10000;

  // Extra raw key/value pairs, applied last. Used by the negative config tests
  // to inject a deliberately incompatible setting (cases E-01 to E-04).
  std::vector<std::pair<std::string, std::string>> extra;
};

class RdKafkaProducer : public KafkaTransport {
 public:
  // Creates the client. Returns nullptr and fills error if librdkafka refuses
  // the configuration, which is the expected outcome of the negative config
  // tests rather than an exception to be swallowed.
  static std::unique_ptr<RdKafkaProducer> Create(const RdKafkaProducerConfig& config,
                                                 std::string* error);

  ~RdKafkaProducer() override;

  RdKafkaProducer(const RdKafkaProducer&) = delete;
  RdKafkaProducer& operator=(const RdKafkaProducer&) = delete;

  bool ValidateEffectiveConfig(std::string* error) override;
  EnqueueOutcome Produce(const Route& route, std::uint64_t attempt_id,
                         std::string* error_detail) override;
  int Poll(int timeout_ms) override;
  int Flush(int timeout_ms) override;
  void SetDeliveryCallback(DeliveryCallback callback) override;
  TransportStats Stats() const override;
  std::string ClientVersion() const override;

  // Records still held by the client with no delivery report yet. Non-zero at
  // the end of a drain is what makes a run INCOMPLETE.
  int OutstandingCount() const;

  // The fatal-error state from rd_kafka_fatal_error, which for an idempotent
  // producer means the producer has been fenced or lost its sequence state and
  // cannot continue safely.
  bool HasFatalError(std::string* detail) const;

  // The underlying handle, for the topic metadata guard, which needs to issue
  // metadata and Admin requests on the same connection the producer uses.
  rd_kafka_s* handle() const { return handle_; }

  // Exposed for unit testing the mapping table without a broker. This is the
  // single most consequential function in the class, and it is pure, so it is
  // tested directly against the full list of librdkafka error codes rather than
  // only through whatever errors a lab happens to produce.
  static DeliveryStatus MapDeliveryError(int rd_kafka_resp_err, ErrorClass* out_class);

 private:
  RdKafkaProducer() = default;

  static void DeliveryReportTrampoline(rd_kafka_s* rk, const void* message,
                                       void* opaque);
  static void ErrorTrampoline(rd_kafka_s* rk, int err, const char* reason,
                              void* opaque);
  static int StatsTrampoline(rd_kafka_s* rk, char* json, size_t json_len,
                             void* opaque);

  void OnDeliveryReport(const void* message);
  void OnError(int err, const char* reason);
  void OnStats(const char* json, std::size_t length);

  rd_kafka_s* handle_ = nullptr;
  DeliveryCallback callback_;
  mutable std::mutex stats_mutex_;
  TransportStats stats_{};
  std::atomic<bool> fatal_{false};
  std::string fatal_detail_;
  RdKafkaProducerConfig config_;
};

}  // namespace p04

#endif  // P04_PRODUCER_RDKAFKA_PRODUCER_H_
