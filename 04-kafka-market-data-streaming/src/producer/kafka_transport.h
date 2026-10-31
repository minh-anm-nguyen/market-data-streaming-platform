// The producer-side transport boundary.
//
// Two implementations sit behind it: RdKafkaProducer, which talks to a real
// broker, and FakeTransport, which is deterministic and fault-injectable.
//
// The seam exists because the unit and property suites in section 10.1 have to
// assert on state transitions, ack accounting and bounded queues *without* a
// broker, while the integration, reliability and chaos suites assert the same
// invariants *with* one. Sharing the publisher pipeline between them is what
// makes those two sets of results comparable; if the no-broker tests exercised
// a different code path they would prove nothing about the real one.
//
// What the seam must not do is let a test pass that a real broker would fail.
// So FakeTransport never reports a success the real client would not: it models
// asynchronous delivery (a report never arrives inside the produce call), it
// models the ambiguous timeout, and it refuses the same incompatible configs.
#ifndef P04_PRODUCER_KAFKA_TRANSPORT_H_
#define P04_PRODUCER_KAFKA_TRANSPORT_H_

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "ops/error_class.h"
#include "routing/route_table.h"

namespace p04 {

// Outcome of handing a record to the transport. Note what is absent: there is
// no kDelivered. A produce call can only accept or refuse; delivery is reported
// later through the callback. Section 4.1 and FR-07 both turn on this.
enum class EnqueueOutcome {
  kQueued,         // accepted into the client queue; NOT delivered
  kQueueFull,      // client queue is full; apply backpressure
  kMessageTooLarge,
  kUnknownTopic,
  kFatalError,     // producer is unusable; the run must stop
  kInvalidArgument,
  kTransportError,
};

const char* ToString(EnqueueOutcome outcome);
ErrorClass ErrorClassFor(EnqueueOutcome outcome);

// What the broker, or the lack of one, eventually said.
enum class DeliveryStatus {
  kAcked,
  kFailedDefinite,  // provably not appended
  kUnknown,         // may or may not have been appended (section 4.2)
};

const char* ToString(DeliveryStatus status);

struct DeliveryReport {
  std::uint64_t attempt_id = 0;
  DeliveryStatus status = DeliveryStatus::kUnknown;
  std::string topic;
  std::int32_t partition = -1;
  std::int64_t offset = -1;
  ErrorClass error_class = ErrorClass::kNone;
  std::string error_detail;
};

using DeliveryCallback = std::function<void(const DeliveryReport&)>;

// Client-side queue occupancy, for the metrics in section 8.1. Reported by the
// transport because only it knows what its internal queue holds; the app-level
// budget is tracked separately by BoundedHandoff and the two must never be
// conflated (NFR-04).
struct TransportStats {
  std::int64_t queue_records = 0;
  std::int64_t queue_bytes = 0;
  std::int64_t transmitted_records = 0;
  std::int64_t transmission_errors = 0;
  std::int64_t broker_connects = 0;
  std::int64_t broker_disconnects = 0;
  bool fatal_error = false;
  std::string fatal_detail;
};

class KafkaTransport {
 public:
  virtual ~KafkaTransport() = default;

  // Must be called before the first Produce. Returns false and fills error if
  // the effective producer configuration violates the pinned policy; FR-06
  // requires this to fail fast rather than degrade.
  virtual bool ValidateEffectiveConfig(std::string* error) = 0;

  // Hands one record to the client. The route is copied, not referenced: the
  // caller is free to destroy it immediately afterwards. Section 4.3 flags
  // payload and opaque lifetime as a real hazard, and copying removes the
  // hazard rather than documenting it.
  virtual EnqueueOutcome Produce(const Route& route, std::uint64_t attempt_id,
                                 std::string* error_detail) = 0;

  // Serves delivery callbacks. Returns how many were served. Must be called
  // regularly; a producer that never polls accumulates reports and eventually
  // its queue.
  virtual int Poll(int timeout_ms) = 0;

  // Waits for outstanding records to reach a terminal state. Returns the number
  // still outstanding when it gave up, so zero means a complete drain and
  // anything else makes the run INCOMPLETE (NFR-10).
  virtual int Flush(int timeout_ms) = 0;

  virtual void SetDeliveryCallback(DeliveryCallback callback) = 0;

  virtual TransportStats Stats() const = 0;

  // Human-readable client identification for the run report: the linked client
  // version, as the binary actually sees it rather than as the config asked for
  // (NFR-14).
  virtual std::string ClientVersion() const = 0;
};

}  // namespace p04

#endif  // P04_PRODUCER_KAFKA_TRANSPORT_H_
