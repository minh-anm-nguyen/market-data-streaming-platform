#include "producer/kafka_transport.h"

namespace p04 {

const char* ToString(EnqueueOutcome outcome) {
  switch (outcome) {
    case EnqueueOutcome::kQueued:          return "QUEUED";
    case EnqueueOutcome::kQueueFull:       return "QUEUE_FULL";
    case EnqueueOutcome::kMessageTooLarge: return "MESSAGE_TOO_LARGE";
    case EnqueueOutcome::kUnknownTopic:    return "UNKNOWN_TOPIC";
    case EnqueueOutcome::kFatalError:      return "FATAL_ERROR";
    case EnqueueOutcome::kInvalidArgument: return "INVALID_ARGUMENT";
    case EnqueueOutcome::kTransportError:  return "TRANSPORT_ERROR";
  }
  return "UNMAPPED";
}

ErrorClass ErrorClassFor(EnqueueOutcome outcome) {
  switch (outcome) {
    case EnqueueOutcome::kQueued:
      return ErrorClass::kNone;
    case EnqueueOutcome::kQueueFull:
      return ErrorClass::kBackpressure;
    // A record the broker would refuse for its size, or a topic that does not
    // exist, is a contract problem on our side rather than a transport blip.
    // Classifying them as transient would send the publisher into a retry loop
    // against a condition that cannot improve.
    case EnqueueOutcome::kMessageTooLarge:
    case EnqueueOutcome::kInvalidArgument:
      return ErrorClass::kContractError;
    case EnqueueOutcome::kUnknownTopic:
      return ErrorClass::kRoutingError;
    case EnqueueOutcome::kTransportError:
      return ErrorClass::kTransientTransport;
    case EnqueueOutcome::kFatalError:
      return ErrorClass::kDurabilityUnavailable;
  }
  return ErrorClass::kTransientTransport;
}

const char* ToString(DeliveryStatus status) {
  switch (status) {
    case DeliveryStatus::kAcked:          return "ACKED";
    case DeliveryStatus::kFailedDefinite: return "FAILED_DEFINITE";
    case DeliveryStatus::kUnknown:        return "UNKNOWN";
  }
  return "UNMAPPED";
}

}  // namespace p04
