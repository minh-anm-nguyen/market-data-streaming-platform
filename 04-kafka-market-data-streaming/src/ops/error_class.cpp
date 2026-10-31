#include "ops/error_class.h"

namespace p04 {

const char* ToString(ErrorClass error_class) {
  switch (error_class) {
    case ErrorClass::kNone:                  return "NONE";
    case ErrorClass::kContractError:         return "CONTRACT_ERROR";
    case ErrorClass::kRoutingError:          return "ROUTING_ERROR";
    case ErrorClass::kBackpressure:          return "BACKPRESSURE";
    case ErrorClass::kTransientTransport:    return "TRANSIENT_TRANSPORT";
    case ErrorClass::kDurabilityUnavailable: return "DURABILITY_UNAVAILABLE";
    case ErrorClass::kDeliveryUnknown:       return "DELIVERY_UNKNOWN";
    case ErrorClass::kConsumerCoordination:  return "CONSUMER_COORDINATION";
    case ErrorClass::kRetentionDataLoss:     return "RETENTION_DATA_LOSS";
    case ErrorClass::kSecurityError:         return "SECURITY_ERROR";
    case ErrorClass::kCapacityExceeded:      return "CAPACITY_EXCEEDED";
    case ErrorClass::kPolicySuppressed:      return "POLICY_SUPPRESSED";
  }
  return "UNMAPPED";
}

const char* ToString(Severity severity) {
  switch (severity) {
    case Severity::kP0:   return "P0";
    case Severity::kP1:   return "P1";
    case Severity::kP2:   return "P2";
    case Severity::kInfo: return "INFO";
  }
  return "UNMAPPED";
}

Severity SeverityOf(ErrorClass error_class) {
  switch (error_class) {
    // Anything that can corrupt data, fake durability or bypass an ACL stops the
    // run (section 8.3).
    case ErrorClass::kContractError:
    case ErrorClass::kRoutingError:
    case ErrorClass::kRetentionDataLoss:
    case ErrorClass::kSecurityError:
      return Severity::kP0;
    // DELIVERY_UNKNOWN is P0 as well: an unresolved outcome is a hole in the
    // completeness claim, not a transient annoyance. It is listed separately
    // only to keep the reason why visible at the call site.
    case ErrorClass::kDeliveryUnknown:
      return Severity::kP0;
    case ErrorClass::kBackpressure:
    case ErrorClass::kTransientTransport:
    case ErrorClass::kDurabilityUnavailable:
    case ErrorClass::kConsumerCoordination:
    case ErrorClass::kCapacityExceeded:
      return Severity::kP1;
    case ErrorClass::kPolicySuppressed:
    case ErrorClass::kNone:
      return Severity::kInfo;
  }
  return Severity::kP1;
}

const char* ToString(AttemptState state) {
  switch (state) {
    case AttemptState::kCreated:          return "CREATED";
    case AttemptState::kQueuedApp:        return "QUEUED_APP";
    case AttemptState::kQueuedLib:        return "QUEUED_LIB";
    case AttemptState::kAcked:            return "ACKED";
    case AttemptState::kFailedDefinite:   return "FAILED_DEFINITE";
    case AttemptState::kUnknown:          return "UNKNOWN";
    case AttemptState::kEnqueueFailed:    return "ENQUEUE_FAILED";
    case AttemptState::kPolicySuppressed: return "POLICY_SUPPRESSED";
    case AttemptState::kRouteRejected:    return "ROUTE_REJECTED";
  }
  return "UNMAPPED";
}

bool IsTerminal(AttemptState state) {
  switch (state) {
    case AttemptState::kAcked:
    case AttemptState::kFailedDefinite:
    case AttemptState::kUnknown:
    case AttemptState::kEnqueueFailed:
    case AttemptState::kPolicySuppressed:
    case AttemptState::kRouteRejected:
      return true;
    case AttemptState::kCreated:
    case AttemptState::kQueuedApp:
    case AttemptState::kQueuedLib:
      return false;
  }
  return false;
}

bool IsBrokerConfirmed(AttemptState state) { return state == AttemptState::kAcked; }

}  // namespace p04
