// The error taxonomy from section 8.2, plus the publish-attempt terminal states
// from section 4.1.
//
// These are not log strings. They are the vocabulary that the run report, the
// metric labels and the GO/NO-GO gate all share, so a class can be counted
// without parsing prose. Section 8.1 caps metric cardinality by requiring the
// label set to be a closed enum, which is exactly what this header provides.
//
// The most important distinction in this file is kFailedDefinite against
// kUnknown. Section 4.2 is blunt about it: a delivery timeout may mean the
// broker appended and the acknowledgement was lost, so labelling it
// NOT_WRITTEN is a false claim. Only errors that are provably pre-append are
// definite failures.
#ifndef P04_OPS_ERROR_CLASS_H_
#define P04_OPS_ERROR_CLASS_H_

#include <cstddef>

namespace p04 {

enum class ErrorClass {
  kNone,
  kContractError,         // wrong oneof/key/header/value or schema mismatch
  kRoutingError,          // unknown topic, partition drift, unsupported event
  kBackpressure,          // app queue or producer queue full
  kTransientTransport,    // broker unreachable, metadata timeout
  kDurabilityUnavailable, // ISR below min.insync.replicas, disk failure
  kDeliveryUnknown,       // timeout, or callback never arrived before a crash
  kConsumerCoordination,  // rebalance, revoked partition, fencing
  kRetentionDataLoss,     // offset out of range and the source is gone
  kSecurityError,         // ACL deny, bad credentials, unexpected listener
  kCapacityExceeded,      // CPU/RAM/disk/network budget exceeded
  kPolicySuppressed,      // not an error: upstream classification blocked it
};

const char* ToString(ErrorClass error_class);

// Severity drives the runbook in section 8.3. P0 means a correctness or
// durability claim is at risk and publishing must stop; it is not a synonym for
// "noisy".
enum class Severity { kP0, kP1, kP2, kInfo };

const char* ToString(Severity severity);

Severity SeverityOf(ErrorClass error_class);

// Terminal and non-terminal states of one publish attempt, from the state
// diagram in section 4.1.
enum class AttemptState {
  kCreated,         // ledger entry exists, nothing reserved yet
  kQueuedApp,       // accepted into the bounded handoff queue
  kQueuedLib,       // handed to librdkafka; NOT delivered
  kAcked,           // delivery report success, broker metadata captured
  kFailedDefinite,  // proven not appended
  kUnknown,         // outcome cannot be proven
  kEnqueueFailed,   // produce call refused it; never reached the library queue
  kPolicySuppressed,
  kRouteRejected,
};

const char* ToString(AttemptState state);

// A terminal state is one that will never change again within this run. Pending
// states (kCreated, kQueuedApp, kQueuedLib) are what make a run INCOMPLETE if
// the drain deadline expires (NFR-10).
bool IsTerminal(AttemptState state);

// Whether a state represents a record the broker is known to hold. Only kAcked
// qualifies, and even then section 4.1 warns that an ACK is not proof of
// consumer visibility or of surviving the loss of every replica.
bool IsBrokerConfirmed(AttemptState state);

// Number of ErrorClass members, for fixed-width counter arrays in the metrics
// registry. Kept next to the enum so the two cannot drift apart silently; a
// static_assert in metrics.cpp enforces the relationship.
constexpr std::size_t kErrorClassCount = 12;

}  // namespace p04

#endif  // P04_OPS_ERROR_CLASS_H_
