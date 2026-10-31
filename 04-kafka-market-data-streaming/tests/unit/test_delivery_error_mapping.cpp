// The delivery-report error mapping (section 4.2, cases E-06, E-10, E-11,
// F-01 to F-05, G-*, H-*).
//
// This is the single most consequential table in the project. Section 4.2 says
// a delivery timeout may mean the broker appended and the acknowledgement was
// lost, so labelling it NOT_WRITTEN is a false claim. The tests below pin three
// properties:
//
//   1. Only errors that are provably pre-append map to FAILED_DEFINITE.
//   2. Everything ambiguous maps to UNKNOWN, including the AFTER_APPEND variant
//      of NotEnoughReplicas, whose name is the clue.
//   3. An error code this build does not know about maps to UNKNOWN. That is
//      tested by sweeping the whole librdkafka error range, so a future client
//      release cannot quietly turn an unmapped code into a false "not written".
#include <gtest/gtest.h>
#include <librdkafka/rdkafka.h>

#include <set>
#include <vector>

#include "producer/rdkafka_producer.h"

namespace p04 {
namespace {

DeliveryStatus Map(rd_kafka_resp_err_t err, ErrorClass* out = nullptr) {
  ErrorClass sink = ErrorClass::kNone;
  return RdKafkaProducer::MapDeliveryError(static_cast<int>(err),
                                           out != nullptr ? out : &sink);
}

TEST(DeliveryErrorMapping, NoErrorIsAcked) {
  ErrorClass error_class = ErrorClass::kContractError;
  EXPECT_EQ(Map(RD_KAFKA_RESP_ERR_NO_ERROR, &error_class), DeliveryStatus::kAcked);
  EXPECT_EQ(error_class, ErrorClass::kNone);
}

// The complete list of codes allowed to be definite. If a code is added here it
// has to be justifiable as pre-append, and the sweep test below guarantees
// nothing else sneaks in.
TEST(DeliveryErrorMapping, OnlyProvablyPreAppendErrorsAreDefinite) {
  const std::vector<rd_kafka_resp_err_t> definite = {
      RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE,
      RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE,
      RD_KAFKA_RESP_ERR_INVALID_RECORD,
      RD_KAFKA_RESP_ERR__INVALID_ARG,
      RD_KAFKA_RESP_ERR__KEY_SERIALIZATION,
      RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION,
      RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC,
      RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION,
      RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
      RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
      RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED,
      RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED,
      RD_KAFKA_RESP_ERR__AUTHENTICATION,
      RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS,
      RD_KAFKA_RESP_ERR__PURGE_QUEUE,
  };
  for (const auto err : definite) {
    EXPECT_EQ(Map(err), DeliveryStatus::kFailedDefinite)
        << rd_kafka_err2name(err) << " should be a definite failure";
  }
}

// Case E-06 and the heart of section 4.2.
TEST(DeliveryErrorMapping, TimeoutsAreAmbiguousNotFailures) {
  for (const auto err : {RD_KAFKA_RESP_ERR__MSG_TIMED_OUT,
                         RD_KAFKA_RESP_ERR__TIMED_OUT,
                         RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE,
                         RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT}) {
    ErrorClass error_class = ErrorClass::kNone;
    EXPECT_EQ(Map(err, &error_class), DeliveryStatus::kUnknown)
        << rd_kafka_err2name(err) << " must not be reported as not-written";
    EXPECT_EQ(error_class, ErrorClass::kDeliveryUnknown) << rd_kafka_err2name(err);
  }
}

// The pair that is easiest to get wrong: one is definite, the other is not, and
// the only difference is the word AFTER_APPEND.
TEST(DeliveryErrorMapping, NotEnoughReplicasBeforeAndAfterAppendDiffer) {
  EXPECT_EQ(Map(RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS),
            DeliveryStatus::kFailedDefinite);
  EXPECT_EQ(Map(RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND),
            DeliveryStatus::kUnknown)
      << "the record was appended; reporting it as not written would be false";

  ErrorClass error_class = ErrorClass::kNone;
  Map(RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND, &error_class);
  EXPECT_EQ(error_class, ErrorClass::kDurabilityUnavailable);
}

// Purge from the queue is provable; purge while in flight is not.
TEST(DeliveryErrorMapping, PurgeQueueAndPurgeInflightDiffer) {
  EXPECT_EQ(Map(RD_KAFKA_RESP_ERR__PURGE_QUEUE), DeliveryStatus::kFailedDefinite);
  EXPECT_EQ(Map(RD_KAFKA_RESP_ERR__PURGE_INFLIGHT), DeliveryStatus::kUnknown);
}

TEST(DeliveryErrorMapping, TransportFailuresAreAmbiguous) {
  for (const auto err : {RD_KAFKA_RESP_ERR__TRANSPORT,
                         RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN,
                         RD_KAFKA_RESP_ERR__DESTROY}) {
    ErrorClass error_class = ErrorClass::kNone;
    EXPECT_EQ(Map(err, &error_class), DeliveryStatus::kUnknown) << rd_kafka_err2name(err);
    EXPECT_EQ(error_class, ErrorClass::kTransientTransport) << rd_kafka_err2name(err);
  }
}

// Case E-12: idempotence bookkeeping failures leave the producer unable to say
// what the broker holds.
TEST(DeliveryErrorMapping, IdempotenceFailuresAreAmbiguous) {
  for (const auto err : {RD_KAFKA_RESP_ERR_OUT_OF_ORDER_SEQUENCE_NUMBER,
                         RD_KAFKA_RESP_ERR_DUPLICATE_SEQUENCE_NUMBER,
                         RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH,
                         RD_KAFKA_RESP_ERR_UNKNOWN_PRODUCER_ID,
                         RD_KAFKA_RESP_ERR__FENCED}) {
    EXPECT_EQ(Map(err), DeliveryStatus::kUnknown) << rd_kafka_err2name(err);
  }
}

TEST(DeliveryErrorMapping, SecurityFailuresAreDefiniteAndClassified) {
  ErrorClass error_class = ErrorClass::kNone;
  EXPECT_EQ(Map(RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED, &error_class),
            DeliveryStatus::kFailedDefinite);
  EXPECT_EQ(error_class, ErrorClass::kSecurityError);
  EXPECT_EQ(SeverityOf(ErrorClass::kSecurityError), Severity::kP0);
}

// The property that protects future librdkafka upgrades: sweep every code in
// the librdkafka range and assert that anything not on the definite list is
// UNKNOWN. A new code added by a client release therefore defaults to the safe
// answer instead of silently claiming a record was never written.
TEST(DeliveryErrorMapping, UnmappedCodesDefaultToUnknown) {
  const std::set<int> definite = {
      RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE,
      RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE,
      RD_KAFKA_RESP_ERR_INVALID_RECORD,
      RD_KAFKA_RESP_ERR__INVALID_ARG,
      RD_KAFKA_RESP_ERR__KEY_SERIALIZATION,
      RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION,
      RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC,
      RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION,
      RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
      RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
      RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED,
      RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED,
      RD_KAFKA_RESP_ERR__AUTHENTICATION,
      RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS,
      RD_KAFKA_RESP_ERR__PURGE_QUEUE,
  };
  int checked = 0;
  // The internal (negative) range and the protocol (non-negative) range.
  for (int code = RD_KAFKA_RESP_ERR__BEGIN; code < RD_KAFKA_RESP_ERR__END; ++code) {
    if (code == RD_KAFKA_RESP_ERR_NO_ERROR) continue;
    ++checked;
    const DeliveryStatus status = Map(static_cast<rd_kafka_resp_err_t>(code));
    if (definite.count(code) > 0) {
      EXPECT_EQ(status, DeliveryStatus::kFailedDefinite) << code;
    } else {
      EXPECT_EQ(status, DeliveryStatus::kUnknown)
          << "code " << code << " (" << rd_kafka_err2name(static_cast<rd_kafka_resp_err_t>(code))
          << ") must default to UNKNOWN, never to a definite failure";
    }
  }
  for (int code = 0; code < RD_KAFKA_RESP_ERR_END_ALL; ++code) {
    if (code == RD_KAFKA_RESP_ERR_NO_ERROR) continue;
    ++checked;
    const DeliveryStatus status = Map(static_cast<rd_kafka_resp_err_t>(code));
    if (definite.count(code) > 0) {
      EXPECT_EQ(status, DeliveryStatus::kFailedDefinite) << code;
    } else {
      EXPECT_EQ(status, DeliveryStatus::kUnknown)
          << "code " << code << " (" << rd_kafka_err2name(static_cast<rd_kafka_resp_err_t>(code))
          << ") must default to UNKNOWN, never to a definite failure";
    }
  }
  EXPECT_GT(checked, 100) << "the sweep should cover the whole error space";
}

// A definite failure is never also broker-confirmed, and only ACKED is.
TEST(AttemptState, OnlyAckedIsBrokerConfirmed) {
  EXPECT_TRUE(IsBrokerConfirmed(AttemptState::kAcked));
  for (const auto state : {AttemptState::kUnknown, AttemptState::kFailedDefinite,
                           AttemptState::kEnqueueFailed, AttemptState::kQueuedLib,
                           AttemptState::kQueuedApp, AttemptState::kCreated,
                           AttemptState::kPolicySuppressed,
                           AttemptState::kRouteRejected}) {
    EXPECT_FALSE(IsBrokerConfirmed(state)) << ToString(state);
  }
}

TEST(AttemptState, PendingStatesAreNotTerminal) {
  EXPECT_FALSE(IsTerminal(AttemptState::kCreated));
  EXPECT_FALSE(IsTerminal(AttemptState::kQueuedApp));
  EXPECT_FALSE(IsTerminal(AttemptState::kQueuedLib));
  EXPECT_TRUE(IsTerminal(AttemptState::kAcked));
  EXPECT_TRUE(IsTerminal(AttemptState::kUnknown));
  EXPECT_TRUE(IsTerminal(AttemptState::kFailedDefinite));
  EXPECT_TRUE(IsTerminal(AttemptState::kEnqueueFailed));
}

// DELIVERY_UNKNOWN is a P0, not a nuisance: an unresolved outcome is a hole in
// the completeness claim (section 8.3).
TEST(ErrorTaxonomy, SeverityAssignment) {
  EXPECT_EQ(SeverityOf(ErrorClass::kDeliveryUnknown), Severity::kP0);
  EXPECT_EQ(SeverityOf(ErrorClass::kContractError), Severity::kP0);
  EXPECT_EQ(SeverityOf(ErrorClass::kRoutingError), Severity::kP0);
  EXPECT_EQ(SeverityOf(ErrorClass::kRetentionDataLoss), Severity::kP0);
  EXPECT_EQ(SeverityOf(ErrorClass::kSecurityError), Severity::kP0);
  EXPECT_EQ(SeverityOf(ErrorClass::kBackpressure), Severity::kP1);
  EXPECT_EQ(SeverityOf(ErrorClass::kDurabilityUnavailable), Severity::kP1);
  EXPECT_EQ(SeverityOf(ErrorClass::kPolicySuppressed), Severity::kInfo);
}

TEST(ErrorTaxonomy, EveryClassHasADistinctName) {
  std::set<std::string> names;
  for (std::size_t i = 0; i < kErrorClassCount; ++i) {
    const std::string name = ToString(static_cast<ErrorClass>(i));
    EXPECT_NE(name, "UNMAPPED") << "index " << i;
    names.insert(name);
  }
  EXPECT_EQ(names.size(), kErrorClassCount);
}

}  // namespace
}  // namespace p04
