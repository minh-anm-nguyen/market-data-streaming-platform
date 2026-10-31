// Admin-side topic verification before a single record is published
// (FR-04, FR-13, NFR-06, cases C-01 to C-05, C-09, B-05).
//
// The rule is fail closed. Section 2.3: no auto-create, no silent repair. If
// the topic does not exist, has the wrong partition count, the wrong
// replication factor, min.insync.replicas below the profile, compaction turned
// on, or unclean leader election enabled, the run stops before publishing
// rather than producing data whose ordering and durability claims would be
// false.
//
// Case C-09 is the subtle one: an ACL that grants WRITE but denies DESCRIBE
// leaves the publisher unable to verify the contract. That is not a reason to
// publish anyway. Verification failure and verification-impossible both stop
// the run, and the error says which one happened.
#ifndef P04_OPS_TOPIC_GUARD_H_
#define P04_OPS_TOPIC_GUARD_H_

#include <map>
#include <string>
#include <vector>

#include "ops/config.h"
#include "ops/run_manifest.h"

struct rd_kafka_s;

namespace p04 {

struct TopicGuardResult {
  bool ok = false;
  // Why it failed, phrased so the operator knows whether the topic is wrong or
  // whether we were not allowed to look.
  std::string error;
  // What was actually observed, for the run report. Populated even on failure
  // when the metadata call succeeded, because a report that records only the
  // expectation is useless for diagnosis.
  std::map<std::string, TopicMetadataReport> metadata;
  // The cluster id and broker count, used to refuse an HA verdict on a
  // single-broker cluster (NFR-06).
  std::string cluster_id;
  int broker_count = 0;
};

// Queries metadata and topic configs for the given topics and compares them to
// the pinned expectations. timeout_ms bounds the whole check: a broker that
// never answers is a failure, not a reason to wait forever (case C-06).
TopicGuardResult VerifyTopics(rd_kafka_s* handle,
                              const std::vector<std::string>& topics,
                              const ExpectedTopicPolicy& expected,
                              const std::string& profile, int timeout_ms);

// Re-reads partition counts only, for the periodic drift check during a run.
// Case C-10 and case B-05 both require drift to be noticed within a bounded
// window rather than at the end.
bool PartitionCountsUnchanged(rd_kafka_s* handle,
                              const std::vector<std::string>& topics,
                              std::int32_t expected_partitions, int timeout_ms,
                              std::string* error);

}  // namespace p04

#endif  // P04_OPS_TOPIC_GUARD_H_
