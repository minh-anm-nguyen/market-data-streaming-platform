#include "ops/topic_guard.h"

#include <librdkafka/rdkafka.h>

#include <cstring>

namespace p04 {
namespace {

// Pulls the three topic configs that carry durability meaning. DescribeConfigs
// rather than metadata, because metadata reports partitions and replicas but
// not min.insync.replicas, cleanup.policy or unclean.leader.election.enable,
// and those three are exactly what separates a durable topic from a topic that
// looks durable.
bool DescribeTopicConfigs(rd_kafka_t* handle, const std::string& topic,
                          TopicMetadataReport* report, int timeout_ms,
                          std::string* error) {
  rd_kafka_ConfigResource_t* resource =
      rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_TOPIC, topic.c_str());
  if (resource == nullptr) {
    *error = "cannot allocate a ConfigResource for " + topic;
    return false;
  }

  rd_kafka_AdminOptions_t* options =
      rd_kafka_AdminOptions_new(handle, RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS);
  char errstr[512];
  rd_kafka_AdminOptions_set_request_timeout(options, timeout_ms, errstr, sizeof(errstr));

  rd_kafka_queue_t* queue = rd_kafka_queue_new(handle);
  rd_kafka_DescribeConfigs(handle, &resource, 1, options, queue);

  rd_kafka_event_t* event = rd_kafka_queue_poll(queue, timeout_ms + 1000);
  bool ok = false;
  if (event == nullptr) {
    *error = "DescribeConfigs timed out for " + topic;
  } else if (rd_kafka_event_error(event) != RD_KAFKA_RESP_ERR_NO_ERROR) {
    *error = std::string("DescribeConfigs failed for ") + topic + ": " +
             rd_kafka_event_error_string(event);
  } else {
    const rd_kafka_DescribeConfigs_result_t* result =
        rd_kafka_event_DescribeConfigs_result(event);
    size_t resource_count = 0;
    const rd_kafka_ConfigResource_t** resources =
        rd_kafka_DescribeConfigs_result_resources(result, &resource_count);
    if (resource_count == 0) {
      *error = "DescribeConfigs returned no resources for " + topic;
    } else {
      const rd_kafka_resp_err_t resource_err =
          rd_kafka_ConfigResource_error(resources[0]);
      if (resource_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        // This is the C-09 path: WRITE granted, DESCRIBE denied. The message
        // distinguishes "the topic is wrong" from "we were not allowed to
        // check", because the operator response differs.
        *error = std::string("cannot describe topic ") + topic + ": " +
                 rd_kafka_err2str(resource_err) + " (" +
                 (rd_kafka_ConfigResource_error_string(resources[0]) != nullptr
                      ? rd_kafka_ConfigResource_error_string(resources[0])
                      : "no detail") +
                 ")";
      } else {
        size_t config_count = 0;
        const rd_kafka_ConfigEntry_t** entries =
            rd_kafka_ConfigResource_configs(resources[0], &config_count);
        for (size_t i = 0; i < config_count; ++i) {
          const char* name = rd_kafka_ConfigEntry_name(entries[i]);
          const char* value = rd_kafka_ConfigEntry_value(entries[i]);
          if (name == nullptr || value == nullptr) continue;
          if (std::strcmp(name, "min.insync.replicas") == 0) {
            report->min_insync_replicas =
                static_cast<std::int32_t>(std::strtol(value, nullptr, 10));
          } else if (std::strcmp(name, "cleanup.policy") == 0) {
            report->cleanup_policy = value;
          } else if (std::strcmp(name, "unclean.leader.election.enable") == 0) {
            report->unclean_leader_election = (std::strcmp(value, "true") == 0);
          }
        }
        ok = true;
      }
    }
  }

  if (event != nullptr) rd_kafka_event_destroy(event);
  rd_kafka_queue_destroy(queue);
  rd_kafka_AdminOptions_destroy(options);
  rd_kafka_ConfigResource_destroy(resource);
  return ok;
}

}  // namespace

TopicGuardResult VerifyTopics(rd_kafka_t* handle,
                              const std::vector<std::string>& topics,
                              const ExpectedTopicPolicy& expected,
                              const std::string& profile, int timeout_ms) {
  TopicGuardResult result;
  if (handle == nullptr) {
    result.error = "no producer handle";
    return result;
  }

  const rd_kafka_metadata_t* metadata = nullptr;
  // all_topics=0 with a null topic handle still returns the full cluster
  // metadata for the topics the client knows about; the per-topic lookup below
  // is what detects a missing topic, since auto-creation is off on both the
  // client and the broker.
  const rd_kafka_resp_err_t err =
      rd_kafka_metadata(handle, 1, nullptr, &metadata, timeout_ms);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR || metadata == nullptr) {
    result.error = std::string("metadata request failed: ") + rd_kafka_err2str(err);
    return result;
  }

  result.broker_count = metadata->broker_cnt;
  const char* cluster_id = rd_kafka_clusterid(handle, timeout_ms);
  if (cluster_id != nullptr) {
    result.cluster_id = cluster_id;
    rd_kafka_mem_free(handle, const_cast<char*>(cluster_id));
  }

  for (const std::string& topic : topics) {
    if (topic.empty()) continue;
    const rd_kafka_metadata_topic_t* found = nullptr;
    for (int i = 0; i < metadata->topic_cnt; ++i) {
      if (topic == metadata->topics[i].topic) {
        found = &metadata->topics[i];
        break;
      }
    }
    TopicMetadataReport report;
    if (found == nullptr) {
      result.error = "topic " + topic +
                     " does not exist; auto-creation is refused, so provision it "
                     "with deploy/topic-create.sh first (FR-13, case C-01)";
      result.metadata[topic] = report;
      rd_kafka_metadata_destroy(metadata);
      return result;
    }
    if (found->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      result.error = "topic " + topic + " metadata error: " + rd_kafka_err2str(found->err);
      result.metadata[topic] = report;
      rd_kafka_metadata_destroy(metadata);
      return result;
    }

    report.partitions = found->partition_cnt;
    // Replication factor is per partition in Kafka. Taking the minimum across
    // partitions rather than partition 0: a topic where one partition lost a
    // replica is not RF=3 any more, and reporting the first partition would
    // hide that.
    std::int32_t min_replicas = -1;
    for (int p = 0; p < found->partition_cnt; ++p) {
      const std::int32_t replicas = found->partitions[p].replica_cnt;
      if (min_replicas < 0 || replicas < min_replicas) min_replicas = replicas;
    }
    report.replication_factor = min_replicas;

    std::string describe_error;
    if (!DescribeTopicConfigs(handle, topic, &report, timeout_ms, &describe_error)) {
      result.error = describe_error +
                     " -- the topic contract cannot be verified, so the run stops "
                     "rather than publishing unverified (case C-09)";
      result.metadata[topic] = report;
      rd_kafka_metadata_destroy(metadata);
      return result;
    }
    result.metadata[topic] = report;
  }
  rd_kafka_metadata_destroy(metadata);

  // Now compare against the pinned expectations. Done after collecting
  // everything so the report shows all observed values even when the first
  // topic already fails.
  for (const auto& [topic, report] : result.metadata) {
    if (!report.partitions.has_value() || *report.partitions != expected.partitions) {
      result.error = "topic " + topic + " has " +
                     (report.partitions ? std::to_string(*report.partitions) : "unknown") +
                     " partitions, expected " + std::to_string(expected.partitions) +
                     "; the key-to-partition mapping is only stable at the pinned "
                     "count (FR-04, case C-02)";
      return result;
    }
    if (!report.replication_factor.has_value() ||
        *report.replication_factor < expected.replication_factor) {
      result.error = "topic " + topic + " replication factor is " +
                     (report.replication_factor ? std::to_string(*report.replication_factor)
                                                : "unknown") +
                     ", expected at least " + std::to_string(expected.replication_factor) +
                     " (case C-04)";
      return result;
    }
    if (!report.min_insync_replicas.has_value() ||
        *report.min_insync_replicas < expected.min_insync_replicas) {
      result.error = "topic " + topic + " min.insync.replicas is " +
                     (report.min_insync_replicas
                          ? std::to_string(*report.min_insync_replicas)
                          : "unknown") +
                     ", expected at least " + std::to_string(expected.min_insync_replicas) +
                     " (case C-05)";
      return result;
    }
    // Compaction deletes older records with the same key, and the key here is
    // the symbol, so compaction would erase tick history. Section 3.2 forbids
    // it for event logs outright.
    if (report.cleanup_policy != expected.cleanup_policy) {
      result.error = "topic " + topic + " cleanup.policy is " + report.cleanup_policy +
                     ", expected " + expected.cleanup_policy +
                     "; compaction on a keyed event log destroys history (case C-03)";
      return result;
    }
    if (report.unclean_leader_election.has_value() &&
        *report.unclean_leader_election != expected.unclean_leader_election) {
      result.error = "topic " + topic + " unclean.leader.election.enable is " +
                     (*report.unclean_leader_election ? "true" : "false") +
                     ", expected " + (expected.unclean_leader_election ? "true" : "false") +
                     "; unclean election can lose acknowledged records";
      return result;
    }
  }

  // A reliability-lab verdict needs enough brokers to lose one. Three replicas
  // configured on a single broker is not a thing Kafka allows, but a profile
  // mismatch is worth catching explicitly rather than inferring from a later
  // failure (NFR-06).
  if (profile == "reliability-lab" && result.broker_count < 3) {
    result.error = "profile reliability-lab needs at least 3 brokers, cluster "
                   "reports " + std::to_string(result.broker_count) +
                   "; an HA verdict on fewer is not supportable";
    return result;
  }

  result.ok = true;
  return result;
}

bool PartitionCountsUnchanged(rd_kafka_t* handle,
                              const std::vector<std::string>& topics,
                              std::int32_t expected_partitions, int timeout_ms,
                              std::string* error) {
  const rd_kafka_metadata_t* metadata = nullptr;
  const rd_kafka_resp_err_t err =
      rd_kafka_metadata(handle, 1, nullptr, &metadata, timeout_ms);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR || metadata == nullptr) {
    // A failed drift check is not a drift finding. Saying so matters: treating
    // an unreachable broker as "the topic changed" would stop runs for the
    // wrong reason.
    *error = std::string("drift check could not reach the cluster: ") +
             rd_kafka_err2str(err);
    return true;
  }
  bool unchanged = true;
  for (const std::string& topic : topics) {
    if (topic.empty()) continue;
    for (int i = 0; i < metadata->topic_cnt; ++i) {
      if (topic != metadata->topics[i].topic) continue;
      if (metadata->topics[i].partition_cnt != expected_partitions) {
        *error = "topic " + topic + " now reports " +
                 std::to_string(metadata->topics[i].partition_cnt) +
                 " partitions, pinned at " + std::to_string(expected_partitions) +
                 "; the key mapping has changed under this run (case B-05)";
        unchanged = false;
      }
    }
  }
  rd_kafka_metadata_destroy(metadata);
  return unchanged;
}

}  // namespace p04
