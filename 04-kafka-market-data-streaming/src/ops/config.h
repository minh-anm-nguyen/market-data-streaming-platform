// The run configuration from section 11.2, and a small TOML reader for it.
//
// Why a hand-written reader: the config shape is flat tables of scalars plus
// one string array, the file is a pinned artifact whose SHA-256 goes into the
// run report, and the requirement is to reject anything unexpected rather than
// to be permissive. A general TOML library would accept more than this contract
// allows and would add a dependency whose version would then also need pinning.
//
// Every unknown key is an error, not a warning. A typo in
// enable_idempotence would otherwise leave the pinned policy silently at its
// default, which is the failure mode cases E-01 to E-04 exist to catch.
#ifndef P04_OPS_CONFIG_H_
#define P04_OPS_CONFIG_H_

#include <cstdint>
#include <string>
#include <vector>

#include "policy/publish_gate.h"
#include "producer/rdkafka_producer.h"
#include "queue/bounded_handoff.h"
#include "routing/route_table.h"

namespace p04 {

// Which job the binary is doing. Separate modes rather than flags, because
// "publish from an offline verified dataset" and "publish from a live adapter"
// have different completeness claims and must not be confusable in a report.
enum class RunMode {
  kOfflineVerifiedPublish,  // from a completed P01 run directory
  kSelfTest,                // no broker: fake transport, for CI smoke
  kTopicVerifyOnly,         // Admin checks only, publishes nothing
};

const char* ToString(RunMode mode);

struct ExpectedTopicPolicy {
  std::int32_t partitions = 8;
  std::int32_t replication_factor = 1;
  std::int32_t min_insync_replicas = 1;
  std::string cleanup_policy = "delete";
  bool unclean_leader_election = false;
};

struct RunConfig {
  // [run]
  std::string run_id = "p04-local-001";
  RunMode mode = RunMode::kOfflineVerifiedPublish;
  std::string source_run_dir;
  std::string proto_descriptor_sha256;  // expected; empty means "record only"
  std::string expected_source_schema = "sim.v1";
  // Which upstream produced the events this run publishes.
  //
  // "p01_mock" is the original path: read a P01 run directory and classify it
  // with MockP03Classifier. It stays the default so every existing test and
  // harness behaves exactly as before.
  //
  // "p03_handoff" reads the real adapter's p04_stub stream, so the
  // classification is Project 03's rather than a re-implementation of it
  // (P09 finding P09-M0-05). source_run_dir is still required in that mode:
  // the P01 report is the independent source manifest every conservation
  // equation is stated against, and the handoff stream is not a substitute
  // for it.
  std::string upstream = "p01_mock";
  std::string p03_handoff_path;
  bool allow_fault_test_into_market_topics = false;
  bool single_active_dataset = true;
  std::string output_dir = "runs";
  // Stop after this many source events; 0 means the whole dataset. Used by the
  // crash-injection harness to cut a run at a known point.
  std::uint64_t max_events = 0;
  // Target offered rate; 0 means as fast as possible. The rate ladder in
  // section 7.2 drives this.
  std::uint64_t target_events_per_second = 0;
  // Publish each event twice, at the application level, to exercise case E-07.
  // Off by default and recorded in the report when on, because a run with this
  // set can produce duplicates by construction.
  bool duplicate_every_event = false;

  // [kafka]
  std::string bootstrap_servers = "127.0.0.1:9092";
  std::string profile = "local-dev";
  std::string ticks_topic = "md.ticks.v1";
  std::string quotes_topic = "md.quotes.v1";
  std::string anomaly_topic;
  ExpectedTopicPolicy expected_topic;
  std::string partitioner = "murmur2_random";
  bool allow_auto_topic_create = false;
  std::string partition_golden_path = "config/topic-partition-golden.json";

  // [producer]
  RdKafkaProducerConfig producer;
  int poll_interval_ms = 10;

  // [handoff]
  HandoffConfig handoff;

  // [contract]
  std::size_t max_kafka_value_bytes = 4096;
  std::size_t max_key_bytes = 32;
  std::size_t max_headers_total_bytes = 512;
  std::size_t max_active_symbols = 256;
  std::size_t max_active_datasets = 4;

  // [shutdown]
  int drain_deadline_ms = 15000;
  bool stop_on_contract_mismatch = true;

  // Digest of the config file as read, for the run report (NFR-14).
  std::string config_sha256;
  std::string config_path;

  RouteConfig ToRouteConfig() const;
  PublishGateConfig ToGateConfig(bool fault_manifest_verified) const;
};

// Loads and validates a TOML config. Returns false and fills error with a
// message naming the file, the line and the problem.
bool LoadRunConfig(const std::string& path, RunConfig* out, std::string* error);

// Applies a key=value override of the form "section.key=value", for the test
// harness to vary one setting without writing a new file. Returns false on an
// unknown key, so a typo in a harness invocation fails the run instead of
// quietly testing the default.
bool ApplyOverride(const std::string& assignment, RunConfig* config,
                   std::string* error);

}  // namespace p04

#endif  // P04_OPS_CONFIG_H_
