#include "ops/config.h"

#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <unordered_map>

#include "ops/sha256.h"

namespace p04 {
namespace {

std::string Trim(const std::string& in) {
  std::size_t begin = 0;
  std::size_t end = in.size();
  while (begin < end && (in[begin] == ' ' || in[begin] == '\t')) ++begin;
  while (end > begin && (in[end - 1] == ' ' || in[end - 1] == '\t' ||
                         in[end - 1] == '\r')) --end;
  return in.substr(begin, end - begin);
}

// Strips a trailing comment, honouring quotes so a '#' inside a string value is
// kept. Without this, a bootstrap list or a path containing '#' would be
// truncated into something that still parses, which is the worst kind of config
// bug: silent and plausible.
std::string StripComment(const std::string& line) {
  bool in_string = false;
  for (std::size_t i = 0; i < line.size(); ++i) {
    if (line[i] == '"') in_string = !in_string;
    if (line[i] == '#' && !in_string) return line.substr(0, i);
  }
  return line;
}

bool ParseBool(const std::string& value, bool* out) {
  if (value == "true") { *out = true; return true; }
  if (value == "false") { *out = false; return true; }
  return false;
}

bool ParseInt(const std::string& value, long long* out) {
  if (value.empty()) return false;
  std::size_t i = (value[0] == '-' || value[0] == '+') ? 1 : 0;
  if (i >= value.size()) return false;
  for (std::size_t j = i; j < value.size(); ++j) {
    // Underscores are TOML-legal digit separators and appear in the example
    // config; stripping them here keeps 2_147_483_647 readable in the file.
    if (value[j] == '_') continue;
    if (value[j] < '0' || value[j] > '9') return false;
  }
  std::string digits;
  for (const char c : value) {
    if (c != '_') digits.push_back(c);
  }
  errno = 0;
  char* end = nullptr;
  const long long parsed = std::strtoll(digits.c_str(), &end, 10);
  if (errno == ERANGE || end == digits.c_str()) return false;
  *out = parsed;
  return true;
}

bool ParseString(const std::string& value, std::string* out) {
  if (value.size() < 2 || value.front() != '"' || value.back() != '"') return false;
  out->assign(value, 1, value.size() - 2);
  return true;
}

bool ParseRunMode(const std::string& text, RunMode* out) {
  if (text == "offline_verified_publish") { *out = RunMode::kOfflineVerifiedPublish; return true; }
  if (text == "self_test") { *out = RunMode::kSelfTest; return true; }
  if (text == "topic_verify_only") { *out = RunMode::kTopicVerifyOnly; return true; }
  return false;
}

struct Assignment {
  std::string section;
  std::string key;
  std::string raw_value;
  std::size_t line = 0;
};

// Applies one assignment. Returns false for an unknown key so the caller can
// report it with the line number: an unrecognised key is a hard error, which is
// the only way a pinned config can be trusted.
bool Apply(const Assignment& a, RunConfig* c, std::string* error) {
  const auto bad = [&](const std::string& why) {
    *error = a.section + "." + a.key + ": " + why;
    return false;
  };
  const auto as_string = [&](std::string* out) {
    if (!ParseString(a.raw_value, out)) return bad("expected a quoted string");
    return true;
  };
  const auto as_int = [&](long long* out) {
    if (!ParseInt(a.raw_value, out)) return bad("expected an integer");
    return true;
  };
  const auto as_bool = [&](bool* out) {
    if (!ParseBool(a.raw_value, out)) return bad("expected true or false");
    return true;
  };

  std::string text;
  long long number = 0;
  bool flag = false;

  if (a.section == "run") {
    if (a.key == "run_id") return as_string(&c->run_id);
    if (a.key == "mode") {
      if (!as_string(&text)) return false;
      if (!ParseRunMode(text, &c->mode)) return bad("unknown mode " + text);
      return true;
    }
    if (a.key == "source_run_dir") return as_string(&c->source_run_dir);
    if (a.key == "proto_descriptor_sha256") return as_string(&c->proto_descriptor_sha256);
    if (a.key == "upstream") return as_string(&c->upstream);
    if (a.key == "p03_handoff_path") return as_string(&c->p03_handoff_path);
    if (a.key == "expected_source_schema") return as_string(&c->expected_source_schema);
    if (a.key == "output_dir") return as_string(&c->output_dir);
    if (a.key == "allow_fault_test_into_market_topics") {
      if (!as_bool(&flag)) return false;
      c->allow_fault_test_into_market_topics = flag;
      return true;
    }
    if (a.key == "single_active_dataset") {
      if (!as_bool(&flag)) return false;
      c->single_active_dataset = flag;
      return true;
    }
    if (a.key == "duplicate_every_event") {
      if (!as_bool(&flag)) return false;
      c->duplicate_every_event = flag;
      return true;
    }
    if (a.key == "max_events") {
      if (!as_int(&number) || number < 0) return bad("expected a non-negative integer");
      c->max_events = static_cast<std::uint64_t>(number);
      return true;
    }
    if (a.key == "target_events_per_second") {
      if (!as_int(&number) || number < 0) return bad("expected a non-negative integer");
      c->target_events_per_second = static_cast<std::uint64_t>(number);
      return true;
    }
    return bad("unknown key");
  }

  if (a.section == "kafka") {
    if (a.key == "bootstrap_servers") {
      if (!as_string(&text)) return false;
      c->bootstrap_servers = text;
      c->producer.bootstrap_servers = text;
      return true;
    }
    if (a.key == "security_protocol") return as_string(&c->producer.security_protocol);
    if (a.key == "client_id") return as_string(&c->producer.client_id);
    if (a.key == "profile") return as_string(&c->profile);
    if (a.key == "ticks_topic") return as_string(&c->ticks_topic);
    if (a.key == "quotes_topic") return as_string(&c->quotes_topic);
    if (a.key == "anomaly_topic") return as_string(&c->anomaly_topic);
    if (a.key == "partition_golden_path") return as_string(&c->partition_golden_path);
    if (a.key == "expected_partitions_per_topic") {
      if (!as_int(&number) || number <= 0) return bad("expected a positive integer");
      c->expected_topic.partitions = static_cast<std::int32_t>(number);
      return true;
    }
    if (a.key == "expected_replication_factor") {
      if (!as_int(&number) || number <= 0) return bad("expected a positive integer");
      c->expected_topic.replication_factor = static_cast<std::int32_t>(number);
      return true;
    }
    if (a.key == "expected_min_insync_replicas") {
      if (!as_int(&number) || number <= 0) return bad("expected a positive integer");
      c->expected_topic.min_insync_replicas = static_cast<std::int32_t>(number);
      return true;
    }
    if (a.key == "expected_cleanup_policy") return as_string(&c->expected_topic.cleanup_policy);
    if (a.key == "expected_unclean_leader_election") {
      if (!as_bool(&flag)) return false;
      c->expected_topic.unclean_leader_election = flag;
      return true;
    }
    if (a.key == "partitioner") {
      if (!as_string(&text)) return false;
      c->partitioner = text;
      c->producer.partitioner = text;
      return true;
    }
    if (a.key == "allow_auto_topic_create") {
      if (!as_bool(&flag)) return false;
      c->allow_auto_topic_create = flag;
      return true;
    }
    return bad("unknown key");
  }

  if (a.section == "producer") {
    if (a.key == "enable_idempotence") {
      if (!as_bool(&flag)) return false;
      c->producer.enable_idempotence = flag;
      return true;
    }
    if (a.key == "acks") return as_string(&c->producer.acks);
    if (a.key == "retries") {
      if (!as_int(&number)) return bad("expected an integer");
      c->producer.retries = std::to_string(number);
      return true;
    }
    if (a.key == "compression_type") return as_string(&c->producer.compression_type);
    if (a.key == "max_in_flight_requests_per_connection") {
      if (!as_int(&number)) return bad("expected an integer");
      c->producer.max_in_flight_requests_per_connection = static_cast<int>(number);
      return true;
    }
    if (a.key == "message_timeout_ms") {
      if (!as_int(&number)) return bad("expected an integer");
      c->producer.message_timeout_ms = static_cast<int>(number);
      return true;
    }
    if (a.key == "linger_ms") {
      if (!as_int(&number)) return bad("expected an integer");
      c->producer.linger_ms = static_cast<int>(number);
      return true;
    }
    if (a.key == "batch_size_bytes") {
      if (!as_int(&number)) return bad("expected an integer");
      c->producer.batch_size_bytes = static_cast<int>(number);
      return true;
    }
    if (a.key == "queue_buffering_max_messages") {
      if (!as_int(&number)) return bad("expected an integer");
      c->producer.queue_buffering_max_messages = static_cast<int>(number);
      return true;
    }
    if (a.key == "queue_buffering_max_kbytes") {
      if (!as_int(&number)) return bad("expected an integer");
      c->producer.queue_buffering_max_kbytes = static_cast<int>(number);
      return true;
    }
    if (a.key == "poll_interval_ms") {
      if (!as_int(&number)) return bad("expected an integer");
      c->poll_interval_ms = static_cast<int>(number);
      return true;
    }
    return bad("unknown key");
  }

  if (a.section == "handoff") {
    if (a.key == "max_records") {
      if (!as_int(&number) || number <= 0) return bad("expected a positive integer");
      c->handoff.max_records = static_cast<std::size_t>(number);
      return true;
    }
    if (a.key == "max_bytes") {
      if (!as_int(&number) || number <= 0) return bad("expected a positive integer");
      c->handoff.max_bytes = static_cast<std::size_t>(number);
      return true;
    }
    if (a.key == "high_watermark_percent") {
      if (!as_int(&number)) return bad("expected an integer");
      c->handoff.high_watermark_percent = static_cast<int>(number);
      return true;
    }
    if (a.key == "low_watermark_percent") {
      if (!as_int(&number)) return bad("expected an integer");
      c->handoff.low_watermark_percent = static_cast<int>(number);
      return true;
    }
    if (a.key == "stall_timeout_ms") {
      if (!as_int(&number)) return bad("expected an integer");
      c->handoff.stall_timeout_ms = static_cast<int>(number);
      return true;
    }
    return bad("unknown key");
  }

  if (a.section == "contract") {
    if (a.key == "max_kafka_value_bytes") {
      if (!as_int(&number) || number <= 0) return bad("expected a positive integer");
      c->max_kafka_value_bytes = static_cast<std::size_t>(number);
      return true;
    }
    if (a.key == "max_key_bytes") {
      if (!as_int(&number) || number <= 0) return bad("expected a positive integer");
      c->max_key_bytes = static_cast<std::size_t>(number);
      return true;
    }
    if (a.key == "max_headers_total_bytes") {
      if (!as_int(&number) || number <= 0) return bad("expected a positive integer");
      c->max_headers_total_bytes = static_cast<std::size_t>(number);
      return true;
    }
    if (a.key == "max_active_symbols") {
      if (!as_int(&number) || number <= 0) return bad("expected a positive integer");
      c->max_active_symbols = static_cast<std::size_t>(number);
      return true;
    }
    if (a.key == "max_active_datasets") {
      if (!as_int(&number) || number <= 0) return bad("expected a positive integer");
      c->max_active_datasets = static_cast<std::size_t>(number);
      return true;
    }
    return bad("unknown key");
  }

  if (a.section == "shutdown") {
    if (a.key == "drain_deadline_ms") {
      if (!as_int(&number) || number < 0) return bad("expected a non-negative integer");
      c->drain_deadline_ms = static_cast<int>(number);
      return true;
    }
    if (a.key == "stop_on_contract_mismatch") {
      if (!as_bool(&flag)) return false;
      c->stop_on_contract_mismatch = flag;
      return true;
    }
    return bad("unknown key");
  }

  // [verifier] is read by the Python verifier, not by this binary. Accepting
  // the section without interpreting it keeps one config file for the whole
  // run, while still rejecting unknown keys inside the sections this binary
  // does own.
  if (a.section == "verifier") return true;

  *error = "unknown section [" + a.section + "]";
  return false;
}

}  // namespace

const char* ToString(RunMode mode) {
  switch (mode) {
    case RunMode::kOfflineVerifiedPublish: return "offline_verified_publish";
    case RunMode::kSelfTest:               return "self_test";
    case RunMode::kTopicVerifyOnly:        return "topic_verify_only";
  }
  return "unknown";
}

RouteConfig RunConfig::ToRouteConfig() const {
  RouteConfig route;
  route.ticks_topic = ticks_topic;
  route.quotes_topic = quotes_topic;
  route.expected_source_schema = expected_source_schema;
  route.max_value_bytes = max_kafka_value_bytes;
  route.max_key_bytes = max_key_bytes;
  route.max_headers_total_bytes = max_headers_total_bytes;
  route.partition_count = expected_topic.partitions;
  route.run_id = run_id;
  route.descriptor_sha256 = proto_descriptor_sha256;
  return route;
}

PublishGateConfig RunConfig::ToGateConfig(bool fault_manifest_verified) const {
  PublishGateConfig gate;
  gate.allow_fault_test_into_market_topics = allow_fault_test_into_market_topics;
  gate.fault_manifest_verified = fault_manifest_verified;
  gate.anomaly_topic = anomaly_topic;
  gate.single_active_dataset = single_active_dataset;
  gate.max_active_symbols = max_active_symbols;
  gate.max_active_datasets = max_active_datasets;
  return gate;
}

bool LoadRunConfig(const std::string& path, RunConfig* out, std::string* error) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    *error = "cannot open config " + path;
    return false;
  }
  RunConfig config;
  config.config_path = path;
  config.config_sha256 = Sha256::OfFile(path);

  std::string section;
  std::string line;
  std::size_t line_number = 0;
  while (std::getline(in, line)) {
    ++line_number;
    const std::string trimmed = Trim(StripComment(line));
    if (trimmed.empty()) continue;
    if (trimmed.front() == '[') {
      if (trimmed.back() != ']') {
        *error = path + ":" + std::to_string(line_number) + ": malformed table header";
        return false;
      }
      section = trimmed.substr(1, trimmed.size() - 2);
      continue;
    }
    const auto equals = trimmed.find('=');
    if (equals == std::string::npos) {
      *error = path + ":" + std::to_string(line_number) + ": expected key = value";
      return false;
    }
    Assignment assignment;
    assignment.section = section;
    assignment.key = Trim(trimmed.substr(0, equals));
    assignment.raw_value = Trim(trimmed.substr(equals + 1));
    assignment.line = line_number;
    if (assignment.section.empty()) {
      *error = path + ":" + std::to_string(line_number) + ": key outside any table";
      return false;
    }
    std::string why;
    if (!Apply(assignment, &config, &why)) {
      *error = path + ":" + std::to_string(line_number) + ": " + why;
      return false;
    }
  }

  // Cross-field validation. These are the combinations that would otherwise
  // produce a run whose report claims more than the setup can support.
  if (config.handoff.low_watermark_percent >= config.handoff.high_watermark_percent) {
    *error = "handoff.low_watermark_percent must be below high_watermark_percent, "
             "otherwise the hysteresis does nothing";
    return false;
  }
  if (config.profile == "reliability-lab") {
    if (config.expected_topic.replication_factor < 3 ||
        config.expected_topic.min_insync_replicas < 2) {
      *error = "profile reliability-lab requires replication_factor>=3 and "
               "min_insync_replicas>=2 (NFR-06); a single-replica topic cannot "
               "support an HA verdict";
      return false;
    }
  }
  if (config.allow_auto_topic_create) {
    *error = "kafka.allow_auto_topic_create=true is refused: FR-13 requires "
             "provisioning through the Admin path so a partition-count mismatch "
             "is caught before publishing";
    return false;
  }
  if (config.upstream != "p01_mock" && config.upstream != "p03_handoff") {
    *error = "run.upstream must be \"p01_mock\" or \"p03_handoff\", got \"" +
             config.upstream + "\"";
    return false;
  }
  // An unreadable handoff path is refused here rather than at the first record:
  // a run that starts and then cannot find its upstream has already published
  // nothing while reporting itself as started.
  if (config.upstream == "p03_handoff" && config.p03_handoff_path.empty()) {
    *error = "run.p03_handoff_path is required when run.upstream=\"p03_handoff\"";
    return false;
  }
  if (config.upstream == "p01_mock" && !config.p03_handoff_path.empty()) {
    *error = "run.p03_handoff_path is set but run.upstream=\"p01_mock\": refusing "
             "a config that names an upstream it will not read";
    return false;
  }
  if (config.mode == RunMode::kOfflineVerifiedPublish && config.source_run_dir.empty()) {
    *error = "run.source_run_dir is required in offline_verified_publish mode";
    return false;
  }

  *out = std::move(config);
  if (error != nullptr) error->clear();
  return true;
}

bool ApplyOverride(const std::string& assignment, RunConfig* config,
                   std::string* error) {
  const auto equals = assignment.find('=');
  if (equals == std::string::npos) {
    *error = "override must be section.key=value, got: " + assignment;
    return false;
  }
  const std::string path = assignment.substr(0, equals);
  const std::string value = assignment.substr(equals + 1);
  const auto dot = path.find('.');
  if (dot == std::string::npos) {
    *error = "override must be section.key=value, got: " + assignment;
    return false;
  }
  Assignment parsed;
  parsed.section = path.substr(0, dot);
  parsed.key = path.substr(dot + 1);
  parsed.raw_value = value;
  return Apply(parsed, config, error);
}

}  // namespace p04
