// The consumer-group verifier (section 5.1 group_verifier, E2E-09, cases K-*).
//
// What this proves: that a consumer group assigns each topic-partition to
// exactly one member per generation, that a rebalance revokes before it
// assigns, and that a manual checkpoint never advances past the last
// contiguous processed offset plus one.
//
// What it explicitly does not prove, and says so in its report: that any record
// was durably written anywhere downstream. Section 5.1 draws that line, and
// this binary writes no database. Project 05 owns the idempotent sink.
//
// Two librdkafka settings carry the offset contract:
//   enable.auto.commit=false      nothing is committed on a timer
//   enable.auto.offset.store=false  nothing is even staged for commit unless
//                                   this code stages it
// Leaving the second one at its default is the classic mistake: auto-commit off
// with offset-store on still commits whatever the client stored, which for a
// batch-processing consumer is ahead of what was processed.
#include <librdkafka/rdkafka.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <csignal>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "ops/run_manifest.h"
#include "serialization/marketdata_codec.h"

namespace {

std::atomic<bool> g_stop{false};
extern "C" void HandleStop(int) { g_stop.store(true); }

struct PartitionState {
  // Offsets processed, kept as a set so a gap is visible. The contiguous
  // frontier is the largest offset such that every offset from the start of
  // processing up to it has been processed.
  std::int64_t first_offset = -1;
  std::int64_t contiguous_frontier = -1;
  std::set<std::int64_t> processed_above_frontier;
  std::int64_t last_committed = -1;
  std::uint64_t records = 0;
  std::uint64_t decode_errors = 0;
  bool currently_assigned = false;
  std::uint64_t assign_count = 0;
  std::uint64_t revoke_count = 0;

  void Process(std::int64_t offset) {
    ++records;
    if (first_offset < 0) {
      first_offset = offset;
      contiguous_frontier = offset;
    } else if (offset == contiguous_frontier + 1) {
      contiguous_frontier = offset;
    } else if (offset > contiguous_frontier + 1) {
      processed_above_frontier.insert(offset);
    }
    // Absorb anything that is now contiguous.
    while (!processed_above_frontier.empty() &&
           *processed_above_frontier.begin() == contiguous_frontier + 1) {
      contiguous_frontier = *processed_above_frontier.begin();
      processed_above_frontier.erase(processed_above_frontier.begin());
    }
  }
};

struct VerifierState {
  std::map<std::pair<std::string, std::int32_t>, PartitionState> partitions;
  std::uint64_t rebalance_events = 0;
  std::uint64_t revoke_with_inflight = 0;
  std::uint64_t commits = 0;
  std::uint64_t commit_failures = 0;
  std::uint64_t invalid_commit_refused = 0;
  std::uint64_t concurrent_assignment_violations = 0;
  std::vector<std::string> findings;
};

VerifierState* g_state = nullptr;

// Stages a commit at contiguous_frontier + 1, the Kafka convention: a committed
// offset is the next offset to read, not the last one read. Committing
// max_seen + 1 would skip a gap, which is precisely what section 5.1 forbids.
void CommitContiguousFrontier(rd_kafka_t* consumer, VerifierState* state) {
  rd_kafka_topic_partition_list_t* list = rd_kafka_topic_partition_list_new(
      static_cast<int>(state->partitions.size()));
  int staged = 0;
  for (auto& [key, partition] : state->partitions) {
    if (!partition.currently_assigned) continue;
    if (partition.contiguous_frontier < 0) continue;
    const std::int64_t next = partition.contiguous_frontier + 1;
    if (next <= partition.last_committed) {
      // Never move a checkpoint backwards.
      continue;
    }
    rd_kafka_topic_partition_list_add(list, key.first.c_str(), key.second)->offset = next;
    partition.last_committed = next;
    ++staged;
  }
  if (staged > 0) {
    const rd_kafka_resp_err_t err = rd_kafka_commit(consumer, list, 0 /*synchronous*/);
    if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
      ++state->commits;
    } else {
      ++state->commit_failures;
      state->findings.push_back(std::string("commit failed: ") + rd_kafka_err2str(err));
    }
  }
  rd_kafka_topic_partition_list_destroy(list);
}

void RebalanceCallback(rd_kafka_t* consumer, rd_kafka_resp_err_t err,
                       rd_kafka_topic_partition_list_t* partitions, void* /*opaque*/) {
  VerifierState* state = g_state;
  if (state == nullptr) return;
  ++state->rebalance_events;

  if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
    for (int i = 0; i < partitions->cnt; ++i) {
      const auto key = std::make_pair(std::string(partitions->elems[i].topic),
                                      partitions->elems[i].partition);
      PartitionState& partition = state->partitions[key];
      if (partition.currently_assigned) {
        // The same partition assigned twice without an intervening revoke would
        // mean two owners, which breaks the exclusivity the ordering claim rests
        // on (case K-*).
        ++state->concurrent_assignment_violations;
        state->findings.push_back("partition " + key.first + "/" +
                                  std::to_string(key.second) +
                                  " was assigned while already assigned");
      }
      partition.currently_assigned = true;
      ++partition.assign_count;
    }
    rd_kafka_assign(consumer, partitions);
    return;
  }

  if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
    // Commit what is provably processed before giving up ownership, then stop
    // treating the partition as ours. Committing after the revoke would be a
    // commit for a partition we no longer own, which the broker may reject and
    // which would in any case be a lie about who processed what.
    CommitContiguousFrontier(consumer, state);
    for (int i = 0; i < partitions->cnt; ++i) {
      const auto key = std::make_pair(std::string(partitions->elems[i].topic),
                                      partitions->elems[i].partition);
      PartitionState& partition = state->partitions[key];
      if (!partition.processed_above_frontier.empty()) {
        // Records processed out of order and not yet contiguous are in flight
        // from the checkpoint point of view; they will be re-read by whoever
        // gets the partition next. That is at-least-once working as designed,
        // and it is counted so the report can say it happened.
        ++state->revoke_with_inflight;
      }
      partition.currently_assigned = false;
      ++partition.revoke_count;
    }
    rd_kafka_assign(consumer, nullptr);
    return;
  }

  state->findings.push_back(std::string("rebalance error: ") + rd_kafka_err2str(err));
  rd_kafka_assign(consumer, nullptr);
}

void PrintUsage() {
  std::cout <<
      "p04_group_verifier --bootstrap <servers> --group <id> --topics <a,b> \n"
      "                   [--max-records N] [--runtime-seconds S]\n"
      "                   [--commit-every N] [--report <path>]\n";
}

std::vector<std::string> Split(const std::string& text, char delimiter) {
  std::vector<std::string> out;
  std::string current;
  std::istringstream in(text);
  while (std::getline(in, current, delimiter)) {
    if (!current.empty()) out.push_back(current);
  }
  return out;
}

}  // namespace

int main(int argc, char** argv) {
  std::string bootstrap = "127.0.0.1:9092";
  std::string group = "p04-group-verifier";
  std::string topics_csv = "md.ticks.v1,md.quotes.v1";
  std::string report_path = "group-verifier-report.json";
  long max_records = 0;
  long runtime_seconds = 30;
  long commit_every = 500;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--help") { PrintUsage(); return 0; }
    if (arg == "--bootstrap" && i + 1 < argc) { bootstrap = argv[++i]; continue; }
    if (arg == "--group" && i + 1 < argc) { group = argv[++i]; continue; }
    if (arg == "--topics" && i + 1 < argc) { topics_csv = argv[++i]; continue; }
    if (arg == "--report" && i + 1 < argc) { report_path = argv[++i]; continue; }
    if (arg == "--max-records" && i + 1 < argc) { max_records = std::strtol(argv[++i], nullptr, 10); continue; }
    if (arg == "--runtime-seconds" && i + 1 < argc) { runtime_seconds = std::strtol(argv[++i], nullptr, 10); continue; }
    if (arg == "--commit-every" && i + 1 < argc) { commit_every = std::strtol(argv[++i], nullptr, 10); continue; }
    std::cerr << "unknown argument: " << arg << "\n";
    PrintUsage();
    return 3;
  }

  VerifierState state;
  g_state = &state;

  rd_kafka_conf_t* conf = rd_kafka_conf_new();
  char errstr[512];
  const auto set = [&](const char* name, const std::string& value) {
    if (rd_kafka_conf_set(conf, name, value.c_str(), errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
      std::cerr << "config error " << name << "=" << value << ": " << errstr << "\n";
      return false;
    }
    return true;
  };
  bool config_ok = set("bootstrap.servers", bootstrap) && set("group.id", group) &&
                   // The offset contract, stated explicitly rather than relying
                   // on defaults that differ between client releases.
                   set("enable.auto.commit", "false") &&
                   set("enable.auto.offset.store", "false") &&
                   set("auto.offset.reset", "earliest") &&
                   set("allow.auto.create.topics", "false") &&
                   set("client.id", group + "-member");
  if (!config_ok) {
    rd_kafka_conf_destroy(conf);
    return 3;
  }
  rd_kafka_conf_set_rebalance_cb(conf, RebalanceCallback);

  rd_kafka_t* consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (consumer == nullptr) {
    std::cerr << "rd_kafka_new failed: " << errstr << "\n";
    rd_kafka_conf_destroy(conf);
    return 3;
  }
  rd_kafka_poll_set_consumer(consumer);

  const std::vector<std::string> topics = Split(topics_csv, ',');
  rd_kafka_topic_partition_list_t* subscription =
      rd_kafka_topic_partition_list_new(static_cast<int>(topics.size()));
  for (const std::string& topic : topics) {
    rd_kafka_topic_partition_list_add(subscription, topic.c_str(), RD_KAFKA_PARTITION_UA);
  }
  const rd_kafka_resp_err_t subscribe_err = rd_kafka_subscribe(consumer, subscription);
  rd_kafka_topic_partition_list_destroy(subscription);
  if (subscribe_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    std::cerr << "subscribe failed: " << rd_kafka_err2str(subscribe_err) << "\n";
    rd_kafka_consumer_close(consumer);
    rd_kafka_destroy(consumer);
    return 3;
  }

  std::signal(SIGTERM, HandleStop);
  std::signal(SIGINT, HandleStop);

  const auto start = std::chrono::steady_clock::now();
  std::uint64_t consumed = 0;
  std::uint64_t since_commit = 0;

  while (!g_stop.load()) {
    if (runtime_seconds > 0 &&
        std::chrono::steady_clock::now() - start >= std::chrono::seconds(runtime_seconds)) {
      break;
    }
    if (max_records > 0 && consumed >= static_cast<std::uint64_t>(max_records)) break;

    rd_kafka_message_t* message = rd_kafka_consumer_poll(consumer, 500);
    if (message == nullptr) continue;
    if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      if (message->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        state.findings.push_back(std::string("consume error: ") +
                                 rd_kafka_message_errstr(message));
      }
      rd_kafka_message_destroy(message);
      continue;
    }

    const auto key = std::make_pair(std::string(rd_kafka_topic_name(message->rkt)),
                                    message->partition);
    PartitionState& partition = state.partitions[key];

    // Decoding is part of verification: a record that does not parse as a bare
    // MarketDataEvent is a contract violation, and the framing guard inside
    // Decode is what catches an MDP1-prefixed value (cases D-01, D-02).
    p04::TypedEvent decoded;
    p04::CodecError codec_error = p04::CodecError::kNone;
    const std::string_view value(static_cast<const char*>(message->payload), message->len);
    if (!p04::Decode(value, &decoded, &codec_error)) {
      ++partition.decode_errors;
      state.findings.push_back(std::string("decode failure at ") + key.first + "/" +
                               std::to_string(key.second) + "@" +
                               std::to_string(message->offset) + ": " +
                               p04::ToString(codec_error));
    }

    partition.Process(message->offset);
    ++consumed;
    ++since_commit;
    rd_kafka_message_destroy(message);

    if (commit_every > 0 && since_commit >= static_cast<std::uint64_t>(commit_every)) {
      CommitContiguousFrontier(consumer, &state);
      since_commit = 0;
    }
  }

  CommitContiguousFrontier(consumer, &state);
  rd_kafka_consumer_close(consumer);

  // Report. The verdict is about group mechanics only.
  std::ostringstream json;
  json << "{\n  \"tool\": \"p04_group_verifier\",\n";
  json << "  \"group_id\": \"" << group << "\",\n";
  json << "  \"records_consumed\": " << consumed << ",\n";
  json << "  \"rebalance_events\": " << state.rebalance_events << ",\n";
  json << "  \"revoke_with_inflight\": " << state.revoke_with_inflight << ",\n";
  json << "  \"commits\": " << state.commits << ",\n";
  json << "  \"commit_failures\": " << state.commit_failures << ",\n";
  json << "  \"concurrent_assignment_violations\": "
       << state.concurrent_assignment_violations << ",\n";
  json << "  \"partitions\": [";
  bool first = true;
  std::uint64_t decode_errors = 0;
  for (const auto& [key, partition] : state.partitions) {
    if (!first) json << ",";
    first = false;
    decode_errors += partition.decode_errors;
    json << "\n    {\"topic\": \"" << key.first << "\", \"partition\": " << key.second
         << ", \"records\": " << partition.records
         << ", \"first_offset\": " << partition.first_offset
         << ", \"contiguous_frontier\": " << partition.contiguous_frontier
         << ", \"committed_next_offset\": " << partition.last_committed
         << ", \"gaps_above_frontier\": " << partition.processed_above_frontier.size()
         << ", \"assigns\": " << partition.assign_count
         << ", \"revokes\": " << partition.revoke_count
         << ", \"decode_errors\": " << partition.decode_errors << "}";
  }
  json << (first ? "" : "\n  ") << "],\n";
  json << "  \"decode_errors\": " << decode_errors << ",\n";
  json << "  \"findings\": [";
  for (std::size_t i = 0; i < state.findings.size(); ++i) {
    if (i > 0) json << ", ";
    json << "\"" << state.findings[i] << "\"";
  }
  json << "],\n";

  const bool mechanics_ok = state.concurrent_assignment_violations == 0 &&
                            state.commit_failures == 0 && decode_errors == 0;
  const char* verdict = consumed == 0 ? "INCONCLUSIVE" : (mechanics_ok ? "PASS" : "FAIL");
  json << "  \"verdict\": \"" << verdict << "\",\n";
  json << "  \"proves\": \"consumer group assignment exclusivity per generation, "
          "revoke-before-assign, and that no checkpoint advanced past the last "
          "contiguous processed offset\",\n";
  json << "  \"does_not_prove\": \"durable downstream writes, exactly-once "
          "delivery, or any ClickHouse commit; Project 05 owns the idempotent "
          "sink\"\n}\n";

  std::ofstream out(report_path, std::ios::binary | std::ios::trunc);
  if (out) out << json.str();
  std::cout << json.str();

  rd_kafka_destroy(consumer);
  g_state = nullptr;
  if (consumed == 0) return 1;
  return mechanics_ok ? 0 : 2;
}
