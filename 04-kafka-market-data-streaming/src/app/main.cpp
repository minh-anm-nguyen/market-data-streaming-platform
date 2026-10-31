// The publisher entry point.
//
// Exit codes are part of the contract, because a harness decides GO/NO-GO from
// them (FR-12, section 11.4):
//
//   0  the run completed, every attempt reached a terminal state, the
//      conservation equations balance and no contract violation was seen
//   1  INCOMPLETE: attempts were left pending or finalised as UNKNOWN, so
//      completeness cannot be claimed (NFR-10)
//   2  FAIL: a contract violation, such as a partition the pinned partitioner
//      did not choose, or counters that do not balance
//   3  NO_GO before publishing: config, schema pin, golden mapping or topic
//      verification refused the run
//
// Exit code 0 is deliberately hard to reach. A run that merely did not crash
// gets 1, which is the point of NFR-10 and section 14.
#include <atomic>
#include <chrono>
#include <fstream>
#include <memory>
#include <optional>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "app/publisher.h"
#include "routing/partition_policy.h"
#include "ops/config.h"
#include "ops/metrics.h"
#include "ops/run_manifest.h"
#include "ops/sha256.h"
#include "ops/topic_guard.h"
#include "producer/fake_transport.h"
#include "producer/rdkafka_producer.h"
#include "serialization/marketdata_codec.h"
#include "source/p01_source.h"
#include "source/p03_source.h"

namespace {

// Set from the signal handler, read from the publish loop. Nothing else may be
// done in a handler: section 4.1 wants SIGTERM to begin an orderly drain, and
// an orderly drain cannot be performed from async-signal context.
std::atomic<bool> g_stop_requested{false};

extern "C" void HandleStopSignal(int /*signum*/) { g_stop_requested.store(true); }

void PrintUsage() {
  std::cout <<
      "p04_publisher --config <file.toml> [options]\n"
      "\n"
      "  --config <path>          run configuration (required)\n"
      "  --set section.key=value  override one config value (repeatable)\n"
      "  --run-id <id>            shorthand for --set run.run_id=\"<id>\"\n"
      "  --print-descriptor-hash  print the pinned descriptor digest and exit\n"
      "  --help                   this message\n";
}

std::string CompilerDescription() {
#if defined(__clang__)
  return std::string("clang ") + __clang_version__;
#elif defined(__GNUC__)
  return std::string("gcc ") + std::to_string(__GNUC__) + "." +
         std::to_string(__GNUC_MINOR__) + "." + std::to_string(__GNUC_PATCHLEVEL__);
#else
  return "unknown";
#endif
}

}  // namespace

int main(int argc, char** argv) {
  using namespace p04;

  std::string config_path;
  std::vector<std::string> overrides;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      PrintUsage();
      return 0;
    }
    if (arg == "--print-descriptor-hash") {
      std::cout << PinnedMessageFullName() << " " << PinnedDescriptorSha256() << "\n";
      return 0;
    }
    if (arg == "--config" && i + 1 < argc) {
      config_path = argv[++i];
      continue;
    }
    if (arg == "--set" && i + 1 < argc) {
      overrides.emplace_back(argv[++i]);
      continue;
    }
    if (arg == "--run-id" && i + 1 < argc) {
      overrides.emplace_back(std::string("run.run_id=\"") + argv[++i] + "\"");
      continue;
    }
    std::cerr << "unknown argument: " << arg << "\n";
    PrintUsage();
    return 3;
  }
  if (config_path.empty()) {
    std::cerr << "--config is required\n";
    return 3;
  }

  RunConfig config;
  std::string error;
  if (!LoadRunConfig(config_path, &config, &error)) {
    std::cerr << "NO_GO config: " << error << "\n";
    return 3;
  }
  for (const std::string& assignment : overrides) {
    if (!ApplyOverride(assignment, &config, &error)) {
      std::cerr << "NO_GO override: " << error << "\n";
      return 3;
    }
  }

  RunManifest manifest;
  manifest.run_id = config.run_id;
  manifest.mode = ToString(config.mode);
  manifest.profile = config.profile;
  manifest.started_at_utc = UtcNow();
  manifest.config_sha256 = config.config_sha256;
  manifest.compiler = CompilerDescription();
  manifest.protobuf_version = ProtobufRuntimeVersion();
  manifest.proto_descriptor_sha256 = PinnedDescriptorSha256();
  manifest.proto_message_full_name = PinnedMessageFullName();
  manifest.binary_sha256 = Sha256::OfFile(argv[0]);
  // Which upstream ran is recorded, never assumed. A report that claimed a real
  // adapter while the mock produced the events would be the false provenance
  // this project refuses elsewhere.
  manifest.mock_upstream_p03 = (config.upstream != "p03_handoff");
  manifest.notes =
      manifest.mock_upstream_p03
          ? "Upstream is the mock P03 classifier. Verifier columns are filled by "
            "the independent Python snapshot verifier."
          : "Upstream is the real Project 03 adapter handoff stream; the "
            "classification and suppression set are P03's. Verifier columns are "
            "filled by the independent Python snapshot verifier.";

  const std::string run_dir = config.output_dir + "/" + config.run_id;
  if (!PrepareRunDirectory(run_dir, &error)) {
    std::cerr << "NO_GO run directory: " << error << "\n";
    return 3;
  }

  const auto finish = [&](int exit_code) {
    manifest.finished_at_utc = UtcNow();
    manifest.exit_code = exit_code;
    // The overall verdict is the worse of the verdicts that actually ran.
    // Folding in a NOT_RUN performance verdict would report NOT_RUN for a run
    // whose safety oracle passed and whose performance was never requested,
    // which ADR-12 separates precisely to avoid.
    if (manifest.performance_verdict == Verdict::kNotRun) {
      manifest.verdict = manifest.safety_verdict;
    } else if (manifest.safety_verdict == Verdict::kNotRun) {
      manifest.verdict = manifest.performance_verdict;
    } else {
      manifest.verdict = Worse(manifest.safety_verdict, manifest.performance_verdict);
    }
    if (!manifest.WriteJson(run_dir + "/report.json")) {
      std::cerr << "WARNING: could not write report.json\n";
    }
    if (!manifest.WriteMarkdown(run_dir + "/report.md")) {
      std::cerr << "WARNING: could not write report.md\n";
    }
    std::cout << "verdict=" << ToString(manifest.verdict)
              << " safety=" << ToString(manifest.safety_verdict)
              << " performance=" << ToString(manifest.performance_verdict)
              << " exit=" << exit_code << " artifacts=" << run_dir << "\n";
    return exit_code;
  };

  // The descriptor pin. An empty expectation means "record it"; a mismatch
  // means the binary was linked against a different schema than the run was
  // configured for, which case D-08 requires to stop the run.
  if (!config.proto_descriptor_sha256.empty() &&
      config.proto_descriptor_sha256 != "SET_FROM_P02_BUILD" &&
      config.proto_descriptor_sha256 != manifest.proto_descriptor_sha256) {
    manifest.safety_verdict = Verdict::kNoGo;
    manifest.verdict_reasons.push_back(
        "descriptor digest mismatch: config expects " + config.proto_descriptor_sha256 +
        ", binary carries " + manifest.proto_descriptor_sha256);
    return finish(3);
  }
  config.proto_descriptor_sha256 = manifest.proto_descriptor_sha256;

  // The golden key-to-partition mapping, audited before anything is produced.
  PartitionGolden golden;
  if (PartitionGolden::LoadFromFile(config.partition_golden_path, &golden, &error)) {
    if (!golden.Audit(config.expected_topic.partitions, &error)) {
      manifest.safety_verdict = Verdict::kNoGo;
      manifest.verdict_reasons.push_back("partition golden audit failed: " + error);
      return finish(3);
    }
    manifest.verdict_reasons.push_back(
        "partition golden audit passed over " + std::to_string(golden.mapping().size()) +
        " keys at partition_count=" + std::to_string(golden.partition_count()));
  } else {
    // A missing golden file is a NO_GO rather than a warning: without it the
    // per-key ordering claim rests on an unverified assumption (case B-07).
    manifest.safety_verdict = Verdict::kNoGo;
    manifest.verdict_reasons.push_back("partition golden mapping unavailable: " + error);
    return finish(3);
  }

  // The source dataset and its manifest.
  SourceManifest source;
  if (config.mode != RunMode::kTopicVerifyOnly) {
    if (!LoadSourceManifest(config.source_run_dir, &source, &error)) {
      manifest.safety_verdict = Verdict::kNoGo;
      manifest.verdict_reasons.push_back("source manifest unusable: " + error);
      return finish(3);
    }
    manifest.dataset_sha256 = source.computed_sha256;
    manifest.dataset_sha256_recorded = source.events_sha256;
    manifest.source_manifest_summary = source.Explain();
    manifest.source_usable_as_oracle = source.UsableAsCompletenessOracle();
    if (!manifest.source_usable_as_oracle) {
      // The run may still be useful as a diagnostic, but it can never be a
      // completeness proof (fixture F13), and the verdict says so.
      manifest.verdict_reasons.push_back(
          "source is not a completeness oracle (missing COMPLETED marker or "
          "digest mismatch); the best achievable verdict is INCOMPLETE");
    }
  }

  // Transport selection. self_test mode uses the in-process fake so CI can run
  // the whole pipeline without a broker, and the report records which one was
  // used so a fake run can never be read as a broker result.
  std::unique_ptr<KafkaTransport> transport;
  RdKafkaProducer* real_producer = nullptr;
  if (config.mode == RunMode::kSelfTest) {
    FakeTransportConfig fake;
    fake.partition_count = config.expected_topic.partitions;
    fake.enable_idempotence = config.producer.enable_idempotence;
    fake.acks = config.producer.acks;
    fake.max_in_flight_requests_per_connection =
        config.producer.max_in_flight_requests_per_connection;
    fake.retries = std::strtol(config.producer.retries.c_str(), nullptr, 10);
    fake.message_timeout_ms = config.producer.message_timeout_ms;
    fake.queue_max_records =
        static_cast<std::size_t>(config.producer.queue_buffering_max_messages);
    fake.queue_max_bytes =
        static_cast<std::size_t>(config.producer.queue_buffering_max_kbytes) * 1024u;
    transport = std::make_unique<FakeTransport>(fake);
  } else {
    auto producer = RdKafkaProducer::Create(config.producer, &error);
    if (producer == nullptr) {
      // The expected outcome of the negative config tests (E-01 to E-04): the
      // client refuses an incompatible combination and the process stops.
      manifest.safety_verdict = Verdict::kNoGo;
      manifest.verdict_reasons.push_back("producer creation refused: " + error);
      return finish(3);
    }
    real_producer = producer.get();
    transport = std::move(producer);
  }
  manifest.librdkafka_version = transport->ClientVersion();

  if (!transport->ValidateEffectiveConfig(&error)) {
    manifest.safety_verdict = Verdict::kNoGo;
    manifest.verdict_reasons.push_back("effective producer config refused: " + error);
    return finish(3);
  }

  // Topic verification. Fail closed before the first record.
  const std::vector<std::string> market_topics = {config.ticks_topic, config.quotes_topic};
  if (real_producer != nullptr) {
    const TopicGuardResult guard = VerifyTopics(
        real_producer->handle(), market_topics, config.expected_topic, config.profile, 10000);
    manifest.topic_metadata = guard.metadata;
    if (!guard.ok) {
      manifest.safety_verdict = Verdict::kNoGo;
      manifest.verdict_reasons.push_back("topic verification failed: " + guard.error);
      return finish(3);
    }
    manifest.kafka_version = "cluster_id=" + guard.cluster_id + " brokers=" +
                             std::to_string(guard.broker_count);
    manifest.verdict_reasons.push_back(
        "topic contract verified for " + std::to_string(market_topics.size()) + " topics");
  }

  if (config.mode == RunMode::kTopicVerifyOnly) {
    manifest.safety_verdict = Verdict::kPass;
    manifest.performance_verdict = Verdict::kNotRun;
    manifest.verdict_reasons.push_back("topic_verify_only: nothing was published");
    return finish(0);
  }

  // Wire the pipeline.
  RouteTable routes(config.ToRouteConfig());
  // The fault manifest is only treated as verified when the source run is a
  // completeness oracle and actually recorded faults. Anything else leaves the
  // gate refusing fault-test events (case A-03).
  const bool fault_manifest_verified =
      source.UsableAsCompletenessOracle() && source.faults_enabled;
  PublishGate gate(config.ToGateConfig(fault_manifest_verified));
  BoundedHandoff handoff(config.handoff);
  PublishLedger ledger;
  Metrics metrics(config.ticks_topic, config.quotes_topic,
                  config.expected_topic.partitions);

  PublisherConfig publisher_config;
  publisher_config.block_on_backpressure = true;
  publisher_config.duplicate_every_event = config.duplicate_every_event;
  publisher_config.stop_on_contract_mismatch = config.stop_on_contract_mismatch;
  Publisher publisher(transport.get(), &routes, &gate, &handoff, &ledger, &metrics,
                      publisher_config);
  publisher.Install();

  if (config.duplicate_every_event) {
    manifest.verdict_reasons.push_back(
        "run.duplicate_every_event=true: this run publishes every event twice at "
        "the application level and can therefore contain duplicates by "
        "construction (case E-07)");
  }

  MockP03Config mock_config;
  mock_config.run_id = config.run_id;
  mock_config.descriptor_sha256 = manifest.proto_descriptor_sha256;
  mock_config.expected_schema_version = config.expected_source_schema;
  MockP03Classifier classifier(mock_config);

  std::signal(SIGTERM, HandleStopSignal);
  std::signal(SIGINT, HandleStopSignal);

  const auto wall_start = std::chrono::steady_clock::now();
  auto next_poll = wall_start;
  const auto poll_period = std::chrono::milliseconds(config.poll_interval_ms);

  // Pacing for the rate ladder (section 7.2). A target of 0 means unthrottled.
  const bool paced = config.target_events_per_second > 0;
  const auto nanos_per_event =
      paced ? std::chrono::nanoseconds(1000000000ull / config.target_events_per_second)
            : std::chrono::nanoseconds(0);
  auto next_send = wall_start;

  std::uint64_t offered = 0;
  std::uint64_t stop_reason_backpressure_timeout = 0;
  bool fatal_stop = false;
  std::vector<SourceParseError> parse_errors;

  // Shared by both upstreams, so pacing, backpressure and the stop signal
  // behave identically whichever one produced the event.
  const auto offer = [&](const OutputEvent& output) -> bool {
    const OfferResult result = publisher.Offer(output);
    ++offered;
    switch (result) {
      case OfferResult::kFatal:
        fatal_stop = true;
        return false;
      case OfferResult::kBackpressureTimeout:
        ++stop_reason_backpressure_timeout;
        return false;
      default:
        break;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= next_poll) {
      publisher.Poll(0);
      next_poll = now + poll_period;
    }
    return true;
  };

  const auto pace = [&]() {
    if (!paced) return;
    // Pace on the steady clock against an absolute schedule rather than
    // sleeping a fixed amount per event, so a slow event does not push the
    // whole run late and under-report the offered rate.
    next_send += nanos_per_event;
    const auto now = std::chrono::steady_clock::now();
    if (next_send > now) std::this_thread::sleep_for(next_send - now);
  };

  P03HandoffStats p03_stats;
  bool read_ok = false;

  if (config.upstream == "p03_handoff") {
    P03HandoffConfig handoff_config;
    handoff_config.run_id = config.run_id;
    handoff_config.descriptor_sha256 = manifest.proto_descriptor_sha256;
    handoff_config.expected_dataset_id = source.dataset_id;
    handoff_config.expected_schema_version = config.expected_source_schema;
    read_ok = ReadP03Handoff(
        config.p03_handoff_path, handoff_config,
        [&](const OutputEvent& output) -> bool {
          if (g_stop_requested.load()) return false;
          if (config.max_events > 0 && offered >= config.max_events) return false;
          pace();
          return offer(output);
        },
        &p03_stats, &parse_errors, &error);
  } else {
    read_ok = ReadEvents(
      source.events_path,
      [&](const TypedEvent& event, const std::string& raw_line) -> bool {
        if (g_stop_requested.load()) return false;
        if (config.max_events > 0 && offered >= config.max_events) return false;

        pace();
        // A stalled queue past the deadline ends the run INCOMPLETE rather than
        // continuing to accept work it cannot deliver (section 4.4); `offer`
        // reports that by returning false.
        return offer(classifier.Classify(event, raw_line));
      },
      &parse_errors, &error);
  }

  if (!read_ok) {
    manifest.safety_verdict = Verdict::kNoGo;
    manifest.verdict_reasons.push_back("source read failed: " + error);
    return finish(3);
  }

  handoff.Close();
  const DrainResult drain = publisher.Drain(config.drain_deadline_ms);

  const LedgerCounters counters = ledger.Counters();
  std::string conservation_error;
  const bool conserved = ledger.CheckConservation(&conservation_error);
  manifest.conservation_check = conserved ? "ok" : conservation_error;
  manifest.contract_error = publisher.contract_error();

  const auto wall_end = std::chrono::steady_clock::now();
  const double wall_seconds = std::chrono::duration<double>(wall_end - wall_start).count();

  manifest.upstream_accepted = counters.upstream_accepted;
  manifest.policy_suppressed = counters.policy_suppressed;
  manifest.route_rejected = counters.route_rejected;
  manifest.p04_enqueued_in_library = counters.queued_lib;
  manifest.p04_broker_acked = counters.acked;
  manifest.p04_delivery_failed_definite = counters.failed_definite;
  manifest.p04_delivery_unknown = counters.unknown;
  manifest.p04_pending_at_shutdown =
      counters.pending_in_library + counters.pending_in_app;
  manifest.p04_app_level_resends = counters.app_level_resends;
  manifest.p04_partition_mismatch = counters.partition_mismatch;
  manifest.p04_double_finalize_refused = counters.double_finalize_refused;
  // The left-hand side of the conservation equations comes from the upstream
  // that actually produced the events. Taking it from the idle classifier would
  // state the equation against a count of zero and make it trivially hold.
  manifest.source_expected_market_valid =
      (config.upstream == "p03_handoff")
          ? p03_stats.market_valid + p03_stats.degraded_forwarded
          : classifier.counts().market_valid;
  manifest.wall_seconds = wall_seconds;
  manifest.requested_events_per_second =
      paced ? std::optional<std::uint64_t>(config.target_events_per_second) : std::nullopt;
  if (wall_seconds > 0.0) {
    manifest.actual_sustained_acked_events_per_second =
        static_cast<double>(counters.acked) / wall_seconds;
  }

  const HandoffStats handoff_stats = handoff.Stats();
  manifest.max_app_queue_records = handoff_stats.max_records_observed;
  manifest.max_app_queue_bytes = handoff_stats.max_bytes_observed;
  manifest.max_app_queue_oldest_age_seconds = handoff_stats.oldest_age_seconds;

  const LatencyHistogram& latency = metrics.ack_latency();
  if (latency.count() > 0) {
    manifest.p50_ack_ms = latency.Quantile(0.50) * 1000.0;
    manifest.p95_ack_ms = latency.Quantile(0.95) * 1000.0;
    manifest.p99_ack_ms = latency.Quantile(0.99) * 1000.0;
    manifest.p999_ack_ms = latency.Quantile(0.999) * 1000.0;
    manifest.max_ack_ms = latency.max() * 1000.0;
  }

  std::uint64_t rss = 0;
  std::uint64_t fds = 0;
  double cpu = 0.0;
  if (ReadProcessStats(&rss, &fds, &cpu)) {
    metrics.SetProcessStats(rss, fds, cpu);
    manifest.max_process_rss_bytes = rss;
    manifest.process_fd_count = fds;
    manifest.process_cpu_seconds = cpu;
  }

  // Artifacts first, verdict second: if the process dies while computing a
  // verdict, the evidence is already on disk.
  if (!ledger.WriteJsonl(run_dir + "/publish-ledger.jsonl")) {
    std::cerr << "WARNING: could not write publish-ledger.jsonl\n";
  }
  {
    std::ofstream prom(run_dir + "/metrics.prom", std::ios::binary | std::ios::trunc);
    if (prom) prom << metrics.RenderPrometheus();
  }
  if (!parse_errors.empty()) {
    std::ofstream errors_file(run_dir + "/source-parse-errors.jsonl",
                              std::ios::binary | std::ios::trunc);
    for (const auto& parse_error : parse_errors) {
      errors_file << "{\"line\":" << parse_error.line_number << ",\"reason\":\""
                  << parse_error.reason << "\",\"detail\":\"" << parse_error.detail
                  << "\"}\n";
    }
  }

  // The safety verdict.
  int exit_code = 0;
  if (!manifest.contract_error.empty()) {
    manifest.safety_verdict = Verdict::kFail;
    manifest.verdict_reasons.push_back("contract violation: " + manifest.contract_error);
    exit_code = 2;
  } else if (!conserved) {
    manifest.safety_verdict = Verdict::kFail;
    manifest.verdict_reasons.push_back("counters do not balance: " + conservation_error);
    exit_code = 2;
  } else if (counters.double_finalize_refused > 0) {
    manifest.safety_verdict = Verdict::kFail;
    manifest.verdict_reasons.push_back(
        std::to_string(counters.double_finalize_refused) +
        " attempts received a second terminal report, which means a client or "
        "shutdown race");
    exit_code = 2;
  } else if (fatal_stop) {
    manifest.safety_verdict = Verdict::kIncomplete;
    manifest.verdict_reasons.push_back("producer reported a fatal error and the run stopped");
    exit_code = 1;
  } else if (counters.unknown > 0 || !drain.complete ||
             manifest.p04_pending_at_shutdown.value_or(0) > 0) {
    // The honest default for an unresolved outcome. Section 4.2 and NFR-10 both
    // land here: unknown is not success and not proven failure.
    manifest.safety_verdict = Verdict::kIncomplete;
    manifest.verdict_reasons.push_back(
        std::to_string(counters.unknown) + " attempts ended UNKNOWN and " +
        std::to_string(drain.finalized_as_unknown) +
        " were finalised as UNKNOWN at the drain deadline; only an independent "
        "verifier can resolve whether the broker holds them");
    exit_code = 1;
  } else if (!manifest.source_usable_as_oracle) {
    manifest.safety_verdict = Verdict::kIncomplete;
    exit_code = 1;
  } else if (stop_reason_backpressure_timeout > 0) {
    manifest.safety_verdict = Verdict::kIncomplete;
    manifest.verdict_reasons.push_back(
        "the run stopped early on a backpressure stall deadline");
    exit_code = 1;
  } else {
    // Still only a producer-side PASS. Consumer visibility is the verifier job,
    // and the report says so in its "not proven" section.
    manifest.safety_verdict = Verdict::kPass;
    manifest.verdict_reasons.push_back(
        "every attempt reached a terminal state, counters balance, and no "
        "contract violation was observed; consumer visibility is not claimed here");
  }

  // The performance verdict is separate (ADR-12). It is NOT_RUN unless a target
  // rate was requested, and a safe refusal under overload never counts as
  // meeting a rate.
  if (!paced) {
    manifest.performance_verdict = Verdict::kNotRun;
  } else if (manifest.actual_sustained_acked_events_per_second.value_or(0.0) >=
             static_cast<double>(config.target_events_per_second) * 0.95) {
    manifest.performance_verdict = Verdict::kPass;
  } else {
    manifest.performance_verdict = Verdict::kFail;
    manifest.verdict_reasons.push_back(
        "requested " + std::to_string(config.target_events_per_second) +
        " events/s, sustained " +
        std::to_string(manifest.actual_sustained_acked_events_per_second.value_or(0.0)));
  }

  return finish(exit_code);
}
