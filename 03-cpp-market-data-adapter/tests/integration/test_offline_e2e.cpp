// Offline replay through the real runner: provenance rules, counters,
// conservation and fail-closed behaviour. Groups H and I.
#include <gtest/gtest.h>

#include <filesystem>

#include "md_adapter/pipeline.hpp"
#include "md_adapter/report.hpp"
#include "md_adapter/sha256.hpp"
#include "test_support.hpp"

using namespace mdtest;
using md::Provenance;
using md::Reason;
using md::RunStatus;

namespace {

struct Harness {
  TempDir dir;
  md::Config cfg = base_config();
  md::VirtualClock clock{0};

  std::string write_stream(const std::vector<EventSpec>& specs,
                           bool manifest = true,
                           const std::string& status = "COMPLETED") {
    std::vector<std::string> payloads;
    for (const auto& s : specs) payloads.push_back(encode(s));
    const std::string data = stream(payloads);
    const std::string path = dir.file("in.pbstream");
    write_file(path, data);
    if (manifest) write_manifest(path, data, static_cast<std::int64_t>(specs.size()), status);
    cfg.input.file = path;
    return path;
  }
};

std::vector<EventSpec> contiguous(int n, const std::string& symbol = "EURUSD") {
  std::vector<EventSpec> out;
  for (int i = 1; i <= n; ++i) {
    EventSpec e;
    e.symbol = symbol;
    e.sequence = i;
    e.event_time_ns = static_cast<std::int64_t>(i) * 1'000'000;
    e.bid = 109000 + i;
    e.ask = e.bid + 5;
    out.push_back(e);
  }
  return out;
}

}  // namespace

// E2E-01: a verified source consumed end to end may be reported COMPLETE.
TEST(OfflineE2E, VerifiedSourceProducesCompleteOffline) {
  Harness h;
  h.write_stream(contiguous(500));
  h.cfg.input.require_completed_manifest = true;
  h.cfg.output.sink = md::SinkKind::kNdjsonTest;
  h.cfg.output.file = h.dir.file("out.ndjson");

  md::NdjsonTestSink sink(h.cfg.output.file);
  ASSERT_EQ(sink.open(), Reason::kNone);
  md::AdapterRunner runner(h.cfg, h.clock, sink);
  const auto out = runner.run_offline();

  EXPECT_EQ(out.status, RunStatus::kCompleteOffline) << out.summary;
  EXPECT_EQ(out.exit_code, md::ExitCode::kOk);
  EXPECT_EQ(out.provenance, Provenance::kVerifiedCompleteFile);

  const auto& m = runner.metrics();
  EXPECT_EQ(m.frames_complete, 500u);
  EXPECT_EQ(m.frames_decode_error, 0u);
  EXPECT_EQ(m.structural_reject, 0u);
  EXPECT_EQ(m.business_ok, 500u);
  EXPECT_EQ(m.policy_suppressed, 0u);
  EXPECT_EQ(m.output_success, 500u);
  EXPECT_EQ(m.gap_detected, 0u);

  const auto cons = m.check_conservation();
  EXPECT_TRUE(cons.ok) << (cons.violations.empty() ? "" : cons.violations[0]);

  EXPECT_TRUE(std::filesystem::exists(h.cfg.output.file + ".complete"));
  EXPECT_EQ(sink.artifact_sha256(), md::sha256_file(h.cfg.output.file));
}

// H-group: EOF is not evidence of completeness. Without a manifest the run
// must refuse to claim COMPLETE even though every byte parsed.
TEST(OfflineE2E, MissingManifestIsRefusedWhenVerificationIsRequired) {
  Harness h;
  h.write_stream(contiguous(10), /*manifest=*/false);
  h.cfg.input.require_completed_manifest = true;

  md::MetricsOnlySink sink;
  md::AdapterRunner runner(h.cfg, h.clock, sink);
  const auto out = runner.run_offline();

  EXPECT_EQ(out.status, RunStatus::kFailed);
  EXPECT_EQ(out.terminal_reason, Reason::kSourceUnverified);
  EXPECT_EQ(out.exit_code, md::ExitCode::kSourceUnverified);
  EXPECT_FALSE(out.notes.empty());
}

TEST(OfflineE2E, UnverifiedSourceIsProcessedButNeverCalledComplete) {
  Harness h;
  h.write_stream(contiguous(10), /*manifest=*/false);
  h.cfg.input.require_completed_manifest = false;

  md::MetricsOnlySink sink;
  md::AdapterRunner runner(h.cfg, h.clock, sink);
  const auto out = runner.run_offline();

  EXPECT_EQ(runner.metrics().business_ok, 10u);
  EXPECT_EQ(out.status, RunStatus::kIncomplete)
      << "processing is fine; claiming completeness is not";
  EXPECT_EQ(out.exit_code, md::ExitCode::kSourceUnverified);
}

TEST(OfflineE2E, ManifestDigestMismatchIsRefused) {
  Harness h;
  const std::string path = h.write_stream(contiguous(10));
  h.cfg.input.require_completed_manifest = true;
  // Corrupt one payload byte after the manifest was written.
  std::string data = read_file(path);
  data[data.size() - 1] = static_cast<char>(data.back() ^ 0x01);
  write_file(path, data);

  md::MetricsOnlySink sink;
  md::AdapterRunner runner(h.cfg, h.clock, sink);
  const auto out = runner.run_offline();

  EXPECT_EQ(out.terminal_reason, Reason::kSourceUnverified);
  bool mentions_digest = false;
  for (const auto& n : out.notes)
    if (n.find("sha256") != std::string::npos) mentions_digest = true;
  EXPECT_TRUE(mentions_digest);
}

TEST(OfflineE2E, ManifestStatusMustBeCompleted) {
  Harness h;
  h.write_stream(contiguous(10), true, "IN_PROGRESS");
  h.cfg.input.require_completed_manifest = true;

  md::MetricsOnlySink sink;
  md::AdapterRunner runner(h.cfg, h.clock, sink);
  EXPECT_EQ(runner.run_offline().terminal_reason, Reason::kSourceUnverified);
}

TEST(OfflineE2E, PartFileIsNeverAVerifiedSource) {
  Harness h;
  const std::string path = h.dir.file("in.pbstream.part");
  write_file(path, stream({encode(EventSpec{})}));
  h.cfg.input.file = path;
  h.cfg.input.require_completed_manifest = true;

  md::MetricsOnlySink sink;
  md::AdapterRunner runner(h.cfg, h.clock, sink);
  const auto out = runner.run_offline();
  EXPECT_EQ(out.terminal_reason, Reason::kSourceUnverified);
}

// E2E-06: a truncated body must fail, and no partial event may be emitted.
TEST(OfflineE2E, TruncatedBodyFailsWithoutEmittingAPartialEvent) {
  Harness h;
  const std::string good = stream({encode(EventSpec{}), encode(EventSpec{})});
  const std::string path = h.dir.file("in.pbstream");
  write_file(path, good.substr(0, good.size() - 3));
  h.cfg.input.file = path;
  h.cfg.input.require_completed_manifest = false;

  md::MetricsOnlySink sink;
  md::AdapterRunner runner(h.cfg, h.clock, sink);
  const auto out = runner.run_offline();

  EXPECT_EQ(out.status, RunStatus::kFailed);
  EXPECT_EQ(out.terminal_reason, Reason::kTruncatedBody);
  EXPECT_EQ(runner.metrics().frames_complete, 1u);
}

TEST(OfflineE2E, MalformedProtobufFailsClosed) {
  Harness h;
  std::string wire = "MDP1";
  wire += frame(encode(EventSpec{}));
  wire += frame(std::string("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", 10));
  wire += frame(encode(EventSpec{}));
  const std::string path = h.dir.file("in.pbstream");
  write_file(path, wire);
  h.cfg.input.file = path;
  h.cfg.input.require_completed_manifest = false;

  md::MetricsOnlySink sink;
  md::AdapterRunner runner(h.cfg, h.clock, sink);
  const auto out = runner.run_offline();

  EXPECT_EQ(out.terminal_reason, Reason::kProtoDecodeError);
  EXPECT_EQ(runner.metrics().frames_decode_error, 1u);
  // The third frame is never reached: there is no resynchronisation.
  EXPECT_EQ(runner.metrics().business_ok, 1u);
}

TEST(OfflineE2E, StructuralRejectFailsClosed) {
  Harness h;
  EventSpec bad;
  bad.has_symbol = false;
  std::vector<EventSpec> specs = {EventSpec{}, bad, EventSpec{}};
  h.write_stream(specs, false);
  h.cfg.input.require_completed_manifest = false;

  md::MetricsOnlySink sink;
  md::AdapterRunner runner(h.cfg, h.clock, sink);
  const auto out = runner.run_offline();

  EXPECT_EQ(out.terminal_reason, Reason::kMissingSymbol);
  EXPECT_EQ(runner.metrics().structural_reject, 1u);
  EXPECT_EQ(runner.metrics().structural_ok, 1u);
}

// A business reject only drops one event; the frame boundary is intact so
// the stream continues.
TEST(OfflineE2E, BusinessRejectSkipsOneEventAndContinues) {
  Harness h;
  std::vector<EventSpec> specs = contiguous(5);
  specs[2].bid = -1;  // invalid quote
  specs[2].ask = -1;
  h.write_stream(specs, false);
  h.cfg.input.require_completed_manifest = false;

  md::MetricsOnlySink sink;
  md::AdapterRunner runner(h.cfg, h.clock, sink);
  const auto out = runner.run_offline();

  EXPECT_NE(out.status, RunStatus::kFailed) << out.summary;
  const auto& m = runner.metrics();
  EXPECT_EQ(m.frames_complete, 5u);
  EXPECT_EQ(m.business_reject, 1u);
  EXPECT_EQ(m.business_ok, 4u);
  // The rejected event never became seen-valid, so sequence 4 reports a gap
  // across it.
  EXPECT_EQ(m.gap_detected, 1u);
  EXPECT_TRUE(m.check_conservation().ok);
}

// E2E-03 end to end through the runner.
TEST(OfflineE2E, SequenceOracleTrace) {
  Harness h;
  const auto mk = [](std::int64_t seq, std::int64_t bid) {
    EventSpec e;
    e.sequence = seq;
    e.event_time_ns = seq * 1'000'000;
    e.bid = bid;
    e.ask = bid + 5;
    return e;
  };
  // 1, 2, 4, 3(late), 2(exact dup), 2*(conflict), 5
  std::vector<EventSpec> specs = {mk(1, 109001), mk(2, 109002), mk(4, 109004),
                                  mk(3, 109003), mk(2, 109002), mk(2, 109222),
                                  mk(5, 109005)};
  h.write_stream(specs, false);
  h.cfg.input.require_completed_manifest = false;
  h.cfg.output.sink = md::SinkKind::kNdjsonTest;
  h.cfg.output.file = h.dir.file("out.ndjson");

  md::NdjsonTestSink sink(h.cfg.output.file);
  ASSERT_EQ(sink.open(), Reason::kNone);
  md::AdapterRunner runner(h.cfg, h.clock, sink);
  const auto out = runner.run_offline();
  (void)out;

  const auto& m = runner.metrics();
  EXPECT_EQ(m.business_ok, 7u);
  EXPECT_EQ(m.gap_detected, 1u);
  EXPECT_EQ(m.reorder_late, 1u);
  EXPECT_EQ(m.duplicate_exact, 1u);
  EXPECT_EQ(m.duplicate_conflict, 1u);
  // Strict mode publishes 1, 2, 4 and 5 only.
  EXPECT_EQ(m.output_success, 4u);
  EXPECT_EQ(m.policy_suppressed, 3u);

  const std::string lines = read_file(h.cfg.output.file);
  EXPECT_NE(lines.find("\"sequence\":1"), std::string::npos);
  EXPECT_NE(lines.find("\"sequence\":4"), std::string::npos);
  EXPECT_NE(lines.find("\"sequence\":5"), std::string::npos);
  EXPECT_EQ(lines.find("\"sequence\":3"), std::string::npos)
      << "a late arrival must not be published in strict mode";

  // ADR-06: observed continuity healed; published continuity did not.
  const auto* ks = runner.pipeline().tracker().stats("ds-test", "EURUSD");
  ASSERT_NE(ks, nullptr);
  EXPECT_EQ(ks->contiguous_observed_valid, 5);
  EXPECT_EQ(ks->contiguous_published, 2);
}

// E2E-08: the classification trace must not depend on how the file is read.
TEST(OfflineE2E, ReadChunkSizeDoesNotChangeTheResult) {
  std::string reference;
  for (std::int64_t chunk : {7, 64, 4096, 65536}) {
    Harness h;
    h.write_stream(contiguous(200));
    h.cfg.input.read_chunk_bytes = chunk;
    h.cfg.output.sink = md::SinkKind::kNdjsonTest;
    h.cfg.output.file = h.dir.file("out.ndjson");

    md::NdjsonTestSink sink(h.cfg.output.file);
    ASSERT_EQ(sink.open(), Reason::kNone);
    md::AdapterRunner runner(h.cfg, h.clock, sink);
    const auto out = runner.run_offline();
    ASSERT_EQ(out.status, RunStatus::kCompleteOffline) << out.summary;

    const std::string got = read_file(h.cfg.output.file);
    if (reference.empty()) reference = got;
    else EXPECT_EQ(got, reference) << "chunk=" << chunk;
  }
}

// E2E-07, part one: a uniformly slow sink does not fail the run. The reader
// pauses, the queue stays at its cap, and every event still arrives -- just
// at the sink's pace. Dropping events to "keep up" would be the bug.
TEST(OfflineE2E, SlowSinkAppliesBackpressureWithoutLosingAnything) {
  Harness h;
  h.write_stream(contiguous(400));
  h.cfg.input.require_completed_manifest = true;
  h.cfg.output.sink = md::SinkKind::kP04Stub;
  h.cfg.output.file = h.dir.file("p04.ndjson");
  h.cfg.output.stub_delay_us = 500;  // 0.5 ms per event
  h.cfg.output.max_queue_events = 8;
  h.cfg.output.max_queue_bytes = 64 * 1024;
  h.cfg.output.output_stall_deadline_ms = 5000;
  h.cfg.output.shutdown_drain_deadline_ms = 5000;

  md::P04StubSink sink(h.cfg.output.file, h.cfg.output.stub_delay_us);
  ASSERT_EQ(sink.open(), Reason::kNone);
  md::SteadyClock real_clock;  // backpressure budgets are wall-clock budgets
  md::AdapterRunner runner(h.cfg, real_clock, sink);
  const auto out = runner.run_offline();

  EXPECT_EQ(out.status, RunStatus::kCompleteOffline) << out.summary;
  const auto& m = runner.metrics();
  EXPECT_EQ(m.output_success, 400u) << "nothing may be dropped to keep up";
  EXPECT_EQ(m.output_failed_or_pending, 0u);
  EXPECT_GT(m.read_pause_events, 0u) << "the reader must actually have paused";
  EXPECT_GT(m.read_paused_ns, 0u);
  EXPECT_LE(runner.queue().peak_events(), 8u);
  EXPECT_TRUE(m.check_conservation().ok);
}

// E2E-07, part two: a sink that stalls for longer than its budget fails the
// run. Nothing is discarded to make room, and the artifact stays visibly
// unfinished.
TEST(OfflineE2E, StalledSinkFailsWithBackpressureTimeout) {
  Harness h;
  h.write_stream(contiguous(50));
  h.cfg.input.require_completed_manifest = false;
  h.cfg.output.sink = md::SinkKind::kP04Stub;
  h.cfg.output.file = h.dir.file("p04.ndjson");
  h.cfg.output.stub_delay_us = 400'000;  // 400 ms for a single write
  h.cfg.output.max_queue_events = 2;
  h.cfg.output.max_queue_bytes = 8 * 1024;
  h.cfg.output.output_stall_deadline_ms = 100;
  h.cfg.output.shutdown_drain_deadline_ms = 100;

  md::P04StubSink sink(h.cfg.output.file, h.cfg.output.stub_delay_us);
  ASSERT_EQ(sink.open(), Reason::kNone);
  md::SteadyClock real_clock;
  md::AdapterRunner runner(h.cfg, real_clock, sink);
  const auto out = runner.run_offline();

  EXPECT_EQ(out.terminal_reason, Reason::kBackpressureTimeout) << out.summary;
  EXPECT_EQ(out.exit_code, md::ExitCode::kBackpressureTimeout);
  EXPECT_EQ(out.status, RunStatus::kIncomplete);
  EXPECT_LT(runner.metrics().output_success, 50u);
  EXPECT_GT(runner.metrics().output_failed_or_pending, 0u)
      << "events that did not reach the sink must be reported, not forgotten";
  EXPECT_LE(runner.queue().peak_events(), 2u);
  // The artifact stays visibly unfinished.
  EXPECT_FALSE(std::filesystem::exists(h.cfg.output.file + ".complete"));
  EXPECT_TRUE(std::filesystem::exists(h.cfg.output.file + ".part"));
}

TEST(OfflineE2E, ReportReflectsProvenanceAndConservation) {
  Harness h;
  h.write_stream(contiguous(50));
  h.cfg.input.require_completed_manifest = true;

  md::MetricsOnlySink sink;
  md::AdapterRunner runner(h.cfg, h.clock, sink);
  const auto out = runner.run_offline();
  ASSERT_EQ(out.status, RunStatus::kCompleteOffline);

  md::ReportInputs ri;
  ri.cfg = &h.cfg;
  ri.metrics = &runner.metrics();
  ri.outcome = &out;
  ri.tracker = &runner.pipeline().tracker();
  ri.freshness = &runner.pipeline().freshness();
  ri.run_id = "test-run";
  ri.timestamp_utc = "2026-10-30T00:00:00Z";
  ri.input_manifest_frames = 50;
  ri.descriptor_sha256_runtime = md::ProtobufDecoder::descriptor_sha256();

  const std::string yaml = md::render_report_yaml(ri);
  EXPECT_NE(yaml.find("status: COMPLETE_OFFLINE"), std::string::npos);
  EXPECT_NE(yaml.find("all_conservation_equations_hold: true"),
            std::string::npos);
  EXPECT_NE(yaml.find("no_silent_loss_when_full_source_verified: true"),
            std::string::npos);
  // Never asserted by a single run.
  EXPECT_NE(yaml.find("sustainable_throughput_verified: false"),
            std::string::npos);
  EXPECT_NE(yaml.find("release_verdict: NOT_EVALUATED"), std::string::npos);
}

TEST(OfflineE2E, UnverifiedRunReportsUnknownNotZeroLoss) {
  Harness h;
  h.write_stream(contiguous(5), /*manifest=*/false);
  h.cfg.input.require_completed_manifest = false;

  md::MetricsOnlySink sink;
  md::AdapterRunner runner(h.cfg, h.clock, sink);
  const auto out = runner.run_offline();

  md::ReportInputs ri;
  ri.cfg = &h.cfg;
  ri.metrics = &runner.metrics();
  ri.outcome = &out;
  ri.tracker = &runner.pipeline().tracker();
  ri.freshness = &runner.pipeline().freshness();
  ri.run_id = "test";
  ri.timestamp_utc = "2026-10-30T00:00:00Z";

  const std::string yaml = md::render_report_yaml(ri);
  EXPECT_NE(yaml.find("no_silent_loss_when_full_source_verified: null"),
            std::string::npos)
      << "unknown must not be reported as zero loss";
  EXPECT_NE(yaml.find("expected_frames: null"), std::string::npos);
}
