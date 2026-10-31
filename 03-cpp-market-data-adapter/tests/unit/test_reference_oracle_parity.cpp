// Parity between the C++ pipeline and the Python reference model.
//
// The reference model in tools/reference_oracle.py is written from the
// design document, in another language, with a deliberately naive data
// structure (a dict of seen sequences instead of a ring buffer). This test
// generates a trace, asks both to classify it, and compares.
//
// It is skipped, not failed, when Python is unavailable: a missing
// interpreter is a gap in the environment, and recording it as a pass would
// be the exact dishonesty the oracle exists to prevent.
#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <optional>
#include <sstream>

#include "md_adapter/pipeline.hpp"
#include "test_support.hpp"

using namespace mdtest;

namespace {

struct Input {
  std::int64_t sequence;
  std::int64_t event_time_ns;
  std::int64_t bid;
  std::int64_t ask;
};

// The trace exercises every stream-state class the oracle models.
const std::vector<Input>& trace() {
  static const std::vector<Input> t = {
      {1, 1'000'000'000, 109000, 109010},   // OK
      {2, 2'000'000'000, 109001, 109011},   // OK
      {4, 4'000'000'000, 109003, 109013},   // GAP over 3
      {3, 3'000'000'000, 109002, 109012},   // REORDER_LATE
      {2, 2'000'000'000, 109001, 109011},   // DUPLICATE_EXACT
      {2, 2'000'000'000, 109999, 110009},   // DUPLICATE_CONFLICT
      {5, 5'000'000'000, 109004, 109014},   // OK
      {6, 1'000'000'000, 109005, 109015},   // EVENT_TIME_OLD (4 s behind)
      {7, 6'000'000'000, 109006, 109016},   // OK
      {9, 7'000'000'000, 109007, 109017},   // GAP over 8
  };
  return t;
}

std::string python_interpreter() {
  for (const char* candidate : {"python3", "python"}) {
    const std::string probe =
        std::string(candidate) + " -c \"import sys\" 2>/dev/null";
    if (std::system(probe.c_str()) == 0) return candidate;
  }
  return {};
}

// Runs the reference model and returns the sequences it says should be
// published, or nullopt when it could not be run at all.
std::optional<std::vector<std::int64_t>> oracle_published(
    const std::filesystem::path& tools_dir) {
  const std::string py = python_interpreter();
  if (py.empty()) return std::nullopt;

  TempDir dir;
  const std::string script = dir.file("drive.py");
  std::ostringstream src;
  src << "import sys, json\n"
      << "sys.path.insert(0, " << std::quoted(tools_dir.string()) << ")\n"
      << "from reference_oracle import ReferenceModel\n"
      << "events = json.loads(sys.argv[1])\n"
      << "m = ReferenceModel(window=4096, mode='strict_monitor', live=False,\n"
      << "                   event_time_stale_threshold_ns=2000000000)\n"
      << "print(json.dumps(m.published(events)))\n";
  write_file(script, src.str());

  std::ostringstream json;
  json << '[';
  bool first = true;
  for (const auto& e : trace()) {
    if (!first) json << ',';
    first = false;
    json << R"({"dataset_id":"ds-test","symbol":"EURUSD","sequence":)"
         << e.sequence << R"(,"event_time_ns":)" << e.event_time_ns
         << R"(,"price_scale":100000,"kind":"quote","bid_ticks":)" << e.bid
         << R"(,"ask_ticks":)" << e.ask << '}';
  }
  json << ']';

  const std::string out_path = dir.file("out.json");
  const std::string cmd = py + " " + script + " '" + json.str() + "' > " +
                          out_path + " 2>" + dir.file("err.txt");
  if (std::system(cmd.c_str()) != 0) return std::nullopt;

  const std::string out = read_file(out_path);
  if (out.empty()) return std::nullopt;

  std::vector<std::int64_t> seqs;
  std::int64_t value = 0;
  bool in_number = false;
  for (char c : out) {
    if (c >= '0' && c <= '9') {
      value = value * 10 + (c - '0');
      in_number = true;
    } else if (in_number) {
      seqs.push_back(value);
      value = 0;
      in_number = false;
    }
  }
  return seqs;
}

// What the C++ pipeline actually publishes for the same trace.
std::vector<std::int64_t> pipeline_published() {
  md::Config cfg = base_config();
  md::VirtualClock clock(0);
  md::BoundedQueue queue({4096, 8u << 20, 80, 40});
  md::Pipeline pipeline(cfg, clock, queue, false, false);

  std::vector<std::string> payloads;
  for (const auto& e : trace()) {
    EventSpec s;
    s.sequence = e.sequence;
    s.event_time_ns = e.event_time_ns;
    s.bid = e.bid;
    s.ask = e.ask;
    payloads.push_back(encode(s));
  }

  md::FramerLimits fl;
  fl.prefix_deadline_ns = 0;
  fl.header_partial_deadline_ns = 0;
  fl.body_deadline_ns = 0;
  md::Mdp1Framer framer(fl);
  framer.reset(0);
  framer.feed(as_bytes(stream(payloads)), 0, pipeline);

  std::vector<md::ClassifiedEvent> out;
  queue.pop_batch(4096, out);
  queue.release(out);

  std::vector<std::int64_t> seqs;
  seqs.reserve(out.size());
  for (const auto& e : out) seqs.push_back(e.source.sequence);
  return seqs;
}

}  // namespace

TEST(ReferenceOracle, PublicationAgreesWithTheIndependentModel) {
  const auto tools = std::filesystem::path(MD_PROJECT_ROOT) / "tools";
  if (!std::filesystem::exists(tools / "reference_oracle.py")) {
    GTEST_SKIP() << "reference_oracle.py not found at " << tools;
  }
  const auto expected = oracle_published(tools);
  if (!expected.has_value()) {
    GTEST_SKIP() << "no usable Python interpreter; the oracle did not run";
  }

  const auto got = pipeline_published();
  EXPECT_EQ(got, *expected)
      << "the C++ pipeline and the independent reference model disagree; "
         "the design document decides which one is wrong";

  // A guard on the guard: if the oracle silently returned nothing, the
  // comparison above would pass vacuously.
  EXPECT_FALSE(expected->empty());
}
