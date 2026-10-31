// Machine-readable run report.
//
// Shape follows design section 18. The rules it encodes matter more than the
// formatting:
//   * a field that was not measured is `null`, never 0;
//   * `sustainable_throughput_verified` is false unless a benchmark lane
//     proved it, which this binary never asserts on its own;
//   * a run may only say COMPLETE_OFFLINE when the source was verified.
#pragma once

#include <string>
#include <vector>

#include "md_adapter/config.hpp"
#include "md_adapter/metrics.hpp"
#include "md_adapter/pipeline.hpp"

namespace md {

struct ReportInputs {
  const Config* cfg = nullptr;
  const Metrics* metrics = nullptr;
  const RunOutcome* outcome = nullptr;
  const SequenceTracker* tracker = nullptr;
  const FreshnessEngine* freshness = nullptr;

  std::string run_id;
  std::string timestamp_utc;
  std::string input_sha256;
  std::string input_manifest_status;
  std::int64_t input_manifest_frames = -1;
  std::string descriptor_sha256_runtime;
  std::string descriptor_sha256_expected;
  bool descriptor_pinned = false;
  bool descriptor_matches = true;
  std::string sink_artifact;
  std::string sink_artifact_sha256;
  std::uint64_t sink_events = 0;
  std::string command_line;
};

std::string render_report_yaml(const ReportInputs& in);

// Writes the report to `path` (parent directories are created). Returns false
// on any I/O error -- a run that cannot write its evidence is not a run that
// can claim success.
bool write_report(const std::string& path, const std::string& yaml);

// RFC3339 UTC timestamp from the system clock. Used for report metadata only,
// never for classification.
std::string utc_now_iso8601();
std::string make_run_id();

}  // namespace md
