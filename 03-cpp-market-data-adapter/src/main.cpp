// md_adapter -- Project 03 FX/CFD market data adapter.
//
// Exit codes are part of the contract (md::ExitCode): a harness asserts on
// them instead of grepping stderr.
#include <csignal>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <memory>
#include <string>
#include <vector>

#include "md_adapter/pipeline.hpp"
#include "md_adapter/report.hpp"
#include "md_adapter/sha256.hpp"

namespace {

void on_signal(int) {
  // Async-signal-safe: only a flag is touched. Draining happens on the main
  // loop, which checks this every iteration.
  md::AdapterRunner::shutdown_flag().store(true);
}

void usage() {
  std::cerr
      << "md_adapter --config <file.toml> [options]\n"
         "\n"
         "  --config <path>         Required. TOML configuration.\n"
         "  --input-mode <m>        pbstream | tcp (overrides config)\n"
         "  --input <path>          .pbstream file (pbstream mode)\n"
         "  --host <h> --port <p>   Endpoint (tcp mode)\n"
         "  --output <path>         Sink artifact path\n"
         "  --sink <kind>           metrics_only | ndjson_test | p04_stub\n"
         "  --validation-mode <m>   strict_monitor | fault_observe | "
         "offline_verify\n"
         "  --report <path>         Write the YAML run report here\n"
         "  --print-descriptor      Print the runtime descriptor digest and "
         "exit\n"
         "  --check-config          Validate the configuration and exit\n"
         "  --help\n";
}

struct Cli {
  std::string config;
  std::string report;
  std::vector<std::pair<std::string, std::string>> overrides;
  bool check_config = false;
  bool print_descriptor = false;
  bool help = false;
};

bool parse_cli(int argc, char** argv, Cli& cli, std::string& err) {
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    const auto need = [&](std::string& dst) {
      if (i + 1 >= argc) {
        err = a + " requires a value";
        return false;
      }
      dst = argv[++i];
      return true;
    };
    const auto need_override = [&](const char* key) {
      if (i + 1 >= argc) {
        err = a + " requires a value";
        return false;
      }
      cli.overrides.emplace_back(key, argv[++i]);
      return true;
    };

    if (a == "--help" || a == "-h") {
      cli.help = true;
    } else if (a == "--config") {
      if (!need(cli.config)) return false;
    } else if (a == "--report") {
      if (!need(cli.report)) return false;
    } else if (a == "--input-mode") {
      if (!need_override("input.mode")) return false;
    } else if (a == "--input") {
      if (!need_override("input.file")) return false;
    } else if (a == "--host") {
      if (!need_override("input.host")) return false;
    } else if (a == "--port") {
      if (!need_override("input.port")) return false;
    } else if (a == "--output") {
      if (!need_override("output.file")) return false;
    } else if (a == "--sink") {
      if (!need_override("output.sink")) return false;
    } else if (a == "--validation-mode") {
      if (!need_override("validation.mode")) return false;
    } else if (a == "--check-config") {
      cli.check_config = true;
    } else if (a == "--print-descriptor") {
      cli.print_descriptor = true;
    } else {
      err = "unknown argument: " + a;
      return false;
    }
  }
  return true;
}

// Renders a value the way the TOML subset expects it.
std::string render_value(const std::string& v) {
  if (v == "true" || v == "false") return v;
  const bool numeric =
      !v.empty() && v.find_first_not_of("0123456789-+_") == std::string::npos;
  return numeric ? v : ("\"" + v + "\"");
}

// Applies overrides by rewriting the key *in place* inside its table, so the
// result goes through exactly the same parser and validator as the file
// itself -- there is no second, looser code path for command-line values.
//
// Rewriting rather than appending matters: the parser rejects duplicate
// keys, which is a deliberate contract, so an appended override of a key the
// file already defines would be refused rather than applied.
std::string apply_overrides(
    const std::string& toml,
    const std::vector<std::pair<std::string, std::string>>& ov) {
  std::string out = toml;
  for (const auto& kv : ov) {
    const std::size_t dot = kv.first.find('.');
    const std::string table = kv.first.substr(0, dot);
    const std::string key = kv.first.substr(dot + 1);
    const std::string header = "[" + table + "]";
    const std::string line = key + " = " + render_value(kv.second);

    std::size_t body = std::string::npos;
    if (out.rfind(header, 0) == 0) {
      body = header.size();
    } else {
      const std::size_t sec = out.find("\n" + header);
      if (sec != std::string::npos) body = sec + 1 + header.size();
    }
    if (body == std::string::npos) {
      out += "\n" + header + "\n" + line + "\n";  // no such table yet
      continue;
    }

    const std::size_t next = out.find("\n[", body);
    const std::size_t limit = (next == std::string::npos) ? out.size() : next;

    // Find an existing definition of this key, inside this table only.
    std::size_t at = std::string::npos;
    std::size_t scan = body;
    while (scan < limit) {
      std::size_t eol = out.find('\n', scan + 1);
      if (eol == std::string::npos || eol > limit) eol = limit;
      const std::size_t first = out.find_first_not_of(" \t\r\n", scan);
      if (first != std::string::npos && first < eol &&
          out.compare(first, key.size(), key) == 0) {
        const std::size_t after =
            out.find_first_not_of(" \t", first + key.size());
        if (after != std::string::npos && after < eol && out[after] == '=') {
          at = first;
          break;
        }
      }
      if (eol >= limit) break;
      scan = eol;
    }

    if (at != std::string::npos) {
      std::size_t eol = out.find('\n', at);
      if (eol == std::string::npos) eol = out.size();
      out.replace(at, eol - at, line);
    } else {
      out.insert(limit, "\n" + line);
    }
  }
  return out;
}

std::string join_args(int argc, char** argv) {
  std::string s;
  for (int i = 0; i < argc; ++i) {
    if (i != 0) s += ' ';
    s += argv[i];
  }
  return s;
}

}  // namespace

int main(int argc, char** argv) {
  md::silence_protobuf_logging();

  Cli cli;
  std::string err;
  if (!parse_cli(argc, argv, cli, err)) {
    std::cerr << "md_adapter: " << err << "\n";
    usage();
    return static_cast<int>(md::ExitCode::kConfigInvalid);
  }
  if (cli.help) {
    usage();
    return 0;
  }
  if (cli.print_descriptor) {
    std::cout << md::ProtobufDecoder::descriptor_sha256() << "\n";
    return 0;
  }
  if (cli.config.empty()) {
    std::cerr << "md_adapter: --config is required\n";
    usage();
    return static_cast<int>(md::ExitCode::kConfigInvalid);
  }

  // --- configuration -------------------------------------------------------
  std::string toml_text;
  {
    std::ifstream in(cli.config, std::ios::binary);
    if (!in) {
      std::cerr << "md_adapter: cannot read config: " << cli.config << "\n";
      return static_cast<int>(md::ExitCode::kConfigInvalid);
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    toml_text = ss.str();
  }
  const std::string effective = apply_overrides(toml_text, cli.overrides);

  md::Config cfg;
  std::vector<std::string> errors;
  if (!md::Config::load_from_string(effective, cli.config, cfg, errors)) {
    std::cerr << "md_adapter: configuration rejected (" << errors.size()
              << " problem(s)):\n";
    for (const auto& e : errors) std::cerr << "  - " << e << "\n";
    return static_cast<int>(md::ExitCode::kConfigInvalid);
  }

  // --- pinned contract -----------------------------------------------------
  const auto desc =
      md::ProtobufDecoder::check_descriptor(cfg.identity.expected_descriptor_sha256);
  if (!desc.matches) {
    std::cerr << "md_adapter: protobuf descriptor mismatch\n"
              << "  expected: " << desc.expected_sha256 << "\n"
              << "  runtime:  " << desc.actual_sha256 << "\n";
    return static_cast<int>(md::ExitCode::kConfigInvalid);
  }
  if (!desc.pinned) {
    std::cerr << "md_adapter: WARNING descriptor is UNPINNED; the wire "
                 "contract is not verified. Runtime digest "
              << desc.actual_sha256 << "\n";
  }

  if (cli.check_config) {
    std::cout << "config OK: " << cli.config << " sha256=" << cfg.source_sha256
              << "\n";
    return 0;
  }

  // --- signals -------------------------------------------------------------
  struct ::sigaction sa {};
  sa.sa_handler = &on_signal;
  ::sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;  // no SA_RESTART: a blocking call should return EINTR
  ::sigaction(SIGTERM, &sa, nullptr);
  ::sigaction(SIGINT, &sa, nullptr);
  // A failed sink write must surface as an error, not kill the process.
  ::signal(SIGPIPE, SIG_IGN);

  // --- sink ----------------------------------------------------------------
  std::unique_ptr<md::Sink> sink;
  switch (cfg.output.sink) {
    case md::SinkKind::kMetricsOnly:
      sink = std::make_unique<md::MetricsOnlySink>();
      break;
    case md::SinkKind::kNdjsonTest: {
      auto s = std::make_unique<md::NdjsonTestSink>(cfg.output.file);
      if (s->open() != md::Reason::kNone) {
        std::cerr << "md_adapter: cannot open sink: " << cfg.output.file << "\n";
        return static_cast<int>(md::ExitCode::kSinkIoError);
      }
      sink = std::move(s);
      break;
    }
    case md::SinkKind::kP04Stub: {
      auto s = std::make_unique<md::P04StubSink>(cfg.output.file,
                                                 cfg.output.stub_delay_us);
      if (s->open() != md::Reason::kNone) {
        std::cerr << "md_adapter: cannot open sink: " << cfg.output.file << "\n";
        return static_cast<int>(md::ExitCode::kSinkIoError);
      }
      sink = std::move(s);
      break;
    }
  }

  // --- clock ---------------------------------------------------------------
  // Offline replay runs on an injected clock so classification never depends
  // on how fast the machine happens to read a file.
  md::SteadyClock steady;
  md::VirtualClock virt(0);
  md::Clock* clock = &steady;
  if (cfg.freshness.clock_mode == md::ClockMode::kVirtualForReplay &&
      cfg.input.mode == md::InputMode::kPbstream) {
    clock = &virt;
  }

  // --- run -----------------------------------------------------------------
  md::AdapterRunner runner(cfg, *clock, *sink);
  const md::RunOutcome outcome = (cfg.input.mode == md::InputMode::kPbstream)
                                     ? runner.run_offline()
                                     : runner.run_live();

  // --- report --------------------------------------------------------------
  md::ReportInputs ri;
  ri.cfg = &cfg;
  ri.metrics = &runner.metrics();
  ri.outcome = &outcome;
  ri.tracker = &runner.pipeline().tracker();
  ri.freshness = &runner.pipeline().freshness();
  ri.run_id = md::make_run_id();
  ri.timestamp_utc = md::utc_now_iso8601();
  ri.descriptor_sha256_runtime = desc.actual_sha256;
  ri.descriptor_sha256_expected = desc.expected_sha256;
  ri.descriptor_pinned = desc.pinned;
  ri.descriptor_matches = desc.matches;
  ri.sink_artifact = sink->artifact_path();
  ri.sink_artifact_sha256 = sink->artifact_sha256();
  ri.sink_events = sink->events_written();
  ri.command_line = join_args(argc, argv);
  if (cfg.input.mode == md::InputMode::kPbstream) {
    ri.input_sha256 = md::sha256_file(cfg.input.file);
    std::vector<std::string> notes;
    const auto man = md::Mdp1FileSource::load_manifest(cfg.input.file, notes);
    ri.input_manifest_status = man.present ? man.status : "ABSENT";
    ri.input_manifest_frames = man.frames;
  }

  const std::string yaml = md::render_report_yaml(ri);
  if (!cli.report.empty() && !md::write_report(cli.report, yaml)) {
    std::cerr << "md_adapter: cannot write report: " << cli.report << "\n";
    return static_cast<int>(md::ExitCode::kSinkIoError);
  }

  std::cerr << "md_adapter: status=" << md::to_string(outcome.status)
            << " reason=" << md::to_string(outcome.terminal_reason)
            << " frames=" << runner.metrics().frames_complete
            << " business_ok=" << runner.metrics().business_ok
            << " published=" << runner.metrics().output_success
            << " suppressed=" << runner.metrics().policy_suppressed << "\n";
  if (!outcome.summary.empty())
    std::cerr << "md_adapter: " << outcome.summary << "\n";

  return static_cast<int>(outcome.exit_code);
}
