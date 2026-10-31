// md_cpp_decode: read an MDP1 .pbstream, emit canonical projection JSONL.
//
// This is the C++ half of the cross-language oracle. It prints a JSON run report
// on stdout with counts broken down by stage, because a decoder that reports
// only "done" cannot be reconciled: the suite needs frames_read, decode_errors,
// envelope_rejects and events_accepted separately to tell a clean run from one
// that lost events quietly (section 6.3).
#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "md/contract.hpp"

namespace {

struct Options {
  std::string input;
  std::string output;
  // Keeps going past a rejected frame, reporting each one. Only safe for an
  // offline corpus: a live strict stream must stop at the first structural
  // failure rather than guess where the next frame starts (ADR-007).
  bool continue_on_error = false;
  std::size_t max_payload_bytes = md::kMaxPayloadBytes;
  bool envelope_check = true;
};

void PrintUsage() {
  std::fprintf(stderr,
               "usage: md_cpp_decode --input FILE.pbstream [--output FILE.jsonl]\n"
               "                     [--continue-on-error] [--no-envelope-check]\n"
               "                     [--max-payload-bytes N]\n");
}

bool ParseArgs(int argc, char** argv, Options& opts) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto next = [&](std::string& dst) {
      if (i + 1 >= argc) return false;
      dst = argv[++i];
      return true;
    };
    if (arg == "--input") { if (!next(opts.input)) return false; }
    else if (arg == "--output") { if (!next(opts.output)) return false; }
    else if (arg == "--continue-on-error") { opts.continue_on_error = true; }
    else if (arg == "--no-envelope-check") { opts.envelope_check = false; }
    else if (arg == "--max-payload-bytes") {
      std::string value;
      if (!next(value)) return false;
      opts.max_payload_bytes = static_cast<std::size_t>(std::strtoull(value.c_str(), nullptr, 10));
    } else if (arg == "--help" || arg == "-h") { PrintUsage(); std::exit(0); }
    else { std::fprintf(stderr, "unknown argument: %s\n", arg.c_str()); return false; }
  }
  return !opts.input.empty();
}

}  // namespace

int main(int argc, char** argv) {
  Options opts;
  if (!ParseArgs(argc, argv, opts)) {
    PrintUsage();
    return 2;
  }

  std::FILE* in = std::fopen(opts.input.c_str(), "rb");
  if (in == nullptr) {
    std::fprintf(stderr, "cannot open %s\n", opts.input.c_str());
    return 2;
  }
  std::FILE* out = nullptr;
  if (!opts.output.empty()) {
    out = std::fopen(opts.output.c_str(), "wb");
    if (out == nullptr) {
      std::fprintf(stderr, "cannot write %s\n", opts.output.c_str());
      std::fclose(in);
      return 2;
    }
  }

  md::FileSource source(in);
  md::FrameReader reader(source, opts.max_payload_bytes);

  std::int64_t accepted = 0, decode_errors = 0, envelope_rejects = 0, framing_errors = 0;
  std::map<std::string, std::int64_t> reasons;
  std::string first_error;
  std::string payload;
  md::Error error;

  while (true) {
    if (!reader.ReadFrame(payload, error)) {
      if (reader.end() == md::StreamEnd::kCompleted) break;
      ++framing_errors;
      ++reasons[std::string(md::ReasonName(error.reason))];
      if (first_error.empty()) first_error = error.ToString();
      break;  // framing failure always stops the stream; no resync
    }

    md::DomainEvent event;
    md::Error decode = md::DecodePayload(
        std::span<const char>(payload.data(), payload.size()), event);
    if (!decode.ok()) {
      ++decode_errors;
      ++reasons[std::string(md::ReasonName(decode.reason))];
      if (first_error.empty()) first_error = decode.ToString();
      if (!opts.continue_on_error) break;
      continue;
    }

    if (opts.envelope_check) {
      md::Error envelope = md::CheckEnvelope(event);
      if (!envelope.ok()) {
        ++envelope_rejects;
        ++reasons[std::string(md::ReasonName(envelope.reason))];
        if (first_error.empty()) first_error = envelope.ToString();
        if (!opts.continue_on_error) break;
        continue;
      }
    }

    ++accepted;
    if (out != nullptr) {
      const std::string line = md::ProjectionJson(event);
      std::fwrite(line.data(), 1, line.size(), out);
      std::fputc('\n', out);
    }
  }

  if (out != nullptr) {
    std::fflush(out);
    std::fclose(out);
  }
  std::fclose(in);

  // The report is machine-readable because the Python conformance runner reads
  // it; `verdict` is deliberately not just "ok", since an INCOMPLETE stream and
  // a FAILED one call for different operator responses.
  std::string report = "{";
  report += "\"events_accepted\":" + std::to_string(accepted);
  report += ",\"frames_read\":" + std::to_string(reader.frames_read());
  report += ",\"framing_errors\":" + std::to_string(framing_errors);
  report += ",\"decode_errors\":" + std::to_string(decode_errors);
  report += ",\"envelope_rejects\":" + std::to_string(envelope_rejects);
  report += ",\"stream_end\":\"" + std::string(md::StreamEndName(reader.end())) + "\"";
  report += ",\"final_offset\":" + std::to_string(reader.offset());
  report += ",\"cpp_protobuf_runtime_version\":\"" + md::ProtobufRuntimeVersion() + "\"";
  report += ",\"first_error\":";
  std::string escaped;
  md::AppendJsonString(escaped, first_error);
  report += escaped;
  report += ",\"reasons\":{";
  bool first = true;
  for (const auto& [name, count] : reasons) {
    if (!first) report += ',';
    first = false;
    std::string key;
    md::AppendJsonString(key, name);
    report += key + ":" + std::to_string(count);
  }
  report += "}}";
  std::printf("%s\n", report.c_str());

  const bool clean =
      framing_errors == 0 && decode_errors == 0 && envelope_rejects == 0 &&
      reader.end() == md::StreamEnd::kCompleted;
  // Nonzero exit on any rejected input, so a CI step cannot pass while the
  // stream was partially unreadable (NFR-12).
  return clean ? 0 : 1;
}
