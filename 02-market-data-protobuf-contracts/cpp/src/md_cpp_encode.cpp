// md_cpp_encode: read sim.v1 JSONL, emit an MDP1 .pbstream.
//
// The reverse direction of md_cpp_decode, and it exists so the matrix can be
// run both ways. E2E-01 passing (Python encode -> C++ decode) says nothing
// about C++ encode -> Python decode: the two directions exercise different
// code, and a bug in the C++ encoder would be invisible if only the first
// direction were tested (E2E-02).
//
// Also emits --projection-out so the encoder's own view of each event can be
// compared against what the input said and against what Python would produce
// from the same line. That makes the C++ strict JSON reader differentially
// testable against the Python one rather than only via Protobuf bytes.
#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include <vector>

#include "md/contract.hpp"

namespace {

struct Options {
  std::string input;
  std::string output;
  std::string projection_out;
  bool continue_on_error = false;
};

void PrintUsage() {
  std::fprintf(stderr,
               "usage: md_cpp_encode --input FILE.jsonl --output FILE.pbstream\n"
               "                     [--projection-out FILE.jsonl] [--continue-on-error]\n");
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
    else if (arg == "--projection-out") { if (!next(opts.projection_out)) return false; }
    else if (arg == "--continue-on-error") { opts.continue_on_error = true; }
    else if (arg == "--help" || arg == "-h") { PrintUsage(); std::exit(0); }
    else { std::fprintf(stderr, "unknown argument: %s\n", arg.c_str()); return false; }
  }
  return !opts.input.empty() && !opts.output.empty();
}

// Reads one line including an arbitrarily long one, so an over-long line is
// measured and rejected by the parser rather than silently split in two here.
bool ReadLine(std::FILE* handle, std::string& line) {
  line.clear();
  int ch;
  while ((ch = std::fgetc(handle)) != EOF) {
    if (ch == '\n') return true;
    line += static_cast<char>(ch);
  }
  return !line.empty();
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

  // Write to a .part and rename only on success, so an aborted run cannot leave
  // a file that looks like a complete stream (cases I10-I12).
  const std::string part = opts.output + ".part";
  std::FILE* out = std::fopen(part.c_str(), "wb");
  if (out == nullptr) {
    std::fprintf(stderr, "cannot write %s\n", part.c_str());
    std::fclose(in);
    return 2;
  }
  std::FILE* proj = nullptr;
  if (!opts.projection_out.empty()) {
    proj = std::fopen(opts.projection_out.c_str(), "wb");
    if (proj == nullptr) {
      std::fprintf(stderr, "cannot write %s\n", opts.projection_out.c_str());
      std::fclose(in);
      std::fclose(out);
      return 2;
    }
  }

  md::FrameWriter writer(out);
  std::int64_t lines = 0, encoded = 0, rejected = 0;
  std::map<std::string, std::int64_t> reasons;
  std::string first_error;
  std::string line, payload;

  while (ReadLine(in, line)) {
    if (line.empty()) continue;
    ++lines;

    md::DomainEvent event;
    md::Error parse = md::ParseSimV1Line(line, event);
    if (!parse.ok()) {
      ++rejected;
      ++reasons[std::string(md::ReasonName(parse.reason))];
      if (first_error.empty()) first_error = parse.ToString();
      if (!opts.continue_on_error) break;
      continue;
    }

    md::Error encode = md::EncodeEvent(event, payload);
    if (!encode.ok()) {
      ++rejected;
      ++reasons[std::string(md::ReasonName(encode.reason))];
      if (first_error.empty()) first_error = encode.ToString();
      if (!opts.continue_on_error) break;
      continue;
    }

    md::Error framed = writer.WriteFrame(payload);
    if (!framed.ok()) {
      ++rejected;
      ++reasons[std::string(md::ReasonName(framed.reason))];
      if (first_error.empty()) first_error = framed.ToString();
      break;
    }
    ++encoded;
    if (proj != nullptr) {
      const std::string projection = md::ProjectionJson(event);
      std::fwrite(projection.data(), 1, projection.size(), proj);
      std::fputc('\n', proj);
    }
  }

  std::fflush(out);
  std::fclose(out);
  std::fclose(in);
  if (proj != nullptr) {
    std::fflush(proj);
    std::fclose(proj);
  }

  const bool clean = rejected == 0;
  if (clean) {
    std::remove(opts.output.c_str());
    if (std::rename(part.c_str(), opts.output.c_str()) != 0) {
      std::fprintf(stderr, "rename failed for %s\n", opts.output.c_str());
      return 1;
    }
  } else {
    // Deliberately left as .part: a partial stream must not be mistaken for a
    // complete one just because some frames were written.
    std::fprintf(stderr, "left incomplete output at %s\n", part.c_str());
  }

  std::string report = "{";
  report += "\"json_lines_read\":" + std::to_string(lines);
  report += ",\"events_encoded\":" + std::to_string(encoded);
  report += ",\"frames_written\":" + std::to_string(writer.frames_written());
  report += ",\"rejected\":" + std::to_string(rejected);
  report += ",\"committed\":" + std::string(clean ? "true" : "false");
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

  return clean ? 0 : 1;
}
