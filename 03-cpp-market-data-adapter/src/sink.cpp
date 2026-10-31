#include "md_adapter/sink.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <thread>

#include "md_adapter/sha256.hpp"

namespace md {
namespace {

constexpr char kHexDigits[] = "0123456789abcdef";

void append_i64(std::int64_t v, std::string& out) {
  char buf[24];
  const int n = std::snprintf(buf, sizeof(buf), "%lld",
                              static_cast<long long>(v));
  if (n > 0) out.append(buf, static_cast<std::size_t>(n));
}

void append_u64(std::uint64_t v, std::string& out) {
  char buf[24];
  const int n = std::snprintf(buf, sizeof(buf), "%llu",
                              static_cast<unsigned long long>(v));
  if (n > 0) out.append(buf, static_cast<std::size_t>(n));
}

void append_flags_array(std::uint32_t flags, std::string& out) {
  std::string names;
  append_quality_flags(flags, names);
  out += '[';
  if (!names.empty()) {
    std::size_t start = 0;
    bool first = true;
    while (start <= names.size()) {
      const std::size_t comma = names.find(',', start);
      const std::string_view tok =
          std::string_view(names).substr(
              start, comma == std::string::npos ? names.size() - start
                                                : comma - start);
      if (!first) out += ',';
      out += '"';
      out += tok;
      out += '"';
      first = false;
      if (comma == std::string::npos) break;
      start = comma + 1;
    }
  }
  out += ']';
}

// Shared core projection, identical in both sinks so the oracle only has to
// understand one shape.
void append_core(const ClassifiedEvent& e, std::string& out) {
  out += "\"dataset_id\":";
  json_escape(e.source.dataset_id, out);
  out += ",\"symbol\":";
  json_escape(e.source.symbol, out);
  out += ",\"sequence\":";
  append_i64(e.source.sequence, out);
  out += ",\"event_time_ns\":";
  append_i64(e.source.event_time_ns, out);
  out += ",\"price_scale\":";
  append_i64(e.source.price_scale, out);
  out += ",\"source_schema_version\":";
  json_escape(e.source.source_schema_version, out);
  if (kind_of(e.source.payload) == PayloadKind::kQuote) {
    const auto& q = std::get<QuotePrice>(e.source.payload);
    out += ",\"kind\":\"quote\",\"bid_ticks\":";
    append_i64(q.bid, out);
    out += ",\"ask_ticks\":";
    append_i64(q.ask, out);
  } else {
    const auto& t = std::get<TickPrice>(e.source.payload);
    out += ",\"kind\":\"tick\",\"price_ticks\":";
    append_i64(t.price, out);
  }
}

Reason fsync_and_rename(std::FILE* f, const std::string& part,
                        const std::string& final_path) {
  if (std::fflush(f) != 0) return Reason::kSinkIoError;
  const int fd = ::fileno(f);
  if (fd < 0 || ::fsync(fd) != 0) return Reason::kSinkIoError;
  if (std::fclose(f) != 0) return Reason::kSinkIoError;
  std::error_code ec;
  std::filesystem::rename(part, final_path, ec);
  if (ec) return Reason::kSinkIoError;

  // Rename durability needs the containing directory synced too; without it a
  // crash can leave the artifact missing while the marker claims COMPLETE.
  const std::filesystem::path dir =
      std::filesystem::path(final_path).parent_path();
  const int dfd = ::open(dir.empty() ? "." : dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (dfd >= 0) {
    (void)::fsync(dfd);
    ::close(dfd);
  }
  return Reason::kNone;
}

Reason write_completion_marker(const std::string& final_path,
                               std::uint64_t events, std::uint64_t bytes,
                               const std::string& digest) {
  const std::string marker = final_path + ".complete";
  std::FILE* m = std::fopen(marker.c_str(), "wb");
  if (m == nullptr) return Reason::kSinkIoError;
  std::string s = "{\"artifact\":";
  json_escape(std::filesystem::path(final_path).filename().string(), s);
  s += ",\"events\":";
  append_u64(events, s);
  s += ",\"bytes\":";
  append_u64(bytes, s);
  s += ",\"sha256\":\"";
  s += digest;
  s += "\"}\n";
  const bool ok = std::fwrite(s.data(), 1, s.size(), m) == s.size();
  const int fd = ::fileno(m);
  const bool synced = fd >= 0 && std::fflush(m) == 0 && ::fsync(fd) == 0;
  const bool closed = std::fclose(m) == 0;
  return (ok && synced && closed) ? Reason::kNone : Reason::kSinkIoError;
}

Reason open_part(const std::string& final_path, std::string& part_path,
                 std::FILE*& f) {
  const std::filesystem::path p(final_path);
  if (p.has_parent_path()) {
    std::error_code ec;
    std::filesystem::create_directories(p.parent_path(), ec);
    if (ec) return Reason::kSinkIoError;
  }
  part_path = final_path + ".part";
  f = std::fopen(part_path.c_str(), "wb");
  return f == nullptr ? Reason::kSinkIoError : Reason::kNone;
}

}  // namespace

void json_escape(std::string_view s, std::string& out) {
  out += '"';
  for (char raw : s) {
    const auto c = static_cast<unsigned char>(raw);
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        // Escape control characters and any non-ASCII byte. Field content can
        // come from an untrusted feed, so nothing raw reaches a log or an
        // artifact a human will open.
        if (c < 0x20 || c > 0x7e) {
          out += "\\u00";
          out += kHexDigits[c >> 4];
          out += kHexDigits[c & 0x0f];
        } else {
          out += static_cast<char>(c);
        }
    }
  }
  out += '"';
}

// ---------------------------------------------------------------------------

Reason MetricsOnlySink::write(const ClassifiedEvent&) {
  ++events_written_;
  return Reason::kNone;
}

// ---------------------------------------------------------------------------

NdjsonTestSink::NdjsonTestSink(std::string path)
    : final_path_(std::move(path)) {}

NdjsonTestSink::~NdjsonTestSink() {
  if (f_ != nullptr) std::fclose(f_);
}

Reason NdjsonTestSink::open() {
  const Reason r = open_part(final_path_, part_path_, f_);
  if (r == Reason::kNone) artifact_path_ = part_path_;
  return r;
}

Reason NdjsonTestSink::write(const ClassifiedEvent& e) {
  if (f_ == nullptr) return Reason::kSinkIoError;
  std::string line = "{\"arrival_index\":";
  append_u64(e.arrival_index, line);
  line += ',';
  append_core(e, line);
  line += ",\"quality_flags\":";
  append_flags_array(e.quality_flags, line);
  line += ",\"feed_state\":\"";
  line += to_string(e.feed_state);
  line += "\",\"primary_reason\":\"";
  line += to_string(e.primary_reason);
  line += "\"}\n";

  if (std::fwrite(line.data(), 1, line.size(), f_) != line.size())
    return Reason::kSinkIoError;
  ++events_written_;
  bytes_written_ += line.size();
  return Reason::kNone;
}

Reason NdjsonTestSink::commit() {
  if (f_ == nullptr) return Reason::kSinkIoError;
  const Reason r = fsync_and_rename(f_, part_path_, final_path_);
  f_ = nullptr;
  if (r != Reason::kNone) return r;
  artifact_sha256_ = sha256_file(final_path_);
  artifact_path_ = final_path_;
  committed_ = true;
  return write_completion_marker(final_path_, events_written_, bytes_written_,
                                 artifact_sha256_);
}

void NdjsonTestSink::abort() {
  if (f_ != nullptr) {
    (void)std::fflush(f_);
    std::fclose(f_);
    f_ = nullptr;
  }
  // The .part file is deliberately left in place: an incomplete run must be
  // visible on disk, not tidied away.
  artifact_path_ = part_path_;
}

// ---------------------------------------------------------------------------

P04StubSink::P04StubSink(std::string path, std::int64_t artificial_delay_us)
    : final_path_(std::move(path)), delay_us_(artificial_delay_us) {}

P04StubSink::~P04StubSink() {
  if (f_ != nullptr) std::fclose(f_);
}

Reason P04StubSink::open() {
  const Reason r = open_part(final_path_, part_path_, f_);
  if (r == Reason::kNone) artifact_path_ = part_path_;
  return r;
}

Reason P04StubSink::write(const ClassifiedEvent& e) {
  if (f_ == nullptr) return Reason::kSinkIoError;
  if (delay_us_ > 0)
    std::this_thread::sleep_for(std::chrono::microseconds(delay_us_));

  // record.key = symbol, record.value = raw MarketDataEvent bytes.
  std::string line = "{\"key\":";
  json_escape(e.source.symbol, line);
  line += ",\"value_hex\":\"";
  for (char raw : e.raw_wire) {
    const auto c = static_cast<unsigned char>(raw);
    line += kHexDigits[c >> 4];
    line += kHexDigits[c & 0x0f];
  }
  line += "\",\"value_bytes\":";
  append_u64(e.raw_wire.size(), line);
  // Data quality travels on a side channel with its own shape. It is never
  // mixed into record.value, which must stay byte-identical to the P02 wire
  // message (ADR-14).
  line += ",\"quality\":{\"flags\":";
  append_flags_array(e.quality_flags, line);
  line += ",\"feed_state\":\"";
  line += to_string(e.feed_state);
  line += "\",\"primary_reason\":\"";
  line += to_string(e.primary_reason);
  line += "\",\"arrival_index\":";
  append_u64(e.arrival_index, line);
  line += ",\"event_id\":{";
  append_core(e, line);
  line += "}}}\n";

  if (std::fwrite(line.data(), 1, line.size(), f_) != line.size())
    return Reason::kSinkIoError;
  ++events_written_;
  bytes_written_ += line.size();
  return Reason::kNone;
}

Reason P04StubSink::commit() {
  if (f_ == nullptr) return Reason::kSinkIoError;
  const Reason r = fsync_and_rename(f_, part_path_, final_path_);
  f_ = nullptr;
  if (r != Reason::kNone) return r;
  artifact_sha256_ = sha256_file(final_path_);
  artifact_path_ = final_path_;
  committed_ = true;
  return write_completion_marker(final_path_, events_written_, bytes_written_,
                                 artifact_sha256_);
}

void P04StubSink::abort() {
  if (f_ != nullptr) {
    (void)std::fflush(f_);
    std::fclose(f_);
    f_ = nullptr;
  }
  artifact_path_ = part_path_;
}

}  // namespace md
