#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <map>
#include <sstream>

#include "md_adapter/input_source.hpp"
#include "md_adapter/sha256.hpp"

namespace md {
namespace {

// Minimal reader for a flat JSON object of string and integer values. The
// manifest contract is deliberately flat, so a full JSON parser would be more
// surface than the job needs.
class FlatJson {
 public:
  bool parse(std::string_view s, std::string& err) {
    std::size_t i = skip_ws(s, 0);
    if (i >= s.size() || s[i] != '{') {
      err = "expected '{'";
      return false;
    }
    ++i;
    i = skip_ws(s, i);
    if (i < s.size() && s[i] == '}') return true;
    while (i < s.size()) {
      std::string key;
      if (!read_string(s, i, key, err)) return false;
      i = skip_ws(s, i);
      if (i >= s.size() || s[i] != ':') {
        err = "expected ':'";
        return false;
      }
      i = skip_ws(s, i + 1);
      if (i >= s.size()) {
        err = "truncated value";
        return false;
      }
      if (s[i] == '"') {
        std::string v;
        if (!read_string(s, i, v, err)) return false;
        strings_[key] = v;
      } else {
        const std::size_t start = i;
        while (i < s.size() && s[i] != ',' && s[i] != '}') ++i;
        std::string raw(s.substr(start, i - start));
        while (!raw.empty() && (raw.back() == ' ' || raw.back() == '\n' ||
                                raw.back() == '\r' || raw.back() == '\t'))
          raw.pop_back();
        if (raw == "true" || raw == "false") {
          strings_[key] = raw;
        } else {
          try {
            ints_[key] = std::stoll(raw);
          } catch (...) {
            err = "value for '" + key + "' is not a string or integer";
            return false;
          }
        }
      }
      i = skip_ws(s, i);
      if (i < s.size() && s[i] == ',') {
        i = skip_ws(s, i + 1);
        continue;
      }
      if (i < s.size() && s[i] == '}') return true;
      err = "expected ',' or '}'";
      return false;
    }
    err = "unterminated object";
    return false;
  }

  std::string str(const std::string& k) const {
    const auto it = strings_.find(k);
    return it == strings_.end() ? std::string() : it->second;
  }
  std::int64_t num(const std::string& k, std::int64_t dflt) const {
    const auto it = ints_.find(k);
    return it == ints_.end() ? dflt : it->second;
  }

 private:
  static std::size_t skip_ws(std::string_view s, std::size_t i) {
    while (i < s.size() && (s[i] == ' ' || s[i] == '\n' || s[i] == '\r' ||
                            s[i] == '\t'))
      ++i;
    return i;
  }
  static bool read_string(std::string_view s, std::size_t& i, std::string& out,
                          std::string& err) {
    i = skip_ws(s, i);
    if (i >= s.size() || s[i] != '"') {
      err = "expected string";
      return false;
    }
    ++i;
    out.clear();
    while (i < s.size() && s[i] != '"') {
      if (s[i] == '\\' && i + 1 < s.size()) {
        ++i;
        switch (s[i]) {
          case 'n': out += '\n'; break;
          case 't': out += '\t'; break;
          case 'r': out += '\r'; break;
          default: out += s[i];
        }
      } else {
        out += s[i];
      }
      ++i;
    }
    if (i >= s.size()) {
      err = "unterminated string";
      return false;
    }
    ++i;
    return true;
  }

  std::map<std::string, std::string> strings_;
  std::map<std::string, std::int64_t> ints_;
};

}  // namespace

PbstreamManifest Mdp1FileSource::load_manifest(const std::string& pbstream_path,
                                               std::vector<std::string>& notes) {
  PbstreamManifest m;
  const std::string path = pbstream_path + ".manifest.json";
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    notes.push_back("manifest not found: " + path);
    return m;
  }
  m.present = true;
  std::ostringstream ss;
  ss << in.rdbuf();
  FlatJson j;
  std::string err;
  if (!j.parse(ss.str(), err)) {
    notes.push_back("manifest is not valid JSON (" + err + "): " + path);
    return m;
  }
  m.parsed = true;
  m.dataset_id = j.str("dataset_id");
  m.sha256 = j.str("sha256");
  m.status = j.str("status");
  m.descriptor_sha256 = j.str("schema_descriptor_sha256");
  m.frames = j.num("frames", -1);
  m.bytes = j.num("bytes", -1);
  return m;
}

Mdp1FileSource::Mdp1FileSource(std::string path, bool require_manifest)
    : path_(std::move(path)), require_manifest_(require_manifest) {}

Mdp1FileSource::~Mdp1FileSource() {
  if (fd_ >= 0) ::close(fd_);
}

Reason Mdp1FileSource::open() {
  // A ".part" input is an unfinished artifact by construction; reading it is
  // only allowed as an explicit salvage, never as a verified source.
  if (path_.size() > 5 && path_.compare(path_.size() - 5, 5, ".part") == 0) {
    notes_.push_back("input is a .part file: provenance cannot be VERIFIED");
    if (require_manifest_) return Reason::kSourceUnverified;
  }

  fd_ = ::open(path_.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd_ < 0) {
    notes_.push_back(std::string("cannot open input: ") + std::strerror(errno));
    return Reason::kIoError;
  }

  struct ::stat st {};
  if (::fstat(fd_, &st) == 0) file_bytes_ = static_cast<std::int64_t>(st.st_size);

  manifest_ = load_manifest(path_, notes_);
  provenance_ = Provenance::kVerifiedPartialFile;

  if (!manifest_.present || !manifest_.parsed) {
    if (require_manifest_) return Reason::kSourceUnverified;
    notes_.push_back("continuing without a manifest: completeness is unknown");
    return Reason::kNone;
  }

  bool verified = true;
  if (manifest_.status != "COMPLETED") {
    notes_.push_back("manifest status is '" + manifest_.status +
                     "', not COMPLETED");
    verified = false;
  }
  if (manifest_.bytes >= 0 && manifest_.bytes != file_bytes_) {
    notes_.push_back("manifest byte count " + std::to_string(manifest_.bytes) +
                     " != file size " + std::to_string(file_bytes_));
    verified = false;
  }
  if (!manifest_.sha256.empty()) {
    file_sha256_ = sha256_file(path_);
    if (file_sha256_ != manifest_.sha256) {
      notes_.push_back("manifest sha256 does not match the file contents");
      verified = false;
    }
  } else {
    notes_.push_back("manifest carries no sha256");
    verified = false;
  }

  if (verified) {
    provenance_ = Provenance::kVerifiedCompleteFile;
    return Reason::kNone;
  }
  return require_manifest_ ? Reason::kSourceUnverified : Reason::kNone;
}

ReadResult Mdp1FileSource::read(std::span<std::byte> out) {
  ReadResult r;
  if (fd_ < 0) {
    r.error = Reason::kIoError;
    return r;
  }
  while (true) {
    const ssize_t n = ::read(fd_, out.data(), out.size());
    if (n > 0) {
      r.bytes = static_cast<std::size_t>(n);
      total_bytes_ += r.bytes;
      return r;
    }
    if (n == 0) {
      r.eof = true;
      return r;
    }
    if (errno == EINTR) continue;  // a signal is not an I/O failure
    r.error = Reason::kIoError;
    return r;
  }
}

}  // namespace md
