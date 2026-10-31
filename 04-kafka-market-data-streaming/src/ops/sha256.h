// SHA-256, used for the digests that NFR-14 and section 14 require: the pinned
// .proto text, the generated descriptor, the resolved config and the source
// dataset.
//
// Implemented here rather than taken from OpenSSL because the digest of a
// pinned contract should not change meaning with the system crypto library, and
// because the EVP API has shifted enough between OpenSSL 1.1 and 3.x that a
// build on a different host could silently take a different code path. This is
// a hash for provenance and reproducibility, never for authentication, so a
// compact self-contained implementation is the right trade. The unit tests
// check it against the published FIPS 180-4 vectors plus the empty string.
#ifndef P04_OPS_SHA256_H_
#define P04_OPS_SHA256_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace p04 {

class Sha256 {
 public:
  Sha256();

  void Update(const void* data, std::size_t length);
  void Update(std::string_view data) { Update(data.data(), data.size()); }

  // Finalises and returns the lowercase hex digest. The object must not be
  // reused afterwards; construct a new one instead.
  std::string HexDigest();

  static std::string OfString(std::string_view data);

  // Returns an empty string if the file cannot be read, so a caller that
  // forgets to check records an empty digest rather than a plausible wrong one.
  static std::string OfFile(const std::string& path);

 private:
  void Transform(const unsigned char block[64]);

  std::array<std::uint32_t, 8> state_;
  std::uint64_t bit_length_ = 0;
  unsigned char buffer_[64] = {};
  std::size_t buffered_ = 0;
};

}  // namespace p04

#endif  // P04_OPS_SHA256_H_
