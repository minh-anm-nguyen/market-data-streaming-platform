// Self-contained SHA-256.
//
// The adapter hashes config bytes, input streams and output artifacts so that
// "COMPLETE" can be backed by a digest rather than by a filename. Vendoring
// ~100 lines avoids making OpenSSL a build dependency of a component whose
// whole point is a small, pinned toolchain.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace md {

class Sha256 {
 public:
  Sha256() { reset(); }
  void reset();
  void update(const void* data, std::size_t len);
  void update(std::string_view s) { update(s.data(), s.size()); }
  // Finalises and returns the lowercase hex digest. The object must not be
  // updated again without reset().
  std::string final_hex();

 private:
  void compress(const std::uint8_t block[64]);

  std::array<std::uint32_t, 8> h_{};
  std::array<std::uint8_t, 64> buf_{};
  std::size_t buf_len_ = 0;
  std::uint64_t total_bits_ = 0;
};

std::string sha256_hex(std::string_view data);
// Streams a file. Returns an empty string when the file cannot be read.
std::string sha256_file(const std::string& path);

}  // namespace md
