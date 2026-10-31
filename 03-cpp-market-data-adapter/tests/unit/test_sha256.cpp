// SHA-256 against published vectors. The adapter's completeness claims rest
// on these digests, so the implementation is checked against values from the
// NIST/FIPS 180-4 examples rather than against itself.
#include <gtest/gtest.h>

#include <string>

#include "md_adapter/sha256.hpp"
#include "test_support.hpp"

TEST(Sha256, KnownVectors) {
  EXPECT_EQ(md::sha256_hex(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  EXPECT_EQ(md::sha256_hex("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  EXPECT_EQ(md::sha256_hex(
                "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
  EXPECT_EQ(md::sha256_hex(std::string(1000000, 'a')),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

// Padding has three distinct paths around the 56/64-byte boundary; each must
// produce the same digest as a single-shot hash.
TEST(Sha256, LengthsAroundTheBlockBoundary) {
  for (std::size_t n = 0; n < 200; ++n) {
    const std::string data(n, 'x');
    md::Sha256 s;
    s.update(data);
    EXPECT_EQ(s.final_hex(), md::sha256_hex(data)) << "n=" << n;
  }
}

TEST(Sha256, IncrementalMatchesOneShot) {
  const std::string data(5000, 'q');
  for (std::size_t chunk : {1u, 3u, 63u, 64u, 65u, 1000u}) {
    md::Sha256 s;
    for (std::size_t off = 0; off < data.size(); off += chunk)
      s.update(data.data() + off, std::min(chunk, data.size() - off));
    EXPECT_EQ(s.final_hex(), md::sha256_hex(data)) << "chunk=" << chunk;
  }
}

TEST(Sha256, ResetAllowsReuse) {
  md::Sha256 s;
  s.update("abc");
  EXPECT_EQ(s.final_hex(), md::sha256_hex("abc"));
  s.reset();
  s.update("");
  EXPECT_EQ(s.final_hex(), md::sha256_hex(""));
}

TEST(Sha256, FileHashing) {
  mdtest::TempDir dir;
  const std::string path = dir.file("data.bin");
  const std::string content(100000, 'z');
  mdtest::write_file(path, content);

  EXPECT_EQ(md::sha256_file(path), md::sha256_hex(content));
  EXPECT_TRUE(md::sha256_file(dir.file("missing.bin")).empty());
}
