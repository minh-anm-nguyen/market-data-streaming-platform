// SHA-256 against published vectors.
//
// The digests in the run report are the provenance chain for the whole project:
// the pinned descriptor, the config and the source dataset are all identified by
// one of these. A hash that is subtly wrong would produce reports that look
// rigorous and compare nothing, so it is checked against FIPS 180-4 and RFC
// 6234 vectors rather than against itself.
#include "ops/sha256.h"

#include <gtest/gtest.h>

#include <string>

namespace p04 {
namespace {

TEST(Sha256, EmptyString) {
  EXPECT_EQ(Sha256::OfString(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

TEST(Sha256, Abc) {
  EXPECT_EQ(Sha256::OfString("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

TEST(Sha256, TwoBlockMessage) {
  EXPECT_EQ(Sha256::OfString("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
}

// 55, 56 and 64 bytes straddle the padding boundary: at 56 the length does not
// fit in the final block and a second block is appended. An implementation that
// gets the padding wrong usually passes "abc" and fails exactly here.
TEST(Sha256, PaddingBoundaries) {
  EXPECT_EQ(Sha256::OfString(std::string(55, 'a')),
            "9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318");
  EXPECT_EQ(Sha256::OfString(std::string(56, 'a')),
            "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a");
  EXPECT_EQ(Sha256::OfString(std::string(64, 'a')),
            "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb");
}

TEST(Sha256, MillionA) {
  Sha256 hasher;
  const std::string chunk(1000, 'a');
  for (int i = 0; i < 1000; ++i) hasher.Update(chunk);
  EXPECT_EQ(hasher.HexDigest(),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

// Chunking must not change the digest. The file reader feeds 64 KiB blocks and
// the descriptor hasher feeds one buffer; both have to agree.
TEST(Sha256, ChunkingIsIrrelevant) {
  const std::string data = "the quick brown fox jumps over the lazy dog, twice over";
  Sha256 single;
  single.Update(data);
  Sha256 chunked;
  for (std::size_t i = 0; i < data.size(); ++i) chunked.Update(data.data() + i, 1);
  EXPECT_EQ(single.HexDigest(), chunked.HexDigest());
}

TEST(Sha256, MissingFileReturnsEmptyNotAPlausibleDigest) {
  // A caller that forgets to check gets an empty string, which is visibly
  // wrong in a report, rather than the digest of nothing, which is not.
  EXPECT_EQ(Sha256::OfFile("/nonexistent/path/that/should/not/exist"), "");
}

}  // namespace
}  // namespace p04
