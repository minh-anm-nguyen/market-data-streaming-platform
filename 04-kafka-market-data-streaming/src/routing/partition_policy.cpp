#include "routing/partition_policy.h"

#include <cctype>
#include <cstdlib>
#include <cstdio>
#include <fstream>
#include <sstream>

namespace p04 {
namespace {

// Kafka murmur2 constants, from org.apache.kafka.common.utils.Utils.
constexpr std::uint32_t kSeed = 0x9747b28cu;
constexpr std::uint32_t kM = 0x5bd1e995u;
constexpr int kR = 24;

}  // namespace

std::uint32_t Murmur2(std::string_view key) {
  const auto length = static_cast<std::uint32_t>(key.size());
  const auto* data = reinterpret_cast<const unsigned char*>(key.data());

  std::uint32_t h = kSeed ^ length;
  const std::uint32_t blocks = length / 4u;

  for (std::uint32_t i = 0; i < blocks; ++i) {
    const std::uint32_t i4 = i * 4u;
    std::uint32_t k = static_cast<std::uint32_t>(data[i4 + 0]) |
                      (static_cast<std::uint32_t>(data[i4 + 1]) << 8) |
                      (static_cast<std::uint32_t>(data[i4 + 2]) << 16) |
                      (static_cast<std::uint32_t>(data[i4 + 3]) << 24);
    k *= kM;
    k ^= k >> kR;
    k *= kM;
    h *= kM;
    h ^= k;
  }

  // The Java switch falls through deliberately; reproduced here with the same
  // fall-through so a 3-byte or 2-byte tail mixes the same way. Note that only
  // the case-1 arm multiplies by m, which is a quirk of the reference
  // implementation rather than of murmur2 in general.
  const std::uint32_t tail = length & ~3u;
  switch (length % 4u) {
    case 3:
      h ^= static_cast<std::uint32_t>(data[tail + 2]) << 16;
      [[fallthrough]];
    case 2:
      h ^= static_cast<std::uint32_t>(data[tail + 1]) << 8;
      [[fallthrough]];
    case 1:
      h ^= static_cast<std::uint32_t>(data[tail]);
      h *= kM;
      break;
    default:
      break;
  }

  h ^= h >> 13;
  h *= kM;
  h ^= h >> 15;
  return h;
}

Partitioner ParsePartitioner(std::string_view name) {
  if (name == "murmur2_random") return Partitioner::kMurmur2Random;
  if (name == "consistent") return Partitioner::kConsistent;
  if (name == "consistent_random") return Partitioner::kConsistentRandom;
  if (name == "random") return Partitioner::kRandom;
  return Partitioner::kUnknown;
}

const char* ToString(Partitioner partitioner) {
  switch (partitioner) {
    case Partitioner::kMurmur2Random:    return "murmur2_random";
    case Partitioner::kConsistent:       return "consistent";
    case Partitioner::kConsistentRandom: return "consistent_random";
    case Partitioner::kRandom:           return "random";
    case Partitioner::kUnknown:          return "unknown";
  }
  return "unknown";
}

std::int32_t SelectPartition(std::string_view key, std::int32_t partition_count) {
  if (key.empty() || partition_count <= 0) return -1;
  const std::uint32_t h = Murmur2(key);
  // toPositive in the Java client is a mask, not an abs(): abs(INT_MIN) would
  // overflow. The mask is what makes the mapping reproducible.
  const std::int32_t positive = static_cast<std::int32_t>(h & 0x7fffffffu);
  return positive % partition_count;
}

namespace {

// Minimal scanner for the fixed golden-file shape. It walks the text looking
// for the three keys it needs and refuses anything it does not recognise, which
// is the point: a tolerant parser would let a malformed audit file through and
// the audit would then prove nothing.
bool FindStringValue(const std::string& text, const std::string& key,
                     std::string* out) {
  const std::string needle = "\"" + key + "\"";
  const auto key_pos = text.find(needle);
  if (key_pos == std::string::npos) return false;
  auto colon = text.find(':', key_pos + needle.size());
  if (colon == std::string::npos) return false;
  auto open = text.find('"', colon + 1);
  if (open == std::string::npos) return false;
  auto close = text.find('"', open + 1);
  if (close == std::string::npos) return false;
  *out = text.substr(open + 1, close - open - 1);
  return true;
}

bool FindIntValue(const std::string& text, const std::string& key, long* out) {
  const std::string needle = "\"" + key + "\"";
  const auto key_pos = text.find(needle);
  if (key_pos == std::string::npos) return false;
  auto colon = text.find(':', key_pos + needle.size());
  if (colon == std::string::npos) return false;
  std::size_t i = colon + 1;
  while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) ++i;
  std::size_t start = i;
  while (i < text.size() && (std::isdigit(static_cast<unsigned char>(text[i])) || text[i] == '-')) ++i;
  if (start == i) return false;
  *out = std::strtol(text.substr(start, i - start).c_str(), nullptr, 10);
  return true;
}

}  // namespace

bool PartitionGolden::LoadFromFile(const std::string& path, PartitionGolden* out,
                                   std::string* error) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    *error = "cannot open golden mapping file: " + path;
    return false;
  }
  std::ostringstream buffer;
  buffer << in.rdbuf();
  const std::string text = buffer.str();

  long count = 0;
  if (!FindIntValue(text, "partition_count", &count) || count <= 0) {
    *error = "golden mapping missing or invalid partition_count";
    return false;
  }
  std::string partitioner_name;
  if (!FindStringValue(text, "partitioner", &partitioner_name)) {
    *error = "golden mapping missing partitioner";
    return false;
  }
  const Partitioner partitioner = ParsePartitioner(partitioner_name);
  if (partitioner == Partitioner::kUnknown) {
    *error = "golden mapping has unknown partitioner: " + partitioner_name;
    return false;
  }

  const auto mapping_pos = text.find("\"mapping\"");
  if (mapping_pos == std::string::npos) {
    *error = "golden mapping missing mapping object";
    return false;
  }
  const auto brace = text.find('{', mapping_pos);
  const auto end_brace = text.find('}', brace);
  if (brace == std::string::npos || end_brace == std::string::npos) {
    *error = "golden mapping object is not well formed";
    return false;
  }

  std::map<std::string, std::int32_t> mapping;
  std::size_t i = brace + 1;
  while (i < end_brace) {
    const auto open = text.find('"', i);
    if (open == std::string::npos || open > end_brace) break;
    const auto close = text.find('"', open + 1);
    if (close == std::string::npos || close > end_brace) {
      *error = "unterminated key in mapping object";
      return false;
    }
    const std::string entry_key = text.substr(open + 1, close - open - 1);
    const auto colon = text.find(':', close + 1);
    if (colon == std::string::npos || colon > end_brace) {
      *error = "mapping entry without a value: " + entry_key;
      return false;
    }
    std::size_t j = colon + 1;
    while (j < end_brace && std::isspace(static_cast<unsigned char>(text[j]))) ++j;
    const std::size_t num_start = j;
    while (j < end_brace && (std::isdigit(static_cast<unsigned char>(text[j])) || text[j] == '-')) ++j;
    if (num_start == j) {
      *error = "mapping entry value is not an integer: " + entry_key;
      return false;
    }
    mapping[entry_key] = static_cast<std::int32_t>(
        std::strtol(text.substr(num_start, j - num_start).c_str(), nullptr, 10));
    i = j;
  }
  if (mapping.empty()) {
    *error = "golden mapping is empty, so it would prove nothing";
    return false;
  }

  out->partition_count_ = static_cast<std::int32_t>(count);
  out->partitioner_ = partitioner;
  out->mapping_ = std::move(mapping);
  return true;
}

bool PartitionGolden::Audit(std::int32_t runtime_partition_count,
                            std::string* error) const {
  if (partitioner_ != Partitioner::kMurmur2Random) {
    *error = std::string("golden mapping pins partitioner ") +
             ToString(partitioner_) + " but only murmur2_random is accepted";
    return false;
  }
  // A golden file computed for a different partition count cannot audit this
  // run: the modulo changes every mapping. Case B-05 depends on this being a
  // hard stop rather than a warning.
  if (runtime_partition_count != partition_count_) {
    *error = "golden mapping is for partition_count=" +
             std::to_string(partition_count_) + " but the topic reports " +
             std::to_string(runtime_partition_count);
    return false;
  }
  for (const auto& [key, expected] : mapping_) {
    const std::int32_t actual = SelectPartition(key, partition_count_);
    if (actual != expected) {
      *error = "partition mapping drift for key " + key + ": golden=" +
               std::to_string(expected) + " computed=" + std::to_string(actual);
      return false;
    }
  }
  return true;
}

}  // namespace p04
