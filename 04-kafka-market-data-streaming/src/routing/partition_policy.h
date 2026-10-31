// The pinned key-to-partition mapping (ADR-02, FR-05, cases B-05 to B-07).
//
// Why this is reimplemented here instead of trusting the client:
//
// The ordering guarantee this project claims is "per (topic, key, partition,
// producer ownership epoch)". That claim only holds while the same key keeps
// landing on the same partition, which depends on three things agreeing: the
// hash algorithm, the exact key bytes, and the partition count. librdkafka,
// the Java client and any Python tooling each have their own default, and the
// defaults are not all the same. So the expected partition is computed here,
// independently, and compared against what the broker actually reports in the
// delivery report. A mismatch increments p04_partition_mapping_mismatch_total
// and is a P0: it means the ordering claim in the report would be false.
//
// The algorithm is Kafka murmur2 as implemented by
// org.apache.kafka.common.utils.Utils.murmur2, with partition selection
// (hash & 0x7fffffff) % partition_count. librdkafka exposes the same thing as
// the "murmur2_random" partitioner, which differs only for a null key by
// falling back to random. Project 04 never produces a null or empty key
// (case B-02 requires rejection before the producer), so the random arm must
// never be reachable, and SelectPartition refuses an empty key rather than
// quietly returning a random value.
#ifndef P04_ROUTING_PARTITION_POLICY_H_
#define P04_ROUTING_PARTITION_POLICY_H_

#include <cstdint>
#include <map>
#include <string>
#include <string_view>

namespace p04 {

// Kafka-compatible murmur2 over raw key bytes.
//
// Returned as uint32 so the caller cannot accidentally sign-extend it. The Java
// implementation works on a signed int with logical right shifts; doing the
// arithmetic unsigned here and masking at the end reproduces it exactly without
// relying on implementation-defined signed shift behaviour.
std::uint32_t Murmur2(std::string_view key);

// The pinned partitioner identifiers. Only kMurmur2Random is accepted for
// market topics; the others exist so a config that asks for them can be
// rejected by name instead of silently accepted (case B-06).
enum class Partitioner {
  kMurmur2Random,  // pinned choice: Java-compatible hash, random only for null keys
  kConsistent,
  kConsistentRandom,
  kRandom,
  kUnknown,
};

Partitioner ParsePartitioner(std::string_view name);
const char* ToString(Partitioner partitioner);

// Expected partition for a key under the pinned algorithm.
//
// Returns -1 when the key is empty or the partition count is not positive. Both
// are contract violations that must have been caught earlier; returning a
// sentinel rather than picking a partition keeps the failure visible.
std::int32_t SelectPartition(std::string_view key, std::int32_t partition_count);

// A golden mapping, loaded from config/topic-partition-golden.json, that pins
// the expected partition for a set of keys at a fixed partition count.
//
// This is the independent oracle required by case B-07: the file is generated
// by tools/gen_partition_golden.py (a separate implementation, in Python) and
// checked in, so a change to the C++ hash, to the key encoding or to the
// partition count fails the audit at startup instead of producing a silently
// repartitioned topic.
class PartitionGolden {
 public:
  // Parses the minimal JSON shape produced by the generator:
  //   {"partition_count": 8, "partitioner": "murmur2_random",
  //    "mapping": {"EURUSD": 4, ...}}
  // A hand-rolled parser is used rather than pulling in a JSON library for one
  // file; it accepts exactly that shape and rejects anything else, because a
  // lenient parser here would weaken the audit it exists to perform.
  static bool LoadFromFile(const std::string& path, PartitionGolden* out,
                           std::string* error);

  std::int32_t partition_count() const { return partition_count_; }
  Partitioner partitioner() const { return partitioner_; }
  const std::map<std::string, std::int32_t>& mapping() const { return mapping_; }

  // Recomputes every entry with SelectPartition and reports the first
  // disagreement. Called at startup before any produce call.
  bool Audit(std::int32_t runtime_partition_count, std::string* error) const;

 private:
  std::int32_t partition_count_ = 0;
  Partitioner partitioner_ = Partitioner::kUnknown;
  std::map<std::string, std::int32_t> mapping_;
};

}  // namespace p04

#endif  // P04_ROUTING_PARTITION_POLICY_H_
