// The pinned key-to-partition mapping (cases B-02, B-05, B-06, B-07, E2E-02).
//
// These tests are the C++ half of a two-implementation agreement. The other
// half is tools/gen_partition_golden.py, written independently in Python, whose
// output is committed as config/topic-partition-golden.json and loaded by the
// audit test below. Agreement between two independent implementations is the
// evidence case B-07 asks for; agreement of one implementation with itself is
// not.
#include "routing/partition_policy.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <unistd.h>
#include <map>
#include <set>
#include <string>

namespace p04 {
namespace {

// mkstemp rather than tmpnam: tmpnam races between the name it returns and the
// open, and the linker warns about it. These tests write real files, so the
// unique-and-already-created guarantee matters.
std::string MakeTempPath(const std::string& suffix) {
  char pattern[] = "/tmp/p04_test_XXXXXX";
  const int fd = mkstemp(pattern);
  if (fd >= 0) close(fd);
  return std::string(pattern) + suffix;
}

// Murmur2 values for the keys E2E-02 names. These are not copied from this
// implementation: they are the values the Kafka murmur2 definition produces,
// cross-checked against the Python generator and against kafka-python's own
// partitioner in tests/integration/test_partitioner_agreement.py.
TEST(Murmur2, KnownPartitionsAtEightPartitions) {
  EXPECT_EQ(SelectPartition("EURUSD", 8), 5);
  EXPECT_EQ(SelectPartition("GBPUSD", 8), 2);
  EXPECT_EQ(SelectPartition("USDJPY", 8), 1);
}

// Every tail length of the murmur2 switch: 4-byte aligned, plus 1, plus 2 and
// plus 3. The reference implementation only multiplies by m in the length-1
// arm, which is easy to get wrong, and only these four shapes expose it.
TEST(Murmur2, CoversEveryTailLength) {
  // 4, 5, 6 and 3 bytes: tail lengths 0, 1, 2 and 3 of the murmur2 switch. The
  // exact hash values are pinned by the committed golden file; what is asserted
  // here is that each tail length contributes, which a bug that dropped the
  // tail entirely would break by collapsing these to one value.
  const std::vector<std::string> keys = {"ABCD", "ABCDE", "ABCDEF", "ABC"};
  std::set<std::uint32_t> hashes;
  for (const std::string& key : keys) hashes.insert(Murmur2(key));
  EXPECT_EQ(hashes.size(), keys.size());

  // The same four keys, differing only in their tail, must also spread over
  // more than one partition at N=8.
  std::set<std::int32_t> partitions;
  for (const std::string& key : keys) partitions.insert(SelectPartition(key, 8));
  EXPECT_GE(partitions.size(), 2u);
}

TEST(Murmur2, IsStableAcrossCalls) {
  const std::uint32_t first = Murmur2("EURUSD");
  for (int i = 0; i < 100; ++i) EXPECT_EQ(Murmur2("EURUSD"), first);
}

// Case B-02: an empty key must never be hashed into a partition. librdkafka
// would fall back to random for a null key, and a random partition for a keyed
// stream silently breaks per-key ordering.
TEST(SelectPartition, RefusesEmptyKey) {
  EXPECT_EQ(SelectPartition("", 8), -1);
}

TEST(SelectPartition, RefusesNonPositivePartitionCount) {
  EXPECT_EQ(SelectPartition("EURUSD", 0), -1);
  EXPECT_EQ(SelectPartition("EURUSD", -1), -1);
}

TEST(SelectPartition, AlwaysInRange) {
  for (int count : {1, 2, 3, 8, 12, 31, 64}) {
    for (int i = 0; i < 500; ++i) {
      const std::string key = "SYM" + std::to_string(i);
      const std::int32_t partition = SelectPartition(key, count);
      ASSERT_GE(partition, 0) << key << " at " << count;
      ASSERT_LT(partition, count) << key << " at " << count;
    }
  }
}

// The mapping changes when the partition count changes. This is not a bug, it
// is the reason section 3.3 forbids altering partitions in place and requires a
// new topic generation instead. Asserting it here keeps the reasoning visible.
TEST(SelectPartition, MappingChangesWithPartitionCount) {
  int moved = 0;
  for (int i = 0; i < 100; ++i) {
    const std::string key = "SYM" + std::to_string(i);
    if (SelectPartition(key, 8) != SelectPartition(key, 12)) ++moved;
  }
  EXPECT_GT(moved, 50) << "growing 8 to 12 partitions should move most keys; "
                          "if it did not, the hash is not being used";
}

TEST(Partitioner, ParsesAndRejectsByName) {
  EXPECT_EQ(ParsePartitioner("murmur2_random"), Partitioner::kMurmur2Random);
  EXPECT_EQ(ParsePartitioner("random"), Partitioner::kRandom);
  EXPECT_EQ(ParsePartitioner("consistent"), Partitioner::kConsistent);
  EXPECT_EQ(ParsePartitioner("murmur2"), Partitioner::kUnknown);
  EXPECT_EQ(ParsePartitioner(""), Partitioner::kUnknown);
}

class PartitionGoldenTest : public ::testing::Test {
 protected:
  std::string WriteTempGolden(const std::string& content) {
    const std::string path = MakeTempPath(".json");
    std::ofstream out(path, std::ios::binary);
    out << content;
    out.close();
    temp_files_.push_back(path);
    return path;
  }
  void TearDown() override {
    for (const auto& path : temp_files_) std::remove(path.c_str());
  }
  std::vector<std::string> temp_files_;
};

// The committed golden file, generated by the Python implementation, audited by
// this one. If this test fails, the two implementations disagree and no ordering
// claim in any report is trustworthy until it is resolved.
TEST_F(PartitionGoldenTest, CommittedGoldenFileAgreesWithThisImplementation) {
  PartitionGolden golden;
  std::string error;
  const std::vector<std::string> candidates = {
      "config/topic-partition-golden.json",
      "../config/topic-partition-golden.json",
      "../../config/topic-partition-golden.json",
  };
  bool loaded = false;
  for (const auto& path : candidates) {
    if (PartitionGolden::LoadFromFile(path, &golden, &error)) {
      loaded = true;
      break;
    }
  }
  if (!loaded) {
    GTEST_SKIP() << "golden file not reachable from the test working directory; "
                    "tests/integration runs the same audit with an absolute path";
  }
  EXPECT_EQ(golden.partitioner(), Partitioner::kMurmur2Random);
  EXPECT_GE(golden.mapping().size(), 20u)
      << "a golden file with only a handful of keys would not detect a hash change";
  ASSERT_TRUE(golden.Audit(golden.partition_count(), &error)) << error;

  // The file must exercise more than one partition, or the audit would pass
  // even if every key hashed to the same place.
  std::set<std::int32_t> partitions;
  for (const auto& [key, partition] : golden.mapping()) partitions.insert(partition);
  EXPECT_GE(partitions.size(), 2u);
}

// Case B-05: a golden file for 8 partitions cannot audit a topic that reports
// 12. The modulo changes every mapping, so this has to be a hard stop.
TEST_F(PartitionGoldenTest, RefusesPartitionCountMismatch) {
  const std::string path = WriteTempGolden(
      R"({"partitioner":"murmur2_random","partition_count":8,
          "mapping":{"EURUSD":5,"GBPUSD":2}})");
  PartitionGolden golden;
  std::string error;
  ASSERT_TRUE(PartitionGolden::LoadFromFile(path, &golden, &error)) << error;
  EXPECT_TRUE(golden.Audit(8, &error)) << error;
  EXPECT_FALSE(golden.Audit(12, &error));
  EXPECT_NE(error.find("partition_count=8"), std::string::npos) << error;
}

// Case B-06: a golden file that pins any other partitioner is refused, so a
// config switched to random cannot pass the audit by supplying a matching file.
TEST_F(PartitionGoldenTest, RefusesNonMurmur2Partitioner) {
  const std::string path = WriteTempGolden(
      R"({"partitioner":"random","partition_count":8,"mapping":{"EURUSD":5}})");
  PartitionGolden golden;
  std::string error;
  ASSERT_TRUE(PartitionGolden::LoadFromFile(path, &golden, &error)) << error;
  EXPECT_FALSE(golden.Audit(8, &error));
  EXPECT_NE(error.find("random"), std::string::npos) << error;
}

TEST_F(PartitionGoldenTest, DetectsDriftInAnEntry) {
  const std::string path = WriteTempGolden(
      R"({"partitioner":"murmur2_random","partition_count":8,
          "mapping":{"EURUSD":5,"GBPUSD":7}})");  // GBPUSD is really 2
  PartitionGolden golden;
  std::string error;
  ASSERT_TRUE(PartitionGolden::LoadFromFile(path, &golden, &error)) << error;
  EXPECT_FALSE(golden.Audit(8, &error));
  EXPECT_NE(error.find("GBPUSD"), std::string::npos) << error;
}

TEST_F(PartitionGoldenTest, RefusesEmptyMapping) {
  const std::string path = WriteTempGolden(
      R"({"partitioner":"murmur2_random","partition_count":8,"mapping":{}})");
  PartitionGolden golden;
  std::string error;
  EXPECT_FALSE(PartitionGolden::LoadFromFile(path, &golden, &error));
  EXPECT_NE(error.find("empty"), std::string::npos) << error;
}

TEST_F(PartitionGoldenTest, RefusesUnknownPartitionerName) {
  const std::string path = WriteTempGolden(
      R"({"partitioner":"sticky","partition_count":8,"mapping":{"EURUSD":5}})");
  PartitionGolden golden;
  std::string error;
  EXPECT_FALSE(PartitionGolden::LoadFromFile(path, &golden, &error));
}

TEST_F(PartitionGoldenTest, RefusesMissingFile) {
  PartitionGolden golden;
  std::string error;
  EXPECT_FALSE(PartitionGolden::LoadFromFile("/nonexistent/golden.json", &golden, &error));
  EXPECT_FALSE(error.empty());
}

}  // namespace
}  // namespace p04
