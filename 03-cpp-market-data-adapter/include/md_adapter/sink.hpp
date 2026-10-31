// Output sinks.
//
// What "success" means is different for every sink and is never inflated:
//
//   metrics_only  -- the classifier finished with this event. Nothing was
//                    written anywhere.
//   ndjson_test   -- bytes were appended to a .part file. The artifact only
//                    becomes COMPLETE after fsync + rename + a marker that
//                    records the count and digest.
//   p04_stub      -- the event was handed to the Project 04 interface stub.
//                    That is not a Kafka broker ack and must never be
//                    reported as one.
#pragma once

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>

#include "md_adapter/domain_event.hpp"
#include "md_adapter/status.hpp"

namespace md {

class Sink {
 public:
  virtual ~Sink() = default;

  // Returns kNone on success. Any other value is a hard sink failure: the run
  // must fail rather than continue dropping events.
  virtual Reason write(const ClassifiedEvent& e) = 0;

  // Durably finishes the artifact. Only after this returns kNone may a run be
  // described as COMPLETE.
  virtual Reason commit() = 0;

  // Leaves the artifact deliberately incomplete (e.g. the .part file stays).
  virtual void abort() = 0;

  virtual const char* name() const = 0;
  virtual bool needs_raw_wire() const { return false; }

  std::uint64_t events_written() const noexcept { return events_written_; }
  std::uint64_t bytes_written() const noexcept { return bytes_written_; }
  // Set by commit() for the report; empty when the sink produces no file.
  const std::string& artifact_path() const noexcept { return artifact_path_; }
  const std::string& artifact_sha256() const noexcept {
    return artifact_sha256_;
  }

 protected:
  std::uint64_t events_written_ = 0;
  std::uint64_t bytes_written_ = 0;
  std::string artifact_path_;
  std::string artifact_sha256_;
};

class MetricsOnlySink final : public Sink {
 public:
  Reason write(const ClassifiedEvent& e) override;
  Reason commit() override { return Reason::kNone; }
  void abort() override {}
  const char* name() const override { return "metrics_only"; }
};

// Writes one JSON object per line with a fixed key order, so two runs of the
// same input produce byte-identical artifacts.
class NdjsonTestSink final : public Sink {
 public:
  // `path` is the final artifact. Writing happens to `path + ".part"`.
  explicit NdjsonTestSink(std::string path);
  ~NdjsonTestSink() override;

  Reason open();
  Reason write(const ClassifiedEvent& e) override;
  Reason commit() override;
  void abort() override;
  const char* name() const override { return "ndjson_test"; }

 private:
  std::string final_path_;
  std::string part_path_;
  std::FILE* f_ = nullptr;
  bool committed_ = false;
};

// Records exactly what would be handed to Project 04: a Kafka-shaped
// (key, value) pair where the value is the untouched MarketDataEvent wire
// bytes -- no MDP1 magic, no length prefix -- plus the data-quality metadata
// on a separate side-channel field.
class P04StubSink final : public Sink {
 public:
  explicit P04StubSink(std::string path, std::int64_t artificial_delay_us = 0);
  ~P04StubSink() override;

  Reason open();
  Reason write(const ClassifiedEvent& e) override;
  Reason commit() override;
  void abort() override;
  const char* name() const override { return "p04_stub"; }
  bool needs_raw_wire() const override { return true; }

 private:
  std::string final_path_;
  std::string part_path_;
  std::FILE* f_ = nullptr;
  std::int64_t delay_us_ = 0;
  bool committed_ = false;
};

// Appends `s` to `out` as a JSON string literal, escaping control characters
// and quoting bytes that are not printable ASCII.
void json_escape(std::string_view s, std::string& out);

}  // namespace md
