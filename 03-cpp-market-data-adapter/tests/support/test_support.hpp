// Helpers shared by the C++ tests: wire-byte builders, a baseline config and
// a frame collector.
//
// Note what is deliberately absent: there is no helper that produces the
// *expected* classification. Expectations are written out by hand in each
// test, or come from the Python reference model, so that a bug in the
// implementation cannot quietly become the expectation.
#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "market_data.pb.h"
#include "md_adapter/config.hpp"
#include "md_adapter/domain_event.hpp"
#include "md_adapter/framer.hpp"

namespace mdtest {

// --- wire bytes ------------------------------------------------------------

std::string be32(std::uint32_t v);
// One framed payload: [BE32 len][payload].
std::string frame(const std::string& payload);
// A full stream: "MDP1" + framed payloads.
std::string stream(const std::vector<std::string>& payloads);

std::span<const std::byte> as_bytes(const std::string& s);

// --- events ----------------------------------------------------------------

struct EventSpec {
  bool has_dataset_id = true;
  bool has_symbol = true;
  bool has_sequence = true;
  bool has_event_time = true;
  bool has_price_scale = true;
  bool has_version = true;

  std::string dataset_id = "ds-test";
  std::string symbol = "EURUSD";
  std::int64_t sequence = 1;
  std::int64_t event_time_ns = 1'000'000'000;
  std::int64_t price_scale = 100000;
  std::string version = "sim.v1";

  // "quote", "tick" or "" for no oneof at all.
  std::string kind = "quote";
  bool has_bid = true;
  bool has_ask = true;
  bool has_price = true;
  std::int64_t bid = 109000;
  std::int64_t ask = 109010;
  std::int64_t price = 109000;
};

marketdata::v1::MarketDataEvent make_message(const EventSpec& s);
std::string encode(const EventSpec& s);

// A business-valid DomainEvent, for tests that start after decoding.
md::DomainEvent quote_event(std::int64_t seq, std::int64_t event_time_ns,
                            std::int64_t bid = 109000,
                            std::int64_t ask = 109010,
                            const std::string& symbol = "EURUSD",
                            const std::string& dataset = "ds-test");
md::DomainEvent tick_event(std::int64_t seq, std::int64_t event_time_ns,
                           std::int64_t price = 109000,
                           const std::string& symbol = "EURUSD",
                           const std::string& dataset = "ds-test");

// --- config ----------------------------------------------------------------

// A valid baseline config. Tests override single fields rather than
// hand-rolling a whole document, so a change to the contract shows up in one
// place.
md::Config base_config();
std::string base_config_toml();

// --- framing helpers -------------------------------------------------------

// Collects every complete frame the framer emits.
class CollectingHandler final : public md::FrameHandler {
 public:
  md::FrameAction on_frame(std::span<const std::byte> payload,
                           md::Reason& fatal_reason) override;

  std::vector<std::string> frames;
  // When set, on_frame returns this action instead of kContinue.
  md::FrameAction next_action = md::FrameAction::kContinue;
  md::Reason fatal_with = md::Reason::kNone;
};

// Feeds `data` to a framer in chunks of exactly `chunk` bytes.
md::Mdp1Framer::FeedResult feed_chunked(md::Mdp1Framer& framer,
                                        const std::string& data,
                                        std::size_t chunk,
                                        md::FrameHandler& handler,
                                        std::int64_t now_ns = 0);

// --- filesystem ------------------------------------------------------------

// Unique scratch directory under the build tree; removed by the destructor.
class TempDir {
 public:
  TempDir();
  ~TempDir();
  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  const std::string& path() const noexcept { return path_; }
  std::string file(const std::string& name) const;

 private:
  std::string path_;
};

void write_file(const std::string& path, const std::string& data);
std::string read_file(const std::string& path);
// Writes the manifest the adapter requires before it will report COMPLETE.
void write_manifest(const std::string& pbstream_path, const std::string& data,
                    std::int64_t frames, const std::string& status = "COMPLETED");

}  // namespace mdtest
