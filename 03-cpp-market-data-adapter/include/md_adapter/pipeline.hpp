// The single-owner processing pipeline and the run drivers around it.
//
// Stage order is fixed and must not be reordered:
//   framer -> decode -> structural -> business -> sequence -> freshness
//          -> output policy -> bounded queue -> sink
//
// Everything up to the queue runs on one thread, so per-key state has exactly
// one owner and no lock is needed on the hot path (ADR-03).
#pragma once

#include <atomic>
#include <deque>
#include <mutex>
#include <optional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "md_adapter/bounded_queue.hpp"
#include "md_adapter/classifier.hpp"
#include "md_adapter/clock.hpp"
#include "md_adapter/config.hpp"
#include "md_adapter/decoder.hpp"
#include "md_adapter/framer.hpp"
#include "md_adapter/input_source.hpp"
#include "md_adapter/metrics.hpp"
#include "md_adapter/quote_freshness.hpp"
#include "md_adapter/sequence_tracker.hpp"
#include "md_adapter/sink.hpp"
#include "md_adapter/validators.hpp"

namespace md {

// Processes complete frames. Owns all per-key state.
class Pipeline final : public FrameHandler {
 public:
  Pipeline(const Config& cfg, Clock& clock, BoundedQueue& queue,
           bool needs_raw_wire, bool live);

  FrameAction on_frame(std::span<const std::byte> payload,
                       Reason& fatal_reason) override;

  // Retries an event that could not be enqueued. Returns true once nothing is
  // pending.
  bool flush_pending();
  bool has_pending() const noexcept { return pending_.has_value(); }

  // Drives the freshness timers; must be called even while the transport is
  // silent.
  void tick(std::int64_t now_mono_ns);

  Metrics& metrics() noexcept { return metrics_; }
  const Metrics& metrics() const noexcept { return metrics_; }
  SequenceTracker& tracker() noexcept { return tracker_; }
  const SequenceTracker& tracker() const noexcept { return tracker_; }
  FreshnessEngine& freshness() noexcept { return freshness_; }
  const FreshnessEngine& freshness() const noexcept { return freshness_; }
  const std::vector<FreshnessEngine::Transition>& transitions() const noexcept {
    return transitions_;
  }
  std::uint64_t arrival_index() const noexcept { return arrival_index_; }

  // Advances published continuity from *confirmed* deliveries. The writer
  // thread only publishes a monotonic count; this runs on the owner thread,
  // so no tracker state is ever touched from two threads.
  void confirm_delivered(std::uint64_t total_confirmed);
  std::uint64_t inflight_count() const noexcept { return inflight_.size(); }

 private:
  const Config& cfg_;
  // Freshness and classification run on the injected clock, so an offline
  // replay is deterministic.
  Clock& clock_;
  // Latency is a performance measurement and is always wall-clock: timing a
  // replay with a clock that does not tick would report zero.
  SteadyClock perf_clock_;
  BoundedQueue& queue_;
  bool needs_raw_wire_;

  ProtobufDecoder decoder_;
  StructuralLimits structural_limits_;
  BusinessValidator business_;
  SequenceTracker tracker_;
  FreshnessEngine freshness_;
  Classifier classifier_;
  Metrics metrics_;

  // Enqueued-but-unconfirmed events, in order, so a delivery count can be
  // mapped back to the keys it advanced. Bounded by the queue depth.
  struct InflightRecord {
    KeyHandle handle;
    Sequence sequence = 0;
  };
  std::deque<InflightRecord> inflight_;
  std::uint64_t confirmed_ = 0;

  DomainEvent scratch_;
  std::optional<ClassifiedEvent> pending_;
  InflightRecord pending_record_;
  std::uint64_t arrival_index_ = 0;
  std::vector<FreshnessEngine::Transition> transitions_;
};

// Drains the queue into the sink on its own thread.
// Note it holds no reference to the Pipeline: the only thing that crosses
// the thread boundary is a monotonic count of confirmed writes, which the
// owner thread maps back to keys in Pipeline::confirm_delivered().
class SinkWriter {
 public:
  SinkWriter(BoundedQueue& q, Sink& sink);
  ~SinkWriter();

  void start();
  // Signals the writer to finish the queue and stop. Returns false if the
  // drain did not finish inside the deadline.
  bool stop_and_drain(std::int64_t deadline_ns);

  Reason error() const noexcept { return error_.load(); }
  std::uint64_t written() const noexcept { return written_.load(); }

 private:
  void run();

  BoundedQueue& q_;
  Sink& sink_;
  std::thread th_;
  std::atomic<Reason> error_{Reason::kNone};
  std::atomic<std::uint64_t> written_{0};
  std::atomic<bool> stop_{false};
};

// ---------------------------------------------------------------------------

struct RunOutcome {
  // Defaults are the conservative answer: if anything escapes without an
  // explicit verdict, the run is INCOMPLETE, never successful.
  RunStatus status = RunStatus::kIncomplete;
  ExitCode exit_code = ExitCode::kIncomplete;
  Reason terminal_reason = Reason::kNone;
  Provenance provenance = Provenance::kLiveUnverified;
  std::string summary;
  std::vector<std::string> notes;
  std::uint64_t frames_from_source = 0;
  std::int64_t wall_ns = 0;

  // True once a loop has deliberately chosen `status`. Without this the
  // default kIncomplete is indistinguishable from a chosen kIncomplete.
  bool status_chosen = false;

  void choose(RunStatus s, ExitCode code, Reason reason = Reason::kNone) {
    status = s;
    exit_code = code;
    if (reason != Reason::kNone) terminal_reason = reason;
    status_chosen = true;
  }
};

// Shared loop body for both transports.
class AdapterRunner {
 public:
  AdapterRunner(const Config& cfg, Clock& clock, Sink& sink);
  ~AdapterRunner();

  RunOutcome run_offline();
  RunOutcome run_live();

  // Set from a signal handler; checked on every loop iteration.
  static std::atomic<bool>& shutdown_flag();

  Pipeline& pipeline() noexcept { return *pipeline_; }
  const Metrics& metrics() const noexcept { return pipeline_->metrics(); }
  BoundedQueue& queue() noexcept { return queue_; }
  const std::vector<std::string>& source_notes() const noexcept {
    return source_notes_;
  }

 private:
  // Returns false when the loop must stop; `out` carries the reason.
  bool handle_feed_result(const Mdp1Framer::FeedResult& fr, RunOutcome& out);
  bool pump_backpressure(std::int64_t now_ns, RunOutcome& out);
  // Shared teardown: reconcile counters, then commit or abort the sink.
  // Returns true only when the run reached the end cleanly AND the sink
  // committed, i.e. when the caller is entitled to declare success.
  // `out.status` must not be used as the sentinel for this: its default is
  // the conservative kIncomplete, so testing it would make success
  // unreachable.
  bool finalise(RunOutcome& out, SinkWriter& writer, bool drained,
                bool aborted);

  const Config& cfg_;
  // The pipeline's clock: injected, and virtual during offline replay.
  Clock& clock_;
  // Transport and backpressure budgets are real-time budgets. They must not
  // be measured on a replay clock -- a deadline on a clock that never
  // advances can never expire (design 4.1 vs design 6).
  SteadyClock io_clock_;
  Sink& sink_;
  BoundedQueue queue_;
  std::unique_ptr<Pipeline> pipeline_;
  Mdp1Framer framer_;
  std::vector<std::byte> read_buf_;
  std::size_t read_buf_fill_ = 0;
  std::vector<std::string> source_notes_;
  std::int64_t last_tick_ns_ = 0;
  std::int64_t pause_started_ns_ = 0;
};

}  // namespace md
