// Transport drivers: the offline file loop and the live TCP loop.
//
// Both share one framer, one pipeline and one bounded queue, so a classified
// result cannot depend on which transport delivered the bytes.
#include <algorithm>
#include <chrono>
#include <cstring>
#include <thread>

#include "md_adapter/pipeline.hpp"

namespace md {
namespace {

QueueLimits queue_limits_from(const Config& c) {
  QueueLimits l;
  l.max_events = static_cast<std::size_t>(c.output.max_queue_events);
  l.max_bytes = static_cast<std::size_t>(c.output.max_queue_bytes);
  l.pause_high_watermark_pct =
      static_cast<int>(c.output.pause_read_high_watermark_pct);
  l.resume_low_watermark_pct =
      static_cast<int>(c.output.resume_read_low_watermark_pct);
  return l;
}

FramerLimits framer_limits_from(const Config& c) {
  FramerLimits l;
  l.max_payload_bytes = static_cast<std::uint32_t>(c.input.max_payload_bytes);
  l.prefix_deadline_ns = c.input.prefix_deadline_ms * 1'000'000;
  l.header_partial_deadline_ns = c.input.header_partial_deadline_ms * 1'000'000;
  l.body_deadline_ns = c.input.body_deadline_ms * 1'000'000;
  l.max_frames_per_iteration =
      static_cast<std::size_t>(c.input.max_frames_per_iteration);
  return l;
}

}  // namespace

std::atomic<bool>& AdapterRunner::shutdown_flag() {
  static std::atomic<bool> flag{false};
  return flag;
}

AdapterRunner::AdapterRunner(const Config& cfg, Clock& clock, Sink& sink)
    : cfg_(cfg),
      clock_(clock),
      sink_(sink),
      queue_(queue_limits_from(cfg)),
      framer_(framer_limits_from(cfg)) {
  pipeline_ = std::make_unique<Pipeline>(cfg, clock, queue_,
                                         sink.needs_raw_wire(),
                                         cfg.input.mode == InputMode::kTcp);
  read_buf_.resize(static_cast<std::size_t>(cfg.input.read_chunk_bytes));
}

AdapterRunner::~AdapterRunner() = default;

bool AdapterRunner::handle_feed_result(const Mdp1Framer::FeedResult& fr,
                                       RunOutcome& out) {
  if (!fr.fatal) return true;
  out.choose(RunStatus::kFailed, ExitCode::kStreamFailed, fr.reason);
  out.summary = "stream closed: " + std::string(to_string(fr.reason));
  return false;
}

bool AdapterRunner::pump_backpressure(std::int64_t now_ns, RunOutcome& out) {
  if (!pipeline_->has_pending()) return true;
  if (pause_started_ns_ == 0) {
    pause_started_ns_ = now_ns;
    ++pipeline_->metrics().read_pause_events;
  }
  if (pipeline_->flush_pending()) {
    pipeline_->metrics().read_paused_ns +=
        static_cast<std::uint64_t>(now_ns - pause_started_ns_);
    pause_started_ns_ = 0;
    return true;
  }
  const std::int64_t stalled = now_ns - pause_started_ns_;
  if (stalled > cfg_.output.output_stall_deadline_ms * 1'000'000) {
    // The sink has not drained inside its budget. Failing the run is the only
    // honest outcome: nothing may be discarded to make room.
    out.choose(RunStatus::kIncomplete, ExitCode::kBackpressureTimeout,
               Reason::kBackpressureTimeout);
    out.summary = "sink stalled for " + std::to_string(stalled / 1'000'000) +
                  " ms with " + std::to_string(queue_.outstanding()) +
                  " events outstanding";
    return false;
  }
  std::this_thread::sleep_for(std::chrono::microseconds(200));
  return true;
}

bool AdapterRunner::finalise(RunOutcome& out, SinkWriter& writer, bool drained,
                             bool aborted) {
  pipeline_->confirm_delivered(writer.written());

  auto& m = pipeline_->metrics();
  m.output_success = writer.written();
  m.output_failed_or_pending =
      m.classified_output_attempted >= m.output_success
          ? m.classified_output_attempted - m.output_success
          : 0;
  m.output_pending =
      queue_.outstanding() + (pipeline_->has_pending() ? 1 : 0);
  m.peak_queue_events =
      std::max<std::uint64_t>(m.peak_queue_events, queue_.peak_events());
  m.peak_queue_bytes =
      std::max<std::uint64_t>(m.peak_queue_bytes, queue_.peak_bytes());

  const Reason sink_err = writer.error();
  if (sink_err != Reason::kNone) {
    sink_.abort();
    out.choose(RunStatus::kFailed, ExitCode::kSinkIoError, sink_err);
    out.summary = "sink write failed";
    return false;
  }
  if (aborted) {
    // The run did not reach the end of its input. The artifact stays
    // deliberately unfinished. A loop that already chose a more specific
    // verdict (a backpressure timeout, a requested shutdown) keeps it.
    sink_.abort();
    if (!out.status_chosen)
      out.choose(RunStatus::kFailed, ExitCode::kStreamFailed);
    return false;
  }
  if (!drained) {
    sink_.abort();
    out.choose(RunStatus::kIncomplete, ExitCode::kIncomplete,
               Reason::kBackpressureTimeout);
    out.summary = "writer did not drain inside the shutdown deadline";
    return false;
  }
  const Reason commit = sink_.commit();
  if (commit != Reason::kNone) {
    out.choose(RunStatus::kFailed, ExitCode::kSinkIoError, commit);
    out.summary = "sink commit failed";
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Offline
// ---------------------------------------------------------------------------

RunOutcome AdapterRunner::run_offline() {
  RunOutcome out;
  const std::int64_t t_start = io_clock_.now_mono_ns();

  Mdp1FileSource source(cfg_.input.file, cfg_.input.require_completed_manifest);
  const Reason open_r = source.open();
  source_notes_ = source.provenance_notes();
  out.provenance = source.provenance();
  if (open_r != Reason::kNone) {
    out.choose(RunStatus::kFailed,
               (open_r == Reason::kSourceUnverified)
                   ? ExitCode::kSourceUnverified
                   : ExitCode::kStreamFailed,
               open_r);
    out.summary = "input could not be established as a verified source";
    out.notes = source_notes_;
    return out;
  }

  SinkWriter writer(queue_, sink_);
  writer.start();
  framer_.reset(io_clock_.now_mono_ns());

  bool aborted = false;
  bool saw_eof = false;
  while (true) {
    if (shutdown_flag().load()) {
      out.choose(RunStatus::kIncomplete, ExitCode::kIncomplete);
      out.summary = "shutdown requested before the input was consumed";
      aborted = true;
      break;
    }
    const std::int64_t now = io_clock_.now_mono_ns();
    if (!pump_backpressure(now, out)) {
      aborted = true;
      break;
    }
    if (pipeline_->has_pending()) continue;

    if (read_buf_fill_ == 0) {
      const ReadResult rr = source.read(std::span<std::byte>(read_buf_));
      if (rr.error != Reason::kNone) {
        out.choose(RunStatus::kFailed, ExitCode::kStreamFailed, rr.error);
        out.summary = "input read failed";
        aborted = true;
        break;
      }
      if (rr.bytes == 0 && rr.eof) {
        saw_eof = true;
        break;
      }
      read_buf_fill_ = rr.bytes;
      pipeline_->metrics().transport_bytes_read += rr.bytes;
    }

    const auto fr =
        framer_.feed(std::span<const std::byte>(read_buf_.data(), read_buf_fill_),
                     io_clock_.now_mono_ns(), *pipeline_);
    // Keep whatever the framer did not consume. The buffer is fixed size, so
    // this compaction is bounded by read_chunk_bytes.
    if (fr.bytes_consumed < read_buf_fill_) {
      std::memmove(read_buf_.data(), read_buf_.data() + fr.bytes_consumed,
                   read_buf_fill_ - fr.bytes_consumed);
    }
    read_buf_fill_ -= fr.bytes_consumed;
    pipeline_->confirm_delivered(writer.written());
    if (!handle_feed_result(fr, out)) {
      aborted = true;
      break;
    }
  }

  if (saw_eof) {
    const auto eof = framer_.on_eof();
    if (!eof.clean) {
      out.choose(RunStatus::kFailed, ExitCode::kStreamFailed, eof.reason);
      out.summary =
          "input ended mid-frame: " + std::string(to_string(eof.reason));
      aborted = true;
    }
  }

  const bool drained =
      writer.stop_and_drain(cfg_.output.shutdown_drain_deadline_ms * 1'000'000);

  if (finalise(out, writer, drained, aborted)) {
    if (out.provenance == Provenance::kVerifiedCompleteFile) {
      // Only a verified source consumed to a clean frame boundary may be
      // reported as COMPLETE.
      out.choose(RunStatus::kCompleteOffline, ExitCode::kOk);
      out.summary = "offline replay completed against a verified source";
    } else {
      out.choose(RunStatus::kIncomplete, ExitCode::kSourceUnverified);
      out.summary =
          "input was processed but its completeness is unverified; not "
          "reporting COMPLETE";
    }
  }

  out.notes = source_notes_;
  out.frames_from_source = framer_.frames_complete();
  out.wall_ns = io_clock_.now_mono_ns() - t_start;
  return out;
}

// ---------------------------------------------------------------------------
// Live
// ---------------------------------------------------------------------------

RunOutcome AdapterRunner::run_live() {
  RunOutcome out;
  out.provenance = Provenance::kLiveUnverified;
  const std::int64_t t_start = io_clock_.now_mono_ns();

  Mdp1TcpClient::Options o;
  o.host = cfg_.input.host;
  o.port = static_cast<int>(cfg_.input.port);
  o.connect_deadline_ns = cfg_.input.connect_deadline_ms * 1'000'000;
  o.initial_backoff_ns = cfg_.reconnect.initial_backoff_ms * 1'000'000;
  o.max_backoff_ns = cfg_.reconnect.max_backoff_ms * 1'000'000;
  o.max_attempts = cfg_.reconnect.max_attempts;
  o.jitter_seed = cfg_.reconnect.jitter_seed;
  o.jitter_pct = cfg_.reconnect.jitter_pct;
  Mdp1TcpClient client(o);

  SinkWriter writer(queue_, sink_);
  writer.start();

  const std::int64_t tick_ns = cfg_.freshness.timer_tick_ms * 1'000'000;
  last_tick_ns_ = io_clock_.now_mono_ns();
  bool fatal = false;

  while (true) {
    const std::int64_t now = io_clock_.now_mono_ns();

    // Timers run on their own schedule: a silent socket must not stop
    // inactivity detection, and a hot socket must not starve it.
    if (now - last_tick_ns_ >= tick_ns) {
      // The cadence is real-time; the value handed to the freshness engine
      // is the pipeline's own clock, so both sides of an inactivity
      // comparison come from the same source.
      pipeline_->tick(clock_.now_mono_ns());
      last_tick_ns_ = now;
    }

    if (shutdown_flag().load()) {
      out.summary = "shutdown requested";
      break;
    }

    if (!client.connected()) {
      if (now < client.next_attempt_at_ns()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        continue;
      }
      bool again = false;
      const Reason cr = client.connect_step(now, again);
      if (cr != Reason::kNone) {
        // Only transient transport errors are retried. A protocol or config
        // error is never retried (design 4.3).
        ++pipeline_->metrics().reconnects;
        if (!client.schedule_retry(now)) {
          out.terminal_reason = cr;
          out.summary = "reconnect budget exhausted: " +
                        std::string(to_string(cr));
          fatal = true;
          break;
        }
        continue;
      }
      if (again) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        continue;
      }
      // Newly connected: a TCP session boundary, not a state reset.
      ++pipeline_->metrics().connection_epochs;
      framer_.reset(io_clock_.now_mono_ns());
      read_buf_fill_ = 0;
      continue;
    }

    if (!pump_backpressure(now, out)) {
      fatal = true;
      break;
    }
    if (pipeline_->has_pending()) continue;

    if (read_buf_fill_ == 0) {
      const ReadResult rr = client.read(std::span<std::byte>(read_buf_));
      if (rr.error != Reason::kNone) {
        out.terminal_reason = rr.error;
        client.close_connection();
        ++pipeline_->metrics().reconnects;
        if (!client.schedule_retry(io_clock_.now_mono_ns())) {
          out.summary = "reconnect budget exhausted after a transport error";
          fatal = true;
          break;
        }
        continue;
      }
      if (rr.would_block) {
        // Nothing to read: check partial-frame deadlines and let the timers
        // run instead of spinning.
        const Reason dl = framer_.check_deadlines(io_clock_.now_mono_ns());
        if (dl != Reason::kNone) {
          out.terminal_reason = dl;
          out.summary = "frame deadline expired: " + std::string(to_string(dl));
          fatal = true;
          break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(500));
        continue;
      }
      if (rr.bytes == 0 && rr.eof) {
        const auto eof = framer_.on_eof();
        if (!eof.clean) {
          out.terminal_reason = eof.reason;
          out.summary =
              "peer closed mid-frame: " + std::string(to_string(eof.reason));
          fatal = true;
          break;
        }
        client.close_connection();
        ++pipeline_->metrics().reconnects;
        if (!client.schedule_retry(io_clock_.now_mono_ns())) {
          out.summary = "peer closed and the reconnect budget is spent";
          break;
        }
        continue;
      }
      read_buf_fill_ = rr.bytes;
      pipeline_->metrics().transport_bytes_read += rr.bytes;
    }

    const auto fr =
        framer_.feed(std::span<const std::byte>(read_buf_.data(), read_buf_fill_),
                     io_clock_.now_mono_ns(), *pipeline_);
    if (fr.bytes_consumed < read_buf_fill_) {
      std::memmove(read_buf_.data(), read_buf_.data() + fr.bytes_consumed,
                   read_buf_fill_ - fr.bytes_consumed);
    }
    read_buf_fill_ -= fr.bytes_consumed;
    pipeline_->confirm_delivered(writer.written());
    if (!handle_feed_result(fr, out)) {
      fatal = true;
      break;
    }
  }

  const bool drained =
      writer.stop_and_drain(cfg_.output.shutdown_drain_deadline_ms * 1'000'000);

  if (finalise(out, writer, drained, fatal)) {
    // A live stream has no app-level ACK and no source-side manifest, so the
    // strongest truthful verdict is PARTIAL_LIVE (design section 2).
    out.choose(RunStatus::kPartialLive, ExitCode::kOk);
    if (out.summary.empty())
      out.summary = "live session observed; completeness is not provable";
  }

  out.frames_from_source = framer_.frames_complete();
  out.wall_ns = io_clock_.now_mono_ns() - t_start;
  return out;
}

}  // namespace md
