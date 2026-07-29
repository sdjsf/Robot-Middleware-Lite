#include "robot_middleware/transport/session_manager.hpp"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "robot_middleware/transport/heartbeat.hpp"

namespace robot_middleware::transport {
namespace {

using Clock = std::chrono::steady_clock;
thread_local const void *active_session_manager = nullptr;

/// 所有轮询间隔必须为正，idle timeout 必须足够容纳至少一次心跳。
void validate_options(const TcpSessionManagerOptions &options) {
  if (options.accept_poll_interval <= std::chrono::milliseconds::zero() ||
      options.receive_poll_interval <= std::chrono::milliseconds::zero() ||
      options.send_timeout <= std::chrono::milliseconds::zero() ||
      options.heartbeat_interval <= std::chrono::milliseconds::zero() ||
      options.idle_timeout <= options.heartbeat_interval ||
      options.accept_poll_interval > std::chrono::milliseconds(100) ||
      options.receive_poll_interval > std::chrono::milliseconds(100) ||
      options.max_clients == 0U) {
    throw TransportError(
        "session intervals must be positive, poll intervals must not exceed "
        "100 ms, idle_timeout must exceed heartbeat_interval, and "
        "max_clients must be positive");
  }
}

/// 对端完成发送后主动关闭属于正常会话结束，不计入传输故障。
bool is_orderly_peer_disconnect(const TransportError &error) {
  return std::string_view(error.what()).find("peer disconnected") !=
         std::string_view::npos;
}

} // namespace

struct TcpSessionManager::Impl {
  struct Session {
    SessionId id{0};
    std::shared_ptr<TcpConnection> connection;
    std::thread worker;
    std::atomic<bool> active{true};
  };

  Impl(std::string address, std::uint16_t port, FrameHandler frame_handler,
       TcpSessionManagerOptions manager_options)
      : options(std::move(manager_options)),
        server(std::move(address), port, options.server),
        handler(std::move(frame_handler)) {
    validate_options(options);
    if (!handler) {
      throw TransportError("session manager frame handler must not be empty");
    }
    manager_id =
        static_cast<std::uint64_t>(Clock::now().time_since_epoch().count()) ^
        static_cast<std::uint64_t>(server.local_port());
  }

  void start() {
    if (active_session_manager == this) {
      throw TransportError(
          "session manager cannot be started from its frame handler");
    }
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex);
    if (running.load(std::memory_order_acquire)) {
      return;
    }
    finish_stop_locked();
    running.store(true, std::memory_order_release);
    try {
      accept_thread = std::thread([this] { accept_loop(); });
    } catch (...) {
      running.store(false, std::memory_order_release);
      throw;
    }
  }

  void stop() noexcept {
    // 回调运行于会话 worker，不能同步 join 自身；这里只发停止请求，
    // 后续外部 stop/start/析构会完成线程回收。
    if (active_session_manager == this) {
      request_stop();
      return;
    }
    try {
      std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex);
      request_stop();
      finish_stop_locked();
    } catch (...) {
    }
  }

  void request_stop() noexcept {
    running.store(false, std::memory_order_release);
    try {
      std::lock_guard<std::mutex> lock(sessions_mutex);
      for (const auto &session : sessions) {
        session->connection->interrupt();
      }
    } catch (...) {
    }
  }

  /// lifecycle_mutex 持有者回收 accept 与全部会话线程。
  void finish_stop_locked() noexcept {
    if (accept_thread.joinable()) {
      accept_thread.join();
    }
    std::vector<std::shared_ptr<Session>> snapshot;
    {
      std::lock_guard<std::mutex> lock(sessions_mutex);
      snapshot.swap(sessions);
    }
    for (const auto &session : snapshot) {
      session->connection->interrupt();
    }
    for (const auto &session : snapshot) {
      if (session->worker.joinable()) {
        session->worker.join();
      }
    }
  }

  void accept_loop() noexcept {
    while (running.load(std::memory_order_acquire)) {
      try {
        auto connection = server.accept(options.accept_poll_interval);
        if (connection) {
          if (active.load(std::memory_order_acquire) >= options.max_clients) {
            rejected_clients.fetch_add(1, std::memory_order_relaxed);
            connection->interrupt();
            connection->close();
            continue;
          }
          auto session = std::make_shared<Session>();
          session->id = next_session_id.fetch_add(1, std::memory_order_relaxed);
          session->connection =
              std::make_shared<TcpConnection>(std::move(*connection));
          session->connection->set_send_timeout(options.send_timeout);
          {
            std::lock_guard<std::mutex> lock(sessions_mutex);
            sessions.push_back(session);
          }
          accepted.fetch_add(1, std::memory_order_relaxed);
          active.fetch_add(1, std::memory_order_relaxed);
          try {
            session->worker =
                std::thread([this, session] { session_loop(session); });
          } catch (...) {
            session->active.store(false, std::memory_order_release);
            session->connection->interrupt();
            session->connection->close();
            active.fetch_sub(1, std::memory_order_relaxed);
            disconnected.fetch_add(1, std::memory_order_relaxed);
            {
              std::lock_guard<std::mutex> lock(sessions_mutex);
              sessions.erase(
                  std::remove(sessions.begin(), sessions.end(), session),
                  sessions.end());
            }
            throw;
          }
        }
        reap_finished_sessions();
      } catch (...) {
        if (running.load(std::memory_order_acquire)) {
          transport_errors.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
    try {
      reap_finished_sessions();
    } catch (...) {
      transport_errors.fetch_add(1, std::memory_order_relaxed);
    }
  }

  void session_loop(const std::shared_ptr<Session> &session) noexcept {
    active_session_manager = this;
    auto last_receive = Clock::now();
    auto next_heartbeat = last_receive + options.heartbeat_interval;
    std::uint64_t heartbeat_sequence = 0;
    try {
      while (running.load(std::memory_order_acquire) &&
             session->active.load(std::memory_order_acquire)) {
        auto frame =
            session->connection->receive_frame(options.receive_poll_interval);
        const auto now = Clock::now();
        if (frame) {
          last_receive = now;
          if (has_heartbeat_flag(*frame)) {
            if (!is_heartbeat_frame(*frame)) {
              protocol_errors.fetch_add(1, std::memory_order_relaxed);
              throw TransportError("malformed TCP heartbeat frame");
            }
            heartbeats_received.fetch_add(1, std::memory_order_relaxed);
          } else if (frame->flags != 0U) {
            protocol_errors.fetch_add(1, std::memory_order_relaxed);
            throw TransportError("TCP frame uses unsupported flags");
          } else {
            frames_received.fetch_add(1, std::memory_order_relaxed);
            try {
              handler(session->id, *frame);
            } catch (...) {
              callback_errors.fetch_add(1, std::memory_order_relaxed);
            }
          }
        }

        if (now >= next_heartbeat) {
          session->connection->send_frame(
              make_heartbeat_frame(manager_id, heartbeat_sequence++));
          heartbeats_sent.fetch_add(1, std::memory_order_relaxed);
          next_heartbeat = now + options.heartbeat_interval;
        }
        if (now - last_receive >= options.idle_timeout) {
          idle_timeouts.fetch_add(1, std::memory_order_relaxed);
          break;
        }
      }
    } catch (const TransportError &error) {
      if (running.load(std::memory_order_acquire) &&
          !is_orderly_peer_disconnect(error)) {
        transport_errors.fetch_add(1, std::memory_order_relaxed);
      }
    } catch (...) {
      if (running.load(std::memory_order_acquire)) {
        transport_errors.fetch_add(1, std::memory_order_relaxed);
      }
    }

    session->connection->interrupt();
    session->connection->close();
    if (session->active.exchange(false, std::memory_order_acq_rel)) {
      active.fetch_sub(1, std::memory_order_relaxed);
      disconnected.fetch_add(1, std::memory_order_relaxed);
    }
    active_session_manager = nullptr;
  }

  /// accept 线程负责回收已退出 worker，避免会话长期运行时积累线程句柄。
  void reap_finished_sessions() {
    std::vector<std::shared_ptr<Session>> finished;
    {
      std::lock_guard<std::mutex> lock(sessions_mutex);
      auto iterator = sessions.begin();
      while (iterator != sessions.end()) {
        if (!(*iterator)->active.load(std::memory_order_acquire)) {
          finished.push_back(*iterator);
          iterator = sessions.erase(iterator);
        } else {
          ++iterator;
        }
      }
    }
    for (const auto &session : finished) {
      if (session->worker.joinable()) {
        session->worker.join();
      }
    }
  }

  std::vector<std::shared_ptr<Session>> active_snapshot() const {
    std::vector<std::shared_ptr<Session>> snapshot;
    std::lock_guard<std::mutex> lock(sessions_mutex);
    for (const auto &session : sessions) {
      if (session->active.load(std::memory_order_acquire)) {
        snapshot.push_back(session);
      }
    }
    return snapshot;
  }

  TcpSessionManagerOptions options;
  TcpServer server;
  FrameHandler handler;
  std::uint64_t manager_id{0};

  std::atomic<bool> running{false};
  mutable std::mutex lifecycle_mutex;
  std::thread accept_thread;
  mutable std::mutex sessions_mutex;
  std::vector<std::shared_ptr<Session>> sessions;
  std::atomic<SessionId> next_session_id{1};

  std::atomic<std::uint64_t> accepted{0};
  std::atomic<std::uint64_t> rejected_clients{0};
  std::atomic<std::uint64_t> active{0};
  std::atomic<std::uint64_t> disconnected{0};
  std::atomic<std::uint64_t> frames_received{0};
  std::atomic<std::uint64_t> frames_sent{0};
  std::atomic<std::uint64_t> heartbeats_received{0};
  std::atomic<std::uint64_t> heartbeats_sent{0};
  std::atomic<std::uint64_t> callback_errors{0};
  std::atomic<std::uint64_t> transport_errors{0};
  std::atomic<std::uint64_t> protocol_errors{0};
  std::atomic<std::uint64_t> idle_timeouts{0};
};

TcpSessionManager::TcpSessionManager(std::string bind_address,
                                     std::uint16_t port, FrameHandler handler,
                                     TcpSessionManagerOptions options)
    : impl_(std::make_unique<Impl>(std::move(bind_address), port,
                                   std::move(handler), std::move(options))) {}

TcpSessionManager::~TcpSessionManager() { stop(); }

void TcpSessionManager::start() { impl_->start(); }

void TcpSessionManager::stop() noexcept { impl_->stop(); }

bool TcpSessionManager::is_running() const noexcept {
  return impl_->running.load(std::memory_order_acquire);
}

std::uint16_t TcpSessionManager::local_port() const {
  return impl_->server.local_port();
}

bool TcpSessionManager::send_to(SessionId session_id,
                                const Frame &frame) noexcept {
  std::shared_ptr<Impl::Session> target;
  try {
    const auto snapshot = impl_->active_snapshot();
    const auto found = std::find_if(snapshot.begin(), snapshot.end(),
                                    [session_id](const auto &session) {
                                      return session->id == session_id;
                                    });
    if (found == snapshot.end()) {
      return false;
    }
    target = *found;
    target->connection->send_frame(frame);
    impl_->frames_sent.fetch_add(1, std::memory_order_relaxed);
    return true;
  } catch (...) {
    impl_->transport_errors.fetch_add(1, std::memory_order_relaxed);
    if (target) {
      target->connection->interrupt();
    }
    return false;
  }
}

std::size_t TcpSessionManager::broadcast(const Frame &frame) noexcept {
  std::size_t sent = 0;
  try {
    for (const auto &session : impl_->active_snapshot()) {
      try {
        session->connection->send_frame(frame);
        impl_->frames_sent.fetch_add(1, std::memory_order_relaxed);
        ++sent;
      } catch (...) {
        impl_->transport_errors.fetch_add(1, std::memory_order_relaxed);
        session->connection->interrupt();
      }
    }
  } catch (...) {
    impl_->transport_errors.fetch_add(1, std::memory_order_relaxed);
  }
  return sent;
}

TcpSessionManagerStats TcpSessionManager::stats() const noexcept {
  TcpSessionManagerStats result;
  result.accepted = impl_->accepted.load(std::memory_order_relaxed);
  result.rejected_clients =
      impl_->rejected_clients.load(std::memory_order_relaxed);
  result.active = impl_->active.load(std::memory_order_relaxed);
  result.disconnected = impl_->disconnected.load(std::memory_order_relaxed);
  result.frames_received =
      impl_->frames_received.load(std::memory_order_relaxed);
  result.frames_sent = impl_->frames_sent.load(std::memory_order_relaxed);
  result.heartbeats_received =
      impl_->heartbeats_received.load(std::memory_order_relaxed);
  result.heartbeats_sent =
      impl_->heartbeats_sent.load(std::memory_order_relaxed);
  result.callback_errors =
      impl_->callback_errors.load(std::memory_order_relaxed);
  result.transport_errors =
      impl_->transport_errors.load(std::memory_order_relaxed);
  result.protocol_errors =
      impl_->protocol_errors.load(std::memory_order_relaxed);
  result.idle_timeouts = impl_->idle_timeouts.load(std::memory_order_relaxed);
  return result;
}

} // namespace robot_middleware::transport
