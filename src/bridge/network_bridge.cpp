#include "robot_middleware/bridge/network_bridge.hpp"

#include <algorithm>
#include <condition_variable>
#include <limits>
#include <stdexcept>
#include <thread>
#include <utility>

#include "robot_middleware/transport/heartbeat.hpp"
#include "robot_middleware/transport/tcp_transport.hpp"

namespace robot_middleware::bridge {
namespace {

using Clock = std::chrono::steady_clock;

std::uint64_t default_publisher_id() noexcept {
  const auto time_part =
      static_cast<std::uint64_t>(Clock::now().time_since_epoch().count());
  const auto thread_part = static_cast<std::uint64_t>(
      std::hash<std::thread::id>{}(std::this_thread::get_id()));
  return time_part ^ (thread_part + UINT64_C(0x9e3779b97f4a7c15));
}

void validate_reconnect_options(const ReconnectOptions &options) {
  if (options.connect_timeout <= std::chrono::milliseconds::zero() ||
      options.io_poll_interval <= std::chrono::milliseconds::zero() ||
      options.send_timeout <= std::chrono::milliseconds::zero() ||
      options.heartbeat_interval <= std::chrono::milliseconds::zero() ||
      options.idle_timeout <= options.heartbeat_interval ||
      options.initial_backoff <= std::chrono::milliseconds::zero() ||
      options.maximum_backoff < options.initial_backoff) {
    throw transport::TransportError(
        "invalid reconnect timing: intervals must be positive, idle_timeout "
        "must exceed heartbeat_interval, and maximum_backoff must not be "
        "smaller than initial_backoff");
  }
}

std::chrono::milliseconds
doubled_backoff(std::chrono::milliseconds current,
                std::chrono::milliseconds maximum) noexcept {
  if (current >= maximum) {
    return maximum;
  }
  const auto count = current.count();
  if (count > std::numeric_limits<decltype(count)>::max() / 2) {
    return maximum;
  }
  return std::min(maximum, std::chrono::milliseconds(count * 2));
}

} // namespace

struct NetworkBridge::ClientState {
  std::atomic<bool> running{false};
  std::atomic<bool> connected{false};
  std::thread worker;
  mutable std::mutex connection_mutex;
  std::shared_ptr<transport::TcpConnection> connection;
  std::mutex wait_mutex;
  std::condition_variable wait_condition;
};

NetworkBridge::NetworkBridge(Node node, NetworkBridgeOptions options)
    : node_(std::move(node)),
      publisher_id_(options.publisher_id == 0 ? default_publisher_id()
                                              : options.publisher_id),
      max_dedup_entries_(options.max_dedup_entries),
      outbound_(options.outbound_queue_depth),
      client_(std::make_unique<ClientState>()) {
  if (options.outbound_queue_depth == 0 || options.max_dedup_entries == 0) {
    throw MiddlewareError(
        "bridge queue depth and dedup entry limit must be greater than zero");
  }
}

NetworkBridge::~NetworkBridge() { stop(); }

void NetworkBridge::require_configurable() const {
  if (stopped_.load(std::memory_order_acquire)) {
    throw MiddlewareError("network bridge is stopped");
  }
}

void NetworkBridge::add_export(const std::string &remote_topic,
                               std::function<void()> cancel) {
  std::lock_guard<std::mutex> lock(configuration_mutex_);
  if (stopped_.load(std::memory_order_acquire)) {
    throw MiddlewareError("network bridge is stopped");
  }
  if (!export_topics_.insert(remote_topic).second) {
    throw MiddlewareError("bridge export topic is already registered: " +
                          remote_topic);
  }
  try {
    export_cancellers_.push_back(std::move(cancel));
  } catch (...) {
    export_topics_.erase(remote_topic);
    throw;
  }
}

void NetworkBridge::add_import(const std::string &remote_topic,
                               std::uint32_t type_id, std::uint64_t schema_hash,
                               ImportHandler handler) {
  std::lock_guard<std::mutex> lock(configuration_mutex_);
  if (stopped_.load(std::memory_order_acquire)) {
    throw MiddlewareError("network bridge is stopped");
  }
  const auto inserted =
      imports_
          .emplace(remote_topic,
                   ImportRoute{type_id, schema_hash, std::move(handler)})
          .second;
  if (!inserted) {
    throw MiddlewareError("bridge import topic is already registered: " +
                          remote_topic);
  }
}

void NetworkBridge::set_frame_sender(FrameSender sender) {
  require_configurable();
  if (!sender) {
    throw MiddlewareError("bridge frame sender must not be empty");
  }
  std::lock_guard<std::mutex> lifecycle_lock(stop_mutex_);
  require_configurable();
  {
    std::lock_guard<std::mutex> lock(configuration_mutex_);
    if (transport_mode_ != TransportMode::Unconfigured) {
      throw MiddlewareError("bridge transport mode is already configured");
    }
    manual_sender_ = sender;
    transport_mode_ = TransportMode::Manual;
    client_->running.store(true, std::memory_order_release);
  }
  try {
    client_->worker = std::thread([this, sender = std::move(sender)]() mutable {
      manual_sender_loop(std::move(sender));
    });
  } catch (...) {
    client_->running.store(false, std::memory_order_release);
    std::lock_guard<std::mutex> lock(configuration_mutex_);
    manual_sender_ = {};
    transport_mode_ = TransportMode::Unconfigured;
    throw;
  }
}

void NetworkBridge::start_tcp_client(std::string host, std::uint16_t port,
                                     ReconnectOptions options) {
  require_configurable();
  if (host.empty()) {
    throw transport::TransportError("bridge TCP host must not be empty");
  }
  validate_reconnect_options(options);
  std::lock_guard<std::mutex> lifecycle_lock(stop_mutex_);
  require_configurable();
  {
    std::lock_guard<std::mutex> lock(configuration_mutex_);
    if (transport_mode_ != TransportMode::Unconfigured) {
      throw MiddlewareError("bridge transport mode is already configured");
    }
    transport_mode_ = TransportMode::TcpClient;
    client_->running.store(true, std::memory_order_release);
  }
  try {
    client_->worker =
        std::thread([this, host = std::move(host), port, options]() {
          client_loop(host, port, options);
        });
  } catch (...) {
    client_->running.store(false, std::memory_order_release);
    std::lock_guard<std::mutex> lock(configuration_mutex_);
    transport_mode_ = TransportMode::Unconfigured;
    throw;
  }
}

void NetworkBridge::emit_frame(transport::Frame frame) noexcept {
  try {
    if (stopped_.load(std::memory_order_acquire)) {
      outbound_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    if (outbound_.try_push(std::move(frame))) {
      exported_.fetch_add(1, std::memory_order_relaxed);
    } else {
      outbound_dropped_.fetch_add(1, std::memory_order_relaxed);
    }
  } catch (...) {
    outbound_dropped_.fetch_add(1, std::memory_order_relaxed);
  }
}

/// 服务端组合模式同样通过有界队列异步交接，避免阻塞 Runtime 回调。
void NetworkBridge::manual_sender_loop(FrameSender sender) noexcept {
  try {
    while (client_->running.load(std::memory_order_acquire) &&
           !stopped_.load(std::memory_order_acquire)) {
      auto frame = outbound_.wait_pop_for(std::chrono::milliseconds(25));
      if (!frame) {
        continue;
      }
      try {
        sender(*frame);
        frames_sent_.fetch_add(1, std::memory_order_relaxed);
      } catch (...) {
        transport_errors_.fetch_add(1, std::memory_order_relaxed);
      }
    }
  } catch (...) {
    transport_errors_.fetch_add(1, std::memory_order_relaxed);
  }
}

bool NetworkBridge::receive_frame(const transport::Frame &frame) noexcept {
  try {
    if (stopped_.load(std::memory_order_acquire)) {
      return false;
    }
    if (transport::has_heartbeat_flag(frame)) {
      if (!transport::is_heartbeat_frame(frame)) {
        rejected_frames_.fetch_add(1, std::memory_order_relaxed);
        return false;
      }
      heartbeats_received_.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
    if (frame.flags != 0U) {
      rejected_frames_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    ImportRoute route;
    {
      std::lock_guard<std::mutex> lock(configuration_mutex_);
      const auto found = imports_.find(frame.topic);
      if (found == imports_.end()) {
        unknown_topic_.fetch_add(1, std::memory_order_relaxed);
        return false;
      }
      route = found->second;
    }
    if (route.type_id != frame.type_id ||
        route.schema_hash != frame.schema_hash) {
      rejected_frames_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    const std::string duplicate_key =
        frame.topic + '\n' + std::to_string(frame.publisher_id);
    {
      std::lock_guard<std::mutex> lock(duplicate_mutex_);
      auto found = highest_sequences_.find(duplicate_key);
      bool new_epoch = false;
      if (found != highest_sequences_.end() &&
          frame.sequence <= found->second.highest) {
        // 固定 publisher_id 的进程重启可从序号 0 开始；更新的发送时间标识新
        // epoch。
        new_epoch = frame.sequence == 0U &&
                    frame.send_time_ns > found->second.latest_send_time_ns;
        if (!new_epoch) {
          duplicate_frames_.fetch_add(1, std::memory_order_relaxed);
          return false;
        }
      }

      // 在调用 handler 前原子预留该序号，避免多个会话线程同时通过检查。
      // 预留后不回滚：handler 即使只完成部分副作用后抛出，同一帧也不会
      // 再次进入 handler；receive_frame 返回 false 并把它计为拒绝帧。
      if (found == highest_sequences_.end()) {
        if (highest_sequences_.size() >= max_dedup_entries_) {
          highest_sequences_.clear();
        }
        highest_sequences_.emplace(
            duplicate_key, SequenceState{frame.sequence, frame.send_time_ns});
      } else if (new_epoch || frame.sequence > found->second.highest) {
        found->second = SequenceState{frame.sequence, frame.send_time_ns};
      }
    }

    try {
      route.handler(frame);
    } catch (...) {
      rejected_frames_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    imported_.fetch_add(1, std::memory_order_relaxed);
    return true;
  } catch (...) {
    rejected_frames_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
}

void NetworkBridge::client_loop(const std::string &host, std::uint16_t port,
                                ReconnectOptions options) noexcept {
  try {
    auto backoff = options.initial_backoff;
    bool connected_before = false;
    while (client_->running.load(std::memory_order_acquire) &&
           !stopped_.load(std::memory_order_acquire)) {
      connect_attempts_.fetch_add(1, std::memory_order_relaxed);
      try {
        transport::TcpClient client(host, port);
        auto connection = std::make_shared<transport::TcpConnection>(
            client.connect(options.connect_timeout));
        connection->set_send_timeout(options.send_timeout);
        {
          std::lock_guard<std::mutex> lock(client_->connection_mutex);
          client_->connection = connection;
        }
        client_->connected.store(true, std::memory_order_release);
        successful_connections_.fetch_add(1, std::memory_order_relaxed);
        if (connected_before) {
          reconnects_.fetch_add(1, std::memory_order_relaxed);
        }
        connected_before = true;

        auto last_receive = Clock::now();
        auto next_heartbeat = last_receive + options.heartbeat_interval;
        std::uint64_t heartbeat_sequence = 0;
        bool healthy_connection = false;
        while (client_->running.load(std::memory_order_acquire) &&
               !stopped_.load(std::memory_order_acquire)) {
          // 每轮限制发送批量，避免持续发布时饿死接收和心跳处理。
          for (std::size_t sent = 0; sent < 64U; ++sent) {
            auto outbound = outbound_.try_pop();
            if (!outbound) {
              break;
            }
            connection->send_frame(*outbound);
            frames_sent_.fetch_add(1, std::memory_order_relaxed);
          }

          auto incoming = connection->receive_frame(options.io_poll_interval);
          const auto now = Clock::now();
          if (incoming) {
            if (transport::has_heartbeat_flag(*incoming)) {
              if (!transport::is_heartbeat_frame(*incoming)) {
                throw transport::TransportError(
                    "bridge received a malformed heartbeat");
              }
              heartbeats_received_.fetch_add(1, std::memory_order_relaxed);
            } else if (incoming->flags != 0U) {
              throw transport::TransportError(
                  "bridge received unsupported frame flags");
            } else {
              receive_frame(*incoming);
            }
            last_receive = now;
            if (!healthy_connection) {
              healthy_connection = true;
              backoff = options.initial_backoff;
            }
          }
          if (now >= next_heartbeat) {
            connection->send_frame(transport::make_heartbeat_frame(
                publisher_id_, heartbeat_sequence++));
            heartbeats_sent_.fetch_add(1, std::memory_order_relaxed);
            next_heartbeat = now + options.heartbeat_interval;
          }
          if (now - last_receive >= options.idle_timeout) {
            throw transport::TransportError(
                "bridge TCP peer heartbeat timed out");
          }
        }
        connection->interrupt();
        connection->close();
      } catch (...) {
        if (client_->running.load(std::memory_order_acquire) &&
            !stopped_.load(std::memory_order_acquire)) {
          transport_errors_.fetch_add(1, std::memory_order_relaxed);
        }
      }

      std::shared_ptr<transport::TcpConnection> connection_to_close;
      {
        std::lock_guard<std::mutex> lock(client_->connection_mutex);
        connection_to_close = std::move(client_->connection);
      }
      if (connection_to_close) {
        connection_to_close->interrupt();
        connection_to_close->close();
      }
      if (client_->connected.exchange(false, std::memory_order_acq_rel)) {
        disconnects_.fetch_add(1, std::memory_order_relaxed);
      }
      if (client_->running.load(std::memory_order_acquire) &&
          !stopped_.load(std::memory_order_acquire)) {
        wait_backoff(backoff);
        backoff = doubled_backoff(backoff, options.maximum_backoff);
      }
    }
  } catch (...) {
    transport_errors_.fetch_add(1, std::memory_order_relaxed);
  }
  client_->connected.store(false, std::memory_order_release);
}

void NetworkBridge::wait_backoff(std::chrono::milliseconds duration) noexcept {
  try {
    std::unique_lock<std::mutex> lock(client_->wait_mutex);
    client_->wait_condition.wait_for(lock, duration, [this] {
      return stopped_.load(std::memory_order_acquire) ||
             !client_->running.load(std::memory_order_acquire);
    });
  } catch (...) {
  }
}

void NetworkBridge::stop() noexcept {
  try {
    std::lock_guard<std::mutex> stop_lock(stop_mutex_);
    const bool first_stop = !stopped_.exchange(true, std::memory_order_acq_rel);

    if (first_stop) {
      std::vector<std::function<void()>> cancellers;
      {
        std::lock_guard<std::mutex> lock(configuration_mutex_);
        cancellers.swap(export_cancellers_);
        export_topics_.clear();
        manual_sender_ = {};
        transport_mode_ = TransportMode::Stopped;
      }
      for (auto &cancel : cancellers) {
        try {
          cancel();
        } catch (...) {
        }
      }

      client_->running.store(false, std::memory_order_release);
      client_->wait_condition.notify_all();
      outbound_.close();
    }

    std::shared_ptr<transport::TcpConnection> connection;
    {
      std::lock_guard<std::mutex> lock(client_->connection_mutex);
      connection = client_->connection;
    }
    if (connection) {
      connection->interrupt();
    }
    if (client_->worker.joinable() &&
        client_->worker.get_id() != std::this_thread::get_id()) {
      client_->worker.join();
    }
    if (connection) {
      connection->close();
    }
  } catch (...) {
  }
}

bool NetworkBridge::is_running() const noexcept {
  return !stopped_.load(std::memory_order_acquire);
}

bool NetworkBridge::is_connected() const noexcept {
  return client_->connected.load(std::memory_order_acquire);
}

NetworkBridgeStats NetworkBridge::stats() const noexcept {
  NetworkBridgeStats result;
  result.exported = exported_.load(std::memory_order_relaxed);
  result.frames_sent = frames_sent_.load(std::memory_order_relaxed);
  result.imported = imported_.load(std::memory_order_relaxed);
  result.outbound_dropped = outbound_dropped_.load(std::memory_order_relaxed);
  result.unknown_topic = unknown_topic_.load(std::memory_order_relaxed);
  result.rejected_frames = rejected_frames_.load(std::memory_order_relaxed);
  result.duplicate_frames = duplicate_frames_.load(std::memory_order_relaxed);
  result.connect_attempts = connect_attempts_.load(std::memory_order_relaxed);
  result.successful_connections =
      successful_connections_.load(std::memory_order_relaxed);
  result.reconnects = reconnects_.load(std::memory_order_relaxed);
  result.disconnects = disconnects_.load(std::memory_order_relaxed);
  result.heartbeats_sent = heartbeats_sent_.load(std::memory_order_relaxed);
  result.heartbeats_received =
      heartbeats_received_.load(std::memory_order_relaxed);
  result.transport_errors = transport_errors_.load(std::memory_order_relaxed);
  result.connected = is_connected();
  return result;
}

} // namespace robot_middleware::bridge
