#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "robot_middleware/core/runtime.hpp"
#include "robot_middleware/core/thread_safe_queue.hpp"
#include "robot_middleware/serialization/network_message.hpp"
#include "robot_middleware/transport/frame.hpp"

namespace robot_middleware::bridge {

/// NetworkBridge 本地导出队列和发布者标识配置。
struct NetworkBridgeOptions {
  std::uint64_t publisher_id{0};
  std::size_t outbound_queue_depth{1024};
  std::size_t max_dedup_entries{4096};
};

/// TCP 客户端连接、心跳、失活判断和指数退避配置。
struct ReconnectOptions {
  std::chrono::milliseconds connect_timeout{500};
  std::chrono::milliseconds io_poll_interval{25};
  std::chrono::milliseconds send_timeout{250};
  std::chrono::milliseconds heartbeat_interval{250};
  std::chrono::milliseconds idle_timeout{1000};
  std::chrono::milliseconds initial_backoff{50};
  std::chrono::milliseconds maximum_backoff{1000};
};

/// 桥接发送、接收、重连和控制面统计。
struct NetworkBridgeStats {
  std::uint64_t exported{0};
  std::uint64_t frames_sent{0};
  std::uint64_t imported{0};
  std::uint64_t outbound_dropped{0};
  std::uint64_t unknown_topic{0};
  std::uint64_t rejected_frames{0};
  std::uint64_t duplicate_frames{0};
  std::uint64_t connect_attempts{0};
  std::uint64_t successful_connections{0};
  std::uint64_t reconnects{0};
  std::uint64_t disconnects{0};
  std::uint64_t heartbeats_sent{0};
  std::uint64_t heartbeats_received{0};
  std::uint64_t transport_errors{0};
  bool connected{false};
};

/// 把强类型 Topic 与网络 Frame 双向连接的显式桥。
///
/// export_topic 创建“只接收本地消息”的订阅；receive_frame 使用
/// Publisher::publish_remote 注入，因此远端消息不会被桥再次导出。
/// TCP 客户端模式在单独 I/O 线程中完成发送、接收、心跳和自动重连。
class NetworkBridge final {
public:
  using FrameSender = std::function<void(const transport::Frame &)>;

  explicit NetworkBridge(Node node, NetworkBridgeOptions options = {});
  ~NetworkBridge();

  NetworkBridge(const NetworkBridge &) = delete;
  NetworkBridge &operator=(const NetworkBridge &) = delete;

  /// 导出本地强类型 Topic；remote_topic 为空时保持原 Topic 名。
  template <typename MessageT>
  void export_topic(const std::string &local_topic,
                    const std::string &remote_topic = {},
                    SubscriptionOptions options = {}) {
    static_assert(is_message_v<MessageT>,
                  "MessageT requires a MessageTraits specialization");
    require_configurable();
    const std::string network_topic =
        remote_topic.empty() ? local_topic : remote_topic;
    if (local_topic.empty() || network_topic.empty()) {
      throw MiddlewareError("bridge topic names must not be empty");
    }

    options.receive_remote = false;
    auto subscription = std::make_shared<Subscription<MessageT>>(
        node_.create_subscription<MessageT>(
            local_topic,
            [this, network_topic](const MessageT &message) {
              auto frame = serialization::make_frame(
                  network_topic, message,
                  next_sequence_.fetch_add(1, std::memory_order_relaxed),
                  publisher_id_);
              emit_frame(std::move(frame));
            },
            options));

    add_export(network_topic,
               [subscription] { subscription->cancel_and_wait(); });
  }

  /// 将远端强类型 Topic 映射成本地 Topic；local_topic 为空时保持原名。
  template <typename MessageT>
  void import_topic(const std::string &remote_topic,
                    const std::string &local_topic = {}) {
    static_assert(is_message_v<MessageT>,
                  "MessageT requires a MessageTraits specialization");
    require_configurable();
    const std::string destination =
        local_topic.empty() ? remote_topic : local_topic;
    if (remote_topic.empty() || destination.empty()) {
      throw MiddlewareError("bridge topic names must not be empty");
    }

    auto publisher = node_.create_publisher<MessageT>(destination);
    add_import(remote_topic, MessageTraits<MessageT>::type_id,
               MessageTraits<MessageT>::schema_hash,
               [publisher = std::move(publisher),
                remote_topic](const transport::Frame &frame) mutable {
                 auto message = serialization::deserialize_frame<MessageT>(
                     frame, remote_topic);
                 publisher.publish_remote(std::move(message));
               });
  }

  /// 服务端组合模式：把导出帧交给 SessionManager::broadcast/send_to。
  void set_frame_sender(FrameSender sender);

  /// 客户端模式：启动单独 I/O 线程，并在断线后按指数退避自动连接。
  void start_tcp_client(std::string host, std::uint16_t port,
                        ReconnectOptions options = {});

  /// 服务端 FrameHandler 或测试可调用此入口，把网络帧注入已注册 import。
  /// 相同发布者序号会在调用 import handler 前原子预留；handler 失败也不回滚，
  /// 因而并发重复帧最多执行一次导入副作用。
  bool receive_frame(const transport::Frame &frame) noexcept;

  /// 停止导出、关闭 TCP、唤醒退避等待并 join 后台线程；可重复调用。
  void stop() noexcept;
  bool is_running() const noexcept;
  bool is_connected() const noexcept;
  NetworkBridgeStats stats() const noexcept;

private:
  using ImportHandler = std::function<void(const transport::Frame &)>;

  struct ImportRoute {
    std::uint32_t type_id{0};
    std::uint64_t schema_hash{0};
    ImportHandler handler;
  };

  struct ClientState;
  enum class TransportMode {
    Unconfigured,
    Manual,
    TcpClient,
    Stopped,
  };
  struct SequenceState {
    std::uint64_t highest{0};
    std::uint64_t latest_send_time_ns{0};
  };

  void require_configurable() const;
  void add_export(const std::string &remote_topic,
                  std::function<void()> cancel);
  void add_import(const std::string &remote_topic, std::uint32_t type_id,
                  std::uint64_t schema_hash, ImportHandler handler);
  void emit_frame(transport::Frame frame) noexcept;
  void client_loop(const std::string &host, std::uint16_t port,
                   ReconnectOptions options) noexcept;
  void manual_sender_loop(FrameSender sender) noexcept;
  void wait_backoff(std::chrono::milliseconds duration) noexcept;

  Node node_;
  std::uint64_t publisher_id_{0};
  std::size_t max_dedup_entries_{0};
  std::atomic<std::uint64_t> next_sequence_{0};
  ThreadSafeQueue<transport::Frame> outbound_;

  mutable std::mutex configuration_mutex_;
  std::vector<std::function<void()>> export_cancellers_;
  std::unordered_set<std::string> export_topics_;
  std::unordered_map<std::string, ImportRoute> imports_;
  FrameSender manual_sender_;
  TransportMode transport_mode_{TransportMode::Unconfigured};

  mutable std::mutex duplicate_mutex_;
  std::unordered_map<std::string, SequenceState> highest_sequences_;

  std::unique_ptr<ClientState> client_;
  mutable std::mutex stop_mutex_;

  std::atomic<bool> stopped_{false};
  std::atomic<std::uint64_t> exported_{0};
  std::atomic<std::uint64_t> frames_sent_{0};
  std::atomic<std::uint64_t> imported_{0};
  std::atomic<std::uint64_t> outbound_dropped_{0};
  std::atomic<std::uint64_t> unknown_topic_{0};
  std::atomic<std::uint64_t> rejected_frames_{0};
  std::atomic<std::uint64_t> duplicate_frames_{0};
  std::atomic<std::uint64_t> connect_attempts_{0};
  std::atomic<std::uint64_t> successful_connections_{0};
  std::atomic<std::uint64_t> reconnects_{0};
  std::atomic<std::uint64_t> disconnects_{0};
  std::atomic<std::uint64_t> heartbeats_sent_{0};
  std::atomic<std::uint64_t> heartbeats_received_{0};
  std::atomic<std::uint64_t> transport_errors_{0};
};

} // namespace robot_middleware::bridge
