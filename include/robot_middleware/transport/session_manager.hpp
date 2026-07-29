#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "robot_middleware/transport/frame.hpp"
#include "robot_middleware/transport/tcp_transport.hpp"

namespace robot_middleware::transport {

/// TCP 多客户端服务端的轮询、心跳和失活策略。
struct TcpSessionManagerOptions {
  std::chrono::milliseconds accept_poll_interval{50};
  std::chrono::milliseconds receive_poll_interval{50};
  std::chrono::milliseconds send_timeout{250};
  std::chrono::milliseconds heartbeat_interval{500};
  std::chrono::milliseconds idle_timeout{2000};
  std::size_t max_clients{64};
  TcpServerOptions server{};
};

/// 多客户端服务端累计统计快照。
struct TcpSessionManagerStats {
  std::uint64_t accepted{0};
  std::uint64_t rejected_clients{0};
  std::uint64_t active{0};
  std::uint64_t disconnected{0};
  std::uint64_t frames_received{0};
  std::uint64_t frames_sent{0};
  std::uint64_t heartbeats_received{0};
  std::uint64_t heartbeats_sent{0};
  std::uint64_t callback_errors{0};
  std::uint64_t transport_errors{0};
  std::uint64_t protocol_errors{0};
  std::uint64_t idle_timeouts{0};
};

/// 持续 accept 并为每个客户端维护独立接收循环的 TCP SessionManager。
///
/// FrameHandler 在会话锁外调用且应快速完成（通常只向业务队列投递）；
/// 一个客户端的回调不会占用其他客户端的会话线程。
/// stop() 会关闭全部连接并 join accept/worker 线程，可重复调用。
class TcpSessionManager final {
public:
  using SessionId = std::uint64_t;
  using FrameHandler = std::function<void(SessionId, const Frame &)>;

  TcpSessionManager(std::string bind_address, std::uint16_t port,
                    FrameHandler handler,
                    TcpSessionManagerOptions options = {});
  ~TcpSessionManager();

  TcpSessionManager(const TcpSessionManager &) = delete;
  TcpSessionManager &operator=(const TcpSessionManager &) = delete;

  /// 启动 accept 循环；已运行时不产生副作用。
  void start();
  /// 停止接入、关闭全部客户端并等待后台线程结束。
  void stop() noexcept;
  bool is_running() const noexcept;

  /// 返回构造时绑定的实际端口，port=0 时用于发现系统分配端口。
  std::uint16_t local_port() const;

  /// 向一个存活客户端发送业务帧；会话不存在或发送失败时返回 false。
  bool send_to(SessionId session_id, const Frame &frame) noexcept;
  /// 向当前全部存活客户端广播，返回成功发送的客户端数。
  std::size_t broadcast(const Frame &frame) noexcept;

  TcpSessionManagerStats stats() const noexcept;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace robot_middleware::transport
