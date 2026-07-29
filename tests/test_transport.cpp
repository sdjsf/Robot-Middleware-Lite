#include "test_harness.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <mutex>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include "robot_middleware/transport/frame.hpp"
#include "robot_middleware/transport/heartbeat.hpp"
#include "robot_middleware/transport/session_manager.hpp"
#include "robot_middleware/transport/tcp_transport.hpp"
#include "robot_middleware/transport/udp_transport.hpp"

namespace {

using namespace std::chrono_literals;
using robot_middleware::transport::decode_frame;
using robot_middleware::transport::encode_frame;
using robot_middleware::transport::Frame;
using robot_middleware::transport::is_heartbeat_frame;
using robot_middleware::transport::make_heartbeat_frame;
using robot_middleware::transport::TcpClient;
using robot_middleware::transport::TcpServer;
using robot_middleware::transport::TcpSessionManager;
using robot_middleware::transport::TcpSessionManagerOptions;
using robot_middleware::transport::TransportError;
using robot_middleware::transport::UdpReceiver;
using robot_middleware::transport::UdpSender;
using robot_middleware::transport::UdpTransportOptions;

/// 异常路径下也保证测试客户端线程被 join。
class ThreadJoiner {
public:
  explicit ThreadJoiner(std::thread &thread) noexcept : thread_(thread) {}

  ~ThreadJoiner() {
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  ThreadJoiner(const ThreadJoiner &) = delete;
  ThreadJoiner &operator=(const ThreadJoiner &) = delete;

private:
  std::thread &thread_;
};

/// 构造包含全部头字段和二进制负载的测试 Frame。
Frame make_frame(std::uint64_t sequence = UINT64_C(0x1112131415161718)) {
  Frame frame;
  frame.flags = 0xa5U;
  frame.type_id = UINT32_C(0x01020304);
  frame.schema_hash = UINT64_C(0x2122232425262728);
  frame.sequence = sequence;
  frame.publisher_id = UINT64_C(0x3132333435363738);
  frame.send_time_ns = UINT64_C(0x4142434445464748);
  frame.topic = "/sensors/imu";
  frame.payload = {0x00U, 0x01U, 0x7fU, 0x80U, 0xfeU, 0xffU, 0x55U, 0xaaU};
  return frame;
}

/// 逐字段比较两个 Frame。
void check_frame_equal(const Frame &actual, const Frame &expected) {
  RML_CHECK_EQ(actual.flags, expected.flags);
  RML_CHECK_EQ(actual.type_id, expected.type_id);
  RML_CHECK_EQ(actual.schema_hash, expected.schema_hash);
  RML_CHECK_EQ(actual.sequence, expected.sequence);
  RML_CHECK_EQ(actual.publisher_id, expected.publisher_id);
  RML_CHECK_EQ(actual.send_time_ns, expected.send_time_ns);
  RML_CHECK_EQ(actual.topic, expected.topic);
  RML_CHECK(actual.payload == expected.payload);
}

/// 验证编码后的固定头部和所有字段能够完整往返。
void frame_fields_roundtrip() {
  const Frame original = make_frame();
  const std::vector<std::uint8_t> encoded = encode_frame(original);

  RML_CHECK_EQ(encoded.size(), static_cast<std::size_t>(Frame::kHeaderSize) +
                                   original.topic.size() +
                                   original.payload.size());
  RML_CHECK_EQ(encoded[0], static_cast<std::uint8_t>('R'));
  RML_CHECK_EQ(encoded[1], static_cast<std::uint8_t>('M'));
  RML_CHECK_EQ(encoded[2], static_cast<std::uint8_t>('W'));
  RML_CHECK_EQ(encoded[3], static_cast<std::uint8_t>('L'));
  RML_CHECK_EQ(encoded[4], Frame::kVersion);
  RML_CHECK_EQ(encoded[6], std::uint8_t{0U});
  RML_CHECK_EQ(encoded[7], std::uint8_t{56U});

  check_frame_equal(decode_frame(encoded), original);
}

/// 验证 magic、版本、保留字段、长度、CRC、截断和尾随错误均被拒绝。
void malformed_frames_are_rejected() {
  const std::vector<std::uint8_t> valid = encode_frame(make_frame());

  std::vector<std::uint8_t> bad_magic = valid;
  bad_magic[0] ^= 0x01U;
  RML_CHECK_THROWS(TransportError, decode_frame(bad_magic));

  std::vector<std::uint8_t> bad_crc = valid;
  bad_crc.back() ^= 0x01U;
  RML_CHECK_THROWS(TransportError, decode_frame(bad_crc));

  std::vector<std::uint8_t> bad_version = valid;
  bad_version[4] = static_cast<std::uint8_t>(Frame::kVersion + 1U);
  RML_CHECK_THROWS(TransportError, decode_frame(bad_version));

  std::vector<std::uint8_t> bad_reserved = valid;
  bad_reserved[10] = 0x01U;
  RML_CHECK_THROWS(TransportError, decode_frame(bad_reserved));

  std::vector<std::uint8_t> oversized_payload = valid;
  oversized_payload[12] = 0xffU;
  oversized_payload[13] = 0xffU;
  oversized_payload[14] = 0xffU;
  oversized_payload[15] = 0xffU;
  RML_CHECK_THROWS(TransportError, decode_frame(oversized_payload));

  std::vector<std::uint8_t> truncated = valid;
  truncated.pop_back();
  RML_CHECK_THROWS(TransportError, decode_frame(truncated));

  std::vector<std::uint8_t> trailing = valid;
  trailing.push_back(0x5aU);
  RML_CHECK_THROWS(TransportError, decode_frame(trailing));
}

/// 验证 UDP 本机单帧往返和无数据超时。
void udp_loopback_and_timeout() {
  UdpReceiver receiver("127.0.0.1", 0U);
  RML_CHECK(receiver.local_port() != 0U);
  UdpSender sender("127.0.0.1", receiver.local_port());

  const Frame expected = make_frame(UINT64_C(1001));
  sender.send_frame(expected);
  const auto received = receiver.receive_frame(1s);
  RML_CHECK(received.has_value());
  check_frame_equal(*received, expected);

  RML_CHECK(!receiver.receive_frame(10ms).has_value());
}

/// 验证发送端尺寸限制以及接收端 MSG_TRUNC 检测。
void udp_datagram_limit_is_enforced() {
  UdpTransportOptions impossible_datagram;
  impossible_datagram.max_datagram_size = 65'508U;
  RML_CHECK_THROWS(TransportError,
                   UdpReceiver("127.0.0.1", 0U, impossible_datagram));

  UdpTransportOptions small_datagram;
  small_datagram.max_datagram_size = 64U;
  UdpReceiver receiver("127.0.0.1", 0U, small_datagram);

  const Frame oversized = make_frame(UINT64_C(1002));
  UdpSender limited_sender("127.0.0.1", receiver.local_port(), small_datagram);
  RML_CHECK_THROWS(TransportError, limited_sender.send_frame(oversized));

  // 使用较大限制的发送端发出报文，接收端必须报告 MSG_TRUNC，不能解码半包。
  UdpSender full_sender("127.0.0.1", receiver.local_port());
  full_sender.send_frame(oversized);
  RML_CHECK_THROWS(TransportError, receiver.receive_frame(1s));
}

/// 验证 TCP 客户端与服务端正常收发一个 Frame。
void tcp_single_frame_loopback() {
  TcpServer server("127.0.0.1", 0U);
  RML_CHECK(server.local_port() != 0U);
  const Frame expected = make_frame(UINT64_C(2001));

  std::exception_ptr client_error;
  std::thread client_thread([&] {
    try {
      TcpClient client("127.0.0.1", server.local_port());
      auto connection = client.connect(2s);
      connection.send_frame(expected);
    } catch (...) {
      client_error = std::current_exception();
    }
  });
  ThreadJoiner joiner(client_thread);

  auto connection = server.accept(2s);
  RML_CHECK(connection.has_value());
  const auto received = connection->receive_frame(2s);
  RML_CHECK(received.has_value());
  check_frame_equal(*received, expected);

  client_thread.join();
  if (client_error) {
    std::rethrow_exception(client_error);
  }
}

/// 将帧按 1/2/3 字节分片发送，验证 TCP 接收端能够重组半包。
void tcp_fragmented_frame_is_reassembled() {
  TcpServer server("127.0.0.1", 0U);
  const Frame expected = make_frame(UINT64_C(2002));
  const std::vector<std::uint8_t> encoded = encode_frame(expected);

  std::exception_ptr client_error;
  std::thread client_thread([&] {
    try {
      TcpClient client("127.0.0.1", server.local_port());
      auto connection = client.connect(2s);
      std::size_t offset = 0U;
      std::size_t chunk_size = 1U;
      while (offset < encoded.size()) {
        const std::size_t count = std::min(chunk_size, encoded.size() - offset);
        connection.send_all(encoded.data() + offset, count);
        offset += count;
        chunk_size = chunk_size == 3U ? 1U : chunk_size + 1U;
      }
    } catch (...) {
      client_error = std::current_exception();
    }
  });
  ThreadJoiner joiner(client_thread);

  auto connection = server.accept(2s);
  RML_CHECK(connection.has_value());
  const auto received = connection->receive_frame(2s);
  RML_CHECK(received.has_value());
  check_frame_equal(*received, expected);

  client_thread.join();
  if (client_error) {
    std::rethrow_exception(client_error);
  }
}

/// 一次写入两个连续帧，验证接收端能够正确拆分粘包。
void tcp_coalesced_frames_are_split() {
  TcpServer server("127.0.0.1", 0U);
  const Frame first = make_frame(UINT64_C(3001));
  const Frame second = make_frame(UINT64_C(3002));
  auto bytes = encode_frame(first);
  const auto second_bytes = encode_frame(second);
  bytes.insert(bytes.end(), second_bytes.begin(), second_bytes.end());

  std::exception_ptr client_error;
  std::thread client_thread([&] {
    try {
      TcpClient client("127.0.0.1", server.local_port());
      auto connection = client.connect(2s);
      connection.send_all(bytes.data(), bytes.size());
    } catch (...) {
      client_error = std::current_exception();
    }
  });
  ThreadJoiner joiner(client_thread);

  auto connection = server.accept(2s);
  RML_CHECK(connection.has_value());
  const auto received_first = connection->receive_frame(2s);
  const auto received_second = connection->receive_frame(2s);
  RML_CHECK(received_first.has_value());
  RML_CHECK(received_second.has_value());
  check_frame_equal(*received_first, first);
  check_frame_equal(*received_second, second);

  client_thread.join();
  if (client_error) {
    std::rethrow_exception(client_error);
  }
}

/// 验证对端在固定头部中途断开时接收端抛出明确错误。
void tcp_disconnect_during_frame_is_rejected() {
  TcpServer server("127.0.0.1", 0U);
  const auto encoded = encode_frame(make_frame(UINT64_C(4001)));

  std::exception_ptr client_error;
  std::thread client_thread([&] {
    try {
      TcpClient client("127.0.0.1", server.local_port());
      auto connection = client.connect(2s);
      connection.send_all(encoded.data(), 10U);
      connection.close();
    } catch (...) {
      client_error = std::current_exception();
    }
  });
  ThreadJoiner joiner(client_thread);

  auto connection = server.accept(2s);
  RML_CHECK(connection.has_value());
  RML_CHECK_THROWS(TransportError, connection->receive_frame(2s));

  client_thread.join();
  if (client_error) {
    std::rethrow_exception(client_error);
  }
}

/// 在给定期限内跳过控制面心跳，返回下一条业务帧。
std::optional<Frame> receive_application_frame(
    robot_middleware::transport::TcpConnection &connection,
    std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    auto frame = connection.receive_frame(25ms);
    if (frame && !is_heartbeat_frame(*frame)) {
      return frame;
    }
  }
  return std::nullopt;
}

template <typename Predicate>
bool wait_until(Predicate &&predicate, std::chrono::milliseconds timeout = 2s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(5ms);
  }
  return predicate();
}

/// 验证持续 accept、两个并发客户端、心跳过滤、业务回调和服务端广播。
void session_manager_handles_multiple_clients() {
  TcpSessionManagerOptions options;
  options.accept_poll_interval = 10ms;
  options.receive_poll_interval = 10ms;
  options.heartbeat_interval = 20ms;
  options.idle_timeout = 500ms;

  std::mutex observed_mutex;
  std::set<TcpSessionManager::SessionId> observed_sessions;
  std::promise<void> both_received_promise;
  auto both_received = both_received_promise.get_future();
  bool completion_signaled = false;

  TcpSessionManager manager(
      "127.0.0.1", 0U,
      [&](TcpSessionManager::SessionId session_id, const Frame &) {
        std::lock_guard<std::mutex> lock(observed_mutex);
        observed_sessions.insert(session_id);
        if (observed_sessions.size() == 2U && !completion_signaled) {
          completion_signaled = true;
          both_received_promise.set_value();
        }
      },
      options);
  manager.start();

  TcpClient first_client("127.0.0.1", manager.local_port());
  TcpClient second_client("127.0.0.1", manager.local_port());
  auto first = first_client.connect(2s);
  auto second = second_client.connect(2s);

  first.send_frame(make_heartbeat_frame(101U, 0U));
  second.send_frame(make_heartbeat_frame(202U, 0U));
  Frame first_application = make_frame(UINT64_C(5001));
  Frame second_application = make_frame(UINT64_C(5002));
  first_application.flags = 0U;
  second_application.flags = 0U;
  first.send_frame(first_application);
  second.send_frame(second_application);

  RML_CHECK(both_received.wait_for(2s) == std::future_status::ready);
  both_received.get();

  Frame broadcast = make_frame(UINT64_C(6001));
  broadcast.flags = 0U;
  RML_CHECK_EQ(manager.broadcast(broadcast), std::size_t{2});
  const auto first_broadcast = receive_application_frame(first, 2s);
  const auto second_broadcast = receive_application_frame(second, 2s);
  RML_CHECK(first_broadcast.has_value());
  RML_CHECK(second_broadcast.has_value());
  check_frame_equal(*first_broadcast, broadcast);
  check_frame_equal(*second_broadcast, broadcast);

  const auto running_stats = manager.stats();
  RML_CHECK_EQ(running_stats.accepted, UINT64_C(2));
  RML_CHECK_EQ(running_stats.active, UINT64_C(2));
  RML_CHECK_EQ(running_stats.frames_received, UINT64_C(2));
  RML_CHECK_EQ(running_stats.frames_sent, UINT64_C(2));
  RML_CHECK(running_stats.heartbeats_received >= UINT64_C(2));

  manager.stop();
  manager.stop();
  RML_CHECK(!manager.is_running());
  RML_CHECK_EQ(manager.stats().active, UINT64_C(0));
}

/// 验证带未知保留位的心跳不会进入业务回调，并触发协议级断开。
void session_manager_rejects_malformed_heartbeat() {
  TcpSessionManagerOptions options;
  options.accept_poll_interval = 10ms;
  options.receive_poll_interval = 10ms;
  options.heartbeat_interval = 20ms;
  options.idle_timeout = 300ms;

  std::atomic<std::size_t> callbacks{0};
  TcpSessionManager manager(
      "127.0.0.1", 0U,
      [&](TcpSessionManager::SessionId, const Frame &) {
        callbacks.fetch_add(1, std::memory_order_relaxed);
      },
      options);
  manager.start();
  TcpClient client("127.0.0.1", manager.local_port());
  auto connection = client.connect(2s);

  Frame malformed = make_heartbeat_frame(303U, 0U);
  malformed.flags |= 0x02U;
  RML_CHECK(robot_middleware::transport::has_heartbeat_flag(malformed));
  RML_CHECK(!is_heartbeat_frame(malformed));
  connection.send_frame(malformed);

  RML_CHECK(wait_until([&] {
    const auto stats = manager.stats();
    return stats.protocol_errors == 1U && stats.active == 0U;
  }));
  RML_CHECK_EQ(callbacks.load(), std::size_t{0});
  manager.stop();
}

/// 验证 stop 使用 shutdown 唤醒不读数据客户端造成的阻塞发送。
void session_manager_stop_interrupts_blocked_sender() {
  TcpSessionManagerOptions options;
  options.accept_poll_interval = 10ms;
  options.receive_poll_interval = 10ms;
  options.send_timeout = 5s;
  options.heartbeat_interval = 100ms;
  options.idle_timeout = 2s;

  TcpSessionManager manager(
      "127.0.0.1", 0U, [](TcpSessionManager::SessionId, const Frame &) {},
      options);
  manager.start();
  TcpClient client("127.0.0.1", manager.local_port());
  auto connection = client.connect(2s);
  RML_CHECK(wait_until([&] { return manager.stats().active == 1U; }));

  Frame large;
  large.topic = "/large";
  large.payload.resize(8U * 1024U * 1024U, 0x5aU);
  auto sending =
      std::async(std::launch::async, [&] { return manager.broadcast(large); });
  std::this_thread::sleep_for(25ms);

  const auto started = std::chrono::steady_clock::now();
  manager.stop();
  const auto elapsed = std::chrono::steady_clock::now() - started;
  RML_CHECK(elapsed < 1s);
  RML_CHECK(sending.wait_for(1s) == std::future_status::ready);
  static_cast<void>(sending.get());
}

} // namespace

// 线协议与 UDP/TCP 回环测试入口。
int main(int argc, char **argv) {
  return rml_test::run(
      {
          {"frame_roundtrip", frame_fields_roundtrip},
          {"frame_rejections", malformed_frames_are_rejected},
          {"udp_loopback", udp_loopback_and_timeout},
          {"udp_limit", udp_datagram_limit_is_enforced},
          {"tcp_loopback", tcp_single_frame_loopback},
          {"tcp_fragmented", tcp_fragmented_frame_is_reassembled},
          {"tcp_coalesced", tcp_coalesced_frames_are_split},
          {"tcp_partial_disconnect", tcp_disconnect_during_frame_is_rejected},
          {"session_manager_multi_client",
           session_manager_handles_multiple_clients},
          {"session_manager_bad_heartbeat",
           session_manager_rejects_malformed_heartbeat},
          {"session_manager_stop_interrupt",
           session_manager_stop_interrupts_blocked_sender},
      },
      argc, argv);
}
