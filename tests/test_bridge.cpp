#include "test_harness.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "robot_middleware/bridge/network_bridge.hpp"
#include "robot_middleware/core/runtime.hpp"
#include "robot_middleware/serialization/network_message.hpp"
#include "robot_middleware/transport/heartbeat.hpp"
#include "robot_middleware/transport/session_manager.hpp"

namespace {

using namespace std::chrono_literals;

using robot_middleware::ImuMsg;
using robot_middleware::MiddlewareError;
using robot_middleware::Runtime;
using robot_middleware::bridge::NetworkBridge;
using robot_middleware::bridge::NetworkBridgeOptions;
using robot_middleware::bridge::ReconnectOptions;
using robot_middleware::transport::Frame;
using robot_middleware::transport::TcpSessionManager;
using robot_middleware::transport::TcpSessionManagerOptions;

template <typename Predicate>
bool wait_until(Predicate &&predicate, std::chrono::milliseconds timeout = 3s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(5ms);
  }
  return predicate();
}

/// 验证 export→Frame→import→本地订阅闭环，并确认远端注入不会再次导出。
void bridge_routes_typed_topic_without_echo() {
  Runtime source_runtime(2);
  Runtime destination_runtime(2);
  auto source_node = source_runtime.create_node("bridge_source");
  auto destination_node = destination_runtime.create_node("bridge_destination");

  NetworkBridgeOptions source_options;
  source_options.publisher_id = 1001U;
  NetworkBridge source_bridge(source_node, source_options);
  NetworkBridge destination_bridge(destination_node);

  destination_bridge.import_topic<ImuMsg>("/imu");
  std::atomic<std::size_t> echoed{0};
  destination_bridge.export_topic<ImuMsg>("/imu");
  destination_bridge.set_frame_sender(
      [&](const Frame &) { echoed.fetch_add(1, std::memory_order_relaxed); });

  std::mutex captured_mutex;
  std::vector<Frame> captured;
  source_bridge.set_frame_sender([&](const Frame &frame) {
    {
      std::lock_guard<std::mutex> lock(captured_mutex);
      captured.push_back(frame);
    }
    destination_bridge.receive_frame(frame);
  });
  source_bridge.export_topic<ImuMsg>("/imu");

  std::mutex received_mutex;
  std::condition_variable received_condition;
  std::vector<ImuMsg> received;
  auto business_subscription = destination_node.create_subscription<ImuMsg>(
      "/imu", [&](const ImuMsg &message) {
        {
          std::lock_guard<std::mutex> lock(received_mutex);
          received.push_back(message);
        }
        received_condition.notify_all();
      });

  auto publisher = source_node.create_publisher<ImuMsg>("/imu");
  ImuMsg expected;
  expected.timestamp_ns = 42U;
  expected.yaw = 1.25;
  expected.gyro_z = -0.5;
  publisher.publish(expected);

  {
    std::unique_lock<std::mutex> lock(received_mutex);
    RML_CHECK(received_condition.wait_for(
        lock, 3s, [&] { return received.size() == 1U; }));
  }
  RML_CHECK_EQ(received[0].timestamp_ns, expected.timestamp_ns);
  RML_CHECK(received[0].yaw == expected.yaw);
  RML_CHECK(received[0].gyro_z == expected.gyro_z);
  RML_CHECK_EQ(echoed.load(), std::size_t{0});

  Frame duplicate;
  {
    std::lock_guard<std::mutex> lock(captured_mutex);
    RML_CHECK_EQ(captured.size(), std::size_t{1});
    duplicate = captured.front();
  }
  RML_CHECK(!destination_bridge.receive_frame(duplicate));
  RML_CHECK_EQ(destination_bridge.stats().duplicate_frames, UINT64_C(1));

  Frame restarted_publisher = duplicate;
  ++restarted_publisher.send_time_ns;
  RML_CHECK(destination_bridge.receive_frame(restarted_publisher));
  {
    std::unique_lock<std::mutex> lock(received_mutex);
    RML_CHECK(received_condition.wait_for(
        lock, 3s, [&] { return received.size() == 2U; }));
  }
  RML_CHECK_EQ(destination_bridge.stats().imported, UINT64_C(2));
  RML_CHECK_EQ(echoed.load(), std::size_t{0});

  Frame malformed_heartbeat =
      robot_middleware::transport::make_heartbeat_frame(7U, 0U);
  malformed_heartbeat.topic = "/imu";
  RML_CHECK(!destination_bridge.receive_frame(malformed_heartbeat));
  RML_CHECK_EQ(destination_bridge.stats().rejected_frames, UINT64_C(1));

  Frame reserved_heartbeat =
      robot_middleware::transport::make_heartbeat_frame(7U, 1U);
  reserved_heartbeat.flags |= 0x02U;
  RML_CHECK(!destination_bridge.receive_frame(reserved_heartbeat));
  RML_CHECK_EQ(destination_bridge.stats().rejected_frames, UINT64_C(2));

  RML_CHECK_THROWS(MiddlewareError, source_bridge.export_topic<ImuMsg>(
                                        "/second_local_topic", "/imu"));

  business_subscription.cancel_and_wait();
  source_bridge.stop();
  destination_bridge.stop();
}

/// 多个会话线程同时提交同一帧时，只有一个线程可以预留序号并执行导入。
void bridge_deduplicates_concurrent_identical_frames() {
  Runtime runtime(2);
  auto node = runtime.create_node("bridge_concurrent_dedup");
  NetworkBridge bridge(node);
  bridge.import_topic<ImuMsg>("/imu");

  // 扩大导入 handler 的 fan-out 工作量，使旧的“检查后再更新”竞态能够
  // 稳定暴露；修复后其余线程在进入 handler 前即被去重。
  constexpr std::size_t kFanoutSubscriptions = 512U;
  std::vector<robot_middleware::Subscription<ImuMsg>> subscriptions;
  subscriptions.reserve(kFanoutSubscriptions);
  for (std::size_t index = 0; index < kFanoutSubscriptions; ++index) {
    subscriptions.push_back(
        node.create_subscription<ImuMsg>("/imu", [](const ImuMsg &) {}));
  }

  ImuMsg message;
  message.timestamp_ns = 123U;
  message.yaw = 0.25;
  message.gyro_z = -0.75;
  const Frame frame =
      robot_middleware::serialization::make_frame("/imu", message, 9U, 4004U);

  constexpr std::size_t kThreadCount = 32U;
  std::mutex gate_mutex;
  std::condition_variable gate_condition;
  std::size_t ready = 0;
  bool start = false;
  std::atomic<std::size_t> accepted{0};
  std::vector<std::thread> workers;
  workers.reserve(kThreadCount);
  for (std::size_t index = 0; index < kThreadCount; ++index) {
    workers.emplace_back([&] {
      {
        std::unique_lock<std::mutex> lock(gate_mutex);
        ++ready;
        gate_condition.notify_all();
        gate_condition.wait(lock, [&] { return start; });
      }
      if (bridge.receive_frame(frame)) {
        accepted.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  {
    std::unique_lock<std::mutex> lock(gate_mutex);
    gate_condition.wait(lock, [&] { return ready == kThreadCount; });
    start = true;
  }
  gate_condition.notify_all();
  for (auto &worker : workers) {
    worker.join();
  }

  const auto stats = bridge.stats();
  RML_CHECK_EQ(accepted.load(), std::size_t{1});
  RML_CHECK_EQ(stats.imported, UINT64_C(1));
  RML_CHECK_EQ(stats.duplicate_frames,
               static_cast<std::uint64_t>(kThreadCount - 1U));
  RML_CHECK_EQ(stats.rejected_frames, UINT64_C(0));
  bridge.stop();
}

/// handler 抛出后保留已预留序号，保证可能产生部分副作用的帧不会被重放。
void bridge_handler_failure_consumes_sequence() {
  Runtime runtime(1);
  auto node = runtime.create_node("bridge_handler_failure");
  NetworkBridge bridge(node);
  bridge.import_topic<ImuMsg>("/imu");

  ImuMsg message;
  Frame malformed =
      robot_middleware::serialization::make_frame("/imu", message, 11U, 5005U);
  malformed.payload.pop_back();

  RML_CHECK(!bridge.receive_frame(malformed));
  auto stats = bridge.stats();
  RML_CHECK_EQ(stats.imported, UINT64_C(0));
  RML_CHECK_EQ(stats.rejected_frames, UINT64_C(1));
  RML_CHECK_EQ(stats.duplicate_frames, UINT64_C(0));

  RML_CHECK(!bridge.receive_frame(malformed));
  stats = bridge.stats();
  RML_CHECK_EQ(stats.imported, UINT64_C(0));
  RML_CHECK_EQ(stats.rejected_frames, UINT64_C(1));
  RML_CHECK_EQ(stats.duplicate_frames, UINT64_C(1));
  bridge.stop();
}

/// 验证服务端退出后客户端检测断线，并在同端口恢复后自动重连继续传输。
void bridge_reconnects_after_server_restart() {
  Runtime server_runtime(2);
  Runtime client_runtime(2);
  auto server_node = server_runtime.create_node("bridge_server");
  auto client_node = client_runtime.create_node("bridge_client");

  NetworkBridge server_bridge(server_node);
  server_bridge.import_topic<ImuMsg>("/imu");

  std::mutex received_mutex;
  std::condition_variable received_condition;
  std::vector<std::uint64_t> timestamps;
  auto server_subscription = server_node.create_subscription<ImuMsg>(
      "/imu", [&](const ImuMsg &message) {
        {
          std::lock_guard<std::mutex> lock(received_mutex);
          timestamps.push_back(message.timestamp_ns);
        }
        received_condition.notify_all();
      });

  TcpSessionManagerOptions server_options;
  server_options.accept_poll_interval = 10ms;
  server_options.receive_poll_interval = 10ms;
  server_options.heartbeat_interval = 40ms;
  server_options.idle_timeout = 300ms;

  auto make_server = [&](std::uint16_t port) {
    return std::make_unique<TcpSessionManager>(
        "127.0.0.1", port,
        [&](TcpSessionManager::SessionId, const Frame &frame) {
          server_bridge.receive_frame(frame);
        },
        server_options);
  };

  auto server = make_server(0U);
  const std::uint16_t port = server->local_port();
  server->start();

  NetworkBridgeOptions client_bridge_options;
  client_bridge_options.publisher_id = 2002U;
  NetworkBridge client_bridge(client_node, client_bridge_options);
  client_bridge.export_topic<ImuMsg>("/imu");

  ReconnectOptions reconnect;
  reconnect.connect_timeout = 100ms;
  reconnect.io_poll_interval = 10ms;
  reconnect.heartbeat_interval = 40ms;
  reconnect.idle_timeout = 300ms;
  reconnect.initial_backoff = 20ms;
  reconnect.maximum_backoff = 100ms;
  client_bridge.start_tcp_client("127.0.0.1", port, reconnect);

  RML_CHECK(wait_until([&] {
    return client_bridge.is_connected() && server->stats().active == 1U;
  }));

  auto publisher = client_node.create_publisher<ImuMsg>("/imu");
  ImuMsg first;
  first.timestamp_ns = 1U;
  publisher.publish(first);
  {
    std::unique_lock<std::mutex> lock(received_mutex);
    RML_CHECK(received_condition.wait_for(
        lock, 3s, [&] { return timestamps.size() >= 1U; }));
  }

  server->stop();
  server.reset();
  RML_CHECK(
      wait_until([&] { return client_bridge.stats().disconnects >= 1U; }, 2s));
  RML_CHECK(wait_until(
      [&] { return client_bridge.stats().connect_attempts >= 2U; }, 2s));

  server = make_server(port);
  server->start();
  RML_CHECK(wait_until([&] {
    const auto stats = client_bridge.stats();
    return stats.connected && stats.successful_connections >= 2U &&
           stats.reconnects >= 1U && server->stats().active == 1U;
  }));

  ImuMsg second;
  second.timestamp_ns = 2U;
  publisher.publish(second);
  {
    std::unique_lock<std::mutex> lock(received_mutex);
    RML_CHECK(received_condition.wait_for(
        lock, 3s, [&] { return timestamps.size() >= 2U; }));
  }
  RML_CHECK_EQ(timestamps[0], UINT64_C(1));
  RML_CHECK_EQ(timestamps[1], UINT64_C(2));

  RML_CHECK(wait_until([&] {
    const auto stats = client_bridge.stats();
    return stats.heartbeats_sent >= 1U && stats.heartbeats_received >= 1U;
  }));
  const auto bridge_stats = client_bridge.stats();
  RML_CHECK(bridge_stats.heartbeats_sent >= UINT64_C(1));
  RML_CHECK(bridge_stats.heartbeats_received >= UINT64_C(1));
  RML_CHECK_EQ(bridge_stats.outbound_dropped, UINT64_C(0));

  client_bridge.stop();
  server->stop();
  server_subscription.cancel_and_wait();
  server_bridge.stop();
}

} // namespace

int main(int argc, char **argv) {
  return rml_test::run(
      {
          {"typed_topic_no_echo", bridge_routes_typed_topic_without_echo},
          {"concurrent_dedup", bridge_deduplicates_concurrent_identical_frames},
          {"handler_failure_consumes_sequence",
           bridge_handler_failure_consumes_sequence},
          {"tcp_reconnect", bridge_reconnects_after_server_restart},
      },
      argc, argv);
}
