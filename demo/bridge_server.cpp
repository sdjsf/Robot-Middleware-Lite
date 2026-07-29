#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>

#include "robot_middleware/bridge/network_bridge.hpp"
#include "robot_middleware/core/runtime.hpp"
#include "robot_middleware/transport/session_manager.hpp"

namespace {

std::uint64_t parse_positive(const char *text, const char *name) {
  std::size_t consumed = 0;
  const std::string value_text(text);
  const auto value = std::stoull(value_text, &consumed);
  if (consumed != value_text.size() || value == 0) {
    throw std::invalid_argument(std::string(name) +
                                " must be a positive integer");
  }
  return value;
}

} // namespace

/// 多客户端桥服务端：网络 IMU Frame 经 NetworkBridge 自动注入本地 /imu Topic。
int main(int argc, char **argv) {
  try {
    const auto port_value =
        argc > 1 ? parse_positive(argv[1], "port") : UINT64_C(7600);
    const auto expected =
        argc > 2 ? parse_positive(argv[2], "expected") : UINT64_C(200);
    const std::string bind_address = argc > 3 ? argv[3] : "127.0.0.1";
    if (port_value > 65535U) {
      throw std::invalid_argument("port must be in the range 1..65535");
    }

    robot_middleware::Runtime runtime(4);
    auto node = runtime.create_node("bridge_server_node");
    robot_middleware::bridge::NetworkBridge bridge(node);
    bridge.import_topic<robot_middleware::ImuMsg>("/imu");

    std::mutex mutex;
    std::condition_variable completed;
    std::uint64_t received = 0;
    auto subscription = node.create_subscription<robot_middleware::ImuMsg>(
        "/imu", [&](const robot_middleware::ImuMsg &) {
          std::lock_guard<std::mutex> lock(mutex);
          ++received;
          if (received >= expected) {
            completed.notify_one();
          }
        });

    robot_middleware::transport::TcpSessionManager manager(
        bind_address, static_cast<std::uint16_t>(port_value),
        [&](robot_middleware::transport::TcpSessionManager::SessionId,
            const robot_middleware::transport::Frame &frame) {
          bridge.receive_frame(frame);
        });
    bridge.set_frame_sender([&](const auto &frame) {
      static_cast<void>(manager.broadcast(frame));
    });
    manager.start();

    std::cout << "Bridge server listening on " << bind_address << ':'
              << manager.local_port() << ", waiting for " << expected
              << " total IMU messages from one or more clients ...\n";
    {
      std::unique_lock<std::mutex> lock(mutex);
      if (!completed.wait_for(lock, std::chrono::seconds(60),
                              [&] { return received >= expected; })) {
        throw std::runtime_error("bridge server timed out");
      }
    }

    const auto session_stats = manager.stats();
    const auto bridge_stats = bridge.stats();
    std::cout << "Bridge result: received=" << received
              << ", accepted_clients=" << session_stats.accepted
              << ", active_clients=" << session_stats.active
              << ", imported=" << bridge_stats.imported
              << ", heartbeats_rx=" << session_stats.heartbeats_received
              << ", transport_errors=" << session_stats.transport_errors
              << '\n';

    manager.stop();
    subscription.cancel_and_wait();
    bridge.stop();
    runtime.shutdown();
    return received >= expected ? 0 : 2;
  } catch (const std::exception &error) {
    std::cerr << "bridge_server: " << error.what() << '\n';
    std::cerr
        << "usage: rml_bridge_server [port] [expected_total] [bind_address]\n";
    return 1;
  }
}
