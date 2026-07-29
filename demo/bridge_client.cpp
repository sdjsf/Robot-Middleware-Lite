#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include "robot_middleware/bridge/network_bridge.hpp"
#include "robot_middleware/core/runtime.hpp"
#include "robot_middleware/serialization/network_message.hpp"

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

template <typename Predicate>
bool wait_until(Predicate &&predicate, std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return predicate();
}

} // namespace

/// 桥客户端：本地 /imu Publisher 自动序列化并经可重连 TCP 链路导出。
int main(int argc, char **argv) {
  try {
    const std::string host = argc > 1 ? argv[1] : "127.0.0.1";
    const auto port_value =
        argc > 2 ? parse_positive(argv[2], "port") : UINT64_C(7600);
    const auto count =
        argc > 3 ? parse_positive(argv[3], "count") : UINT64_C(100);
    const auto rate_hz =
        argc > 4 ? parse_positive(argv[4], "rate_hz") : UINT64_C(100);
    if (port_value > 65535U) {
      throw std::invalid_argument("port must be in the range 1..65535");
    }

    robot_middleware::Runtime runtime(2);
    auto node = runtime.create_node("bridge_client_node");
    robot_middleware::bridge::NetworkBridge bridge(node);
    bridge.export_topic<robot_middleware::ImuMsg>("/imu");
    bridge.start_tcp_client(host, static_cast<std::uint16_t>(port_value));

    if (!wait_until([&] { return bridge.is_connected(); },
                    std::chrono::seconds(10))) {
      throw std::runtime_error("bridge client could not connect");
    }

    auto publisher = node.create_publisher<robot_middleware::ImuMsg>("/imu");
    const auto period =
        std::chrono::nanoseconds(UINT64_C(1000000000) / rate_hz);
    for (std::uint64_t sequence = 0; sequence < count; ++sequence) {
      robot_middleware::ImuMsg message;
      message.timestamp_ns =
          robot_middleware::serialization::monotonic_time_ns();
      message.yaw = static_cast<double>(sequence) * 0.01;
      message.gyro_z = 0.1;
      publisher.publish(std::move(message));
      std::this_thread::sleep_for(period);
    }

    if (!wait_until([&] { return bridge.stats().frames_sent >= count; },
                    std::chrono::seconds(10))) {
      throw std::runtime_error("bridge client did not flush outbound messages");
    }
    const auto stats = bridge.stats();
    std::cout << "Bridge client result: published=" << count
              << ", queued=" << stats.exported << ", sent=" << stats.frames_sent
              << ", reconnects=" << stats.reconnects
              << ", heartbeats_tx=" << stats.heartbeats_sent
              << ", dropped=" << stats.outbound_dropped << '\n';

    bridge.stop();
    runtime.shutdown();
    return stats.frames_sent >= count && stats.outbound_dropped == 0U ? 0 : 2;
  } catch (const std::exception &error) {
    std::cerr << "bridge_client: " << error.what() << '\n';
    std::cerr << "usage: rml_bridge_client [host] [port] [count] [rate_hz]\n";
    return 1;
  }
}
