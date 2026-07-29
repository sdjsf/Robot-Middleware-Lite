#include <chrono>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>

#include "robot_middleware/benchmark/statistics.hpp"
#include "robot_middleware/core/message.hpp"
#include "robot_middleware/serialization/network_message.hpp"
#include "robot_middleware/transport/tcp_transport.hpp"

namespace {

/// 解析必须大于 0 的命令行整数参数。
std::uint64_t parse_positive(const char* text, const char* name) {
  std::size_t consumed = 0;
  const std::string value_text(text);
  const auto value = std::stoull(value_text, &consumed);
  if (consumed != value_text.size() || value == 0) {
    throw std::invalid_argument(std::string(name) + " must be a positive integer");
  }
  return value;
}

}  // namespace

// TCP 控制服务端：接受一个客户端，接收 ControlMsg 并输出可靠传输统计。
int main(int argc, char** argv) {
  try {
    const auto port_value = argc > 1 ? parse_positive(argv[1], "port") : 7500U;
    const auto expected = argc > 2 ? parse_positive(argv[2], "expected") : 100U;
    const std::string bind_address = argc > 3 ? argv[3] : "127.0.0.1";
    if (port_value > 65535U) {
      throw std::invalid_argument("port must be in the range 1..65535");
    }

    robot_middleware::transport::TcpServer server(
        bind_address, static_cast<std::uint16_t>(port_value));
    std::cout << "TCP control server listening on " << bind_address << ':'
              << server.local_port() << " ...\n";
    // accept 设置上限，防止演示程序在没有客户端时永久挂起。
    auto connection = server.accept(std::chrono::seconds(30));
    if (!connection) {
      throw std::runtime_error("timed out waiting for a TCP client");
    }

    robot_middleware::benchmark::LatencyRecorder latencies;
    robot_middleware::benchmark::SequenceTracker sequences;
    std::uint64_t received = 0;
    // TCP 保证字节可靠有序，但仍通过协议 sequence 检查应用层完整性。
    while (received < expected) {
      auto frame = connection->receive_frame(std::chrono::seconds(5));
      if (!frame) {
        break;
      }
      const auto command = robot_middleware::serialization::deserialize_frame<
          robot_middleware::ControlMsg>(*frame, "/cmd_vel");
      static_cast<void>(command);
      const auto now_ns = robot_middleware::serialization::monotonic_time_ns();
      if (now_ns >= frame->send_time_ns) {
        latencies.record(std::chrono::nanoseconds(now_ns - frame->send_time_ns));
      }
      sequences.observe(frame->sequence);
      ++received;
    }

    const auto packets = sequences.summary(expected);
    const auto latency = latencies.summary();
    std::cout << std::fixed << std::setprecision(3)
              << "TCP result: received=" << packets.unique_received << '/'
              << packets.expected << ", lost=" << packets.lost
              << ", duplicate=" << packets.duplicates
              << ", out_of_order=" << packets.out_of_order << '\n'
              << "latency_us: p50=" << latency.p50_us
              << ", p95=" << latency.p95_us << ", p99=" << latency.p99_us
              << ", max=" << latency.max_us << '\n';
    return received == expected ? 0 : 2;
  } catch (const std::exception& error) {
    std::cerr << "tcp_control_server: " << error.what() << '\n';
    std::cerr
        << "usage: rml_tcp_control_server [port] [expected] [bind_address]\n";
    return 1;
  }
}
