#include <chrono>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>

#include "robot_middleware/benchmark/statistics.hpp"
#include "robot_middleware/core/message.hpp"
#include "robot_middleware/serialization/network_message.hpp"
#include "robot_middleware/transport/udp_transport.hpp"

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

// UDP 控制进程：接收并校验 IMU Frame，输出丢包、乱序、吞吐和延迟统计。
int main(int argc, char** argv) {
  try {
    const std::string bind_address = argc > 1 ? argv[1] : "127.0.0.1";
    const auto port_value = argc > 2 ? parse_positive(argv[2], "port") : 7400U;
    const auto expected = argc > 3 ? parse_positive(argv[3], "expected") : 1000U;
    const auto timeout_ms =
        argc > 4 ? parse_positive(argv[4], "timeout_ms") : 3000U;
    if (port_value > 65535U) {
      throw std::invalid_argument("port must be in the range 1..65535");
    }

    robot_middleware::transport::UdpReceiver receiver(
        bind_address, static_cast<std::uint16_t>(port_value));
    robot_middleware::benchmark::LatencyRecorder latencies;
    robot_middleware::benchmark::SequenceTracker sequences;
    std::uint64_t malformed = 0;
    std::uint64_t received = 0;
    std::optional<std::chrono::steady_clock::time_point> first_receive;
    std::optional<std::chrono::steady_clock::time_point> last_receive;

    std::cout << "UDP control listening on " << bind_address << ':'
              << receiver.local_port() << " ...\n";
    // 只统计有效且类型、schema、Topic 均匹配的消息。
    while (received < expected) {
      try {
        auto frame = receiver.receive_frame(
            std::chrono::milliseconds(static_cast<long long>(timeout_ms)));
        if (!frame) {
          break;
        }
        const auto imu = robot_middleware::serialization::deserialize_frame<
            robot_middleware::ImuMsg>(*frame, "/imu");
        static_cast<void>(imu);
        // 单调时钟只适用于本机进程间单向延迟；跨机器需要时钟同步或 RTT。
        const auto now_ns = robot_middleware::serialization::monotonic_time_ns();
        if (now_ns >= frame->send_time_ns) {
          latencies.record(std::chrono::nanoseconds(now_ns - frame->send_time_ns));
        }
        sequences.observe(frame->sequence);
        ++received;
        const auto receive_time = std::chrono::steady_clock::now();
        if (!first_receive) {
          first_receive = receive_time;
        }
        last_receive = receive_time;
      } catch (const std::exception& error) {
        // 单个坏 Datagram 不终止长期运行的接收进程。
        ++malformed;
        std::cerr << "discarded datagram: " << error.what() << '\n';
      }
    }

    // 吞吐时间窗从首条有效接收到末条有效接收，排除启动等待时间。
    const double duration_s = first_receive && last_receive
                                  ? std::chrono::duration<double>(
                                        *last_receive - *first_receive)
                                        .count()
                                  : 0.0;
    const auto packet_stats = sequences.summary(expected);
    const auto latency = latencies.summary();
    std::cout << std::fixed << std::setprecision(3)
              << "UDP result: received=" << packet_stats.unique_received
              << '/' << packet_stats.expected << ", lost=" << packet_stats.lost
              << ", loss_rate=" << packet_stats.loss_rate * 100.0
              << "%, duplicate=" << packet_stats.duplicates
              << ", out_of_order=" << packet_stats.out_of_order
              << ", malformed=" << malformed
              << ", throughput_msg_s="
              << (duration_s > 0.0
                      ? packet_stats.unique_received / duration_s
                      : 0.0)
              << '\n'
              << "latency_us: p50=" << latency.p50_us
              << ", p95=" << latency.p95_us << ", p99=" << latency.p99_us
              << ", max=" << latency.max_us << '\n';
    return received == 0 ? 2 : 0;
  } catch (const std::exception& error) {
    std::cerr << "udp_control: " << error.what() << '\n';
    std::cerr
        << "usage: rml_udp_control [bind_address] [port] [expected] [timeout_ms]\n";
    return 1;
  }
}
