#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include <unistd.h>

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

// UDP 传感器进程：按指定频率生成 ImuMsg，封装 Frame 后发送到控制进程。
int main(int argc, char** argv) {
  try {
    const std::string host = argc > 1 ? argv[1] : "127.0.0.1";
    const auto port_value = argc > 2 ? parse_positive(argv[2], "port") : 7400U;
    const auto count = argc > 3 ? parse_positive(argv[3], "count") : 1000U;
    const auto rate_hz = argc > 4 ? parse_positive(argv[4], "rate_hz") : 100U;
    if (port_value > 65535U) {
      throw std::invalid_argument("port must be in the range 1..65535");
    }

    robot_middleware::transport::UdpSender sender(
        host, static_cast<std::uint16_t>(port_value));
    // publisher_id 区分不同发送实例，sequence 用于接收端统计丢包和乱序。
    const std::uint64_t publisher_id =
        robot_middleware::serialization::monotonic_time_ns() ^
        static_cast<std::uint64_t>(::getpid());
    const auto period = std::chrono::nanoseconds(1'000'000'000ULL / rate_hz);
    auto next_send = std::chrono::steady_clock::now();

    // 使用绝对下一发送时刻，避免每轮处理耗时累积到周期中。
    for (std::uint64_t sequence = 0; sequence < count; ++sequence) {
      next_send += period;
      const auto now_ns = robot_middleware::serialization::monotonic_time_ns();
      robot_middleware::ImuMsg imu;
      imu.timestamp_ns = now_ns;
      imu.yaw = static_cast<double>(sequence % 360U);
      imu.gyro_z = 0.05;
      sender.send_frame(robot_middleware::serialization::make_frame(
          "/imu", imu, sequence, publisher_id, now_ns));
      std::this_thread::sleep_until(next_send);
    }

    std::cout << "UDP sensor finished: sent=" << count
              << ", rate_hz=" << rate_hz << ", target=" << host << ':'
              << port_value << '\n';
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "udp_sensor: " << error.what() << '\n';
    std::cerr << "usage: rml_udp_sensor [host] [port] [count] [rate_hz]\n";
    return 1;
  }
}
