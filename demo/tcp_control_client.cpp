#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include <unistd.h>

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

// TCP 控制客户端：建立可靠连接后按固定频率发送 ControlMsg。
int main(int argc, char** argv) {
  try {
    const std::string host = argc > 1 ? argv[1] : "127.0.0.1";
    const auto port_value = argc > 2 ? parse_positive(argv[2], "port") : 7500U;
    const auto count = argc > 3 ? parse_positive(argv[3], "count") : 100U;
    const auto rate_hz = argc > 4 ? parse_positive(argv[4], "rate_hz") : 50U;
    if (port_value > 65535U) {
      throw std::invalid_argument("port must be in the range 1..65535");
    }

    // 客户端连接使用显式超时，避免目标不可达时永久阻塞。
    robot_middleware::transport::TcpClient client(
        host, static_cast<std::uint16_t>(port_value));
    auto connection = client.connect(std::chrono::seconds(5));
    const std::uint64_t publisher_id =
        robot_middleware::serialization::monotonic_time_ns() ^
        static_cast<std::uint64_t>(::getpid());
    const auto period = std::chrono::nanoseconds(1'000'000'000ULL / rate_hz);
    auto next_send = std::chrono::steady_clock::now();

    // 以绝对时间调度发送周期，sequence 在单连接内单调递增。
    for (std::uint64_t sequence = 0; sequence < count; ++sequence) {
      next_send += period;
      const auto now_ns = robot_middleware::serialization::monotonic_time_ns();
      robot_middleware::ControlMsg command;
      command.timestamp_ns = now_ns;
      command.linear_x = 0.2;
      command.linear_y = 0.0;
      command.angular_z = sequence % 20U == 0U ? 0.1 : 0.0;
      connection.send_frame(robot_middleware::serialization::make_frame(
          "/cmd_vel", command, sequence, publisher_id, now_ns));
      std::this_thread::sleep_until(next_send);
    }

    std::cout << "TCP client finished: sent=" << count
              << ", rate_hz=" << rate_hz << '\n';
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "tcp_control_client: " << error.what() << '\n';
    std::cerr << "usage: rml_tcp_control_client [host] [port] [count] [rate_hz]\n";
    return 1;
  }
}
