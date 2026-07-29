#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <thread>

#include "robot_middleware/core/runtime.hpp"
#include "robot_middleware/serialization/network_message.hpp"

// 单进程演示：IMU 节点发布数据，控制节点生成速度指令，执行器节点消费指令。
int main() {
  try {
    constexpr std::uint64_t kMessageCount = 200;
    robot_middleware::Runtime runtime(4);
    // 三个 Node 共享同一个 Runtime/TopicBus，但业务角色彼此解耦。
    const auto imu_node = runtime.create_node("imu_node");
    const auto control_node = runtime.create_node("control_node");
    const auto actuator_node = runtime.create_node("actuator_node");

    const auto imu_publisher =
        imu_node.create_publisher<robot_middleware::ImuMsg>("/imu");
    const auto command_publisher =
        control_node.create_publisher<robot_middleware::ControlMsg>("/cmd_vel");

    // 用条件变量等待完整链路结束，避免用固定 sleep 猜测回调是否完成。
    std::mutex completion_mutex;
    std::condition_variable completion;
    std::uint64_t commands_received = 0;

    robot_middleware::SubscriptionOptions options;
    options.queue_depth = 64;
    // 执行器订阅控制指令，并每 50 条打印一次可观察状态。
    auto command_subscription =
        actuator_node.create_subscription<robot_middleware::ControlMsg>(
            "/cmd_vel",
            [&](const robot_middleware::ControlMsg& command) {
              std::lock_guard<std::mutex> lock(completion_mutex);
              ++commands_received;
              if (commands_received % 50U == 0U) {
                std::cout << "[ACTUATOR] count=" << commands_received
                          << ", linear_x=" << command.linear_x
                          << ", angular_z=" << command.angular_z << '\n';
              }
              completion.notify_all();
            },
            options);

    // 控制回调将 IMU 偏航角转换为简单的角速度指令，并再次发布。
    auto imu_subscription =
        control_node.create_subscription<robot_middleware::ImuMsg>(
            "/imu",
            [command_publisher](const robot_middleware::ImuMsg& imu) {
              robot_middleware::ControlMsg command;
              command.timestamp_ns =
                  robot_middleware::serialization::monotonic_time_ns();
              command.linear_x = 0.25;
              command.angular_z = imu.yaw > 180.0 ? -0.1 : 0.1;
              command_publisher.publish(std::move(command));
            },
            options);

    // 以 sleep_until 维持稳定 100 Hz，减少循环耗时导致的累计漂移。
    std::cout << "Publishing IMU data at 100 Hz ...\n";
    auto next_publish = std::chrono::steady_clock::now();
    for (std::uint64_t sequence = 0; sequence < kMessageCount; ++sequence) {
      next_publish += std::chrono::milliseconds(10);
      robot_middleware::ImuMsg imu;
      imu.timestamp_ns = robot_middleware::serialization::monotonic_time_ns();
      imu.yaw = static_cast<double>(sequence % 360U);
      imu.gyro_z = 0.05;
      imu_publisher.publish(std::move(imu));
      std::this_thread::sleep_until(next_publish);
    }

    {
      std::unique_lock<std::mutex> lock(completion_mutex);
      completion.wait_for(lock, std::chrono::seconds(2), [&] {
        return commands_received == kMessageCount;
      });
    }

    // 严格取消确保回调全部退出后再关闭 Runtime。
    const auto imu_stats = imu_subscription.stats();
    const auto command_stats = command_subscription.stats();
    imu_subscription.cancel_and_wait();
    command_subscription.cancel_and_wait();
    runtime.shutdown();

    std::cout << "Completed: imu_callbacks=" << imu_stats.delivered
              << ", command_callbacks=" << command_stats.delivered
              << ", queue_drops="
              << imu_stats.dropped_oldest + command_stats.dropped_oldest
              << '\n';
    return commands_received == kMessageCount ? 0 : 2;
  } catch (const std::exception& error) {
    std::cerr << "inprocess_demo: " << error.what() << '\n';
    return 1;
  }
}
