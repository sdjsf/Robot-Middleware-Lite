#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

#include "robot_middleware/benchmark/process_metrics.hpp"
#include "robot_middleware/benchmark/statistics.hpp"
#include "robot_middleware/core/runtime.hpp"
#include "robot_middleware/core/thread_safe_queue.hpp"
#include "robot_middleware/serialization/network_message.hpp"

namespace {

enum class BenchmarkPath {
  all,
  queue,
  pubsub,
};

/// 解析必须大于 0 的命令行整数参数。
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

/// 解析可选的 benchmark 路径，便于矩阵脚本避免重复运行无关负载。
BenchmarkPath parse_benchmark_path(const char *text) {
  const std::string value(text);
  if (value == "all") {
    return BenchmarkPath::all;
  }
  if (value == "queue") {
    return BenchmarkPath::queue;
  }
  if (value == "pubsub") {
    return BenchmarkPath::pubsub;
  }
  throw std::invalid_argument(
      "benchmark_path must be one of: all, queue, pubsub");
}

/// 测量单生产者、单消费者队列的端到端吞吐量。
void run_queue_benchmark(std::uint64_t message_count) {
  robot_middleware::ThreadSafeQueue<std::uint64_t> queue;
  std::uint64_t received = 0;
  robot_middleware::benchmark::ProcessMetricsSampler process_sampler;
  std::thread consumer([&] {
    while (queue.wait_pop()) {
      ++received;
    }
  });
  for (std::uint64_t value = 0; value < message_count; ++value) {
    queue.push(value);
  }
  // close 后消费者排空剩余数据并自然退出。
  queue.close();
  consumer.join();
  const auto process = process_sampler.finish();
  std::cout << std::fixed << std::setprecision(6)
            << "{\"path\":\"queue_spsc\",\"messages\":" << received
            << ",\"duration_s\":" << process.wall_time_seconds
            << ",\"wall_time_s\":" << process.wall_time_seconds << ",\"msg_s\":"
            << (process.wall_time_seconds > 0.0
                    ? received / process.wall_time_seconds
                    : 0.0)
            << ",\"user_cpu_s\":" << process.user_cpu_seconds
            << ",\"system_cpu_s\":" << process.system_cpu_seconds
            << ",\"cpu_percent\":" << process.cpu_percent
            << ",\"current_rss_bytes\":" << process.current_rss_bytes
            << ",\"peak_rss_bytes\":" << process.peak_rss_bytes << "}\n";
}

/// 测量 publish 入口到订阅回调的进程内吞吐和延迟分布。
void run_pubsub_benchmark(std::uint64_t message_count,
                          std::size_t worker_count) {
  robot_middleware::Runtime runtime(worker_count);
  const auto publisher_node = runtime.create_node("benchmark_publisher");
  const auto subscriber_node = runtime.create_node("benchmark_subscriber");
  const auto publisher =
      publisher_node.create_publisher<robot_middleware::ImuMsg>(
          "/benchmark/imu");

  robot_middleware::benchmark::LatencyRecorder latencies;
  std::mutex mutex;
  std::condition_variable completed;
  std::uint64_t received = 0;
  // 队列深度覆盖本轮突发消息总数，使本测试测量执行能力而不是丢弃策略。
  robot_middleware::SubscriptionOptions options;
  options.queue_depth = static_cast<std::size_t>(message_count);

  auto subscription =
      subscriber_node.create_subscription<robot_middleware::ImuMsg>(
          "/benchmark/imu",
          [&](const robot_middleware::ImuMsg &imu) {
            const auto now_ns =
                robot_middleware::serialization::monotonic_time_ns();
            if (now_ns >= imu.timestamp_ns) {
              latencies.record(
                  std::chrono::nanoseconds(now_ns - imu.timestamp_ns));
            }
            std::lock_guard<std::mutex> lock(mutex);
            ++received;
            if (received == message_count) {
              completed.notify_one();
            }
          },
          options);

  robot_middleware::benchmark::ProcessMetricsSampler process_sampler;
  for (std::uint64_t sequence = 0; sequence < message_count; ++sequence) {
    robot_middleware::ImuMsg message;
    message.timestamp_ns = robot_middleware::serialization::monotonic_time_ns();
    message.yaw = static_cast<double>(sequence);
    publisher.publish(std::move(message));
  }
  // 等待全部回调完成，超时被视为功能错误而不是性能结果。
  {
    std::unique_lock<std::mutex> lock(mutex);
    if (!completed.wait_for(lock, std::chrono::seconds(30),
                            [&] { return received == message_count; })) {
      throw std::runtime_error("in-process benchmark timed out");
    }
  }
  const auto process = process_sampler.finish();
  const auto latency = latencies.summary();
  const auto stats = subscription.stats();
  subscription.cancel_and_wait();
  runtime.shutdown();

  std::cout << std::fixed << std::setprecision(6)
            << "{\"path\":\"inproc_pubsub\",\"messages\":" << received
            << ",\"workers\":" << worker_count
            << ",\"duration_s\":" << process.wall_time_seconds
            << ",\"wall_time_s\":" << process.wall_time_seconds << ",\"msg_s\":"
            << (process.wall_time_seconds > 0.0
                    ? received / process.wall_time_seconds
                    : 0.0)
            << ",\"drops\":" << stats.dropped_oldest
            << ",\"latency_p50_us\":" << latency.p50_us
            << ",\"latency_p95_us\":" << latency.p95_us
            << ",\"latency_p99_us\":" << latency.p99_us
            << ",\"user_cpu_s\":" << process.user_cpu_seconds
            << ",\"system_cpu_s\":" << process.system_cpu_seconds
            << ",\"cpu_percent\":" << process.cpu_percent
            << ",\"current_rss_bytes\":" << process.current_rss_bytes
            << ",\"peak_rss_bytes\":" << process.peak_rss_bytes << "}\n";
}

} // namespace

// Benchmark 入口：默认输出全部结果，也可按第三个参数只运行一条测量路径。
int main(int argc, char **argv) {
  try {
    if (argc > 4) {
      throw std::invalid_argument("too many command-line arguments");
    }
    const auto message_count =
        argc > 1 ? parse_positive(argv[1], "message_count") : 100000U;
    const auto worker_count_value =
        argc > 2 ? parse_positive(argv[2], "worker_count") : 4U;
    const auto benchmark_path =
        argc > 3 ? parse_benchmark_path(argv[3]) : BenchmarkPath::all;
    if (benchmark_path == BenchmarkPath::all ||
        benchmark_path == BenchmarkPath::queue) {
      run_queue_benchmark(message_count);
    }
    if (benchmark_path == BenchmarkPath::all ||
        benchmark_path == BenchmarkPath::pubsub) {
      run_pubsub_benchmark(message_count,
                           static_cast<std::size_t>(worker_count_value));
    }
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "middleware_benchmark: " << error.what() << '\n';
    std::cerr << "usage: rml_benchmark [message_count] [worker_count] "
                 "[all|queue|pubsub]\n";
    return 1;
  }
}
