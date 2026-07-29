#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace robot_middleware {
namespace benchmark {

/// 一组延迟样本的统计摘要，单位统一为微秒。
struct LatencySummary {
  std::size_t count{0};
  double mean_us{0.0};
  double min_us{0.0};
  double p50_us{0.0};
  double p95_us{0.0};
  double p99_us{0.0};
  double max_us{0.0};
};

/// 线程安全的延迟样本记录器，百分位采用 nearest-rank 算法。
class LatencyRecorder {
 public:
  /// 记录一条非负延迟样本。
  void record(std::chrono::nanoseconds latency);
  /// 对当前样本生成统计摘要。
  LatencySummary summary() const;
  /// 清空全部样本。
  void clear();

 private:
  mutable std::mutex mutex_;
  std::vector<std::uint64_t> samples_ns_;
};

/// 序号接收情况摘要。
struct SequenceSummary {
  std::uint64_t expected{0};
  std::uint64_t unique_received{0};
  std::uint64_t lost{0};
  std::uint64_t duplicates{0};
  std::uint64_t out_of_order{0};
  double loss_rate{0.0};
};

/// 根据消息序号统计唯一接收数、重复和乱序。
class SequenceTracker {
 public:
  /// 观察一条消息序号。
  void observe(std::uint64_t sequence);
  /// 根据发送端已知总数计算丢包数量和比例。
  SequenceSummary summary(std::uint64_t expected_messages) const;
  /// 清空全部序号状态。
  void clear();

 private:
  mutable std::mutex mutex_;
  std::unordered_set<std::uint64_t> seen_;
  bool has_highest_{false};
  std::uint64_t highest_{0};
  std::uint64_t duplicates_{0};
  std::uint64_t out_of_order_{0};
};

/// 返回 steady_clock 纳秒时间戳，供 Benchmark 统一计时。
std::uint64_t steady_time_ns() noexcept;

}  // namespace benchmark
}  // namespace robot_middleware
