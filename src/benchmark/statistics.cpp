#include "robot_middleware/benchmark/statistics.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>

// 性能统计实现：负责延迟百分位和消息序号质量统计。
namespace robot_middleware {
namespace benchmark {
namespace {

/// nearest-rank 百分位：返回排序后 ceil(p*n)-1 位置的样本。
std::uint64_t nearest_rank(const std::vector<std::uint64_t>& sorted,
                           double percentile) {
  if (sorted.empty()) {
    return 0;
  }
  const auto rank = static_cast<std::size_t>(
      std::ceil(percentile * static_cast<double>(sorted.size())));
  const std::size_t index = rank == 0 ? 0 : rank - 1;
  return sorted[std::min(index, sorted.size() - 1)];
}

/// 将纳秒转换为便于输出的微秒浮点数。
double to_microseconds(std::uint64_t nanoseconds) {
  return static_cast<double>(nanoseconds) / 1000.0;
}

}  // namespace

/// 记录非负延迟；负值通常表示时钟或输入异常，直接忽略。
void LatencyRecorder::record(std::chrono::nanoseconds latency) {
  const auto count = latency.count();
  if (count < 0) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  samples_ns_.push_back(static_cast<std::uint64_t>(count));
}

/// 复制样本后在锁外排序，避免长时间占用记录锁。
LatencySummary LatencyRecorder::summary() const {
  std::vector<std::uint64_t> samples;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    samples = samples_ns_;
  }

  LatencySummary result;
  result.count = samples.size();
  if (samples.empty()) {
    return result;
  }

  std::sort(samples.begin(), samples.end());
  const long double total =
      std::accumulate(samples.begin(), samples.end(), static_cast<long double>(0));
  result.mean_us = static_cast<double>(total / samples.size() / 1000.0L);
  result.min_us = to_microseconds(samples.front());
  result.p50_us = to_microseconds(nearest_rank(samples, 0.50));
  result.p95_us = to_microseconds(nearest_rank(samples, 0.95));
  result.p99_us = to_microseconds(nearest_rank(samples, 0.99));
  result.max_us = to_microseconds(samples.back());
  return result;
}

/// 清空延迟样本，便于多轮 Benchmark 复用记录器。
void LatencyRecorder::clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  samples_ns_.clear();
}

/// 记录消息序号，并分别统计重复和低于历史最高值的乱序。
void SequenceTracker::observe(std::uint64_t sequence) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto inserted = seen_.insert(sequence).second;
  if (!inserted) {
    ++duplicates_;
    return;
  }

  if (has_highest_ && sequence < highest_) {
    ++out_of_order_;
  }
  if (!has_highest_ || sequence > highest_) {
    highest_ = sequence;
    has_highest_ = true;
  }
}

/// 使用发送端给出的 expected_messages 计算尾部也可能存在的丢包。
SequenceSummary SequenceTracker::summary(
    std::uint64_t expected_messages) const {
  std::lock_guard<std::mutex> lock(mutex_);
  SequenceSummary result;
  result.expected = expected_messages;
  result.unique_received = static_cast<std::uint64_t>(seen_.size());
  result.lost = expected_messages > result.unique_received
                    ? expected_messages - result.unique_received
                    : 0;
  result.duplicates = duplicates_;
  result.out_of_order = out_of_order_;
  result.loss_rate = expected_messages == 0
                         ? 0.0
                         : static_cast<double>(result.lost) /
                               static_cast<double>(expected_messages);
  return result;
}

/// 清空序号集合及累计计数。
void SequenceTracker::clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  seen_.clear();
  has_highest_ = false;
  highest_ = 0;
  duplicates_ = 0;
  out_of_order_ = 0;
}

/// 获取进程内单调时钟纳秒值。
std::uint64_t steady_time_ns() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

}  // namespace benchmark
}  // namespace robot_middleware
