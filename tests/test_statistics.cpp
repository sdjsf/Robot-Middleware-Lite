#include "test_harness.hpp"

#include <chrono>
#include <cmath>
#include <cstdint>
#include <thread>

#include "robot_middleware/benchmark/process_metrics.hpp"
#include "robot_middleware/benchmark/statistics.hpp"

namespace {

using robot_middleware::benchmark::LatencyRecorder;
using robot_middleware::benchmark::ProcessMetricsSampler;
using robot_middleware::benchmark::SequenceTracker;

/// 验证 1..100 微秒样本的 nearest-rank 百分位和均值。
void nearest_rank_percentiles() {
  LatencyRecorder recorder;
  for (std::int64_t microseconds = 1; microseconds <= 100; ++microseconds) {
    recorder.record(std::chrono::microseconds(microseconds));
  }

  const auto summary = recorder.summary();
  RML_CHECK_EQ(summary.count, std::size_t{100});
  RML_CHECK_EQ(summary.min_us, 1.0);
  RML_CHECK_EQ(summary.mean_us, 50.5);
  RML_CHECK_EQ(summary.p50_us, 50.0);
  RML_CHECK_EQ(summary.p95_us, 95.0);
  RML_CHECK_EQ(summary.p99_us, 99.0);
  RML_CHECK_EQ(summary.max_us, 100.0);
}

/// 验证丢包、重复和乱序被分别统计。
void sequence_loss_duplicate_and_reordering() {
  SequenceTracker tracker;
  for (const std::uint64_t sequence :
       {UINT64_C(100), UINT64_C(101), UINT64_C(103), UINT64_C(103),
        UINT64_C(102), UINT64_C(106)}) {
    tracker.observe(sequence);
  }

  const auto summary = tracker.summary(7);
  RML_CHECK_EQ(summary.expected, UINT64_C(7));
  RML_CHECK_EQ(summary.unique_received, UINT64_C(5));
  RML_CHECK_EQ(summary.lost, UINT64_C(2));
  RML_CHECK_EQ(summary.duplicates, UINT64_C(1));
  RML_CHECK_EQ(summary.out_of_order, UINT64_C(1));
  RML_CHECK(std::abs(summary.loss_rate - (2.0 / 7.0)) < 1e-12);
}

/// 验证 Linux 进程 CPU/RSS 采样字段有效且单位关系自洽。
void process_metrics_are_sampled() {
  ProcessMetricsSampler sampler;
  std::this_thread::sleep_for(std::chrono::milliseconds(2));
  const auto metrics = sampler.finish();

  RML_CHECK(metrics.wall_time_seconds > 0.0);
  RML_CHECK(metrics.user_cpu_seconds >= 0.0);
  RML_CHECK(metrics.system_cpu_seconds >= 0.0);
  RML_CHECK(metrics.cpu_percent >= 0.0);
  RML_CHECK(metrics.current_rss_bytes > UINT64_C(0));
  RML_CHECK(metrics.peak_rss_bytes >= metrics.current_rss_bytes);
}

}  // namespace

// 性能统计测试入口。
int main(int argc, char** argv) {
  return rml_test::run(
      {
          {"nearest_rank", nearest_rank_percentiles},
          {"sequence_tracking", sequence_loss_duplicate_and_reordering},
          {"process_metrics", process_metrics_are_sampled},
      },
      argc, argv);
}
