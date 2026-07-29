#pragma once

#include <chrono>
#include <cstdint>
#include <fstream>
#include <stdexcept>

#include <sys/resource.h>
#include <unistd.h>

namespace robot_middleware::benchmark {

/// 进程资源采样快照，用于计算一段工作负载内的 CPU 增量。
struct ProcessMetricsSnapshot {
  std::chrono::steady_clock::time_point wall_time;
  double user_cpu_seconds{0.0};
  double system_cpu_seconds{0.0};
  std::uint64_t current_rss_bytes{0};
  std::uint64_t peak_rss_bytes{0};
};

/// 一段工作负载的进程资源指标。
struct ProcessMetrics {
  double wall_time_seconds{0.0};
  double user_cpu_seconds{0.0};
  double system_cpu_seconds{0.0};
  double cpu_percent{0.0};
  /// 结束采样点的 RSS。
  std::uint64_t current_rss_bytes{0};
  /// 进程启动至结束采样点的 RSS 高水位，不是区间内存增量。
  std::uint64_t peak_rss_bytes{0};
};

namespace detail {

/// 将 getrusage 的 timeval 转换为秒。
inline double timeval_to_seconds(const timeval &value) noexcept {
  return static_cast<double>(value.tv_sec) +
         static_cast<double>(value.tv_usec) / 1'000'000.0;
}

/// 读取 Linux /proc/self/statm 中的当前常驻内存。
inline std::uint64_t read_current_rss_bytes() {
  std::ifstream statm("/proc/self/statm");
  std::uint64_t virtual_pages = 0;
  std::uint64_t resident_pages = 0;
  if (!(statm >> virtual_pages >> resident_pages)) {
    throw std::runtime_error("failed to read /proc/self/statm");
  }

  const long page_size = ::sysconf(_SC_PAGESIZE);
  if (page_size <= 0) {
    throw std::runtime_error("failed to query Linux page size");
  }
  return resident_pages * static_cast<std::uint64_t>(page_size);
}

} // namespace detail

/// 获取当前进程的墙钟、CPU 与 RSS 快照。
inline ProcessMetricsSnapshot sample_process_metrics() {
  const auto current_rss_bytes = detail::read_current_rss_bytes();
  rusage usage{};
  if (::getrusage(RUSAGE_SELF, &usage) != 0) {
    throw std::runtime_error("getrusage(RUSAGE_SELF) failed");
  }

  ProcessMetricsSnapshot snapshot;
  snapshot.wall_time = std::chrono::steady_clock::now();
  snapshot.user_cpu_seconds = detail::timeval_to_seconds(usage.ru_utime);
  snapshot.system_cpu_seconds = detail::timeval_to_seconds(usage.ru_stime);
  snapshot.current_rss_bytes = current_rss_bytes;
  // Linux 将 ru_maxrss 定义为 KiB；转换为字节后与 current RSS 保持同一单位。
  const auto kernel_peak_rss_bytes =
      static_cast<std::uint64_t>(usage.ru_maxrss) * 1024U;
  // 内核高水位更新和 /proc 快照可能存在极短时序差，峰值至少不能小于当前值。
  snapshot.peak_rss_bytes = kernel_peak_rss_bytes >= current_rss_bytes
                                ? kernel_peak_rss_bytes
                                : current_rss_bytes;
  return snapshot;
}

/// 根据起止快照计算区间资源消耗；进程多线程并行时 CPU 百分比可超过 100%。
inline ProcessMetrics
calculate_process_metrics(const ProcessMetricsSnapshot &started,
                          const ProcessMetricsSnapshot &finished) noexcept {
  ProcessMetrics metrics;
  metrics.wall_time_seconds =
      std::chrono::duration<double>(finished.wall_time - started.wall_time)
          .count();
  metrics.user_cpu_seconds =
      finished.user_cpu_seconds >= started.user_cpu_seconds
          ? finished.user_cpu_seconds - started.user_cpu_seconds
          : 0.0;
  metrics.system_cpu_seconds =
      finished.system_cpu_seconds >= started.system_cpu_seconds
          ? finished.system_cpu_seconds - started.system_cpu_seconds
          : 0.0;
  const double total_cpu_seconds =
      metrics.user_cpu_seconds + metrics.system_cpu_seconds;
  metrics.cpu_percent =
      metrics.wall_time_seconds > 0.0
          ? total_cpu_seconds / metrics.wall_time_seconds * 100.0
          : 0.0;
  metrics.current_rss_bytes = finished.current_rss_bytes;
  metrics.peak_rss_bytes = finished.peak_rss_bytes;
  return metrics;
}

/// RAII 风格的区间采样器，构造时记录起点，finish 时返回增量指标。
class ProcessMetricsSampler {
public:
  ProcessMetricsSampler() : started_(sample_process_metrics()) {}

  ProcessMetrics finish() const {
    return calculate_process_metrics(started_, sample_process_metrics());
  }

private:
  ProcessMetricsSnapshot started_;
};

} // namespace robot_middleware::benchmark
