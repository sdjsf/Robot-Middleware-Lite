#pragma once

#include <cstddef>
#include <future>
#include <utility>

#include "robot_middleware/executor/thread_pool.hpp"

namespace robot_middleware {

/// 中间件对外使用的任务调度边界。
/// Executor 负责表达“执行任务”的语义，底层 ThreadPool 仍可作为独立组件复用。
class Executor final {
 public:
  using Task = ThreadPool::Task;
  using Stats = ThreadPool::Stats;

  /// 使用固定数量的 worker 创建执行器。
  explicit Executor(std::size_t worker_count = ThreadPool::default_worker_count())
      : pool_(worker_count) {}

  /// 异步执行一个无返回值任务。
  bool execute(Task task) { return pool_.post(std::move(task)); }

  /// 提交带返回值任务并返回 future。
  template <typename Function, typename... Args>
  auto submit(Function&& function, Args&&... args) {
    return pool_.submit(std::forward<Function>(function),
                        std::forward<Args>(args)...);
  }

  /// 以 drain 语义关闭底层线程池。
  void shutdown() noexcept { pool_.shutdown(); }
  /// 返回任务提交、完成和失败统计。
  Stats stats() const noexcept { return pool_.stats(); }
  /// 查询执行器是否仍接受任务。
  bool is_accepting() const noexcept { return pool_.is_accepting(); }
  /// 返回 worker 数量。
  std::size_t worker_count() const noexcept { return pool_.worker_count(); }

 private:
  ThreadPool pool_;
};

}  // namespace robot_middleware
