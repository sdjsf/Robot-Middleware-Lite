#pragma once

#include <cstddef>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace robot_middleware {

/// 固定工作线程数量的通用任务线程池。
///
/// 线程池采用 drain 关闭语义：停止接收新任务后，执行完所有已接受任务再退出。
/// worker 边界会捕获任务异常并计入统计，避免异常导致进程终止。
class ThreadPool {
 public:
  /// @brief 以后直接写 Task，等价于写完整的 std::function<void()>
  using Task = std::function<void()>;

  /// 线程池累计运行统计。
  struct Stats {
    std::uint64_t submitted{0};
    std::uint64_t completed{0};
    std::uint64_t failed{0};
  };

  /// 创建指定数量的 worker；worker_count 必须大于 0。
  explicit ThreadPool(std::size_t worker_count = default_worker_count());
  ~ThreadPool() noexcept;


  /// @brief 禁止拷贝构造和拷贝赋值。
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  ThreadPool(ThreadPool&&) = delete;
  ThreadPool& operator=(ThreadPool&&) = delete;

  /// 提交无返回值任务；关闭已开始或任务为空时返回 false。
  bool post(Task task);

  /// 提交可带参数和返回值的任务，通过 future 观察结果或异常。
  template <typename Function, typename... Args>
  auto submit(Function&& function, Args&&... args)
  /// @brief 提交可带参数和返回值的任务，通过 future 观察结果或异常。
      -> std::future<std::invoke_result_t<std::decay_t<Function>,
                                          std::decay_t<Args>...>> {
    using Result = std::invoke_result_t<std::decay_t<Function>,
                                        std::decay_t<Args>...>;
    using FunctionType = std::decay_t<Function>;
    using ArgsTuple = std::tuple<std::decay_t<Args>...>;

    struct Invocation {
      Invocation(Function&& fn, Args&&... values)
          : function(std::forward<Function>(fn)),
            args(std::forward<Args>(values)...) {}

      FunctionType function;
      ArgsTuple args;
    };

    auto invocation = std::make_shared<Invocation>(
        std::forward<Function>(function), std::forward<Args>(args)...);
    auto promise = std::make_shared<std::promise<Result>>();
    std::future<Result> future = promise->get_future();

    Task task = [invocation, promise]() mutable {
      try {
        auto call = [&]() -> Result {
          return std::apply(
              [&](auto&... unpacked) -> Result {
                return std::invoke(std::move(invocation->function),
                                   std::move(unpacked)...);
              },
              invocation->args);
        };

        if constexpr (std::is_void_v<Result>) {
          call();
          promise->set_value();
        } else {
          promise->set_value(call());
        }
      } catch (...) {
        const std::exception_ptr error = std::current_exception();
        try {
          promise->set_exception(error);
        } catch (...) {
          // promise 自身异常不覆盖原始任务异常，worker 边界仍会记录失败。
        }
        std::rethrow_exception(error);
      }
    };

    if (!post(std::move(task))) {
      throw std::runtime_error("ThreadPool is not accepting tasks");
    }
    return future;
  }

  /// 停止接收任务、排空任务队列并等待 worker 结束。
  /// 重复或并发调用是安全的；worker 内调用时只分离自身线程句柄，State 由 shared_ptr 保活。
  void shutdown() noexcept;

  /// 获取线程安全的累计统计快照。
  Stats stats() const noexcept;
  /// 查询线程池是否仍接受新任务。
  bool is_accepting() const noexcept;
  /// 返回固定 worker 数量。
  std::size_t worker_count() const noexcept { return worker_count_; }

  /// 返回默认 worker 数量；无法读取硬件并发数时使用 1。
  static std::size_t default_worker_count() noexcept;

 private:
  struct State;

  std::shared_ptr<State> state_;
  const std::size_t worker_count_;
  mutable std::mutex workers_mutex_;
  std::condition_variable shutdown_complete_;
  bool shutdown_in_progress_{false};
  bool shutdown_finished_{false};
  std::vector<std::thread> workers_;
};

}  // namespace robot_middleware
