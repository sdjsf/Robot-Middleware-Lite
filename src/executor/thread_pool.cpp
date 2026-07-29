#include "robot_middleware/executor/thread_pool.hpp"

#include "robot_middleware/core/thread_safe_queue.hpp"

#include <atomic>
#include <stdexcept>
#include <utility>

// 固定线程池实现：使用可关闭队列协调任务提交、排空和安全退出。
namespace robot_middleware {

namespace {
// 标识当前线程正在执行哪个线程池的任务，用于识别 worker 内 shutdown，避免自等待。
thread_local const void* active_pool_state = nullptr;
}  // namespace

/// worker 共享状态独立于 ThreadPool 对象本身，保证 worker 内析构时不会访问悬空 this。
struct ThreadPool::State {
  ThreadSafeQueue<Task> tasks;
  std::atomic<bool> accepting{true};
  std::atomic<std::uint64_t> submitted{0};
  std::atomic<std::uint64_t> completed{0};
  std::atomic<std::uint64_t> failed{0};
};

/// 根据硬件并发数选择默认 worker 数量。
std::size_t ThreadPool::default_worker_count() noexcept {
  const unsigned count = std::thread::hardware_concurrency();
  return count == 0U ? std::size_t{1} : static_cast<std::size_t>(count);
}

/// 创建 worker；若中途创建线程失败，则关闭队列并回收已创建线程。
ThreadPool::ThreadPool(std::size_t worker_count)
    : state_(std::make_shared<State>()), worker_count_(worker_count) {
  if (worker_count_ == 0) {
    throw std::invalid_argument("ThreadPool worker_count must be greater than zero");
  }

  workers_.reserve(worker_count_);
  try {
    for (std::size_t index = 0; index < worker_count_; ++index) {
      const std::shared_ptr<State> state = state_;
      workers_.emplace_back([state] {
        active_pool_state = state.get();
        while (auto task = state->tasks.wait_pop()) {
          // 用户任务异常在 worker 边界被隔离，不允许逃逸并触发 std::terminate。
          try {
            (*task)();
          } catch (...) {
            state->failed.fetch_add(1, std::memory_order_relaxed);
          }
          state->completed.fetch_add(1, std::memory_order_relaxed);
        }
        active_pool_state = nullptr;
      });
    }
  } catch (...) {
    state_->accepting.store(false, std::memory_order_release);
    state_->tasks.close();
    for (std::thread& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    throw;
  }
}

/// 析构统一走幂等 shutdown。
ThreadPool::~ThreadPool() noexcept { shutdown(); }

/// 非阻塞提交任务，并维护 submitted 计数与关闭竞态的一致性。
bool ThreadPool::post(Task task) {
  const std::shared_ptr<State> state = state_;
  if (!task || !state->accepting.load(std::memory_order_acquire)) {
    return false;
  }

  // 任务对 worker 可见前先增加 submitted，保证稳定快照中 completed 不会超过 submitted。
  // 若 close 赢得竞态或入队分配失败，则回滚计数。
  state->submitted.fetch_add(1, std::memory_order_relaxed);
  try {
    if (state->tasks.try_push(std::move(task))) {
      return true;
    }
  } catch (...) {
    state->submitted.fetch_sub(1, std::memory_order_relaxed);
    throw;
  }
  state->submitted.fetch_sub(1, std::memory_order_relaxed);
  return false;
}

/// 关闭队列并由唯一调用者接管线程句柄完成 join。
void ThreadPool::shutdown() noexcept {
  const std::shared_ptr<State> state = state_;
  state->accepting.store(false, std::memory_order_release);
  state->tasks.close();

  const bool caller_is_worker = active_pool_state == state.get();

  // 只有一个调用者接管线程句柄；其他外部调用者等待其完成。
  // worker 不能在此等待，因为持有句柄的调用者可能正在 join 这个 worker。
  std::vector<std::thread> workers;
  {
    std::unique_lock<std::mutex> lock(workers_mutex_);
    if (shutdown_finished_) {
      return;
    }
    if (shutdown_in_progress_) {
      if (caller_is_worker) {
        return;
      }
      shutdown_complete_.wait(lock, [this] { return shutdown_finished_; });
      return;
    }
    shutdown_in_progress_ = true;
    workers.swap(workers_);
  }

  const std::thread::id caller = std::this_thread::get_id();
  for (std::thread& worker : workers) {
    if (!worker.joinable()) {
      continue;
    }
    if (worker.get_id() == caller) {
      // 当前 worker 无法 join 自身，只分离线程句柄；共享 State 会持续存活到循环退出。
      worker.detach();
    } else {
      worker.join();
    }
  }

  {
    std::lock_guard<std::mutex> lock(workers_mutex_);
    shutdown_finished_ = true;
    shutdown_in_progress_ = false;
  }
  shutdown_complete_.notify_all();
}

/// 从原子计数器生成无锁统计快照。
ThreadPool::Stats ThreadPool::stats() const noexcept {
  const std::shared_ptr<State> state = state_;
  Stats result;
  result.submitted = state->submitted.load(std::memory_order_relaxed);
  result.completed = state->completed.load(std::memory_order_relaxed);
  result.failed = state->failed.load(std::memory_order_relaxed);
  return result;
}

/// 查询提交入口是否仍开放。
bool ThreadPool::is_accepting() const noexcept {
  return state_->accepting.load(std::memory_order_acquire);
}

}  // namespace robot_middleware
