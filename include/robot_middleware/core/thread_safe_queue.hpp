#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>
#include <utility>

namespace robot_middleware {

/// 可关闭的多生产者、多消费者 FIFO 队列。
///
/// 容量为 0 表示不限制长度；有界队列满时 push/emplace 会阻塞。
/// close() 会同时唤醒阻塞的生产者和消费者，关闭前已入队的数据仍可继续排空。
template <typename T>
class ThreadSafeQueue {
public:
  /// 创建队列。capacity=0 表示无界队列。
  explicit ThreadSafeQueue(std::size_t capacity = 0) noexcept
      : capacity_(capacity) {}
  /// 禁止拷贝构造和拷贝赋值。
  ThreadSafeQueue(const ThreadSafeQueue &) = delete;
  ThreadSafeQueue &operator=(const ThreadSafeQueue &) = delete;

  /// 阻塞写入一个元素；队列关闭时返回 false。
  bool push(const T &value) { return push_impl(value); }
  bool push(T &&value) { return push_impl(std::move(value)); }

  /// 在队列中直接构造元素；队列满时等待，关闭时返回 false。
  template <typename... Args> bool emplace(Args &&...args) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_full_.wait(lock, [this] { return closed_ || !full_locked(); });
    if (closed_) {
      return false;
    }
    queue_.emplace_back(std::forward<Args>(args)...);
    lock.unlock();
    not_empty_.notify_one();
    return true;
  }

  /// 非阻塞写入；队列已满或已关闭时返回 false。
  bool try_push(const T &value) { return try_push_impl(value); }
  bool try_push(T &&value) { return try_push_impl(std::move(value)); }

  /// 非阻塞地在队列中构造元素。
  template <typename... Args> bool try_emplace(Args &&...args) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (closed_ || full_locked()) {
      return false;
    }
    queue_.emplace_back(std::forward<Args>(args)...);
    lock.unlock();
    not_empty_.notify_one();
    return true;
  }

  /// 尝试立即取出队首元素；队列为空时返回 std::nullopt。
  std::optional<T> try_pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (queue_.empty()) {
      return std::nullopt;
    }
    std::optional<T> result(std::in_place, std::move(queue_.front()));
    queue_.pop_front();
    lock.unlock();
    not_full_.notify_one();
    return result;
  }

  /// 阻塞等待队首元素；队列关闭且已排空时返回 std::nullopt。
  std::optional<T> wait_pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [this] { return closed_ || !queue_.empty(); });
    if (queue_.empty()) {
      return std::nullopt;
    }
    std::optional<T> result(std::in_place, std::move(queue_.front()));
    queue_.pop_front();
    lock.unlock();
    not_full_.notify_one();
    return result;
  }

  /// 在指定时间内等待元素；超时或关闭并排空时返回 std::nullopt。
  template <typename Rep, typename Period>
  std::optional<T>
  wait_pop_for(const std::chrono::duration<Rep, Period> &timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!not_empty_.wait_for(lock, timeout,
                             [this] { return closed_ || !queue_.empty(); })) {
      return std::nullopt;
    }
    if (queue_.empty()) {
      return std::nullopt;
    }
    std::optional<T> result(std::in_place, std::move(queue_.front()));
    queue_.pop_front();
    lock.unlock();
    not_full_.notify_one();
    return result;
  }

  /// 关闭队列并唤醒全部等待线程；只有首次关闭返回 true。
  bool close() noexcept {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (closed_) {
        return false;
      }
      closed_ = true;
    }
    not_empty_.notify_all();
    not_full_.notify_all();
    return true;
  }

  /// 查询队列是否已关闭。
  bool is_closed() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return closed_;
  }

  /// 查询队列当前是否为空。
  bool empty() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
  }

  /// 返回当前缓存的元素数量。
  std::size_t size() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }

  /// 返回配置容量；0 表示无界。
  std::size_t capacity() const noexcept { return capacity_; }

private:
  /// push 的阻塞实现，统一处理左值和右值。
  template <typename U> bool push_impl(U &&value) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_full_.wait(lock, [this] { return closed_ || !full_locked(); });
    if (closed_) {
      return false;
    }
    queue_.emplace_back(std::forward<U>(value));
    lock.unlock();
    not_empty_.notify_one();
    return true;
  }

  /// try_push 的非阻塞实现，统一处理左值和右值。
  template <typename U> bool try_push_impl(U &&value) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (closed_ || full_locked()) {
      return false;
    }
    queue_.emplace_back(std::forward<U>(value));
    lock.unlock();
    not_empty_.notify_one();
    return true;
  }

  /// 调用者持有 mutex_ 时判断有界队列是否已满。
  bool full_locked() const noexcept {
    return capacity_ != 0 && queue_.size() >= capacity_;
  }

  const std::size_t capacity_;
  mutable std::mutex mutex_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  std::deque<T> queue_;
  bool closed_{false};
};

} // namespace robot_middleware
