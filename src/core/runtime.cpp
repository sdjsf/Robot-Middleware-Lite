#include "robot_middleware/core/runtime.hpp"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

// 进程内发布订阅运行时实现。
// 核心策略是“每订阅独立有界邮箱 + 单个 scheduled drain”，从而兼顾顺序与并行性。
namespace robot_middleware {
namespace detail {

/// SubscriptionControlBlock 的并发状态，所有可变字段由 mutex 保护。
struct SubscriptionControlBlock::Impl {
  explicit Impl(SubscriptionOptions subscription_options,
                ErasedCallback subscription_callback)
      : options(subscription_options), callback(std::move(subscription_callback)) {}

  mutable std::mutex mutex;
  std::condition_variable idle;
  SubscriptionOptions options;
  ErasedCallback callback;
  // pending 仅保存尚未进入回调的共享消息；同一时刻最多一个 drain 消费它。
  std::deque<ErasedMessage> pending;
  bool active{true};
  bool scheduled{false};
  std::size_t in_flight{0};
  std::thread::id callback_thread;
  std::uint64_t enqueued{0};
  std::uint64_t delivered{0};
  std::uint64_t dropped_oldest{0};
  std::uint64_t rejected_newest{0};
  std::uint64_t executor_rejected{0};
  std::uint64_t callback_errors{0};
};

/// 校验订阅深度和回调后创建控制块。
SubscriptionControlBlock::SubscriptionControlBlock(
    SubscriptionOptions options, ErasedCallback callback)
    : impl_(std::make_unique<Impl>(options, std::move(callback))) {
  if (impl_->options.queue_depth == 0) {
    throw MiddlewareError("subscription queue_depth must be greater than zero");
  }
  if (!impl_->callback) {
    throw MiddlewareError("subscription callback must not be empty");
  }
}

SubscriptionControlBlock::~SubscriptionControlBlock() = default;

/// 投递消息并处理邮箱溢出；只有从“未调度”切换为“已调度”时才提交 drain 任务。
DeliveryOutcome SubscriptionControlBlock::enqueue(
    ErasedMessage message, const std::shared_ptr<Executor>& executor,
    bool is_remote) {
  DeliveryOutcome outcome;
  bool must_schedule = false;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (!impl_->active) {
      return outcome;
    }
    if (is_remote && !impl_->options.receive_remote) {
      return outcome;
    }
    if (impl_->pending.size() >= impl_->options.queue_depth) {
      // RejectNewest 保留队列中的旧数据，适合要求处理历史顺序的任务。
      if (impl_->options.overflow_policy == OverflowPolicy::RejectNewest) {
        ++impl_->rejected_newest;
        outcome.rejected_newest = true;
        return outcome;
      }
      // DropOldest 保留最新状态，适合 IMU、位姿等高频状态流。
      impl_->pending.pop_front();
      ++impl_->dropped_oldest;
      outcome.dropped_oldest = true;
    }
    impl_->pending.push_back(std::move(message));
    ++impl_->enqueued;
    outcome.enqueued = true;
    if (!impl_->scheduled) {
      impl_->scheduled = true;
      must_schedule = true;
    }
  }

  if (must_schedule) {
    auto self = shared_from_this();
    bool posted = false;
    try {
      posted = executor && executor->execute([self] { self->drain(); });
    } catch (...) {
      // 提交期间若发生分配异常，恢复 scheduled 状态并清空无法执行的消息。
      std::lock_guard<std::mutex> lock(impl_->mutex);
      impl_->executor_rejected +=
          static_cast<std::uint64_t>(impl_->pending.size());
      impl_->pending.clear();
      impl_->scheduled = false;
      impl_->idle.notify_all();
      throw;
    }
    if (!posted) {
      // Executor 已关闭时，所有待处理消息都无法再启动回调。
      std::lock_guard<std::mutex> lock(impl_->mutex);
      const auto rejected = static_cast<std::uint64_t>(impl_->pending.size());
      impl_->executor_rejected += rejected;
      impl_->pending.clear();
      impl_->scheduled = false;
      impl_->idle.notify_all();
      outcome.enqueued = false;
      outcome.executor_rejected = true;
    }
  }
  return outcome;
}

/// 标记订阅失活并清空尚未开始的消息；正在执行的回调不被强行中断。
void SubscriptionControlBlock::cancel() noexcept {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->active = false;
  impl_->pending.clear();
  impl_->idle.notify_all();
}

/// 等待已开始回调结束；检测回调线程自等待并给出明确错误。
void SubscriptionControlBlock::cancel_and_wait() {
  cancel();
  std::unique_lock<std::mutex> lock(impl_->mutex);
  if (impl_->in_flight != 0 &&
      impl_->callback_thread == std::this_thread::get_id()) {
    throw MiddlewareError(
        "cancel_and_wait cannot be called from the subscription callback");
  }
  impl_->idle.wait(lock, [this] { return impl_->in_flight == 0; });
}

/// 在锁内复制一致的订阅状态和累计计数。
SubscriptionStats SubscriptionControlBlock::stats() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  SubscriptionStats result;
  result.enqueued = impl_->enqueued;
  result.delivered = impl_->delivered;
  result.dropped_oldest = impl_->dropped_oldest;
  result.rejected_newest = impl_->rejected_newest;
  result.executor_rejected = impl_->executor_rejected;
  result.callback_errors = impl_->callback_errors;
  result.pending = impl_->pending.size();
  result.in_flight = impl_->in_flight;
  result.active = impl_->active;
  return result;
}

/// 串行排空单个订阅邮箱，用户回调始终在锁外运行。
void SubscriptionControlBlock::drain() noexcept {
  while (true) {
    ErasedMessage message;
    {
      std::lock_guard<std::mutex> lock(impl_->mutex);
      if (!impl_->active) {
        impl_->pending.clear();
        impl_->scheduled = false;
        impl_->idle.notify_all();
        return;
      }
      if (impl_->pending.empty()) {
        impl_->scheduled = false;
        impl_->idle.notify_all();
        return;
      }
      message = std::move(impl_->pending.front());
      impl_->pending.pop_front();
      ++impl_->in_flight;
      impl_->callback_thread = std::this_thread::get_id();
    }

    bool callback_failed = false;
    try {
      impl_->callback(message);
    } catch (...) {
      // 回调异常只计数，不允许破坏 drain 循环或终止 worker。
      callback_failed = true;
    }

    {
      std::lock_guard<std::mutex> lock(impl_->mutex);
      --impl_->in_flight;
      ++impl_->delivered;
      if (callback_failed) {
        ++impl_->callback_errors;
      }
      impl_->callback_thread = std::thread::id{};
      impl_->idle.notify_all();
    }
  }
}

/// TopicBus 的注册表实现；全局 mutex 只保护元数据，不在其下执行用户回调。
struct TopicBus::Impl {
  /// 一个 Topic 首次注册后固定的类型契约和订阅者集合。
  struct TopicRecord {
    TopicRecord(std::uint32_t topic_type_id, std::uint64_t topic_schema_hash,
                std::type_index topic_local_type)
        : type_id(topic_type_id),
          schema_hash(topic_schema_hash),
          local_type(topic_local_type),
          dispatch_mutex(std::make_shared<std::mutex>()) {}

    std::uint32_t type_id;
    std::uint64_t schema_hash;
    std::type_index local_type;
    std::shared_ptr<std::mutex> dispatch_mutex;
    std::unordered_map<std::uint64_t,
                       std::weak_ptr<SubscriptionControlBlock>>
        subscriptions;
  };

  explicit Impl(std::shared_ptr<Executor> topic_executor)
      : executor(std::move(topic_executor)) {}

  /// 检查 Topic 名称最基本的合法性。
  static void validate_topic(const std::string& topic) {
    if (topic.empty()) {
      throw MiddlewareError("topic name must not be empty");
    }
  }

  /// 同名 Topic 的协议 ID、schema 与本地类型必须全部一致。
  static void validate_type(const std::string& topic, const TopicRecord& record,
                            std::uint32_t type_id,
                            std::uint64_t schema_hash,
                            std::type_index local_type) {
    if (record.type_id != type_id || record.schema_hash != schema_hash ||
        record.local_type != local_type) {
      throw MiddlewareError("message type conflict on topic '" + topic + "'");
    }
  }

  mutable std::mutex mutex;
  bool accepting{true};
  std::uint64_t next_subscription_id{1};
  std::shared_ptr<Executor> executor;
  std::unordered_map<std::string, TopicRecord> topics;
};

/// 创建绑定到指定 Executor 的进程内总线。
TopicBus::TopicBus(std::shared_ptr<Executor> executor)
    : impl_(std::make_unique<Impl>(std::move(executor))) {
  if (!impl_->executor) {
    throw MiddlewareError("topic bus requires an executor");
  }
}

TopicBus::~TopicBus() { stop(); }

/// 注册发布端；首次出现的 Topic 建立类型记录，后续注册执行一致性检查。
void TopicBus::advertise(const std::string& topic, std::uint32_t type_id,
                         std::uint64_t schema_hash,
                         std::type_index local_type) {
  Impl::validate_topic(topic);
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (!impl_->accepting) {
    throw MiddlewareError("middleware runtime is stopped");
  }
  const auto found = impl_->topics.find(topic);
  if (found == impl_->topics.end()) {
    impl_->topics.emplace(topic,
                          Impl::TopicRecord(type_id, schema_hash, local_type));
    return;
  }
  Impl::validate_type(topic, found->second, type_id, schema_hash, local_type);
}

/// 创建订阅控制块、分配唯一 ID 并加入 Topic 注册表。
SubscriptionRegistration TopicBus::subscribe(
    const std::string& topic, std::uint32_t type_id,
    std::uint64_t schema_hash, std::type_index local_type,
    SubscriptionOptions options,
    SubscriptionControlBlock::ErasedCallback callback) {
  Impl::validate_topic(topic);
  auto state = std::make_shared<SubscriptionControlBlock>(
      options, std::move(callback));
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (!impl_->accepting) {
    throw MiddlewareError("middleware runtime is stopped");
  }
  auto found = impl_->topics.find(topic);
  if (found == impl_->topics.end()) {
    found = impl_->topics
                .emplace(topic,
                         Impl::TopicRecord(type_id, schema_hash, local_type))
                .first;
  } else {
    Impl::validate_type(topic, found->second, type_id, schema_hash, local_type);
  }
  const auto id = impl_->next_subscription_id++;
  found->second.subscriptions.emplace(id, state);
  return SubscriptionRegistration{id, std::move(state)};
}

/// 获取订阅快照后在注册表锁外进行 fan-out，避免发布路径和用户回调形成锁环。
PublishResult TopicBus::publish(
    const std::string& topic, std::uint32_t type_id,
    std::uint64_t schema_hash, std::type_index local_type,
    SubscriptionControlBlock::ErasedMessage message, bool is_remote) {
  std::vector<std::shared_ptr<SubscriptionControlBlock>> subscriptions;
  std::shared_ptr<std::mutex> dispatch_mutex;
  std::shared_ptr<Executor> executor;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (!impl_->accepting) {
      throw MiddlewareError("middleware runtime is stopped");
    }
    const auto found = impl_->topics.find(topic);
    if (found == impl_->topics.end()) {
      throw MiddlewareError("publisher topic is not registered");
    }
    Impl::validate_type(topic, found->second, type_id, schema_hash, local_type);
    dispatch_mutex = found->second.dispatch_mutex;
    executor = impl_->executor;
    for (auto iterator = found->second.subscriptions.begin();
         iterator != found->second.subscriptions.end();) {
      if (auto state = iterator->second.lock()) {
        subscriptions.push_back(std::move(state));
        ++iterator;
      } else {
        // 发布时顺便清理已经析构的弱引用。
        iterator = found->second.subscriptions.erase(iterator);
      }
    }
  }

  PublishResult result;
  result.matched_subscribers = subscriptions.size();
  // dispatch_mutex 将同一 Topic 的一次 fan-out 串行化，维持单发布流的入队顺序。
  std::lock_guard<std::mutex> dispatch_lock(*dispatch_mutex);
  for (const auto& subscription : subscriptions) {
    const auto outcome = subscription->enqueue(message, executor, is_remote);
    result.enqueued += outcome.enqueued ? 1U : 0U;
    result.dropped_oldest += outcome.dropped_oldest ? 1U : 0U;
    result.rejected_newest += outcome.rejected_newest ? 1U : 0U;
    result.executor_rejected += outcome.executor_rejected ? 1U : 0U;
  }
  return result;
}

/// 删除订阅 ID；控制块取消由 Subscription 自身负责。
void TopicBus::unsubscribe(const std::string& topic, std::uint64_t id) noexcept {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const auto found = impl_->topics.find(topic);
  if (found != impl_->topics.end()) {
    found->second.subscriptions.erase(id);
  }
}

/// 停止总线并在注册表锁外取消全部存活订阅。
void TopicBus::stop() noexcept {
  std::vector<std::shared_ptr<SubscriptionControlBlock>> subscriptions;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (!impl_->accepting) {
      return;
    }
    impl_->accepting = false;
    for (auto& topic : impl_->topics) {
      for (auto& entry : topic.second.subscriptions) {
        if (auto state = entry.second.lock()) {
          subscriptions.push_back(std::move(state));
        }
      }
      topic.second.subscriptions.clear();
    }
  }
  for (const auto& state : subscriptions) {
    state->cancel();
  }
}

/// 查询总线接收状态。
bool TopicBus::is_accepting() const noexcept {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->accepting;
}

}  // namespace detail

/// 创建 Executor 和 TopicBus；0 线程配置会转换为至少一个硬件 worker。
Runtime::Runtime(std::size_t worker_threads) {
  if (worker_threads == 0) {
    worker_threads = std::max<std::size_t>(1, std::thread::hardware_concurrency());
  }
  executor_ = std::make_shared<Executor>(worker_threads);
  bus_ = std::make_shared<detail::TopicBus>(executor_);
}

Runtime::~Runtime() { shutdown(); }

/// 创建绑定当前总线的命名节点。
Node Runtime::create_node(const std::string& name) const {
  if (name.empty()) {
    throw MiddlewareError("node name must not be empty");
  }
  if (!bus_ || !bus_->is_accepting()) {
    throw MiddlewareError("middleware runtime is stopped");
  }
  return Node(name, bus_);
}

/// 按固定顺序关闭总线和执行器，重复调用不会产生副作用。
void Runtime::shutdown() noexcept {
  if (bus_) {
    bus_->stop();
  }
  if (executor_) {
    executor_->shutdown();
  }
}

/// 查询运行时状态。
bool Runtime::is_running() const noexcept {
  return bus_ && bus_->is_accepting();
}

}  // namespace robot_middleware
