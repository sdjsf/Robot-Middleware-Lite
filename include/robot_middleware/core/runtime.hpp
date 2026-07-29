#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <typeindex>
#include <utility>

#include "robot_middleware/core/message.hpp"
#include "robot_middleware/executor/executor.hpp"

namespace robot_middleware {

/// 中间件 API 的统一运行时错误。
class MiddlewareError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

/// 订阅邮箱满时的处理策略。
enum class OverflowPolicy {
  DropOldest,   ///< 丢弃最旧消息，为最新消息腾出空间。
  RejectNewest, ///< 保留现有消息，拒绝本次新消息。
};

/// 单个订阅者的队列配置。
struct SubscriptionOptions {
  std::size_t queue_depth{10};
  OverflowPolicy overflow_policy{OverflowPolicy::DropOldest};
  /// false 表示该订阅只接收本地发布，供网络桥导出端阻止远端消息回环。
  bool receive_remote{true};
};

/// 一次 publish 调用对全部匹配订阅者的投递结果。
struct PublishResult {
  std::size_t matched_subscribers{0};
  std::size_t enqueued{0};
  std::size_t dropped_oldest{0};
  std::size_t rejected_newest{0};
  std::size_t executor_rejected{0};
};

/// 单个订阅从创建至今的累计统计和当前状态。
struct SubscriptionStats {
  std::uint64_t enqueued{0};
  std::uint64_t delivered{0};
  std::uint64_t dropped_oldest{0};
  std::uint64_t rejected_newest{0};
  std::uint64_t executor_rejected{0};
  std::uint64_t callback_errors{0};
  std::size_t pending{0};
  std::size_t in_flight{0};
  bool active{false};
};

namespace detail {

/// TopicBus 向单个订阅邮箱投递时的内部结果。
struct DeliveryOutcome {
  bool enqueued{false};
  bool dropped_oldest{false};
  bool rejected_newest{false};
  bool executor_rejected{false};
};

/// 订阅生命周期控制块。
///
/// 排队任务只捕获该共享控制块，不捕获裸 Subscription/Node 指针，
/// 从而保证取消和异步回调之间不会发生对象悬空。
class SubscriptionControlBlock final
    : public std::enable_shared_from_this<SubscriptionControlBlock> {
 public:
  using ErasedMessage = std::shared_ptr<const void>;
  using ErasedCallback = std::function<void(const ErasedMessage&)>;

  /// 创建带独立有界邮箱的订阅控制块。
  SubscriptionControlBlock(SubscriptionOptions options,
                           ErasedCallback callback);
  ~SubscriptionControlBlock();

  SubscriptionControlBlock(const SubscriptionControlBlock&) = delete;
  SubscriptionControlBlock& operator=(const SubscriptionControlBlock&) = delete;

  /// 将消息放入订阅邮箱；必要时仅调度一个串行 drain 任务。
  DeliveryOutcome enqueue(ErasedMessage message,
                          const std::shared_ptr<Executor>& executor,
                          bool is_remote);
  /// 非阻塞取消：禁止新回调启动并清空待处理消息。
  void cancel() noexcept;
  /// 取消并等待已经进入用户回调的任务结束。
  void cancel_and_wait();
  /// 返回当前订阅统计快照。
  SubscriptionStats stats() const;

 private:
  /// 在一个 worker 中串行排空该订阅邮箱，保证回调不并发执行。
  void drain() noexcept;

  struct Impl;
  std::unique_ptr<Impl> impl_;
};

/// TopicBus 注册订阅后返回的内部标识和控制块。
struct SubscriptionRegistration {
  std::uint64_t id{0};
  std::shared_ptr<SubscriptionControlBlock> state;
};

/// 进程内 Topic 注册表和 fan-out 总线。
/// 同名 Topic 首次注册后固定消息 type_id、schema_hash 和本地 C++ 类型。
class TopicBus final : public std::enable_shared_from_this<TopicBus> {
 public:
  explicit TopicBus(std::shared_ptr<Executor> executor);
  ~TopicBus();

  TopicBus(const TopicBus&) = delete;
  TopicBus& operator=(const TopicBus&) = delete;

  /// 注册发布端，并检查 Topic 类型契约。
  void advertise(const std::string& topic, std::uint32_t type_id,
                 std::uint64_t schema_hash, std::type_index local_type);
  /// 注册订阅端及其类型擦除回调。
  SubscriptionRegistration subscribe(
      const std::string& topic, std::uint32_t type_id,
      std::uint64_t schema_hash, std::type_index local_type,
      SubscriptionOptions options,
      SubscriptionControlBlock::ErasedCallback callback);
  /// 将一条共享消息扇出到全部存活的匹配订阅者。
  PublishResult publish(const std::string& topic, std::uint32_t type_id,
                        std::uint64_t schema_hash, std::type_index local_type,
                        SubscriptionControlBlock::ErasedMessage message,
                        bool is_remote);
  /// 从注册表删除指定订阅。
  void unsubscribe(const std::string& topic, std::uint64_t id) noexcept;
  /// 停止总线并取消全部订阅。
  void stop() noexcept;
  /// 查询总线是否仍接受注册和发布。
  bool is_accepting() const noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace detail

/// 强类型 Topic 发布端；对象可复制，并与 TopicBus 共享生命周期。
template <typename MessageT>
class Publisher {
 public:
  /// 复制消息并异步投递。
  PublishResult publish(const MessageT& message) const {
    return publish_shared(std::make_shared<const MessageT>(message), false);
  }

  /// 移动消息并异步投递。
  PublishResult publish(MessageT&& message) const {
    return publish_shared(
        std::make_shared<const MessageT>(std::move(message)), false);
  }

  /// 将网络接收的消息注入本地 Topic；只接收本地消息的桥导出订阅会跳过它。
  PublishResult publish_remote(const MessageT& message) const {
    return publish_shared(std::make_shared<const MessageT>(message), true);
  }

  /// 移动注入网络接收消息，语义与 publish_remote(const MessageT&) 相同。
  PublishResult publish_remote(MessageT&& message) const {
    return publish_shared(
        std::make_shared<const MessageT>(std::move(message)), true);
  }

  /// 返回绑定的 Topic 名称。
  const std::string& topic() const noexcept { return topic_; }
  /// 判断发布端是否已初始化。
  explicit operator bool() const noexcept { return bus_ != nullptr; }

 private:
  friend class Node;

  Publisher(std::shared_ptr<detail::TopicBus> bus, std::string topic)
      : bus_(std::move(bus)), topic_(std::move(topic)) {}

  /// 将强类型 shared_ptr 转为总线使用的类型擦除消息。
  PublishResult publish_shared(std::shared_ptr<const MessageT> message,
                               bool is_remote) const {
    static_assert(is_message_v<MessageT>,
                  "MessageT requires a MessageTraits specialization");
    if (!bus_) {
      throw MiddlewareError("publisher is not initialized");
    }
    return bus_->publish(topic_, MessageTraits<MessageT>::type_id,
                         MessageTraits<MessageT>::schema_hash,
                         std::type_index(typeid(MessageT)), std::move(message),
                         is_remote);
  }

  std::shared_ptr<detail::TopicBus> bus_;
  std::string topic_;
};

/// 强类型订阅句柄；采用移动语义，析构时自动执行非阻塞取消。
template <typename MessageT>
class Subscription {
 public:
  Subscription() = default;
  ~Subscription() { cancel(); }

  Subscription(const Subscription&) = delete;
  Subscription& operator=(const Subscription&) = delete;

  Subscription(Subscription&& other) noexcept { move_from(std::move(other)); }

  Subscription& operator=(Subscription&& other) noexcept {
    if (this != &other) {
      cancel();
      move_from(std::move(other));
    }
    return *this;
  }

  /// 非阻塞取消订阅；已开始的回调可以继续完成。
  void cancel() noexcept {
    if (!state_) {
      return;
    }
    state_->cancel();
    if (bus_) {
      bus_->unsubscribe(topic_, id_);
    }
    state_.reset();
    bus_.reset();
    topic_.clear();
    id_ = 0;
  }

  /// 严格取消并等待 in-flight 回调结束。
  /// 禁止在该订阅自己的回调中调用，否则抛出 MiddlewareError 防止自等待。
  void cancel_and_wait() {
    if (!state_) {
      return;
    }
    auto state = state_;
    state->cancel();
    if (bus_) {
      bus_->unsubscribe(topic_, id_);
    }
    state->cancel_and_wait();
    state_.reset();
    bus_.reset();
    topic_.clear();
    id_ = 0;
  }

  /// 返回订阅统计；空句柄返回零值统计。
  SubscriptionStats stats() const {
    return state_ ? state_->stats() : SubscriptionStats{};
  }

  /// 返回绑定的 Topic 名称。
  const std::string& topic() const noexcept { return topic_; }
  /// 判断句柄当前是否持有有效订阅。
  explicit operator bool() const noexcept { return state_ != nullptr; }

 private:
  friend class Node;

  Subscription(std::shared_ptr<detail::TopicBus> bus, std::string topic,
               detail::SubscriptionRegistration registration)
      : bus_(std::move(bus)),
        topic_(std::move(topic)),
        id_(registration.id),
        state_(std::move(registration.state)) {}

  /// 转移订阅所有权并清空源句柄。
  void move_from(Subscription&& other) noexcept {
    bus_ = std::move(other.bus_);
    topic_ = std::move(other.topic_);
    id_ = std::exchange(other.id_, 0);
    state_ = std::move(other.state_);
  }

  std::shared_ptr<detail::TopicBus> bus_;
  std::string topic_;
  std::uint64_t id_{0};
  std::shared_ptr<detail::SubscriptionControlBlock> state_;
};

/// 中间件节点，用于按名称创建强类型发布端和订阅端。
class Node {
 public:
  /// 返回节点名称。
  const std::string& name() const noexcept { return name_; }

  /// 创建并注册指定消息类型的发布端。
  template <typename MessageT>
  Publisher<MessageT> create_publisher(const std::string& topic) const {
    static_assert(is_message_v<MessageT>,
                  "MessageT requires a MessageTraits specialization");
    require_initialized();
    bus_->advertise(topic, MessageTraits<MessageT>::type_id,
                    MessageTraits<MessageT>::schema_hash,
                    std::type_index(typeid(MessageT)));
    return Publisher<MessageT>(bus_, topic);
  }

  /// 创建指定消息类型的订阅，并将用户回调包装为类型擦除回调。
  /// move-only 回调通过 shared_ptr 保持，可安全存入 std::function。
  template <typename MessageT, typename Callback>
  Subscription<MessageT> create_subscription(
      const std::string& topic, Callback&& callback,
      SubscriptionOptions options = {}) const {
    static_assert(is_message_v<MessageT>,
                  "MessageT requires a MessageTraits specialization");
    static_assert(std::is_invocable<Callback&, const MessageT&>::value,
                  "subscription callback must accept const MessageT&");
    require_initialized();

    using CallbackT = typename std::decay<Callback>::type;
    auto typed_callback = std::make_shared<CallbackT>(
        std::forward<Callback>(callback));
    auto erased_callback =
        [typed_callback = std::move(typed_callback)](
            const detail::SubscriptionControlBlock::ErasedMessage& message)
            mutable {
              (*typed_callback)(
                  *std::static_pointer_cast<const MessageT>(message));
            };

    auto registration = bus_->subscribe(
        topic, MessageTraits<MessageT>::type_id,
        MessageTraits<MessageT>::schema_hash, std::type_index(typeid(MessageT)),
        options, std::move(erased_callback));
    return Subscription<MessageT>(bus_, topic, std::move(registration));
  }

 private:
  friend class Runtime;

  Node(std::string name, std::shared_ptr<detail::TopicBus> bus)
      : name_(std::move(name)), bus_(std::move(bus)) {}

  /// 确保节点由 Runtime 正确创建。
  void require_initialized() const {
    if (!bus_) {
      throw MiddlewareError("node is not initialized");
    }
  }

  std::string name_;
  std::shared_ptr<detail::TopicBus> bus_;
};

/// 中间件运行时，统一持有 Executor 与进程内 TopicBus。
/// 析构时按“停止总线、取消订阅、排空执行器”的顺序关闭。
class Runtime final {
 public:
  /// 创建运行时；worker_threads=0 时使用硬件并发数（至少 1）。
  explicit Runtime(std::size_t worker_threads = 0);
  ~Runtime();

  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;

  /// 创建一个绑定到当前 TopicBus 的节点。
  Node create_node(const std::string& name) const;
  /// 幂等关闭运行时。
  void shutdown() noexcept;
  /// 查询运行时是否仍接受操作。
  bool is_running() const noexcept;

 private:
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<detail::TopicBus> bus_;
};

}  // namespace robot_middleware
