#include "test_harness.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "robot_middleware/core/runtime.hpp"
#include "robot_middleware/core/thread_safe_queue.hpp"
#include "robot_middleware/executor/thread_pool.hpp"

namespace {

using namespace std::chrono_literals;

using robot_middleware::ImuMsg;
using robot_middleware::MiddlewareError;
using robot_middleware::OverflowPolicy;
using robot_middleware::PoseMsg;
using robot_middleware::PublishResult;
using robot_middleware::Runtime;
using robot_middleware::SubscriptionOptions;
using robot_middleware::ThreadPool;
using robot_middleware::ThreadSafeQueue;

constexpr auto kTimeout = std::chrono::seconds(3);

/// 可由测试线程显式打开的共享门，用于确定性控制并发时序。
class ReleaseGate {
 public:
  ReleaseGate() : future_(promise_.get_future().share()) {}
  ~ReleaseGate() { open(); }

  ReleaseGate(const ReleaseGate&) = delete;
  ReleaseGate& operator=(const ReleaseGate&) = delete;

  std::shared_future<void> future() const { return future_; }

  void open() noexcept {
    if (open_) {
      return;
    }
    open_ = true;
    try {
      promise_.set_value();
    } catch (...) {
    }
  }

 private:
  std::promise<void> promise_;
  std::shared_future<void> future_;
  bool open_{false};
};

/// 验证 move-only 元素、FIFO、关闭排空和幂等 close。
void queue_fifo_close_and_move_only() {
  ThreadSafeQueue<std::unique_ptr<int>> queue;
  RML_CHECK(queue.push(std::make_unique<int>(10)));
  RML_CHECK(queue.push(std::make_unique<int>(20)));
  RML_CHECK(queue.close());
  RML_CHECK(!queue.close());
  RML_CHECK(!queue.push(std::make_unique<int>(30)));

  auto first = queue.try_pop();
  auto second = queue.wait_pop();
  auto exhausted = queue.wait_pop_for(1ms);
  RML_CHECK(first.has_value());
  RML_CHECK(second.has_value());
  RML_CHECK_EQ(**first, 10);
  RML_CHECK_EQ(**second, 20);
  RML_CHECK(!exhausted.has_value());
  RML_CHECK(!queue.try_pop().has_value());
}

/// 验证 close 能唤醒因有界队列已满而阻塞的生产者。
void queue_close_wakes_blocked_producer() {
  ThreadSafeQueue<std::unique_ptr<int>> queue(1);
  RML_CHECK(queue.push(std::make_unique<int>(1)));

  std::promise<void> started_promise;
  auto started = started_promise.get_future();
  auto producer = std::async(
      std::launch::async,
      [&queue, started_promise = std::move(started_promise)]() mutable {
        started_promise.set_value();
        return queue.push(std::make_unique<int>(2));
      });

  const auto started_status = started.wait_for(kTimeout);
  const bool returned_while_full =
      producer.wait_for(25ms) == std::future_status::ready;
  queue.close();
  const auto completion_status = producer.wait_for(kTimeout);
  const bool push_result = producer.get();

  RML_CHECK(started_status == std::future_status::ready);
  RML_CHECK(!returned_while_full);
  RML_CHECK(completion_status == std::future_status::ready);
  RML_CHECK(!push_result);

  auto retained = queue.wait_pop();
  RML_CHECK(retained.has_value());
  RML_CHECK_EQ(**retained, 1);
  RML_CHECK(!queue.wait_pop().has_value());
}

/// 以 4 生产者、4 消费者验证 20 万条消息恰好消费一次。
void queue_mpmc_delivers_each_item_once() {
  constexpr std::size_t kProducerCount = 4;
  constexpr std::size_t kConsumerCount = 4;
  constexpr std::size_t kItemsPerProducer = 50000;
  constexpr std::size_t kTotal = kProducerCount * kItemsPerProducer;

  ThreadSafeQueue<std::size_t> queue(1024);
  std::vector<std::atomic<std::uint8_t>> seen(kTotal);
  for (auto& count : seen) {
    count.store(0, std::memory_order_relaxed);
  }
  std::atomic<std::size_t> consumed{0};
  std::atomic<bool> producer_failed{false};

  std::vector<std::thread> consumers;
  for (std::size_t index = 0; index < kConsumerCount; ++index) {
    consumers.emplace_back([&] {
      while (auto value = queue.wait_pop()) {
        seen[*value].fetch_add(1, std::memory_order_relaxed);
        consumed.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  std::vector<std::thread> producers;
  for (std::size_t producer = 0; producer < kProducerCount; ++producer) {
    producers.emplace_back([&, producer] {
      const std::size_t base = producer * kItemsPerProducer;
      for (std::size_t sequence = 0; sequence < kItemsPerProducer; ++sequence) {
        if (!queue.push(base + sequence)) {
          producer_failed.store(true, std::memory_order_relaxed);
          return;
        }
      }
    });
  }
  for (auto& producer : producers) {
    producer.join();
  }
  queue.close();
  for (auto& consumer : consumers) {
    consumer.join();
  }

  RML_CHECK(!producer_failed.load(std::memory_order_relaxed));
  RML_CHECK_EQ(consumed.load(), kTotal);
  for (const auto& count : seen) {
    RML_CHECK_EQ(count.load(std::memory_order_relaxed), std::uint8_t{1});
  }
}

/// 验证 submit 返回值、future 异常、失败统计和重复 shutdown。
void thread_pool_results_failures_and_shutdown() {
  ThreadPool pool(2);

  auto value = pool.submit([](int left, int right) { return left + right; },
                           20, 22);
  auto failure = pool.submit([]() -> int {
    throw std::runtime_error("expected task failure");
  });
  auto continuation = pool.submit([] { return std::make_unique<int>(7); });

  RML_CHECK_EQ(value.get(), 42);
  RML_CHECK_THROWS(std::runtime_error, failure.get());
  auto continued_value = continuation.get();
  RML_CHECK(continued_value != nullptr);
  RML_CHECK_EQ(*continued_value, 7);

  pool.shutdown();
  pool.shutdown();
  const auto stats = pool.stats();
  RML_CHECK_EQ(stats.submitted, UINT64_C(3));
  RML_CHECK_EQ(stats.completed, UINT64_C(3));
  RML_CHECK_EQ(stats.failed, UINT64_C(1));
  RML_CHECK(!pool.is_accepting());
  RML_CHECK(!pool.post([] {}));
  RML_CHECK_THROWS(std::runtime_error, pool.submit([] { return 1; }));
}

/// 验证 shutdown 会排空 10 万个已接受任务。
void thread_pool_drains_high_load() {
  constexpr std::size_t kTasks = 100000;
  ThreadPool pool(4);
  std::atomic<std::size_t> completed{0};
  for (std::size_t index = 0; index < kTasks; ++index) {
    RML_CHECK(pool.post(
        [&completed] { completed.fetch_add(1, std::memory_order_relaxed); }));
  }
  pool.shutdown();
  const auto stats = pool.stats();
  RML_CHECK_EQ(completed.load(std::memory_order_relaxed), kTasks);
  RML_CHECK_EQ(stats.submitted, static_cast<std::uint64_t>(kTasks));
  RML_CHECK_EQ(stats.completed, static_cast<std::uint64_t>(kTasks));
  RML_CHECK_EQ(stats.failed, UINT64_C(0));
}

/// 验证多个外部 shutdown 调用者都会等待实际 drain 完成。
void concurrent_shutdown_callers_wait_for_drain() {
  ThreadPool pool(2);
  ReleaseGate task_gate;
  const auto release = task_gate.future();
  std::atomic<std::size_t> entered_count{0};
  std::promise<void> both_entered_promise;
  auto both_entered = both_entered_promise.get_future();
  for (std::size_t index = 0; index < 2; ++index) {
    RML_CHECK(pool.post([&] {
      if (entered_count.fetch_add(1, std::memory_order_acq_rel) + 1 == 2) {
        both_entered_promise.set_value();
      }
      release.wait();
    }));
  }
  RML_CHECK(both_entered.wait_for(kTimeout) == std::future_status::ready);

  auto first_shutdown =
      std::async(std::launch::async, [&pool] { pool.shutdown(); });
  auto second_shutdown =
      std::async(std::launch::async, [&pool] { pool.shutdown(); });
  const bool first_returned_early =
      first_shutdown.wait_for(25ms) == std::future_status::ready;
  const bool second_returned_early =
      second_shutdown.wait_for(25ms) == std::future_status::ready;
  task_gate.open();
  RML_CHECK(first_shutdown.wait_for(kTimeout) == std::future_status::ready);
  RML_CHECK(second_shutdown.wait_for(kTimeout) == std::future_status::ready);
  first_shutdown.get();
  second_shutdown.get();
  RML_CHECK(!first_returned_early);
  RML_CHECK(!second_returned_early);
  RML_CHECK_EQ(pool.stats().completed, UINT64_C(2));
}

/// 验证最后一个 ThreadPool shared_ptr 可在自身 worker 中安全释放。
void thread_pool_can_be_released_by_its_worker() {
  auto pool = std::make_shared<ThreadPool>(1);
  std::weak_ptr<ThreadPool> weak_pool = pool;
  auto last_owner = pool;
  std::promise<void> released_promise;
  auto released = released_promise.get_future();
  RML_CHECK(pool->post(
      [last_owner = std::move(last_owner), &released_promise]() mutable {
        last_owner.reset();
        released_promise.set_value();
      }));
  pool.reset();
  RML_CHECK(released.wait_for(kTimeout) == std::future_status::ready);
  released.get();
  RML_CHECK(weak_pool.expired());
}

/// 验证一万条突发消息保持顺序，且同一订阅回调不会并发执行。
void runtime_preserves_order_for_large_burst() {
  constexpr std::size_t kMessages = 10000;
  Runtime runtime(4);
  auto node = runtime.create_node("ordered_node");
  auto publisher = node.create_publisher<ImuMsg>("/ordered_imu");

  std::mutex received_mutex;
  std::vector<std::uint64_t> received;
  received.reserve(kMessages);
  std::atomic<std::size_t> callback_count{0};
  std::atomic<std::size_t> callbacks_in_flight{0};
  std::atomic<bool> concurrent_callback{false};
  std::promise<void> complete_promise;
  auto complete = complete_promise.get_future();

  SubscriptionOptions options;
  options.queue_depth = kMessages + 1;
  auto subscription = node.create_subscription<ImuMsg>(
      "/ordered_imu",
      [&](const ImuMsg& message) {
        if (callbacks_in_flight.fetch_add(1, std::memory_order_acq_rel) != 0) {
          concurrent_callback.store(true, std::memory_order_release);
        }
        {
          std::lock_guard<std::mutex> lock(received_mutex);
          received.push_back(message.timestamp_ns);
        }
        callbacks_in_flight.fetch_sub(1, std::memory_order_acq_rel);
        if (callback_count.fetch_add(1, std::memory_order_acq_rel) + 1 ==
            kMessages) {
          complete_promise.set_value();
        }
      },
      options);

  std::size_t dropped = 0;
  std::size_t rejected = 0;
  for (std::size_t index = 0; index < kMessages; ++index) {
    ImuMsg message{};
    message.timestamp_ns = static_cast<std::uint64_t>(index);
    const PublishResult result = publisher.publish(std::move(message));
    dropped += result.dropped_oldest;
    rejected += result.rejected_newest;
  }

  const auto status = complete.wait_for(kTimeout);
  RML_CHECK(status == std::future_status::ready);
  complete.get();
  subscription.cancel_and_wait();

  RML_CHECK_EQ(dropped, std::size_t{0});
  RML_CHECK_EQ(rejected, std::size_t{0});
  RML_CHECK(!concurrent_callback.load(std::memory_order_acquire));
  RML_CHECK_EQ(received.size(), kMessages);
  for (std::size_t index = 0; index < received.size(); ++index) {
    RML_CHECK_EQ(received[index], static_cast<std::uint64_t>(index));
  }
}

/// 验证发布消息会完整 fan-out 到每个订阅者，而不是竞争消费。
void runtime_fans_out_to_every_subscription() {
  constexpr std::size_t kMessages = 256;
  Runtime runtime(3);
  auto node = runtime.create_node("fanout_node");
  auto publisher = node.create_publisher<ImuMsg>("/fanout");

  std::atomic<std::size_t> first_count{0};
  std::atomic<std::size_t> second_count{0};
  std::promise<void> first_done_promise;
  std::promise<void> second_done_promise;
  auto first_done = first_done_promise.get_future();
  auto second_done = second_done_promise.get_future();
  SubscriptionOptions options;
  options.queue_depth = kMessages + 1;

  auto first = node.create_subscription<ImuMsg>(
      "/fanout",
      [&](const ImuMsg&) {
        if (first_count.fetch_add(1, std::memory_order_acq_rel) + 1 ==
            kMessages) {
          first_done_promise.set_value();
        }
      },
      options);
  auto second = node.create_subscription<ImuMsg>(
      "/fanout",
      [&](const ImuMsg&) {
        if (second_count.fetch_add(1, std::memory_order_acq_rel) + 1 ==
            kMessages) {
          second_done_promise.set_value();
        }
      },
      options);

  std::size_t total_matched = 0;
  std::size_t total_enqueued = 0;
  for (std::size_t index = 0; index < kMessages; ++index) {
    ImuMsg message{};
    message.timestamp_ns = index;
    const auto result = publisher.publish(std::move(message));
    total_matched += result.matched_subscribers;
    total_enqueued += result.enqueued;
  }

  const auto first_status = first_done.wait_for(kTimeout);
  const auto second_status = second_done.wait_for(kTimeout);
  RML_CHECK(first_status == std::future_status::ready);
  RML_CHECK(second_status == std::future_status::ready);
  first_done.get();
  second_done.get();
  first.cancel_and_wait();
  second.cancel_and_wait();

  RML_CHECK_EQ(first_count.load(), kMessages);
  RML_CHECK_EQ(second_count.load(), kMessages);
  RML_CHECK_EQ(total_matched, kMessages * 2);
  RML_CHECK_EQ(total_enqueued, kMessages * 2);
}

/// 验证同名 Topic 注册不同消息类型时立即失败。
void runtime_rejects_topic_type_conflicts() {
  Runtime runtime(1);
  auto node = runtime.create_node("type_check_node");
  auto imu_publisher = node.create_publisher<ImuMsg>("/typed_topic");
  RML_CHECK(static_cast<bool>(imu_publisher));
  RML_CHECK_THROWS(
      MiddlewareError,
      node.create_subscription<PoseMsg>("/typed_topic", [](const PoseMsg&) {}));
  RML_CHECK_THROWS(MiddlewareError,
                   node.create_publisher<PoseMsg>("/typed_topic"));
}

/// 验证持有 unique_ptr 的 move-only 用户回调可以注册和执行。
void runtime_accepts_move_only_callbacks() {
  Runtime runtime(1);
  auto node = runtime.create_node("move_only_callback_node");
  std::promise<int> observed_promise;
  auto observed = observed_promise.get_future();
  auto owned_value = std::make_unique<int>(42);
  auto subscription = node.create_subscription<ImuMsg>(
      "/move_only_callback",
      [value = std::move(owned_value),
       &observed_promise](const ImuMsg&) mutable {
        observed_promise.set_value(*value);
      });
  auto publisher = node.create_publisher<ImuMsg>("/move_only_callback");
  publisher.publish(ImuMsg{});
  RML_CHECK(observed.wait_for(kTimeout) == std::future_status::ready);
  RML_CHECK_EQ(observed.get(), 42);
  subscription.cancel_and_wait();
}

/// 溢出策略测试结果，包含实际回调序列和统计快照。
struct OverflowObservation {
  std::vector<std::uint64_t> received;
  robot_middleware::SubscriptionStats stats;
  std::size_t publish_dropped{0};
  std::size_t publish_rejected{0};
};

/// 用阻塞任务占住唯一 worker，确定性制造订阅邮箱溢出。
OverflowObservation run_overflow_case(OverflowPolicy policy) {
  constexpr std::size_t kDepth = 3;
  constexpr std::size_t kPublished = 10;

  Runtime runtime(1);
  auto node = runtime.create_node("overflow_node");
  ReleaseGate worker_gate;

  std::promise<void> blocker_entered_promise;
  auto blocker_entered = blocker_entered_promise.get_future();
  const auto worker_release = worker_gate.future();
  auto blocker_subscription = node.create_subscription<ImuMsg>(
      "/executor_blocker",
      [worker_release, &blocker_entered_promise](const ImuMsg&) {
        blocker_entered_promise.set_value();
        worker_release.wait();
      });
  auto blocker_publisher =
      node.create_publisher<ImuMsg>("/executor_blocker");
  blocker_publisher.publish(ImuMsg{});

  const auto blocker_status = blocker_entered.wait_for(kTimeout);
  if (blocker_status != std::future_status::ready) {
    worker_gate.open();
  }
  RML_CHECK(blocker_status == std::future_status::ready);
  blocker_entered.get();

  OverflowObservation observation;
  SubscriptionOptions options;
  options.queue_depth = kDepth;
  options.overflow_policy = policy;
  auto target_subscription = node.create_subscription<ImuMsg>(
      "/overflow_target",
      [&observation](const ImuMsg& message) {
        observation.received.push_back(message.timestamp_ns);
      },
      options);
  auto target_publisher =
      node.create_publisher<ImuMsg>("/overflow_target");

  for (std::size_t index = 0; index < kPublished; ++index) {
    ImuMsg message{};
    message.timestamp_ns = index;
    const auto result = target_publisher.publish(std::move(message));
    observation.publish_dropped += result.dropped_oldest;
    observation.publish_rejected += result.rejected_newest;
  }

  // sentinel 排在目标 drain 后执行；它触发时目标统计已稳定，无需 sleep 或轮询。
  std::promise<void> sentinel_promise;
  auto sentinel_done = sentinel_promise.get_future();
  auto sentinel_subscription = node.create_subscription<ImuMsg>(
      "/overflow_sentinel",
      [&sentinel_promise](const ImuMsg&) { sentinel_promise.set_value(); });
  auto sentinel_publisher =
      node.create_publisher<ImuMsg>("/overflow_sentinel");
  sentinel_publisher.publish(ImuMsg{});

  worker_gate.open();
  const auto sentinel_status = sentinel_done.wait_for(kTimeout);
  RML_CHECK(sentinel_status == std::future_status::ready);
  sentinel_done.get();
  observation.stats = target_subscription.stats();
  target_subscription.cancel_and_wait();
  sentinel_subscription.cancel_and_wait();
  blocker_subscription.cancel_and_wait();
  return observation;
}

/// 分别验证 DropOldest 保留最新数据、RejectNewest 保留最早数据。
void runtime_applies_overflow_policies_deterministically() {
  const OverflowObservation drop =
      run_overflow_case(OverflowPolicy::DropOldest);
  RML_CHECK_EQ(drop.publish_dropped, std::size_t{7});
  RML_CHECK_EQ(drop.publish_rejected, std::size_t{0});
  RML_CHECK_EQ(drop.stats.enqueued, UINT64_C(10));
  RML_CHECK_EQ(drop.stats.delivered, UINT64_C(3));
  RML_CHECK_EQ(drop.stats.dropped_oldest, UINT64_C(7));
  RML_CHECK_EQ(drop.stats.rejected_newest, UINT64_C(0));
  RML_CHECK_EQ(drop.received.size(), std::size_t{3});
  RML_CHECK_EQ(drop.received[0], UINT64_C(7));
  RML_CHECK_EQ(drop.received[1], UINT64_C(8));
  RML_CHECK_EQ(drop.received[2], UINT64_C(9));

  const OverflowObservation reject =
      run_overflow_case(OverflowPolicy::RejectNewest);
  RML_CHECK_EQ(reject.publish_dropped, std::size_t{0});
  RML_CHECK_EQ(reject.publish_rejected, std::size_t{7});
  RML_CHECK_EQ(reject.stats.enqueued, UINT64_C(3));
  RML_CHECK_EQ(reject.stats.delivered, UINT64_C(3));
  RML_CHECK_EQ(reject.stats.dropped_oldest, UINT64_C(0));
  RML_CHECK_EQ(reject.stats.rejected_newest, UINT64_C(7));
  RML_CHECK_EQ(reject.received.size(), std::size_t{3});
  RML_CHECK_EQ(reject.received[0], UINT64_C(0));
  RML_CHECK_EQ(reject.received[1], UINT64_C(1));
  RML_CHECK_EQ(reject.received[2], UINT64_C(2));
}

/// 验证 cancel_and_wait 会等待当前回调，返回后不再启动新回调。
void cancel_and_wait_prevents_future_callbacks() {
  Runtime runtime(2);
  auto node = runtime.create_node("cancel_node");
  ReleaseGate callback_gate;
  const auto callback_release = callback_gate.future();

  std::atomic<std::size_t> callback_count{0};
  std::promise<void> callback_entered_promise;
  auto callback_entered = callback_entered_promise.get_future();
  auto subscription = node.create_subscription<ImuMsg>(
      "/cancel_topic",
      [&](const ImuMsg&) {
        callback_count.fetch_add(1, std::memory_order_relaxed);
        callback_entered_promise.set_value();
        callback_release.wait();
      });
  auto publisher = node.create_publisher<ImuMsg>("/cancel_topic");
  publisher.publish(ImuMsg{});

  const auto entered_status = callback_entered.wait_for(kTimeout);
  if (entered_status != std::future_status::ready) {
    callback_gate.open();
  }
  RML_CHECK(entered_status == std::future_status::ready);
  callback_entered.get();

  std::promise<void> cancellation_started_promise;
  auto cancellation_started = cancellation_started_promise.get_future();
  auto cancellation = std::async(
      std::launch::async,
      [&subscription,
       cancellation_started_promise =
           std::move(cancellation_started_promise)]() mutable {
        cancellation_started_promise.set_value();
        subscription.cancel_and_wait();
      });
  const auto cancellation_started_status =
      cancellation_started.wait_for(kTimeout);
  const bool returned_before_callback =
      cancellation.wait_for(25ms) == std::future_status::ready;
  callback_gate.open();
  const auto cancellation_status = cancellation.wait_for(kTimeout);
  cancellation.get();

  RML_CHECK(cancellation_started_status == std::future_status::ready);
  RML_CHECK(!returned_before_callback);
  RML_CHECK(cancellation_status == std::future_status::ready);

  for (std::size_t index = 0; index < 20; ++index) {
    const auto result = publisher.publish(ImuMsg{});
    RML_CHECK_EQ(result.matched_subscribers, std::size_t{0});
  }
  RML_CHECK_EQ(callback_count.load(), std::size_t{1});
}

/// 验证用户回调异常被隔离和计数，后续消息仍可执行。
void callback_failure_isolated_and_counted() {
  Runtime runtime(1);
  auto node = runtime.create_node("callback_error_node");
  std::atomic<std::size_t> calls{0};
  auto subscription = node.create_subscription<ImuMsg>(
      "/callback_error",
      [&calls](const ImuMsg& message) {
        calls.fetch_add(1, std::memory_order_relaxed);
        if (message.timestamp_ns == 1) {
          throw std::runtime_error("callback failure");
        }
      });
  auto publisher = node.create_publisher<ImuMsg>("/callback_error");

  ImuMsg first{};
  first.timestamp_ns = 1;
  ImuMsg second{};
  second.timestamp_ns = 2;
  publisher.publish(std::move(first));
  publisher.publish(std::move(second));

  std::promise<void> sentinel_promise;
  auto sentinel_done = sentinel_promise.get_future();
  auto sentinel_subscription = node.create_subscription<ImuMsg>(
      "/callback_error_sentinel",
      [&sentinel_promise](const ImuMsg&) { sentinel_promise.set_value(); });
  auto sentinel_publisher =
      node.create_publisher<ImuMsg>("/callback_error_sentinel");
  sentinel_publisher.publish(ImuMsg{});

  const auto sentinel_status = sentinel_done.wait_for(kTimeout);
  RML_CHECK(sentinel_status == std::future_status::ready);
  sentinel_done.get();
  const auto stats = subscription.stats();
  RML_CHECK_EQ(calls.load(), std::size_t{2});
  RML_CHECK_EQ(stats.delivered, UINT64_C(2));
  RML_CHECK_EQ(stats.callback_errors, UINT64_C(1));
}

/// 验证回调内再次 publish 不会因总线锁或订阅锁产生死锁。
void callback_can_publish_recursively_without_deadlock() {
  Runtime runtime(1);
  auto node = runtime.create_node("nested_publish_node");
  auto publisher = node.create_publisher<ImuMsg>("/nested_publish");

  std::atomic<std::size_t> calls{0};
  std::promise<void> nested_done_promise;
  auto nested_done = nested_done_promise.get_future();
  auto subscription = node.create_subscription<ImuMsg>(
      "/nested_publish",
      [publisher, &calls, &nested_done_promise](const ImuMsg& message) mutable {
        calls.fetch_add(1, std::memory_order_relaxed);
        if (message.timestamp_ns == 0) {
          ImuMsg nested{};
          nested.timestamp_ns = 1;
          publisher.publish(std::move(nested));
        } else if (message.timestamp_ns == 1) {
          nested_done_promise.set_value();
        }
      });

  ImuMsg initial{};
  initial.timestamp_ns = 0;
  publisher.publish(std::move(initial));
  const auto status = nested_done.wait_for(kTimeout);
  RML_CHECK(status == std::future_status::ready);
  nested_done.get();
  subscription.cancel_and_wait();
  RML_CHECK_EQ(calls.load(), std::size_t{2});
}

/// 验证远端注入仍投递给业务订阅，但不会再次进入仅导出本地消息的桥订阅。
void runtime_filters_remote_messages_for_bridge_exports() {
  Runtime runtime(2);
  auto node = runtime.create_node("remote_origin_node");
  auto publisher = node.create_publisher<ImuMsg>("/remote_origin");

  std::atomic<std::size_t> business_count{0};
  std::atomic<std::size_t> export_count{0};
  std::promise<void> remote_delivered_promise;
  auto remote_delivered = remote_delivered_promise.get_future();
  std::promise<void> local_exported_promise;
  auto local_exported = local_exported_promise.get_future();

  auto business_subscription = node.create_subscription<ImuMsg>(
      "/remote_origin",
      [&](const ImuMsg& message) {
        business_count.fetch_add(1, std::memory_order_relaxed);
        if (message.timestamp_ns == 2U) {
          remote_delivered_promise.set_value();
        }
      });

  SubscriptionOptions export_options;
  export_options.receive_remote = false;
  auto export_subscription = node.create_subscription<ImuMsg>(
      "/remote_origin",
      [&](const ImuMsg&) {
        if (export_count.fetch_add(1, std::memory_order_relaxed) == 0U) {
          local_exported_promise.set_value();
        }
      },
      export_options);

  ImuMsg local{};
  local.timestamp_ns = 1U;
  publisher.publish(local);
  ImuMsg remote{};
  remote.timestamp_ns = 2U;
  publisher.publish_remote(remote);

  RML_CHECK(remote_delivered.wait_for(kTimeout) == std::future_status::ready);
  RML_CHECK(local_exported.wait_for(kTimeout) == std::future_status::ready);
  remote_delivered.get();
  local_exported.get();
  business_subscription.cancel_and_wait();
  export_subscription.cancel_and_wait();

  RML_CHECK_EQ(business_count.load(), std::size_t{2});
  RML_CHECK_EQ(export_count.load(), std::size_t{1});
}

}  // namespace

// 核心并发与发布订阅测试入口。
int main(int argc, char** argv) {
  return rml_test::run(
      {
          {"queue_fifo_close", queue_fifo_close_and_move_only},
          {"queue_blocked_producer", queue_close_wakes_blocked_producer},
          {"queue_mpmc", queue_mpmc_delivers_each_item_once},
          {"thread_pool", thread_pool_results_failures_and_shutdown},
          {"thread_pool_high_load", thread_pool_drains_high_load},
          {"thread_pool_concurrent_shutdown",
           concurrent_shutdown_callers_wait_for_drain},
          {"thread_pool_worker_release",
           thread_pool_can_be_released_by_its_worker},
          {"pubsub_order", runtime_preserves_order_for_large_burst},
          {"pubsub_fanout", runtime_fans_out_to_every_subscription},
          {"topic_type_conflict", runtime_rejects_topic_type_conflicts},
          {"move_only_callback", runtime_accepts_move_only_callbacks},
          {"overflow_policies",
           runtime_applies_overflow_policies_deterministically},
          {"cancel_and_wait", cancel_and_wait_prevents_future_callbacks},
          {"callback_errors", callback_failure_isolated_and_counted},
          {"nested_publish", callback_can_publish_recursively_without_deadlock},
          {"remote_origin_filter",
           runtime_filters_remote_messages_for_bridge_exports},
      },
      argc, argv);
}
