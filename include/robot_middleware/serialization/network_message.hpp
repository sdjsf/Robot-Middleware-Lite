#pragma once

#include <chrono>
#include <cstdint>
#include <limits>
#include <string>

#include "robot_middleware/core/message.hpp"
#include "robot_middleware/serialization/message_codec.hpp"
#include "robot_middleware/transport/frame.hpp"

namespace robot_middleware {
namespace serialization {

/// 获取 steady_clock 单调时钟的纳秒时间戳，适合本机进程间延迟测量。
inline std::uint64_t monotonic_time_ns() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

/// 将强类型消息及其稳定元数据封装为网络 Frame。
/// topic 不能为空且必须能由线协议的 uint16 长度字段表示。
template <typename MessageT>
transport::Frame make_frame(const std::string& topic, const MessageT& message,
                            std::uint64_t sequence,
                            std::uint64_t publisher_id,
                            std::uint64_t send_time_ns = monotonic_time_ns()) {
  static_assert(is_message_v<MessageT>,
                "MessageT must have a robot_middleware::MessageTraits specialization");
  if (topic.empty()) {
    throw SerializationError("network topic cannot be empty");
  }
  if (topic.size() > std::numeric_limits<std::uint16_t>::max()) {
    throw SerializationError("network topic exceeds the wire-format limit");
  }

  transport::Frame frame;
  frame.type_id = MessageTraits<MessageT>::type_id;
  frame.schema_hash = MessageTraits<MessageT>::schema_hash;
  frame.sequence = sequence;
  frame.publisher_id = publisher_id;
  frame.send_time_ns = send_time_ns;
  frame.topic = topic;
  frame.payload = serialize(message);
  return frame;
}

/// 校验 Topic、type_id 和 schema_hash 后，从 Frame 解码强类型消息。
template <typename MessageT>
MessageT deserialize_frame(const transport::Frame& frame,
                           const std::string& expected_topic = {}) {
  static_assert(is_message_v<MessageT>,
                "MessageT must have a robot_middleware::MessageTraits specialization");
  if (!expected_topic.empty() && frame.topic != expected_topic) {
    throw SerializationError("network frame topic does not match subscription");
  }
  if (frame.type_id != MessageTraits<MessageT>::type_id) {
    throw SerializationError("network frame message type does not match subscription");
  }
  if (frame.schema_hash != MessageTraits<MessageT>::schema_hash) {
    throw SerializationError("network frame schema does not match subscription");
  }
  return deserialize<MessageT>(frame.payload);
}

}  // namespace serialization
}  // namespace robot_middleware
