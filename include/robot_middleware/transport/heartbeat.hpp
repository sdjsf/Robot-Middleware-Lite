#pragma once

#include <chrono>
#include <cstdint>
#include <string_view>

#include "robot_middleware/transport/frame.hpp"

namespace robot_middleware::transport {

/// flags bit 0 表示中间件控制面心跳帧，不进入业务 Topic。
inline constexpr std::uint8_t kHeartbeatFlag = 0x01U;
inline constexpr std::string_view kHeartbeatTopic = "__rml/heartbeat";

/// 创建控制面心跳。publisher_id 标识连接端，sequence 用于诊断心跳连续性。
inline Frame make_heartbeat_frame(std::uint64_t publisher_id,
                                  std::uint64_t sequence) {
  Frame frame;
  frame.flags = kHeartbeatFlag;
  frame.sequence = sequence;
  frame.publisher_id = publisher_id;
  frame.send_time_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
  frame.topic = std::string(kHeartbeatTopic);
  return frame;
}

/// 严格识别本协议心跳，避免带未知控制字段的帧被错误吞掉。
inline bool is_heartbeat_frame(const Frame &frame) noexcept {
  return frame.flags == kHeartbeatFlag && frame.topic == kHeartbeatTopic &&
         frame.type_id == 0U && frame.schema_hash == 0U &&
         frame.payload.empty();
}

/// 先检查控制位，便于把“置位但字段不合法”的伪心跳作为协议错误拒绝。
inline bool has_heartbeat_flag(const Frame &frame) noexcept {
  return (frame.flags & kHeartbeatFlag) != 0U;
}

} // namespace robot_middleware::transport
