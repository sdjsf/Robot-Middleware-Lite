#pragma once

#include <cstdint>
#include <vector>

#include "robot_middleware/core/message.hpp"
#include "robot_middleware/serialization/binary_codec.hpp"

namespace robot_middleware {
namespace serialization {

/// 消息 Codec 扩展点；每种可传输消息需要提供特化。
template <typename MessageT>
struct MessageCodec;

/// ImuMsg 的逐字段编解码器。
template <>
struct MessageCodec<ImuMsg> {
  static ByteBuffer encode(const ImuMsg& message);
  static ImuMsg decode(BinaryReader& reader);
};

/// PoseMsg 的逐字段编解码器。
template <>
struct MessageCodec<PoseMsg> {
  static ByteBuffer encode(const PoseMsg& message);
  static PoseMsg decode(BinaryReader& reader);
};

/// ControlMsg 的逐字段编解码器。
template <>
struct MessageCodec<ControlMsg> {
  static ByteBuffer encode(const ControlMsg& message);
  static ControlMsg decode(BinaryReader& reader);
};

/// 将强类型消息编码为确定性的网络序负载。
template <typename MessageT>
ByteBuffer serialize(const MessageT& message) {
  return MessageCodec<MessageT>::encode(message);
}

/// 从裸字节区解码消息，并拒绝未消费的尾随字节。
template <typename MessageT>
MessageT deserialize(const std::uint8_t* data, std::size_t size) {
  BinaryReader reader(data, size);
  MessageT message = MessageCodec<MessageT>::decode(reader);
  reader.require_consumed();
  return message;
}

/// 从 ByteBuffer 解码消息。
template <typename MessageT>
MessageT deserialize(const ByteBuffer& buffer) {
  return deserialize<MessageT>(buffer.data(), buffer.size());
}

}  // namespace serialization
}  // namespace robot_middleware
