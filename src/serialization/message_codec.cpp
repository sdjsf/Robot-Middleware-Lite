#include "robot_middleware/serialization/message_codec.hpp"

// 内置机器人消息的逐字段编解码实现。
// 不直接复制结构体内存，避免字节序、padding 和 ABI 差异进入线协议。
namespace robot_middleware {
namespace serialization {

/// 按 schema 顺序编码 IMU 时间戳、偏航角和角速度。
ByteBuffer MessageCodec<ImuMsg>::encode(const ImuMsg& message) {
  BinaryWriter writer;
  writer.write_u64(message.timestamp_ns);
  writer.write_double(message.yaw);
  writer.write_double(message.gyro_z);
  return writer.take_buffer();
}

/// 按相同字段顺序恢复 ImuMsg。
ImuMsg MessageCodec<ImuMsg>::decode(BinaryReader& reader) {
  ImuMsg message{};
  message.timestamp_ns = reader.read_u64();
  message.yaw = reader.read_double();
  message.gyro_z = reader.read_double();
  return message;
}

/// 按 schema 顺序编码位置和四元数。
ByteBuffer MessageCodec<PoseMsg>::encode(const PoseMsg& message) {
  BinaryWriter writer;
  writer.write_u64(message.timestamp_ns);
  writer.write_double(message.x);
  writer.write_double(message.y);
  writer.write_double(message.z);
  writer.write_double(message.qx);
  writer.write_double(message.qy);
  writer.write_double(message.qz);
  writer.write_double(message.qw);
  return writer.take_buffer();
}

/// 从负载恢复 PoseMsg。
PoseMsg MessageCodec<PoseMsg>::decode(BinaryReader& reader) {
  PoseMsg message{};
  message.timestamp_ns = reader.read_u64();
  message.x = reader.read_double();
  message.y = reader.read_double();
  message.z = reader.read_double();
  message.qx = reader.read_double();
  message.qy = reader.read_double();
  message.qz = reader.read_double();
  message.qw = reader.read_double();
  return message;
}

/// 编码平面底盘速度控制指令。
ByteBuffer MessageCodec<ControlMsg>::encode(const ControlMsg& message) {
  BinaryWriter writer;
  writer.write_u64(message.timestamp_ns);
  writer.write_double(message.linear_x);
  writer.write_double(message.linear_y);
  writer.write_double(message.angular_z);
  return writer.take_buffer();
}

/// 从负载恢复 ControlMsg。
ControlMsg MessageCodec<ControlMsg>::decode(BinaryReader& reader) {
  ControlMsg message{};
  message.timestamp_ns = reader.read_u64();
  message.linear_x = reader.read_double();
  message.linear_y = reader.read_double();
  message.angular_z = reader.read_double();
  return message;
}

}  // namespace serialization
}  // namespace robot_middleware
