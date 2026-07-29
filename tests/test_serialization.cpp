#include "test_harness.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>

#include "robot_middleware/serialization/message_codec.hpp"
#include "robot_middleware/serialization/network_message.hpp"

namespace {

using robot_middleware::ControlMsg;
using robot_middleware::ImuMsg;
using robot_middleware::PoseMsg;
using robot_middleware::serialization::ByteBuffer;
using robot_middleware::serialization::SerializationError;
using robot_middleware::serialization::deserialize;
using robot_middleware::serialization::serialize;

/// 以位模式比较 double，能够覆盖 NaN、负零和无穷值。
std::uint64_t double_bits(double value) {
  static_assert(sizeof(double) == sizeof(std::uint64_t),
                "tests require IEEE-754 64-bit doubles");
  std::uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

/// 从指定 IEEE-754 位模式构造 double。
double double_from_bits(std::uint64_t bits) {
  double value = 0.0;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

/// 构造固定 IMU 样本，供 golden bytes 与 round-trip 测试复用。
ImuMsg make_imu() {
  ImuMsg message{};
  message.timestamp_ns = UINT64_C(0x0102030405060708);
  message.yaw = 1.0;
  message.gyro_z = -2.0;
  return message;
}

/// 构造位姿测试样本。
PoseMsg make_pose() {
  PoseMsg message{};
  message.timestamp_ns = UINT64_C(42);
  message.x = 1.25;
  message.y = -2.5;
  message.z = 3.75;
  message.qx = 0.0;
  message.qy = -0.5;
  message.qz = 0.5;
  message.qw = 1.0;
  return message;
}

/// 构造控制消息测试样本。
ControlMsg make_control() {
  ControlMsg message{};
  message.timestamp_ns = UINT64_C(999999999);
  message.linear_x = 0.75;
  message.linear_y = -0.25;
  message.angular_z = 1.5;
  return message;
}

/// 对合法负载的每一个截断位置验证解码都会失败。
template <typename MessageT>
void check_every_truncation_is_rejected(const MessageT& message) {
  const ByteBuffer encoded = serialize(message);
  RML_CHECK(!encoded.empty());

  for (std::size_t size = 0; size < encoded.size(); ++size) {
    bool rejected = false;
    try {
      static_cast<void>(deserialize<MessageT>(encoded.data(), size));
    } catch (const SerializationError&) {
      rejected = true;
    }
    RML_CHECK(rejected);
  }
}

/// 验证解码器拒绝合法消息后的尾随字节。
template <typename MessageT>
void check_trailing_byte_is_rejected(const MessageT& message) {
  ByteBuffer encoded = serialize(message);
  encoded.push_back(0xa5U);
  RML_CHECK_THROWS(SerializationError, deserialize<MessageT>(encoded));
}

/// 验证三种内置消息逐字段往返后位模式完全一致。
void roundtrip_all_message_types() {
  const ImuMsg imu = make_imu();
  const ImuMsg decoded_imu = deserialize<ImuMsg>(serialize(imu));
  RML_CHECK_EQ(decoded_imu.timestamp_ns, imu.timestamp_ns);
  RML_CHECK_EQ(double_bits(decoded_imu.yaw), double_bits(imu.yaw));
  RML_CHECK_EQ(double_bits(decoded_imu.gyro_z), double_bits(imu.gyro_z));

  const PoseMsg pose = make_pose();
  const PoseMsg decoded_pose = deserialize<PoseMsg>(serialize(pose));
  RML_CHECK_EQ(decoded_pose.timestamp_ns, pose.timestamp_ns);
  RML_CHECK_EQ(double_bits(decoded_pose.x), double_bits(pose.x));
  RML_CHECK_EQ(double_bits(decoded_pose.y), double_bits(pose.y));
  RML_CHECK_EQ(double_bits(decoded_pose.z), double_bits(pose.z));
  RML_CHECK_EQ(double_bits(decoded_pose.qx), double_bits(pose.qx));
  RML_CHECK_EQ(double_bits(decoded_pose.qy), double_bits(pose.qy));
  RML_CHECK_EQ(double_bits(decoded_pose.qz), double_bits(pose.qz));
  RML_CHECK_EQ(double_bits(decoded_pose.qw), double_bits(pose.qw));

  const ControlMsg control = make_control();
  const ControlMsg decoded_control =
      deserialize<ControlMsg>(serialize(control));
  RML_CHECK_EQ(decoded_control.timestamp_ns, control.timestamp_ns);
  RML_CHECK_EQ(double_bits(decoded_control.linear_x),
               double_bits(control.linear_x));
  RML_CHECK_EQ(double_bits(decoded_control.linear_y),
               double_bits(control.linear_y));
  RML_CHECK_EQ(double_bits(decoded_control.angular_z),
               double_bits(control.angular_z));
}

/// 使用固定字节向量验证整数和 double 均采用网络字节序。
void imu_golden_bytes_are_network_order() {
  const ByteBuffer encoded = serialize(make_imu());
  const ByteBuffer expected{
      // 时间戳 timestamp_ns = 0x0102030405060708
      0x01U, 0x02U, 0x03U, 0x04U, 0x05U, 0x06U, 0x07U, 0x08U,
      // 偏航角 yaw = 1.0，IEEE-754 位模式 0x3ff0000000000000
      0x3fU, 0xf0U, 0x00U, 0x00U, 0x00U, 0x00U, 0x00U, 0x00U,
      // Z 轴角速度 gyro_z = -2.0，IEEE-754 位模式 0xc000000000000000
      0xc0U, 0x00U, 0x00U, 0x00U, 0x00U, 0x00U, 0x00U, 0x00U,
  };

  RML_CHECK(encoded == expected);
}

/// 汇总测试全部消息的截断输入。
void every_truncation_is_rejected() {
  check_every_truncation_is_rejected(make_imu());
  check_every_truncation_is_rejected(make_pose());
  check_every_truncation_is_rejected(make_control());
}

/// 汇总测试全部消息的尾随字节拒绝行为。
void trailing_bytes_are_rejected() {
  check_trailing_byte_is_rejected(make_imu());
  check_trailing_byte_is_rejected(make_pose());
  check_trailing_byte_is_rejected(make_control());
}

/// 验证 NaN payload 和正无穷的位模式不被序列化过程改变。
void nan_and_infinity_preserve_bits() {
  constexpr std::uint64_t kNanBits = UINT64_C(0x7ff8000000001234);
  constexpr std::uint64_t kPositiveInfinityBits =
      UINT64_C(0x7ff0000000000000);

  ImuMsg message{};
  message.timestamp_ns = UINT64_C(123);
  message.yaw = double_from_bits(kNanBits);
  message.gyro_z = std::numeric_limits<double>::infinity();

  const ImuMsg decoded = deserialize<ImuMsg>(serialize(message));
  RML_CHECK_EQ(double_bits(decoded.yaw), kNanBits);
  RML_CHECK_EQ(double_bits(decoded.gyro_z), kPositiveInfinityBits);
}

/// 验证 Frame 解码前严格检查 Topic、type_id 和 schema_hash。
void network_frame_contract_is_checked() {
  auto frame = robot_middleware::serialization::make_frame(
      "/imu", make_imu(), UINT64_C(7), UINT64_C(11), UINT64_C(13));
  const auto decoded =
      robot_middleware::serialization::deserialize_frame<ImuMsg>(frame, "/imu");
  RML_CHECK_EQ(decoded.timestamp_ns, make_imu().timestamp_ns);

  auto wrong_type = frame;
  wrong_type.type_id = robot_middleware::MessageTraits<PoseMsg>::type_id;
  RML_CHECK_THROWS(
      SerializationError,
      robot_middleware::serialization::deserialize_frame<ImuMsg>(wrong_type));

  auto wrong_schema = frame;
  wrong_schema.schema_hash ^= UINT64_C(1);
  RML_CHECK_THROWS(
      SerializationError,
      robot_middleware::serialization::deserialize_frame<ImuMsg>(wrong_schema));

  RML_CHECK_THROWS(
      SerializationError,
      robot_middleware::serialization::deserialize_frame<ImuMsg>(frame,
                                                                  "/other"));
}

}  // namespace

// 序列化测试入口。
int main(int argc, char** argv) {
  return rml_test::run(
      {
          {"roundtrip", roundtrip_all_message_types},
          {"golden_bytes", imu_golden_bytes_are_network_order},
          {"truncation", every_truncation_is_rejected},
          {"trailing_bytes", trailing_bytes_are_rejected},
          {"special_values", nan_and_infinity_preserve_bits},
          {"network_contract", network_frame_contract_is_checked},
      },
      argc, argv);
}
