#pragma once

#include <cstdint>
#include <string_view>
#include <type_traits>

namespace robot_middleware {

// 本文件定义中间件内置消息及其稳定的类型元数据。
// 消息结构体的内存布局不直接作为线协议；跨进程传输必须经过 MessageCodec
// 逐字段编码。 修改字段时应引入新 schema（从而产生新的
// schema_hash），避免静默破坏协议兼容性。

/// IMU 示例消息：携带采样时间、偏航角和 Z 轴角速度。
struct ImuMsg {
  std::uint64_t timestamp_ns{0};
  double yaw{0.0};
  double gyro_z{0.0};
};

/// 位姿示例消息：位置采用米，姿态采用四元数。
struct PoseMsg {
  std::uint64_t timestamp_ns{0};
  double x{0.0};
  double y{0.0};
  double z{0.0};
  double qx{0.0};
  double qy{0.0};
  double qz{0.0};
  double qw{1.0};
};

/// 控制示例消息：描述平面移动底盘的速度指令。
struct ControlMsg {
  std::uint64_t timestamp_ns{0};
  double linear_x{0.0};
  double linear_y{0.0};
  double angular_z{0.0};
};

namespace detail {

/// 对规范化 schema 字符串计算 FNV-1a 64 位哈希，作为消息 schema 的稳定标识。
/// 与 typeid(T).hash_code() 不同，该结果可在不同进程和不同构建之间保持稳定。
constexpr std::uint64_t fnv1a_64(std::string_view text) noexcept {
  std::uint64_t hash = UINT64_C(14695981039346656037);
  for (const char ch : text) {
    hash ^= static_cast<std::uint8_t>(ch);
    hash *= UINT64_C(1099511628211);
  }
  return hash;
}

} // namespace detail

template <typename T> struct MessageTraits;

/// ImuMsg 的稳定类型编号、名称和字段描述。
template <> struct MessageTraits<ImuMsg> {
  static constexpr std::uint32_t type_id = UINT32_C(0x00010001);
  static constexpr std::string_view name = "robot_middleware/ImuMsg";
  static constexpr std::string_view schema =
      "uint64 timestamp_ns;float64 yaw;float64 gyro_z";
  static constexpr std::uint64_t schema_hash = detail::fnv1a_64(schema);
};

/// PoseMsg 的稳定类型编号、名称和字段描述。
template <> struct MessageTraits<PoseMsg> {
  static constexpr std::uint32_t type_id = UINT32_C(0x00010002);
  static constexpr std::string_view name = "robot_middleware/PoseMsg";
  static constexpr std::string_view schema =
      "uint64 timestamp_ns;float64 x;float64 y;float64 z;float64 qx;"
      "float64 qy;float64 qz;float64 qw";
  static constexpr std::uint64_t schema_hash = detail::fnv1a_64(schema);
};

/// ControlMsg 的稳定类型编号、名称和字段描述。
template <> struct MessageTraits<ControlMsg> {
  static constexpr std::uint32_t type_id = UINT32_C(0x00010003);
  static constexpr std::string_view name = "robot_middleware/ControlMsg";
  static constexpr std::string_view schema =
      "uint64 timestamp_ns;float64 linear_x;float64 linear_y;float64 angular_z";
  static constexpr std::uint64_t schema_hash = detail::fnv1a_64(schema);
};

template <typename T, typename = void> struct IsMessage : std::false_type {};

/// 编译期检测类型是否提供了完整的 MessageTraits 特化。
template <typename T>
struct IsMessage<T, std::void_t<decltype(MessageTraits<T>::type_id),
                                decltype(MessageTraits<T>::schema_hash),
                                decltype(MessageTraits<T>::name)>>
    : std::true_type {};

template <typename T> inline constexpr bool is_message_v = IsMessage<T>::value;

} // namespace robot_middleware
