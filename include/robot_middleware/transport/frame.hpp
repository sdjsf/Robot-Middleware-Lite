#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace robot_middleware::transport {

/// 传输层协议、Socket 或帧校验失败时抛出的统一异常。
class TransportError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/// 解码前使用的资源上限，防止恶意长度触发超大内存分配。
struct FrameLimits {
    std::size_t max_topic_size{65'535U};
    std::size_t max_payload_size{16U * 1024U * 1024U};
    std::size_t max_frame_size{56U + 65'535U + 16U * 1024U * 1024U};
};

/// Robot Middleware Lite 版本 1 的内存帧表示。
/// 固定头部在线上占 56 字节，Topic 和 Payload 紧随其后。
struct Frame {
    static constexpr std::uint32_t kMagic = 0x524D574CU;  // "RMWL"
    static constexpr std::uint8_t kVersion = 1U;
    static constexpr std::uint16_t kHeaderSize = 56U;

    std::uint8_t flags{0U};
    std::uint32_t type_id{0U};
    std::uint64_t schema_hash{0U};
    std::uint64_t sequence{0U};
    std::uint64_t publisher_id{0U};
    std::uint64_t send_time_ns{0U};
    std::string topic;
    std::vector<std::uint8_t> payload;
};

/// 校验固定头部并返回帧声明的完整长度。
/// 调用时至少需要提供 56 字节；本函数只检查头部和长度上限，不检查正文与 CRC。
std::size_t frame_size_from_header(
    const std::uint8_t* data,
    std::size_t size,
    const FrameLimits& limits = {});

/// 将 Frame 编码为大端线格式并计算 Topic+Payload 的 CRC32。
std::vector<std::uint8_t> encode_frame(
    const Frame& frame,
    const FrameLimits& limits = {});

/// 从完整字节区解码帧，严格校验版本、长度和 CRC32。
Frame decode_frame(
    const std::uint8_t* data,
    std::size_t size,
    const FrameLimits& limits = {});

/// ByteBuffer 形式的便捷解码重载。
inline Frame decode_frame(
    const std::vector<std::uint8_t>& data,
    const FrameLimits& limits = {}) {
    return decode_frame(data.data(), data.size(), limits);
}

}  // namespace robot_middleware::transport
