#include "robot_middleware/transport/frame.hpp"

#include <algorithm>
#include <limits>
#include <sstream>

// 版本 1 网络帧的编码、解码与边界校验实现。
namespace robot_middleware::transport {
namespace {

// 固定 56 字节头部中各字段的字节偏移。
constexpr std::size_t kMagicOffset = 0U;
constexpr std::size_t kVersionOffset = 4U;
constexpr std::size_t kFlagsOffset = 5U;
constexpr std::size_t kHeaderSizeOffset = 6U;
constexpr std::size_t kTopicSizeOffset = 8U;
constexpr std::size_t kReservedOffset = 10U;
constexpr std::size_t kPayloadSizeOffset = 12U;
constexpr std::size_t kTypeIdOffset = 16U;
constexpr std::size_t kSchemaHashOffset = 20U;
constexpr std::size_t kSequenceOffset = 28U;
constexpr std::size_t kPublisherIdOffset = 36U;
constexpr std::size_t kSendTimeOffset = 44U;
constexpr std::size_t kCrcOffset = 52U;

/// 非空输入必须提供有效指针。
void require_pointer(const std::uint8_t* data, std::size_t size) {
    if (data == nullptr && size != 0U) {
        throw TransportError("frame data pointer is null");
    }
}

/// 以下函数以大端顺序写入固定宽度整数。
void write_u16(std::uint8_t* out, std::uint16_t value) noexcept {
    out[0] = static_cast<std::uint8_t>((value >> 8U) & 0xFFU);
    out[1] = static_cast<std::uint8_t>(value & 0xFFU);
}

void write_u32(std::uint8_t* out, std::uint32_t value) noexcept {
    out[0] = static_cast<std::uint8_t>((value >> 24U) & 0xFFU);
    out[1] = static_cast<std::uint8_t>((value >> 16U) & 0xFFU);
    out[2] = static_cast<std::uint8_t>((value >> 8U) & 0xFFU);
    out[3] = static_cast<std::uint8_t>(value & 0xFFU);
}

void write_u64(std::uint8_t* out, std::uint64_t value) noexcept {
    for (std::size_t i = 0U; i < 8U; ++i) {
        const auto shift = static_cast<unsigned>((7U - i) * 8U);
        out[i] = static_cast<std::uint8_t>((value >> shift) & 0xFFU);
    }
}

/// 以下函数从大端字节区读取固定宽度整数。
std::uint16_t read_u16(const std::uint8_t* in) noexcept {
    return static_cast<std::uint16_t>(
        (static_cast<std::uint16_t>(in[0]) << 8U) |
        static_cast<std::uint16_t>(in[1]));
}

std::uint32_t read_u32(const std::uint8_t* in) noexcept {
    return (static_cast<std::uint32_t>(in[0]) << 24U) |
           (static_cast<std::uint32_t>(in[1]) << 16U) |
           (static_cast<std::uint32_t>(in[2]) << 8U) |
           static_cast<std::uint32_t>(in[3]);
}

std::uint64_t read_u64(const std::uint8_t* in) noexcept {
    std::uint64_t result = 0U;
    for (std::size_t i = 0U; i < 8U; ++i) {
        result = (result << 8U) | static_cast<std::uint64_t>(in[i]);
    }
    return result;
}

/// 使用标准多项式 0xEDB88320 计算 CRC32。
std::uint32_t crc32(const std::uint8_t* data, std::size_t size) noexcept {
    std::uint32_t crc = 0xFFFFFFFFU;
    for (std::size_t i = 0U; i < size; ++i) {
        crc ^= static_cast<std::uint32_t>(data[i]);
        for (unsigned bit = 0U; bit < 8U; ++bit) {
            const std::uint32_t mask = 0U - (crc & 1U);
            crc = (crc >> 1U) ^ (0xEDB88320U & mask);
        }
    }
    return ~crc;
}

/// 校验 Topic、Payload 和总帧长度上限，并防止 size_t 加法溢出。
std::size_t checked_frame_size(
    std::size_t topic_size,
    std::size_t payload_size,
    const FrameLimits& limits) {
    if (topic_size > std::numeric_limits<std::uint16_t>::max()) {
        throw TransportError("topic is too large for the wire format");
    }
    if (payload_size > std::numeric_limits<std::uint32_t>::max()) {
        throw TransportError("payload is too large for the wire format");
    }
    if (topic_size > limits.max_topic_size) {
        throw TransportError("topic exceeds configured size limit");
    }
    if (payload_size > limits.max_payload_size) {
        throw TransportError("payload exceeds configured size limit");
    }

    constexpr std::size_t header_size = Frame::kHeaderSize;
    if (topic_size > std::numeric_limits<std::size_t>::max() - header_size ||
        payload_size >
            std::numeric_limits<std::size_t>::max() - header_size - topic_size) {
        throw TransportError("declared frame size overflows size_t");
    }
    const std::size_t total_size = header_size + topic_size + payload_size;
    if (total_size > limits.max_frame_size) {
        throw TransportError("frame exceeds configured size limit");
    }
    return total_size;
}

}  // namespace

/// 只解析固定头部，供 TCP 在分配和读取正文前获知安全总长度。
std::size_t frame_size_from_header(
    const std::uint8_t* data,
    std::size_t size,
    const FrameLimits& limits) {
    require_pointer(data, size);
    if (size < Frame::kHeaderSize) {
        throw TransportError("truncated frame header");
    }
    if (read_u32(data + kMagicOffset) != Frame::kMagic) {
        throw TransportError("invalid frame magic");
    }
    if (data[kVersionOffset] != Frame::kVersion) {
        throw TransportError("unsupported frame version");
    }
    if (read_u16(data + kHeaderSizeOffset) != Frame::kHeaderSize) {
        throw TransportError("invalid frame header size");
    }
    if (read_u16(data + kReservedOffset) != 0U) {
        throw TransportError("reserved frame header field must be zero");
    }

    const std::size_t topic_size = read_u16(data + kTopicSizeOffset);
    const std::size_t payload_size = read_u32(data + kPayloadSizeOffset);
    return checked_frame_size(topic_size, payload_size, limits);
}

/// 构造固定头部、复制 Topic/Payload，并写入正文 CRC32。
std::vector<std::uint8_t> encode_frame(
    const Frame& frame,
    const FrameLimits& limits) {
    const std::size_t total_size =
        checked_frame_size(frame.topic.size(), frame.payload.size(), limits);
    std::vector<std::uint8_t> encoded(total_size, 0U);

    write_u32(encoded.data() + kMagicOffset, Frame::kMagic);
    encoded[kVersionOffset] = Frame::kVersion;
    encoded[kFlagsOffset] = frame.flags;
    write_u16(encoded.data() + kHeaderSizeOffset, Frame::kHeaderSize);
    write_u16(
        encoded.data() + kTopicSizeOffset,
        static_cast<std::uint16_t>(frame.topic.size()));
    write_u16(encoded.data() + kReservedOffset, 0U);
    write_u32(
        encoded.data() + kPayloadSizeOffset,
        static_cast<std::uint32_t>(frame.payload.size()));
    write_u32(encoded.data() + kTypeIdOffset, frame.type_id);
    write_u64(encoded.data() + kSchemaHashOffset, frame.schema_hash);
    write_u64(encoded.data() + kSequenceOffset, frame.sequence);
    write_u64(encoded.data() + kPublisherIdOffset, frame.publisher_id);
    write_u64(encoded.data() + kSendTimeOffset, frame.send_time_ns);

    auto body = encoded.begin() + static_cast<std::ptrdiff_t>(Frame::kHeaderSize);
    body = std::copy(frame.topic.begin(), frame.topic.end(), body);
    std::copy(frame.payload.begin(), frame.payload.end(), body);

    const auto checksum = crc32(
        encoded.data() + Frame::kHeaderSize,
        total_size - Frame::kHeaderSize);
    write_u32(encoded.data() + kCrcOffset, checksum);
    return encoded;
}

/// 对完整帧执行精确长度、CRC 和字段校验后恢复内存对象。
Frame decode_frame(
    const std::uint8_t* data,
    std::size_t size,
    const FrameLimits& limits) {
    require_pointer(data, size);
    const std::size_t declared_size = frame_size_from_header(data, size, limits);
    if (size != declared_size) {
        std::ostringstream message;
        message << "frame length mismatch: declared " << declared_size
                << " bytes, received " << size;
        throw TransportError(message.str());
    }

    const std::uint32_t expected_crc = read_u32(data + kCrcOffset);
    const std::uint32_t actual_crc =
        crc32(data + Frame::kHeaderSize, size - Frame::kHeaderSize);
    if (actual_crc != expected_crc) {
        throw TransportError("frame CRC32 mismatch");
    }

    const std::size_t topic_size = read_u16(data + kTopicSizeOffset);
    const std::size_t payload_size = read_u32(data + kPayloadSizeOffset);
    const auto* topic_begin = data + Frame::kHeaderSize;
    const auto* payload_begin = topic_begin + topic_size;

    Frame frame;
    frame.flags = data[kFlagsOffset];
    frame.type_id = read_u32(data + kTypeIdOffset);
    frame.schema_hash = read_u64(data + kSchemaHashOffset);
    frame.sequence = read_u64(data + kSequenceOffset);
    frame.publisher_id = read_u64(data + kPublisherIdOffset);
    frame.send_time_ns = read_u64(data + kSendTimeOffset);
    frame.topic.assign(
        reinterpret_cast<const char*>(topic_begin),
        reinterpret_cast<const char*>(payload_begin));
    frame.payload.assign(payload_begin, payload_begin + payload_size);
    return frame;
}

}  // namespace robot_middleware::transport
