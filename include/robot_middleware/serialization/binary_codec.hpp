#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace robot_middleware {
namespace serialization {

/// 序列化模块统一使用的字节缓冲区。
using ByteBuffer = std::vector<std::uint8_t>;

/// 编解码输入不合法、越界或协议不匹配时抛出的异常。
class SerializationError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

/// 按网络字节序向连续缓冲区写入基础类型。
/// 浮点数先保留 IEEE-754 位模式，再按无符号整数编码。
class BinaryWriter {
 public:
  /// 写入 8 位无符号整数。
  void write_u8(std::uint8_t value) { buffer_.push_back(value); }

  /// 写入大端 16/32/64 位无符号整数。
  void write_u16(std::uint16_t value) { write_unsigned(value); }
  void write_u32(std::uint32_t value) { write_unsigned(value); }
  void write_u64(std::uint64_t value) { write_unsigned(value); }

  /// 写入 IEEE-754 64 位 double。
  void write_double(double value) {
    static_assert(sizeof(double) == sizeof(std::uint64_t) &&
                      std::numeric_limits<double>::is_iec559,
                  "Robot Middleware Lite requires IEEE-754 64-bit doubles");
    std::uint64_t bits = 0;
    std::memcpy(&bits, &value, sizeof(bits));
    write_u64(bits);
  }

  /// 追加一段原始字节；非零长度时 data 不能为空。
  void write_bytes(const std::uint8_t* data, std::size_t size) {
    if (size == 0) {
      return;
    }
    if (data == nullptr) {
      throw SerializationError("cannot write bytes from a null pointer");
    }
    buffer_.insert(buffer_.end(), data, data + size);
  }

  /// 只读访问当前缓冲区。
  const ByteBuffer& buffer() const noexcept { return buffer_; }
  /// 转移缓冲区所有权，避免最终结果再次复制。
  ByteBuffer take_buffer() noexcept { return std::move(buffer_); }

 private:
  /// 大端编码任意固定宽度无符号整数。
  template <typename UInt>
  void write_unsigned(UInt value) {
    static_assert(std::is_unsigned<UInt>::value, "UInt must be unsigned");
    for (std::size_t i = 0; i < sizeof(UInt); ++i) {
      const auto shift = static_cast<unsigned>((sizeof(UInt) - 1U - i) * 8U);
      buffer_.push_back(static_cast<std::uint8_t>((value >> shift) & 0xffU));
    }
  }

  ByteBuffer buffer_;
};

/// 从只读字节区按网络字节序解析基础类型。
/// 每次读取前都会检查剩余长度，禁止越界访问。
class BinaryReader {
 public:
  /// 使用裸字节区创建读取器。
  BinaryReader(const std::uint8_t* data, std::size_t size)
      : data_(data), size_(size) {
    if (size_ != 0 && data_ == nullptr) {
      throw SerializationError("cannot read bytes from a null pointer");
    }
  }

  /// 使用 ByteBuffer 创建读取器。
  explicit BinaryReader(const ByteBuffer& buffer)
      : BinaryReader(buffer.data(), buffer.size()) {}

  /// 读取 8 位无符号整数。
  std::uint8_t read_u8() {
    require(1);
    return data_[offset_++];
  }

  /// 读取大端 16/32/64 位无符号整数。
  std::uint16_t read_u16() { return read_unsigned<std::uint16_t>(); }
  std::uint32_t read_u32() { return read_unsigned<std::uint32_t>(); }
  std::uint64_t read_u64() { return read_unsigned<std::uint64_t>(); }

  /// 读取并恢复 IEEE-754 64 位 double。
  double read_double() {
    const std::uint64_t bits = read_u64();
    double value = 0.0;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
  }

  /// 读取指定数量的原始字节。
  ByteBuffer read_bytes(std::size_t count) {
    if (count == 0) {
      return {};
    }
    require(count);
    ByteBuffer result(data_ + offset_, data_ + offset_ + count);
    offset_ += count;
    return result;
  }

  /// 返回当前未消费字节的首地址。
  const std::uint8_t* current_data() const noexcept {
    return data_ == nullptr ? nullptr : data_ + offset_;
  }
  /// 返回剩余字节数和当前偏移。
  std::size_t remaining() const noexcept { return size_ - offset_; }
  std::size_t offset() const noexcept { return offset_; }
  bool empty() const noexcept { return remaining() == 0; }

  /// 确认负载已被精确消费，拒绝尾随数据。
  void require_consumed() const {
    if (!empty()) {
      throw SerializationError("message payload contains trailing bytes");
    }
  }

 private:
  /// 检查接下来 count 个字节是否可读。
  void require(std::size_t count) const {
    if (count > size_ - offset_) {
      throw SerializationError("truncated message payload at byte " +
                               std::to_string(offset_));
    }
  }

  /// 按大端顺序读取任意固定宽度无符号整数。
  template <typename UInt>
  UInt read_unsigned() {
    static_assert(std::is_unsigned<UInt>::value, "UInt must be unsigned");
    require(sizeof(UInt));
    UInt value = 0;
    for (std::size_t i = 0; i < sizeof(UInt); ++i) {
      value = static_cast<UInt>((value << 8U) | data_[offset_++]);
    }
    return value;
  }

  const std::uint8_t* data_{nullptr};
  std::size_t size_{0};
  std::size_t offset_{0};
};

}  // namespace serialization
}  // namespace robot_middleware
