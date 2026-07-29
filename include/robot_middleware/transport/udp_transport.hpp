#pragma once

#include "robot_middleware/transport/frame.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include <sys/socket.h>

namespace robot_middleware::transport {

/// UDP 收发配置；默认 1400 字节用于降低 IP 分片风险。
struct UdpTransportOptions {
    std::size_t max_datagram_size{1400U};
    FrameLimits frame_limits{};
};

/// 面向固定远端地址的 UDP 帧发送器，负责 Socket RAII 管理。
class UdpSender final {
public:
    /// 解析目标主机并创建 UDP Socket。
    UdpSender(
        const std::string& host,
        std::uint16_t port,
        UdpTransportOptions options = {});
    ~UdpSender();

    UdpSender(const UdpSender&) = delete;
    UdpSender& operator=(const UdpSender&) = delete;
    UdpSender(UdpSender&& other) noexcept;
    UdpSender& operator=(UdpSender&& other) noexcept;

    /// 编码并发送一个完整 Frame，一个 Frame 对应一个 Datagram。
    void send_frame(const Frame& frame);
    /// 发送已经编码好的原始 Datagram。
    void send_datagram(const std::uint8_t* data, std::size_t size);

private:
    /// 析构和移动赋值共用的无异常关闭函数。
    void close_noexcept() noexcept;

    int socket_fd_{-1};
    sockaddr_storage destination_{};
    socklen_t destination_length_{0U};
    UdpTransportOptions options_{};
};

/// UDP 帧接收器，支持绑定临时端口、poll 超时和截断检测。
class UdpReceiver final {
public:
    /// 绑定指定地址和端口；port=0 时由内核分配临时端口。
    UdpReceiver(
        const std::string& bind_address,
        std::uint16_t port,
        UdpTransportOptions options = {});
    /// 绑定任意本地地址的便捷构造函数。
    explicit UdpReceiver(
        std::uint16_t port,
        UdpTransportOptions options = {});
    ~UdpReceiver();

    UdpReceiver(const UdpReceiver&) = delete;
    UdpReceiver& operator=(const UdpReceiver&) = delete;
    UdpReceiver(UdpReceiver&& other) noexcept;
    UdpReceiver& operator=(UdpReceiver&& other) noexcept;

    /// 在超时时间内接收并校验一个完整 Frame；超时返回 std::nullopt。
    std::optional<Frame> receive_frame(std::chrono::milliseconds timeout);
    /// 查询实际绑定的本地端口。
    std::uint16_t local_port() const;

private:
    /// 无异常释放 Socket。
    void close_noexcept() noexcept;

    int socket_fd_{-1};
    UdpTransportOptions options_{};
};

}  // namespace robot_middleware::transport
