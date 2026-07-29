#pragma once

#include "robot_middleware/transport/frame.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace robot_middleware::transport {

class TcpServer;
class TcpClient;

/// 已建立的 TCP 连接，负责帧发送、流式重组和 Socket 生命周期。
class TcpConnection final {
public:
    ~TcpConnection();

    TcpConnection(const TcpConnection&) = delete;
    TcpConnection& operator=(const TcpConnection&) = delete;
    TcpConnection(TcpConnection&& other) noexcept;
    TcpConnection& operator=(TcpConnection&& other) noexcept;

    /// 查询连接是否仍持有有效 Socket。
    bool is_open() const;
    /// 返回底层文件描述符，仅用于高级测试或系统集成。
    int native_handle() const;
    /// 使用 shutdown 唤醒并发阻塞 I/O，但保留 fd 供持有 I/O 锁的线程安全收尾。
    void interrupt() noexcept;
    /// 设置内核发送超时，防止不读数据的对端无限阻塞生命周期线程。
    void set_send_timeout(std::chrono::milliseconds timeout);
    /// 线程安全地关闭连接并清空未完成接收缓冲区。
    void close() noexcept;

    /// 编码并发送完整 Frame；并发发送会被串行化，避免帧字节交叉。
    void send_frame(const Frame& frame);
    /// 循环处理 partial send 和 EINTR，直到全部字节发送完成。
    void send_all(const std::uint8_t* data, std::size_t size);

    /// 接收并重组一个完整 Frame。
    /// 超时返回 std::nullopt；半帧会保留到下次调用，断连和坏帧会抛出异常。
    std::optional<Frame> receive_frame(std::chrono::milliseconds timeout);

private:
    friend class TcpServer;
    friend class TcpClient;

    /// 仅允许 Server/Client 用已建立的 Socket 创建连接对象。
    explicit TcpConnection(int socket_fd, FrameLimits limits);
    /// 无异常关闭底层 Socket。
    void close_noexcept() noexcept;

    int socket_fd_{-1};
    FrameLimits limits_{};
    std::vector<std::uint8_t> receive_buffer_;
    std::mutex send_mutex_;
    std::mutex receive_mutex_;
    mutable std::mutex descriptor_mutex_;
};

/// TCP 监听端配置。
struct TcpServerOptions {
    int backlog{128};
    FrameLimits frame_limits{};
};

/// TCP 监听服务器，支持临时端口和带超时的 accept。
class TcpServer final {
public:
    /// 绑定指定地址并开始监听。
    TcpServer(
        const std::string& bind_address,
        std::uint16_t port,
        TcpServerOptions options = {});
    /// 绑定任意本地地址的便捷构造函数。
    explicit TcpServer(
        std::uint16_t port,
        TcpServerOptions options = {});
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;
    TcpServer(TcpServer&& other) noexcept;
    TcpServer& operator=(TcpServer&& other) noexcept;

    /// 等待一个客户端连接；超时返回 std::nullopt。
    std::optional<TcpConnection> accept(std::chrono::milliseconds timeout);
    /// 查询实际监听端口。
    std::uint16_t local_port() const;

private:
    /// 无异常关闭监听 Socket。
    void close_noexcept() noexcept;

    int socket_fd_{-1};
    TcpServerOptions options_{};
};

/// TCP 客户端配置。
struct TcpClientOptions {
    FrameLimits frame_limits{};
};

/// 保存远端 endpoint 的 TCP 客户端工厂。
class TcpClient final {
public:
    /// 创建远端连接配置，但尚不发起网络连接。
    TcpClient(
        std::string host,
        std::uint16_t port,
        TcpClientOptions options = {});

    /// 使用非阻塞 connect 和 poll 建立连接；失败或超时抛出 TransportError。
    TcpConnection connect(std::chrono::milliseconds timeout) const;

private:
    std::string host_;
    std::uint16_t port_{0U};
    TcpClientOptions options_{};
};

}  // namespace robot_middleware::transport
