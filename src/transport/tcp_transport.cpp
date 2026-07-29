#include "robot_middleware/transport/tcp_transport.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>
#include <utility>

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

// TCP 传输实现：处理连接建立、部分发送、半包重组、超时和断连。
namespace robot_middleware::transport {
namespace {

using Clock = std::chrono::steady_clock;

/// 将 errno 与操作名称组合为可诊断文本。
std::string errno_message(const char* operation, int error_number = errno) {
    std::ostringstream message;
    message << operation << ": " << std::strerror(error_number);
    return message.str();
}

/// 将绝对 deadline 换算为 poll 毫秒超时，向上取整避免提前返回。
int poll_timeout_ms(Clock::time_point deadline) noexcept {
    const auto now = Clock::now();
    if (now >= deadline) {
        return 0;
    }
    const auto remaining = deadline - now;
    const auto milliseconds =
        std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
    std::int64_t value = milliseconds.count();
    if (milliseconds < remaining) {
        ++value;
    }
    return static_cast<int>(std::min<std::int64_t>(
        value, std::numeric_limits<int>::max()));
}

/// 等待指定 Socket 事件；返回 revents，超时返回 std::nullopt。
std::optional<short> poll_socket(
    int socket_fd,
    short events,
    Clock::time_point deadline) {
    while (true) {
        pollfd descriptor{};
        descriptor.fd = socket_fd;
        descriptor.events = events;
        const int result = ::poll(&descriptor, 1, poll_timeout_ms(deadline));
        if (result == 0) {
            return std::nullopt;
        }
        if (result < 0) {
            if (errno == EINTR) {
                if (Clock::now() >= deadline) {
                    return std::nullopt;
                }
                continue;
            }
            throw TransportError(errno_message("poll TCP socket"));
        }
        if ((descriptor.revents & POLLNVAL) != 0) {
            throw TransportError("poll reported an invalid TCP socket");
        }
        if ((descriptor.revents & (events | POLLERR | POLLHUP)) != 0) {
            return descriptor.revents;
        }
    }
}

/// 查询监听 Socket 的实际端口。
std::uint16_t socket_local_port(int socket_fd) {
    sockaddr_storage address{};
    socklen_t address_length = sizeof(address);
    if (::getsockname(
            socket_fd,
            reinterpret_cast<sockaddr*>(&address),
            &address_length) != 0) {
        throw TransportError(errno_message("getsockname"));
    }
    if (address.ss_family == AF_INET) {
        const auto* ipv4 = reinterpret_cast<const sockaddr_in*>(&address);
        return ntohs(ipv4->sin_port);
    }
    if (address.ss_family == AF_INET6) {
        const auto* ipv6 = reinterpret_cast<const sockaddr_in6*>(&address);
        return ntohs(ipv6->sin6_port);
    }
    throw TransportError("TCP socket has an unsupported address family");
}

/// 为 accept 返回的 Socket 设置 close-on-exec，防止子进程意外继承。
void set_close_on_exec(int socket_fd) {
    const int descriptor_flags = ::fcntl(socket_fd, F_GETFD, 0);
    if (descriptor_flags < 0 ||
        ::fcntl(socket_fd, F_SETFD, descriptor_flags | FD_CLOEXEC) < 0) {
        throw TransportError(errno_message("set FD_CLOEXEC"));
    }
}

}  // namespace

/// 使用已连接 Socket 构造连接，并为固定头部预留接收缓冲区。
TcpConnection::TcpConnection(int socket_fd, FrameLimits limits)
    : socket_fd_(socket_fd), limits_(std::move(limits)) {
    receive_buffer_.reserve(Frame::kHeaderSize);
}

TcpConnection::~TcpConnection() {
    close_noexcept();
}

/// 移动构造转移 Socket 与未完成帧缓冲区。
TcpConnection::TcpConnection(TcpConnection&& other) noexcept
    : socket_fd_(std::exchange(other.socket_fd_, -1)),
      limits_(std::move(other.limits_)),
      receive_buffer_(std::move(other.receive_buffer_)) {}

/// 移动赋值前关闭旧连接，再接管新连接状态。
TcpConnection& TcpConnection::operator=(TcpConnection&& other) noexcept {
    if (this != &other) {
        close_noexcept();
        socket_fd_ = std::exchange(other.socket_fd_, -1);
        limits_ = std::move(other.limits_);
        receive_buffer_ = std::move(other.receive_buffer_);
    }
    return *this;
}

/// 查询底层描述符状态。
bool TcpConnection::is_open() const {
    std::lock_guard<std::mutex> lock(descriptor_mutex_);
    return socket_fd_ >= 0;
}

/// 暴露原生描述符，主要供分片/粘包测试和高级系统集成使用。
int TcpConnection::native_handle() const {
    std::lock_guard<std::mutex> lock(descriptor_mutex_);
    return socket_fd_;
}

/// shutdown 不等待 send/receive mutex，可立即唤醒被内核阻塞的并发 I/O。
void TcpConnection::interrupt() noexcept {
    try {
        std::lock_guard<std::mutex> lock(descriptor_mutex_);
        if (socket_fd_ >= 0) {
            static_cast<void>(::shutdown(socket_fd_, SHUT_RDWR));
        }
    } catch (...) {
    }
}

/// 配置 SO_SNDTIMEO；超时后 send 返回 EAGAIN/EWOULDBLOCK，由上层触发断线处理。
void TcpConnection::set_send_timeout(std::chrono::milliseconds timeout) {
    if (timeout <= std::chrono::milliseconds::zero()) {
        throw TransportError("TCP send timeout must be positive");
    }
    const auto seconds =
        std::chrono::duration_cast<std::chrono::seconds>(timeout);
    const auto remainder =
        std::chrono::duration_cast<std::chrono::microseconds>(
            timeout - seconds);
    timeval value{};
    value.tv_sec = static_cast<decltype(value.tv_sec)>(seconds.count());
    value.tv_usec = static_cast<decltype(value.tv_usec)>(remainder.count());

    std::lock_guard<std::mutex> lock(descriptor_mutex_);
    if (socket_fd_ < 0) {
        throw TransportError("TCP connection is closed");
    }
    if (::setsockopt(socket_fd_, SOL_SOCKET, SO_SNDTIMEO, &value,
                     sizeof(value)) != 0) {
        throw TransportError(errno_message("set TCP send timeout"));
    }
}

/// 同时锁住发送和接收方向后关闭，避免与并发 I/O 交叉。
void TcpConnection::close() noexcept {
    std::scoped_lock lock(send_mutex_, receive_mutex_);
    close_noexcept();
    receive_buffer_.clear();
}

/// 编码完整 Frame 后调用 send_all。
void TcpConnection::send_frame(const Frame& frame) {
    const auto encoded = encode_frame(frame, limits_);
    send_all(encoded.data(), encoded.size());
}

/// 循环 send 直到发送完整缓冲区；使用 MSG_NOSIGNAL 避免 SIGPIPE 终止进程。
void TcpConnection::send_all(
    const std::uint8_t* data,
    std::size_t size) {
    if (data == nullptr && size != 0U) {
        throw TransportError("TCP send data pointer is null");
    }
    std::lock_guard<std::mutex> lock(send_mutex_);
    int socket_fd = -1;
    {
        std::lock_guard<std::mutex> descriptor_lock(descriptor_mutex_);
        socket_fd = socket_fd_;
        if (socket_fd < 0) {
            throw TransportError("TCP connection is closed");
        }
    }

    std::size_t offset = 0U;
    while (offset < size) {
        const std::size_t chunk_size = std::min<std::size_t>(
            size - offset,
            static_cast<std::size_t>(std::numeric_limits<ssize_t>::max()));
        const ssize_t sent = ::send(
            socket_fd,
            data + offset,
            chunk_size,
            MSG_NOSIGNAL);
        if (sent > 0) {
            offset += static_cast<std::size_t>(sent);
            continue;
        }
        if (sent == 0) {
            throw TransportError("TCP send returned zero before completion");
        }
        if (errno == EINTR) {
            continue;
        }
        throw TransportError(errno_message("send TCP socket"));
    }
}

/// 按“固定头部→安全总长度→剩余正文”两阶段读取并重组 Frame。
std::optional<Frame> TcpConnection::receive_frame(
    std::chrono::milliseconds timeout) {
    std::lock_guard<std::mutex> lock(receive_mutex_);
    int socket_fd = -1;
    {
        std::lock_guard<std::mutex> descriptor_lock(descriptor_mutex_);
        socket_fd = socket_fd_;
        if (socket_fd < 0) {
            throw TransportError("TCP connection is closed");
        }
    }
    if (timeout < std::chrono::milliseconds::zero()) {
        timeout = std::chrono::milliseconds::zero();
    }
    const auto deadline = Clock::now() + timeout;

    // 只读取到 target_size，不多读下一帧，因此天然支持连续粘连帧。
    const auto read_until = [this, deadline, socket_fd](std::size_t target_size) {
        std::array<std::uint8_t, 8192U> chunk{};
        while (receive_buffer_.size() < target_size) {
            const auto readiness = poll_socket(socket_fd, POLLIN, deadline);
            if (!readiness.has_value()) {
                return false;
            }
            const std::size_t wanted = std::min(
                chunk.size(), target_size - receive_buffer_.size());
            const ssize_t received = ::recv(
                socket_fd, chunk.data(), wanted, MSG_DONTWAIT);
            if (received > 0) {
                receive_buffer_.insert(
                    receive_buffer_.end(),
                    chunk.begin(),
                    chunk.begin() + received);
                continue;
            }
            if (received == 0) {
                interrupt();
                throw TransportError(
                    "TCP peer disconnected while receiving a frame");
            }
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                if (Clock::now() >= deadline) {
                    return false;
                }
                continue;
            }
            const int receive_error = errno;
            interrupt();
            throw TransportError(
                errno_message("receive TCP frame", receive_error));
        }
        return true;
    };

    // 先拿到完整头部，检查长度上限后才读取/扩展正文，避免恶意大分配。
    if (!read_until(Frame::kHeaderSize)) {
        return std::nullopt;
    }

    std::size_t total_size = 0U;
    try {
        total_size = frame_size_from_header(
            receive_buffer_.data(), Frame::kHeaderSize, limits_);
    } catch (...) {
        receive_buffer_.clear();
        interrupt();
        throw;
    }

    if (!read_until(total_size)) {
        return std::nullopt;
    }

    try {
        Frame frame = decode_frame(receive_buffer_, limits_);
        receive_buffer_.clear();
        return frame;
    } catch (...) {
        // 已消费完整声明帧；即使 CRC 或正文校验失败，下次调用仍保持帧边界对齐。
        receive_buffer_.clear();
        throw;
    }
}

/// 无异常关闭连接 Socket。
void TcpConnection::close_noexcept() noexcept {
    try {
        std::lock_guard<std::mutex> lock(descriptor_mutex_);
        if (socket_fd_ >= 0) {
            ::close(socket_fd_);
            socket_fd_ = -1;
        }
    } catch (...) {
    }
}

/// 解析本地地址，创建非阻塞监听 Socket 并依次尝试 bind/listen。
TcpServer::TcpServer(
    const std::string& bind_address,
    std::uint16_t port,
    TcpServerOptions options)
    : options_(std::move(options)) {
    if (options_.backlog <= 0) {
        throw TransportError("TCP listen backlog must be positive");
    }

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_PASSIVE;

    addrinfo* raw_addresses = nullptr;
    const std::string service = std::to_string(port);
    const char* node = bind_address.empty() ? nullptr : bind_address.c_str();
    const int resolve_result =
        ::getaddrinfo(node, service.c_str(), &hints, &raw_addresses);
    if (resolve_result != 0) {
        throw TransportError(
            std::string("getaddrinfo for TCP bind: ") +
            ::gai_strerror(resolve_result));
    }
    const std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> addresses(
        raw_addresses, &::freeaddrinfo);

    int last_error = 0;
    for (const addrinfo* address = addresses.get(); address != nullptr;
         address = address->ai_next) {
        const int candidate = ::socket(
            address->ai_family,
            address->ai_socktype | SOCK_CLOEXEC | SOCK_NONBLOCK,
            address->ai_protocol);
        if (candidate < 0) {
            last_error = errno;
            continue;
        }
        const int reuse_address = 1;
        static_cast<void>(::setsockopt(
            candidate,
            SOL_SOCKET,
            SO_REUSEADDR,
            &reuse_address,
            sizeof(reuse_address)));
        if (::bind(candidate, address->ai_addr, address->ai_addrlen) != 0) {
            last_error = errno;
            ::close(candidate);
            continue;
        }
        if (::listen(candidate, options_.backlog) != 0) {
            last_error = errno;
            ::close(candidate);
            continue;
        }
        socket_fd_ = candidate;
        break;
    }

    if (socket_fd_ < 0) {
        if (last_error != 0) {
            throw TransportError(errno_message("bind/listen TCP socket", last_error));
        }
        throw TransportError("could not create a TCP server socket");
    }
}

/// 绑定任意地址的委托构造。
TcpServer::TcpServer(
    std::uint16_t port,
    TcpServerOptions options)
    : TcpServer(std::string{}, port, std::move(options)) {}

TcpServer::~TcpServer() {
    close_noexcept();
}

/// 移动构造转移监听 Socket。
TcpServer::TcpServer(TcpServer&& other) noexcept
    : socket_fd_(std::exchange(other.socket_fd_, -1)),
      options_(std::move(other.options_)) {}

/// 移动赋值替换监听 Socket。
TcpServer& TcpServer::operator=(TcpServer&& other) noexcept {
    if (this != &other) {
        close_noexcept();
        socket_fd_ = std::exchange(other.socket_fd_, -1);
        options_ = std::move(other.options_);
    }
    return *this;
}

/// poll 等待连接并 accept；瞬时 EINTR/EAGAIN 会在 deadline 内重试。
std::optional<TcpConnection> TcpServer::accept(
    std::chrono::milliseconds timeout) {
    if (socket_fd_ < 0) {
        throw TransportError("TCP server is closed");
    }
    if (timeout < std::chrono::milliseconds::zero()) {
        timeout = std::chrono::milliseconds::zero();
    }
    const auto deadline = Clock::now() + timeout;

    while (true) {
        const auto readiness = poll_socket(socket_fd_, POLLIN, deadline);
        if (!readiness.has_value()) {
            return std::nullopt;
        }

        const int client_socket = ::accept(socket_fd_, nullptr, nullptr);
        if (client_socket >= 0) {
            try {
                set_close_on_exec(client_socket);
            } catch (...) {
                ::close(client_socket);
                throw;
            }
            return TcpConnection(client_socket, options_.frame_limits);
        }
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            if (Clock::now() >= deadline) {
                return std::nullopt;
            }
            continue;
        }
        throw TransportError(errno_message("accept TCP connection"));
    }
}

/// 查询实际监听端口。
std::uint16_t TcpServer::local_port() const {
    if (socket_fd_ < 0) {
        throw TransportError("TCP server is closed");
    }
    return socket_local_port(socket_fd_);
}

/// 无异常关闭监听 Socket。
void TcpServer::close_noexcept() noexcept {
    if (socket_fd_ >= 0) {
        ::close(socket_fd_);
        socket_fd_ = -1;
    }
}

/// 保存客户端 endpoint 并校验主机名。
TcpClient::TcpClient(
    std::string host,
    std::uint16_t port,
    TcpClientOptions options)
    : host_(std::move(host)), port_(port), options_(std::move(options)) {
    if (host_.empty()) {
        throw TransportError("TCP destination host must not be empty");
    }
}

/// 使用非阻塞 connect + poll 实现有上限的连接等待，再恢复阻塞模式。
TcpConnection TcpClient::connect(std::chrono::milliseconds timeout) const {
    if (timeout < std::chrono::milliseconds::zero()) {
        timeout = std::chrono::milliseconds::zero();
    }
    const auto deadline = Clock::now() + timeout;

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    addrinfo* raw_addresses = nullptr;
    const std::string service = std::to_string(port_);
    const int resolve_result = ::getaddrinfo(
        host_.c_str(), service.c_str(), &hints, &raw_addresses);
    if (resolve_result != 0) {
        throw TransportError(
            std::string("getaddrinfo for TCP destination: ") +
            ::gai_strerror(resolve_result));
    }
    const std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> addresses(
        raw_addresses, &::freeaddrinfo);

    int last_error = 0;
    bool timed_out = false;
    for (const addrinfo* address = addresses.get(); address != nullptr;
         address = address->ai_next) {
        const int candidate = ::socket(
            address->ai_family,
            address->ai_socktype | SOCK_CLOEXEC,
            address->ai_protocol);
        if (candidate < 0) {
            last_error = errno;
            continue;
        }

        const int original_flags = ::fcntl(candidate, F_GETFL, 0);
        if (original_flags < 0 ||
            ::fcntl(candidate, F_SETFL, original_flags | O_NONBLOCK) < 0) {
            last_error = errno;
            ::close(candidate);
            continue;
        }

        int connect_result =
            ::connect(candidate, address->ai_addr, address->ai_addrlen);
        if (connect_result < 0 &&
            errno != EINPROGRESS && errno != EALREADY && errno != EINTR) {
            last_error = errno;
            ::close(candidate);
            continue;
        }

        if (connect_result < 0) {
            const auto readiness = poll_socket(candidate, POLLOUT, deadline);
            if (!readiness.has_value()) {
                timed_out = true;
                ::close(candidate);
                break;
            }
            int socket_error = 0;
            socklen_t error_length = sizeof(socket_error);
            if (::getsockopt(
                    candidate,
                    SOL_SOCKET,
                    SO_ERROR,
                    &socket_error,
                    &error_length) != 0) {
                last_error = errno;
                ::close(candidate);
                continue;
            }
            if (socket_error != 0) {
                last_error = socket_error;
                ::close(candidate);
                if (Clock::now() >= deadline) {
                    timed_out = true;
                    break;
                }
                continue;
            }
        }

        if (::fcntl(candidate, F_SETFL, original_flags) != 0) {
            const int restore_error = errno;
            ::close(candidate);
            throw TransportError(
                errno_message("restore TCP socket blocking mode", restore_error));
        }
        return TcpConnection(candidate, options_.frame_limits);
    }

    if (timed_out) {
        throw TransportError("TCP connect timed out");
    }
    if (last_error != 0) {
        throw TransportError(errno_message("connect TCP socket", last_error));
    }
    throw TransportError("could not create a TCP client socket");
}

}  // namespace robot_middleware::transport
