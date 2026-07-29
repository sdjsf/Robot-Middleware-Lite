#include "robot_middleware/transport/udp_transport.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <sstream>
#include <utility>
#include <vector>

#include <netdb.h>
#include <poll.h>
#include <unistd.h>

// UDP 传输实现：一个中间件 Frame 对应一个 Datagram，不进行应用层分片。
namespace robot_middleware::transport {
namespace {

using Clock = std::chrono::steady_clock;

/// 将 errno 转换为带操作上下文的异常文本。
std::string errno_message(const char* operation, int error_number = errno) {
    std::ostringstream message;
    message << operation << ": " << std::strerror(error_number);
    return message.str();
}

/// 校验 UDP 配置及 IPv4 最大负载上限。
void validate_options(const UdpTransportOptions& options) {
    constexpr std::size_t kMaximumUdpPayload = 65'507U;
    if (options.max_datagram_size < Frame::kHeaderSize) {
        throw TransportError(
            "UDP maximum datagram size must fit the 56-byte frame header");
    }
    if (options.max_datagram_size > kMaximumUdpPayload) {
        throw TransportError(
            "UDP maximum datagram size exceeds the IPv4 UDP payload limit");
    }
    if (options.max_datagram_size >
        static_cast<std::size_t>(std::numeric_limits<ssize_t>::max())) {
        throw TransportError("UDP maximum datagram size exceeds ssize_t");
    }
}

/// 将绝对 deadline 转换为 poll 使用的向上取整毫秒值。
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

/// 等待 UDP Socket 可读，并处理 EINTR、错误和绝对超时。
bool wait_readable(int socket_fd, Clock::time_point deadline) {
    while (true) {
        pollfd descriptor{};
        descriptor.fd = socket_fd;
        descriptor.events = POLLIN;
        const int result = ::poll(&descriptor, 1, poll_timeout_ms(deadline));
        if (result == 0) {
            return false;
        }
        if (result < 0) {
            if (errno == EINTR) {
                if (Clock::now() >= deadline) {
                    return false;
                }
                continue;
            }
            throw TransportError(errno_message("poll UDP socket"));
        }
        if ((descriptor.revents & POLLNVAL) != 0) {
            throw TransportError("poll reported an invalid UDP socket");
        }
        if ((descriptor.revents & POLLERR) != 0) {
            int socket_error = 0;
            socklen_t error_length = sizeof(socket_error);
            if (::getsockopt(
                    socket_fd,
                    SOL_SOCKET,
                    SO_ERROR,
                    &socket_error,
                    &error_length) == 0 &&
                socket_error != 0) {
                throw TransportError(
                    errno_message("UDP socket error", socket_error));
            }
            throw TransportError("poll reported a UDP socket error");
        }
        if ((descriptor.revents & POLLIN) != 0) {
            return true;
        }
    }
}

/// 通过 getsockname 查询 IPv4/IPv6 Socket 的实际本地端口。
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
    throw TransportError("UDP socket has an unsupported address family");
}

}  // namespace

/// 解析目标 endpoint 并选择第一个可用地址创建发送 Socket。
UdpSender::UdpSender(
    const std::string& host,
    std::uint16_t port,
    UdpTransportOptions options)
    : options_(std::move(options)) {
    validate_options(options_);
    if (host.empty()) {
        throw TransportError("UDP destination host must not be empty");
    }

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = IPPROTO_UDP;

    addrinfo* addresses = nullptr;
    const std::string service = std::to_string(port);
    const int resolve_result =
        ::getaddrinfo(host.c_str(), service.c_str(), &hints, &addresses);
    if (resolve_result != 0) {
        throw TransportError(
            std::string("getaddrinfo for UDP destination: ") +
            ::gai_strerror(resolve_result));
    }

    for (const addrinfo* address = addresses; address != nullptr;
         address = address->ai_next) {
        if (address->ai_addrlen > sizeof(destination_)) {
            continue;
        }
        const int candidate = ::socket(
            address->ai_family,
            address->ai_socktype | SOCK_CLOEXEC,
            address->ai_protocol);
        if (candidate < 0) {
            continue;
        }
        socket_fd_ = candidate;
        std::memcpy(&destination_, address->ai_addr, address->ai_addrlen);
        destination_length_ = static_cast<socklen_t>(address->ai_addrlen);
        break;
    }
    ::freeaddrinfo(addresses);

    if (socket_fd_ < 0) {
        throw TransportError("could not create a UDP sender socket");
    }
}

UdpSender::~UdpSender() {
    close_noexcept();
}

/// 移动构造时转移文件描述符所有权。
UdpSender::UdpSender(UdpSender&& other) noexcept
    : socket_fd_(std::exchange(other.socket_fd_, -1)),
      destination_(other.destination_),
      destination_length_(other.destination_length_),
      options_(std::move(other.options_)) {
    other.destination_length_ = 0U;
}

/// 移动赋值前先关闭当前 Socket，再接管来源对象。
UdpSender& UdpSender::operator=(UdpSender&& other) noexcept {
    if (this != &other) {
        close_noexcept();
        socket_fd_ = std::exchange(other.socket_fd_, -1);
        destination_ = other.destination_;
        destination_length_ = other.destination_length_;
        options_ = std::move(other.options_);
        other.destination_length_ = 0U;
    }
    return *this;
}

/// 先按统一协议编码，再发送单个 Datagram。
void UdpSender::send_frame(const Frame& frame) {
    const auto encoded = encode_frame(frame, options_.frame_limits);
    send_datagram(encoded.data(), encoded.size());
}

/// 使用 sendto 原子发送 Datagram；EINTR 时重试，部分发送视为错误。
void UdpSender::send_datagram(
    const std::uint8_t* data,
    std::size_t size) {
    if (socket_fd_ < 0) {
        throw TransportError("UDP sender is closed");
    }
    if (data == nullptr && size != 0U) {
        throw TransportError("UDP datagram data pointer is null");
    }
    if (size > options_.max_datagram_size) {
        throw TransportError("UDP datagram exceeds configured maximum size");
    }

    while (true) {
        const ssize_t sent = ::sendto(
            socket_fd_,
            data,
            size,
            MSG_NOSIGNAL,
            reinterpret_cast<const sockaddr*>(&destination_),
            destination_length_);
        if (sent < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw TransportError(errno_message("sendto UDP socket"));
        }
        if (static_cast<std::size_t>(sent) != size) {
            throw TransportError("sendto performed a partial UDP datagram send");
        }
        return;
    }
}

/// 释放发送 Socket，供析构和移动赋值复用。
void UdpSender::close_noexcept() noexcept {
    if (socket_fd_ >= 0) {
        ::close(socket_fd_);
        socket_fd_ = -1;
    }
}

/// 解析绑定地址、创建 Socket，并设置 SO_REUSEADDR 后完成 bind。
UdpReceiver::UdpReceiver(
    const std::string& bind_address,
    std::uint16_t port,
    UdpTransportOptions options)
    : options_(std::move(options)) {
    validate_options(options_);

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = IPPROTO_UDP;
    hints.ai_flags = AI_PASSIVE;

    addrinfo* addresses = nullptr;
    const std::string service = std::to_string(port);
    const char* node = bind_address.empty() ? nullptr : bind_address.c_str();
    const int resolve_result =
        ::getaddrinfo(node, service.c_str(), &hints, &addresses);
    if (resolve_result != 0) {
        throw TransportError(
            std::string("getaddrinfo for UDP bind: ") +
            ::gai_strerror(resolve_result));
    }

    int last_error = 0;
    for (const addrinfo* address = addresses; address != nullptr;
         address = address->ai_next) {
        const int candidate = ::socket(
            address->ai_family,
            address->ai_socktype | SOCK_CLOEXEC,
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
        if (::bind(candidate, address->ai_addr, address->ai_addrlen) == 0) {
            socket_fd_ = candidate;
            break;
        }
        last_error = errno;
        ::close(candidate);
    }
    ::freeaddrinfo(addresses);

    if (socket_fd_ < 0) {
        if (last_error != 0) {
            throw TransportError(errno_message("bind UDP socket", last_error));
        }
        throw TransportError("could not create a UDP receiver socket");
    }
}

/// 绑定任意本地地址的委托构造。
UdpReceiver::UdpReceiver(
    std::uint16_t port,
    UdpTransportOptions options)
    : UdpReceiver(std::string{}, port, std::move(options)) {}

UdpReceiver::~UdpReceiver() {
    close_noexcept();
}

/// 移动构造转移接收 Socket 所有权。
UdpReceiver::UdpReceiver(UdpReceiver&& other) noexcept
    : socket_fd_(std::exchange(other.socket_fd_, -1)),
      options_(std::move(other.options_)) {}

/// 移动赋值安全替换当前接收 Socket。
UdpReceiver& UdpReceiver::operator=(UdpReceiver&& other) noexcept {
    if (this != &other) {
        close_noexcept();
        socket_fd_ = std::exchange(other.socket_fd_, -1);
        options_ = std::move(other.options_);
    }
    return *this;
}

/// 使用 poll+recvmsg 接收 Datagram，并通过 MSG_TRUNC 拒绝截断数据。
std::optional<Frame> UdpReceiver::receive_frame(
    std::chrono::milliseconds timeout) {
    if (socket_fd_ < 0) {
        throw TransportError("UDP receiver is closed");
    }
    if (timeout < std::chrono::milliseconds::zero()) {
        timeout = std::chrono::milliseconds::zero();
    }
    const auto deadline = Clock::now() + timeout;
    std::vector<std::uint8_t> buffer(options_.max_datagram_size);

    while (wait_readable(socket_fd_, deadline)) {
        iovec vector{};
        vector.iov_base = buffer.data();
        vector.iov_len = buffer.size();
        msghdr message{};
        message.msg_iov = &vector;
        message.msg_iovlen = 1U;

        const ssize_t received =
            ::recvmsg(socket_fd_, &message, MSG_DONTWAIT);
        if (received < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                if (Clock::now() >= deadline) {
                    return std::nullopt;
                }
                continue;
            }
            throw TransportError(errno_message("recvmsg UDP socket"));
        }
        if ((message.msg_flags & MSG_TRUNC) != 0) {
            throw TransportError(
                "received UDP datagram exceeds configured maximum size");
        }
        return decode_frame(
            buffer.data(),
            static_cast<std::size_t>(received),
            options_.frame_limits);
    }
    return std::nullopt;
}

/// 查询 bind 后的实际端口，支持测试使用 port=0。
std::uint16_t UdpReceiver::local_port() const {
    if (socket_fd_ < 0) {
        throw TransportError("UDP receiver is closed");
    }
    return socket_local_port(socket_fd_);
}

/// 无异常释放接收 Socket。
void UdpReceiver::close_noexcept() noexcept {
    if (socket_fd_ >= 0) {
        ::close(socket_fd_);
        socket_fd_ = -1;
    }
}

}  // namespace robot_middleware::transport
