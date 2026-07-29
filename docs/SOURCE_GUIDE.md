# Robot Middleware Lite 详细源码手册

基于 Linux Socket 与 C++17 多线程实现的轻量级机器人通信中间件。

项目面向机器人控制器、传感器和执行器等模块之间的通信需求，提供进程内强类型
发布订阅、TCP/UDP 跨进程传输、统一二进制消息协议、多线程异步处理、心跳检测、
异常重连和多客户端连接管理，并通过机器人节点模拟程序测试通信延迟、吞吐量、
丢包情况和资源占用。

当前版本为 `v0.1.0` MVP，运行平台为 Linux，不依赖 ROS、DDS 或第三方网络库。

## 阅读导航

1. [项目概述](#1-项目概述)
2. [Linux Socket、TCP/UDP 与二进制协议](#2-基于-linux-socket-api-封装-tcpudp并设计统一二进制协议)
3. [线程池与多线程通信架构](#3-基于线程池和生产者消费者模型的多线程通信架构)
4. [消息队列、心跳重连与多客户端](#4-消息队列序列化心跳重连与多客户端处理)
5. [机器人节点模拟与性能测试](#5-机器人节点模拟与通信性能测试)
6. [构建、运行与验证](#6-构建运行与验证)
7. [发布订阅 API](#7-发布订阅-api-示例)
8. [工程结构](#8-工程结构)
9. [能力边界与后续路线](#9-并发生命周期与能力边界)
10. [项目描述与代码对应关系](#10-项目描述与代码对应关系)

---

## 1. 项目概述

### 1.1 项目背景

一个机器人系统通常同时运行多个功能模块：

- 传感器模块持续产生 IMU、位姿等高频状态数据；
- 控制模块消费传感器数据并生成速度、转向等控制指令；
- 执行模块接收控制指令并驱动底盘或执行机构；
- 状态管理模块需要检测节点是否在线、连接是否中断以及消息是否丢失。

如果业务代码直接调用 `send()`、`recv()`，网络阻塞、消息解析、线程同步和异常恢复会与
控制逻辑耦合。因此，本项目将系统拆分为消息层、调度层、协议层、传输层和桥接层，
让业务模块通过统一 Topic 接口通信。

### 1.2 项目目标

项目围绕以下四项核心能力展开：

1. 基于 Linux Socket API 封装 TCP/UDP 通信模块，并设计统一二进制消息协议；
2. 基于线程池和生产者—消费者模型实现网络、缓存与业务处理解耦；
3. 实现消息队列、序列化、心跳检测、异常重连、多客户端和异步消息处理；
4. 搭建机器人节点模拟环境，测试不同频率和负载下的延迟、吞吐量与 CPU 占用。

### 1.3 总体分层

```text
┌─────────────────────────────────────────────────────────────┐
│ 机器人业务层                                                │
│ IMU Node        Control Node        Actuator Node            │
└───────────────┬───────────────────┬─────────────────────────┘
                │ Publisher<T>      │ Subscription<T>
┌───────────────▼───────────────────▼─────────────────────────┐
│ 进程内通信层：Runtime / Node / TopicBus                     │
│ 类型校验、Topic 路由、每订阅有界邮箱、远端来源标记          │
└───────────────────────────┬─────────────────────────────────┘
                            │ callback task
┌───────────────────────────▼─────────────────────────────────┐
│ 并发调度层：Executor / ThreadPool / ThreadSafeQueue          │
│ 生产者—消费者、回调串行性、跨订阅并行、过载保护             │
└───────────────────────────┬─────────────────────────────────┘
                            │ typed message
┌───────────────────────────▼─────────────────────────────────┐
│ 序列化与协议层：MessageTraits / MessageCodec / Frame / CRC32 │
│ 强类型消息 → 稳定字节序列 → 统一二进制网络帧               │
└───────────────────────────┬─────────────────────────────────┘
                            │ Frame
┌───────────────────────────▼─────────────────────────────────┐
│ 网络与桥接层：UDP / TCP / NetworkBridge / SessionManager    │
│ Socket 收发、半包粘包、多客户端、心跳、重连、Topic 导入导出 │
└─────────────────────────────────────────────────────────────┘
```

### 1.4 技术栈

| 分类 | 使用技术 | 项目中的作用 |
|---|---|---|
| 开发语言 | C++17 | 强类型消息、RAII、模板化 API、线程与资源管理 |
| 操作系统 | Linux | Socket API、`poll()`、`getrusage()`、`/proc` 指标 |
| 网络通信 | TCP / UDP | 可靠控制链路与低开销传感器数据报传输 |
| 并发编程 | `std::thread`、`mutex`、`condition_variable` | 网络线程、线程池、队列同步和安全关闭 |
| 架构模式 | Pub/Sub、生产者—消费者 | 解耦消息生产、缓存、网络 I/O 与业务处理 |
| 构建系统 | CMake 3.16+ | 库、Demo、Benchmark、CTest 和安装包构建 |
| 质量保障 | CTest、ASan、UBSan、TSan | 功能、协议、网络和并发回归检查 |

> 当前网络等待机制使用 `poll()` 实现超时和可读/可写检测，TCP 服务端采用
> “一个 accept 线程 + 每客户端一个会话线程”。`epoll Reactor` 是后续扩展方向，
> 当前代码没有把它作为已实现能力。

---

## 2. 基于 Linux Socket API 封装 TCP/UDP，并设计统一二进制协议

这一部分对应项目描述：

> 基于 Linux Socket API 封装 TCP/UDP 通信模块，设计二进制消息协议，实现机器人控制指令、
> 传感器数据和状态信息的统一传输。

### 2.1 设计目标

- 屏蔽原始文件描述符、地址转换和系统调用细节；
- TCP 与 UDP 使用相同的上层 `Frame`；
- 明确处理 TCP 半包、粘包和 partial send；
- 检测 UDP 数据报截断；
- 在反序列化前完成长度、版本、类型和 CRC 校验；
- 不直接传输 C++ 结构体内存，避免填充、端序和 ABI 差异。

### 2.2 传输层结构

```text
业务消息
  ├── ImuMsg       传感器数据
  ├── PoseMsg      机器人状态
  └── ControlMsg   控制指令
        │
        ▼
MessageTraits<T>       类型编号与 schema
        │
        ▼
MessageCodec<T>        逐字段大端序编码
        │
        ▼
Frame                  统一消息头、Topic、Payload、CRC32
        │
        ├── UdpSender / UdpReceiver
        └── TcpClient / TcpServer / TcpConnection
```

对应源码：

| 层次 | 代码位置 | 主要职责 |
|---|---|---|
| 消息定义 | `include/robot_middleware/core/message.hpp` | 定义 IMU、Pose、Control 消息及类型信息 |
| 字段编解码 | `include/robot_middleware/serialization/message_codec.hpp`、`src/serialization/message_codec.cpp` | 声明 Codec 扩展点并实现内置消息逐字段编解码 |
| 消息与帧转换 | `include/robot_middleware/serialization/network_message.hpp` | `make_frame()`、`deserialize_frame()` |
| 帧协议 | `include/robot_middleware/transport/frame.hpp`、`src/transport/frame.cpp` | 定义 Frame，并实现 56 字节帧头、长度校验和 CRC32 |
| UDP 封装 | `include/robot_middleware/transport/udp_transport.hpp`、`src/transport/udp_transport.cpp` | Datagram 发送、接收、超时和截断检测 |
| TCP 封装 | `include/robot_middleware/transport/tcp_transport.hpp`、`src/transport/tcp_transport.cpp` | 连接、监听、完整帧收发、半包粘包处理 |

#### 2.2.1 `message.hpp`：定义“系统能够识别什么消息”

文件位置：`include/robot_middleware/core/message.hpp`

这个文件位于通信链路的最上游，负责定义业务数据和消息的稳定类型身份，但不负责把消息
转换成字节，也不调用 Socket。

它包含四组功能。

**1. 定义机器人业务消息结构体**

```cpp
struct ImuMsg {
  std::uint64_t timestamp_ns;
  double yaw;
  double gyro_z;
};
```

- `ImuMsg` 表示传感器数据，包括采样时间、偏航角和 Z 轴角速度；
- `PoseMsg` 表示机器人位置和四元数姿态；
- `ControlMsg` 表示平面底盘的 X/Y 线速度和 Z 轴角速度指令。

这些结构体既可用于进程内 `Publisher<T>/Subscription<T>`，也可在序列化后用于跨进程传输。
结构体本身只是内存中的 C++ 对象，其内存布局不等于网络协议。

**2. 为消息分配稳定的类型信息**

每种消息都有一个 `MessageTraits<T>` 特化：

```cpp
template <>
struct MessageTraits<ImuMsg> {
  static constexpr std::uint32_t type_id = 0x00010001;
  static constexpr std::string_view name = "robot_middleware/ImuMsg";
  static constexpr std::string_view schema =
      "uint64 timestamp_ns;float64 yaw;float64 gyro_z";
  static constexpr std::uint64_t schema_hash = detail::fnv1a_64(schema);
};
```

各字段的作用：

| 元数据 | 作用 |
|---|---|
| `type_id` | 在线协议中的消息类型编号，用于区分 IMU、Pose 和 Control |
| `name` | 供诊断、日志和类型说明使用的稳定名称 |
| `schema` | 描述字段名称、类型和顺序的规范化字符串 |
| `schema_hash` | schema 的 FNV-1a 64 位哈希，用于发现同类型编号下的结构不兼容 |

发送端把 `type_id/schema_hash` 写入 Frame；接收端在反序列化前比较这两个值。如果发送端修改了
字段而接收端仍使用旧结构，就会因 schema 不一致而拒绝消息，而不是按照错误布局静默解析。

**3. 生成跨进程稳定的 schema 标识**

`detail::fnv1a_64()` 是一个 `constexpr` FNV-1a 哈希函数，可在编译期根据 schema 生成
`schema_hash`。它没有使用 `typeid(T).hash_code()`，因为后者不保证在不同进程、编译器和
构建之间保持一致。

**4. 在编译期判断一个类型是不是中间件消息**

`IsMessage<T>` 和 `is_message_v<T>` 使用模板检测 `MessageTraits<T>` 是否完整。后面的
`make_frame<T>()` 会通过 `static_assert` 限制参数，避免用户把没有注册类型元数据的普通
结构体直接放进网络帧。

该文件的输入与输出关系是：

```text
输入：消息字段设计
  ↓
message struct + MessageTraits<T>
  ↓
输出：可被 TopicBus 和序列化层识别的强类型消息
```

#### 2.2.2 `message_codec.hpp/.cpp`：定义“消息字段怎样变成 Payload”

文件位置：

- `include/robot_middleware/serialization/message_codec.hpp`
- `src/serialization/message_codec.cpp`
- 底层字节工具：`include/robot_middleware/serialization/binary_codec.hpp`

这一层只处理“消息对象 ↔ Payload 字节”，还没有添加 Topic、序号、CRC 或 TCP/UDP 信息。

**头文件 `message_codec.hpp` 的职责**

1. 声明 `MessageCodec<MessageT>` 扩展点；
2. 为 `ImuMsg/PoseMsg/ControlMsg` 声明 `encode()` 和 `decode()`；
3. 提供统一的 `serialize<T>()` 模板入口；
4. 提供统一的 `deserialize<T>()` 模板入口；
5. 解码结束后调用 `require_consumed()`，拒绝消息末尾多出的未知字节。

```text
serialize(message)
  → MessageCodec<MessageT>::encode(message)
  → ByteBuffer

deserialize<MessageT>(buffer)
  → 创建 BinaryReader
  → MessageCodec<MessageT>::decode(reader)
  → require_consumed()
  → MessageT
```

**实现文件 `message_codec.cpp` 的职责**

该文件按照 `message.hpp` 中的 schema 顺序逐字段编解码。例如 `ImuMsg`：

```text
timestamp_ns  → write_u64   → 8 字节
yaw           → write_double → 8 字节
gyro_z        → write_double → 8 字节
总 Payload                     24 字节
```

解码时必须使用完全相同的顺序：

```text
read_u64 → timestamp_ns
read_double → yaw
read_double → gyro_z
```

`PoseMsg` 编码 1 个 `uint64` 和 7 个 `double`，`ControlMsg` 编码 1 个 `uint64` 和
3 个 `double`。

**底层 `binary_codec.hpp` 的职责**

- `BinaryWriter` 把 `u8/u16/u32/u64` 写成大端网络字节序；
- `double` 先通过 `memcpy` 保留 IEEE-754 位模式，再作为 `uint64` 编码；
- `BinaryReader` 每次读取前检查剩余长度，避免越界访问；
- 输入被截断时抛出 `SerializationError`；
- `take_buffer()` 转移字节缓冲区，避免结果再复制一次。

这一层不直接复制结构体：

```cpp
// 项目没有采用这种方式
send(fd, &message, sizeof(message), ...);
```

因此可以避免结构体 padding、主机端序以及不同编译器 ABI 对网络协议造成影响。

#### 2.2.3 `network_message.hpp`：连接强类型消息与通用 Frame

文件位置：`include/robot_middleware/serialization/network_message.hpp`

这个文件是消息层和传输协议层之间的适配器。上游认识的是 `ImuMsg`，下游认识的是统一
`Frame`，该文件负责完成两者之间的转换。

**1. `monotonic_time_ns()`**

读取 `steady_clock` 并转换成纳秒，用作 `send_time_ns`。因为单调时钟不会受到系统时间校准
影响，所以适合本机不同进程之间的延迟测试；它不能直接用于未同步的跨机器单向延迟。

**2. `make_frame<MessageT>()`**

输入：

- Topic 名称；
- 强类型消息；
- 消息序号 `sequence`；
- 发布者实例编号 `publisher_id`；
- 可选发送时间戳。

处理步骤：

```text
static_assert 检查 MessageTraits
  → 检查 Topic 非空
  → 检查 Topic 能放入 uint16 长度字段
  → 从 MessageTraits 填入 type_id/schema_hash
  → 填入 sequence/publisher_id/send_time_ns/topic
  → serialize(message) 生成 payload
  → 返回内存 Frame
```

这里返回的还是 `Frame` 对象，不是最终网络字节。后续需要由 `encode_frame()` 生成完整线格式。

**3. `deserialize_frame<MessageT>()`**

接收端按照以下顺序检查：

1. 如果调用者给出 `expected_topic`，检查 Frame 的 Topic 是否匹配；
2. 比较 `type_id`，防止把 Control Frame 当成 ImuMsg；
3. 比较 `schema_hash`，防止新旧消息结构不兼容；
4. 调用 `deserialize<MessageT>(payload)` 恢复消息。

CRC、Magic、协议版本和帧长度不是在这里检查，而是在更下层的 `decode_frame()` 中检查。
因此职责边界是：

```text
decode_frame()             保证“这是一条结构完整的合法 Frame”
deserialize_frame<T>()     保证“这条 Frame 正是订阅者期望的消息”
```

#### 2.2.4 `frame.hpp/.cpp`：定义统一线协议和帧级校验

文件位置：

- `include/robot_middleware/transport/frame.hpp`
- `src/transport/frame.cpp`

这一层与具体消息类型解耦。无论 Payload 是 IMU、Pose 还是 Control，TCP 和 UDP 都只传输
同一种 `Frame`。

**头文件 `frame.hpp` 的职责**

1. 定义统一异常 `TransportError`；
2. 定义 `FrameLimits`，限制 Topic、Payload 和完整 Frame 大小；
3. 定义内存中的 `Frame` 对象；
4. 声明帧头解析、完整编码和完整解码接口。

`Frame` 中的重要字段：

| 字段 | 作用 |
|---|---|
| `kMagic = "RMWL"` | 快速判断是否为本项目协议 |
| `kVersion = 1` | 协议版本管理 |
| `kHeaderSize = 56` | 固定帧头长度 |
| `flags` | 标记普通数据帧或心跳帧 |
| `type_id/schema_hash` | 消息类型和字段结构校验 |
| `sequence/publisher_id` | 丢包、重复、乱序与去重 |
| `send_time_ns` | 延迟测量 |
| `topic` | 消息路由名称 |
| `payload` | `MessageCodec` 产生的业务字节 |

默认 `FrameLimits` 将 Payload 限制为 16 MiB。接收端会先检查声明长度，再分配或读取正文，
防止伪造超大长度造成不受控内存分配。

**实现文件 `frame.cpp` 的职责**

`encode_frame()`：

```text
检查 Topic/Payload/总长度
  → 分配完整连续缓冲区
  → 按固定偏移写入 56 字节大端帧头
  → 复制 Topic
  → 复制 Payload
  → 计算 Topic+Payload 的 CRC32
  → 把 CRC 写入帧头
```

`frame_size_from_header()`：

- 只要求已经收到 56 字节帧头；
- 检查 Magic、Version、HeaderSize 和保留字段；
- 读取 Topic/Payload 声明长度；
- 检查单项上限、总长度上限和 `size_t` 加法溢出；
- 返回 TCP 接下来需要收取的完整帧长度。

`decode_frame()`：

```text
frame_size_from_header()
  → 要求收到长度与声明长度完全一致
  → 重新计算并比较 CRC32
  → 提取所有帧头字段
  → 拆分 Topic 和 Payload
  → 返回 Frame
```

这一文件解决的是“字节流中的帧是否合法”，不判断 `/imu` 是否应该被某个具体订阅消费。

#### 2.2.5 `udp_transport.hpp/.cpp`：一条 Frame 对应一个 Datagram

文件位置：

- `include/robot_middleware/transport/udp_transport.hpp`
- `src/transport/udp_transport.cpp`

UDP 天然保留数据报边界，因此项目采用“一条完整 Frame 对应一个 UDP Datagram”的模型，
没有在该模块中实现应用层分片和重组。

**头文件 `udp_transport.hpp` 的职责**

- `UdpTransportOptions`
  - 默认最大 Datagram 为 1400 字节，降低 IP 分片概率；
  - 内含 `FrameLimits`，继续限制协议层资源使用。
- `UdpSender`
  - 保存目标地址和 Socket；
  - `send_frame()` 发送 Frame；
  - `send_datagram()` 支持已编码字节；
  - 禁止复制、允许移动，确保一个文件描述符只有一个所有者。
- `UdpReceiver`
  - 绑定本地地址和端口；
  - `receive_frame(timeout)` 带超时接收一条完整 Frame；
  - 超时返回 `std::nullopt`；
  - `local_port()` 支持查询 `port=0` 时由内核分配的临时端口。

**发送实现**

```text
UdpSender(host, port)
  → getaddrinfo() 解析 IPv4/IPv6 地址
  → socket(SOCK_DGRAM | SOCK_CLOEXEC)
  → 保存目标 sockaddr

send_frame(frame)
  → encode_frame()
  → 检查 Datagram 大小
  → sendto()
  → EINTR 时重试
  → 要求发送字节数等于完整 Datagram 长度
```

**接收实现**

```text
UdpReceiver(address, port)
  → getaddrinfo(AI_PASSIVE)
  → socket()
  → SO_REUSEADDR
  → bind()

receive_frame(timeout)
  → poll(POLLIN) 等待可读
  → recvmsg(MSG_DONTWAIT)
  → 检查 MSG_TRUNC
  → decode_frame()
  → 返回 Frame
```

使用 `recvmsg()` 而不是普通 `recv()` 的关键原因，是可以检查 `MSG_TRUNC`。如果发送的数据报
超过接收缓冲区，模块会明确报错，而不会把被截断的半条 Frame 交给上层。

当前边界：

- 没有 UDP 重传、确认和有序保证；
- 没有应用层分片，Frame 必须小于配置的 Datagram 上限；
- `poll()` 只等待当前接收 Socket，不是多连接 `epoll` Reactor；
- UDP Demo 已支持跨进程 Frame，但 UDP 尚未接入自动 Topic Bridge。

#### 2.2.6 `tcp_transport.hpp/.cpp`：在字节流上恢复完整 Frame

文件位置：

- `include/robot_middleware/transport/tcp_transport.hpp`
- `src/transport/tcp_transport.cpp`

TCP 不保留消息边界。一次 `recv()` 可能只收到半个 Frame，也可能底层已经连续到达多个
Frame，因此 TCP 模块的核心任务是从字节流中恢复一条条完整帧。

**头文件中的三个核心对象**

| 类 | 作用 |
|---|---|
| `TcpClient` | 保存远端 endpoint，并通过带超时的 `connect()` 创建连接 |
| `TcpServer` | 完成 `bind/listen`，通过带超时的 `accept()` 接收连接 |
| `TcpConnection` | 管理已建立 Socket，负责完整发送、流式接收、关闭和中断 |

`TcpConnection` 内部有三类同步状态：

- `send_mutex_`：多个线程同时发送时串行化完整 Frame，避免两个 Frame 的字节交叉；
- `receive_mutex_`：保证同一连接只有一个接收流程修改 `receive_buffer_`；
- `descriptor_mutex_`：保护文件描述符的查询、关闭和中断；
- `receive_buffer_`：超时时保留尚未收完整的半帧，下一次继续接收。

**客户端连接流程**

```text
TcpClient::connect(timeout)
  → getaddrinfo() 解析候选地址
  → socket(SOCK_CLOEXEC)
  → 临时设置 O_NONBLOCK
  → connect()
  → EINPROGRESS 时 poll(POLLOUT)
  → getsockopt(SO_ERROR) 获取真实连接结果
  → 恢复原阻塞模式
  → 返回 TcpConnection
```

非阻塞 `connect + poll` 让连接过程拥有明确超时，避免目标不可达时无限等待。

**服务端监听流程**

```text
TcpServer(address, port)
  → getaddrinfo(AI_PASSIVE)
  → socket(SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK)
  → SO_REUSEADDR
  → bind()
  → listen(backlog)

accept(timeout)
  → poll(POLLIN)
  → accept()
  → 为客户端 fd 设置 FD_CLOEXEC
  → 返回 TcpConnection
```

`TcpServer` 只完成单个监听 Socket 和单次 accept。多客户端会话线程、广播和心跳生命周期由
更上层的 `TcpSessionManager` 负责。

**完整发送和 partial send**

```text
send_frame(frame)
  → encode_frame()
  → send_all()
  → 获取 send_mutex_
  → 循环 send(data + offset)
  → 每次增加已发送 offset
  → 直到完整 Frame 全部发送
```

发送时使用 `MSG_NOSIGNAL`，避免对端关闭后 `SIGPIPE` 直接终止进程。`set_send_timeout()` 通过
`SO_SNDTIMEO` 防止对端长期不读取时发送线程永久阻塞。

**半包和粘包处理**

接收采用两阶段读取：

```text
第一阶段：读取固定 56 字节帧头
  → frame_size_from_header()
  → 安全得到 total_size

第二阶段：只读取到 total_size
  → 半帧：保存在 receive_buffer_，等待下次继续
  → 完整帧：decode_frame()
  → 返回一条 Frame
```

每次 `recv()` 请求的最大长度不会超过当前 Frame 剩余长度，因此即使内核缓冲区中已经粘连
了下一条 Frame，本次也只消费当前 Frame；下一次 `receive_frame()` 再处理下一条。

**关闭与异常处理**

- 对端返回 0：判定连接已关闭并抛出 `TransportError`；
- `EINTR/EAGAIN/EWOULDBLOCK`：在 deadline 内继续等待；
- 帧头非法：清空接收缓存、调用 `interrupt()` 并抛出异常；
- 完整帧 CRC 或正文校验失败：清空当前帧缓存并抛出异常，由上层会话管理决定是否关闭连接；
- `interrupt()` 调用 `shutdown(SHUT_RDWR)`，用于唤醒另一个线程中阻塞的 I/O；
- `close()` 同步发送和接收方向后关闭 fd，并清理未完成帧。

当前 TCP I/O 使用 `poll(&descriptor, 1, ...)` 等待单个 Socket。多客户端层采用
“accept 线程 + 每客户端一个会话线程”，当前没有实现 `epoll`。

#### 2.2.7 六层文件的完整调用关系

发送一条 IMU 的实际调用链：

```text
message.hpp
ImuMsg + MessageTraits<ImuMsg>
  ↓
message_codec.hpp / message_codec.cpp
serialize(imu) → 24 字节 Payload
  ↓
network_message.hpp
make_frame("/imu", imu, sequence, publisher_id)
  ↓
frame.cpp
encode_frame(frame) → 56 字节头部 + Topic + Payload
  ↓
udp_transport.cpp 或 tcp_transport.cpp
sendto() 或循环 send()
```

接收端执行相反过程：

```text
recvmsg() 或循环 recv()
  ↓
frame.cpp
decode_frame()：Magic/版本/长度/CRC
  ↓
network_message.hpp
deserialize_frame<ImuMsg>()：Topic/type_id/schema_hash
  ↓
message_codec.cpp
逐字段读取 Payload
  ↓
message.hpp
恢复 ImuMsg，交给业务层
```

可以用一句话区分每层：

| 文件 | 只负责回答的问题 |
|---|---|
| `message.hpp` | 这是什么机器人消息？ |
| `message_codec.hpp/.cpp` | 消息字段如何变成 Payload？ |
| `network_message.hpp` | Payload 如何带上 Topic 和类型元数据？ |
| `frame.hpp/.cpp` | Frame 如何变成可校验的统一线协议？ |
| `udp_transport.hpp/.cpp` | 如何用一个 Datagram 发送/接收一个 Frame？ |
| `tcp_transport.hpp/.cpp` | 如何从无边界字节流中恢复一个完整 Frame？ |

完整线协议和字段宽度见 [PROTOCOL.md](PROTOCOL.md)。

### 2.3 二进制帧分层

每条网络消息都被封装为统一 `Frame`：

```text
固定头部
├── magic / version       协议识别与版本检查
├── flags                 数据帧或心跳帧
├── topic_length          Topic 长度
├── payload_length        Payload 长度
├── type_id               消息类型编号
├── schema_hash           消息字段结构校验
├── sequence              发布序号
├── publisher_id          发布者实例编号
├── send_time_ns          发送时间戳
└── CRC32                 Topic+Payload 完整性校验

可变区域
├── topic bytes           例如 /imu、/cmd_vel
└── payload bytes         MessageCodec 编码结果
```

统一帧解决了三类消息的统一传输问题：

| 机器人数据 | Topic 示例 | 消息类型 | 典型传输方式 |
|---|---|---|---|
| IMU 传感器数据 | `/imu` | `ImuMsg` | UDP 或 TCP |
| 位姿/状态信息 | `/pose` | `PoseMsg` | UDP 或 TCP |
| 控制指令 | `/cmd_vel` | `ControlMsg` | TCP |

### 2.4 UDP 数据流

```text
传感器进程
ImuMsg
  → MessageCodec
  → make_frame()
  → UdpSender::send_frame()
  → sendto()

控制进程
recvmsg()
  → 截断检查
  → Frame 长度与 CRC 校验
  → type_id / schema_hash / Topic 校验
  → deserialize_frame<ImuMsg>()
```

UDP 保留消息边界，开销较小，适合允许少量丢包的高频状态流。项目通过
`publisher_id + sequence` 统计丢包、重复和乱序。

运行方式：

```bash
# 终端 A：控制端接收 1000 条消息，超时 5000 ms
./build/rml_udp_control 127.0.0.1 7400 1000 5000

# 终端 B：传感器端以 100 Hz 发送 1000 条 IMU
./build/rml_udp_sensor 127.0.0.1 7400 1000 100
```

入口：

- `demo/udp_sensor.cpp`：模拟高频 IMU 传感器进程；
- `demo/udp_control.cpp`：接收、校验并统计传输指标。

### 2.5 TCP 数据流

```text
控制客户端
ControlMsg
  → MessageCodec
  → Frame
  → TcpConnection::send_frame()
  → 循环 send，直到完整帧发送完成

控制服务端
poll() 等待可读
  → 先读取固定头部
  → 根据长度读取 Topic 与 Payload
  → 处理半包/粘包
  → 校验并反序列化 ControlMsg
```

TCP 提供有序可靠字节流，适合控制命令、配置和需要维持连接状态的消息。项目没有假设一次
`send()` 或 `recv()` 就能处理完整帧，而是按目标长度循环读写。

运行方式：

```bash
# 终端 A：服务端
./build/rml_tcp_control_server 7500 100

# 终端 B：客户端以 50 Hz 发送 100 条控制指令
./build/rml_tcp_control_client 127.0.0.1 7500 100 50
```

---

## 3. 基于线程池和生产者—消费者模型的多线程通信架构

这一部分对应项目描述：

> 设计多线程通信架构，采用生产者—消费者模型实现网络接收、消息缓存和业务处理解耦；
> 基于 mutex、condition_variable 实现线程同步与资源保护。

### 3.1 线程职责划分

项目没有让一个线程同时承担连接、收包、解析和业务计算，而是按职责分离：

| 线程类型 | 生产/消费内容 | 主要职责 |
|---|---|---|
| 业务发布线程 | 生产 Topic 消息 | 采集传感器数据或发布控制指令 |
| Runtime worker | 消费回调任务 | 执行订阅回调、Bridge 导出回调 |
| TCP accept 线程 | 生产客户端会话 | 持续接受新连接 |
| TCP 会话线程 | 生产完整 Frame | 接收一个客户端的数据、解析帧、处理心跳 |
| Bridge 网络线程 | 消费出站 Frame | TCP 发送、接收、心跳和断线重连 |

### 3.2 线程池结构

```text
多个任务生产者
├── Subscription A
├── Subscription B
└── Bridge export callback
          │ post(task)
          ▼
ThreadSafeQueue<std::function<void()>>
          │ wait_pop()
          ▼
┌─────────┬─────────┬─────────┬─────────┐
│ worker1 │ worker2 │ worker3 │ worker4 │
└─────────┴─────────┴─────────┴─────────┘
```

核心实现：

- `include/robot_middleware/core/thread_safe_queue.hpp`
  - 使用 `mutex` 保护内部队列；
  - 使用 `condition_variable` 等待“队列非空”或“队列有空间”；
  - 支持阻塞与非阻塞接口；
  - `close()` 后拒绝新任务、唤醒等待线程并允许排空已有任务。
- `include/robot_middleware/executor/thread_pool.hpp`
  - 维护固定数量 worker；
  - `post()` 提交普通任务；
  - `submit()` 返回 `future`；
  - 捕获任务异常，避免 worker 因异常退出。
- `src/executor/thread_pool.cpp`
  - worker 循环执行 `wait_pop() → task()`；
  - shutdown 关闭任务队列并安全 `join()`。

### 3.3 每订阅独立邮箱

TopicBus 不让多个订阅者竞争同一条消息。每个订阅都有自己的有界邮箱：

```text
Publisher 发布一条 /imu
          │ fan-out
          ├── Subscription A mailbox → drain A
          ├── Subscription B mailbox → drain B
          └── Bridge export mailbox  → drain Bridge
```

实现位于 `src/core/runtime.cpp`：

1. `enqueue()` 把消息加入该订阅的 `pending`；
2. 如果当前没有 drain 任务，则向 Executor 提交一个；
3. worker 执行 `drain()`；
4. `drain()` 按顺序取出消息，并在锁外调用用户回调。

该设计提供以下并发语义：

- 同一个 Subscription 的回调不会与自身并发，保持消息顺序；
- 不同 Subscription 可以由不同 worker 并行执行；
- 用户回调在内部锁之外运行，避免长时间占有队列锁；
- Publisher 只负责投递，不需要同步等待业务回调结束。

### 3.4 过载与背压

每个订阅可配置队列深度和溢出策略：

| 策略 | 行为 | 适用消息 |
|---|---|---|
| `DropOldest` | 丢弃最旧消息，保留最新状态 | IMU、Pose 等高频状态 |
| `RejectNewest` | 保留已有任务，拒绝新消息 | 必须按历史顺序处理的命令 |

队列记录 `enqueued`、`delivered`、`dropped_oldest`、`rejected_newest`、
`callback_errors` 等统计，便于发现业务处理能力不足。

### 3.5 多进程与多线程如何组合

项目中的“多进程”不是在库内调用 `fork()`，而是把传感器、控制器和 Bridge 编译成不同
可执行程序，由用户从多个终端分别启动。

以 TCP Bridge 为例：

```text
客户端进程
├── 主线程：模拟 IMU 发布
├── Runtime worker × 2：Topic 和序列化回调
└── Bridge 网络线程：TCP 发送、接收、重连

服务端进程
├── 主线程：状态等待与结果输出
├── Runtime worker × 4：本地业务回调
├── TCP accept 线程：接受客户端
├── 每客户端会话线程：接收和解析 Frame
└── Bridge 发送线程：异步广播出站 Frame
```

这样既验证了操作系统进程间 Socket 通信，也验证了单个进程内部的多线程并发处理。

---

## 4. 消息队列、序列化、心跳重连与多客户端处理

这一部分对应项目描述：

> 开发消息队列、序列化、心跳检测和异常重连模块，提高通信系统稳定性；
> 支持多客户端连接和异步消息处理。

### 4.1 消息队列

项目存在两级队列：

```text
第一级：Subscription mailbox
作用：隔离 Publisher 与业务回调

第二级：NetworkBridge outbound queue
作用：隔离 Runtime 回调与阻塞式网络发送
```

本地消息导出时，Runtime worker 只完成序列化并把 Frame 加入有界出站队列，不直接在业务
回调中执行阻塞 `send()`。Bridge 网络线程随后消费队列并发送。

```text
Runtime callback
  → serialize
  → outbound_.try_push(frame)
  → 立即返回

Bridge network worker
  → outbound_.try_pop()
  → TcpConnection::send_frame()
```

实现位于：

- `include/robot_middleware/bridge/network_bridge.hpp`
- `src/bridge/network_bridge.cpp`

### 4.2 序列化与类型安全

项目不使用以下不稳定方式：

```cpp
send(fd, &message, sizeof(message), ...);
```

原因是结构体可能包含编译器填充，主机端序也可能不同。项目采用逐字段编码：

```text
C++ message
  → MessageTraits<T>：稳定 type_id + schema_hash
  → MessageCodec<T>：固定字段顺序 + 大端序
  → Frame：统一元数据、Topic、Payload、CRC32
```

新增消息需要完成四步：

1. 定义普通 C++ 消息结构体；
2. 增加固定 `type_id`、规范化 `schema` 和 `schema_hash`；
3. 实现逐字段 `MessageCodec<T>`；
4. 增加 golden bytes、截断输入和 round-trip 测试。

### 4.3 心跳检测

TCP Bridge 和 SessionManager 会在空闲期间发送控制面心跳。

```text
连接建立
  → 周期性发送 heartbeat frame
  → 收到数据或心跳，刷新 last_receive
  → 超过 inactivity_timeout
  → 判定连接失活并关闭
```

心跳使用协议 `flags` 区分，不会被误投递为业务 Topic。畸形心跳或未知 flag 会作为协议错误
处理。

实现位置：

- `include/robot_middleware/transport/heartbeat.hpp`
- `src/bridge/network_bridge.cpp`
- `src/transport/session_manager.cpp`

### 4.4 异常重连

客户端 TCP 链路断开后由 Bridge 网络线程自动重连：

```text
连接失败或连接中断
  → 清理旧 TcpConnection
  → initial_backoff
  → 再次 connect
  → 连续失败时指数增加等待时间
  → 到 max_backoff 后不再继续增长
  → 连接成功后恢复初始退避
```

重连等待可被 `stop()` 中断，避免程序退出时长时间卡住。

当前可靠性边界：

- 已从队列取出但尚未成功写完的 Frame 不做磁盘持久化；
- 重连后不会自动重放所有历史消息；
- 当前语义是有界内存队列上的 at-most-once MVP，而不是可靠消息队列。

### 4.5 多客户端连接

服务端 `TcpSessionManager` 使用以下结构：

```text
TcpSessionManager
├── accept thread
│   └── 持续 accept 新连接
├── Session 1 worker
│   └── receive_frame / heartbeat / timeout
├── Session 2 worker
│   └── receive_frame / heartbeat / timeout
└── Session N worker
    └── receive_frame / heartbeat / timeout
```

支持能力：

- 限制最大客户端数量；
- 为每个连接分配唯一 SessionId；
- 向指定客户端发送 Frame；
- 向当前客户端广播 Frame；
- 统计 accepted、active、disconnected 和 transport errors；
- stop 时中断阻塞 I/O 并回收 accept 与会话线程。

当前模型适合中小规模机器人节点。大量连接场景下，后续可把每连接线程升级为
`epoll` I/O Reactor，再把完整 Frame 投递给现有业务线程池。

### 4.6 远端 Topic 导入导出

`NetworkBridge` 把本地强类型 Topic 和网络 Frame 连接起来。

客户端导出：

```cpp
robot_middleware::Runtime runtime(2);
auto node = runtime.create_node("sensor_bridge");

robot_middleware::bridge::NetworkBridge bridge(node);
bridge.export_topic<robot_middleware::ImuMsg>("/imu");
bridge.start_tcp_client("127.0.0.1", 7600);
```

服务端导入：

```cpp
robot_middleware::Runtime runtime(4);
auto node = runtime.create_node("control_bridge");

robot_middleware::bridge::NetworkBridge bridge(node);
bridge.import_topic<robot_middleware::ImuMsg>("/imu");

robot_middleware::transport::TcpSessionManager sessions(
    "127.0.0.1", 7600,
    [&](auto, const auto& frame) {
      bridge.receive_frame(frame);
    });

bridge.set_frame_sender([&](const auto& frame) {
  sessions.broadcast(frame);
});
sessions.start();
```

完整链路：

```text
客户端本地 Publisher
  → TopicBus
  → export_topic
  → serialize
  → outbound queue
  → TCP
  → 服务端 Session worker
  → NetworkBridge::receive_frame
  → deserialize
  → publish_remote
  → 服务端 TopicBus
  → Runtime ThreadPool
  → 业务 Subscription callback
```

远端消息带有来源标记，`export_topic` 默认只导出本地消息；同时使用
`(Topic, publisher_id, sequence)` 去重，避免重复帧和立即回环。

---

## 5. 机器人节点模拟与通信性能测试

这一部分对应项目描述：

> 搭建机器人节点模拟环境，对不同消息频率、负载条件下的通信延迟、吞吐量和 CPU 占用进行测试。

### 5.1 模拟场景

| 场景 | 进程结构 | 主要验证内容 |
|---|---|---|
| 进程内 IMU→控制→执行 | 单进程、多线程 | Topic 路由、线程池、两级业务回调 |
| UDP 传感器→控制器 | 双进程 | Datagram、丢包、乱序、延迟 |
| TCP 控制客户端→服务端 | 双进程 | 连接、完整帧、半包粘包、控制指令 |
| TCP Bridge 多客户端 | 三个及以上进程 | Topic 自动导入导出、多客户端、心跳与重连 |

### 5.2 进程内机器人链路

```text
IMU Publisher
  → /imu
  → Control Callback
  → Control Publisher
  → /cmd_vel
  → Actuator Callback
```

运行：

```bash
./build/rml_inprocess_demo
```

Demo 以 100 Hz 发布 200 条 IMU 消息，并输出控制回调、执行回调和队列丢弃数量。

### 5.3 多客户端 Bridge 场景

先启动服务端，等待总计 200 条 IMU：

```bash
./build/rml_bridge_server 7600 200
```

再启动两个独立传感器客户端：

```bash
./build/rml_bridge_client 127.0.0.1 7600 100 100
./build/rml_bridge_client 127.0.0.1 7600 100 100
```

这个场景同时覆盖：

- 多进程 Linux Socket 通信；
- 两个客户端并发连接；
- 客户端 Runtime 线程池；
- 服务端 accept 与会话线程；
- Topic 自动序列化、发送、反序列化和注入；
- 心跳、连接状态与异步出站队列。

### 5.4 测试指标

| 指标 | 含义 | 采集方式 |
|---|---|---|
| throughput | 单位时间处理消息数量 | 接收数量 / 有效时间 |
| p50/p95/p99/max latency | 延迟分布和尾延迟 | 单调时钟时间戳 |
| loss | 序号区间内缺失数量 | `SequenceTracker` |
| duplicate | 重复序号数量 | `SequenceTracker` |
| out-of-order | 乱序到达数量 | `SequenceTracker` |
| user/system CPU | 用户态和内核态 CPU 时间 | `getrusage()` |
| process CPU% | 测试期间进程 CPU 使用率 | CPU 时间 / 墙钟时间 |
| current/peak RSS | 当前和峰值常驻内存 | `/proc/self/statm`、`getrusage()` |

本机多进程延迟使用单调时钟；跨机器测试需要先同步时钟，或改为 ping-pong RTT。

### 5.5 线程池负载矩阵

建议使用 Release 构建：

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j
./build-release/rml_benchmark 1000000 4
```

一键测试多档消息数量和 worker 数：

```bash
benchmark/run_load_matrix.sh ./build-release/rml_benchmark
```

默认矩阵：

```text
消息数量：10000 / 100000 / 1000000
worker数：1 / 2 / 4 / 8
```

自定义矩阵：

```bash
RML_MESSAGE_COUNTS="10000 50000" \
RML_WORKER_COUNTS="1 4" \
benchmark/run_load_matrix.sh \
  ./build-release/rml_benchmark \
  benchmark/results/custom.jsonl
```

### 5.6 UDP 频率矩阵

UDP 自动测试脚本会启动独立接收进程和发送进程，在不同频率下采样：

```bash
python3 benchmark/run_udp_matrix.py
```

测试方法、环境说明和可复核样本见 [BENCHMARK.md](BENCHMARK.md)。
仓库中的性能数字只描述对应机器、构建类型和负载条件，不作为其他硬件或跨机器网络的保证。

---

## 6. 构建、运行与验证

### 6.1 环境要求

- Ubuntu/Linux；
- 支持 C++17 的 GCC 或 Clang；
- CMake 3.16 及以上；
- POSIX Threads。

Ubuntu 安装基础依赖：

```bash
sudo apt update
sudo apt install -y build-essential cmake
```

### 6.2 Debug 构建与 CTest

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

测试分层：

| 测试目标 | 覆盖内容 |
|---|---|
| `rml_test_core` | Queue、ThreadPool、Runtime、Pub/Sub 并发语义 |
| `rml_test_serialization` | Codec、golden bytes、截断和协议校验 |
| `rml_test_statistics` | 延迟、丢包、重复、乱序和资源指标 |
| `rml_test_transport` | UDP/TCP 回环、超时、半包粘包和错误路径 |
| `rml_test_bridge` | Topic Bridge、多客户端、心跳、重连和去重 |

### 6.3 Sanitizer

ASan + UBSan：

```bash
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DRML_ENABLE_ASAN=ON
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure
```

TSan 需要单独构建：

```bash
cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DRML_ENABLE_TSAN=ON
cmake --build build-tsan -j
ctest --test-dir build-tsan -L threading --output-on-failure
```

若宿主 Linux 报 `ThreadSanitizer: unexpected memory mapping`，可只对 CTest 进程关闭 ASLR：

```bash
setarch x86_64 -R \
  ctest --test-dir build-tsan -L threading --output-on-failure
```

### 6.4 最小化构建

只构建中间件库：

```bash
cmake -S . -B build-min \
  -DRML_BUILD_DEMOS=OFF \
  -DRML_BUILD_BENCHMARKS=OFF \
  -DBUILD_TESTING=OFF
cmake --build build-min -j
```

### 6.5 安装与下游使用

```bash
cmake -S . -B build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DRML_BUILD_DEMOS=OFF \
  -DRML_BUILD_BENCHMARKS=OFF \
  -DBUILD_TESTING=OFF
cmake --build build-release -j
cmake --install build-release --prefix "$PWD/install"
```

下游 CMake：

```cmake
find_package(RobotMiddlewareLite CONFIG REQUIRED)

target_link_libraries(your_target
  PRIVATE RobotMiddlewareLite::robot_middleware)
```

配置下游项目时指定安装目录：

```bash
cmake -S consumer -B consumer/build \
  -DCMAKE_PREFIX_PATH="/absolute/path/to/robot-middleware-lite/install"
```

---

## 7. 发布订阅 API 示例

```cpp
#include "robot_middleware/robot_middleware.hpp"

int main() {
  robot_middleware::Runtime runtime(4);
  auto sensor = runtime.create_node("imu_node");
  auto controller = runtime.create_node("control_node");

  auto publisher =
      sensor.create_publisher<robot_middleware::ImuMsg>("/imu");

  robot_middleware::SubscriptionOptions options;
  options.queue_depth = 32;
  options.overflow_policy =
      robot_middleware::OverflowPolicy::DropOldest;

  auto subscription =
      controller.create_subscription<robot_middleware::ImuMsg>(
          "/imu",
          [](const robot_middleware::ImuMsg& message) {
            // 业务控制逻辑。
          },
          options);

  publisher.publish(robot_middleware::ImuMsg{});

  subscription.cancel_and_wait();
  runtime.shutdown();
}
```

注意：

- 同一个订阅回调不会与自身并发；
- 回调中可以再次发布消息；
- 回调中可以调用非阻塞 `cancel()`；
- 不要在自己的回调中调用该订阅的 `cancel_and_wait()`，框架会检测自等待并抛出异常。

---

## 8. 工程结构

```text
robot_middleware_lite/
├── .github/workflows/          # GitHub Actions 构建与测试
├── include/robot_middleware/
│   ├── core/                   # Message、Queue、Runtime、Pub/Sub
│   ├── executor/               # Executor、ThreadPool
│   ├── serialization/          # MessageCodec、消息与 Frame 转换
│   ├── transport/              # Frame、Heartbeat、UDP、TCP、Session
│   ├── bridge/                 # Topic 导入导出、心跳与重连
│   └── benchmark/              # 延迟、序号和进程资源统计
├── src/                        # 非模板模块实现
├── demo/
│   ├── inprocess_demo.cpp      # 进程内 IMU→控制→执行
│   ├── udp_sensor.cpp          # UDP 传感器进程
│   ├── udp_control.cpp         # UDP 控制进程
│   ├── tcp_control_client.cpp  # TCP 控制客户端
│   ├── tcp_control_server.cpp  # TCP 控制服务端
│   ├── bridge_client.cpp       # Topic Bridge 客户端
│   └── bridge_server.cpp       # 多客户端 Topic Bridge 服务端
├── benchmark/                  # Benchmark、负载矩阵和样本
├── tests/                      # 单元、网络和并发回归测试
├── docs/
│   ├── PROTOCOL.md             # 二进制线协议
│   └── BENCHMARK.md            # 性能测试方法与结果
├── LICENSE                     # MIT License
└── CMakeLists.txt
```

---

## 9. 并发、生命周期与能力边界

### 9.1 已实现

- 强类型进程内 Pub/Sub 和每订阅独立邮箱；
- 固定大小线程池和可关闭 MPMC 队列；
- TCP/UDP 统一二进制 Frame；
- TCP 半包、粘包、partial send 和超时处理；
- UDP 截断检测、丢包、重复和乱序统计；
- TCP Topic Bridge、多客户端、心跳和异常重连；
- 有界异步出站队列和远端消息去重；
- CTest、ASan/UBSan、TSan 和负载矩阵。

### 9.2 当前限制

- UDP Demo 已实现跨进程 Frame 通信，但尚未接入自动 Topic Bridge；
- TCP 服务端当前为每客户端一个接收线程，不是 `epoll` Reactor；
- 不支持 DDS 式自动发现、动态消息反射和跨网络可靠 QoS；
- 不提供 TLS、身份认证和访问控制；
- 不提供磁盘持久化和断线消息重放；
- 不提供共享内存零拷贝。

网络 Demo 默认绑定回环地址。协议没有 TLS 和身份认证，不应直接暴露到不可信网络。

### 9.3 后续路线

1. 增加 YAML endpoint/Topic 配置和服务调用；
2. 使用 `epoll` I/O Reactor 管理大量连接，并增加每会话出站队列；
3. 增加 deadline/liveliness 事件、结构化故障码和运行指标；
4. 增加 UDP Topic Bridge、分片重组与可配置可靠性策略；
5. 引入对象池、批量调度和共享内存传输；
6. 固定测试平台后，与 ROS 2/CycloneDDS 进行同条件对照。

---

## 10. 项目描述与代码对应关系

| 项目描述 | 核心代码 | 可运行验证 |
|---|---|---|
| Linux Socket、TCP/UDP、二进制协议 | `src/transport/`、`src/serialization/` | UDP/TCP 双进程 Demo |
| 线程池、生产者—消费者、线程同步 | `src/executor/`、`src/core/runtime.cpp` | 进程内 Demo、`rml_test_core` |
| 消息队列、心跳、重连、多客户端 | `src/bridge/`、`src/transport/session_manager.cpp` | Bridge 多客户端 Demo、`rml_test_bridge` |
| 延迟、吞吐量、CPU 和负载测试 | `benchmark/`、`include/.../benchmark/` | load matrix、UDP matrix |

这张表可作为阅读源码和面试演示的入口：从项目描述中的任意一点，都能定位到对应模块、
测试程序和可执行 Demo。

## License

本项目采用 [MIT License](../LICENSE)。
