# Robot Middleware Lite

基于 Linux Socket 与 C++17 多线程从零实现的轻量级机器人通信中间件。

> A lightweight C++17 robot communication middleware featuring typed in-process
> Pub/Sub, TCP/UDP transport, binary framing, heartbeat/reconnect, multi-client
> sessions, tests, and reproducible benchmarks.

[![C++17](https://img.shields.io/badge/C%2B%2B-17-00599C.svg)](https://isocpp.org/)
[![Platform](https://img.shields.io/badge/platform-Linux-lightgrey.svg)](#环境要求)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

当前版本为 `v0.1.0` MVP，面向 Linux 学习、机器人节点模拟和中小规模通信实验。
项目不依赖 ROS、DDS 或第三方网络库，直接使用 Linux Socket API 和 C++17 标准库实现。

## 项目概览

机器人系统通常同时运行传感器、控制器和执行器等模块。如果业务代码直接调用
`send()`、`recv()`，网络阻塞、协议解析、线程同步和断线恢复就会与控制逻辑耦合。

本项目提供统一的强类型 Topic 接口，并把系统拆分为：

- 消息与类型系统；
- 进程内发布订阅；
- 队列与并发调度；
- 序列化与二进制协议；
- TCP/UDP 传输；
- 跨进程 Topic Bridge；
- 心跳、重连与会话管理；
- 测试与性能测量。

核心目标不是替代 ROS 2 或 DDS，而是通过一套可运行、可测试的实现，完整展示
C++ 多线程、Linux Socket、消息协议和跨进程通信的工程链路。

## 核心能力

| 能力 | 当前实现 |
|---|---|
| 进程内通信 | 强类型 `Publisher<T>` / `Subscription<T>` 和 Topic 路由 |
| 并发调度 | 固定大小线程池、生产者—消费者模型、可关闭 MPMC 队列 |
| 订阅邮箱 | 每订阅有界队列、串行回调、跨订阅并行 |
| 过载策略 | 丢弃最旧消息或拒绝新消息，并提供统计 |
| 消息协议 | 固定 56 字节头部、Topic、Payload、CRC32 |
| 类型安全 | 稳定 `type_id`、`schema_hash` 和逐字段编解码 |
| UDP | Datagram 收发、超时、截断检测、丢包/重复/乱序统计 |
| TCP | 非阻塞连接、partial send、半包/粘包、超时与安全关闭 |
| Topic Bridge | 强类型 Topic 导入/导出和远端来源标记 |
| 连接管理 | 多客户端 Session、心跳、失活检测、指数退避重连 |
| 回环防护 | `(Topic, publisher_id, sequence)` 去重与远端消息过滤 |
| 工程验证 | CTest、ASan/UBSan、TSan、Demo 和负载矩阵 |

## 总体架构

```text
┌──────────────────────────────────────────────────────────────┐
│ 机器人业务层                                                 │
│ IMU Node           Control Node           Actuator Node      │
└───────────────┬───────────────────┬──────────────────────────┘
                │ Publisher<T>      │ Subscription<T>
┌───────────────▼───────────────────▼──────────────────────────┐
│ 进程内通信：Runtime / Node / TopicBus                        │
│ Topic 路由、类型检查、每订阅邮箱、远端来源标记               │
└───────────────────────────┬──────────────────────────────────┘
                            │ callback task
┌───────────────────────────▼──────────────────────────────────┐
│ 并发调度：Executor / ThreadPool / ThreadSafeQueue            │
│ 生产者—消费者、回调串行性、跨订阅并行、过载保护              │
└───────────────────────────┬──────────────────────────────────┘
                            │ typed message
┌───────────────────────────▼──────────────────────────────────┐
│ 序列化与协议：MessageTraits / MessageCodec / Frame / CRC32   │
│ 强类型消息 → 稳定字段编码 → 统一二进制帧                    │
└───────────────────────────┬──────────────────────────────────┘
                            │ Frame
┌───────────────────────────▼──────────────────────────────────┐
│ 网络与桥接：UDP / TCP / NetworkBridge / SessionManager      │
│ Socket 收发、半包粘包、多客户端、心跳、重连、Topic Bridge   │
└──────────────────────────────────────────────────────────────┘
```

### 进程内数据流

```text
Publisher<T>
  → TopicBus
  → Subscription mailbox
  → Executor / ThreadPool
  → callback(const T&)
```

进程内路径直接传递强类型 C++ 对象，不经过 Socket，也不执行网络序列化。
同一个订阅的回调保持串行，不同订阅可以由线程池并行执行。

### 跨进程数据流

```text
本地 Publisher<T>
  → TopicBus
  → NetworkBridge::export_topic<T>()
  → MessageCodec<T>
  → Frame + CRC32
  → TCP
  → TcpSessionManager
  → NetworkBridge::receive_frame()
  → deserialize_frame<T>()
  → publish_remote()
  → 远端 TopicBus
  → 业务回调
```

UDP Demo 走显式的 Frame 收发路径；当前自动 Topic Bridge 使用 TCP。

## 二进制协议

所有整数采用大端网络字节序。C++ 结构体不会通过 `memcpy(struct)` 直接发送，
避免结构体填充、CPU 端序和 ABI 差异。

```text
固定头部（56 B） | Topic（M B） | Payload（N B）
```

| 字段 | 作用 |
|---|---|
| `magic` / `version` | 协议识别和版本检查 |
| `flags` | 区分业务帧与心跳控制帧 |
| `topic_size` / `payload_size` | 完整帧重建与长度限制 |
| `type_id` | 标识消息家族 |
| `schema_hash` | 检查精确字段布局 |
| `sequence` | 丢包、乱序和重复帧识别 |
| `publisher_id` | 区分发布者实例 |
| `send_time_ns` | 本机延迟测量 |
| `crc32` | 校验 Topic 与 Payload 完整性 |

完整字段布局、解码顺序和兼容性规则见
[docs/PROTOCOL.md](docs/PROTOCOL.md)。

## 并发模型

### Runtime 与订阅

- 一个 `Runtime` 持有共享 `TopicBus` 和线程池；
- 每个订阅拥有独立的有界邮箱；
- 发布线程只负责路由和投递，不直接执行回调；
- 同一订阅通过单一 drain 任务维持回调串行；
- 不同订阅可在不同 worker 上并行；
- `cancel_and_wait()` 等待在途回调结束；
- `shutdown()` 关闭入口、排空任务并回收线程。

### TCP 会话

```text
TcpSessionManager
├── accept thread
├── Session 1 receive thread
├── Session 2 receive thread
└── Session N receive thread
```

当前模型适合中小规模机器人节点。每个会话独立接收，一个客户端的阻塞不会占用
其他客户端的会话线程。大量连接场景下，后续可升级为 `epoll` Reactor。

### 心跳与重连

- 客户端与服务端按配置周期发送控制面心跳；
- 合法业务帧和心跳都会刷新连接活性；
- 超过 `idle_timeout` 未收到数据时主动关闭连接；
- 客户端使用有上限的指数退避重新连接；
- `stop()` 可以中断 I/O、心跳和退避等待；
- 重连后不自动重放已经丢失的历史消息。

## 快速开始

### 环境要求

- Linux；
- GCC 或 Clang，支持 C++17；
- CMake 3.16 及以上；
- POSIX Threads；
- Python 3，仅用于 UDP Benchmark 编排。

Ubuntu安装基础依赖：

```bash
sudo apt update
sudo apt install -y build-essential cmake python3
```

### 构建与测试

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON

cmake --build build -j

ctest --test-dir build --output-on-failure
```

成功时应看到5个测试目标全部通过：

```text
rml_test_core
rml_test_serialization
rml_test_statistics
rml_test_transport
rml_test_bridge
```

网络测试需要允许本机 loopback Socket 的运行环境。

### Release构建

```bash
cmake -S . -B build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=ON

cmake --build build-release -j

ctest --test-dir build-release --output-on-failure
```

### 最小化构建

只构建中间件库：

```bash
cmake -S . -B build-min \
  -DRML_BUILD_DEMOS=OFF \
  -DRML_BUILD_BENCHMARKS=OFF \
  -DBUILD_TESTING=OFF

cmake --build build-min -j
```

## Demo

| 可执行文件 | 场景 |
|---|---|
| `rml_inprocess_demo` | 单进程 IMU → 控制器 → 执行器 |
| `rml_udp_sensor` | UDP IMU 发送端 |
| `rml_udp_control` | UDP 接收与延迟/丢包统计 |
| `rml_tcp_control_client` | TCP 控制指令客户端 |
| `rml_tcp_control_server` | TCP 控制指令服务端 |
| `rml_bridge_client` | 本地 Topic 自动导出到 TCP |
| `rml_bridge_server` | 多客户端 Topic 导入服务端 |

### 进程内机器人链路

```bash
./build/rml_inprocess_demo
```

Demo 以100 Hz发布200条IMU消息，控制回调生成 `/cmd_vel`，执行器回调消费指令，
最后输出回调数量和队列丢弃数量。

### UDP双进程

终端A：

```bash
./build/rml_udp_control 127.0.0.1 7400 1000 5000
```

终端B：

```bash
./build/rml_udp_sensor 127.0.0.1 7400 1000 100
```

接收端输出有效接收、丢包、重复、乱序、吞吐和延迟分位数。

### TCP双进程

终端A：

```bash
./build/rml_tcp_control_server 7500 100 127.0.0.1
```

终端B：

```bash
./build/rml_tcp_control_client 127.0.0.1 7500 100 50
```

该Demo验证TCP连接、完整帧接收和控制指令反序列化。

### 多客户端Topic Bridge

先启动服务端，等待总计200条IMU：

```bash
./build/rml_bridge_server 7600 200 127.0.0.1
```

再从两个终端启动独立客户端：

```bash
./build/rml_bridge_client 127.0.0.1 7600 100 100
./build/rml_bridge_client 127.0.0.1 7600 100 100
```

该场景覆盖多进程Socket、两个并发客户端、Topic自动序列化与导入、
服务端会话管理、心跳和异步出站队列。

## Pub/Sub API示例

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
            // 处理传感器数据。
          },
          options);

  publisher.publish(robot_middleware::ImuMsg{});

  subscription.cancel_and_wait();
  runtime.shutdown();
}
```

## 性能指标

仓库提供两类可复现实验：

| 测试路径 | 主要指标 |
|---|---|
| Queue / 进程内 Pub/Sub | 吞吐、回调延迟、CPU、RSS、队列丢弃 |
| UDP loopback 双进程 | 收发、丢包、吞吐、单向延迟、双进程CPU/RSS |

UDP基线环境：

- Ubuntu 24.04.4 LTS；
- AMD Ryzen 5 9600X，6核12线程；
- Release构建；
- `127.0.0.1` loopback；
- 固定84字节应用层Datagram；
- 每档约2秒，重复3次。

三次运行中位数：

| 请求频率 | 吞吐 msg/s | 三轮合计丢包 | p50 µs | p95 µs | p99 µs | 双进程CPU | 并发RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 Hz | 100.501 | 0 | 32.720 | 68.839 | 71.860 | 0.543% | 7.37 MiB |
| 1000 Hz | 1000.503 | 0 | 5.050 | 19.890 | 28.990 | 1.041% | 7.41 MiB |
| 5000 Hz | 5000.486 | 0 | 4.610 | 10.710 | 20.969 | 3.379% | 7.79 MiB |

运行进程内负载矩阵：

```bash
benchmark/run_load_matrix.sh \
  ./build-release/rml_benchmark \
  benchmark/results/inproc_matrix.jsonl
```

运行UDP频率矩阵：

```bash
python3 benchmark/run_udp_matrix.py \
  --control ./build-release/rml_udp_control \
  --sensor ./build-release/rml_udp_sensor \
  --output benchmark/results/udp_matrix.jsonl
```

完整测试方法、原始字段和测量边界见
[docs/BENCHMARK.md](docs/BENCHMARK.md)。

这些结果是该机器与该版本二进制的回归参考，不是跨机器或嵌入式硬件性能承诺。
跨机器单向延迟必须先同步时钟，或者改为测量RTT。

## 工程结构

```text
robot_middleware_lite/
├── .github/workflows/ci.yml
├── include/robot_middleware/
│   ├── core/
│   ├── executor/
│   ├── serialization/
│   ├── transport/
│   ├── bridge/
│   └── benchmark/
├── src/
├── demo/
├── benchmark/
│   ├── middleware_benchmark.cpp
│   ├── run_load_matrix.sh
│   ├── run_udp_matrix.py
│   └── baselines/
├── tests/
├── docs/
│   ├── PROTOCOL.md
│   ├── BENCHMARK.md
│   └── SOURCE_GUIDE.md
├── CMakeLists.txt
└── LICENSE
```

## 测试与质量保障

| 测试目标 | 主要覆盖内容 |
|---|---|
| `rml_test_core` | Queue、ThreadPool、Runtime、Pub/Sub并发语义 |
| `rml_test_serialization` | Codec、golden bytes、截断与协议校验 |
| `rml_test_statistics` | 延迟、丢包、重复、乱序与资源指标 |
| `rml_test_transport` | UDP/TCP回环、超时、半包粘包与错误路径 |
| `rml_test_bridge` | Topic Bridge、多客户端、心跳、重连与去重 |

ASan与UBSan：

```bash
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DRML_ENABLE_ASAN=ON

cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure
```

TSan需要单独构建：

```bash
cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DRML_ENABLE_TSAN=ON

cmake --build build-tsan -j
ctest --test-dir build-tsan -L threading --output-on-failure
```

GitHub Actions会执行Debug、ASan/UBSan、CTest、Benchmark工具校验和安装树验证。

## 能力边界

当前已经实现：

- 强类型进程内Pub/Sub；
- 有界订阅邮箱和过载统计；
- C++17线程池与安全关闭；
- TCP/UDP统一Frame；
- TCP半包、粘包、partial send和超时处理；
- UDP截断、丢包、重复和乱序统计；
- TCP Topic Bridge；
- 多客户端Session；
- 心跳、失活检测和自动重连；
- 远端消息标记与重复帧过滤；
- 单元测试、网络集成测试和性能矩阵。

当前没有实现：

- DDS式自动发现与QoS协商；
- UDP自动Topic Bridge；
- TLS、身份认证和访问控制；
- 共享内存与零拷贝；
- 消息持久化和断线重放；
- 跨平台Socket抽象；
- 大规模连接的`epoll` Reactor；
- 真实机器人硬件与跨机器长稳验证。

协议当前提供有界内存队列上的at-most-once MVP语义，不是可靠消息队列。
示例程序默认绑定回环地址；由于没有TLS和认证，不应直接暴露在不可信网络中。

## 后续路线

1. 增加TCP Bridge延迟、吞吐和重连时间Benchmark；
2. 增加1/2/4/8/16客户端扩展测试和长时间稳定性测试；
3. 使用`epoll` Reactor替代每客户端一个接收线程；
4. 增加YAML endpoint/Topic配置与服务调用；
5. 增加UDP Topic Bridge、分片重组和可靠性策略；
6. 增加共享内存传输、对象池和批量调度；
7. 在同一硬件和负载下与ROS 2/CycloneDDS进行对照测试。

## 详细文档

- [二进制线协议](docs/PROTOCOL.md)
- [Benchmark方法与原始基线](docs/BENCHMARK.md)
- [详细源码与调用链手册](docs/SOURCE_GUIDE.md)

## License

本项目采用 [MIT License](LICENSE)。
