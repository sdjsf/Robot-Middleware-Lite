# Robot Middleware Lite

[![CI](https://github.com/sdjsf/Robot-Middleware-Lite/actions/workflows/ci.yml/badge.svg)](https://github.com/sdjsf/Robot-Middleware-Lite/actions/workflows/ci.yml)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-00599C.svg)](https://isocpp.org/)
[![Platform](https://img.shields.io/badge/platform-Linux-lightgrey.svg)](#快速开始)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

基于 Linux Socket 与 C++17 多线程实现的轻量级机器人通信中间件。

项目提供进程内强类型 Pub/Sub、TCP/UDP 跨进程传输、自定义二进制协议、
TCP Topic Bridge、心跳重连和多客户端会话管理。当前版本为 `v0.1.0` MVP，
面向 Linux 系统编程学习、机器人节点模拟与通信实验，不依赖 ROS、DDS
或第三方网络库。

## 核心能力

- 强类型 `Publisher<T>` / `Subscription<T>` 与进程内 Topic 路由；
- 线程池、生产者—消费者模型和每订阅有界消息队列；
- 统一二进制 Frame，支持类型、Schema、长度和 CRC32 校验；
- UDP Datagram 收发、截断检测及丢包/重复/乱序统计；
- TCP 非阻塞连接、partial send、半包/粘包和超时处理；
- TCP Topic 导入/导出、多客户端会话、心跳与指数退避重连；
- CTest、ASan/UBSan、TSan、GitHub Actions 和可复现 Benchmark。

## 架构

```text
机器人业务节点
      │  Publisher<T> / Subscription<T>
      ▼
Runtime / TopicBus
      │  每订阅邮箱
      ▼
Executor / ThreadPool
      │  typed message
      ▼
MessageCodec / Frame / CRC32
      │
      ├── UDP Transport
      └── TCP Transport / NetworkBridge / SessionManager
```

进程内路径直接传递强类型 C++ 对象；跨进程路径将消息逐字段序列化为 Frame，
再通过 TCP 或 UDP Socket 传输。当前自动 Topic Bridge 使用 TCP，UDP Demo
使用显式 Frame 收发。

## 快速开始

环境要求：Linux、C++17 编译器、CMake 3.16+ 和 POSIX Threads。

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON

cmake --build build -j
ctest --test-dir build --output-on-failure
```

成功时应看到 5 个测试目标全部通过：

```text
rml_test_core
rml_test_serialization
rml_test_statistics
rml_test_transport
rml_test_bridge
```

## 基本 API

```cpp
#include "robot_middleware/robot_middleware.hpp"

int main() {
  robot_middleware::Runtime runtime(4);
  auto sensor = runtime.create_node("sensor");
  auto controller = runtime.create_node("controller");

  auto publisher =
      sensor.create_publisher<robot_middleware::ImuMsg>("/imu");

  auto subscription =
      controller.create_subscription<robot_middleware::ImuMsg>(
          "/imu",
          [](const robot_middleware::ImuMsg& imu) {
            // 处理传感器消息。
          });

  publisher.publish(robot_middleware::ImuMsg{});

  subscription.cancel_and_wait();
  runtime.shutdown();
}
```

## Demo

| 可执行文件 | 验证场景 |
|---|---|
| `rml_inprocess_demo` | 单进程 IMU → 控制器 → 执行器 |
| `rml_udp_sensor` / `rml_udp_control` | UDP 双进程传感器通信 |
| `rml_tcp_control_client` / `rml_tcp_control_server` | TCP 双进程控制通信 |
| `rml_bridge_client` / `rml_bridge_server` | 多客户端 TCP Topic Bridge |

进程内链路：

```bash
./build/rml_inprocess_demo
```

UDP 双进程：

```bash
# 终端 A
./build/rml_udp_control 127.0.0.1 7400 1000 5000

# 终端 B
./build/rml_udp_sensor 127.0.0.1 7400 1000 100
```

多客户端 Topic Bridge：

```bash
# 服务端等待 200 条消息
./build/rml_bridge_server 7600 200 127.0.0.1

# 两个独立客户端各发送 100 条消息
./build/rml_bridge_client 127.0.0.1 7600 100 100
./build/rml_bridge_client 127.0.0.1 7600 100 100
```

## 性能基线

测试环境：

- Ubuntu 24.04.4 LTS，AMD Ryzen 5 9600X；
- Release 构建，同机 `127.0.0.1` UDP loopback；
- 84 字节应用层 Datagram，每档约 2 秒，重复 3 次。

| 请求频率 | 吞吐 msg/s | 三轮丢包 | p95 | p99 | 双进程 CPU | 并发 RSS |
|---:|---:|---:|---:|---:|---:|---:|
| 100 Hz | 100.501 | 0 | 68.839 µs | 71.860 µs | 0.543% | 7.37 MiB |
| 1000 Hz | 1000.503 | 0 | 19.890 µs | 28.990 µs | 1.041% | 7.41 MiB |
| 5000 Hz | 5000.486 | 0 | 10.710 µs | 20.969 µs | 3.379% | 7.79 MiB |

运行完整 UDP 矩阵：

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j

python3 benchmark/run_udp_matrix.py \
  --control ./build-release/rml_udp_control \
  --sensor ./build-release/rml_udp_sensor \
  --output benchmark/results/udp_matrix.jsonl
```

以上数据仅代表指定机器、二进制和 loopback 负载，不是跨机器或嵌入式硬件
性能承诺。测试方法与原始结果见 [Benchmark 文档](docs/BENCHMARK.md)。

## 能力边界

当前尚未实现：

- DDS 式自动发现与 QoS 协商；
- UDP 自动 Topic Bridge；
- TLS、身份认证和访问控制；
- 共享内存、零拷贝和消息持久化；
- 大规模连接的 `epoll` Reactor；
- 真实机器人硬件与跨机器长稳验证。

当前协议提供有界内存队列上的 at-most-once MVP 语义。示例默认绑定回环地址，
不应直接暴露到不可信网络。

## 文档

- [二进制线协议](docs/PROTOCOL.md)
- [Benchmark 方法与原始基线](docs/BENCHMARK.md)
- [详细源码与调用链](docs/SOURCE_GUIDE.md)

## License

[MIT License](LICENSE)
