# Robot Middleware Lite

[![CI](https://github.com/sdjsf/Robot-Middleware-Lite/actions/workflows/ci.yml/badge.svg)](https://github.com/sdjsf/Robot-Middleware-Lite/actions/workflows/ci.yml)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-00599C.svg)](https://isocpp.org/)
[![Platform](https://img.shields.io/badge/platform-Linux-lightgrey.svg)](#快速开始)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

基于 Linux Socket 与 C++17 多线程实现的轻量级机器人通信中间件。

项目面向机器人传感器、控制器和执行器之间的通信需求，提供进程内强类型
Pub/Sub、TCP/UDP 跨进程传输、自定义二进制协议、TCP Topic Bridge、
心跳重连和多客户端会话管理。当前版本为 `v0.1.0` MVP，不依赖 ROS、
DDS 或第三方网络库。

## 核心能力

- 强类型 `Publisher<T>` / `Subscription<T>` 与 Topic 路由；
- 线程池、生产者—消费者模型和每订阅有界消息队列；
- 二进制 Frame、稳定类型标识、Schema 校验和 CRC32；
- UDP 收发、超时、截断及丢包/重复/乱序统计；
- TCP 非阻塞连接、partial send、半包/粘包处理；
- TCP Topic 导入/导出、多客户端、心跳和指数退避重连；
- CTest、Sanitizer、GitHub Actions 和可复现 Benchmark。

## 架构

```text
机器人业务节点
  → Publisher / Subscription
  → Runtime / TopicBus
  → Subscription Queue / ThreadPool
  → MessageCodec / Frame / CRC32
  → UDP | TCP / NetworkBridge / SessionManager
```

进程内路径直接传递强类型 C++ 对象；跨进程路径将消息逐字段序列化为
Frame，再通过 TCP 或 UDP Socket 传输。当前自动 Topic Bridge 使用 TCP。

## 快速开始

要求：Linux、C++17 编译器、CMake 3.16+、POSIX Threads。

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON

cmake --build build -j
ctest --test-dir build --output-on-failure
```

成功标准：5 个测试目标全部通过。

## Demo

| 程序 | 场景 |
|---|---|
| `rml_inprocess_demo` | IMU → 控制器 → 执行器 |
| `rml_udp_sensor` / `rml_udp_control` | UDP 双进程传感器通信 |
| `rml_tcp_control_client` / `rml_tcp_control_server` | TCP 双进程控制通信 |
| `rml_bridge_client` / `rml_bridge_server` | 多客户端 TCP Topic Bridge |

进程内链路：

```bash
./build/rml_inprocess_demo
```

多客户端 Topic Bridge：

```bash
# 服务端
./build/rml_bridge_server 7600 200 127.0.0.1

# 两个独立客户端
./build/rml_bridge_client 127.0.0.1 7600 100 100
./build/rml_bridge_client 127.0.0.1 7600 100 100
```

## 性能基线

测试环境：Ubuntu 24.04.4、Ryzen 5 9600X、Release、同机 UDP loopback、
84 B/消息、每档约 2 秒并重复 3 次。

| 负载 | 吞吐 | 三轮丢包 | p95 | p99 | 双进程 CPU | RSS |
|---:|---:|---:|---:|---:|---:|---:|
| 5000 Hz | 5000.486 msg/s | 0 | 10.710 µs | 20.969 µs | 3.379% | 7.79 MiB |

完整测试方法和原始数据见 [Benchmark 文档](docs/BENCHMARK.md)。
这些结果仅代表指定机器和 loopback 环境，不是跨机器或嵌入式硬件性能承诺。

## 当前边界

- UDP 已支持显式 Frame 通信，尚未接入自动 Topic Bridge；
- TCP 服务端采用每客户端一个会话线程，不是 `epoll` Reactor；
- 不支持自动发现、DDS QoS、TLS、身份认证和消息持久化；
- 不支持共享内存零拷贝；
- 尚未完成真实机器人硬件和跨机器长稳验证。

## 文档

- [线协议](docs/PROTOCOL.md)
- [Benchmark](docs/BENCHMARK.md)
- [详细源码与调用链](docs/SOURCE_GUIDE.md)

## License

[MIT License](LICENSE)
