# Benchmark 与可复现实验

本项目把两类性能问题分开测量，避免用进程内结果替代真实 Socket 结果：

| 测试路径 | 变化参数 | 主要指标 |
|---|---|---|
| Queue / 进程内 Pub/Sub | 消息总量、Runtime worker 数 | 吞吐、回调延迟、CPU、当前/峰值 RSS |
| UDP loopback 双进程 | 发送频率、消息总量、重复次数 | 收发/丢包、吞吐、传输延迟、两个进程的 CPU/RSS |

这些结果是工程回归数据，不是跨机器通用的性能承诺。正式采样应使用 Release 构建，并记录
CPU、内核、编译器、构建选项和系统负载。

## 环境与构建

要求：

- Linux（UDP 资源采集使用 `wait4` 和 `/proc`）
- C++17 编译器、CMake、pthread
- `python3`，仅使用 Python 标准库，不依赖 `jq`、`psutil` 或第三方包

建议从干净的 Release 目录构建：

```bash
cmake -S . -B build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=ON
cmake --build build-release -j
ctest --test-dir build-release --output-on-failure
```

## 进程内负载矩阵

一键运行 Queue SPSC 与进程内 Pub/Sub 矩阵：

```bash
benchmark/run_load_matrix.sh \
  ./build-release/rml_benchmark \
  benchmark/results/inproc_matrix.jsonl
```

默认消息数为 `10000/100000/1000000`，worker 数为 `1/2/4/8`。可通过环境变量缩小或
扩展矩阵：

```bash
RML_MESSAGE_COUNTS="10000 50000" \
RML_WORKER_COUNTS="1 4" \
benchmark/run_load_matrix.sh \
  ./build-release/rml_benchmark \
  benchmark/results/inproc_custom.jsonl
```

输出是一行一个 JSON 对象。Queue 每档消息数运行一次；Pub/Sub 对消息数与 worker 数取
笛卡尔积。Pub/Sub 延迟是 `publish()` 入口时间戳到订阅回调执行的进程内延迟，不包含
Socket 或内核网络栈。

## UDP 消息频率矩阵

`run_udp_matrix.py` 直接编排现有 `rml_udp_control` 与 `rml_udp_sensor`，每次场景都启动
两个独立进程并走 Linux UDP Socket：

```bash
python3 benchmark/run_udp_matrix.py \
  --control ./build-release/rml_udp_control \
  --sensor ./build-release/rml_udp_sensor \
  --output benchmark/results/udp_matrix.jsonl
```

默认执行：

- 请求频率：`100/1000/5000 Hz`
- 每档持续约 `2 s`，消息数按 `rate_hz × duration_s` 计算
- 每档重复 `3` 次
- 接收端启动预热 `150 ms`
- RSS 采样间隔 `10 ms`

自定义固定时长矩阵：

```bash
python3 benchmark/run_udp_matrix.py \
  --rates "100 500 1000 5000 10000" \
  --duration-s 5 \
  --repetitions 5 \
  --output benchmark/results/udp_frequency.jsonl
```

也可以显式提供消息总量；此时脚本运行“频率 × 消息数”的笛卡尔积：

```bash
python3 benchmark/run_udp_matrix.py \
  --rates "1000 5000" \
  --counts "10000 50000" \
  --repetitions 3 \
  --output benchmark/results/udp_load.jsonl
```

`--rates`、`--counts`、`--duration-s` 和 `--repetitions` 分别可由
`RML_UDP_RATES_HZ`、`RML_UDP_COUNTS`、`RML_UDP_DURATION_S` 和
`RML_UDP_REPETITIONS` 设置。命令行参数优先。

脚本先写临时文件，全部结束后再原子替换目标文件。扩展名为 `.jsonl` 时，第一行是环境
元数据，后续每行是一次原始运行；扩展名为 `.json` 时，输出包含 `metadata` 和
`results` 的单个 JSON 文档。两种格式都不需要 `jq`：

```bash
python3 -c \
  'import json,sys; [json.loads(line) for line in open(sys.argv[1])]' \
  benchmark/results/udp_matrix.jsonl

python3 -m json.tool \
  benchmark/baselines/ubuntu24_loopback_baseline.json >/dev/null
```

### UDP 结果字段

| 字段 | 含义 |
|---|---|
| `rate_hz_requested` / `messages_requested` | 请求发送频率与消息总量 |
| `messages_sent/received/lost` | Demo 报告的发送、有效接收和推算丢包数 |
| `loss_rate_percent` | UDP 丢包百分比 |
| `throughput_msg_s` | 首条到末条有效接收窗口内的消息吞吐 |
| `latency_p50/p95/p99/max_us` | 同机 `steady_clock` 单向传输延迟 |
| `sender/receiver_*_cpu_s` | `wait4` 返回的各进程 user/system CPU 时间 |
| `combined_cpu_percent` | 两进程 CPU 时间之和除以该场景墙钟时间 |
| `sender/receiver_peak_rss_bytes` | `/proc/<pid>/status` 的进程 `VmHWM` |
| `combined_peak_rss_bytes_sampled` | 按采样周期相加的两个进程并发 RSS 峰值 |

两个进程同时工作，因此 `combined_cpu_percent` 理论上可以超过 `100%`。高负载出现 UDP
丢包仍是有效测量结果，不会仅因丢包被脚本改写为成功传输；二进制退出失败、输出无法解析
或进程超时才会让脚本返回非零。

## 仓库基线

可提交的原始基线位于
[`benchmark/baselines/ubuntu24_loopback_baseline.json`](../benchmark/baselines/ubuntu24_loopback_baseline.json)。
该文件包含二进制 SHA-256 和完整的逐次结果。以下数值为 3 次运行的中位数：

| 请求频率 | 每次消息数 | 丢包（3 次合计） | 吞吐 msg/s | p50 µs | p95 µs | p99 µs | 两进程 CPU | 并发 RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 Hz | 200 | 0 | 100.501 | 32.720 | 68.839 | 71.860 | 0.543% | 7.37 MiB |
| 1000 Hz | 2000 | 0 | 1000.503 | 5.050 | 19.890 | 28.990 | 1.041% | 7.41 MiB |
| 5000 Hz | 10000 | 0 | 5000.486 | 4.610 | 10.710 | 20.969 | 3.379% | 7.79 MiB |

基线环境：

- Ubuntu 24.04.4 LTS，Linux 6.17.0-35-generic，x86_64
- AMD Ryzen 5 9600X，6 核 12 线程
- Release 构建
- 同一台机器的 `127.0.0.1` loopback
- 每档约 2 秒，3 次重复，RSS 每 10 ms 采样

这份数据生成于 2026-07-24，仅用于该机器、该版本二进制的回归参考。短测试容易受 CPU
调频、调度、后台任务和虚拟化影响，不能据此声称其他设备也能达到同样延迟或吞吐。

## 测量边界

- 当前 UDP Demo 发送固定的 `ImuMsg`，应用层 Datagram 为 56 字节头部、4 字节
  `/imu` Topic 和 24 字节 Payload，共 84 字节；指标是 msg/s，不等价于任意载荷下的
  Mbps。
- 单向延迟只在同机成立，因为两个进程共享单调时钟。跨机器测试必须先做时钟同步，或改用
  RTT，不能直接比较各机器的 `steady_clock`。
- `sleep_until` 给出请求频率，实际到达速率应以 `throughput_msg_s` 为准；它不是实时调度
  保证。
- UDP 无重传。高频率下的丢包、乱序是测试结果的一部分，不能把“发送成功”解释为“接收
  成功”。
- `combined_peak_rss_bytes_sampled` 可能错过短于采样周期的瞬时峰值；单进程峰值同时使用
  内核维护的 `VmHWM`。
- 基准没有固定 CPU 核、实时优先级、CPU 频率或后台负载。需要发表严谨性能结论时，应在
  受控硬件上增加预热、更多重复次数和置信区间。
