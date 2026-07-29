# Robot Middleware Lite 线协议

本文档定义版本 1 的跨进程消息帧。协议字段使用大端（网络字节序），消息结构体不能通过
`memcpy(struct)` 直接发送。

## 帧布局

固定头部为 56 字节，之后依次为 UTF-8 Topic 名和消息负载：

| 偏移 | 长度 | 字段 | 说明 |
|---:|---:|---|---|
| 0 | 4 | `magic` | `RMWL`，十六进制 `0x524D574C` |
| 4 | 1 | `version` | 当前为 `1` |
| 5 | 1 | `flags` | `0x01` 为控制面心跳；业务帧为 `0x00`，其余值拒绝 |
| 6 | 2 | `header_size` | 固定为 `56` |
| 8 | 2 | `topic_size` | Topic 字节数 |
| 10 | 2 | `reserved` | 必须为 `0` |
| 12 | 4 | `payload_size` | 序列化负载字节数 |
| 16 | 4 | `type_id` | `MessageTraits<T>::type_id` |
| 20 | 8 | `schema_hash` | 规范化字段描述的 FNV-1a 64 位哈希 |
| 28 | 8 | `sequence` | 单个发布者内单调递增的消息序号 |
| 36 | 8 | `publisher_id` | 发布者实例标识 |
| 44 | 8 | `send_time_ns` | 进入传输层前的时间戳 |
| 52 | 4 | `crc32` | Topic 与 Payload 拼接后的 CRC32 |
| 56 | M | `topic` | 不带终止符的 Topic 字节 |
| 56+M | N | `payload` | 消息负载 |

解码顺序必须是：校验固定头部、检查长度上限与整数溢出、确认收到完整帧、校验 CRC，
最后才构造 Topic 和 Payload。

## 消息兼容性

接收者同时比较 `type_id` 和 `schema_hash`。`type_id` 表示消息家族，`schema_hash` 表示精确
字段布局；任一不匹配都拒绝反序列化。不能使用 `typeid(T).hash_code()` 作为协议标识，
因为它不保证跨进程、跨编译器稳定。

增加新消息时需要：

1. 定义只包含固定语义字段的普通 C++ 结构体。
2. 增加 `MessageTraits<T>`，分配未使用的 `type_id` 并写规范化 `schema` 字符串。
3. 增加 `MessageCodec<T>`，逐字段编码和解码。
4. 增加 golden bytes、截断输入和 round-trip 测试。

已经发布的 schema 不应原地修改。字段变化应分配新的消息版本或新的 `type_id`。

## UDP 约束

一个 Frame 对应一个 UDP Datagram。版本 1 不做分片与重组，默认最大 Datagram 为 1400
字节，以降低 IP 分片风险。超限发送会失败；接收使用 `recvmsg()` 检测 `MSG_TRUNC`，截断包
不会进入帧解码。

UDP 的丢包、重复和乱序通过 `(publisher_id, sequence)` 统计。尾部丢包不能只由接收端看到
的最大序号推断，Benchmark 必须另行知道发送总数。

## TCP 约束

TCP 复用相同帧格式。接收端先累计 56 字节头部，根据受限的 Topic/Payload 长度继续累计
完整帧，因此能够处理半包和粘包。发送端必须循环处理 partial send 和 `EINTR`，并使用
`MSG_NOSIGNAL` 防止断连触发进程级 `SIGPIPE`。

同一连接的并发发送必须覆盖整个帧加锁，禁止两个帧的字节交叉。

## 心跳与失活检测

`flags == 0x01` 表示心跳帧。版本 1 心跳必须满足：

- Topic 为 `__rml/heartbeat`
- `type_id`、`schema_hash` 和 Payload 均为 0/空
- `publisher_id` 标识发送端，`sequence` 在该连接生命周期内递增
- `send_time_ns` 使用发送端单调时钟，仅供本机诊断，不直接用于跨机器超时判断

心跳属于控制面，接收后只刷新连接活性和统计，不得注入业务 Topic。双方按配置间隔主动发送；
如果在 `idle_timeout` 内没有收到任何业务帧或合法心跳，则关闭连接。客户端随后采用有上限的
指数退避重连，服务端由 SessionManager 继续接受其他客户端。带未知标志位的帧属于协议错误，
即使同时设置了心跳位也必须拒绝。

## 桥接与回环防护

`NetworkBridge` 对桥实例分配 `publisher_id` 和跨导出 Topic 单调递增的序号。导入端按
`(Topic, publisher_id, sequence)` 拒绝重复/倒退帧，并通过 `publish_remote()` 标记网络来源。
桥的导出订阅配置为仅接收本地消息，因此远端注入不会立刻沿同一桥回发；普通业务订阅仍会
收到该消息。

多客户端可能并发递交同一帧，因此导入端必须在调用业务路由前，以同一临界区完成序号检查和
预留。版本 1 采用 at-most-once 导入语义：路由失败后不会回滚已经预留的序号，因为业务
fan-out 可能已经部分发生，自动重试反而可能制造重复。

## 交付与安全边界

TCP 保证单条存活连接上的有序字节流，但版本 1 不持久化出站队列。断线时已经取出、尚未完成
发送的帧不会在重连后自动重放；需要可靠补发的业务应在应用层增加确认、持久化和幂等处理。
UDP 仍保持无连接、尽力而为语义。

协议本身不提供 TLS、对端身份认证或访问控制。示例程序默认使用本机回环地址；在局域网监听前
应由部署者配置防火墙或受保护的网络通道。
