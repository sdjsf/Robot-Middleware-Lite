#pragma once

// Robot Middleware Lite 聚合头文件。
// 应用只需包含本文件即可访问核心通信、执行器、序列化、传输和统计 API。

#include "robot_middleware/benchmark/process_metrics.hpp"
#include "robot_middleware/benchmark/statistics.hpp"
#include "robot_middleware/bridge/network_bridge.hpp"
#include "robot_middleware/core/message.hpp"
#include "robot_middleware/core/runtime.hpp"
#include "robot_middleware/core/thread_safe_queue.hpp"
#include "robot_middleware/executor/executor.hpp"
#include "robot_middleware/executor/thread_pool.hpp"
#include "robot_middleware/serialization/binary_codec.hpp"
#include "robot_middleware/serialization/message_codec.hpp"
#include "robot_middleware/serialization/network_message.hpp"
#include "robot_middleware/transport/frame.hpp"
#include "robot_middleware/transport/heartbeat.hpp"
#include "robot_middleware/transport/session_manager.hpp"
#include "robot_middleware/transport/tcp_transport.hpp"
#include "robot_middleware/transport/udp_transport.hpp"
