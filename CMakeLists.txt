cmake_minimum_required(VERSION 3.16)

# Robot Middleware Lite 顶层构建入口：统一管理库、Demo、Benchmark、测试和安装包。
project(
  RobotMiddlewareLite
  VERSION 0.1.0
  DESCRIPTION "A lightweight C++17 robot communication middleware"
  LANGUAGES CXX)

include(CTest)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

find_package(Threads REQUIRED)

# 可按使用场景裁剪演示、性能测试和 Sanitizer。
option(RML_BUILD_DEMOS "Build runnable middleware demos" ON)
option(RML_BUILD_BENCHMARKS "Build performance benchmark executables" ON)
option(RML_ENABLE_ASAN "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(RML_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(RML_ENABLE_ASAN AND RML_ENABLE_TSAN)
  message(FATAL_ERROR "RML_ENABLE_ASAN and RML_ENABLE_TSAN cannot be enabled together")
endif()

# 项目统一使用严格 C++17，不启用编译器私有语言扩展。
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 为每个目标统一配置警告与互斥的 Sanitizer 选项。
function(rml_apply_project_options target)
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
    if(RML_ENABLE_ASAN)
      target_compile_options(${target} PRIVATE
        -fsanitize=address,undefined -fno-omit-frame-pointer)
      target_link_options(${target} PRIVATE
        -fsanitize=address,undefined -fno-omit-frame-pointer)
    elseif(RML_ENABLE_TSAN)
      target_compile_options(${target} PRIVATE
        -fsanitize=thread -fno-omit-frame-pointer)
      target_link_options(${target} PRIVATE
        -fsanitize=thread -fno-omit-frame-pointer)
    endif()
  endif()
endfunction()

# 主静态库：公开头文件位于 include/，非模板实现位于 src/。
add_library(robot_middleware
  src/benchmark/statistics.cpp
  src/bridge/network_bridge.cpp
  src/core/runtime.cpp
  src/executor/thread_pool.cpp
  src/serialization/message_codec.cpp
  src/transport/frame.cpp
  src/transport/session_manager.cpp
  src/transport/tcp_transport.cpp
  src/transport/udp_transport.cpp)
add_library(RobotMiddlewareLite::robot_middleware ALIAS robot_middleware)

target_compile_features(robot_middleware PUBLIC cxx_std_17)
target_include_directories(robot_middleware
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(robot_middleware PUBLIC Threads::Threads)
rml_apply_project_options(robot_middleware)

if(RML_BUILD_DEMOS)
  # 每个 Demo 都是独立进程入口，便于分别演示单进程、UDP 和 TCP 链路。
  set(RML_DEMOS
    inprocess_demo
    bridge_server
    bridge_client
    udp_sensor
    udp_control
    tcp_control_server
    tcp_control_client)
  foreach(demo IN LISTS RML_DEMOS)
    add_executable(rml_${demo} demo/${demo}.cpp)
    target_link_libraries(rml_${demo} PRIVATE robot_middleware)
    rml_apply_project_options(rml_${demo})
  endforeach()
endif()

if(RML_BUILD_BENCHMARKS)
  # Benchmark 默认不注册为 CTest，避免不同机器性能抖动导致功能测试失败。
  add_executable(rml_benchmark benchmark/middleware_benchmark.cpp)
  target_link_libraries(rml_benchmark PRIVATE robot_middleware)
  rml_apply_project_options(rml_benchmark)
endif()

if(BUILD_TESTING)
  # 创建无第三方依赖的测试可执行文件，并配置标签与死锁保护超时。
  function(rml_add_test target source label timeout)
    add_executable(${target} ${source})
    target_include_directories(${target} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(${target} PRIVATE robot_middleware)
    rml_apply_project_options(${target})
    add_test(NAME ${target} COMMAND ${target})
    set_tests_properties(${target} PROPERTIES LABELS "${label}" TIMEOUT ${timeout})
  endfunction()

  rml_add_test(rml_test_serialization tests/test_serialization.cpp unit 10)
  rml_add_test(rml_test_statistics tests/test_statistics.cpp unit 10)
  rml_add_test(rml_test_transport tests/test_transport.cpp "integration;threading" 20)
  rml_add_test(rml_test_bridge tests/test_bridge.cpp "integration;threading" 30)
  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_core.cpp)
    rml_add_test(rml_test_core tests/test_core.cpp "unit;threading" 20)
  endif()
endif()

# 安装静态库、全部公开头文件和可被 find_package 使用的导出目标。
install(TARGETS robot_middleware
  EXPORT RobotMiddlewareLiteTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

configure_package_config_file(
  cmake/RobotMiddlewareLiteConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/RobotMiddlewareLiteConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/RobotMiddlewareLite)
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/RobotMiddlewareLiteConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)

install(EXPORT RobotMiddlewareLiteTargets
  FILE RobotMiddlewareLiteTargets.cmake
  NAMESPACE RobotMiddlewareLite::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/RobotMiddlewareLite)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/RobotMiddlewareLiteConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/RobotMiddlewareLiteConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/RobotMiddlewareLite)
install(FILES LICENSE DESTINATION ${CMAKE_INSTALL_DOCDIR})
