#pragma once

#include <exception>
#include <functional>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace rml_test {

/// 测试断言失败异常，自动附加源文件和行号。
class Failure : public std::runtime_error {
 public:
  Failure(const char* file, int line, const std::string& message)
      : std::runtime_error(format(file, line, message)) {}

 private:
  static std::string format(const char* file, int line,
                            const std::string& message) {
    std::ostringstream output;
    output << file << ':' << line << ": " << message;
    return output.str();
  }
};

using TestFunction = std::function<void()>;

/// 单个命名测试用例。
struct TestCase {
  std::string name;
  TestFunction function;
};

/// 运行全部用例，或根据第一个命令行参数只运行指定用例。
inline int run(const std::vector<TestCase>& tests, int argc, char** argv) {
  const std::string selected = argc > 1 ? argv[1] : std::string{};
  std::size_t executed = 0;
  std::size_t failed = 0;

  for (const auto& test : tests) {
    if (!selected.empty() && selected != test.name) {
      continue;
    }
    ++executed;
    try {
      test.function();
      std::cout << "[PASS] " << test.name << '\n';
    } catch (const std::exception& error) {
      ++failed;
      std::cerr << "[FAIL] " << test.name << ": " << error.what() << '\n';
    } catch (...) {
      ++failed;
      std::cerr << "[FAIL] " << test.name << ": unknown exception\n";
    }
  }

  if (executed == 0) {
    std::cerr << "unknown test case: " << selected << '\n';
    return 2;
  }
  return failed == 0 ? 0 : 1;
}

/// 验证表达式抛出指定异常类型。
template <typename ExceptionT, typename Function>
void check_throws(Function&& function, const char* expression, const char* file,
                  int line) {
  try {
    std::forward<Function>(function)();
  } catch (const ExceptionT&) {
    return;
  } catch (const std::exception& error) {
    throw Failure(file, line,
                  std::string("expected exception from ") + expression +
                      ", got: " + error.what());
  }
  throw Failure(file, line,
                std::string("expected exception from ") + expression);
}

}  // namespace rml_test

// 最小测试框架宏：保持测试无第三方依赖，并提供可定位的失败信息。
#define RML_CHECK(expression)                                                \
  do {                                                                       \
    if (!(expression)) {                                                     \
      throw ::rml_test::Failure(__FILE__, __LINE__,                          \
                                "check failed: " #expression);              \
    }                                                                        \
  } while (false)

#define RML_CHECK_EQ(left, right)                                            \
  do {                                                                       \
    const auto& rml_left_value = (left);                                     \
    const auto& rml_right_value = (right);                                   \
    if (!(rml_left_value == rml_right_value)) {                              \
      std::ostringstream rml_check_output;                                   \
      rml_check_output << "expected " #left " == " #right << " ("         \
                       << rml_left_value << " != " << rml_right_value << ')'; \
      throw ::rml_test::Failure(__FILE__, __LINE__, rml_check_output.str()); \
    }                                                                        \
  } while (false)

#define RML_CHECK_THROWS(exception_type, expression)                         \
  ::rml_test::check_throws<exception_type>(                                  \
      [&] { static_cast<void>(expression); }, #expression, __FILE__, __LINE__)
