#!/usr/bin/env bash

set -euo pipefail

# 多档负载矩阵：可通过环境变量覆盖消息数量和工作线程列表。
benchmark_binary="${1:-./build/rml_benchmark}"
output_file="${2:-benchmark/results/load_matrix_$(date +%Y%m%d_%H%M%S).jsonl}"
message_counts="${RML_MESSAGE_COUNTS:-10000 100000 1000000}"
worker_counts="${RML_WORKER_COUNTS:-1 2 4 8}"
read -r -a message_count_values <<<"${message_counts}"
read -r -a worker_count_values <<<"${worker_counts}"

if [[ ! -x "${benchmark_binary}" ]]; then
  echo "benchmark executable not found or not executable: ${benchmark_binary}" >&2
  exit 1
fi

# 在正式运行前校验矩阵参数，避免生成只完成了一部分的报告。
if [[ "${#message_count_values[@]}" -eq 0 || "${#worker_count_values[@]}" -eq 0 ]]; then
  echo "message and worker matrices must not be empty" >&2
  exit 1
fi
for message_count in "${message_count_values[@]}"; do
  if [[ ! "${message_count}" =~ ^[1-9][0-9]*$ ]]; then
    echo "invalid message count: ${message_count}" >&2
    exit 1
  fi
done
for worker_count in "${worker_count_values[@]}"; do
  if [[ ! "${worker_count}" =~ ^[1-9][0-9]*$ ]]; then
    echo "invalid worker count: ${worker_count}" >&2
    exit 1
  fi
done

output_directory="$(dirname "${output_file}")"
mkdir -p "${output_directory}"
temporary_file="$(mktemp "${output_file}.tmp.XXXXXX")"
trap 'rm -f "${temporary_file}"' EXIT

# Queue 不受 worker 数影响，每档消息数量只运行一次。
for message_count in "${message_count_values[@]}"; do
  echo "running path=queue, messages=${message_count}" >&2
  "${benchmark_binary}" "${message_count}" 1 queue >>"${temporary_file}"
  for worker_count in "${worker_count_values[@]}"; do
    echo "running path=pubsub, messages=${message_count}, workers=${worker_count}" >&2
    "${benchmark_binary}" "${message_count}" "${worker_count}" pubsub \
      >>"${temporary_file}"
  done
done

mv "${temporary_file}" "${output_file}"
trap - EXIT
echo "${output_file}"
