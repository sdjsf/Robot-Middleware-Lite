#!/usr/bin/env python3
"""Run the existing UDP demos as a repeatable loopback benchmark matrix.

Only Python's standard library is used.  Each run launches rml_udp_control and
rml_udp_sensor as separate processes, parses their existing text output, and
adds process CPU/RSS measurements obtained from Linux wait4/procfs.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import math
import os
import platform
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


RESULT_RE = re.compile(
    r"UDP result: received=(?P<received>\d+)/(?P<expected>\d+), "
    r"lost=(?P<lost>\d+), loss_rate=(?P<loss_rate>[0-9.]+)%, "
    r"duplicate=(?P<duplicate>\d+), out_of_order=(?P<out_of_order>\d+), "
    r"malformed=(?P<malformed>\d+), "
    r"throughput_msg_s=(?P<throughput>[0-9.]+)"
)
LATENCY_RE = re.compile(
    r"latency_us: p50=(?P<p50>[0-9.]+), p95=(?P<p95>[0-9.]+), "
    r"p99=(?P<p99>[0-9.]+), max=(?P<maximum>[0-9.]+)"
)
SENSOR_RE = re.compile(
    r"UDP sensor finished: sent=(?P<sent>\d+), rate_hz=(?P<rate>\d+)"
)


def parse_positive_int_list(text: str, name: str) -> List[int]:
    values: List[int] = []
    for item in text.replace(",", " ").split():
        try:
            value = int(item)
        except ValueError as error:
            raise argparse.ArgumentTypeError(
                f"{name} must contain positive integers"
            ) from error
        if value <= 0:
            raise argparse.ArgumentTypeError(
                f"{name} must contain positive integers"
            )
        values.append(value)
    if not values:
        raise argparse.ArgumentTypeError(f"{name} must not be empty")
    return values


def positive_int(text: str) -> int:
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return value


def positive_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value) or value <= 0.0:
        raise argparse.ArgumentTypeError("value must be a positive number")
    return value


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            block = source.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def read_os_name() -> str:
    try:
        for line in Path("/etc/os-release").read_text(
            encoding="utf-8"
        ).splitlines():
            if line.startswith("PRETTY_NAME="):
                return line.split("=", 1)[1].strip().strip('"')
    except OSError:
        pass
    return platform.platform()


def read_cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text(
            encoding="utf-8", errors="replace"
        ).splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return "unknown"


def choose_loopback_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def read_process_rss_bytes(pid: int) -> Tuple[int, int]:
    """Return current RSS and process high-water RSS from Linux procfs."""
    current_rss = 0
    peak_rss = 0
    try:
        for line in Path(f"/proc/{pid}/status").read_text(
            encoding="ascii", errors="replace"
        ).splitlines():
            if line.startswith("VmRSS:"):
                # Linux exposes VmRSS in KiB.
                current_rss = int(line.split()[1]) * 1024
            elif line.startswith("VmHWM:"):
                peak_rss = int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return current_rss, peak_rss


@dataclasses.dataclass
class ChildUsage:
    returncode: int
    wall_time_s: float
    user_cpu_s: float
    system_cpu_s: float
    peak_rss_bytes: int
    stdout: str
    stderr: str


class ManagedChild:
    """Popen wrapper that retains wait4 resource usage for one child."""

    def __init__(self, command: Sequence[str], environment: Dict[str, str]):
        self.command = list(command)
        self.stdout_file = tempfile.TemporaryFile(mode="w+b")
        self.stderr_file = tempfile.TemporaryFile(mode="w+b")
        self.started_at = time.monotonic()
        self.exited_at: Optional[float] = None
        self.rusage = None
        self.sampled_peak_rss_bytes = 0
        self.process = subprocess.Popen(
            self.command,
            stdin=subprocess.DEVNULL,
            stdout=self.stdout_file,
            stderr=self.stderr_file,
            env=environment,
            close_fds=True,
        )

    @property
    def alive(self) -> bool:
        return self.process.returncode is None

    def sample_rss(self) -> int:
        if not self.alive:
            return 0
        current_rss, process_peak_rss = read_process_rss_bytes(
            self.process.pid
        )
        self.sampled_peak_rss_bytes = max(
            self.sampled_peak_rss_bytes, process_peak_rss, current_rss
        )
        return current_rss

    def reap(self) -> bool:
        if not self.alive:
            return True
        try:
            pid, status, usage = os.wait4(self.process.pid, os.WNOHANG)
        except ChildProcessError:
            # This should not occur because no Popen poll/wait method is used.
            raise RuntimeError(f"lost child process {self.process.pid}")
        if pid == 0:
            return False
        self.process.returncode = os.waitstatus_to_exitcode(status)
        self.exited_at = time.monotonic()
        self.rusage = usage
        return True

    def signal(self, signum: int) -> None:
        if self.alive:
            try:
                os.kill(self.process.pid, signum)
            except ProcessLookupError:
                pass

    def result(self) -> ChildUsage:
        if self.alive or self.exited_at is None or self.rusage is None:
            raise RuntimeError("child result requested before process exit")
        self.stdout_file.seek(0)
        self.stderr_file.seek(0)
        stdout = self.stdout_file.read().decode("utf-8", errors="replace")
        stderr = self.stderr_file.read().decode("utf-8", errors="replace")
        return ChildUsage(
            returncode=int(self.process.returncode),
            wall_time_s=self.exited_at - self.started_at,
            user_cpu_s=float(self.rusage.ru_utime),
            system_cpu_s=float(self.rusage.ru_stime),
            # VmHWM belongs to the post-exec process image.  In contrast,
            # wait4.ru_maxrss can retain the Python launcher's pre-exec high
            # water mark on Linux, so it is intentionally not reported here.
            peak_rss_bytes=self.sampled_peak_rss_bytes,
            stdout=stdout,
            stderr=stderr,
        )

    def close(self) -> None:
        self.stdout_file.close()
        self.stderr_file.close()


def reap_until(
    children: Iterable[ManagedChild],
    deadline: float,
    sample_interval_s: float,
) -> int:
    """Wait for every child and return sampled concurrent peak RSS."""
    child_list = list(children)
    combined_peak_rss = 0
    while any(child.alive for child in child_list):
        combined_rss = sum(child.sample_rss() for child in child_list)
        combined_peak_rss = max(combined_peak_rss, combined_rss)
        for child in child_list:
            child.reap()
        if not any(child.alive for child in child_list):
            break
        if time.monotonic() >= deadline:
            for child in child_list:
                child.signal(signal.SIGTERM)
            terminate_deadline = time.monotonic() + 1.0
            while any(child.alive for child in child_list):
                for child in child_list:
                    child.reap()
                if time.monotonic() >= terminate_deadline:
                    for child in child_list:
                        child.signal(signal.SIGKILL)
                time.sleep(min(sample_interval_s, 0.01))
            raise TimeoutError("UDP benchmark child process timed out")
        time.sleep(sample_interval_s)
    return combined_peak_rss


def wait_for_receiver_start(
    receiver: ManagedChild, startup_s: float, sample_interval_s: float
) -> None:
    deadline = time.monotonic() + startup_s
    while time.monotonic() < deadline:
        receiver.sample_rss()
        if receiver.reap():
            result = receiver.result()
            raise RuntimeError(
                "UDP receiver exited during startup: "
                + (result.stderr.strip() or result.stdout.strip())
            )
        time.sleep(min(sample_interval_s, max(0.0, deadline - time.monotonic())))


def parse_demo_output(
    receiver: ChildUsage, sender: ChildUsage
) -> Dict[str, object]:
    result_match = RESULT_RE.search(receiver.stdout)
    latency_match = LATENCY_RE.search(receiver.stdout)
    sender_match = SENSOR_RE.search(sender.stdout)
    if result_match is None or latency_match is None or sender_match is None:
        raise RuntimeError(
            "could not parse UDP demo output; "
            f"receiver_stdout={receiver.stdout.strip()!r}, "
            f"receiver_stderr={receiver.stderr.strip()!r}, "
            f"sender_stdout={sender.stdout.strip()!r}, "
            f"sender_stderr={sender.stderr.strip()!r}"
        )
    return {
        "messages_sent": int(sender_match.group("sent")),
        "messages_received": int(result_match.group("received")),
        "messages_expected": int(result_match.group("expected")),
        "messages_lost": int(result_match.group("lost")),
        "loss_rate_percent": float(result_match.group("loss_rate")),
        "duplicates": int(result_match.group("duplicate")),
        "out_of_order": int(result_match.group("out_of_order")),
        "malformed": int(result_match.group("malformed")),
        "throughput_msg_s": float(result_match.group("throughput")),
        "latency_p50_us": float(latency_match.group("p50")),
        "latency_p95_us": float(latency_match.group("p95")),
        "latency_p99_us": float(latency_match.group("p99")),
        "latency_max_us": float(latency_match.group("maximum")),
    }


def run_one(
    control_binary: Path,
    sensor_binary: Path,
    rate_hz: int,
    message_count: int,
    repetition: int,
    timeout_ms: int,
    startup_ms: int,
    sample_ms: int,
) -> Dict[str, object]:
    port = choose_loopback_udp_port()
    environment = os.environ.copy()
    environment["LC_ALL"] = "C"
    receiver = ManagedChild(
        [
            str(control_binary),
            "127.0.0.1",
            str(port),
            str(message_count),
            str(timeout_ms),
        ],
        environment,
    )
    sender: Optional[ManagedChild] = None
    benchmark_started_at: Optional[float] = None
    combined_peak_rss = 0
    try:
        wait_for_receiver_start(
            receiver, startup_ms / 1000.0, sample_ms / 1000.0
        )
        sender = ManagedChild(
            [
                str(sensor_binary),
                "127.0.0.1",
                str(port),
                str(message_count),
                str(rate_hz),
            ],
            environment,
        )
        benchmark_started_at = time.monotonic()
        expected_runtime_s = message_count / float(rate_hz)
        deadline = (
            benchmark_started_at
            + expected_runtime_s
            + timeout_ms / 1000.0
            + 10.0
        )
        combined_peak_rss = reap_until(
            [receiver, sender], deadline, sample_ms / 1000.0
        )
        benchmark_finished_at = time.monotonic()
        receiver_usage = receiver.result()
        sender_usage = sender.result()
        if receiver_usage.returncode != 0 or sender_usage.returncode != 0:
            raise RuntimeError(
                "UDP demo failed: "
                f"receiver_rc={receiver_usage.returncode}, "
                f"sender_rc={sender_usage.returncode}, "
                f"receiver_stderr={receiver_usage.stderr.strip()!r}, "
                f"sender_stderr={sender_usage.stderr.strip()!r}"
            )
        observed = parse_demo_output(receiver_usage, sender_usage)
        benchmark_wall_s = benchmark_finished_at - benchmark_started_at
        combined_cpu_s = (
            receiver_usage.user_cpu_s
            + receiver_usage.system_cpu_s
            + sender_usage.user_cpu_s
            + sender_usage.system_cpu_s
        )
        return {
            "record_type": "result",
            "schema_version": 1,
            "benchmark": "udp_loopback",
            "status": "ok",
            "rate_hz_requested": rate_hz,
            "messages_requested": message_count,
            "repetition": repetition,
            **observed,
            "wall_time_s": round(benchmark_wall_s, 6),
            "sender_user_cpu_s": round(sender_usage.user_cpu_s, 6),
            "sender_system_cpu_s": round(sender_usage.system_cpu_s, 6),
            "receiver_user_cpu_s": round(receiver_usage.user_cpu_s, 6),
            "receiver_system_cpu_s": round(receiver_usage.system_cpu_s, 6),
            "combined_cpu_s": round(combined_cpu_s, 6),
            "combined_cpu_percent": round(
                100.0 * combined_cpu_s / benchmark_wall_s, 3
            )
            if benchmark_wall_s > 0.0
            else 0.0,
            "sender_peak_rss_bytes": sender_usage.peak_rss_bytes,
            "receiver_peak_rss_bytes": receiver_usage.peak_rss_bytes,
            "combined_peak_rss_bytes_sampled": combined_peak_rss,
            "rss_sample_interval_ms": sample_ms,
        }
    finally:
        children = [receiver] + ([sender] if sender is not None else [])
        for child in children:
            child.signal(signal.SIGTERM)
        cleanup_deadline = time.monotonic() + 1.0
        while any(child.alive for child in children):
            for child in children:
                child.reap()
            if time.monotonic() >= cleanup_deadline:
                for child in children:
                    child.signal(signal.SIGKILL)
            time.sleep(0.005)
        for child in children:
            child.close()


def build_metadata(
    control_binary: Path,
    sensor_binary: Path,
    rates: Sequence[int],
    counts: Optional[Sequence[int]],
    duration_s: float,
    repetitions: int,
    sample_ms: int,
) -> Dict[str, object]:
    return {
        "record_type": "metadata",
        "schema_version": 1,
        "benchmark": "udp_loopback",
        "generated_at_utc": dt.datetime.now(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
        "environment": {
            "os": read_os_name(),
            "kernel": platform.release(),
            "machine": platform.machine(),
            "logical_cpus": os.cpu_count(),
            "cpu_model": read_cpu_model(),
            "python": platform.python_version(),
        },
        "binaries": {
            "control_name": control_binary.name,
            "control_sha256": file_sha256(control_binary),
            "sensor_name": sensor_binary.name,
            "sensor_sha256": file_sha256(sensor_binary),
        },
        "matrix": {
            "rates_hz": list(rates),
            "counts": list(counts) if counts is not None else None,
            "fixed_duration_s": None if counts is not None else duration_s,
            "repetitions": repetitions,
        },
        "measurement": {
            "latency_clock": "steady_clock, same-host one-way only",
            "cpu_source": "Linux wait4 per-child rusage",
            "peak_rss_source": "Linux /proc/<pid>/status VmHWM",
            "combined_rss_source": f"/proc sampled every {sample_ms} ms",
        },
    }


def atomic_write(
    output_path: Path,
    metadata: Dict[str, object],
    results: Sequence[Dict[str, object]],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=output_path.parent,
        prefix=output_path.name + ".tmp.",
        delete=False,
    ) as temporary:
        temporary_path = Path(temporary.name)
        try:
            if output_path.suffix == ".json":
                json.dump(
                    {"metadata": metadata, "results": list(results)},
                    temporary,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
                temporary.write("\n")
            else:
                temporary.write(
                    json.dumps(metadata, ensure_ascii=False, sort_keys=True)
                    + "\n"
                )
                for result in results:
                    temporary.write(
                        json.dumps(result, ensure_ascii=False, sort_keys=True)
                        + "\n"
                    )
            temporary.flush()
            os.fsync(temporary.fileno())
        except BaseException:
            temporary_path.unlink(missing_ok=True)
            raise
    os.replace(temporary_path, output_path)


def parse_arguments(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run rml_udp_sensor/control over loopback and write an atomic "
            "JSONL matrix (or a JSON document when --output ends in .json)."
        )
    )
    parser.add_argument(
        "--sensor",
        type=Path,
        default=Path("./build-release/rml_udp_sensor"),
        help="path to rml_udp_sensor",
    )
    parser.add_argument(
        "--control",
        type=Path,
        default=Path("./build-release/rml_udp_control"),
        help="path to rml_udp_control",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "benchmark/results/udp_matrix_"
            + dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            + ".jsonl"
        ),
        help="output .jsonl or .json path",
    )
    parser.add_argument(
        "--rates",
        default=os.environ.get("RML_UDP_RATES_HZ", "100 1000 5000"),
        help="space/comma-separated requested message rates",
    )
    parser.add_argument(
        "--counts",
        default=os.environ.get("RML_UDP_COUNTS"),
        help=(
            "optional space/comma-separated counts; when set, run the "
            "rate x count Cartesian matrix"
        ),
    )
    parser.add_argument(
        "--duration-s",
        type=positive_float,
        default=os.environ.get("RML_UDP_DURATION_S", "2"),
        help="messages per rate are rate*duration when --counts is omitted",
    )
    parser.add_argument(
        "--repetitions",
        type=positive_int,
        default=os.environ.get("RML_UDP_REPETITIONS", "3"),
    )
    parser.add_argument(
        "--receive-timeout-ms",
        type=positive_int,
        default=os.environ.get("RML_UDP_RECEIVE_TIMEOUT_MS", "1500"),
    )
    parser.add_argument(
        "--startup-ms",
        type=positive_int,
        default=os.environ.get("RML_UDP_STARTUP_MS", "150"),
        help="receiver warm-up before launching the sender",
    )
    parser.add_argument(
        "--sample-ms",
        type=positive_int,
        default=os.environ.get("RML_UDP_RSS_SAMPLE_MS", "10"),
        help="/proc interval for concurrent RSS and per-process VmHWM",
    )
    arguments = parser.parse_args(argv)
    try:
        arguments.rates = parse_positive_int_list(arguments.rates, "rates")
        if arguments.counts is not None:
            arguments.counts = parse_positive_int_list(
                arguments.counts, "counts"
            )
    except argparse.ArgumentTypeError as error:
        parser.error(str(error))
    return arguments


def main(argv: Sequence[str]) -> int:
    if not sys.platform.startswith("linux"):
        print("run_udp_matrix.py requires Linux wait4 and procfs", file=sys.stderr)
        return 2
    arguments = parse_arguments(argv)
    sensor_binary = arguments.sensor.resolve()
    control_binary = arguments.control.resolve()
    for name, binary in (
        ("sensor", sensor_binary),
        ("control", control_binary),
    ):
        if not binary.is_file() or not os.access(binary, os.X_OK):
            print(
                f"{name} executable not found or not executable: {binary}",
                file=sys.stderr,
            )
            return 2

    metadata = build_metadata(
        control_binary,
        sensor_binary,
        arguments.rates,
        arguments.counts,
        arguments.duration_s,
        arguments.repetitions,
        arguments.sample_ms,
    )
    results: List[Dict[str, object]] = []
    scenarios: List[Tuple[int, int]] = []
    for rate_hz in arguments.rates:
        if arguments.counts is None:
            message_count = max(2, int(round(rate_hz * arguments.duration_s)))
            scenarios.append((rate_hz, message_count))
        else:
            scenarios.extend(
                (rate_hz, message_count)
                for message_count in arguments.counts
            )

    try:
        for rate_hz, message_count in scenarios:
            for repetition in range(1, arguments.repetitions + 1):
                print(
                    "running "
                    f"transport=udp rate_hz={rate_hz} "
                    f"messages={message_count} repetition={repetition}",
                    file=sys.stderr,
                    flush=True,
                )
                results.append(
                    run_one(
                        control_binary,
                        sensor_binary,
                        rate_hz,
                        message_count,
                        repetition,
                        arguments.receive_timeout_ms,
                        arguments.startup_ms,
                        arguments.sample_ms,
                    )
                )
    except (OSError, RuntimeError, TimeoutError) as error:
        metadata["status"] = "failed"
        metadata["error"] = str(error)
        atomic_write(arguments.output, metadata, results)
        print(f"UDP benchmark failed: {error}", file=sys.stderr)
        print(arguments.output)
        return 1

    metadata["status"] = "ok"
    atomic_write(arguments.output, metadata, results)
    print(arguments.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
