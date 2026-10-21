#!/usr/bin/env python3
"""Measure an actual Envoy admin HTTP scrape without buffering its body in the client."""

import argparse
import contextlib
import dataclasses
import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
import re
import signal
import socket
import statistics
import subprocess
import threading
import time


@dataclasses.dataclass(frozen=True)
class Distribution:
    counters: int = 1000
    gauges: int = 100
    histograms: int = 100
    scopes: int = 8
    families: int = 10
    shared_percent: int = 80
    label_length: int = 64

    def validate(self):
        if min(self.counters, self.gauges, self.histograms,
               self.label_length) < 0:
            raise ValueError("counts and label length must be nonnegative")
        if self.scopes < 1 or self.families < 1:
            raise ValueError("scopes and families must be positive")
        if not 0 <= self.shared_percent <= 100 or self.label_length > 16384:
            raise ValueError("shared percentage or label length out of range")
        if self.counters + self.gauges + self.histograms == 0:
            raise ValueError("at least one metric is required")

    def environment(self):
        self.validate()
        return {
            f"ENVOY_SCRAPE_{key.upper()}": str(value)
            for key, value in dataclasses.asdict(self).items()
        }


def process_stats(pid, proc_root=Path("/proc")):
    # comm can contain spaces and parentheses; field 3 follows the final ') '.
    fields = (proc_root / str(pid) / "stat").read_text().rsplit(") ",
                                                                1)[1].split()
    status = {}
    for line in (proc_root / str(pid) / "status").read_text().splitlines():
        if line.startswith(("VmRSS:", "VmHWM:")):
            key, value, _ = line.split()
            status[key.rstrip(":")] = int(value) * 1024
    return {
        "cpu_seconds":
        (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
        "rss_bytes":
        status["VmRSS"],
        "hwm_bytes":
        status["VmHWM"]
    }


class ProcessSampler:

    def __init__(self, pid, interval):
        self.pid = pid
        self.interval = interval
        self.stop_event = threading.Event()
        self.samples = []
        self.failure = None
        self.thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self):
        while not self.stop_event.is_set():
            try:
                self.samples.append({
                    "monotonic_seconds": time.monotonic(),
                    **process_stats(self.pid)
                })
            except Exception as error:
                self.failure = error
                return
            self.stop_event.wait(self.interval)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop_event.set()
        self.thread.join()
        if self.failure is not None:
            raise RuntimeError(
                "server memory sampler failed") from self.failure


@contextlib.contextmanager
def connection(address, timeout, receive_buffer=None):
    host, port = address.rsplit(":", 1)
    if host != "127.0.0.1":
        raise ValueError("benchmark admin address must be IPv4 loopback")
    conn = http.client.HTTPConnection(host, int(port), timeout=timeout)
    try:
        conn.connect()
        if receive_buffer is not None:
            conn.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                                 receive_buffer)
        yield conn
    finally:
        conn.close()


def wait_until_ready(process, address_file, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"Envoy exited during startup: {process.returncode}; see server.log"
            )
        try:
            address = address_file.read_text().strip()
            with connection(address, 1) as conn:
                conn.request("GET", "/benchmark/ready")
                response = conn.getresponse()
                metadata = json.loads(response.read(4096))
                if response.status == 200 and metadata.get("ready") is True:
                    return address, metadata
        except (OSError, ValueError, http.client.HTTPException):
            pass
        time.sleep(0.1)
    raise TimeoutError("Envoy readiness timed out; see server.log")


def scrape(address, endpoint, body_path, rate, timeout, receive_buffer=262144):
    digest = hashlib.sha256()
    size = 0
    first_body = None
    started = time.monotonic()
    # A small receive buffer plus paced reads applies TCP backpressure to slow trials.
    with connection(address, timeout,
                    receive_buffer if rate else None) as conn:
        conn.request("GET",
                     endpoint,
                     headers={
                         "Accept": "text/plain",
                         "Connection": "close"
                     })
        read_socket = conn.sock
        actual_receive_buffer = read_socket.getsockopt(socket.SOL_SOCKET,
                                                       socket.SO_RCVBUF)
        response = conn.getresponse()
        header_time = time.monotonic()
        if response.status != 200:
            raise RuntimeError(f"scrape failed: HTTP {response.status}")
        if not response.getheader("Content-Type", "").startswith("text/plain"):
            raise RuntimeError("scrape did not return Prometheus text")
        headers = dict(response.getheaders())
        read_size = max(1024, min(65536, rate // 20)) if rate else 65536
        with body_path.open("xb") as output:
            while True:
                remaining_timeout = timeout - (time.monotonic() - started)
                if remaining_timeout <= 0:
                    raise TimeoutError("scrape exceeded total timeout")
                if response.isclosed():
                    break
                # Bound each socket read by the remaining wall-time budget, not a fresh timeout.
                read_socket.settimeout(remaining_timeout)
                chunk = response.read1(read_size)
                if not chunk:
                    break
                if first_body is None:
                    first_body = time.monotonic()
                output.write(chunk)
                digest.update(chunk)
                size += len(chunk)
                if rate:
                    remaining = size / rate - (time.monotonic() - first_body)
                    if remaining > 0:
                        time.sleep(
                            min(remaining,
                                max(0,
                                    timeout - (time.monotonic() - started))))
    finished = time.monotonic()
    if size == 0:
        raise RuntimeError("empty metrics response")
    return {
        "body_bytes": size,
        "sha256": digest.hexdigest(),
        "headers": headers,
        "requested_receive_buffer_bytes": receive_buffer if rate else None,
        "initial_socket_receive_buffer_bytes": actual_receive_buffer,
        "headers_seconds": header_time - started,
        "first_body_seconds": first_body - started,
        "scrape_seconds": finished - started
    }


SAMPLE = re.compile(r"^([A-Za-z_:][A-Za-z0-9_:]*)\{(.*)\} ([^ ]+)$")
LABEL = re.compile(r'(?:^|,)cluster="scope_(\d+)"(?:,|$)')
INSTANCE = re.compile(r'(?:^|,)instance="(\d+)"(?:,|$)')


def expected_family(distribution, metric_type, instance):
    scope = instance % distribution.scopes
    family = (instance // distribution.scopes) % distribution.families
    shared = distribution.families * distribution.shared_percent // 100
    middle = "shared" if family < shared else f"local_scope_{scope:04d}"
    return f"envoy_scrape_bench_{middle}_{metric_type}_family_{family}"


def validate_body(body_path, distribution, finite_buckets):
    """Validation is outside measurement; checks generated series without an O(N) ID set."""
    types = {}
    active_family = None
    samples = {"counter": 0, "gauge": 0, "histogram": 0}
    # Per-family counts and ID sums/XORs detect common missing/duplicate-series errors, while
    # byte-for-byte checksums across variants establish equivalence to the buffered oracle.
    ids = {"counter": [0, 0], "gauge": [0, 0], "histogram": [0, 0]}
    counts = dataclasses.asdict(distribution)
    with body_path.open() as body:
        for line in body:
            line = line.rstrip("\n")
            if line.startswith("# TYPE "):
                _, _, name, metric_type = line.split()
                if name in types or metric_type not in samples:
                    raise ValueError(
                        "duplicate TYPE or unexpected metric type")
                types[name] = metric_type
                active_family = name
                continue
            match = SAMPLE.fullmatch(line)
            if match is None:
                raise ValueError(f"unexpected exposition line: {line[:160]}")
            name, labels, value = match.groups()
            family = name
            if family not in types:
                for suffix in ("_bucket", "_sum", "_count"):
                    if name.endswith(suffix) and name[:-len(suffix)] in types:
                        family = name[:-len(suffix)]
                        break
            if family != active_family:
                raise ValueError(
                    "sample outside its contiguous declared family")
            metric_type = types[family]
            scope_match, instance_match = LABEL.search(
                labels), INSTANCE.search(labels)
            if scope_match is None or instance_match is None:
                raise ValueError("missing cluster/instance labels")
            instance = int(instance_match[1])
            if instance >= counts[metric_type + "s"]:
                raise ValueError("instance outside requested cardinality")
            if int(scope_match[1]) != instance % distribution.scopes:
                raise ValueError("wrong scope tag")
            if family != expected_family(distribution, metric_type, instance):
                raise ValueError("wrong shared/local family mapping")
            if metric_type == "counter" and float(
                    value) != 1 + instance % 1000:
                raise ValueError("wrong counter value")
            if metric_type == "gauge" and float(value) != instance % 100:
                raise ValueError("wrong gauge value")
            if metric_type == "histogram" and name.endswith(
                    "_count") and float(value) != 8:
                raise ValueError("wrong histogram observation count")
            samples[metric_type] += 1
            if metric_type != "histogram" or name.endswith("_count"):
                ids[metric_type][0] += instance
                ids[metric_type][1] ^= instance
    expected = {
        "counter": distribution.counters,
        "gauge": distribution.gauges,
        "histogram": distribution.histograms * (finite_buckets + 3)
    }
    if samples != expected:
        raise ValueError(
            f"wrong exported sample counts: {samples} != {expected}")
    for metric_type, (total, xor) in ids.items():
        count = counts[metric_type + "s"]
        expected_xor = [count - 1, 1, count, 0][(count - 1) %
                                                4] if count else 0
        if total != count * (count - 1) // 2 or xor != expected_xor:
            raise ValueError(f"missing/duplicate {metric_type} instances")
    expected_types = {}
    for metric_type in samples:
        # Every scope/family assignment repeats after scopes * families instances.
        for instance in range(
                min(counts[metric_type + "s"],
                    distribution.scopes * distribution.families)):
            expected_types[expected_family(distribution, metric_type,
                                           instance)] = metric_type
    if types != expected_types:
        raise ValueError("wrong set of metric families")
    return {"families": len(types), "samples": samples}


def stop_server(process, address):
    if process.poll() is not None:
        return
    if address:
        try:
            with connection(address, 2) as conn:
                conn.request("POST", "/quitquitquit")
                conn.getresponse().read(4096)
        except (OSError, http.client.HTTPException):
            pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            process.wait()
            return
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()


def run_trial(binary, directory, distribution, mode, rate, args):
    directory.mkdir()
    config = {
        "admin": {
            "address": {
                "socket_address": {
                    "address": "127.0.0.1",
                    "port_value": 0
                }
            }
        },
        "stats_flush_interval": "299s",
        "static_resources": {}
    }
    config_path = directory / "bootstrap.json"
    config_path.write_text(json.dumps(config))
    address_file = directory / "admin-address.txt"
    command = [
        str(binary), "-c",
        str(config_path), "--admin-address-path",
        str(address_file), "--disable-hot-restart", "--concurrency",
        str(args.workers), "--log-level", "error"
    ]
    address = None
    with (directory / "server.log").open("xb") as log:
        process = subprocess.Popen(command,
                                   stdout=log,
                                   stderr=log,
                                   start_new_session=True,
                                   env={
                                       **os.environ,
                                       **distribution.environment()
                                   })
        try:
            address, metadata = wait_until_ready(process, address_file,
                                                 args.startup_timeout)
            # No warm-up exposition: it would retain large formatter allocations before baseline.
            time.sleep(args.settle_seconds)
            baseline = process_stats(process.pid)
            endpoint = (
                "/stats/prometheus" if mode == "streaming" else
                "/benchmark/prometheus-buffered") + "?filter=^scrape_bench"
            with ProcessSampler(process.pid, args.sample_interval) as sampler:
                transfer = scrape(address, endpoint,
                                  directory / "body.prometheus", rate,
                                  args.scrape_timeout, args.receive_buffer)
                completed = process_stats(process.pid)
                # Keep sampling briefly to show whether buffers are retained after the scrape.
                time.sleep(args.post_scrape_seconds)
                retained = process_stats(process.pid)
            if process.poll() is not None:
                raise RuntimeError(
                    "Envoy exited unexpectedly during the scrape")
        finally:
            stop_server(process, address)
    if process.returncode != 0:
        raise RuntimeError(
            f"Envoy did not shut down cleanly: {process.returncode}")
    checks = validate_body(directory / "body.prometheus", distribution,
                           metadata["finite_buckets"])
    peak = max([baseline["rss_bytes"], completed["rss_bytes"]] +
               [sample["rss_bytes"] for sample in sampler.samples])
    result = {
        "mode": mode,
        "read_bytes_per_second": rate,
        "distribution": dataclasses.asdict(distribution),
        "baseline": baseline,
        "completed": completed,
        "retained": retained,
        "sampled_peak_rss_bytes": peak,
        "sampled_rss_growth_bytes": max(0, peak - baseline["rss_bytes"]),
        "scrape_cpu_seconds":
        completed["cpu_seconds"] - baseline["cpu_seconds"],
        "cpu_clock_resolution_seconds": 1 / os.sysconf("SC_CLK_TCK"),
        "sample_interval_seconds": args.sample_interval,
        "validation": checks,
        "finite_buckets": metadata["finite_buckets"],
        **transfer
    }
    (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    with (directory / "memory.jsonl").open("x") as output:
        for sample in sampler.samples:
            output.write(json.dumps(sample) + "\n")
    return result


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--binary", type=Path, required=True)
    result.add_argument("--output",
                        type=Path,
                        required=True,
                        help="new results directory")
    for key, value in dataclasses.asdict(Distribution()).items():
        result.add_argument("--" + key.replace("_", "-"),
                            type=int,
                            default=value)
    result.add_argument("--trials", type=int, default=3)
    result.add_argument("--reader-rates",
                        type=int,
                        nargs="+",
                        default=[0, 1024 * 1024],
                        help="bytes/s; 0 means unthrottled")
    result.add_argument("--workers", type=int, default=2)
    result.add_argument(
        "--receive-buffer",
        type=int,
        default=262144,
        help="requested socket receive buffer for throttled trials")
    result.add_argument("--startup-timeout", type=float, default=600)
    result.add_argument("--scrape-timeout", type=float, default=600)
    result.add_argument("--settle-seconds", type=float, default=1)
    result.add_argument("--post-scrape-seconds", type=float, default=0.2)
    result.add_argument("--sample-interval", type=float, default=0.005)
    return result


def summarize(results):
    lines = [
        "| Reader bytes/s | Mode | Trials | Peak RSS MiB | RSS growth MiB | CPU ms | First body ms | Scrape ms |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |"
    ]
    for rate in sorted({result["read_bytes_per_second"]
                        for result in results}):
        for mode in ["buffered", "streaming"]:
            trials = [
                result for result in results if result["mode"] == mode
                and result["read_bytes_per_second"] == rate
            ]
            if not trials:
                continue
            values = [
                statistics.median(result[field] for result in trials) * scale
                for field, scale in [(
                    "sampled_peak_rss_bytes",
                    1 / 1048576), ("sampled_rss_growth_bytes",
                                   1 / 1048576), (
                                       "scrape_cpu_seconds",
                                       1000), ("first_body_seconds",
                                               1000), ("scrape_seconds", 1000)]
            ]
            lines.append(f"| {rate} | {mode} | {len(trials)} | " +
                         " | ".join(f"{value:.1f}" for value in values) + " |")
    lines.append(
        "\nMedians; process RSS only. 0 bytes/s means unthrottled. "
        "Synthetic metrics, real Envoy HTTP transport. See README for measurement caveats."
    )
    return "\n".join(lines) + "\n"


def main():
    args = parser().parse_args()
    distribution = Distribution(**{
        key: getattr(args, key)
        for key in dataclasses.asdict(Distribution())
    })
    distribution.validate()
    if args.trials < 1 or args.workers < 1 or min(args.reader_rates) < 0:
        raise ValueError("invalid trials, workers, or reader rates")
    if len(set(args.reader_rates)) != len(args.reader_rates):
        raise ValueError("reader rates must be unique")
    if not 4096 <= args.receive_buffer <= 2147483647:
        raise ValueError(
            "receive buffer must be between 4096 and 2147483647 bytes")
    if min(args.startup_timeout, args.scrape_timeout,
           args.sample_interval) <= 0:
        raise ValueError("timeouts and sample interval must be positive")
    if min(args.settle_seconds, args.post_scrape_seconds) < 0:
        raise ValueError("settling durations must be nonnegative")
    binary = args.binary.resolve(strict=True)
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    with binary.open("rb") as executable:
        binary_digest = hashlib.file_digest(executable, "sha256").hexdigest()
    metadata = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "binary": str(binary),
        "binary_sha256": binary_digest,
        "arguments": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
        }
    }
    (args.output /
     "run.json").write_text(json.dumps(metadata, indent=2) + "\n")
    results = []
    expected_output = None
    for rate in args.reader_rates:
        for trial in range(1, args.trials + 1):
            modes = ["buffered", "streaming"
                     ] if trial % 2 else ["streaming", "buffered"]
            for mode in modes:
                directory = args.output / f"{mode}-rate{rate}-trial{trial}"
                result = run_trial(binary, directory, distribution, mode, rate,
                                   args)
                signature = (result["body_bytes"], result["sha256"],
                             result["validation"])
                expected_output = expected_output or signature
                if signature != expected_output:
                    raise ValueError(
                        f"buffered/streaming exposition mismatch: {directory}")
                results.append(result)
                (args.output / "results.json"
                 ).write_text(json.dumps(results, indent=2) + "\n")
                print(
                    f"{directory.name}: peak={result['sampled_peak_rss_bytes'] / 1048576:.1f} MiB "
                    f"growth={result['sampled_rss_growth_bytes'] / 1048576:.1f} MiB "
                    f"CPU={result['scrape_cpu_seconds']:.3f}s "
                    f"scrape={result['scrape_seconds']:.3f}s bytes={result['body_bytes']}",
                    flush=True)
    print(
        f"All {len(results)} scrapes passed with identical output. Results: {args.output}"
    )
    summary = summarize(results)
    (args.output / "summary.md").write_text(summary)
    print(summary)


if __name__ == "__main__":
    main()
