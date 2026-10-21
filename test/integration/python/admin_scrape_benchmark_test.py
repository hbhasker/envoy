import argparse
import http.server
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from test.server.admin.benchmark import scrape_benchmark as bench

BINARY = None


class ScrapeBenchmarkIntegrationTest(unittest.TestCase):

    def test_chunked_http_reading_and_pacing(self):
        payload = b"a" * 4096

        class Handler(http.server.BaseHTTPRequestHandler):

            def do_GET(self):
                if self.path == "/error":
                    self.send_error(503)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                if self.path == "/fixed":
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                if self.path == "/stall":
                    self.wfile.flush()
                    time.sleep(0.2)
                    return
                for chunk in [payload[:17], payload[17:]]:
                    self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk +
                                     b"\r\n")
                self.wfile.write(b"0\r\n\r\n")

            def log_message(self, *_):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                signatures = []
                for rate, endpoint in [(0, "/"), (32768, "/"), (0, "/fixed")]:
                    path = Path(directory) / f"body-{rate}-{len(signatures)}"
                    result = bench.scrape(f"127.0.0.1:{server.server_port}",
                                          endpoint, path, rate, 5)
                    self.assertEqual(path.read_bytes(), payload)
                    self.assertEqual(result["body_bytes"], len(payload))
                    self.assertLessEqual(result["headers_seconds"],
                                         result["first_body_seconds"])
                    if rate:
                        self.assertGreaterEqual(result["scrape_seconds"],
                                                len(payload) / rate)
                    signatures.append(result["sha256"])
                self.assertEqual(len(set(signatures)), 1)
                with self.assertRaises(RuntimeError):
                    bench.scrape(f"127.0.0.1:{server.server_port}", "/error",
                                 Path(directory) / "error", 0, 1)
                with self.assertRaises(TimeoutError):
                    bench.scrape(f"127.0.0.1:{server.server_port}", "/stall",
                                 Path(directory) / "timeout", 0, 0.05)
        finally:
            server.shutdown()
            thread.join()
            server.server_close()

    def test_real_process_sampler(self):
        with bench.ProcessSampler(os.getpid(), 0.001) as sampler:
            time.sleep(0.01)
        self.assertGreater(len(sampler.samples), 0)
        self.assertGreater(sampler.samples[0]["rss_bytes"], 0)

    def test_real_envoy_scrapes(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "results"
            command = [sys.executable, bench.__file__, "--binary", BINARY,
                       "--output", str(output), "--counters", "100", "--gauges", "10",
                       "--histograms", "10", "--scopes", "4", "--families", "5",
                       "--trials", "1", "--reader-rates", "0", "65536",
                       "--settle-seconds", "0", "--post-scrape-seconds", "0",
                       "--startup-timeout", "60", "--scrape-timeout", "60"]
            subprocess.run(command, check=True, timeout=180)
            rows = json.loads((output / "results.json").read_text())
            self.assertEqual(len(rows), 4)
            self.assertEqual(len({row["sha256"] for row in rows}), 1)
            for row in rows:
                self.assertEqual(row["validation"]["samples"],
                                 {"counter": 100, "gauge": 10, "histogram": 220})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--envoy-binary", required=True)
    arguments, remaining = parser.parse_known_args()
    BINARY = str(Path(arguments.envoy_binary).resolve(strict=True))
    unittest.main(argv=[sys.argv[0], *remaining])
