from pathlib import Path
import tempfile
import unittest
from unittest import mock

from test.server.admin.benchmark import scrape_benchmark as bench


class HarnessTest(unittest.TestCase):

    def test_distribution_validation(self):
        for distribution in [
                bench.Distribution(scopes=0),
                bench.Distribution(families=0),
                bench.Distribution(counters=-1),
                bench.Distribution(shared_percent=101),
                bench.Distribution(label_length=16385),
                bench.Distribution(counters=0, gauges=0, histograms=0)
        ]:
            with self.subTest(
                    distribution=distribution), self.assertRaises(ValueError):
                distribution.validate()
        self.assertEqual(
            bench.Distribution().environment()["ENVOY_SCRAPE_GAUGES"], "100")

    def test_proc_stat_handles_spaces_and_parentheses(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "123").mkdir()
            fields = ["S"] + ["0"] * 30
            fields[11], fields[12] = "25", "15"
            (root / "123/stat").write_text("123 (envoy (benchmark)) " +
                                           " ".join(fields))
            (root /
             "123/status").write_text("VmRSS: 1024 kB\nVmHWM: 2048 kB\n")
            with mock.patch.object(bench.os, "sysconf", return_value=100):
                result = bench.process_stats(123, root)
            self.assertEqual(result["rss_bytes"], 1048576)
            self.assertEqual(result["hwm_bytes"], 2097152)
            self.assertEqual(result["cpu_seconds"], 0.4)

    def test_family_mapping_spans_scopes(self):
        distribution = bench.Distribution(scopes=2,
                                          families=2,
                                          shared_percent=50)
        self.assertEqual(bench.expected_family(distribution, "counter", 0),
                         bench.expected_family(distribution, "counter", 1))
        self.assertNotEqual(bench.expected_family(distribution, "counter", 2),
                            bench.expected_family(distribution, "counter", 3))

    def validate_text(self, text, distribution, buckets=0):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "body"
            path.write_text(text)
            return bench.validate_body(path, distribution, buckets)

    def test_series_validation_and_grouping(self):
        distribution = bench.Distribution(counters=2,
                                          gauges=0,
                                          histograms=0,
                                          scopes=2,
                                          families=1,
                                          shared_percent=100)
        family = "envoy_scrape_bench_shared_counter_family_0"
        body = (f"# TYPE {family} counter\n"
                f'{family}{{cluster="scope_0000",instance="0000000000"}} 1\n'
                f'{family}{{cluster="scope_0001",instance="0000000001"}} 2\n')
        self.assertEqual(
            self.validate_text(body, distribution)["samples"]["counter"], 2)
        for malformed in [
                body + f"# TYPE {family} counter\n",
                body.replace('scope_0001', 'scope_0000'),
                body.replace('} 2', '} 99'),
                body.splitlines(True)[0] + body.splitlines(True)[1] * 2,
                body.replace('shared_counter', 'local_scope_0000_counter')
        ]:
            with self.subTest(body=malformed), self.assertRaises(ValueError):
                self.validate_text(malformed, distribution)

    def test_histogram_and_gauge_validation(self):
        distribution = bench.Distribution(counters=0,
                                          gauges=1,
                                          histograms=1,
                                          scopes=1,
                                          families=1,
                                          shared_percent=0)
        gauge = "envoy_scrape_bench_local_scope_0000_gauge_family_0"
        histogram = "envoy_scrape_bench_local_scope_0000_histogram_family_0"
        labels = 'cluster="scope_0000",instance="0000000000"'
        body = (
            f"# TYPE {gauge} gauge\n{gauge}{{{labels}}} 0\n"
            f"# TYPE {histogram} histogram\n"
            f'{histogram}_bucket{{{labels},le="+Inf"}} 8\n'
            f"{histogram}_sum{{{labels}}} 6676\n{histogram}_count{{{labels}}} 8\n"
        )
        self.assertEqual(
            self.validate_text(body, distribution)["samples"], {
                "counter": 0,
                "gauge": 1,
                "histogram": 3
            })

    def test_non_loopback_is_rejected(self):
        with self.assertRaises(ValueError), bench.connection(
                "example.com:80", 1):
            pass

    def test_summary_uses_medians(self):
        trials = [{
            "mode": "streaming",
            "read_bytes_per_second": 0,
            "sampled_peak_rss_bytes": value * 1048576,
            "sampled_rss_growth_bytes": value * 1048576,
            "scrape_cpu_seconds": value / 1000,
            "first_body_seconds": value / 1000,
            "scrape_seconds": value / 1000
        } for value in [1, 9, 2]]
        self.assertIn("| 0 | streaming | 3 | 2.0 | 2.0 | 2.0 | 2.0 | 2.0 |",
                      bench.summarize(trials))


if __name__ == "__main__":
    unittest.main()
