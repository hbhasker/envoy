# Admin Prometheus scrape benchmark

This test-only harness measures an actual Envoy admin scrape over loopback TCP.
The fixture populates the production stats store with configurable counters,
gauges and histograms. It exposes the normal streaming Prometheus route and a
benchmark-only route using the retained buffered formatter as an output oracle.
No production handler or endpoint is added.

## Build and run

Run in Envoy's supported build environment, using the normal Docker wrapper when
needed. Build optimized binaries for measurements:

```sh
bazel build -c opt //test/server/admin/benchmark:envoy_scrape_server
bazel run -c opt //test/server/admin/benchmark:scrape_benchmark -- \
  --output /absolute/path/to/new-results \
  --counters 1000000 --gauges 0 --histograms 0 \
  --scopes 100 --families 1000 --shared-percent 100 --label-length 128 \
  --trials 3 --reader-rates 0 10485760
```

The output path must not already exist. It must be writable from the environment
running the benchmark; paths inside a build container refer to that container.
The controller can also be run with Python 3.11+ and an explicit `--binary` path.
Use `--help` for timeouts, settling, worker count and receive-buffer controls.
This measurement tool uses Linux `/proc`; it is not a cross-platform benchmark.

For a mixed workload, use `--counters 100000 --histograms 10000 --families 200`.
Each histogram records eight observations and merges before readiness. It emits
22 classic-histogram samples. The family setting is a slot count per metric type;
the realized family count depends on cardinality, scopes and shared percentage.
Values remain fixed during each measured scrape. This is synthetic data, not
application traffic or a claim of production representativeness.

## Validation

```sh
bazel test //test/server/admin/benchmark:scrape_benchmark_test
bazel test -c opt //test/integration/python:admin_scrape_benchmark_test
```

Unit tests cover parsing, metric validation, configuration and summaries without
real processes, sleeps or network access. The separate Linux integration target
covers process sampling, HTTP chunk reading/pacing and four small real Envoy
scrapes. Large cardinalities are opt-in manual runs, not CI performance gates.

## Measurements and caveats

Each scrape runs in a fresh Envoy process. The controller records server CPU,
baseline/peak/growth/retained RSS, first-body and completion time, binary SHA256,
arguments, response bytes/checksum, sample counts and family counts. It validates
buffered/streaming output equivalence after measurement. Admin binds IPv4 loopback
on an ephemeral port. Shutdown targets only the child process started by the tool.

The controller writes the response incrementally to disk, without retaining the
whole response in memory. Paced reads plus a bounded receive buffer exercise TCP
backpressure; a zero reader rate means unthrottled. Response-file I/O and hashing
are included in elapsed transfer time. Large runs need sufficient RAM, disk and
timeouts. These are individual scrapes, not a periodic scrape-interval workload.

RSS includes the resident stats store and allocator retention. Sampling can miss
brief peaks; process-lifetime high-water RSS also includes fixture initialization.
Client file page cache is not server RSS. CPU has scheduler-tick granularity and
shared hosts introduce noise. The tool makes no fixed CPU/RSS improvement assertion
and works with or without admin response backpressure. It adds no profiler dependency.
