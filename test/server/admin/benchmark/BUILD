load("@rules_python//python:defs.bzl", "py_library")
load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_test_binary",
    "envoy_package",
    "envoy_py_test",
    "envoy_py_test_binary",
)

licenses(["notice"])  # Apache 2

envoy_package()

envoy_cc_test_binary(
    name = "envoy_scrape_server",
    srcs = ["envoy_scrape_server.cc"],
    deps = [
        "//envoy/server:admin_interface",
        "//envoy/server:instance_interface",
        "//envoy/stats:scope_interface",
        "//envoy/stats:store_interface",
        "//source/common/common:fmt_lib",
        "//source/common/stats:symbol_table_lib",
        "//source/common/version:version_string_lib",
        "//source/exe:envoy_main_common_with_core_extensions_lib",
        "//source/server/admin:stats_handler_lib",
    ],
)

py_library(
    name = "controller_lib",
    testonly = True,
    srcs = ["scrape_benchmark.py"],
    visibility = ["//test/integration/python:__pkg__"],
)

envoy_py_test_binary(
    name = "scrape_benchmark",
    testonly = True,
    srcs = ["scrape_benchmark.py"],
    args = ["--binary=$(location :envoy_scrape_server)"],
    data = [":envoy_scrape_server"],
    main = "scrape_benchmark.py",
    target_compatible_with = ["@platforms//os:linux"],
)

envoy_py_test(
    name = "scrape_benchmark_test",
    srcs = ["scrape_benchmark_test.py"],
    deps = [":controller_lib"],
)
