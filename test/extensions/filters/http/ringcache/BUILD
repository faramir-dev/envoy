load(
    "//bazel:envoy_build_system.bzl",
    "envoy_package",
)
load(
    "//test/extensions:extensions_build_system.bzl",
    "envoy_extension_cc_test",
)

licenses(["notice"])  # Apache 2

envoy_package()

envoy_extension_cc_test(
    name = "ringcache_filter_test",
    srcs = ["ringcache_filter_test.cc"],
    extension_names = ["envoy.filters.http.ringcache"],
    rbe_pool = "linux_x64_small",
    deps = [
        "//source/extensions/filters/http/ringcache:ringcache_filter_lib",
        "//test/mocks/http:http_mocks",
        "//test/test_common:utility_lib",
    ],
)
