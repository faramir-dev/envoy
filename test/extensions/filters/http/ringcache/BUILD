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
        "//source/common/protobuf:message_validator_lib",
        "//source/common/stats:isolated_store_lib",
        "//source/extensions/filters/http/ringcache:config",
        "//source/extensions/filters/http/ringcache:ringcache_filter_lib",
        "//test/mocks/http:http_mocks",
        "//test/mocks/server:server_factory_context_mocks",
        "//test/test_common:simulated_time_system_lib",
        "//test/test_common:thread_factory_for_test_lib",
        "//test/test_common:utility_lib",
        "@abseil-cpp//absl/strings",
        "@envoy_api//envoy/extensions/filters/http/ringcache/v3:pkg_cc_proto",
    ],
)
