load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_extension",
    "envoy_cc_library",
    "envoy_extension_package",
)

licenses(["notice"])  # Apache 2

# Ring-buffer in-memory HTTP response cache filter.
# This filter stores HTTP GET/200 responses in a set of ring buffers sharded by
# a hash of the cache key (Host + path + optional headers). Cache entries are
# evicted in FIFO order when a partition reaches its configured capacity.

envoy_extension_package()

envoy_cc_library(
    name = "ringcache_filter_lib",
    hdrs = [
        "cache_filter.h",
        "ring_buffer_cache.h",
    ],
    deps = [
        "//envoy/buffer:buffer_interface",
        "//envoy/common:time_interface",
        "//envoy/http:filter_interface",
        "//envoy/http:header_map_interface",
        "//envoy/singleton:instance_interface",
        "//envoy/stats:scope_interface",
        "//envoy/stats:stats_interface",
        "//envoy/stats:stats_macros",
        "//source/common/buffer:buffer_lib",
        "//source/common/common:logger_lib",
        "//source/common/http:headers_lib",
        "//source/common/http:utility_lib",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/synchronization",
        "@abseil-cpp//absl/types:optional",
    ],
)

envoy_cc_extension(
    name = "config",
    srcs = ["config.cc"],
    hdrs = ["config.h"],
    deps = [
        ":ringcache_filter_lib",
        "//envoy/registry",
        "//envoy/singleton:manager_interface",
        "//source/common/protobuf:utility_lib",
        "//source/extensions/filters/http/common:factory_base_lib",
        "@envoy_api//envoy/extensions/filters/http/ringcache/v3:pkg_cc_proto",
    ],
)
