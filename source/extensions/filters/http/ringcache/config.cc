#include "source/extensions/filters/http/ringcache/config.h"

#include "envoy/registry/registry.h"

#include "source/extensions/filters/http/ringcache/cache_filter.h"
#include "source/extensions/filters/http/ringcache/ring_buffer_cache.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingCache {

namespace {
// Default values matching the proto documentation.
constexpr uint32_t kDefaultNumPartitions = 64;
constexpr uint64_t kDefaultPartitionMaxBytes = 8 * 1024 * 1024;  // 8 MiB
constexpr uint64_t kDefaultMaxCacheableBodyBytes = 1 * 1024 * 1024; // 1 MiB
} // namespace

absl::StatusOr<Http::FilterFactoryCb>
RingCacheFilterFactory::createHttpFilterFactoryFromProtoTyped(
    const envoy::extensions::filters::http::ringcache::v3::RingCacheConfig& proto_config,
    Server::Configuration::ServerFactoryContext& /*context*/,
    Server::Configuration::ExtraFactoryContext& /*extra_context*/) {

  const uint32_t num_partitions = proto_config.has_num_partitions()
                                      ? proto_config.num_partitions().value()
                                      : kDefaultNumPartitions;

  const uint64_t partition_max_bytes = proto_config.has_partition_max_bytes()
                                           ? proto_config.partition_max_bytes().value()
                                           : kDefaultPartitionMaxBytes;

  const uint64_t max_cacheable_body_bytes = proto_config.has_max_cacheable_body_bytes()
                                                ? proto_config.max_cacheable_body_bytes().value()
                                                : kDefaultMaxCacheableBodyBytes;

  KeyConfig key_config;
  if (proto_config.has_key_config()) {
    const auto& kc = proto_config.key_config();
    key_config.include_host = !kc.exclude_host();
    key_config.include_path = !kc.exclude_path();
    for (const auto& h : kc.additional_headers()) {
      key_config.additional_headers.push_back(h);
    }
  }

  auto store = std::make_shared<SharedCacheStore>(num_partitions, partition_max_bytes);
  auto filter_config =
      std::make_shared<CacheFilterConfig>(std::move(key_config), std::move(store),
                                          static_cast<size_t>(max_cacheable_body_bytes));

  return [filter_config](Http::FilterChainFactoryCallbacks& callbacks) {
    callbacks.addStreamFilter(std::make_shared<CacheFilter>(filter_config));
  };
}

REGISTER_FACTORY(RingCacheFilterFactory, Server::Configuration::NamedHttpFilterConfigFactory);

} // namespace RingCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
