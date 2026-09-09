#include "source/extensions/filters/http/ringcache/config.h"

#include <chrono>

#include "envoy/registry/registry.h"
#include "envoy/singleton/manager.h"

#include "source/common/protobuf/utility.h"
#include "source/extensions/filters/http/ringcache/cache_filter.h"
#include "source/extensions/filters/http/ringcache/ring_buffer_cache.h"

#include "absl/strings/str_cat.h"

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

SINGLETON_MANAGER_REGISTRATION(ringcache_store_registry);

absl::StatusOr<Http::FilterFactoryCb>
RingCacheFilterFactory::createHttpFilterFactoryFromProtoTyped(
    const envoy::extensions::filters::http::ringcache::v3::RingCacheConfig& proto_config,
    Server::Configuration::ServerFactoryContext& context,
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

  absl::optional<std::chrono::milliseconds> ttl;
  if (proto_config.has_ttl()) {
    auto ttl_ms = DurationUtil::durationToMillisecondsNoThrow(proto_config.ttl());
    if (!ttl_ms.ok()) {
      return ttl_ms.status();
    }
    if (*ttl_ms > 0) {
      ttl = std::chrono::milliseconds(*ttl_ms);
    }
  }

  KeyConfig key_config;
  if (proto_config.has_key_config()) {
    const auto& kc = proto_config.key_config();
    key_config.include_host = !kc.exclude_host();
    key_config.include_path = !kc.exclude_path();
    for (const auto& h : kc.additional_headers()) {
      key_config.additional_headers.push_back(h);
    }
  }

  auto registry = context.singletonManager().getTyped<RingCacheStoreRegistry>(
      SINGLETON_MANAGER_REGISTERED_NAME(ringcache_store_registry),
      [] { return std::make_shared<RingCacheStoreRegistry>(); });

  // Stores are keyed by cache geometry so identical configs — across filter
  // chains and LDS updates — share one cache, while incompatible geometries
  // get separate stores. Stats deliberately use the server scope: the store
  // can outlive any individual listener's scope.
  const std::string store_key = absl::StrCat(num_partitions, "|", partition_max_bytes, "|",
                                             ttl.has_value() ? ttl->count() : 0);
  auto store = registry->getOrCreate(store_key, [&]() {
    return std::make_shared<SharedCacheStore>(num_partitions, partition_max_bytes, ttl,
                                              context.timeSource(), context.scope());
  });

  auto filter_config = std::make_shared<CacheFilterConfig>(
      std::move(key_config), std::move(store), static_cast<size_t>(max_cacheable_body_bytes),
      std::move(registry));

  return [filter_config](Http::FilterChainFactoryCallbacks& callbacks) {
    callbacks.addStreamFilter(std::make_shared<CacheFilter>(filter_config));
  };
}

REGISTER_FACTORY(RingCacheFilterFactory, Server::Configuration::NamedHttpFilterConfigFactory);

} // namespace RingCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
