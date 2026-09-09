#pragma once

#include "envoy/extensions/filters/http/ringcache/v3/ringcache.pb.h"
#include "envoy/extensions/filters/http/ringcache/v3/ringcache.pb.validate.h"

#include "source/extensions/filters/http/common/factory_base.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingCache {

class RingCacheFilterFactory
    : public Common::UnifiedFactoryBase<
          envoy::extensions::filters::http::ringcache::v3::RingCacheConfig> {
public:
  RingCacheFilterFactory() : UnifiedFactoryBase("envoy.filters.http.ringcache") {}

private:
  absl::StatusOr<Http::FilterFactoryCb> createHttpFilterFactoryFromProtoTyped(
      const envoy::extensions::filters::http::ringcache::v3::RingCacheConfig& proto_config,
      Server::Configuration::ServerFactoryContext& context,
      Server::Configuration::ExtraFactoryContext& extra_context) override;
};

DECLARE_FACTORY(RingCacheFilterFactory);

} // namespace RingCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
