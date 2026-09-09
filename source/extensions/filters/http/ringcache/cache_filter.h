#pragma once

#include <string>
#include <vector>

#include "envoy/http/filter.h"

#include "source/common/common/logger.h"
#include "source/common/http/utility.h"
#include "source/extensions/filters/http/ringcache/ring_buffer_cache.h"

#include "absl/strings/str_cat.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingCache {

struct KeyConfig {
  bool include_host{true};
  bool include_path{true};
  std::vector<std::string> additional_headers;
};

// Per-filter-chain configuration. Shared (via shared_ptr) across all per-
// request filter instances so that the cache store is truly global to the
// filter chain.
class CacheFilterConfig {
public:
  CacheFilterConfig(KeyConfig key_config, std::shared_ptr<SharedCacheStore> store,
                    size_t max_cacheable_size)
      : key_config_(std::move(key_config)), store_(std::move(store)),
        max_cacheable_size_(max_cacheable_size) {}

  std::string buildKey(const Http::RequestHeaderMap& headers) const {
    std::string key;
    if (key_config_.include_host && headers.Host()) {
      absl::StrAppend(&key, headers.getHostValue(), "|");
    }
    if (key_config_.include_path && headers.Path()) {
      absl::StrAppend(&key, headers.getPathValue(), "|");
    }
    for (const auto& h : key_config_.additional_headers) {
      const auto entry = headers.get(Http::LowerCaseString(h));
      if (!entry.empty()) {
        absl::StrAppend(&key, h, "=", entry[0]->value().getStringView(), "|");
      }
    }
    return key;
  }

  SharedCacheStore& store() { return *store_; }
  size_t maxCacheableSize() const { return max_cacheable_size_; }

private:
  KeyConfig key_config_;
  std::shared_ptr<SharedCacheStore> store_;
  size_t max_cacheable_size_;
};

// HTTP stream filter that serves GET responses from a ring-buffer in-memory
// cache. On a cache miss the request is forwarded upstream and the 200 OK
// response is inserted into the cache for future hits.
class CacheFilter : public Http::StreamFilter,
                    public Logger::Loggable<Logger::Id::filter> {
public:
  explicit CacheFilter(std::shared_ptr<CacheFilterConfig> config) : config_(std::move(config)) {}

  // --- Http::StreamDecoderFilter ---

  Http::FilterHeadersStatus decodeHeaders(Http::RequestHeaderMap& headers,
                                          bool /*end_stream*/) override {
    // Only cache idempotent GET requests.
    if (headers.getMethodValue() != Http::Headers::get().MethodValues.Get) {
      is_cacheable_ = false;
      return Http::FilterHeadersStatus::Continue;
    }

    cache_key_ = config_->buildKey(headers);
    Http::ResponseHeaderMapPtr cached_headers;
    Buffer::OwnedImpl cached_body;

    if (config_->store().getPartition(cache_key_).get(cache_key_, cached_headers, cached_body)) {
      ENVOY_LOG(debug, "ringcache: hit for key '{}'", cache_key_);

      // Materialise the body as a string; sendLocalReply is synchronous so
      // the string_view remains valid for the duration of the call.
      std::string body_str = cached_body.toString();
      const Http::Code status_code =
          static_cast<Http::Code>(Http::Utility::getResponseStatus(*cached_headers));

      decoder_callbacks_->sendLocalReply(
          status_code, body_str,
          [hdrs = std::move(cached_headers)](Http::ResponseHeaderMap& resp) mutable {
            hdrs->iterate([&resp](const Http::HeaderEntry& e) -> Http::HeaderMap::Iterate {
              // Skip pseudo-headers; sendLocalReply sets :status itself.
              const absl::string_view name = e.key().getStringView();
              if (!name.empty() && name[0] != ':') {
                resp.addCopy(Http::LowerCaseString(std::string(name)),
                             e.value().getStringView());
              }
              return Http::HeaderMap::Iterate::Continue;
            });
          },
          absl::nullopt, "cache_hit");
      return Http::FilterHeadersStatus::StopIteration;
    }

    ENVOY_LOG(debug, "ringcache: miss for key '{}'", cache_key_);
    is_cacheable_ = true;
    return Http::FilterHeadersStatus::Continue;
  }

  Http::FilterDataStatus decodeData(Buffer::Instance&, bool) override {
    return Http::FilterDataStatus::Continue;
  }

  Http::FilterTrailersStatus decodeTrailers(Http::RequestTrailerMap&) override {
    return Http::FilterTrailersStatus::Continue;
  }

  void setDecoderFilterCallbacks(Http::StreamDecoderFilterCallbacks& callbacks) override {
    decoder_callbacks_ = &callbacks;
  }

  // --- Http::StreamEncoderFilter ---

  Http::FilterHeadersStatus encodeHeaders(Http::ResponseHeaderMap& headers,
                                          bool end_stream) override {
    if (!is_cacheable_) {
      return Http::FilterHeadersStatus::Continue;
    }
    // Only cache 200 OK; also skip if there is no body (nothing to buffer).
    if (headers.getStatusValue() != "200" || end_stream) {
      is_cacheable_ = false;
      return Http::FilterHeadersStatus::Continue;
    }
    response_headers_ = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(headers);
    return Http::FilterHeadersStatus::Continue;
  }

  Http::FilterDataStatus encodeData(Buffer::Instance& data, bool end_stream) override {
    if (!is_cacheable_) {
      return Http::FilterDataStatus::Continue;
    }

    // Watermark guard: abort caching if accumulated body exceeds the limit.
    if (accumulated_body_.length() + data.length() > config_->maxCacheableSize()) {
      ENVOY_LOG(warn, "ringcache: response for key '{}' exceeds watermark, skipping", cache_key_);
      is_cacheable_ = false;
      accumulated_body_.drain(accumulated_body_.length());
      return Http::FilterDataStatus::Continue;
    }

    // Buffer slices from the upstream response; this does not copy data if
    // slices can be referenced directly.
    accumulated_body_.add(data);

    if (end_stream && response_headers_ != nullptr) {
      config_->store().getPartition(cache_key_).put(cache_key_, std::move(response_headers_),
                                                    accumulated_body_);
    }

    return Http::FilterDataStatus::Continue;
  }

  Http::FilterTrailersStatus encodeTrailers(Http::ResponseTrailerMap&) override {
    // If the response ended via trailers, flush whatever we buffered.
    if (is_cacheable_ && response_headers_ != nullptr && accumulated_body_.length() > 0) {
      config_->store().getPartition(cache_key_).put(cache_key_, std::move(response_headers_),
                                                    accumulated_body_);
    }
    return Http::FilterTrailersStatus::Continue;
  }

  Http::Filter1xxHeadersStatus encode1xxHeaders(Http::ResponseHeaderMap&) override {
    return Http::Filter1xxHeadersStatus::Continue;
  }

  Http::FilterMetadataStatus encodeMetadata(Http::MetadataMap&) override {
    return Http::FilterMetadataStatus::Continue;
  }

  void setEncoderFilterCallbacks(Http::StreamEncoderFilterCallbacks& callbacks) override {
    encoder_callbacks_ = &callbacks;
  }

  void onDestroy() override {
    accumulated_body_.drain(accumulated_body_.length());
  }

private:
  std::shared_ptr<CacheFilterConfig> config_;
  Http::StreamDecoderFilterCallbacks* decoder_callbacks_{nullptr};
  Http::StreamEncoderFilterCallbacks* encoder_callbacks_{nullptr};

  bool is_cacheable_{false};
  std::string cache_key_;
  Http::ResponseHeaderMapPtr response_headers_;
  Buffer::OwnedImpl accumulated_body_;
};

} // namespace RingCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
