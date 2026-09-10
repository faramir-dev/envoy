#pragma once

#include <algorithm>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "envoy/buffer/buffer.h"
#include "envoy/common/time.h"
#include "envoy/http/header_map.h"
#include "envoy/singleton/instance.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/stats_macros.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/http/header_map_impl.h"

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingCache {

/**
 * All ringcache stats. @see stats_macros.h
 */
#define ALL_RINGCACHE_STATS(COUNTER)                                                               \
  COUNTER(hit)                                                                                     \
  COUNTER(miss)                                                                                    \
  COUNTER(insert)                                                                                  \
  COUNTER(insert_rejected)                                                                         \
  COUNTER(eviction)                                                                                \
  COUNTER(expired)

struct RingCacheStats {
  ALL_RINGCACHE_STATS(GENERATE_COUNTER_STRUCT)

  static RingCacheStats generate(Stats::Scope& scope) {
    return RingCacheStats{ALL_RINGCACHE_STATS(POOL_COUNTER_PREFIX(scope, "ringcache"))};
  }
};

struct CachedResponse {
  Http::ResponseHeaderMapPtr headers;
  Buffer::OwnedImpl body;
  // Full accounted footprint: body length + headers byteSize() + key length.
  size_t total_size{0};
  // Expiry deadline; only meaningful when the owning partition has a TTL.
  MonotonicTime expires_at{};
};

// A single ring-buffer partition that holds cached HTTP responses up to
// max_bytes. When the partition is full the oldest entry is evicted (FIFO).
// Entries older than the optional TTL are treated as misses and lazily
// erased. All public methods are thread-safe.
class RingBufferPartition {
public:
  RingBufferPartition(size_t max_bytes, TimeSource& time_source,
                      absl::optional<std::chrono::milliseconds> ttl, RingCacheStats& stats)
      : max_bytes_(max_bytes), time_source_(time_source), ttl_(ttl), stats_(stats),
        current_bytes_(0) {}

  // Looks up key and, if found and not expired, copies the cached headers and
  // body to the caller-supplied output parameters. Returns true on hit.
  bool get(const std::string& key, Http::ResponseHeaderMapPtr& out_headers,
           Buffer::Instance& out_body) {
    {
      absl::ReaderMutexLock lock(&mu_);
      auto it = lookup_.find(key);
      if (it == lookup_.end()) {
        return false;
      }
      if (!isExpired(*it->second)) {
        const auto& entry = *it->second;
        out_headers = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*entry.headers);
        // Copy slices from the cached buffer — a reader lock is held so the
        // entry cannot be evicted while we copy.
        out_body.add(entry.body);
        return true;
      }
    }
    // The entry expired: erase it under the writer lock so its budget is
    // reclaimed. Re-check expiry because a concurrent put() may have replaced
    // the entry between the two lock scopes.
    absl::WriterMutexLock lock(&mu_);
    auto it = lookup_.find(key);
    if (it != lookup_.end() && isExpired(*it->second)) {
      current_bytes_ -= it->second->total_size;
      order_.erase(std::remove(order_.begin(), order_.end(), key), order_.end());
      lookup_.erase(it);
      stats_.expired_.inc();
    }
    return false;
  }

  // Inserts a new entry into the partition, evicting old entries as needed.
  // Returns false (and drops the entry) if it exceeds the partition capacity.
  // The body slices are moved in (zero-copy from the caller's buffer).
  bool put(const std::string& key, Http::ResponseHeaderMapPtr headers, Buffer::Instance& body) {
    // Account for the entry's full footprint — body, headers, and key — so
    // header-heavy or empty-body entries cannot blow past the partition
    // budget (body-only accounting left headers unbounded and let zero-byte
    // entries accumulate without ever triggering eviction).
    const size_t entry_size = body.length() + headers->byteSize() + key.size();
    if (entry_size > max_bytes_) {
      stats_.insert_rejected_.inc();
      return false;
    }

    absl::WriterMutexLock lock(&mu_);

    // Remove any existing entry for the same key before making room, so its
    // stale size is not counted by the eviction loop — otherwise updating a
    // key could needlessly evict unrelated entries.
    auto existing = lookup_.find(key);
    if (existing != lookup_.end()) {
      current_bytes_ -= existing->second->total_size;
      lookup_.erase(existing);
      // Remove from order_ as well (O(n) but infrequent; simplifies eviction).
      order_.erase(std::remove(order_.begin(), order_.end(), key), order_.end());
    }

    // Evict stale entries in insertion order until there is room.
    while (current_bytes_ + entry_size > max_bytes_ && !order_.empty()) {
      evictOldest();
    }

    auto cached = std::make_shared<CachedResponse>();
    cached->headers = std::move(headers);
    cached->body.move(body); // zero-copy: transfers slices from caller buffer
    cached->total_size = entry_size;
    if (ttl_.has_value()) {
      cached->expires_at = time_source_.monotonicTime() + *ttl_;
    }

    current_bytes_ += entry_size;
    order_.push_back(key);
    lookup_[key] = std::move(cached);
    stats_.insert_.inc();
    return true;
  }

private:
  // expires_at is immutable after insert, so this is safe under either lock.
  bool isExpired(const CachedResponse& entry) const {
    return ttl_.has_value() && time_source_.monotonicTime() >= entry.expires_at;
  }

  void evictOldest() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (order_.empty()) {
      return;
    }
    std::string victim = std::move(order_.front());
    order_.pop_front();
    auto it = lookup_.find(victim);
    if (it != lookup_.end()) {
      current_bytes_ -= it->second->total_size;
      lookup_.erase(it);
      stats_.eviction_.inc();
    }
  }

  const size_t max_bytes_;
  TimeSource& time_source_;
  const absl::optional<std::chrono::milliseconds> ttl_;
  RingCacheStats& stats_;
  size_t current_bytes_ ABSL_GUARDED_BY(mu_);
  mutable absl::Mutex mu_;
  std::deque<std::string> order_ ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<std::string, std::shared_ptr<CachedResponse>> lookup_ ABSL_GUARDED_BY(mu_);
};

// Owns a fixed set of RingBufferPartition instances. Requests are routed to a
// partition by hashing the cache key, providing striped concurrency without
// any global lock.
class SharedCacheStore {
public:
  SharedCacheStore(size_t num_partitions, size_t partition_max_bytes,
                   absl::optional<std::chrono::milliseconds> ttl, TimeSource& time_source,
                   Stats::Scope& scope)
      : stats_(RingCacheStats::generate(scope)) {
    partitions_.reserve(num_partitions);
    for (size_t i = 0; i < num_partitions; ++i) {
      partitions_.push_back(
          std::make_unique<RingBufferPartition>(partition_max_bytes, time_source, ttl, stats_));
    }
  }

  RingBufferPartition& getPartition(const std::string& key) {
    const size_t idx = std::hash<std::string>{}(key) % partitions_.size();
    return *partitions_[idx];
  }

  RingCacheStats& stats() { return stats_; }

private:
  // Declared before partitions_: the partitions hold a reference to stats_.
  RingCacheStats stats_;
  std::vector<std::unique_ptr<RingBufferPartition>> partitions_;
};

// Process-wide registry of cache stores, held by the singleton manager so
// stores survive listener (LDS) updates and are shared between filter chains
// with identical cache geometry. The registry (and therefore every store in
// it) stays alive as long as at least one filter config pins it.
class RingCacheStoreRegistry : public Singleton::Instance {
public:
  std::shared_ptr<SharedCacheStore>
  getOrCreate(const std::string& config_key,
              const std::function<std::shared_ptr<SharedCacheStore>()>& create) {
    absl::MutexLock lock(&mu_);
    auto& store = stores_[config_key];
    if (store == nullptr) {
      store = create();
    }
    return store;
  }

private:
  absl::Mutex mu_;
  absl::flat_hash_map<std::string, std::shared_ptr<SharedCacheStore>> stores_ ABSL_GUARDED_BY(mu_);
};

} // namespace RingCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
