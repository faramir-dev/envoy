#pragma once

#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "envoy/buffer/buffer.h"
#include "envoy/http/header_map.h"

#include "source/common/buffer/buffer_impl.h"

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingCache {

struct CachedResponse {
  Http::ResponseHeaderMapPtr headers;
  Buffer::OwnedImpl body;
  size_t total_size{0};
};

// A single ring-buffer partition that holds cached HTTP responses up to
// max_bytes. When the partition is full the oldest entry is evicted (FIFO).
// All public methods are thread-safe.
class RingBufferPartition {
public:
  explicit RingBufferPartition(size_t max_bytes) : max_bytes_(max_bytes), current_bytes_(0) {}

  // Looks up key and, if found, copies the cached headers and body to the
  // caller-supplied output parameters. Returns true on hit.
  bool get(const std::string& key, Http::ResponseHeaderMapPtr& out_headers,
           Buffer::Instance& out_body) {
    absl::ReaderMutexLock lock(&mu_);
    auto it = lookup_.find(key);
    if (it == lookup_.end()) {
      return false;
    }
    const auto& entry = *it->second;
    out_headers = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*entry.headers);
    // Copy slices from the cached buffer — a reader lock is held so the entry
    // cannot be evicted while we copy.
    out_body.add(entry.body);
    return true;
  }

  // Inserts a new entry into the partition, evicting old entries as needed.
  // Returns false (and drops the entry) if it exceeds the partition capacity.
  // The body slices are moved in (zero-copy from the caller's buffer).
  bool put(const std::string& key, Http::ResponseHeaderMapPtr headers, Buffer::Instance& body) {
    const size_t entry_size = body.length();
    if (entry_size > max_bytes_) {
      return false;
    }

    absl::WriterMutexLock lock(&mu_);

    // Evict stale entries in insertion order until there is room.
    while (current_bytes_ + entry_size > max_bytes_ && !order_.empty()) {
      evictOldest();
    }

    // Overwrite any existing entry for the same key before inserting.
    auto existing = lookup_.find(key);
    if (existing != lookup_.end()) {
      current_bytes_ -= existing->second->total_size;
      lookup_.erase(existing);
      // Remove from order_ as well (O(n) but infrequent; simplifies eviction).
      order_.erase(std::remove(order_.begin(), order_.end(), key), order_.end());
    }

    auto cached = std::make_shared<CachedResponse>();
    cached->headers = std::move(headers);
    cached->body.move(body); // zero-copy: transfers slices from caller buffer
    cached->total_size = entry_size;

    current_bytes_ += entry_size;
    order_.push_back(key);
    lookup_[key] = std::move(cached);
    return true;
  }

private:
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
    }
  }

  const size_t max_bytes_;
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
  SharedCacheStore(size_t num_partitions, size_t partition_max_bytes) {
    partitions_.reserve(num_partitions);
    for (size_t i = 0; i < num_partitions; ++i) {
      partitions_.push_back(std::make_unique<RingBufferPartition>(partition_max_bytes));
    }
  }

  RingBufferPartition& getPartition(const std::string& key) {
    const size_t idx = std::hash<std::string>{}(key) % partitions_.size();
    return *partitions_[idx];
  }

private:
  std::vector<std::unique_ptr<RingBufferPartition>> partitions_;
};

} // namespace RingCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
