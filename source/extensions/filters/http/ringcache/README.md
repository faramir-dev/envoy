# ringcache HTTP filter

RAM-only ring-buffer HTTP response cache. Serves repeated GET requests from
memory without forwarding to upstream. Does **not** use the CacheV2 storage
backend extension.

## Configuration

```yaml
http_filters:
  - name: envoy.filters.http.ringcache
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.filters.http.ringcache.v3.RingCacheConfig
      num_partitions: 64           # default: 64
      partition_max_bytes: 8388608 # default: 8 MiB (per partition)
      max_cacheable_body_bytes: 1048576  # default: 1 MiB
      ttl: 300s                    # default: unset (no time-based expiry)
      key_config:
        exclude_host: false        # default: false (Host is included)
        exclude_path: false        # default: false (path is included)
        additional_headers:
          - x-tenant
```

The proto lives at
`api/envoy/extensions/filters/http/ringcache/v3/ringcache.proto`.

**Key defaults:** because proto3 zero-value for bool is `false`, `exclude_host`
and `exclude_path` default to `false`, meaning Host and path are **included** in
the cache key by default. This is the most useful behaviour without any config.

## What is and is not cached

- **Cached:** `GET` requests that receive a `200 OK` response whose body is
  delivered through `encodeData` (including a zero-length body, which is
  cached as an empty entry), subject to the RFC 9111 guards below.
- **Not cached:** non-GET methods, requests with `Authorization` or `Cookie`,
  non-200 status codes, header-only responses (`end_stream=true` in
  `encodeHeaders`), responses ending in trailers, responses with `set-cookie` /
  `Vary` / forbidding `cache-control` directives, bodies exceeding
  `max_cacheable_body_bytes`.
- Expiry is lazy TTL (if configured) plus capacity-driven FIFO eviction.

## HTTP caching semantics (RFC 9111 guards)

Request side (`decodeHeaders`):

- **Authorization header** → cache bypassed entirely (no lookup, no store):
  a shared cache must not serve one client's authorized response to another.
- **Cookie header** → cache bypassed entirely (no lookup, no store): cookies are
  almost always per-user state and the default cache key does not include them.
  This bypass cannot be overridden via ``key_config.additional_headers``; it is
  applied before the cache key is built.
- **`cache-control: no-cache`** → skip lookup (the filter cannot revalidate);
  the response may still be stored.
- **`cache-control: no-store`** → serve from cache if hit, but never store.

Response side (`responseAllowsCaching()` in `encodeHeaders`), never stored if:

- `cache-control` contains `no-store`, `private`, or `no-cache`;
- the response sets cookies (`set-cookie` — almost always per-user state);
- the response has a `Vary` header (no variant matching implemented, so
  storing could serve the wrong representation).

Directive checks are conservative lowercase **substring** matches (not a full
directive parser), erring on the side of not caching.

Before storing, `sanitizeStoredHeaders()` removes headers that must not be
replayed with a cached body: framing headers (`content-length`,
`transfer-encoding` — sendLocalReply computes its own content-length) and
hop-by-hop headers (`connection`, `keep-alive`, `proxy-connection`,
`proxy-authenticate`, `upgrade`). It also removes `trailer`, since a cached
replay cannot include trailers.

## Architecture

### Source layout

| File | Role |
|------|------|
| `ring_buffer_cache.h` | `RingBufferPartition`, `SharedCacheStore`, `RingCacheStoreRegistry`, `RingCacheStats` — the in-memory store |
| `cache_filter.h` | `CacheFilter : Http::StreamFilter` — the per-request filter |
| `config.h` / `config.cc` | `RingCacheFilterFactory` — builds config + resolves the shared store via the singleton registry |
| `BUILD` | Bazel targets: `ringcache_filter_lib` (headers) and `config` (extension) |

Tests live at `test/extensions/filters/http/ringcache/`.

### Request / response flow

```
Request (GET)
  └─► decodeHeaders()
        ├─ has Authorization header → bypass cache entirely (RFC 9111 §3.5)
        ├─ has Cookie header → bypass cache entirely (per-user state guard)
        ├─ request cache-control: no-cache → skip lookup; no-store → skip storing
        ├─ cache HIT (not expired) → sendLocalReply() → StopIteration (upstream never sees request)
        └─ cache MISS → Continue (request forwarded upstream)

Response (200 OK)
  └─► encodeHeaders() → responseAllowsCaching() guard, then saves
        sanitized response_headers_, marks is_cacheable_
  └─► encodeData()    → accumulates body in accumulated_body_
        ├─ body > max_cacheable_body_bytes → abort, drain buffer (watermark guard)
        └─ end_stream  → put() into SharedCacheStore
  └─► encodeTrailers() → aborts caching (a cached replay cannot include
        trailers, e.g. gRPC status), drops the buffered data
```

### Cache key

`CacheFilterConfig::buildKey()` builds an **injective** key: every component is
emitted as `<length>:<bytes>;` so a value containing separator characters can
never be confused with a component boundary, and absent or excluded components
are emitted as the empty component `0:;` so each component's position is fixed.
Components, in order: scheme, host, path, then one component per configured
additional header.

### SharedCacheStore / RingBufferPartition

- `SharedCacheStore` holds a fixed array of `RingBufferPartition` instances
  allocated once at store creation and shared across all per-request
  `CacheFilter` instances via `shared_ptr<CacheFilterConfig>`.
- Requests are sharded to a partition by `std::hash<string>(key) % num_partitions`.
- Each partition uses `absl::Mutex` with reader/writer locking: `get()` takes a
  `ReaderMutexLock` for the lookup and copies out a `shared_ptr` to the entry,
  then copies the headers/body outside the lock (entries are immutable after
  insertion). `put()` takes a `WriterMutexLock`.
- **Size accounting is the full entry footprint**: body length + headers
  `byteSize()` + key length. Body-only accounting left headers unbounded and
  let zero-byte entries accumulate without ever triggering eviction.
- Eviction is FIFO: `order_` deque tracks insertion order; oldest key is removed
  when `current_bytes_ + entry_size > max_bytes_`.
- When a key is **updated** (same key inserted again), the old entry is removed
  from both `lookup_` and `order_` *before* the eviction loop runs, so its
  stale size is not counted and unrelated entries are not needlessly evicted.

### TTL

- Optional, configured via the proto `ttl` field (`google.protobuf.Duration`);
  unset or zero means entries never expire by time.
- `expires_at` is stamped at insert from `TimeSource::monotonicTime()` and is
  immutable afterwards, so it can be read under either lock.
- Expiry is **lazy**: `get()` treats an expired entry as a miss, then re-takes
  the writer lock to erase it and reclaim its budget (re-checking expiry, since
  a concurrent `put()` may have replaced the entry between the two lock
  scopes). Increments the `expired` counter.

### Singleton store registry / LDS lifetime

- `RingCacheStoreRegistry` is a `Singleton::Instance` obtained via
  `context.singletonManager().getTyped<…>()` in the factory.
- Stores are keyed by cache geometry (`num_partitions|partition_max_bytes|ttl_ms`):
  configs with the same geometry — across filter chains and LDS updates — share
  one cache, while incompatible geometries get separate stores. The store key
  deliberately excludes `key_config` and `max_cacheable_body_bytes`, so two
  filter chains that differ only in those settings still share one store.
- `CacheFilterConfig` pins the registry via `shared_ptr`, so the shared store
  (and every other store in the registry) survives LDS updates while any
  config is alive.

### Thread safety

- `CacheFilterConfig` and `SharedCacheStore` are immutable after construction
  (except the partition data behind the mutex).
- `CacheFilter` instances are per-request (one per worker thread); they share
  the config/store but own their own `accumulated_body_` and `response_headers_`.
- Concurrent reads from multiple workers are safe via `ReaderMutexLock`. The
  reader critical section is just a map lookup plus a `shared_ptr` copy; the
  response body/header copy happens outside the lock because entries are
  immutable after insertion.
- `RingCacheStoreRegistry::getOrCreate()` is guarded by its own mutex.
- In `SharedCacheStore`, `stats_` is declared before `partitions_` because the
  partitions hold a reference to it.

### OOM / watermark guard

`CacheFilter::encodeData()` aborts caching and drains `accumulated_body_` if
the accumulated length would exceed `max_cacheable_body_bytes`. The upstream
response still passes through unmodified; only the caching side-effect is
suppressed.

## Stats

`RingCacheStats` (defined via `ALL_RINGCACHE_STATS` macro), rooted at
`ringcache.` in the **server scope** — deliberately, because the store can
outlive any individual listener's scope:

| Counter | Incremented when |
|---------|------------------|
| `ringcache.hit` | lookup found a live entry |
| `ringcache.miss` | lookup found nothing (or an expired entry); not counted when lookup is skipped |
| `ringcache.insert` | entry stored |
| `ringcache.insert_rejected` | entry footprint alone exceeds partition capacity |
| `ringcache.eviction` | FIFO eviction to make room |
| `ringcache.expired` | expired entry lazily erased on lookup |

## Building

```bash
# Build the extension
bazel build //source/extensions/filters/http/ringcache:config

# Build the full binary with the extension linked in
bazel build -c dbg --config=clang //source/exe:envoy-static
```

## Registration

The extension is registered in two places:

1. `source/extensions/extensions_build_config.bzl` — maps the extension name to
   its Bazel target so it is linked into `envoy-static`.
2. `source/extensions/extensions_metadata.yaml` — declares the extension's
   category, status, and proto `type_url` for tooling and docs generation.

## Known limitations / future work

- No revalidation: `no-cache` (request or response) simply bypasses/forbids
  caching instead of conditionally revalidating.
- No `Vary` variant matching: responses with `Vary` are not cached at all.
- Cache-control parsing is a conservative substring match, not a full
  directive parser (e.g. `max-age` and `s-maxage` are ignored).
- Header-only 200 responses (where `encodeHeaders` ends the stream) are not
  cached. A zero-length body delivered via `encodeData` would be cached as an
  empty entry, since the filter only skips caching when `encodeHeaders`
  itself ends the stream.
- Responses that end with trailers are never cached.
- Expired entries are only reclaimed lazily on lookup; an idle key holds its
  budget until touched or evicted.
- The filter is downstream-only (`NamedHttpFilterConfigFactory`); upstream filter
  chain use is not supported.
- Cache is local to the Envoy process; there is no distributed / shared-memory
  layer.

## Testing guide

The extension can be tested at three levels: unit tests, config validation,
and a manual end-to-end run.

### 1. Unit tests

The unit test suite lives at
`test/extensions/filters/http/ringcache/ringcache_filter_test.cc` and covers
the filter logic, TTL expiry, FIFO eviction, and the RFC 9111 caching guards.

```bash
# Run the suite
bazel test -c dbg --config=clang //test/extensions/filters/http/ringcache:ringcache_filter_test

# See failure output inline
bazel test -c dbg --config=clang --test_output=errors \
    //test/extensions/filters/http/ringcache:ringcache_filter_test

# Run a single test case
bazel test -c dbg --config=clang \
    --test_arg=--gtest_filter='RingCache*.MyTestName' \
    //test/extensions/filters/http/ringcache:ringcache_filter_test
```

The cache store is accessed concurrently from all worker threads, so also run
the suite under sanitizers after touching `ring_buffer_cache.h`:

```bash
# Address sanitizer (memory errors, use-after-free)
bazel test -c dbg --config=asan //test/extensions/filters/http/ringcache:ringcache_filter_test

# Thread sanitizer (data races, lock misuse); TSAN needs an instrumented libc++,
# so it runs under the docker sandbox (or --config=local-tsan after local setup).
bazel test -c dbg --config=docker-tsan //test/extensions/filters/http/ringcache:ringcache_filter_test
```

### 2. Config validation

Validation mode confirms that the extension is linked into the binary, its
registration name resolves, and a given proto config parses — without opening
sockets or starting workers.

```bash
# Build the full binary with the extension linked in
bazel build -c dbg --config=clang //source/exe:envoy-static

# Validate the example config that uses the filter
bazel-bin/source/exe/envoy-static --mode validate -c configs/ringcache.yaml
```

Expected output ends with `configuration 'configs/ringcache.yaml' OK`. Typical
failures at this stage: the extension not registered in
`source/extensions/extensions_build_config.bzl` (unknown filter name), or a
proto field mismatch (unknown/invalid field in `typed_config`).

### 3. Manual end-to-end test

The example configuration lives at [`configs/ringcache.yaml`](../../../../../configs/ringcache.yaml).
It listens on `:10000`, proxies to a local backend on `:8000`, and exposes the
admin endpoint on `:9901`. The ringcache filter is configured with a 60s TTL.

Run each of these in its own terminal:

```bash
# Terminal 1: a trivial backend that logs every request it receives
python3 -m http.server 8000

# Terminal 2: Envoy, with ringcache debug logging
bazel-bin/source/exe/envoy-static -c configs/ringcache.yaml -l debug 2>&1 | grep ringcache

# Terminal 3: exercise the cache
curl -v http://localhost:10000/     # miss — forwarded upstream
curl -v http://localhost:10000/     # hit — served from memory
curl -s http://localhost:9901/stats | grep ringcache
```

#### What to look for

- The Envoy debug log prints `ringcache: miss for key '...'` for the first
  request and `ringcache: hit for key '...'` for the second.
- The backend terminal logs an access line for the first request only — on a
  hit the request never reaches upstream.
- Stats on the admin endpoint track your requests:
  `ringcache.hit`, `ringcache.miss`, `ringcache.insert`,
  `ringcache.insert_rejected`, `ringcache.eviction`, `ringcache.expired`.
  `ringcache.hit` and `ringcache.miss` are only incremented when a cache
  lookup actually runs: bypassed requests (`Authorization` or `Cookie`) and
  skipped lookups (`cache-control: no-cache`) change neither counter.

#### Behavior checks

```bash
# Authorization bypasses the cache entirely: no hit/miss counter change,
# request always forwarded upstream.
curl -H 'Authorization: Bearer x' http://localhost:10000/

# Cookie-bearing requests also bypass the cache entirely (per-user state guard).
curl -H 'Cookie: session=user-a' http://localhost:10000/

# no-cache skips the lookup (no hit/miss counter change): always forwarded,
# and the 200 OK response is still stored (ringcache.insert increments).
curl -H 'Cache-Control: no-cache' http://localhost:10000/

# no-store still runs the lookup (hit/miss counters change), but the response
# is never stored: ringcache.insert does not increment.
curl -H 'Cache-Control: no-store' http://localhost:10000/

# no-cache, no-store: no lookup (no hit/miss counter change) and no store
# (ringcache.insert does not increment): always forwarded.
curl -H 'Cache-Control: no-cache, no-store' http://localhost:10000/

# Non-GET methods are never cached.
curl -X POST http://localhost:10000/

# Lowercase HTTP/1.1 methods (e.g. "get") are also never cached: the method
# comparison is case-sensitive and only matches uppercase "GET".
curl -X get http://localhost:10000/

# TTL expiry: after the configured ttl (60s above) the next request
# increments ringcache.expired and ringcache.miss, then re-caches.
sleep 61 && curl http://localhost:10000/

# Different paths produce different cache keys.
curl http://localhost:10000/a; curl http://localhost:10000/b
```

To watch counters between checks:

```bash
watch -n1 "curl -s http://localhost:9901/stats | grep ringcache"
```

### 4. End-to-end lab: cacheability, capacity and keying

A more involved manual run that exercises the response-side RFC 9111 guards,
per-partition capacity limits, the body watermark, FIFO eviction, and
per-tenant cache keys. It uses a custom backend at
[`configs/ringcache_backend.py`](../../../../../configs/ringcache_backend.py) and the
configuration [`configs/ringcache-lab.yaml`](../../../../../configs/ringcache-lab.yaml), which
sets `num_partitions: 1` (deterministic eviction order) and a small
`partition_max_bytes` (one lab entry fits; the second evicts the first). `ttl`
is deliberately unset so entries only disappear by eviction during the lab.

Run each of these in its own terminal:

```bash
# Terminal 1: custom backend that logs every request it receives and serves
# deterministic bodies/headers per path.
python3 configs/ringcache_backend.py 8000

# Terminal 2: validate the lab config, then run Envoy with ringcache debug
# logging.
bazel-bin/source/exe/envoy-static --mode validate -c configs/ringcache-lab.yaml
bazel-bin/source/exe/envoy-static -c configs/ringcache-lab.yaml -l debug 2>&1 | grep ringcache

# Terminal 3: zero the counters before each phase.
curl -X POST http://localhost:9901/reset_counters
```

#### Phase 1 — response guards and size limits

Run each request twice. The backend logs every request (nothing is stored),
but the counters distinguish why:

```bash
curl -s http://localhost:10000/private;    curl -s http://localhost:10000/private
curl -s http://localhost:10000/setcookie;  curl -s http://localhost:10000/setcookie
curl -s http://localhost:10000/vary;       curl -s http://localhost:10000/vary
curl -s http://localhost:10000/medium;     curl -s http://localhost:10000/medium
curl -s http://localhost:10000/giant | wc -c; curl -s http://localhost:10000/giant | wc -c
```

- `/private` (`cache-control: private`), `/setcookie` (`set-cookie`), and
  `/vary` (`vary`) are never stored: `ringcache.miss` grows but
  `ringcache.insert` stays flat.
- `/medium` (900-byte body) passes the 1024-byte watermark, but the full entry
  footprint (body + key + stored headers) exceeds the 800-byte partition, so
  `ringcache.insert_rejected` increments.
- `/giant` (100 KB body) exceeds `max_cacheable_body_bytes`, so the watermark
  guard aborts caching before `put()` runs: neither `ringcache.insert` nor
  `ringcache.insert_rejected` changes, and Envoy logs
  `ringcache: response for key '...' exceeds watermark, skipping`.

#### Phase 2 — FIFO eviction

Phase 1 stores nothing, so the cache is still empty. Reset the counters and
drive the eviction loop:

```bash
curl -X POST http://localhost:9901/reset_counters

curl -s http://localhost:10000/a    # miss — insert (entry ~667 B, fits)
curl -s http://localhost:10000/a    # hit — served from memory
curl -s http://localhost:10000/b    # miss — insert, evicts /a to make room
curl -s http://localhost:10000/a    # miss again — /a was evicted
```

The backend logs `/a`, `/b`, `/a` (not the second `/a`). Stats: `hit` 1,
`miss` 3, `insert` 3, `eviction` 2. With the default 64 partitions `/a` and
`/b` would usually hash to different partitions and never evict each other;
`num_partitions: 1` makes the behavior observable.

#### Phase 3 — per-tenant keys with additional headers

Restart Envoy (terminal 2) so the RAM cache starts empty, then:

```bash
curl -X POST http://localhost:9901/reset_counters

curl -s -H 'x-tenant: alice' http://localhost:10000/tenant   # miss — caches "alice"
curl -s -H 'x-tenant: bob'   http://localhost:10000/tenant   # miss — caches "bob"
curl -s -H 'x-tenant: alice' http://localhost:10000/tenant   # hit — "alice"
curl -s                        http://localhost:10000/tenant # miss — caches "none"
```

The bodies printed are `alice`, `bob`, `alice`, `none`; the backend only logs
`alice`, `bob`, `none` because the third request is served from memory. Stats:
`hit` 1, `miss` 3, `insert` 3. Without `key_config.additional_headers:
[x-tenant]`, all four requests would share one key and the first body would be
replayed to every tenant.
