# ringcache HTTP filter — testing guide

RAM-only ring-buffer HTTP response cache filter. This document describes how to
test the extension at three levels: unit tests, config validation, and a manual
end-to-end run.

## 1. Unit tests

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
bazel test --config=clang-asan //test/extensions/filters/http/ringcache:ringcache_filter_test

# Thread sanitizer (data races, lock misuse)
bazel test --config=clang-tsan //test/extensions/filters/http/ringcache:ringcache_filter_test
```

## 2. Config validation

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

## 3. Manual end-to-end test

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

### What to look for

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

### Behavior checks

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

## 4. End-to-end lab: cacheability, capacity and keying

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

### Phase 1 — response guards and size limits

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

### Phase 2 — FIFO eviction

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

### Phase 3 — per-tenant keys with additional headers

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
