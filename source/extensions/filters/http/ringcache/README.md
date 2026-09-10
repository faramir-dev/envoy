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

# Validate a config that uses the filter (see example config below)
bazel-bin/source/exe/envoy-static --mode validate -c /tmp/ringcache.yaml
```

Expected output ends with `configuration '/tmp/ringcache.yaml' OK`. Typical
failures at this stage: the extension not registered in
`source/extensions/extensions_build_config.bzl` (unknown filter name), or a
proto field mismatch (unknown/invalid field in `typed_config`).

## 3. Manual end-to-end test

Save the following as `/tmp/ringcache.yaml`. It listens on `:10000`, proxies to
a local backend on `:8000`, and exposes the admin endpoint on `:9901`:

```yaml
static_resources:
  listeners:
    - address:
        socket_address: { address: 0.0.0.0, port_value: 10000 }
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: ingress
                route_config:
                  virtual_hosts:
                    - name: backend
                      domains: ["*"]
                      routes:
                        - match: { prefix: "/" }
                          route: { cluster: backend }
                http_filters:
                  - name: envoy.filters.http.ringcache
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.ringcache.v3.RingCacheConfig
                      ttl: 60s
                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
  clusters:
    - name: backend
      type: STRICT_DNS
      load_assignment:
        cluster_name: backend
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address: { address: 127.0.0.1, port_value: 8000 }
admin:
  address:
    socket_address: { address: 127.0.0.1, port_value: 9901 }
```

Run each of these in its own terminal:

```bash
# Terminal 1: a trivial backend that logs every request it receives
python3 -m http.server 8000

# Terminal 2: Envoy, with ringcache debug logging
bazel-bin/source/exe/envoy-static -c /tmp/ringcache.yaml -l debug 2>&1 | grep ringcache

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

### Behavior checks

```bash
# Authorization bypasses the cache entirely: no hit/miss counter change,
# request always forwarded upstream.
curl -H 'Authorization: Bearer x' http://localhost:10000/

# no-cache skips the lookup: always counted as a miss, always forwarded.
curl -H 'Cache-Control: no-cache' http://localhost:10000/

# Non-GET methods are never cached.
curl -X POST http://localhost:10000/

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
