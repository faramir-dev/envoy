#include "envoy/extensions/filters/http/ringcache/v3/ringcache.pb.h"

#include "source/common/protobuf/message_validator_impl.h"
#include "source/common/stats/isolated_store_impl.h"
#include "source/extensions/filters/http/ringcache/cache_filter.h"
#include "source/extensions/filters/http/ringcache/config.h"
#include "source/extensions/filters/http/ringcache/ring_buffer_cache.h"

#include "test/mocks/http/mocks.h"
#include "test/mocks/server/server_factory_context.h"
#include "test/test_common/simulated_time_system.h"
#include "test/test_common/thread_factory_for_test.h"
#include "test/test_common/utility.h"

#include "absl/strings/str_cat.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using testing::_;
using testing::NiceMock;
using testing::Return;
using testing::SaveArg;

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingCache {

// ---- RingBufferPartition unit tests ----

class RingBufferPartitionTest : public testing::Test {
protected:
  static Http::ResponseHeaderMapPtr makeHeaders(absl::string_view status) {
    auto hdrs = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(
        Http::TestResponseHeaderMapImpl{{":status", std::string(status)}});
    return hdrs;
  }

  static Buffer::OwnedImpl makeBody(absl::string_view data) {
    Buffer::OwnedImpl buf;
    buf.add(data);
    return buf;
  }

  // Mirrors the accounting in RingBufferPartition::put(): entry footprint is
  // body + headers byteSize + key length (headers here are always ":status".)
  static size_t entrySize(absl::string_view key, absl::string_view body) {
    return body.size() + makeHeaders("200")->byteSize() + key.size();
  }

  std::unique_ptr<RingBufferPartition>
  makePartition(size_t max_bytes, absl::optional<std::chrono::milliseconds> ttl = absl::nullopt) {
    return std::make_unique<RingBufferPartition>(max_bytes, time_system_, ttl, stats_);
  }

  Stats::IsolatedStoreImpl stats_store_;
  RingCacheStats stats_{RingCacheStats::generate(*stats_store_.rootScope())};
  Event::SimulatedTimeSystem time_system_;
};

TEST_F(RingBufferPartitionTest, MissOnEmpty) {
  auto p = makePartition(1024);
  Http::ResponseHeaderMapPtr h;
  Buffer::OwnedImpl b;
  EXPECT_FALSE(p->get("key", h, b));
}

TEST_F(RingBufferPartitionTest, PutAndGet) {
  auto p = makePartition(1024);
  auto body = makeBody("hello");
  p->put("k1", makeHeaders("200"), body);

  Http::ResponseHeaderMapPtr out_headers;
  Buffer::OwnedImpl out_body;
  ASSERT_TRUE(p->get("k1", out_headers, out_body));
  EXPECT_EQ(out_headers->getStatusValue(), "200");
  EXPECT_EQ(out_body.toString(), "hello");
}

TEST_F(RingBufferPartitionTest, EvictsOldestWhenFull) {
  // Size the partition to hold exactly two entries; a third insert must
  // evict the oldest. Entry size = body + headers + key.
  const size_t entry_size = entrySize("k1", "ab");
  auto p = makePartition(2 * entry_size);

  auto b1 = makeBody("ab");
  auto b2 = makeBody("cd");
  auto b3 = makeBody("ef");

  p->put("k1", makeHeaders("200"), b1);
  p->put("k2", makeHeaders("200"), b2);
  p->put("k3", makeHeaders("200"), b3);

  Http::ResponseHeaderMapPtr h;
  Buffer::OwnedImpl b;
  EXPECT_FALSE(p->get("k1", h, b)); // evicted
  EXPECT_TRUE(p->get("k2", h, b));
  EXPECT_TRUE(p->get("k3", h, b));
}

TEST_F(RingBufferPartitionTest, DropsEntryExceedingCapacity) {
  auto p = makePartition(3);
  auto body = makeBody("toolong");
  EXPECT_FALSE(p->put("k", makeHeaders("200"), body));

  Http::ResponseHeaderMapPtr h;
  Buffer::OwnedImpl b;
  EXPECT_FALSE(p->get("k", h, b));
}

TEST_F(RingBufferPartitionTest, UpdateExistingKey) {
  auto p = makePartition(1024);
  auto b1 = makeBody("first");
  auto b2 = makeBody("second");
  p->put("key", makeHeaders("200"), b1);
  p->put("key", makeHeaders("200"), b2);

  Http::ResponseHeaderMapPtr h;
  Buffer::OwnedImpl b;
  ASSERT_TRUE(p->get("key", h, b));
  EXPECT_EQ(b.toString(), "second");
}

TEST_F(RingBufferPartitionTest, SizeAccountingIncludesHeadersAndKey) {
  // The body alone fits, but body + headers + key exceeds capacity, so the
  // entry must be rejected.
  const size_t entry_size = entrySize("k", "ab");
  auto p = makePartition(entry_size - 1);
  auto body = makeBody("ab");
  EXPECT_FALSE(p->put("k", makeHeaders("200"), body));

  Http::ResponseHeaderMapPtr h;
  Buffer::OwnedImpl b;
  EXPECT_FALSE(p->get("k", h, b));
}

TEST_F(RingBufferPartitionTest, EmptyBodyEntriesStillConsumeBudget) {
  // Zero-length bodies must still count (headers + key), so filling the
  // partition with them triggers eviction instead of growing unboundedly.
  const size_t entry_size = entrySize("k1", "");
  auto p = makePartition(2 * entry_size);

  auto b1 = makeBody("");
  auto b2 = makeBody("");
  auto b3 = makeBody("");
  p->put("k1", makeHeaders("200"), b1);
  p->put("k2", makeHeaders("200"), b2);
  p->put("k3", makeHeaders("200"), b3);

  Http::ResponseHeaderMapPtr h;
  Buffer::OwnedImpl b;
  EXPECT_FALSE(p->get("k1", h, b)); // evicted
  EXPECT_TRUE(p->get("k2", h, b));
  EXPECT_TRUE(p->get("k3", h, b));
}

TEST_F(RingBufferPartitionTest, UpdatingKeyDoesNotEvictOtherEntries) {
  // Partition sized to hold exactly ka and kb. Re-inserting kb (same size)
  // must reclaim kb's old footprint before the eviction loop runs; counting
  // the stale copy would wrongly evict ka.
  const size_t entry_size = entrySize("ka", "ab");
  auto p = makePartition(2 * entry_size);

  auto b1 = makeBody("ab");
  auto b2 = makeBody("cd");
  auto b2_new = makeBody("ef");
  p->put("ka", makeHeaders("200"), b1);
  p->put("kb", makeHeaders("200"), b2);
  p->put("kb", makeHeaders("200"), b2_new);

  Http::ResponseHeaderMapPtr h;
  Buffer::OwnedImpl b;
  EXPECT_TRUE(p->get("ka", h, b)) << "update of kb must not evict ka";
  Buffer::OwnedImpl b_kb;
  ASSERT_TRUE(p->get("kb", h, b_kb));
  EXPECT_EQ(b_kb.toString(), "ef");
}

TEST_F(RingBufferPartitionTest, TtlExpiresEntries) {
  auto p = makePartition(4096, std::chrono::milliseconds(5000));
  auto body = makeBody("hello");
  p->put("k", makeHeaders("200"), body);

  Http::ResponseHeaderMapPtr h;
  Buffer::OwnedImpl b1;
  ASSERT_TRUE(p->get("k", h, b1));
  EXPECT_EQ(stats_.expired_.value(), 0);

  time_system_.setMonotonicTime(time_system_.monotonicTime() + std::chrono::milliseconds(5001));
  Buffer::OwnedImpl b2;
  EXPECT_FALSE(p->get("k", h, b2));
  EXPECT_EQ(stats_.expired_.value(), 1);

  // The expired entry was erased (not just hidden): a repeat lookup must not
  // count a second expiry.
  Buffer::OwnedImpl b3;
  EXPECT_FALSE(p->get("k", h, b3));
  EXPECT_EQ(stats_.expired_.value(), 1);
}

TEST_F(RingBufferPartitionTest, NoTtlNeverExpires) {
  auto p = makePartition(4096); // no TTL
  auto body = makeBody("hello");
  p->put("k", makeHeaders("200"), body);

  time_system_.setMonotonicTime(time_system_.monotonicTime() + std::chrono::hours(24 * 365));
  Http::ResponseHeaderMapPtr h;
  Buffer::OwnedImpl b;
  EXPECT_TRUE(p->get("k", h, b));
}

TEST_F(RingBufferPartitionTest, StatsCountInsertsEvictionsAndRejections) {
  const size_t entry_size = entrySize("k1", "ab");
  auto p = makePartition(2 * entry_size);

  auto b1 = makeBody("ab");
  auto b2 = makeBody("cd");
  auto b3 = makeBody("ef");
  p->put("k1", makeHeaders("200"), b1);
  p->put("k2", makeHeaders("200"), b2);
  p->put("k3", makeHeaders("200"), b3); // evicts k1
  EXPECT_EQ(stats_.insert_.value(), 3);
  EXPECT_EQ(stats_.eviction_.value(), 1);

  auto oversized = makeBody(std::string(3 * entry_size, 'x'));
  EXPECT_FALSE(p->put("big", makeHeaders("200"), oversized));
  EXPECT_EQ(stats_.insert_rejected_.value(), 1);
}

// ---- SharedCacheStore / RingCacheStoreRegistry tests ----

class SharedCacheStoreTest : public testing::Test {
protected:
  std::shared_ptr<SharedCacheStore> makeStore(size_t partitions, size_t max_bytes) {
    return std::make_shared<SharedCacheStore>(partitions, max_bytes, absl::nullopt, time_system_,
                                              *stats_store_.rootScope());
  }

  Stats::IsolatedStoreImpl stats_store_;
  Event::SimulatedTimeSystem time_system_;
};

TEST_F(SharedCacheStoreTest, SameKeyAlwaysMapsToSamePartition) {
  auto store = makeStore(8, 4096);
  EXPECT_EQ(&store->getPartition("some-key"), &store->getPartition("some-key"));
  EXPECT_EQ(&store->getPartition(""), &store->getPartition(""));
}

TEST_F(SharedCacheStoreTest, RegistrySharesStoresByConfigKey) {
  RingCacheStoreRegistry registry;
  int factory_calls = 0;
  const auto factory = [&]() {
    ++factory_calls;
    return makeStore(4, 4096);
  };

  auto s1 = registry.getOrCreate("geometry-a", factory);
  auto s2 = registry.getOrCreate("geometry-a", factory);
  auto s3 = registry.getOrCreate("geometry-b", factory);

  EXPECT_EQ(s1, s2);
  EXPECT_NE(s1, s3);
  EXPECT_EQ(factory_calls, 2);
}

TEST_F(SharedCacheStoreTest, ConcurrentPutsAndGets) {
  auto store = makeStore(4, 32 * 1024);
  auto& thread_factory = Thread::threadFactoryForTest();

  std::vector<Thread::ThreadPtr> threads;
  for (int t = 0; t < 4; ++t) {
    threads.push_back(thread_factory.createThread([&store, t]() {
      for (int i = 0; i < 200; ++i) {
        // Overlapping key space across threads to force lock contention.
        const std::string key = absl::StrCat("key-", (t + i) % 10);
        auto headers = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(
            Http::TestResponseHeaderMapImpl{{":status", "200"}});
        Buffer::OwnedImpl body("payload");
        store->getPartition(key).put(key, std::move(headers), body);

        Http::ResponseHeaderMapPtr out_headers;
        Buffer::OwnedImpl out_body;
        if (store->getPartition(key).get(key, out_headers, out_body)) {
          EXPECT_EQ(out_body.toString(), "payload");
        }
      }
    }));
  }
  for (auto& thread : threads) {
    thread->join();
  }
}

// ---- CacheFilterConfig key-building tests ----

class CacheFilterConfigTest : public testing::Test {
protected:
  std::shared_ptr<CacheFilterConfig> makeConfig(KeyConfig kc) {
    auto store = std::make_shared<SharedCacheStore>(4, 4096, absl::nullopt, time_system_,
                                                    *stats_store_.rootScope());
    return std::make_shared<CacheFilterConfig>(std::move(kc), std::move(store), 1024);
  }

  Stats::IsolatedStoreImpl stats_store_;
  Event::SimulatedTimeSystem time_system_;
};

TEST_F(CacheFilterConfigTest, KeyIncludesHostAndPath) {
  KeyConfig kc;
  kc.include_host = true;
  kc.include_path = true;
  auto cfg = makeConfig(kc);

  Http::TestRequestHeaderMapImpl hdrs{
      {":method", "GET"}, {":path", "/foo"}, {":authority", "example.com"}};
  const std::string key = cfg->buildKey(hdrs);
  EXPECT_NE(key.find("example.com"), std::string::npos);
  EXPECT_NE(key.find("/foo"), std::string::npos);
}

TEST_F(CacheFilterConfigTest, KeyExcludesHostWhenConfigured) {
  KeyConfig kc;
  kc.include_host = false;
  kc.include_path = true;
  auto cfg = makeConfig(kc);

  Http::TestRequestHeaderMapImpl hdrs{
      {":method", "GET"}, {":path", "/bar"}, {":authority", "example.com"}};
  const std::string key = cfg->buildKey(hdrs);
  EXPECT_EQ(key.find("example.com"), std::string::npos);
  EXPECT_NE(key.find("/bar"), std::string::npos);
}

TEST_F(CacheFilterConfigTest, KeyIncludesAdditionalHeaders) {
  KeyConfig kc;
  kc.include_host = true;
  kc.include_path = true;
  kc.additional_headers.push_back("x-tenant");
  auto cfg = makeConfig(kc);

  Http::TestRequestHeaderMapImpl hdrs{{":method", "GET"},
                                      {":path", "/api"},
                                      {":authority", "svc.local"},
                                      {"x-tenant", "acme"}};
  const std::string key = cfg->buildKey(hdrs);
  EXPECT_NE(key.find("acme"), std::string::npos);

  // A request without the header must produce a different key.
  Http::TestRequestHeaderMapImpl hdrs_no_tenant{
      {":method", "GET"}, {":path", "/api"}, {":authority", "svc.local"}};
  EXPECT_NE(key, cfg->buildKey(hdrs_no_tenant));
}

TEST_F(CacheFilterConfigTest, SeparatorCharactersInValuesDoNotCollide) {
  KeyConfig kc;
  kc.include_host = true;
  kc.include_path = true;
  auto cfg = makeConfig(kc);

  // Under a naive "<host>|<path>|" concatenation both of these produce
  // "example.com||/x|". Length-prefixed components must keep them distinct.
  Http::TestRequestHeaderMapImpl a{
      {":method", "GET"}, {":path", "/x"}, {":authority", "example.com|"}};
  Http::TestRequestHeaderMapImpl b{
      {":method", "GET"}, {":path", "|/x"}, {":authority", "example.com"}};
  EXPECT_NE(cfg->buildKey(a), cfg->buildKey(b));
}

TEST_F(CacheFilterConfigTest, AbsentComponentsKeepPositions) {
  KeyConfig kc;
  kc.include_host = false;
  kc.include_path = false;
  kc.additional_headers.push_back("x-a");
  kc.additional_headers.push_back("x-b");
  auto cfg = makeConfig(kc);

  // The same value in different header positions must not collide.
  Http::TestRequestHeaderMapImpl a{
      {":method", "GET"}, {":path", "/"}, {":authority", "h"}, {"x-a", "v"}};
  Http::TestRequestHeaderMapImpl b{
      {":method", "GET"}, {":path", "/"}, {":authority", "h"}, {"x-b", "v"}};
  EXPECT_NE(cfg->buildKey(a), cfg->buildKey(b));
}

TEST_F(CacheFilterConfigTest, SchemeIsPartOfKey) {
  KeyConfig kc;
  kc.include_host = true;
  kc.include_path = true;
  auto cfg = makeConfig(kc);

  Http::TestRequestHeaderMapImpl http_req{{":method", "GET"},
                                          {":path", "/x"},
                                          {":authority", "example.com"},
                                          {":scheme", "http"}};
  Http::TestRequestHeaderMapImpl https_req{{":method", "GET"},
                                           {":path", "/x"},
                                           {":authority", "example.com"},
                                           {":scheme", "https"}};
  EXPECT_NE(cfg->buildKey(http_req), cfg->buildKey(https_req));
}

// ---- CacheFilter integration-style tests ----

class CacheFilterTest : public testing::Test {
public:
  void SetUp() override {
    KeyConfig kc;
    kc.include_host = true;
    kc.include_path = true;
    store_ = std::make_shared<SharedCacheStore>(4, 64 * 1024, absl::nullopt, time_system_,
                                                *stats_store_.rootScope());
    config_ = std::make_shared<CacheFilterConfig>(std::move(kc), store_, 32 * 1024);
    filter_ = std::make_shared<CacheFilter>(config_);
    filter_->setDecoderFilterCallbacks(decoder_callbacks_);
    filter_->setEncoderFilterCallbacks(encoder_callbacks_);
  }

  // Runs a full miss+store cycle through a fresh filter instance.
  void runMissAndStore(Http::TestRequestHeaderMapImpl req, Http::TestResponseHeaderMapImpl resp,
                       const std::string& body_str, bool end_with_trailers = false) {
    auto f = std::make_shared<CacheFilter>(config_);
    NiceMock<Http::MockStreamDecoderFilterCallbacks> cb;
    f->setDecoderFilterCallbacks(cb);
    f->setEncoderFilterCallbacks(encoder_callbacks_);
    f->decodeHeaders(req, true);
    f->encodeHeaders(resp, false);
    Buffer::OwnedImpl body(body_str);
    f->encodeData(body, !end_with_trailers);
    if (end_with_trailers) {
      Http::TestResponseTrailerMapImpl trailers{{"grpc-status", "0"}};
      f->encodeTrailers(trailers);
    }
  }

  // Sends the request through a fresh filter and reports whether it was
  // served from cache (i.e. sendLocalReply was called).
  bool lookupHits(Http::TestRequestHeaderMapImpl req) {
    auto f = std::make_shared<CacheFilter>(config_);
    NiceMock<Http::MockStreamDecoderFilterCallbacks> cb;
    f->setDecoderFilterCallbacks(cb);
    f->setEncoderFilterCallbacks(encoder_callbacks_);
    bool hit = false;
    EXPECT_CALL(cb, sendLocalReply(_, _, _, _, _))
        .Times(testing::AnyNumber())
        .WillRepeatedly(testing::InvokeWithoutArgs([&hit]() { hit = true; }));
    f->decodeHeaders(req, true);
    return hit;
  }

  NiceMock<Http::MockStreamDecoderFilterCallbacks> decoder_callbacks_;
  NiceMock<Http::MockStreamEncoderFilterCallbacks> encoder_callbacks_;
  Stats::IsolatedStoreImpl stats_store_;
  Event::SimulatedTimeSystem time_system_;
  std::shared_ptr<SharedCacheStore> store_;
  std::shared_ptr<CacheFilterConfig> config_;
  std::shared_ptr<CacheFilter> filter_;
};

TEST_F(CacheFilterTest, NonGetPassesThrough) {
  Http::TestRequestHeaderMapImpl req_headers{
      {":method", "POST"}, {":path", "/submit"}, {":authority", "host"}};
  EXPECT_EQ(Http::FilterHeadersStatus::Continue,
            filter_->decodeHeaders(req_headers, true));

  Http::TestResponseHeaderMapImpl resp_headers{{":status", "200"}};
  EXPECT_EQ(Http::FilterHeadersStatus::Continue,
            filter_->encodeHeaders(resp_headers, false));

  Buffer::OwnedImpl data("body");
  EXPECT_EQ(Http::FilterDataStatus::Continue, filter_->encodeData(data, true));
}

TEST_F(CacheFilterTest, CacheMissThenHit) {
  // --- First request: cache miss, response is stored ---
  Http::TestRequestHeaderMapImpl req1{{":method", "GET"},
                                      {":path", "/resource"},
                                      {":authority", "example.com"}};
  EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter_->decodeHeaders(req1, true));

  Http::TestResponseHeaderMapImpl resp_hdrs{{":status", "200"}, {"content-type", "text/plain"}};
  EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter_->encodeHeaders(resp_hdrs, false));

  Buffer::OwnedImpl body("cached-content");
  EXPECT_EQ(Http::FilterDataStatus::Continue, filter_->encodeData(body, true));

  // --- Second request: same key → cache hit, sendLocalReply called ---
  auto filter2 = std::make_shared<CacheFilter>(config_);
  NiceMock<Http::MockStreamDecoderFilterCallbacks> cb2;
  filter2->setDecoderFilterCallbacks(cb2);
  filter2->setEncoderFilterCallbacks(encoder_callbacks_);

  EXPECT_CALL(cb2, sendLocalReply(Http::Code::OK, _, _, _, _));

  Http::TestRequestHeaderMapImpl req2{{":method", "GET"},
                                      {":path", "/resource"},
                                      {":authority", "example.com"}};
  EXPECT_EQ(Http::FilterHeadersStatus::StopIteration, filter2->decodeHeaders(req2, true));
}

TEST_F(CacheFilterTest, ReplayStripsFramingAndHopByHopHeaders) {
  // --- First request: store a response carrying framing and hop-by-hop
  // headers that must not be replayed with a cached body. ---
  Http::TestRequestHeaderMapImpl req1{{":method", "GET"},
                                      {":path", "/asset"},
                                      {":authority", "example.com"}};
  EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter_->decodeHeaders(req1, true));

  Http::TestResponseHeaderMapImpl resp_hdrs{{":status", "200"},
                                            {"content-type", "text/plain"},
                                            {"content-length", "999"},
                                            {"transfer-encoding", "chunked"},
                                            {"connection", "keep-alive"},
                                            {"keep-alive", "timeout=5"},
                                            {"upgrade", "h2c"},
                                            {"etag", "\"abc\""}};
  EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter_->encodeHeaders(resp_hdrs, false));

  Buffer::OwnedImpl body("cached-content");
  EXPECT_EQ(Http::FilterDataStatus::Continue, filter_->encodeData(body, true));

  // --- Second request: cache hit; the replayed headers must keep end-to-end
  // headers but drop framing and hop-by-hop ones. ---
  auto filter2 = std::make_shared<CacheFilter>(config_);
  NiceMock<Http::MockStreamDecoderFilterCallbacks> cb2;
  filter2->setDecoderFilterCallbacks(cb2);
  filter2->setEncoderFilterCallbacks(encoder_callbacks_);

  EXPECT_CALL(cb2, sendLocalReply(Http::Code::OK, _, _, _, _))
      .WillOnce(testing::WithArg<2>(
          testing::Invoke([](std::function<void(Http::ResponseHeaderMap&)> modify_headers) {
            Http::TestResponseHeaderMapImpl reply{{":status", "200"}};
            modify_headers(reply);
            EXPECT_EQ(reply.get_("content-type"), "text/plain");
            EXPECT_EQ(reply.get_("etag"), "\"abc\"");
            EXPECT_TRUE(reply.get(Http::LowerCaseString("content-length")).empty());
            EXPECT_TRUE(reply.get(Http::LowerCaseString("transfer-encoding")).empty());
            EXPECT_TRUE(reply.get(Http::LowerCaseString("connection")).empty());
            EXPECT_TRUE(reply.get(Http::LowerCaseString("keep-alive")).empty());
            EXPECT_TRUE(reply.get(Http::LowerCaseString("upgrade")).empty());
          })));

  Http::TestRequestHeaderMapImpl req2{{":method", "GET"},
                                      {":path", "/asset"},
                                      {":authority", "example.com"}};
  EXPECT_EQ(Http::FilterHeadersStatus::StopIteration, filter2->decodeHeaders(req2, true));
}

TEST_F(CacheFilterTest, WatermarkDropsOversizedResponse) {
  // max_cacheable_size is 32 KiB; send 33 KiB chunks.
  Http::TestRequestHeaderMapImpl req{{":method", "GET"},
                                     {":path", "/big"},
                                     {":authority", "host"}};
  EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter_->decodeHeaders(req, true));

  Http::TestResponseHeaderMapImpl resp{{":status", "200"}};
  EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter_->encodeHeaders(resp, false));

  Buffer::OwnedImpl chunk(std::string(33 * 1024, 'x'));
  EXPECT_EQ(Http::FilterDataStatus::Continue, filter_->encodeData(chunk, true));

  // A second filter should get a miss since the entry was never stored.
  auto filter2 = std::make_shared<CacheFilter>(config_);
  NiceMock<Http::MockStreamDecoderFilterCallbacks> cb2;
  filter2->setDecoderFilterCallbacks(cb2);
  filter2->setEncoderFilterCallbacks(encoder_callbacks_);

  EXPECT_CALL(cb2, sendLocalReply(_, _, _, _, _)).Times(0);

  Http::TestRequestHeaderMapImpl req2{{":method", "GET"},
                                      {":path", "/big"},
                                      {":authority", "host"}};
  EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter2->decodeHeaders(req2, true));
}

TEST_F(CacheFilterTest, Non200ResponseNotCached) {
  Http::TestRequestHeaderMapImpl req{{":method", "GET"},
                                     {":path", "/err"},
                                     {":authority", "host"}};
  EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter_->decodeHeaders(req, true));

  Http::TestResponseHeaderMapImpl resp{{":status", "404"}};
  EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter_->encodeHeaders(resp, false));

  Buffer::OwnedImpl body("not found");
  EXPECT_EQ(Http::FilterDataStatus::Continue, filter_->encodeData(body, true));

  // Second request should still be a cache miss.
  auto filter2 = std::make_shared<CacheFilter>(config_);
  NiceMock<Http::MockStreamDecoderFilterCallbacks> cb2;
  filter2->setDecoderFilterCallbacks(cb2);
  filter2->setEncoderFilterCallbacks(encoder_callbacks_);

  EXPECT_CALL(cb2, sendLocalReply(_, _, _, _, _)).Times(0);

  Http::TestRequestHeaderMapImpl req2{{":method", "GET"},
                                      {":path", "/err"},
                                      {":authority", "host"}};
  EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter2->decodeHeaders(req2, true));
}

// ---- Cacheability guard tests ----

TEST_F(CacheFilterTest, AuthorizationRequestNeverServedFromCache) {
  Http::TestRequestHeaderMapImpl plain{
      {":method", "GET"}, {":path", "/doc"}, {":authority", "host"}};
  runMissAndStore(plain, Http::TestResponseHeaderMapImpl{{":status", "200"}}, "public-content");
  ASSERT_TRUE(lookupHits(plain));

  Http::TestRequestHeaderMapImpl authed{{":method", "GET"},
                                        {":path", "/doc"},
                                        {":authority", "host"},
                                        {"authorization", "Bearer tok"}};
  EXPECT_FALSE(lookupHits(authed));
}

TEST_F(CacheFilterTest, AuthorizationResponseNeverStored) {
  Http::TestRequestHeaderMapImpl authed{{":method", "GET"},
                                        {":path", "/secret"},
                                        {":authority", "host"},
                                        {"authorization", "Bearer tok"}};
  runMissAndStore(authed, Http::TestResponseHeaderMapImpl{{":status", "200"}}, "user-a-secret");

  // The same key without credentials must still miss.
  Http::TestRequestHeaderMapImpl plain{
      {":method", "GET"}, {":path", "/secret"}, {":authority", "host"}};
  EXPECT_FALSE(lookupHits(plain));
}

TEST_F(CacheFilterTest, RequestNoCacheSkipsLookup) {
  Http::TestRequestHeaderMapImpl plain{
      {":method", "GET"}, {":path", "/fresh"}, {":authority", "host"}};
  runMissAndStore(plain, Http::TestResponseHeaderMapImpl{{":status", "200"}}, "content");
  ASSERT_TRUE(lookupHits(plain));

  Http::TestRequestHeaderMapImpl no_cache{{":method", "GET"},
                                          {":path", "/fresh"},
                                          {":authority", "host"},
                                          {"cache-control", "no-cache"}};
  EXPECT_FALSE(lookupHits(no_cache));
}

TEST_F(CacheFilterTest, RequestNoStoreSkipsStore) {
  Http::TestRequestHeaderMapImpl no_store{{":method", "GET"},
                                          {":path", "/nostore"},
                                          {":authority", "host"},
                                          {"cache-control", "no-store"}};
  runMissAndStore(no_store, Http::TestResponseHeaderMapImpl{{":status", "200"}}, "content");

  Http::TestRequestHeaderMapImpl plain{
      {":method", "GET"}, {":path", "/nostore"}, {":authority", "host"}};
  EXPECT_FALSE(lookupHits(plain));
}

TEST_F(CacheFilterTest, ResponseCacheControlForbidsStoring) {
  const std::vector<std::string> directives = {"no-store", "private", "no-cache",
                                               "max-age=60, private"};
  for (const auto& directive : directives) {
    const std::string path = absl::StrCat("/cc/", directive);
    Http::TestRequestHeaderMapImpl req{
        {":method", "GET"}, {":path", path}, {":authority", "host"}};
    runMissAndStore(
        req,
        Http::TestResponseHeaderMapImpl{{":status", "200"}, {"cache-control", directive}},
        "content");
    EXPECT_FALSE(lookupHits(req)) << "directive: " << directive;
  }
}

TEST_F(CacheFilterTest, ResponseSetCookieNotStored) {
  Http::TestRequestHeaderMapImpl req{
      {":method", "GET"}, {":path", "/cookie"}, {":authority", "host"}};
  runMissAndStore(req,
                  Http::TestResponseHeaderMapImpl{{":status", "200"},
                                                  {"set-cookie", "session=abc; HttpOnly"}},
                  "per-user-content");
  EXPECT_FALSE(lookupHits(req));
}

TEST_F(CacheFilterTest, ResponseVaryNotStored) {
  Http::TestRequestHeaderMapImpl req{
      {":method", "GET"}, {":path", "/vary"}, {":authority", "host"}};
  runMissAndStore(
      req,
      Http::TestResponseHeaderMapImpl{{":status", "200"}, {"vary", "accept-encoding"}},
      "gzipped-content");
  EXPECT_FALSE(lookupHits(req));
}

TEST_F(CacheFilterTest, TrailerTerminatedResponseNotStored) {
  Http::TestRequestHeaderMapImpl req{
      {":method", "GET"}, {":path", "/grpcish"}, {":authority", "host"}};
  runMissAndStore(req, Http::TestResponseHeaderMapImpl{{":status", "200"}}, "partial-body",
                  /*end_with_trailers=*/true);
  EXPECT_FALSE(lookupHits(req));
}

// ---- Stats tests ----

TEST_F(CacheFilterTest, CountsHitsMissesAndInserts) {
  Http::TestRequestHeaderMapImpl req{
      {":method", "GET"}, {":path", "/stats"}, {":authority", "host"}};
  runMissAndStore(req, Http::TestResponseHeaderMapImpl{{":status", "200"}}, "content");
  EXPECT_EQ(store_->stats().miss_.value(), 1);
  EXPECT_EQ(store_->stats().insert_.value(), 1);
  EXPECT_EQ(store_->stats().hit_.value(), 0);

  ASSERT_TRUE(lookupHits(req));
  EXPECT_EQ(store_->stats().hit_.value(), 1);
  EXPECT_EQ(store_->stats().miss_.value(), 1);
}

TEST_F(CacheFilterTest, BypassedLookupsCountNeitherHitNorMiss) {
  Http::TestRequestHeaderMapImpl authed{{":method", "GET"},
                                        {":path", "/auth"},
                                        {":authority", "host"},
                                        {"authorization", "Bearer tok"}};
  EXPECT_FALSE(lookupHits(authed));
  EXPECT_EQ(store_->stats().hit_.value(), 0);
  EXPECT_EQ(store_->stats().miss_.value(), 0);
}

// ---- Factory / config translation tests ----

class RingCacheFactoryTest : public testing::Test {
protected:
  Http::StreamFilterSharedPtr
  createFilter(const envoy::extensions::filters::http::ringcache::v3::RingCacheConfig& proto) {
    Server::Configuration::ExtraFactoryContext extra{
        ProtobufMessage::getStrictValidationVisitor(), stats_prefix_};
    auto cb_or = factory_.createHttpFilterFactoryFromProto(proto, context_, extra);
    EXPECT_TRUE(cb_or.ok());
    Http::StreamFilterSharedPtr filter;
    NiceMock<Http::MockFilterChainFactoryCallbacks> callbacks;
    EXPECT_CALL(callbacks, addStreamFilter(_)).WillOnce(SaveArg<0>(&filter));
    (*cb_or)(callbacks);
    return filter;
  }

  // Drives a full miss+store cycle through the given filter.
  void storeThrough(const Http::StreamFilterSharedPtr& filter, const std::string& path) {
    NiceMock<Http::MockStreamDecoderFilterCallbacks> decoder_cb;
    filter->setDecoderFilterCallbacks(decoder_cb);
    filter->setEncoderFilterCallbacks(encoder_callbacks_);
    Http::TestRequestHeaderMapImpl req{
        {":method", "GET"}, {":path", path}, {":authority", "host"}};
    EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter->decodeHeaders(req, true));
    Http::TestResponseHeaderMapImpl resp{{":status", "200"}};
    filter->encodeHeaders(resp, false);
    Buffer::OwnedImpl body("shared-content");
    filter->encodeData(body, true);
  }

  bool hitsThrough(const Http::StreamFilterSharedPtr& filter, const std::string& path) {
    NiceMock<Http::MockStreamDecoderFilterCallbacks> decoder_cb;
    filter->setDecoderFilterCallbacks(decoder_cb);
    filter->setEncoderFilterCallbacks(encoder_callbacks_);
    bool hit = false;
    EXPECT_CALL(decoder_cb, sendLocalReply(_, _, _, _, _))
        .Times(testing::AnyNumber())
        .WillRepeatedly(testing::InvokeWithoutArgs([&hit]() { hit = true; }));
    Http::TestRequestHeaderMapImpl req{
        {":method", "GET"}, {":path", path}, {":authority", "host"}};
    filter->decodeHeaders(req, true);
    return hit;
  }

  RingCacheFilterFactory factory_;
  NiceMock<Server::Configuration::MockServerFactoryContext> context_;
  NiceMock<Http::MockStreamEncoderFilterCallbacks> encoder_callbacks_;
  const std::string stats_prefix_{"test."};
};

TEST_F(RingCacheFactoryTest, CreatesFilterFromFullConfig) {
  envoy::extensions::filters::http::ringcache::v3::RingCacheConfig proto;
  proto.mutable_num_partitions()->set_value(4);
  proto.mutable_partition_max_bytes()->set_value(4096);
  proto.mutable_max_cacheable_body_bytes()->set_value(1024);
  proto.mutable_ttl()->set_seconds(60);
  proto.mutable_key_config()->set_exclude_host(true);
  proto.mutable_key_config()->add_additional_headers("x-tenant");

  EXPECT_NE(createFilter(proto), nullptr);
}

TEST_F(RingCacheFactoryTest, IdenticalConfigsShareTheCacheAcrossFactoryCalls) {
  // Two factory invocations with the same proto, as happens on an LDS update
  // or when the same config appears in two filter chains.
  envoy::extensions::filters::http::ringcache::v3::RingCacheConfig proto;
  auto filter1 = createFilter(proto);
  auto filter2 = createFilter(proto);
  ASSERT_NE(filter1, nullptr);
  ASSERT_NE(filter2, nullptr);

  storeThrough(filter1, "/shared");
  EXPECT_TRUE(hitsThrough(filter2, "/shared"));
}

TEST_F(RingCacheFactoryTest, DifferentGeometriesGetSeparateCaches) {
  envoy::extensions::filters::http::ringcache::v3::RingCacheConfig proto_a;
  envoy::extensions::filters::http::ringcache::v3::RingCacheConfig proto_b;
  proto_b.mutable_partition_max_bytes()->set_value(4096);

  auto filter1 = createFilter(proto_a);
  auto filter2 = createFilter(proto_b);
  ASSERT_NE(filter1, nullptr);
  ASSERT_NE(filter2, nullptr);

  storeThrough(filter1, "/separate");
  EXPECT_FALSE(hitsThrough(filter2, "/separate"));
}

} // namespace RingCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
