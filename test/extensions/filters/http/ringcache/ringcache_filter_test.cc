#include "source/extensions/filters/http/ringcache/cache_filter.h"
#include "source/extensions/filters/http/ringcache/ring_buffer_cache.h"

#include "test/mocks/http/mocks.h"
#include "test/test_common/utility.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using testing::_;
using testing::NiceMock;
using testing::Return;

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingCache {

// ---- RingBufferPartition unit tests ----

class RingBufferPartitionTest : public testing::Test {
protected:
  static Http::ResponseHeaderMapPtr makeHeaders(absl::string_view status) {
    auto hdrs = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(
        Http::TestResponseHeaderMapImpl{{":status", status}});
    return hdrs;
  }

  static Buffer::OwnedImpl makeBody(absl::string_view data) {
    Buffer::OwnedImpl buf;
    buf.add(data);
    return buf;
  }
};

TEST_F(RingBufferPartitionTest, MissOnEmpty) {
  RingBufferPartition p(1024);
  Http::ResponseHeaderMapPtr h;
  Buffer::OwnedImpl b;
  EXPECT_FALSE(p.get("key", h, b));
}

TEST_F(RingBufferPartitionTest, PutAndGet) {
  RingBufferPartition p(1024);
  auto body = makeBody("hello");
  p.put("k1", makeHeaders("200"), body);

  Http::ResponseHeaderMapPtr out_headers;
  Buffer::OwnedImpl out_body;
  ASSERT_TRUE(p.get("k1", out_headers, out_body));
  EXPECT_EQ(out_headers->getStatusValue(), "200");
  EXPECT_EQ(out_body.toString(), "hello");
}

TEST_F(RingBufferPartitionTest, EvictsOldestWhenFull) {
  // Partition holds 5 bytes, each entry is 2 bytes.
  RingBufferPartition p(5);

  auto b1 = makeBody("ab");
  auto b2 = makeBody("cd");
  auto b3 = makeBody("ef");

  p.put("k1", makeHeaders("200"), b1);
  p.put("k2", makeHeaders("200"), b2);
  // Now 4 bytes used; inserting 2 more bytes requires evicting k1.
  p.put("k3", makeHeaders("200"), b3);

  Http::ResponseHeaderMapPtr h;
  Buffer::OwnedImpl b;
  EXPECT_FALSE(p.get("k1", h, b)); // evicted
  EXPECT_TRUE(p.get("k2", h, b));
  EXPECT_TRUE(p.get("k3", h, b));
}

TEST_F(RingBufferPartitionTest, DropsEntryExceedingCapacity) {
  RingBufferPartition p(3);
  auto body = makeBody("toolong");
  EXPECT_FALSE(p.put("k", makeHeaders("200"), body));

  Http::ResponseHeaderMapPtr h;
  Buffer::OwnedImpl b;
  EXPECT_FALSE(p.get("k", h, b));
}

TEST_F(RingBufferPartitionTest, UpdateExistingKey) {
  RingBufferPartition p(1024);
  auto b1 = makeBody("first");
  auto b2 = makeBody("second");
  p.put("key", makeHeaders("200"), b1);
  p.put("key", makeHeaders("200"), b2);

  Http::ResponseHeaderMapPtr h;
  Buffer::OwnedImpl b;
  ASSERT_TRUE(p.get("key", h, b));
  EXPECT_EQ(b.toString(), "second");
}

// ---- CacheFilterConfig key-building tests ----

class CacheFilterConfigTest : public testing::Test {
protected:
  static std::shared_ptr<CacheFilterConfig> makeConfig(KeyConfig kc) {
    auto store = std::make_shared<SharedCacheStore>(4, 4096);
    return std::make_shared<CacheFilterConfig>(std::move(kc), std::move(store), 1024);
  }
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
    auto store = std::make_shared<SharedCacheStore>(4, 64 * 1024);
    config_ = std::make_shared<CacheFilterConfig>(std::move(kc), std::move(store), 32 * 1024);
    filter_ = std::make_shared<CacheFilter>(config_);
    filter_->setDecoderFilterCallbacks(decoder_callbacks_);
    filter_->setEncoderFilterCallbacks(encoder_callbacks_);
  }

  NiceMock<Http::MockStreamDecoderFilterCallbacks> decoder_callbacks_;
  NiceMock<Http::MockStreamEncoderFilterCallbacks> encoder_callbacks_;
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

} // namespace RingCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
