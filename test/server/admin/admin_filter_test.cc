#include <memory>
#include <utility>
#include <vector>

#include "source/common/buffer/buffer_impl.h"
#include "source/server/admin/admin.h"
#include "source/server/admin/admin_filter.h"

#include "test/mocks/event/mocks.h"
#include "test/mocks/http/mocks.h"
#include "test/mocks/server/admin.h"
#include "test/test_common/environment.h"
#include "test/test_common/utility.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using testing::_;
using testing::ByMove;
using testing::InSequence;
using testing::NiceMock;
using testing::Return;

namespace Envoy {
namespace Server {

class AdminFilterTest : public testing::TestWithParam<Network::Address::IpVersion> {
public:
  AdminFilterTest() : filter_(admin_), request_headers_{{":path", "/"}} {
    EXPECT_CALL(admin_, makeRequest(_)).WillOnce(Return(ByMove(adminHandlerCallback())));
    filter_.setDecoderFilterCallbacks(callbacks_);
  }

  NiceMock<MockAdmin> admin_;
  AdminFilter filter_;
  NiceMock<Http::MockStreamDecoderFilterCallbacks> callbacks_;
  Http::TestRequestHeaderMapImpl request_headers_;

  static Admin::RequestPtr adminHandlerCallback() {
    return AdminImpl::makeStaticTextRequest("OK\n", Http::Code::OK);
  }
};

INSTANTIATE_TEST_SUITE_P(IpVersions, AdminFilterTest,
                         testing::ValuesIn(TestEnvironment::getIpVersionsForTest()),
                         TestUtility::ipTestParamsToString);

TEST_P(AdminFilterTest, HeaderOnly) {
  EXPECT_CALL(callbacks_, encodeHeaders_(_, false));
  EXPECT_EQ(Http::FilterHeadersStatus::StopIteration,
            filter_.decodeHeaders(request_headers_, true));
  ASSERT_TRUE(callbacks_.stream_info_.responseCodeDetails().has_value());
  EXPECT_EQ(callbacks_.stream_info_.responseCodeDetails().value(), "admin_filter_response");
}

TEST_P(AdminFilterTest, Body) {
  InSequence s;

  EXPECT_EQ(Http::FilterHeadersStatus::StopIteration,
            filter_.decodeHeaders(request_headers_, false));
  Buffer::OwnedImpl data("hello");
  Http::MetadataMap metadata_map{{"metadata", "metadata"}};
  EXPECT_EQ(Http::FilterMetadataStatus::Continue, filter_.decodeMetadata(metadata_map));
  EXPECT_CALL(callbacks_, addDecodedData(_, false));
  EXPECT_CALL(callbacks_, encodeHeaders_(_, false));
  EXPECT_EQ(Http::FilterDataStatus::StopIterationNoBuffer, filter_.decodeData(data, true));
}

TEST_P(AdminFilterTest, Trailers) {
  InSequence s;

  EXPECT_EQ(Http::FilterHeadersStatus::StopIteration,
            filter_.decodeHeaders(request_headers_, false));
  Buffer::OwnedImpl data("hello");
  EXPECT_CALL(callbacks_, addDecodedData(_, false));
  EXPECT_EQ(Http::FilterDataStatus::StopIterationNoBuffer, filter_.decodeData(data, false));
  EXPECT_CALL(callbacks_, decodingBuffer());
  filter_.getRequestBody();
  EXPECT_CALL(callbacks_, encodeHeaders_(_, false));
  Http::TestRequestTrailerMapImpl request_trailers;
  EXPECT_EQ(Http::FilterTrailersStatus::StopIteration, filter_.decodeTrailers(request_trailers));
}

struct BackpressureRequestState {
  std::vector<std::pair<std::string, bool>> chunks{{"one", true}, {"two", false}};
  std::function<void()> on_start;
  std::function<void()> on_next;
  size_t next_calls{0};
  size_t destructions{0};
};

class BackpressureRequest : public Admin::Request {
public:
  explicit BackpressureRequest(std::shared_ptr<BackpressureRequestState> state)
      : state_(std::move(state)) {}
  ~BackpressureRequest() override { ++state_->destructions; }
  Http::Code start(Http::ResponseHeaderMap&) override {
    if (state_->on_start) {
      state_->on_start();
    }
    return Http::Code::OK;
  }
  bool nextChunk(Buffer::Instance& response) override {
    const size_t index = state_->next_calls++;
    if (state_->on_next) {
      state_->on_next();
    }
    if (index >= state_->chunks.size()) {
      ADD_FAILURE() << "request called after its final chunk";
      return false;
    }
    response.add(state_->chunks[index].first);
    return state_->chunks[index].second;
  }

private:
  std::shared_ptr<BackpressureRequestState> state_;
};

class AdminBackpressureTest : public testing::Test {
public:
  AdminBackpressureTest() : filter_(admin_) {
    callbacks_.stream_info_.protocol_ = Http::Protocol::Http11;
  }
  void TearDown() override { filter_.onDestroy(); }

  void start() {
    const auto protocol = callbacks_.stream_info_.protocol_;
    EXPECT_CALL(callbacks_, setBufferLimit(0))
        .Times(protocol == Http::Protocol::Http10 || protocol == Http::Protocol::Http11 ? 1 : 0);
    EXPECT_CALL(callbacks_, addDownstreamWatermarkCallbacks(testing::Ref(filter_)))
        .WillOnce([this](Http::DownstreamWatermarkCallbacks& watermarks) {
          for (uint32_t index = 0; index < preexisting_watermarks_; ++index) {
            watermarks.onAboveWriteBufferHighWatermark();
          }
        });
    EXPECT_CALL(callbacks_, removeDownstreamWatermarkCallbacks(testing::Ref(filter_)));
    EXPECT_CALL(callbacks_, streamInfo())
        .WillRepeatedly(testing::ReturnRef(callbacks_.stream_info_));
    EXPECT_CALL(callbacks_, dispatcher())
        .WillRepeatedly(testing::ReturnRef(callbacks_.dispatcher_));
    EXPECT_CALL(callbacks_, encodeHeaders_(_, false))
        .Times(testing::AnyNumber())
        .WillRepeatedly([this](Http::ResponseHeaderMap&, bool) {
          ++header_calls_;
          if (on_headers_) {
            on_headers_();
          }
        });
    EXPECT_CALL(callbacks_, encodeData(_, _))
        .Times(testing::AnyNumber())
        .WillRepeatedly([this](Buffer::Instance& data, bool end_stream) {
          sent_.emplace_back(data.toString(), end_stream);
          if (on_encode_) {
            on_encode_();
          }
          data.drain(data.length());
        });
    EXPECT_CALL(admin_, makeRequest(_))
        .WillOnce(Return(ByMove(std::make_unique<BackpressureRequest>(state_))));
    filter_.setDecoderFilterCallbacks(callbacks_);
    EXPECT_EQ(Http::FilterHeadersStatus::StopIteration, filter_.decodeHeaders(headers_, true));
  }

  void prepareResume(bool next_iteration = false) {
    EXPECT_CALL(callbacks_.dispatcher_, createSchedulableCallback_(_))
        .WillOnce([this,
                   next_iteration](std::function<void()> callback) -> Event::SchedulableCallback* {
          late_resume_ = callback;
          resume_ = new testing::StrictMock<Event::MockSchedulableCallback>(&callbacks_.dispatcher_,
                                                                            std::move(callback));
          EXPECT_CALL(*resume_, enabled()).Times(testing::AnyNumber());
          EXPECT_CALL(*resume_, scheduleCallbackCurrentIteration())
              .Times(next_iteration ? testing::Exactly(0) : testing::AnyNumber());
          EXPECT_CALL(*resume_, scheduleCallbackNextIteration())
              .Times(next_iteration ? testing::AnyNumber() : testing::Exactly(0));
          EXPECT_CALL(*resume_, cancel()).Times(testing::AnyNumber());
          return resume_;
        });
  }

  testing::StrictMock<MockAdmin> admin_;
  testing::StrictMock<Http::MockStreamDecoderFilterCallbacks> callbacks_;
  std::shared_ptr<BackpressureRequestState> state_ = std::make_shared<BackpressureRequestState>();
  Http::TestRequestHeaderMapImpl headers_{{":path", "/stats/prometheus"}};
  std::vector<std::pair<std::string, bool>> sent_;
  std::function<void()> on_encode_;
  std::function<void()> on_headers_;
  std::function<void()> late_resume_;
  Event::MockSchedulableCallback* resume_{nullptr};
  uint32_t preexisting_watermarks_{0};
  size_t header_calls_{0};
  AdminFilter filter_;
};

TEST_F(AdminBackpressureTest, UnblockedRequestCompletesSynchronously) {
  start();
  EXPECT_EQ(state_->next_calls, 2);
  EXPECT_EQ(state_->destructions, 1);
  EXPECT_EQ(sent_, (std::vector<std::pair<std::string, bool>>{{"one", false}, {"two", true}}));
}

TEST_F(AdminBackpressureTest, Http10RetainsUnlimitedRequestBuffering) {
  callbacks_.stream_info_.protocol_ = Http::Protocol::Http10;
  start();
  EXPECT_EQ(state_->next_calls, 2);
}

TEST_F(AdminBackpressureTest, Http2RetainsExistingRequestBufferLimit) {
  callbacks_.stream_info_.protocol_ = Http::Protocol::Http2;
  start();
  EXPECT_EQ(state_->next_calls, 2);
}

TEST_F(AdminBackpressureTest, InProcessHeadersDoNotRequireNetworkCallbacks) {
  EXPECT_EQ(Http::FilterHeadersStatus::StopIteration, filter_.decodeHeaders(headers_, false));
  EXPECT_EQ(&filter_.getRequestHeaders(), &headers_);
}

TEST_F(AdminBackpressureTest, DestructionBeforeCallbacksAreInstalledIsSafe) {
  filter_.onDestroy();
  filter_.onDestroy();
  EXPECT_EQ(state_->destructions, 0);
}

TEST_F(AdminBackpressureTest, UnbalancedLowWatermarkDoesNotUnderflow) {
  EXPECT_ENVOY_BUG(filter_.onBelowWriteBufferLowWatermark(),
                   "admin low watermark without a preceding high watermark");
}

TEST_F(AdminBackpressureTest, StopsAtHighWatermarkAndResumesLater) {
  on_encode_ = [this]() {
    if (sent_.size() == 1) {
      filter_.onAboveWriteBufferHighWatermark();
    }
  };
  start();
  EXPECT_EQ(state_->next_calls, 1);
  EXPECT_EQ(state_->destructions, 0);
  prepareResume();
  filter_.onBelowWriteBufferLowWatermark();
  EXPECT_EQ(state_->next_calls, 1);
  ASSERT_NE(resume_, nullptr);
  EXPECT_TRUE(resume_->enabled_);
  resume_->invokeCallback();
  EXPECT_EQ(state_->next_calls, 2);
  EXPECT_EQ(state_->destructions, 1);
  EXPECT_EQ(sent_.back(), (std::pair<std::string, bool>{"two", true}));
}

TEST_F(AdminBackpressureTest, WaitsForAllOverlappingWatermarks) {
  preexisting_watermarks_ = 2;
  start();
  EXPECT_EQ(header_calls_, 1);
  EXPECT_EQ(state_->next_calls, 0);
  filter_.onBelowWriteBufferLowWatermark();
  EXPECT_EQ(resume_, nullptr);
  prepareResume();
  filter_.onBelowWriteBufferLowWatermark();
  resume_->invokeCallback();
  EXPECT_EQ(state_->next_calls, 2);
}

TEST_F(AdminBackpressureTest, ScheduledResumeRechecksBackpressure) {
  preexisting_watermarks_ = 1;
  start();
  prepareResume();
  filter_.onBelowWriteBufferLowWatermark();
  filter_.onAboveWriteBufferHighWatermark();
  resume_->invokeCallback();
  EXPECT_EQ(state_->next_calls, 0);
  filter_.onBelowWriteBufferLowWatermark();
  EXPECT_TRUE(resume_->enabled_);
  resume_->invokeCallback();
  EXPECT_EQ(state_->next_calls, 2);
}

TEST_F(AdminBackpressureTest, PendingResumeIsNotScheduledTwice) {
  preexisting_watermarks_ = 1;
  start();
  prepareResume();
  filter_.onBelowWriteBufferLowWatermark();
  EXPECT_CALL(*resume_, scheduleCallbackCurrentIteration()).Times(0);
  filter_.onAboveWriteBufferHighWatermark();
  filter_.onBelowWriteBufferLowWatermark();
  resume_->invokeCallback();
  EXPECT_EQ(state_->next_calls, 2);
}

TEST_F(AdminBackpressureTest, EmptyNonFinalChunkYields) {
  state_->chunks[0].first.clear();
  prepareResume(true);
  start();
  EXPECT_EQ(state_->next_calls, 1);
  EXPECT_TRUE(sent_.empty());
  resume_->invokeCallback();
  EXPECT_EQ(state_->next_calls, 2);
  EXPECT_EQ(sent_, (std::vector<std::pair<std::string, bool>>{{"two", true}}));
}

TEST_F(AdminBackpressureTest, EmptyFinalChunkEndsStream) {
  state_->chunks[1].first.clear();
  start();
  EXPECT_EQ(sent_.back(), (std::pair<std::string, bool>{"", true}));
  EXPECT_EQ(state_->destructions, 1);
}

TEST_F(AdminBackpressureTest, RepeatedEmptyChunksYieldBetweenRetries) {
  state_->chunks = {{"", true}, {"", true}, {"last", false}};
  prepareResume(true);
  start();
  EXPECT_EQ(state_->next_calls, 1);
  EXPECT_TRUE(resume_->enabled_);
  resume_->invokeCallback();
  EXPECT_EQ(state_->next_calls, 2);
  EXPECT_TRUE(resume_->enabled_);
  EXPECT_TRUE(sent_.empty());
  resume_->invokeCallback();
  EXPECT_EQ(state_->next_calls, 3);
  EXPECT_EQ(sent_, (std::vector<std::pair<std::string, bool>>{{"last", true}}));
}

TEST_F(AdminBackpressureTest, AutomaticEndStreamCanRemainDisabled) {
  filter_.setEndStreamOnComplete(false);
  start();
  EXPECT_EQ(state_->destructions, 1);
  EXPECT_EQ(sent_.back(), (std::pair<std::string, bool>{"two", false}));
}

TEST_F(AdminBackpressureTest, DestructionReleasesPausedRequestAfterCallbacks) {
  preexisting_watermarks_ = 1;
  filter_.addOnDestroyCallback([this]() { EXPECT_EQ(state_->destructions, 0); });
  start();
  filter_.onDestroy();
  EXPECT_EQ(state_->destructions, 1);
  EXPECT_EQ(state_->next_calls, 0);
  filter_.onDestroy();
  EXPECT_EQ(state_->destructions, 1);
}

TEST_F(AdminBackpressureTest, DestructionCancelsPendingResume) {
  preexisting_watermarks_ = 1;
  start();
  prepareResume();
  filter_.onBelowWriteBufferLowWatermark();
  EXPECT_TRUE(resume_->enabled_);
  filter_.onDestroy();
  EXPECT_FALSE(resume_->enabled_);
  late_resume_();
  EXPECT_EQ(state_->next_calls, 0);
  EXPECT_EQ(state_->destructions, 1);
}

TEST_F(AdminBackpressureTest, DestructionDuringStartKeepsHandlerAlive) {
  state_->on_start = [this]() {
    filter_.onDestroy();
    EXPECT_EQ(state_->destructions, 0);
  };
  start();
  EXPECT_EQ(state_->destructions, 1);
  EXPECT_EQ(header_calls_, 0);
  EXPECT_TRUE(sent_.empty());
}

TEST_F(AdminBackpressureTest, DestructionDuringNextChunkKeepsHandlerAlive) {
  state_->on_next = [this]() {
    filter_.onDestroy();
    EXPECT_EQ(state_->destructions, 0);
  };
  start();
  EXPECT_EQ(state_->destructions, 1);
  EXPECT_EQ(state_->next_calls, 1);
  EXPECT_TRUE(sent_.empty());
}

TEST_F(AdminBackpressureTest, DestructionDuringHeadersStopsProduction) {
  on_headers_ = [this]() {
    filter_.onDestroy();
    EXPECT_EQ(state_->destructions, 0);
  };
  start();
  EXPECT_EQ(state_->destructions, 1);
  EXPECT_EQ(state_->next_calls, 0);
}

TEST_F(AdminBackpressureTest, DestructionDuringEncodingStopsProduction) {
  on_encode_ = [this]() {
    filter_.onDestroy();
    EXPECT_EQ(state_->destructions, 0);
  };
  start();
  EXPECT_EQ(state_->destructions, 1);
  EXPECT_EQ(state_->next_calls, 1);
}

} // namespace Server
} // namespace Envoy
