#include "source/server/admin/admin_filter.h"

#include "source/common/common/assert.h"
#include "source/server/admin/utils.h"

namespace Envoy {
namespace Server {

AdminFilter::AdminFilter(const Admin& admin) : admin_(admin) {}

void AdminFilter::setDecoderFilterCallbacks(Http::StreamDecoderFilterCallbacks& callbacks) {
  Http::PassThroughFilter::setDecoderFilterCallbacks(callbacks);
  watermark_callbacks_registered_ = true;
  callbacks.addDownstreamWatermarkCallbacks(*this);
}

void AdminFilter::onAboveWriteBufferHighWatermark() { ++high_watermark_count_; }

void AdminFilter::onBelowWriteBufferLowWatermark() {
  if (high_watermark_count_ == 0) {
    IS_ENVOY_BUG("admin low watermark without a preceding high watermark");
    return;
  }
  if (--high_watermark_count_ == 0 && request_ != nullptr && !destroyed_) {
    scheduleResume();
  }
}

void AdminFilter::scheduleResume(bool next_iteration) {
  if (destroyed_) {
    return;
  }
  if (resume_callback_ == nullptr) {
    resume_callback_ =
        decoder_callbacks_->dispatcher().createSchedulableCallback([this]() { pumpResponse(); });
  }
  if (!resume_callback_->enabled()) {
    if (next_iteration) {
      resume_callback_->scheduleCallbackNextIteration();
    } else {
      resume_callback_->scheduleCallbackCurrentIteration();
    }
  }
}

Http::FilterHeadersStatus AdminFilter::decodeHeaders(Http::RequestHeaderMap& headers,
                                                     bool end_stream) {
  // In-process admin requests use decodeHeaders without network filter callbacks.
  if (decoder_callbacks_ != nullptr) {
    const auto protocol = decoder_callbacks_->streamInfo().protocol();
    if (protocol == Http::Protocol::Http10 || protocol == Http::Protocol::Http11) {
      // HTTP/1 inherits the connection limit. Preserve unlimited admin request buffering
      // while enabling response watermarks; HTTP/2 retains its existing per-stream limit.
      decoder_callbacks_->setBufferLimit(0);
    }
  }
  request_headers_ = &headers;
  if (end_stream) {
    onComplete();
  }

  return Http::FilterHeadersStatus::StopIteration;
}

Http::FilterDataStatus AdminFilter::decodeData(Buffer::Instance& data, bool end_stream) {
  // Currently we generically buffer all admin request data in case a handler wants to use it.
  // If we ever support streaming admin requests we may need to revisit this. Note, we must use
  // addDecodedData() here since we might need to perform onComplete() processing if end_stream is
  // true.
  decoder_callbacks_->addDecodedData(data, false);

  if (end_stream) {
    onComplete();
  }

  return Http::FilterDataStatus::StopIterationNoBuffer;
}

Http::FilterTrailersStatus AdminFilter::decodeTrailers(Http::RequestTrailerMap&) {
  onComplete();
  return Http::FilterTrailersStatus::StopIteration;
}

void AdminFilter::onDestroy() {
  if (destroyed_) {
    return;
  }
  destroyed_ = true;
  if (resume_callback_ != nullptr) {
    resume_callback_->cancel();
  }
  if (watermark_callbacks_registered_) {
    decoder_callbacks_->removeDownstreamWatermarkCallbacks(*this);
    watermark_callbacks_registered_ = false;
  }
  // Keep handler state alive through destruction callbacks. During start/nextChunk,
  // the caller holds local ownership instead, so cancellation cannot destroy an active handler.
  Admin::RequestPtr handler = std::move(request_);
  for (const auto& callback : on_destroy_callbacks_) {
    callback();
  }
}

void AdminFilter::addOnDestroyCallback(std::function<void()> cb) {
  on_destroy_callbacks_.push_back(std::move(cb));
}

Http::StreamDecoderFilterCallbacks& AdminFilter::getDecoderFilterCallbacks() const {
  ASSERT(decoder_callbacks_ != nullptr);
  return *decoder_callbacks_;
}

const Buffer::Instance* AdminFilter::getRequestBody() const {
  return decoder_callbacks_->decodingBuffer();
}

const Http::RequestHeaderMap& AdminFilter::getRequestHeaders() const {
  ASSERT(request_headers_ != nullptr);
  return *request_headers_;
}

Http::Utility::QueryParamsMulti AdminFilter::queryParams() const {
  absl::string_view path = request_headers_->getPathValue();
  Http::Utility::QueryParamsMulti query =
      Http::Utility::QueryParamsMulti::parseAndDecodeQueryString(path);
  if (!query.data().empty()) {
    return query;
  }

  // Check if the params are in the request's body.
  if (request_headers_->getContentTypeValue() ==
      Http::Headers::get().ContentTypeValues.FormUrlEncoded) {
    const Buffer::Instance* body = getRequestBody();
    if (body != nullptr) {
      query = Http::Utility::QueryParamsMulti::parseParameters(body->toString(), 0, true);
    }
  }

  return query;
}

void AdminFilter::onComplete() {
  absl::string_view path = request_headers_->getPathValue();
  ENVOY_STREAM_LOG(debug, "request complete: path: {}", *decoder_callbacks_, path);

  auto header_map = Http::ResponseHeaderMapImpl::create();
  RELEASE_ASSERT(request_headers_, "");
  Admin::RequestPtr handler = admin_.makeRequest(*this);
  if (destroyed_) {
    return;
  }
  Http::Code code = handler->start(*header_map);
  if (destroyed_) {
    return;
  }
  Utility::populateFallbackResponseHeaders(code, *header_map);
  decoder_callbacks_->encodeHeaders(std::move(header_map), false,
                                    StreamInfo::ResponseCodeDetails::get().AdminFilterResponse);
  if (destroyed_) {
    return;
  }
  request_ = std::move(handler);
  pumpResponse();
}

void AdminFilter::pumpResponse() {
  if (destroyed_ || high_watermark_count_ != 0 || request_ == nullptr) {
    return;
  }
  Admin::RequestPtr handler = std::move(request_);
  while (!destroyed_ && high_watermark_count_ == 0) {
    Buffer::OwnedImpl response;
    const bool more_data = handler->nextChunk(response);
    if (destroyed_) {
      return;
    }
    const bool has_data = response.length() > 0;
    bool end_stream = end_stream_on_complete_ && !more_data;
    ENVOY_LOG_MISC(debug, "nextChunk: response.length={} more_data={} end_stream={}",
                   response.length(), more_data, end_stream);
    if (has_data || end_stream) {
      decoder_callbacks_->encodeData(response, end_stream);
    }
    if (destroyed_ || !more_data) {
      return;
    }
    if (!has_data) {
      // Poll for I/O before retrying an empty non-final chunk. Repeated empty chunks
      // must not keep reactivating this callback within the same event-loop iteration.
      request_ = std::move(handler);
      scheduleResume(true);
      return;
    }
  }
  if (!destroyed_) {
    request_ = std::move(handler);
  }
}

} // namespace Server
} // namespace Envoy
