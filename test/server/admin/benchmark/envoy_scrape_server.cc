#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "envoy/server/admin.h"
#include "envoy/server/instance.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/store.h"

#include "source/common/common/fmt.h"
#include "source/common/stats/symbol_table.h"
#include "source/exe/main_common.h"
#include "source/server/admin/stats_handler.h"

namespace Envoy {
namespace {

uint64_t setting(const char* name, uint64_t default_value) {
  const char* raw = std::getenv(name);
  if (raw == nullptr) {
    return default_value;
  }
  const std::string value(raw);
  if (value.empty() || value.find_first_not_of("0123456789") != std::string::npos) {
    throw std::invalid_argument(fmt::format("{} must be an unsigned integer", name));
  }
  return std::stoull(value);
}

struct Distribution {
  uint64_t counters = setting("ENVOY_SCRAPE_COUNTERS", 1000);
  uint64_t gauges = setting("ENVOY_SCRAPE_GAUGES", 100);
  uint64_t histograms = setting("ENVOY_SCRAPE_HISTOGRAMS", 100);
  uint64_t scopes = setting("ENVOY_SCRAPE_SCOPES", 8);
  uint64_t families = setting("ENVOY_SCRAPE_FAMILIES", 10);
  uint64_t shared_percent = setting("ENVOY_SCRAPE_SHARED_PERCENT", 80);
  uint64_t label_length = setting("ENVOY_SCRAPE_LABEL_LENGTH", 64);

  Distribution() {
    if (scopes == 0 || families == 0 || shared_percent > 100 || label_length > 16384) {
      throw std::invalid_argument("invalid scope/family count, shared percentage, or label length");
    }
  }
};

// This is a benchmark-only executable: the server, admin filter, codecs,
// network buffers, allocator and stats store are real. Only the metric
// population and extra routes are synthetic.
class ScrapeFixture : public std::enable_shared_from_this<ScrapeFixture> {
public:
  ScrapeFixture(Server::Instance& server, const Distribution& distribution)
      : server_(server), distribution_(distribution), buffered_handler_(server) {}

  void install() {
    auto self = shared_from_this();
    if (!server_.admin()->addHandler(
            "/benchmark/ready", "scrape benchmark readiness",
            [self](Http::ResponseHeaderMap&, Buffer::Instance& response, Server::AdminStream&) {
              response.add(fmt::format("{{\"ready\":{},\"finite_buckets\":{}}}\n", self->ready_,
                                       self->finite_buckets_));
              return self->ready_ ? Http::Code::OK : Http::Code::ServiceUnavailable;
            },
            true, false) ||
        !server_.admin()->addHandler(
            "/benchmark/prometheus-buffered", "retained buffered prometheus formatter",
            [self](Http::ResponseHeaderMap& headers, Buffer::Instance& response,
                   Server::AdminStream& stream) {
              return self->buffered_handler_.handlerPrometheusStats(headers, response, stream);
            },
            true, false)) {
      throw std::runtime_error("could not register benchmark handlers");
    }
    // Populate on Envoy's registered main thread. Publish readiness only after
    // the asynchronous histogram merge completes. No metric values change
    // during the subsequent measured scrape.
    server_.dispatcher().post([self]() { self->populate(); });
  }

private:
  void populate() {
    auto& store = server_.stats();
    for (uint64_t i = 0; i < distribution_.scopes; ++i) {
      // Distinct scope objects deliberately share the tag-extracted prefix.
      // Their explicit cluster/instance metric tags give them distinct backing
      // metrics within shared families.
      scopes_.push_back(store.createScope("scrape_bench."));
    }
    Stats::StatNamePool pool(store.symbolTable());
    std::vector<Stats::StatName> clusters;
    for (uint64_t i = 0; i < distribution_.scopes; ++i) {
      clusters.push_back(pool.add(fmt::format("scope_{:04d}", i)));
    }
    Stats::StatNameTagVector tags{
        {pool.add("cluster"), {}},
        {pool.add("instance"), {}},
        {pool.add("service"), pool.add("example_service")},
        {pool.add("method"), pool.add("GET")},
        {pool.add("route"),
         pool.add("/api/resources/" + std::string(distribution_.label_length, 'x'))}};
    const uint64_t shared_families = distribution_.families * distribution_.shared_percent / 100;
    for (const auto& [type, count] :
         std::vector<std::pair<std::string, uint64_t>>{{"counter", distribution_.counters},
                                                       {"gauge", distribution_.gauges},
                                                       {"histogram", distribution_.histograms}}) {
      // The pool retains only family/tag symbols; each per-instance symbol is
      // temporary.
      std::vector<std::vector<Stats::StatName>> names(distribution_.scopes);
      for (uint64_t scope = 0; scope < distribution_.scopes; ++scope) {
        for (uint64_t family = 0; family < distribution_.families; ++family) {
          const std::string name =
              family < shared_families
                  ? fmt::format("shared.{}.family_{}", type, family)
                  : fmt::format("local.scope_{:04d}.{}.family_{}", scope, type, family);
          names[scope].push_back(pool.add(name));
        }
      }
      for (uint64_t i = 0; i < count; ++i) {
        const uint64_t scope = i % distribution_.scopes;
        const uint64_t family = (i / distribution_.scopes) % distribution_.families;
        tags[0].second = clusters[scope];
        Stats::StatNameManagedStorage instance(fmt::format("{:010d}", i), store.symbolTable());
        tags[1].second = instance.statName();
        auto& metric_scope = *scopes_[scope];
        const auto name = names[scope][family];
        if (type == "counter") {
          metric_scope.counterFromTaggedName(name, Stats::StatNameTagSpan(tags), {})
              .add(1 + i % 1000);
        } else if (type == "gauge") {
          metric_scope
              .gaugeFromTaggedName(name, Stats::StatNameTagSpan(tags), {},
                                   Stats::Gauge::ImportMode::NeverImport)
              .set(i % 100);
        } else {
          auto& histogram = metric_scope.histogramFromTaggedName(
              name, Stats::StatNameTagSpan(tags), {}, Stats::Histogram::Unit::Milliseconds);
          for (const uint64_t value : {1, 5, 20, 50, 100, 500, 1000, 5000}) {
            histogram.recordValue(value + i % 7);
          }
        }
      }
    }
    dynamic_cast<Stats::StoreRoot&>(store).mergeHistograms([self = shared_from_this()]() {
      for (const auto& histogram : self->server_.stats().histograms()) {
        if (histogram->tagExtractedName().find("scrape_bench.") == 0) {
          self->finite_buckets_ = histogram->cumulativeStatistics().supportedBuckets().size();
          break;
        }
      }
      self->ready_ = true;
      std::cerr << "SCRAPE_BENCHMARK_READY\n";
    });
  }

  Server::Instance& server_;
  const Distribution distribution_;
  Server::StatsHandler buffered_handler_;
  std::vector<Stats::ScopeSharedPtr> scopes_;
  bool ready_{false};
  uint64_t finite_buckets_{0};
};

} // namespace
} // namespace Envoy

int main(int argc, char** argv) {
  try {
    const Envoy::Distribution distribution;
    return Envoy::MainCommon::main(argc, argv, [&distribution](Envoy::Server::Instance& server) {
      std::make_shared<Envoy::ScrapeFixture>(server, distribution)->install();
    });
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
