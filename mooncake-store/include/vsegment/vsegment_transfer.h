#pragma once

#include <algorithm>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "replica.h"
#include "vsegment/vsegment_runtime.h"

namespace mooncake::vsegment {

class VSegmentViewProvider {
   public:
    virtual ~VSegmentViewProvider() = default;
    virtual ErrorCode LoadView(const std::string& partition_id,
                               const std::string& vsegment_id,
                               VSegmentView* view,
                               std::string* detail = nullptr) = 0;
};

enum class PSegmentBackend : uint8_t { TRANSFER_ENGINE = 0, NOF = 1 };

struct PSegmentLocation {
    std::string endpoint;
    uint64_t base_address{0};
    PSegmentBackend backend{PSegmentBackend::TRANSFER_ENGINE};
    uint64_t io_alignment{1};
    std::string persistence_endpoint;
};
YLT_REFL(PSegmentLocation, endpoint, base_address, backend, io_alignment,
         persistence_endpoint);

class SegmentEndpointResolver {
   public:
    virtual ~SegmentEndpointResolver() = default;
    virtual ErrorCode ResolveLocation(const std::string& segment_id,
                                      PSegmentLocation* location) = 0;
};

class VSegmentViewCache {
   public:
    explicit VSegmentViewCache(size_t max_entries = 4096)
        : max_entries_(std::max<size_t>(1, max_entries)) {}
    bool Find(const std::string& partition_id, const std::string& vsegment_id,
              VSegmentView* view) const;
    ErrorCode Insert(const VSegmentView& view,
                     std::string* detail = nullptr);
    void Erase(const std::string& partition_id,
               const std::string& vsegment_id);
    size_t Size() const;

   private:
    const size_t max_entries_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, VSegmentView> views_;
    std::deque<std::string> insertion_order_;
};

struct EndpointTransferSubRequest {
    TransferSubRequest transfer;
    std::string endpoint;
    PSegmentBackend backend{PSegmentBackend::TRANSFER_ENGINE};
    std::string persistence_endpoint;
    uint64_t segment_offset{0};
};

struct VSegmentTransferPlan {
    ErrorCode error{ErrorCode::OK};
    std::vector<EndpointTransferSubRequest> requests;
    std::string detail;
    explicit operator bool() const { return error == ErrorCode::OK; }
};

class VSegmentTransferPlanner {
   public:
    VSegmentTransferPlanner(VSegmentViewProvider* provider,
                            SegmentEndpointResolver* endpoint_resolver,
                            VSegmentViewCache* cache)
        : provider_(provider),
          endpoint_resolver_(endpoint_resolver),
          cache_(cache) {}

    VSegmentTransferPlan Plan(const VSegmentDescriptor& replica,
                              const std::vector<ClientSlice>& slices);

   private:
    VSegmentViewProvider* provider_;
    SegmentEndpointResolver* endpoint_resolver_;
    VSegmentViewCache* cache_;
};

}  // namespace mooncake::vsegment
