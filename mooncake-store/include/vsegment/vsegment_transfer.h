#pragma once

#include <optional>
#include <string>
#include <vector>

#include "replica.h"
#include "rpc_types.h"
#include "vsegment/vsegment_runtime.h"

namespace mooncake::vsegment {

// Hard C: the runtime-resolved layout table carried by the RPC response is the
// ONLY source of view + endpoint information. There is no view cache, no
// provider, no endpoint resolver and no fallback path: a missing or invalid
// entry makes the transfer fail.

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
    std::optional<VSegmentDescriptor> file_write_replica{};
    explicit operator bool() const { return error == ErrorCode::OK; }
};

class VSegmentTransferPlanner {
   public:
    VSegmentTransferPlan Plan(
        const VSegmentDescriptor& replica,
        const std::vector<ClientSlice>& slices,
        const std::vector<VSegmentLayoutEntry>* layouts) const;
};

}  // namespace mooncake::vsegment
