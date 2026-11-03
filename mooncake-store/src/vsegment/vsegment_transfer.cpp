#include "vsegment/vsegment_transfer.h"

#include <limits>
#include <unordered_map>

namespace mooncake::vsegment {

namespace {

std::string ViewCacheKey(const std::string& partition_id,
                         const std::string& vsegment_id) {
    return partition_id + '\0' + vsegment_id;
}

bool SameImmutableView(const VSegmentView& left,
                       const VSegmentView& right) {
    if (left.vsegment_id != right.vsegment_id ||
        left.partition_id != right.partition_id ||
        left.mapping_algorithm != right.mapping_algorithm ||
        left.stripe_size != right.stripe_size ||
        left.logical_capacity != right.logical_capacity ||
        left.members.size() != right.members.size()) {
        return false;
    }
    for (size_t index = 0; index < left.members.size(); ++index) {
        const auto& a = left.members[index];
        const auto& b = right.members[index];
        if (a.segment_id != b.segment_id || a.base_offset != b.base_offset ||
            a.length != b.length) {
            return false;
        }
    }
    return true;
}

}  // namespace

bool VSegmentViewCache::Find(const std::string& partition_id,
                             const std::string& vsegment_id,
                             VSegmentView* view) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = views_.find(ViewCacheKey(partition_id, vsegment_id));
    if (found == views_.end()) return false;
    if (view) *view = found->second;
    return true;
}

ErrorCode VSegmentViewCache::Insert(const VSegmentView& view,
                                    std::string* detail) {
    auto validation = ValidateViewStructure(view, detail);
    if (validation != ErrorCode::OK) return validation;
    std::lock_guard<std::mutex> lock(mutex_);
    const auto key = ViewCacheKey(view.partition_id, view.vsegment_id);
    auto found = views_.find(key);
    if (found != views_.end()) {
        if (!SameImmutableView(found->second, view)) {
            if (detail) {
                *detail = "immutable view identity has another layout";
            }
            return ErrorCode::INVALID_VERSION;
        }
        return ErrorCode::OK;
    }
    while (views_.size() >= max_entries_ && !insertion_order_.empty()) {
        const auto oldest = std::move(insertion_order_.front());
        insertion_order_.pop_front();
        views_.erase(oldest);
    }
    views_.emplace(key, view);
    insertion_order_.push_back(key);
    return ErrorCode::OK;
}

void VSegmentViewCache::Erase(const std::string& partition_id,
                              const std::string& vsegment_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto key = ViewCacheKey(partition_id, vsegment_id);
    views_.erase(key);
    insertion_order_.erase(
        std::remove(insertion_order_.begin(), insertion_order_.end(), key),
        insertion_order_.end());
}

size_t VSegmentViewCache::Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return views_.size();
}

VSegmentTransferPlan VSegmentTransferPlanner::Plan(
    const VSegmentDescriptor& replica,
    const std::vector<ClientSlice>& slices) {
    VSegmentTransferPlan plan;
    if (!provider_ || !endpoint_resolver_ || !cache_ ||
        replica.partition_id.empty() || replica.vsegment_id.empty() ||
        replica.length == 0) {
        plan.error = ErrorCode::INVALID_PARAMS;
        plan.detail = "invalid vsegment transfer planner input";
        return plan;
    }
    VSegmentView view;
    if (!cache_->Find(replica.partition_id, replica.vsegment_id, &view)) {
        auto result = provider_->LoadView(replica.partition_id,
                                          replica.vsegment_id, &view,
                                          &plan.detail);
        if (result != ErrorCode::OK) {
            plan.error = result;
            return plan;
        }
        result = cache_->Insert(view, &plan.detail);
        if (result != ErrorCode::OK) {
            plan.error = result;
            return plan;
        }
    }
    if (view.partition_id != replica.partition_id) {
        plan.error = ErrorCode::INVALID_VERSION;
        plan.detail = "cached view belongs to another Partition";
        return plan;
    }
    auto resolved = ResolveTransfer(view, replica.logical_offset,
                                    replica.length, slices);
    if (!resolved) return {resolved.error, {}, std::move(resolved.detail)};
    // Resolve each distinct psegment only once per Plan. The sub-request count
    // grows with (length / stripe_size) while the number of distinct segments
    // is bounded by member_count, so with a small stripe this loop used to
    // issue dozens of identical GetPSegmentEndpoint RPCs for the same three
    // answers (e.g. 128 calls for a 512 KiB object with member_count = 3).
    // The cache is local to this call: no lifetime or invalidation concerns.
    std::unordered_map<std::string, PSegmentLocation> endpoints;
    for (auto& request : resolved.requests) {
        auto [entry, inserted] = endpoints.try_emplace(request.segment_id);
        if (inserted) {
            auto result = endpoint_resolver_->ResolveLocation(
                request.segment_id, &entry->second);
            if (result != ErrorCode::OK || entry->second.endpoint.empty()) {
                plan.error = result == ErrorCode::OK
                                 ? ErrorCode::SEGMENT_NOT_FOUND
                                 : result;
                plan.detail =
                    "cannot resolve endpoint for " + request.segment_id;
                plan.requests.clear();
                return plan;
            }
        }
        const PSegmentLocation& location = entry->second;
        if (request.physical_offset >
            std::numeric_limits<uint64_t>::max() - location.base_address) {
            plan.error = ErrorCode::INVALID_PARAMS;
            plan.detail = "psegment target address overflows";
            plan.requests.clear();
            return plan;
        }
        const auto segment_offset = request.physical_offset;
        request.physical_offset += location.base_address;
        // NOTE: endpoint is shared by every sub-request of this segment, so it
        // must be copied rather than moved.
        if (location.backend == PSegmentBackend::NOF &&
            (location.io_alignment == 0 ||
             request.physical_offset % location.io_alignment != 0 ||
             request.client_address % location.io_alignment != 0 ||
             request.length % location.io_alignment != 0)) {
            return {ErrorCode::INVALID_PARAMS, {}, "unaligned NoF stripe or client slice"};
        }
        plan.requests.push_back({std::move(request), location.endpoint,
                                 location.backend, location.persistence_endpoint,
                                 segment_offset});
    }
    return plan;
}

}  // namespace mooncake::vsegment
