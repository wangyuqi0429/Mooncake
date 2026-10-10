#include "vsegment/vsegment_service.h"

#include "vsegment/vsegment_metrics.h"

namespace mooncake::vsegment {

void VSegmentService::RefreshMetrics() {
    std::vector<std::shared_ptr<VSegmentManager>> managers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [id, manager] : partitions_) {
            (void)id;
            managers.push_back(manager);
        }
    }
    uint64_t total = 0, free = 0, reservations = 0, committed = 0,
             pending = 0;
    for (const auto& manager : managers) {
        const auto stats = manager->Stats();
        total += stats.logical_capacity_bytes;
        free += stats.logical_free_bytes;
        reservations += stats.reservations;
        committed += stats.committed_allocations;
        pending += stats.pending_creations;
    }
    VSegmentMetrics::Instance().SetCapacity(total, free, reservations,
                                            committed, pending);
}

std::shared_ptr<VSegmentManager> VSegmentService::FindPartition(
    const std::string& partition_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = partitions_.find(partition_id);
    return found == partitions_.end() ? nullptr : found->second;
}

void VSegmentService::SetExcludedSegmentsProvider(
    std::function<std::set<std::string>()> provider) {
    std::vector<std::shared_ptr<VSegmentManager>> managers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        excluded_segments_provider_ = provider;
        managers.reserve(partitions_.size());
        for (const auto& [id, manager] : partitions_) {
            (void)id;
            managers.push_back(manager);
        }
    }
    for (const auto& manager : managers)
        manager->SetExcludedSegmentsProvider(provider);
}

ErrorCode VSegmentService::AddPartition(
    const std::string& partition_id, uint64_t route_epoch,
    std::shared_ptr<VSegmentStateCommitter> committer,
    const PartitionVSegmentSnapshot* recovered, std::string* detail) {
    if (partition_id.empty() || route_epoch == 0 || !committer)
        return ErrorCode::INVALID_PARAMS;
    PartitionVSegmentConfig default_config;
    auto result = BuildPartitionConfig(quota_snapshot_, partition_id,
                                       quota_snapshot_.default_profile,
                                       &default_config, detail);
    if (result != ErrorCode::OK) return result;
    auto manager = std::make_shared<VSegmentManager>(
        quota_snapshot_, partition_id, std::move(committer));
    {
        // 透传排除集提供者（若在 service 构造后注入）。持 service mutex_ 读
        // provider 指针，与 SetExcludedSegmentsProvider 的写入互斥。
        std::lock_guard<std::mutex> lock(mutex_);
        if (excluded_segments_provider_)
            manager->SetExcludedSegmentsProvider(excluded_segments_provider_);
    }
    result = manager->SetRouteEpoch(route_epoch);
    if (result != ErrorCode::OK) return result;
    if (recovered) {
        if (recovered->route_epoch > route_epoch) return ErrorCode::STALE_ROUTE;
        auto state = *recovered;
        state.route_epoch = route_epoch;
        result = manager->Restore(state, detail);
        if (result != ErrorCode::OK) {
            VSegmentMetrics::Instance().IncRecoveryFailure();
            return result;
        }
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (partitions_.count(partition_id))
            return ErrorCode::SEGMENT_ALREADY_EXISTS;
        partitions_.emplace(partition_id, manager);
    }
    // Keep the manager installed if precreation persistence fails. Its
    // already-durable views must remain reserved, and the next ownership
    // reconciliation can safely retry only the missing count.
    result = manager->EnsureInitialVSegments(detail);
    RefreshMetrics();
    return result;
}

ErrorCode VSegmentService::RemovePartition(const std::string& partition_id,
                                           uint64_t route_epoch) {
    std::shared_ptr<VSegmentManager> removed;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = partitions_.find(partition_id);
        if (found == partitions_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        if (found->second->SetRouteEpoch(route_epoch) != ErrorCode::OK)
            return ErrorCode::STALE_ROUTE;
        removed = std::move(found->second);
        partitions_.erase(found);
    }
    // Destruction outside the registry lock waits for any asynchronous
    // creation without blocking unrelated Partitions.
    RefreshMetrics();
    return ErrorCode::OK;
}

ErrorCode VSegmentService::SnapshotPartition(
    const std::string& partition_id, PartitionVSegmentSnapshot* snapshot) {
    if (!snapshot) return ErrorCode::INVALID_PARAMS;
    auto manager = FindPartition(partition_id);
    if (!manager) return ErrorCode::STALE_ROUTE;
    *snapshot = manager->Snapshot();
    return ErrorCode::OK;
}

ErrorCode VSegmentService::ReconcilePartitionRoute(
    const partition::PartitionRoute& route,
    const std::string& local_submaster_id,
    std::shared_ptr<VSegmentStateCommitter> committer,
    const PartitionVSegmentSnapshot* recovered, std::string* detail) {
    const auto& partition_id = route.partition_id.partition_id;
    if (partition_id.empty() || local_submaster_id.empty() ||
        route.route_epoch == 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (route.state !=
            static_cast<int32_t>(partition::PartitionState::kActive) &&
        route.state !=
            static_cast<int32_t>(partition::PartitionState::kMigrating)) {
        if (detail) *detail = "unknown PartitionRoute state";
        return ErrorCode::INVALID_PARAMS;
    }
    const bool owned_here =
        route.owner_submaster_id == local_submaster_id;
    if (!owned_here) {
        auto current = FindPartition(partition_id);
        if (!current) return ErrorCode::OK;
        return RemovePartition(partition_id, route.route_epoch);
    }

    // During MIGRATING the old owner remains the sole writer. The target does
    // not install a live manager until the route atomically switches owner.
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = partitions_.find(partition_id);
        if (found != partitions_.end()) {
            auto result = found->second->SetRouteEpoch(route.route_epoch);
            if (result != ErrorCode::OK) return result;
            return found->second->EnsureInitialVSegments(detail);
        }
    }
    return AddPartition(partition_id, route.route_epoch,
                        std::move(committer), recovered, detail);
}

std::vector<PartitionVSegmentSnapshot>
VSegmentService::SnapshotAllPartitions() {
    std::vector<std::shared_ptr<VSegmentManager>> managers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        managers.reserve(partitions_.size());
        for (const auto& [partition_id, manager] : partitions_) {
            (void)partition_id;
            managers.push_back(manager);
        }
    }

    std::vector<PartitionVSegmentSnapshot> snapshots;
    snapshots.reserve(managers.size());
    for (const auto& manager : managers) snapshots.push_back(manager->Snapshot());
    return snapshots;
}

ErrorCode VSegmentService::ReconcileObjectReferences(
    const std::string& partition_id,
    const std::vector<VSegmentObjectReference>& references,
    std::string* detail) {
    auto manager = FindPartition(partition_id);
    if (!manager) return ErrorCode::STALE_ROUTE;
    const auto result = manager->ReconcileObjectReferences(references, detail);
    if (result != ErrorCode::OK)
        VSegmentMetrics::Instance().IncRecoveryFailure();
    RefreshMetrics();
    return result;
}

VSegmentPutStartResult VSegmentService::StartPut(
    const std::string& partition_id, uint64_t route_epoch,
    const std::string& operation_id, uint64_t length,
    const std::string& profile_name) {
    auto manager = FindPartition(partition_id);
    if (!manager)
        return {ErrorCode::STALE_ROUTE, operation_id, {},
                "Partition is not owned by this SubMaster"};
    auto result =
        manager->StartPut(operation_id, length, profile_name, route_epoch);
    if (result.error == ErrorCode::STALE_ROUTE)
        VSegmentMetrics::Instance().IncStaleRoute();
    RefreshMetrics();
    return result;
}

VSegmentPutStartResult VSegmentService::StartPutOwned(
    const std::string& partition_id, const std::string& operation_id,
    uint64_t length, const std::string& profile_name) {
    auto manager = FindPartition(partition_id);
    if (!manager)
        return {ErrorCode::STALE_ROUTE, operation_id, {},
                "Partition is not owned by this SubMaster"};
    auto result = manager->StartPut(operation_id, length, profile_name,
                                    manager->Snapshot().route_epoch);
    RefreshMetrics();
    return result;
}

tl::expected<std::vector<VSegmentPutStartResult>, ErrorCode>
VSegmentService::StartPutReplicasOwned(
    const std::string& partition_id,
    const std::vector<std::string>& operation_ids, uint64_t length,
    const std::string& profile_name) {
    auto manager = FindPartition(partition_id);
    if (!manager) return tl::make_unexpected(ErrorCode::STALE_ROUTE);
    const auto epoch = manager->Snapshot().route_epoch;
    std::vector<VSegmentPutStartResult> results;
    std::vector<std::string> selected;
    results.reserve(operation_ids.size());
    for (const auto& operation_id : operation_ids) {
        auto result = manager->StartPut(operation_id, length, profile_name,
                                        epoch, selected);
        if (!result) {
            // The group was not returned to the caller. Roll its earlier
            // reservations back without creating terminal abort tombstones,
            // allowing the same operation ids to retry after async creation.
            for (const auto& prior : results) {
                const auto rollback = manager->RollbackPutReservation(
                    prior.replica.vsegment_id, prior.operation_id, epoch);
                if (rollback != ErrorCode::OK)
                    return tl::make_unexpected(rollback);
            }
            return tl::make_unexpected(result.error);
        }
        selected.push_back(result.replica.vsegment_id);
        results.push_back(std::move(result));
    }
    RefreshMetrics();
    return results;
}

ErrorCode VSegmentService::CommitPut(const VSegmentDescriptor& replica,
                                     uint64_t route_epoch,
                                     const std::string& operation_id,
                                     const std::string& object_id) {
    auto manager = FindPartition(replica.partition_id);
    if (!manager) return ErrorCode::STALE_ROUTE;
    LogicalRange committed;
    const LogicalRange expected{replica.logical_offset, replica.length};
    auto result = manager->CommitPut(replica.vsegment_id, operation_id,
                                     object_id, &committed, route_epoch,
                                     &expected);
    if (result != ErrorCode::OK) return result;
    RefreshMetrics();
    return ErrorCode::OK;
}

ErrorCode VSegmentService::CommitPutOwned(
    const VSegmentDescriptor& replica, const std::string& operation_id,
    const std::string& object_id) {
    auto manager = FindPartition(replica.partition_id);
    return manager ? CommitPut(replica, manager->Snapshot().route_epoch,
                               operation_id, object_id)
                   : ErrorCode::STALE_ROUTE;
}

ErrorCode VSegmentService::AbortPut(const std::string& partition_id,
                                    const std::string& vsegment_id,
                                    uint64_t route_epoch,
                                    const std::string& operation_id) {
    auto manager = FindPartition(partition_id);
    if (!manager) return ErrorCode::STALE_ROUTE;
    const auto result =
        manager->AbortPut(vsegment_id, operation_id, route_epoch);
    RefreshMetrics();
    return result;
}

ErrorCode VSegmentService::AbortPutOwned(
    const std::string& partition_id, const std::string& vsegment_id,
    const std::string& operation_id) {
    auto manager = FindPartition(partition_id);
    return manager ? AbortPut(partition_id, vsegment_id,
                              manager->Snapshot().route_epoch, operation_id)
                   : ErrorCode::STALE_ROUTE;
}

ErrorCode VSegmentService::ReleaseObject(const VSegmentDescriptor& replica,
                                         const std::string& object_id) {
    if (object_id.empty() || replica.partition_id.empty() ||
        replica.vsegment_id.empty() || replica.length == 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    // Keep the registry lock through release so a concurrent route reconcile
    // cannot detach or advance this manager between ownership validation and
    // the persistent logical-range update.
    std::unique_lock<std::mutex> lock(mutex_);
    auto found = partitions_.find(replica.partition_id);
    if (found == partitions_.end()) return ErrorCode::STALE_ROUTE;
    const auto epoch = found->second->Snapshot().route_epoch;
    const auto result = found->second->ReleaseObject(
        replica.vsegment_id, object_id,
        {replica.logical_offset, replica.length}, epoch);
    lock.unlock();
    if (result == ErrorCode::OK || result == ErrorCode::OBJECT_NOT_FOUND)
        VSegmentMetrics::Instance().IncReleaseSuccess();
    else
        VSegmentMetrics::Instance().IncReleaseFailure();
    RefreshMetrics();
    return result == ErrorCode::OBJECT_NOT_FOUND ? ErrorCode::OK : result;
}

ErrorCode VSegmentService::LoadView(const std::string& partition_id,
                                    const std::string& vsegment_id,
                                    VSegmentView* view, std::string* detail) {
    auto manager = FindPartition(partition_id);
    if (!manager) {
        if (detail) *detail = "Partition is not owned by this SubMaster";
        return ErrorCode::STALE_ROUTE;
    }
    if (!manager->FindView(vsegment_id, view)) {
        if (detail) *detail = "vsegment view is not active";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    return ErrorCode::OK;
}

}  // namespace mooncake::vsegment
