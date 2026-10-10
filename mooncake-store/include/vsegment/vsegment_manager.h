#pragma once

#include <functional>
#include <memory>
#include <future>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "vsegment/vsegment.h"
#include "vsegment/vsegment_runtime.h"
#include "replica.h"

namespace mooncake::vsegment {

struct VSegmentStateSnapshot {
    std::string profile_name;
    Lifecycle lifecycle{Lifecycle::PREPARING};
    VSegmentView view;
    LogicalAllocationSnapshot logical_allocation;
};
YLT_REFL(VSegmentStateSnapshot, profile_name, lifecycle, view,
         logical_allocation);

struct PartitionVSegmentSnapshot {
    std::string partition_id;
    uint64_t config_generation{0};
    uint64_t route_epoch{0};
    uint64_t metadata_revision{0};
    std::vector<VSegmentStateSnapshot> vsegments;
    std::unordered_map<std::string, std::string> operation_vsegments;
};
YLT_REFL(PartitionVSegmentSnapshot, partition_id, config_generation,
         route_epoch, metadata_revision, vsegments, operation_vsegments);

class VSegmentStateCommitter {
   public:
    virtual ~VSegmentStateCommitter() = default;
    virtual ErrorCode Commit(const PartitionVSegmentSnapshot& state,
                             const std::string& mutation,
                             std::string* detail) = 0;
};

struct VSegmentPutStartResult {
    ErrorCode error{ErrorCode::OK};
    std::string operation_id;
    VSegmentDescriptor replica;
    std::string detail;
    explicit operator bool() const { return error == ErrorCode::OK; }
};
YLT_REFL(VSegmentPutStartResult, error, operation_id, replica, detail);

struct VSegmentManagerStats {
    uint64_t logical_capacity_bytes{0};
    uint64_t logical_free_bytes{0};
    size_t preparing{0};
    size_t active{0};
    size_t draining{0};
    size_t retired{0};
    size_t reservations{0};
    size_t committed_allocations{0};
    size_t pending_creations{0};
};

// ObjectMetadata is authoritative after recovery. These references are used
// once, before serving traffic, to remove allocator state left behind by a
// crash between the vsegment-state and object-metadata durable records.
struct VSegmentObjectReference {
    VSegmentDescriptor replica;
    std::string allocation_id;
    bool committed{false};
};

// Partition-local facade intended to be owned by the Partition's SubMaster.
// Persistence transport is deliberately outside this class: HA Snapshot/OpLog
// code serializes the returned state and calls Restore after failover.
class VSegmentManager {
   public:
    VSegmentManager(PartitionVSegmentConfig config,
                    std::vector<VSegmentProfile> profiles,
                    std::shared_ptr<VSegmentStateCommitter> committer,
                    size_t max_operation_tombstones = 4096);
    VSegmentManager(const PartitionPhysicalQuotaSnapshot& quota_snapshot,
                    std::string partition_id,
                    std::shared_ptr<VSegmentStateCommitter> committer,
                    size_t max_operation_tombstones = 4096);

    VSegmentAllocationResult Create(const std::string& profile_name);
    // Hot-path API: starts at most one background creation per
    // partition/profile and asks callers to retry while it is running.
    VSegmentAllocationResult RequestCreate(const std::string& profile_name,
                                           uint64_t retry_after_ms = 50);
    VSegmentPutStartResult StartPut(const std::string& operation_id,
                                    uint64_t length,
                                    const std::string& profile_name = {},
                                    uint64_t expected_route_epoch = 0,
                                    const std::vector<std::string>&
                                        excluded_vsegments = {});
    ErrorCode SetRouteEpoch(uint64_t route_epoch);
    ErrorCode EnsureInitialVSegments(std::string* detail = nullptr);
    ReservationResult ReservePut(const std::string& vsegment_id,
                                 const std::string& operation_id,
                                 uint64_t length,
                                 uint64_t expected_route_epoch = 0);
    ErrorCode CommitPut(const std::string& vsegment_id,
                        const std::string& operation_id,
                        const std::string& allocation_id,
                        LogicalRange* range = nullptr,
                        uint64_t expected_route_epoch = 0,
                        const LogicalRange* expected_range = nullptr);
    ErrorCode AbortPut(const std::string& vsegment_id,
                       const std::string& operation_id,
                       uint64_t expected_route_epoch = 0);
    ErrorCode RollbackPutReservation(const std::string& vsegment_id,
                                     const std::string& operation_id,
                                     uint64_t expected_route_epoch = 0);
    ErrorCode ReleaseObject(const std::string& vsegment_id,
                            const std::string& allocation_id,
                            LogicalRange range,
                            uint64_t expected_route_epoch = 0);
    ErrorCode ForgetOperation(const std::string& operation_id);
    ErrorCode TransitionLifecycle(const std::string& vsegment_id,
                                  Lifecycle target);

    PartitionVSegmentSnapshot Snapshot() const;
    ErrorCode Restore(const PartitionVSegmentSnapshot& snapshot,
                      std::string* detail = nullptr);
    ErrorCode ReconcileObjectReferences(
        const std::vector<VSegmentObjectReference>& references,
        std::string* detail = nullptr);
    bool FindView(const std::string& vsegment_id, VSegmentView* view) const;
    VSegmentManagerStats Stats() const;
    // 注入运行时坏端点排除集提供者（te_endpoint 与 segment_name 双形式）。
    // 物理分配（CreateSingleFlight）时调用获取快照，跳过其中的段，避免把
    // 新 vsegment 的条带成员落在已判死的物理段上。锁序：manager mutex_ →
    // provider 内的 invalid 锁（与 MountSegment 自愈 erase 方向一致）。
    void SetExcludedSegmentsProvider(
        std::function<std::set<std::string>()> provider) {
        excluded_segments_provider_ = std::move(provider);
    }

   private:
    struct ManagedVSegment {
        std::string profile_name;
        Lifecycle lifecycle{Lifecycle::PREPARING};
        VSegmentView view;
        std::unique_ptr<LogicalRangeAllocator> logical_allocator;
    };

    VSegmentAllocationResult CreateSingleFlight(const std::string& profile_name,
                                                 uint64_t expected_route_epoch = 0);
    PartitionVSegmentSnapshot SnapshotLocked() const;
    ErrorCode PersistLocked(const std::string& mutation,
                            std::string* detail = nullptr);
    void TrimOperationTombstonesLocked(ManagedVSegment& managed,
                                       size_t max_completed);
    std::string partition_id_;
    uint64_t config_generation_{0};
    std::string default_profile_;
    std::unordered_map<std::string, VSegmentProfile> profiles_;
    std::vector<PartitionVSegmentConfig> quota_configs_;
    PartitionQuotaAllocator physical_allocator_;
    CreationCoordinator profile_creation_coordinator_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, ManagedVSegment> vsegments_;
    std::unordered_map<std::string, std::string> operation_vsegments_;
    uint64_t route_epoch_{0};
    uint64_t metadata_revision_{0};
    std::shared_ptr<VSegmentStateCommitter> committer_;
    size_t max_operation_tombstones_{4096};
    mutable std::mutex pending_mutex_;
    std::unordered_map<std::string,
                       std::shared_future<VSegmentAllocationResult>>
        pending_creations_;
    std::function<std::set<std::string>()> excluded_segments_provider_;
};

}  // namespace mooncake::vsegment
