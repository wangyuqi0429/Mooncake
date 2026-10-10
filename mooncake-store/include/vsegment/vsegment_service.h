#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "vsegment/vsegment_manager.h"
#include "vsegment/vsegment_transfer.h"
#include "partition/vsegment_types.h"

namespace mooncake::vsegment {

// SubMaster-local registry. One instance owns only the Partitions currently
// routed to that SubMaster; Partition migration removes/adds the whole manager.
class VSegmentService final : public VSegmentViewProvider {
   public:
    explicit VSegmentService(PartitionPhysicalQuotaSnapshot quota_snapshot)
        : quota_snapshot_(std::move(quota_snapshot)) {}

    ErrorCode AddPartition(
        const std::string& partition_id, uint64_t route_epoch,
        std::shared_ptr<VSegmentStateCommitter> committer,
        const PartitionVSegmentSnapshot* recovered = nullptr,
        std::string* detail = nullptr);
    ErrorCode RemovePartition(const std::string& partition_id,
                              uint64_t route_epoch);
    ErrorCode ReconcilePartitionRoute(
        const partition::PartitionRoute& route,
        const std::string& local_submaster_id,
        std::shared_ptr<VSegmentStateCommitter> committer,
        const PartitionVSegmentSnapshot* recovered = nullptr,
        std::string* detail = nullptr);
    ErrorCode SnapshotPartition(const std::string& partition_id,
                                PartitionVSegmentSnapshot* snapshot);
    std::vector<PartitionVSegmentSnapshot> SnapshotAllPartitions();
    ErrorCode ReconcileObjectReferences(
        const std::string& partition_id,
        const std::vector<VSegmentObjectReference>& references,
        std::string* detail = nullptr);
    const PartitionPhysicalQuotaSnapshot& quota_snapshot() const {
        return quota_snapshot_;
    }
    // 注入运行时坏端点排除集提供者，并向所有已存在的 partition manager
    // 透传；后续新增的 manager 在 AddPartition 时同样透传。物理分配时用
    // 于把新 vsegment 的条带成员避开已判死的物理段。
    void SetExcludedSegmentsProvider(
        std::function<std::set<std::string>()> provider);

    VSegmentPutStartResult StartPut(const std::string& partition_id,
                                    uint64_t route_epoch,
                                    const std::string& operation_id,
                                    uint64_t length,
                                    const std::string& profile_name = {});
    VSegmentPutStartResult StartPutOwned(
        const std::string& partition_id, const std::string& operation_id,
        uint64_t length, const std::string& profile_name = {});
    tl::expected<std::vector<VSegmentPutStartResult>, ErrorCode>
    StartPutReplicasOwned(const std::string& partition_id,
                          const std::vector<std::string>& operation_ids,
                          uint64_t length,
                          const std::string& profile_name = {});
    ErrorCode CommitPut(const VSegmentDescriptor& replica,
                        uint64_t route_epoch,
                        const std::string& operation_id,
                        const std::string& object_id);
    ErrorCode CommitPutOwned(const VSegmentDescriptor& replica,
                             const std::string& operation_id,
                             const std::string& object_id);
    ErrorCode AbortPut(const std::string& partition_id,
                       const std::string& vsegment_id,
                       uint64_t route_epoch,
                       const std::string& operation_id);
    ErrorCode AbortPutOwned(const std::string& partition_id,
                            const std::string& vsegment_id,
                            const std::string& operation_id);
    ErrorCode ReleaseObject(const VSegmentDescriptor& replica,
                            const std::string& object_id);

    ErrorCode LoadView(const std::string& partition_id,
                       const std::string& vsegment_id, VSegmentView* view,
                       std::string* detail = nullptr) override;

   private:
    void RefreshMetrics();
    std::shared_ptr<VSegmentManager> FindPartition(
        const std::string& partition_id);

    PartitionPhysicalQuotaSnapshot quota_snapshot_;
    std::function<std::set<std::string>()> excluded_segments_provider_;
    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<VSegmentManager>>
        partitions_;
};

}  // namespace mooncake::vsegment
