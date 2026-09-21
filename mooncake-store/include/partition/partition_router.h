#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "cvm/cvm_types.h"
#include "mutex.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {
namespace partition {

// client 侧路由：把逻辑 slot 解析为 submaster_id（即 master_id，其值等于
// RPC 端点 address）。映射来源为本地确定性哈希环推导（读成员列表 +
// cluster_meta 的 submaster_count），与服务端 ResolveOwnedSlotsForCvm 一致，
// 不再从 etcd 快照读逐 slot 归属。
//
// RingSlot 路由（§16.17）：优先读 ring_slots/ 建 rank 路由表（rank →
// primary master_id，G 条）；ring_slots 空（旧服务端/新集群）回退本地
// 建环。ResolveSubmaster 优先 rank 表。
class PartitionRouter {
   public:
    // 加载 slot → submaster 映射（覆盖式）。仅用于单元测试 / 兼容旧路径。
    void LoadSlotOwners(const std::vector<cvm::SlotOwner>& owners);

    // 读 cluster_meta + ring_slots/ 建 rank 路由表；ring_slots 空时回退
    // 本地建环（读成员列表 + submaster_count，§16.17.6 新旧混跑兜底）。
    ErrorCode LoadFromEtcdSnapshot(const std::string& cluster_namespace);

    // slot → submaster_id（primary_master_id）；未命中返回 nullopt。
    std::optional<std::string> ResolveSubmaster(uint16_t slot) const;

    // key → submaster_id（先哈希再路由）；未命中返回 nullopt。
    std::optional<std::string> Route(const TenantId& tenant,
                                     const std::string& key) const;

    void Clear();
    size_t Size() const;

   private:
    mutable SharedMutex mutex_;
    // ring_slots 路由表（§16.17.2）：rank → primary master_id。空 = 模型
    // 未启用（回退 slot_to_submaster_ 建环表）。
    uint32_t group_count_{1};  // RingMeta.slot_group_count (G)
    std::vector<std::string> rank_to_primary_;
    // 兼容旧路径/单测：显式逐 slot 覆盖（rank_to_primary_ 为空时兜底）。
    std::unordered_map<uint16_t, std::string> slot_to_submaster_;
};

}  // namespace partition
}  // namespace mooncake
