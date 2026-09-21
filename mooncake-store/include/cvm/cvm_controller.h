#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cvm/cvm_types.h"
#include "types.h"

namespace mooncake {
namespace cvm {

class CvmServiceDelegate;
class CvmHttpServer;

// ring_slots 快照的只读关系视图（快照模式，Snapshot Pattern）。
//
// 一次构造即在锁内完成拷贝（assigns + group_count），之后全部关系查询
// 在锁外进行——把「加锁→拷贝→锁外判断」的锁粒度约定从各消费点的注释
// 收拢为类的不变量（快照后与缓存解耦，长期持有安全）。G 折入快照，
// 消除「读 assigns 与读 G 两次加锁」的错配窗口。控制面所有归属判定
//（角色/配对/晋升/补位/自愈/告警门控）统一经由本类查询，避免
// 谓词逻辑散落各处产生行为漂移。
//
// 不提供任何写接口：写路径（CAS 状态推进）仍归 CvmController/
// reshard driver，读写分离保持明确。
class RingSlotsView {
   public:
    // 空快照（模型未启用 / controller 缺失时的占位值）。
    RingSlotsView() = default;
    // 由 CvmController 在 ring_slots_mutex_ 锁内构建（BuildRingSlotsView）。
    explicit RingSlotsView(std::vector<RingSlotAssign> assigns,
                           uint32_t group_count)
        : assigns_(std::move(assigns)), group_count_(group_count) {}

    // 模型是否启用（存在任一归属记录）。等价于
    // CvmController::HasCachedRingSlotAssigns 的快照时刻取值。
    bool active() const { return !assigns_.empty(); }

    // 快照的槽位组数 G（模型未启用时为 0；启用判定用 active()，见
    // HasCachedRingSlotAssigns 注释——旧集群 G 也落默认 1）。
    uint32_t group_count() const { return group_count_; }

    // id 是否是某 rank 的 primary（含兼管持有的多个 rank）。
    bool is_primary(const std::string& id) const;

    // id 是否是某 rank 的迁移目标（仅 kMigrating 状态认 migrating_to，
    // 与原散落谓词的状态门控一致）。原生认领 / 扩容热迁移的目标在
    // owner CAS 完成前不持有 primary_id，仍需判 kPrimary 拉起 driver
    //（§16.15.3）。
    bool is_migration_target(const std::string& id) const;

    // id 是否是某 rank 的配对 standby。out_rank 非空时回填首个配对
    //（1:1 配对下唯一）。
    bool is_standby(const std::string& id,
                    uint32_t* out_rank = nullptr) const;

    // id 是否已被任何归属记录覆盖（primary / standby / 迁移目标）。
    // 补位门控与兼管自愈门控共用。
    bool is_assigned(const std::string& id) const;

    // id 作为配对 standby 时，其 primary 的 master_id；非配对 standby
    // 返回空（GetBindingSources 的回放源解析）。
    std::string paired_primary_of(const std::string& id) const;

    // 原始快照（rank 升序）。需要逐 rank 遍历的消费者（晋升扫描/
    // 补位扫描/自愈扫描）直接使用，避免类接口为每个遍历场景膨胀。
    const std::vector<RingSlotAssign>& assigns() const { return assigns_; }

   private:
    std::vector<RingSlotAssign> assigns_;
    uint32_t group_count_{0};
};

// In-process control plane for the CVM (Cache View Master).
//
// Responsibilities:
//   - Register this master under an etcd lease (liveness + role).
//   - Persist the cluster-wide ring config (cluster_meta) once at startup.
//   - Coordinate the submaster quota (first-come-first-served ranking) and
//     publish role changes to the delegate.
//
// Slot ownership is derived deterministically from the primary member list
// (see slot_hash.h), so no per-slot ownership view is cached or watched here.
//
// RingSlot 槽位组归属（§16.14）：rank 级归属状态持久化于
// ring_slots/{rank}，本 controller 维护其内存缓存并由 RingSlotsWatchLoop
//（watch 即时 + 周期兜底）刷新，供角色判定 / 回放绑定 / slot 归属查询消费。
class CvmController {
   public:
    struct Config {
        std::string cluster_namespace;
        std::string master_id;
        std::string address;  // RPC endpoint of this master.
        MasterRole role = MasterRole::kPrimary;
        int64_t registration_lease_ttl_sec = 3;

        // CvmHttpServer（外部 HTTP 接口）配置；http_port == 0 表示不启动。
        std::string http_host = "0.0.0.0";
        uint16_t http_port = 0;

        // 集群中允许同时 serving 的 submaster 上限（名额协调，先到先得）。
        // 排名前 submaster_count 个 master 为 kPrimary，其余降级为 kStandby。
        uint32_t submaster_count = 1;

        // 成员协调（ReconcileRole）的调度周期。slot 归属为本地确定性推导，
        // 不再周期性回写视图快照。
        std::chrono::milliseconds sync_interval{5000};
    };

    explicit CvmController(Config config);
    ~CvmController();

    CvmController(const CvmController&) = delete;
    CvmController& operator=(const CvmController&) = delete;

    void SetDelegate(CvmServiceDelegate* delegate);

    // 当前角色（随名额协调动态变化）。
    MasterRole GetCurrentRole() const { return current_role_.load(); }

    // 排名第一（先到先得）的 primary 的 RPC 地址，作为 standby 的单源回放
    // 目标。当成员列表为空或本机即为排名第一的 primary 时返回空字符串。
    std::string GetPrimaryAddress();

    // standby 动态绑定：按「本 standby 负责的 slot 区间」，用本地一致性哈希
    // 环（与客户端/服务端一致）推导出拥有这些 slot 的 primary，作为回放源
    // （而非回放全部 primary）。本机为 primary 或无法确定负责区间时返回空列表。
    std::vector<MasterRegistration> GetBindingSources();

    // ---- RingSlots 缓存读取（步骤 2-4 消费接口，§16.14.6）----
    // 拷贝语义：锁内复制后返回，调用方长期持有安全。未命中（缓存空 /
    // rank 越界）返回 false；G=0 表示尚未加载。
    bool GetCachedRingSlotAssign(uint32_t rank, RingSlotAssign& out);
    uint32_t GetCachedSlotGroupCount();
    // ring_slots 模型是否启用（存在任一归属记录）。注意 group_count 不能
    // 作启用判定：旧集群（§15）的 cluster_meta 反序列化后 G 也落默认 1。
    bool HasCachedRingSlotAssigns();
    // 锁内快照构建 RingSlotsView（控制面关系查询的统一入口，见类注释；
    // 快照含 group_count——G 与 assigns 单锁一致，消费方无需再单独调
    // GetCachedSlotGroupCount，消除两锁错配窗口）。
    RingSlotsView BuildRingSlotsView();
    // 本进程是否曾观测到 ring_slots 记录（一旦启用永不回落，§16.8 G 恒定
    // 前提）。数据面幽灵写告警的门控：seen + 当前缓存空 = 写判定正在走
    // 旧环回退（危险瞬态），MasterService 据此打节流 WARNING；旧集群
    // （从未 seen）回退是正常稳态，不告警。
    bool HasSeenRingSlotsModel() const {
        return ring_slots_model_seen_.load(std::memory_order_relaxed);
    }

    ErrorCode Start();
    void Stop();

    // etcd lease id backing this master's registration. Callers may reuse it
    // for their own records (segment mounts) so they share the same lifecycle
    // and are auto-removed on master death. 0 until Start() succeeds.
    EtcdLeaseId GetLeaseId() const { return lease_id_; }

   private:
    struct WatchState {
        std::mutex mutex;
        std::condition_variable cv;
        bool dirty = false;
        bool broken = false;
    };

    Config config_;
    CvmServiceDelegate* delegate_ = nullptr;

    EtcdLeaseId lease_id_{0};
    std::atomic<bool> running_{false};
    std::atomic<bool> masters_watch_armed_{false};

    // 当前角色（随名额协调动态变化）；初始为启动配置的 role。
    std::atomic<MasterRole> current_role_{MasterRole::kPrimary};

    // 上次观测到的成员集（master_id 排序去重后），MastersWatchLoop 据此探测
    // 成员增删并打 membership change 日志（joined/left + 当前 primaries）。
    // 仅 MastersWatchLoop 线程访问，无需加锁。
    std::vector<std::string> last_member_ids_;

    std::unique_ptr<WatchState> masters_watch_state_;
    std::thread masters_watch_thread_;
    std::thread keepalive_thread_;
    std::thread membership_thread_;

    // ---- RingSlot 归属缓存与 watch（§16.14.6 / §16.19.4）----
    // ring_slots_mutex_ 保护整个视图（group_count + assigns）：
    // RingSlotsWatchLoop / MembershipLoop 写，GetCached* 读。assigns 按
    // rank 索引，G 上限 kSlotCount，size 与 group_count 允许暂不一致
    //（成员死亡晋升的修正流未完成时）。
    std::mutex ring_slots_mutex_;
    uint32_t ring_slot_group_count_{0};
    std::map<uint32_t, RingSlotAssign> ring_slots_cache_;
    // 本进程曾观测到 ring_slots 记录（模型启用标记，只置位不回落）。
    // 幽灵写告警门控（见 HasSeenRingSlotsModel）。
    std::atomic<bool> ring_slots_model_seen_{false};
    // 「模型已启用但 refresh 失败」节流告警的上次时刻（steady_clock ms）。
    // etcd 持续抖动时 MembershipLoop 每 5s 刷一次缓存，不节流会刷屏。
    std::atomic<int64_t> last_stale_cache_warn_ms_{0};
    // ---- bootstrap 启动竞态退避（§16.15.3，全新集群多节点同时启动）----
    // 首次观测「多成员在场且无记录」时随机退避 10-30s 再重判：给竞态中
    // 的另一节点（启动失败退出 / lease 过期）留收敛时间，幸存者下轮判定
    // 即成唯一成员。退避期满仍多成员 → 灰度门控生效（旧集群安全），仅
    // 首次打 WARNING 提示（新集群需先起单节点，旧集群忽略）。
    std::atomic<bool> bootstrap_defer_armed_{false};
    std::atomic<int64_t> bootstrap_defer_until_ms_{0};
    std::atomic<bool> multi_member_no_record_warned_{false};
    // rank → 首次观测到其 primary 死亡的时刻（steady_clock）。挂同一把
    // ring_slots_mutex_（ReconcileRole 的 watch/membership 双触发线程共享）。
    // 用途（§16.15.8 liveness backstop）：primary 死亡持续 >= 30s 时，
    // 越过 standby 优先级直接 CAS 接管——防「高优先 standby 活着但晋升
    // 逻辑卡死」导致 rank 永久无主。安全性由 epoch CAS 单赢家 + 失秩
    // 降级 fencing 承担。primary 恢复存活即清除。
    std::map<uint32_t, std::chrono::steady_clock::time_point>
        dead_primary_since_;
    std::atomic<bool> ring_slots_watch_armed_{false};
    std::unique_ptr<WatchState> ring_slots_watch_state_;
    std::thread ring_slots_watch_thread_;

    // ---- 补位/晋升/自愈可观测性（验证辅助，§16.23；防刷屏设计）----
    // 「变更触发」日志：路径结果相对上次 ReconcileRole 周期变化才打
    // 一条 INFO（含跳过原因统计，见 LogStepOutcome），稳态（连续
    // no-candidate）静默；逐候选跳过原因在 VLOG(1)（-v=1 开启）。
    // 索引 0-3 = 补位四阶段，4 = 晋升路径（TryPromoteDeadPrimaryRanks），
    // 5 = 兼管自愈（TryDesignatedTakeover）——见 cpp 匿名命名空间
    // AssignPhase。原子不加锁：ReconcileRole 的 watch/membership 双触发
    // 并发下最多漏/重一条诊断行，无害。
    std::atomic<int> last_phase_outcome_[6]{{-1}, {-1}, {-1}, {-1},
                                           {-1}, {-1}};
    std::atomic<bool> last_my_turn_{false};

    std::mutex membership_mutex_;
    std::condition_variable membership_cv_;

    std::unique_ptr<CvmHttpServer> http_server_;

    void CancelMastersWatchAndWait();
    void MastersWatchLoop();
    void KeepaliveLoop();
    void MembershipLoop();
    void CancelRingSlotsWatchAndWait();
    // 两层循环（外层 arm / 内层处理，broken 才重 arm），结构与
    // MastersWatchLoop 同构（避免 "already being watched" 刷屏教训）。
    void RingSlotsWatchLoop();
    // 全量重拉 ring_slots + cluster_meta(G) 并在锁内整体替换缓存。
    // watch 事件与周期兜底共用；失败保留旧缓存（宁可旧，不可无）。
    void RefreshRingSlotsCache();
    // 重算并回写角色（先到先得排名）；membership 轮询与 masters watch 回调
    // 共用，用 CAS 去重，保证并发下仅一次迁移与通知。
    void ReconcileRole();
    // §16.15.4 晋升 CAS：扫 ring_slots 找「primary 已死且本机是其最高优先
    // 存活 standby」的 rank，CASSwitchRingSlotOwner 抢 owner（standby 列表
    // 去掉本机）。成功/被抢（STALE）返回 true（缓存需刷新），本机晋升与
    // 否交由随后的 ComputeDesiredRole 判定。缓存空（模型未启用）返回 false。
    // 超时兜底：primary 死亡持续 >= kDeadPrimaryTakeoverTimeout（见 cpp），
    // 越过 standby 优先级直接接管（liveness backstop）。
    // 晋升 CAS 成功后幂等删除该 rank 的 reshard_intent 残留（§16.15.4/
    // §16.16.7：kStable 覆盖 kMigrating 即中止迁移，残留 intent 会把新
    // owner 误拉入多余迁移）。
    bool TryPromoteDeadPrimaryRanks(
        const std::vector<MasterRegistration>& members);
    // 新集群冷启动 bootstrap（§16.15.3 关键段）：唯一存活成员创建全 G 个
    // rank——rank0 原生持有，rank1..G-1 由本机兼管（primary_id 相同，
    // §16.8.1 隐含判定），保证 16384 slot 自始有 owner；G = submaster_count
    // 随 cluster_meta 一并定型（此后恒不变）。多成员在场不 bootstrap（旧
    // 集群灰度回退语义：无记录 = 模型未启用）。返回 true = 有 etcd 写入。
    bool TryBootstrapRingSlots();
    // §16.15.3 / §16.18.2 新成员确定性补位（幂等 CAS，仅模型启用时生效）：
    // 本机无归属且是「排序最早的未归属存活成员」时，按 rank 升序——
    //   ① primary 已死/为空的 rank：直接 CAS kStable 接管（冷启动，
    //      §16.18.3 待命者优先，数据依赖 segment 重建；同步清残留
    //      reshard_intent）；
    //   ② 仍被存活兼管者持有的空 rank（primary_id 重复出现在更小 rank，
    //      §16.8.1 隐含判定）：写 reshard_intent + CAS kMigrating 发起段级
    //      热迁移（§16.8.2），后续阶段由本机 reshard driver 断点续传；
    //   ③ 否则无存活 standby 的 rank：CAS 加入 standby_ids（1:1 配对，
    //      §16.18.1/§16.18.2，整体替换顺带清死亡 standby）。
    // 另含 rank 记录缺失的防御性补建（bootstrap 中途失败/人工误删）。
    // 返回 true = 有 etcd 写入（调用方需刷新缓存）。
    bool TryAssignRankForSelf(
        const std::vector<MasterRegistration>& members);
    // ---- TryAssignRankForSelf 的四阶段拆分（阶段方法模式，§16.18 各步
    // 语义独立、幂等可单独重试；外壳只做门控与调度）----
    // 阶段 0：缺 rank 记录防御性补建（缺失 rank 由确定性兼管者本机
    // kKeyNotExists 创建，天然幂等）。返回 true = 有补建。
    bool RepairMissingRankRecords(const RingSlotsView& view,
                                  uint32_t group_count);
    // 阶段 ①：接管 primary 已死/为空且无存活 standby 的 rank（冷启动）。
    // 三态结果（StepOutcome，见 cpp 匿名命名空间）。
    int ClaimDeadPrimaryRank(const RingSlotsView& view,
                             const std::vector<MasterRegistration>& members);
    // 阶段 ②：认领存活兼管者持有的空 rank——写 reshard_intent + CAS
    // kMigrating 发起热迁移，后续由本机 driver 续传。
    int ClaimCaretakerRank(const RingSlotsView& view,
                           const std::vector<MasterRegistration>& members);
    // 阶段 ③：以 standby 身份加入无存活 standby 的 rank（1:1 配对）。
    int JoinAsStandby(const RingSlotsView& view,
                      const std::vector<MasterRegistration>& members);
    // 补位阶段结果的变更触发日志（phase 索引 / outcome 三态见 cpp 匿名
    // 命名空间的 AssignPhase / StepOutcome；detail 为跳过统计或动作
    // 摘要）。结果与上次周期相同则静默（防刷屏，见 last_phase_outcome_
    // 注释）。
    void LogStepOutcome(int phase, int outcome, const std::string& detail);
    // §16.8.1 / §16.8.3 兼管自愈：primary 已死、无存活 standby、且无未归属
    // 成员（待命池空，补位路径无 taker）的 rank，由确定性指定的兼管者
    // （r>0：前一个有存活 primary 的 rank 的 primary；rank0 断链：最小有
    // 存活 primary 的 rank 的 primary）CAS 接管（冷启动语义）。所有节点从
    // 同一快照推导同一接管者，仅其本机执行。返回 true = 有 etcd 写入。
    bool TryDesignatedTakeover(
        const std::vector<MasterRegistration>& members);
    MasterRole ComputeDesiredRole();
    // 加载所有存活 master 并按 master_id 稳定排序（tie 一致）。失败返回 false。
    bool LoadRankedMembers(std::vector<MasterRegistration>& out);
    // 对比成员集相对上次是否变化，变化时记录一条 membership change 日志
    // （joined/left/当前 primaries）。仅 MastersWatchLoop 调用。
    void LogMembershipChange(const std::vector<MasterRegistration>& members);

    static void WatchCallback(void* ctx, const char* key, size_t key_size,
                              const char* value, size_t value_size,
                              int event_type, int64_t mod_revision);
};

}  // namespace cvm
}  // namespace mooncake