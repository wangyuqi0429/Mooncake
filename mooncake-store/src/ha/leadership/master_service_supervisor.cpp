#include "ha/leadership/master_service_supervisor.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "ha/leadership/leader_label_reconciler.h"
#include "ha/master_metrics_reporter.h"
#include "ha/standby_controller.h"
#include "k8s_lease_helper.h"
#include "master_admin_service.h"
#include "rpc_service.h"
#include "types.h"

#include "cvm/cvm_controller.h"
#include "cvm/cvm_service_delegate.h"
#include "cvm/etcd_view_store.h"
#include "etcd_helper.h"

namespace mooncake {
namespace ha {

namespace {

constexpr auto kLabelReconcileRetryInterval = std::chrono::seconds(1);
constexpr auto kSupervisorRetryInterval = std::chrono::seconds(1);
constexpr char kLeaderLabelKey[] = "mooncake.io/store-role";
constexpr char kLeaderLabelValue[] = "leader";

bool HasPodIdentity(const MasterServiceSupervisorConfig& config) {
    return !config.pod_name.empty() && !config.pod_namespace.empty() &&
           config.ha_backend_type == "k8s";
}

LeaderLabelReconciler MakeLeaderLabelReconciler(
    const MasterServiceSupervisorConfig& config) {
    return LeaderLabelReconciler(
        HasPodIdentity(config),
        [ns = config.pod_namespace, pod = config.pod_name](bool desired) {
            return desired ? K8sLeaseHelper::SetPodLabel(
                                 ns, pod, kLeaderLabelKey, kLeaderLabelValue)
                           : K8sLeaseHelper::ClearPodLabel(ns, pod,
                                                           kLeaderLabelKey);
        },
        kLabelReconcileRetryInterval);
}

std::string ResolveHABackendConnstring(
    const MasterServiceSupervisorConfig& config) {
    return ResolveConfiguredHABackendConnstring(config.ha_backend_type,
                                                config.ha_backend_connstring,
                                                config.etcd_endpoints);
}

tl::expected<HABackendSpec, ErrorCode> BuildHABackendSpec(
    const MasterServiceSupervisorConfig& config) {
    auto backend_type = ParseHABackendType(config.ha_backend_type);
    if (!backend_type.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto availability = ValidateHABackendAvailability(backend_type.value());
    if (availability != ErrorCode::OK) {
        return tl::make_unexpected(availability);
    }

    auto connstring = ResolveHABackendConnstring(config);
    if (connstring.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return HABackendSpec{
        .type = backend_type.value(),
        .connstring = connstring,
        .cluster_namespace = config.cluster_id,
        .master_view_lease_ttl_sec = config.master_view_lease_ttl_sec,
    };
}

bool IsFatalHABackendError(ErrorCode err) {
    return err == ErrorCode::INVALID_PARAMS ||
           err == ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

bool HandleSupervisorError(std::string_view action, ErrorCode err,
                           HABackendType backend_type) {
    if (IsFatalHABackendError(err)) {
        LOG(ERROR) << "Failed to " << action << ": " << toString(err)
                   << ", backend_type=" << HABackendTypeToString(backend_type);
        return true;
    }

    LOG(WARNING) << "Failed to " << action << ": " << toString(err)
                 << ", backend_type=" << HABackendTypeToString(backend_type)
                 << ", retrying in " << kSupervisorRetryInterval.count() << "s";
    std::this_thread::sleep_for(kSupervisorRetryInterval);
    return false;
}

void SetRuntimeState(MasterAdminServer& admin_server,
                     MasterRuntimeState state) {
    admin_server.SetRuntimeState(state);
    LOG(INFO) << "Master runtime state -> " << MasterRuntimeStateToString(state)
              << ", role=" << MasterRuntimeRoleToString(state);
}

void ActivateServingState(MasterAdminServer& admin_server,
                          const std::shared_ptr<WrappedMasterService>& service,
                          LeaderLabelReconciler& label_reconciler) {
    admin_server.SetServiceDelegate(service);
    admin_server.SetServiceAvailable(true);
    SetRuntimeState(admin_server, MasterRuntimeState::kServing);
    label_reconciler.SetLeader(true);
}

void DeactivateServingState(MasterAdminServer& admin_server,
                            LeaderLabelReconciler& label_reconciler) {
    admin_server.SetServiceAvailable(false);
    admin_server.SetServiceDelegate(nullptr);
    label_reconciler.SetLeader(false);
}

void UpdateObservedLeader(MasterAdminServer& admin_server,
                          StandbyController& standby_controller,
                          const std::optional<MasterView>& leader_view,
                          const MasterSources& sources) {
    admin_server.SetObservedLeader(leader_view);
    standby_controller.UpdateObservedLeader(sources);
}

// Forward declarations: definitions live below the CVM membership bridge.
std::optional<MasterView> BuildCvmObservedLeader(
    const std::unique_ptr<cvm::CvmController>& cvm_controller);
MasterSources BuildCvmObservedSources(
    const std::unique_ptr<cvm::CvmController>& cvm_controller);

void EnterStandbyMode(
    MasterAdminServer& admin_server,
    StandbyController& standby_controller,
    std::atomic<bool>& accept_runtime_updates,
    const std::unique_ptr<cvm::CvmController>& cvm_controller) {
    accept_runtime_updates.store(true, std::memory_order_release);
    const MasterSources sources = BuildCvmObservedSources(cvm_controller);
    UpdateObservedLeader(admin_server, standby_controller,
                         BuildCvmObservedLeader(cvm_controller), sources);

    auto err = standby_controller.StartStandby(sources);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to start standby replication: "
                     << toString(err);
        SetRuntimeState(admin_server, MasterRuntimeState::kStandby);
        return;
    }

    SetRuntimeState(admin_server, standby_controller.GetStandbyRuntimeState());
}

// Bridges CvmController's membership role decisions to the supervisor's
// serving/standby state machine. CvmController::MembershipLoop runs on its own
// thread and calls OnRoleChanged; this class persists the role back to etcd
// (so slot partitioning immediately excludes demoted nodes) and publishes the
// change to the supervisor main loop. It never mutates the supervisor state
// machine directly: on demotion it only invokes a lightweight stop-server
// signal installed by the serve phase, and the main loop performs the full
// downgrade sequence serially after the server unblocks.
class CvmMembershipCoordinator : public cvm::CvmServiceDelegate {
   public:
    CvmMembershipCoordinator(std::string cluster_namespace,
                             std::string master_id)
        : cluster_namespace_(std::move(cluster_namespace)),
          master_id_(std::move(master_id)) {}

    void SetLeaseId(EtcdLeaseId lease_id) {
        lease_id_.store(lease_id, std::memory_order_relaxed);
    }

    // Installed by the serve phase; cleared (empty) when not serving. The
    // signal only stops the coro_rpc server so the serve phase unblocks; the
    // main loop then reads CvmController::GetCurrentRole() and performs the
    // full downgrade sequence.
    void SetServeStopSignal(std::function<void()> signal) {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_serve_signal_ = std::move(signal);
    }

    // Blocks until either a role change or a membership (member set) change is
    // pending, then clears both so each change is consumed exactly once. The
    // caller re-reads CvmController::GetCurrentRole() as the source of truth
    // after waking; a standby uses the wake-up to re-bind its replay sources.
    void WaitForRoleOrMembershipChange() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] {
            return has_pending_role_ || has_pending_membership_;
        });
        has_pending_role_ = false;
        has_pending_membership_ = false;
    }

    void OnSlotAcquired(uint16_t /*slot*/) override {}
    void OnSlotReleased(uint16_t /*slot*/) override {}

    void OnMembershipChanged() override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            has_pending_membership_ = true;
        }
        cv_.notify_all();
    }

    void OnRoleChanged(cvm::MasterRole new_role) override {
        const EtcdLeaseId lease_id =
            lease_id_.load(std::memory_order_relaxed);
        ErrorCode err = cvm::EtcdViewStore::UpdateMasterRole(
            cluster_namespace_, master_id_, new_role, lease_id);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "CvmMembershipCoordinator: failed to persist role="
                         << static_cast<int32_t>(new_role)
                         << " for master_id=" << master_id_ << ": " << err;
        }
        LOG(INFO) << "CvmMembershipCoordinator::OnRoleChanged: master_id="
                  << master_id_ << ", role=" << static_cast<int32_t>(new_role)
                  << ", lease_id=" << lease_id;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            has_pending_role_ = true;
        }
        cv_.notify_all();

        if (new_role == cvm::MasterRole::kStandby) {
            std::function<void()> signal;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                signal = stop_serve_signal_;
            }
            if (signal) {
                signal();
            }
        }
    }

   private:
    std::string cluster_namespace_;
    std::string master_id_;
    std::atomic<EtcdLeaseId> lease_id_{0};
    std::mutex mutex_;
    std::condition_variable cv_;
    bool has_pending_role_{false};
    bool has_pending_membership_{false};
    std::function<void()> stop_serve_signal_;
};

// Builds the representative single-leader view for the admin surface. In the
// multi-submaster model there is no single "leader"; this reports the earliest
// primary for display only. Returns nullopt when no primary is available yet
// (or this node is the first primary itself, so there is no upstream).
std::optional<MasterView> BuildCvmObservedLeader(
    const std::unique_ptr<cvm::CvmController>& cvm_controller) {
    if (!cvm_controller) {
        return std::nullopt;
    }
    const std::string primary = cvm_controller->GetPrimaryAddress();
    if (primary.empty()) {
        return std::nullopt;
    }
    MasterView view;
    view.leader_address = primary;
    view.view_version = 0;  // 无持久化 kv_view；版本不再用于路由。
    return view;
}

// Builds the standby replay source set from the CVM membership ranking. Under
// dynamic binding (2c) a standby only follows the primary(s) that own the slot
// range it is responsible for, instead of following every primary (2b).
MasterSources BuildCvmObservedSources(
    const std::unique_ptr<cvm::CvmController>& cvm_controller) {
    MasterSources sources;
    if (!cvm_controller) {
        return sources;
    }
    for (const auto& member : cvm_controller->GetBindingSources()) {
        sources.push_back(MasterSource{member.master_id, member.address});
    }
    return sources;
}

// ── 环互备双角色回放运行时（互备 1.3.2）──
// serve phase 内本机在 serving 自己 rank 的同时，低频拉取（5s）
// CvmController::GetRingStandbySources（互备 1.3.1）跟随被指派 rank 的
// OpLog 流。与待命池独占模式的 standby_controller 完全独立：
//   - 生命周期不相交：standby 模式用待命池控制器（EnterStandbyMode），
//     serve phase 用本运行时；职责集不相交：phase3 只配未归属成员，
//     JoinAsStandbyRing（互备 1.1）只配已归属成员。
//   - 职责集空 → StopStandby；非空未跟随 → StartStandby；集合变化 →
//     再次 StartStandby（具体实现为动态重绑 UpdateSources：幂等，未运行
//     等价 Start、源集合不变 no-op、变化 Stop+Start）；未变 → 静默
//    （5s 轮询无逐周期日志，防刷屏）。
//   - 起停/重绑仅变更时各一条 INFO；StartStandby 失败 60s 节流 WARNING。
//   - 开关源：CvmController::StandbyMutualPairingEnabled（与互备 1.1
//     JoinAsStandbyRing 门控共用同一进程级缓存值，杜绝配对记录与回放
//     运行时失配）。OFF 时 Start 不启动线程，行为与现状等价。
//   - 线程模型：自持线程串行调用 GetRingStandbySources / StartStandby /
//     StopStandby；主线程仅在 Start/Stop 生命周期边界介入（Stop 先
//     join 线程再停跟随，天然串行）。
//   - 互备 1.4（staging 领养编排）：同 Loop 每 poll 增量执行
//     OrchestrateRingAdoptions——检测 ring-adopt staging rank
//    （FindStagedRingAdoptions）→ 经 wrapped service 物化死 primary 的
//     OpLog 流（AdoptMaterializeFromDeadOwner，P4 过滤幂等）→ CAS
//     kMigrating→kStable 收尾（FinalizeRingAdoption）。稳态 staging 集
//     为空零动作；1.4 全链路（配对→领养→物化→收尾）就绪前开关仍保持
//     OFF。
class RingStandbyRuntime {
   public:
    RingStandbyRuntime(const HABackendSpec& spec,
                       const MasterServiceSupervisorConfig& config,
                       cvm::CvmController* cvm_controller)
        : spec_(spec), config_(config), cvm_controller_(cvm_controller) {}

    ~RingStandbyRuntime() { Stop(); }

    // 单线程生命周期契约：Start/Stop/析构均由 supervisor 主循环同一线程
    // 串行调用（serve 就绪 → Start，降级序列 → Stop，RunSupervisorLoop
    // 退出 → 析构）。mutex_ 仅防御重入/竞态下的状态位一致性，并非为
    // 并发 Start/Stop 设计——thread_ 的赋值与 join 因此无需在锁内。
    // Loop 线程从不在运行期触碰 mutex_（只读 stop_ 原子），故无锁序死锁。
    void Start() {
        if (cvm_controller_ == nullptr ||
            !cvm::CvmController::StandbyMutualPairingEnabled()) {
            return;  // 无 CVM 成员身份 / 开关 OFF：no-op（与现状等价）
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (running_) {
                return;
            }
            running_ = true;
            stop_ = false;
        }
        // 线程创建失败（资源耗尽等）：复位 running_ 防状态失真——否则
        // 后续 Start 静默 no-op、Stop 因 thread_ 非 joinable 跳过 join，
        // running_ 永久卡 true。当前调用方对返回值不敏感（下个 serve
        // 周期重试），故仅吞掉异常并告警。
        try {
            thread_ = std::thread(&RingStandbyRuntime::Loop, this);
        } catch (const std::system_error& e) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                running_ = false;
            }
            LOG(WARNING) << "RingStandbyRuntime: thread creation failed: "
                         << e.what() << " (ring following disabled this cycle)";
        }
    }

    // 幂等；join 线程后统一停跟随（保证控制器调用串行）。
    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_) {
                return;
            }
            running_ = false;
            stop_ = true;
        }
        if (thread_.joinable()) {
            thread_.join();
        }
        if (standby_controller_ && following_.load()) {
            standby_controller_->StopStandby();
            following_.store(false);
            LOG(INFO) << "RingStandbyRuntime: stopped ring following on "
                         "serve exit";
        }
    }

    // 互备 1.4：注入 serve phase 的 wrapped service（物化编排经其安全
    // API AdoptMaterializeFromDeadOwner 进入 MasterService）。单线程生命
    // 周期契约同 Start/Stop：每 serve phase 在 Start 前注入新实例、Stop
    //（已 join Loop 线程）后清空，Loop 线程运行期指针恒有效。
    void SetWrappedMasterService(WrappedMasterService* service) {
        wrapped_service_ = service;
    }

   private:
    // 源集合指纹（master_id@address，排序后逐元素比较）：排序消除 rank
    // 扫描序影响；address 入指纹覆盖同身份换地址的动态重绑。
    static std::vector<std::pair<std::string, std::string>> SignatureOf(
        const MasterSources& sources) {
        std::vector<std::pair<std::string, std::string>> sig;
        sig.reserve(sources.size());
        for (const auto& s : sources) {
            sig.emplace_back(s.master_id, s.address);
        }
        std::sort(sig.begin(), sig.end());
        return sig;
    }

    static std::string JoinSignature(
        const std::vector<std::pair<std::string, std::string>>& sig) {
        std::string joined;
        for (const auto& [id, addr] : sig) {
            if (!joined.empty()) {
                joined += ",";
            }
            joined += id + "@" + addr;
        }
        return joined;
    }

    void Loop() {
        std::vector<std::pair<std::string, std::string>> last_signature;
        // 同步健康观测（1.3.4）：kRecovering 覆盖 FAILED/RECONNECTING/
        // RECOVERING/SYNCING。连续 2 个 poll（10s）仍劣化才告警（排除
        // 重绑后的正常追赶），再 60s 节流；仅 Loop 线程访问。
        int unhealthy_streak = 0;
        int64_t last_health_warn_ms = 0;
        while (!stop_.load(std::memory_order_relaxed)) {
            const auto duties = cvm_controller_->GetRingStandbySources();
            MasterSources sources;
            sources.reserve(duties.size());
            for (const auto& duty : duties) {
                sources.push_back(MasterSource{duty.master_id, duty.address});
            }
            const auto signature = SignatureOf(sources);
            const bool changed = signature != last_signature;
            const bool following = following_.load();
            if (sources.empty()) {
                if (following) {
                    // 职责集空（重指派/晋升完成/降配）：停跟随。controller
                    // 必已创建（following 为真）。
                    standby_controller_->StopStandby();
                    following_.store(false);
                    LOG(INFO) << "RingStandbyRuntime: ring duty set empty, "
                                 "stopped following";
                }
                unhealthy_streak = 0;
            } else {
                // 懒创建：首个非空职责集到达才建控制器（etcd 连接等）。
                if (!standby_controller_) {
                    standby_controller_ = CreateStandbyController(spec_,
                                                                  config_);
                }
                // 跟随期每 poll 调和 StartStandby（1.3.3 核验发现的缺陷
                // 修复）：服务转入 FAILED/STOPPED 时同步回调会把控制器
                // 内部 standby_running_ 静默置 false（本运行时的
                // following_ 无从得知），职责集不变则永远不会再触发
                // StartStandby → 跟随卡死（池模式有成员变动唤醒
                // EnterStandbyMode 兜底，环运行时无此兜底）。每 poll
                // 无条件调用：源未变且运行中 → 内部 UpdateSources 幂等
                // no-op（零开销）；已停 → 走全量 Start 分支自愈。
                const ErrorCode err =
                    standby_controller_->StartStandby(sources);
                if (err == ErrorCode::OK) {
                    const bool started_transition = !following || changed;
                    following_.store(true);
                    if (started_transition) {
                        LOG(INFO) << "RingStandbyRuntime: following "
                                  << sources.size() << " ring source(s) ["
                                  << JoinSignature(signature) << "]";
                    }
                    // 健康检查（1.3.4）：GetStandbyRuntimeState 读控制器
                    // 最后同步状态（锁保护，开销一次锁获取）。
                    const MasterRuntimeState state =
                        standby_controller_->GetStandbyRuntimeState();
                    if (state == MasterRuntimeState::kRecovering) {
                        ++unhealthy_streak;
                        const int64_t now_ms = std::chrono::duration_cast<
                            std::chrono::milliseconds>(
                            std::chrono::steady_clock::now()
                                .time_since_epoch())
                            .count();
                        if (unhealthy_streak >= 2 &&
                            now_ms - last_health_warn_ms >= 60000) {
                            last_health_warn_ms = now_ms;
                            LOG(WARNING) << "RingStandbyRuntime: sync "
                                            "degraded (state="
                                         << static_cast<int>(state)
                                         << ", sources="
                                         << JoinSignature(signature) << ")";
                        }
                    } else {
                        unhealthy_streak = 0;
                    }
                } else {
                    // 失败按周期静默重试；60s 节流一条 WARNING。
                    unhealthy_streak = 0;
                    const int64_t now_ms =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now()
                                .time_since_epoch())
                            .count();
                    if (now_ms - last_warn_ms_ >= 60000) {
                        last_warn_ms_ = now_ms;
                        LOG(WARNING) << "RingStandbyRuntime: StartStandby "
                                        "failed, err="
                                     << toString(err)
                                     << " (retrying each poll)";
                    }
                }
            }
            last_signature = signature;
            // 互备 1.4：领养编排（staging 检测 → 物化 → 收尾），稳态零动作。
            OrchestrateRingAdoptions();
            // 5s 低频拉取；100ms 粒度可中断睡眠保证降级时及时退出。
            for (int i = 0; i < 50 && !stop_.load(std::memory_order_relaxed);
                 ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }

    // 互备 1.4：环互备领养编排（staging-first ②③）。检测 ring-adopt
    // staging rank（FindStagedRingAdoptions，签名 = primary=本机且
    // kMigrating 且 migrating_to 为空，与 reshard 阶段 1 签名不相交）
    // → 物化（WrappedMasterService::AdoptMaterializeFromDeadOwner：重放
    // 死 primary 的 OpLog 流，P4 owned-slot 过滤只落本机 slot，已定局
    // 幂等短路）→ CAS kMigrating→kStable 收尾（FinalizeRingAdoption），
    // 写屏障解除后领养 slot 随下轮心跳 ImportSlotMetadata→MarkSlotsReady
    // 自然就绪。三态决策（详见 AdoptMaterializeFromDeadOwner 契约）：
    //   OK                → 收尾（INFO 一条）；
    //   INCOMPLETE（预算耗尽，瞬态）→ 保持屏障下轮重试（60s 节流告警）；
    //   其余 FATAL/UNAVAILABLE（越窗 / prefix 缺失 / OpLog 未启用）→
    //     按既有冷接管回退语义（判空、客户端冷重建）收尾 + WARNING。
    // dead_primary_id 为空（领养者在 staging→收尾窗口内重启，进程内源
    // 记录丢失）：保持写屏障 fail-safe（宁可冻结，不可空元数据 serve），
    // 60s 节流 ERROR 供人工处置（admin reshard 重建 / 手工 CAS）。
    void OrchestrateRingAdoptions() {
        if (cvm_controller_ == nullptr) {
            return;
        }
        const auto staged = cvm_controller_->FindStagedRingAdoptions();
        if (staged.empty()) {
            return;  // 稳态：零动作（视图快照为锁内拷贝，无 etcd 读）
        }
        const int64_t now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
        for (const auto& adoption : staged) {
            if (adoption.dead_primary_id.empty()) {
                if (now_ms - last_adoption_warn_ms_ >= 60000) {
                    last_adoption_warn_ms_ = now_ms;
                    LOG(ERROR) << "RingStandbyRuntime: staged ring adoption "
                                  "has no recorded dead primary (adopter "
                                  "restarted in staging window?): rank="
                               << adoption.rank
                               << ", writes fenced until manual resolution";
                }
                continue;
            }
            if (wrapped_service_ == nullptr) {
                continue;  // 非 serve 窗口（防御；Loop 生命周期与 serve
                           // phase 同步，正常不可达）
            }
            bool stream_present = false;
            const ErrorCode err =
                wrapped_service_->AdoptMaterializeFromDeadOwner(
                    adoption.dead_primary_id, stream_present);
            if (err == ErrorCode::INCOMPLETE_OPLOG_CATCH_UP) {
                // 瞬态（30s 重放预算耗尽，etcd 抖动）：保持屏障下轮重试。
                if (now_ms - last_adoption_warn_ms_ >= 60000) {
                    last_adoption_warn_ms_ = now_ms;
                    LOG(WARNING) << "RingStandbyRuntime: adoption replay "
                                    "budget exhausted (retrying): rank="
                                 << adoption.rank << ", source="
                                 << adoption.dead_primary_id;
                }
                continue;
            }
            if (err != ErrorCode::OK) {
                LOG(WARNING) << "RingStandbyRuntime: adoption replay "
                                "unrecoverable, finalizing with cold-rebuild "
                                "fallback: rank="
                             << adoption.rank
                             << ", source=" << adoption.dead_primary_id
                             << ", err=" << toString(err);
            }
            const ErrorCode fin = cvm_controller_->FinalizeRingAdoption(
                adoption.rank, adoption.epoch, adoption.dead_primary_id);
            if (fin == ErrorCode::OK) {
                LOG(INFO) << "RingStandbyRuntime: ring adoption complete: "
                             "rank="
                          << adoption.rank
                          << ", dead_primary=" << adoption.dead_primary_id
                          << ", stream_present="
                          << (stream_present ? "true" : "false");
            } else if (fin != ErrorCode::STALE_ROUTE) {
                // STALE = 并发推进（他方收尾 / 管理员操作），下轮检测自然
                // 离开 staging 集，静默；其余失败保持屏障下轮重试（物化
                // 已定局，重试仅重走幂等短路 + CAS）。
                LOG(WARNING) << "RingStandbyRuntime: adoption finalize "
                                "failed (retrying): rank="
                             << adoption.rank << ", err=" << toString(fin);
            }
        }
    }

    // supervisor 局部变量的引用/裸指针：声明序保证先于本对象析构。
    const HABackendSpec& spec_;
    const MasterServiceSupervisorConfig& config_;
    cvm::CvmController* cvm_controller_;
    // serve phase 注入（SetWrappedMasterService）：物化编排入口，Loop
    // 线程只读。生命周期见 setter 注释。
    WrappedMasterService* wrapped_service_ = nullptr;
    // 懒创建的独立控制器实例（与待命池 standby_controller 无共享）。
    std::unique_ptr<StandbyController> standby_controller_;
    std::thread thread_;
    std::mutex mutex_;  // 仅保护 Start/Stop 生命周期边界
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_{false};
    std::atomic<bool> following_{false};
    int64_t last_warn_ms_ = 0;  // 仅 Loop 线程访问
    int64_t last_adoption_warn_ms_ = 0;  // 仅 Loop 线程访问（1.4 领养告警节流）
};

int RunSupervisorLoop(const HABackendSpec& spec,
                      const MasterServiceSupervisorConfig& config,
                      MasterAdminServer& admin_server) {
    // ── Metrics reporter (writes master capacity/usage to HA backend) ──
    MasterMetricsReporter::Config reporter_config;
    reporter_config.enabled = config.enable_metrics_report_to_backend;
    reporter_config.report_interval_sec = config.metrics_report_interval_sec;
    reporter_config.lease_ttl_sec = config.metrics_report_lease_ttl_sec;
    reporter_config.master_id = UuidToString(generate_uuid());
    reporter_config.local_hostname = config.local_hostname;
    reporter_config.cluster_namespace = config.cluster_id;
    reporter_config.ha_backend_connstring =
        ResolveHABackendConnstring(config);
    MasterMetricsReporter metrics_reporter(reporter_config);

    auto label_reconciler = MakeLeaderLabelReconciler(config);
    label_reconciler.SetLeader(false);
    SetRuntimeState(admin_server, MasterRuntimeState::kStarting);
    auto standby_controller = CreateStandbyController(spec, config);
    std::atomic<bool> accept_standby_runtime_updates{false};
    standby_controller->SetStandbyRuntimeStateCallback(
        [&](MasterRuntimeState state) {
            if (!accept_standby_runtime_updates.load(
                    std::memory_order_acquire)) {
                return;
            }
            SetRuntimeState(admin_server, state);
        });

    // ── CVM membership (quota coordination) ──
    // The CvmController owns the etcd lease, master registration, view
    // snapshot aggregation and the membership loop that decides this node's
    // role (primary vs standby) via first-come-first-served ranking. It lives
    // here (supervisor layer) so membership keeps running whether or not this
    // node is currently serving; MasterService only publishes slot ownership.
    // NOTE: the coordinator is declared before the controller so it is
    // destroyed after it — the controller's Stop() joins the membership thread,
    // guaranteeing no OnRoleChanged callback is in flight when the coordinator
    // is torn down.
    CvmMembershipCoordinator cvm_membership_coordinator(config.cluster_id,
                                                        config.local_hostname);
    std::unique_ptr<cvm::CvmController> cvm_controller;
    if (spec.type == HABackendType::ETCD && !config.local_hostname.empty() &&
        !config.cluster_id.empty()) {
        ErrorCode connect_err = EtcdHelper::ConnectToEtcdStoreClient(
            ResolveHABackendConnstring(config));
        if (connect_err != ErrorCode::OK) {
            LOG(WARNING) << "CVM membership disabled: failed to connect etcd: "
                         << connect_err;
        } else {
            cvm::CvmController::Config cc_config;
            cc_config.cluster_namespace = config.cluster_id;
            cc_config.master_id = config.local_hostname;
            cc_config.address = config.local_hostname;
            // Start as standby; the membership loop promotes to primary when
            // this node ranks within the submaster quota.
            cc_config.role = cvm::MasterRole::kStandby;
            cc_config.http_port = config.cvm_http_port;
            cc_config.http_host = config.cvm_http_host;
            cc_config.submaster_count = config.submaster_count;
            cvm_controller =
                std::make_unique<cvm::CvmController>(std::move(cc_config));
            cvm_controller->SetDelegate(&cvm_membership_coordinator);
            ErrorCode cc_err = cvm_controller->Start();
            if (cc_err != ErrorCode::OK) {
                LOG(WARNING) << "Failed to start CvmController: " << cc_err;
                cvm_controller.reset();
            } else {
                cvm_membership_coordinator.SetLeaseId(
                    cvm_controller->GetLeaseId());
                LOG(INFO) << "Started supervisor-owned CvmController: master_id="
                         << config.local_hostname
                          << ", cluster_namespace=" << config.cluster_id
                          << ", lease_id=" << cvm_controller->GetLeaseId()
                          << ", submaster_count=" << config.submaster_count;
            }
        }
    }

    // 环互备双角色回放运行时（互备 1.3.2）：serve phase 内跟随被指派
    // rank 的流。声明序在 cvm_controller 之后 → 析构先于它（裸指针
    // 安全）；开关 OFF / 无 CVM 时 Start/Stop 均为 no-op。
    RingStandbyRuntime ring_standby_runtime(spec, config,
                                            cvm_controller.get());

    // standby 覆盖率 provider 的共享生命周期状态：metric 线程（拷贝的
    // provider 闭包）与退出路径（guard 析构）经互斥互达——provider 要么
    // 先于 guard 完成 live etcd 调用，要么看到 alive=false 直接返回
    // unavailable，消除 cvm_controller 悬垂窗口（admin_server 生命周期
    // 长于本函数作用域，RunSupervisorLoop 不调用其 Stop）。
    struct StandbyCoverageSharedState {
        std::mutex mutex;
        bool alive = false;
        cvm::CvmController* controller = nullptr;
    };
    std::shared_ptr<StandbyCoverageSharedState> coverage_state;
    if (cvm_controller) {
        coverage_state = std::make_shared<StandbyCoverageSharedState>();
        coverage_state->controller = cvm_controller.get();
        coverage_state->alive = true;
        const auto state_ptr = coverage_state;
        // P2-7-C（§16.18.2 满配缺口可观测化）：admin metrics 线程按
        // kMetricReportIntervalSeconds（10s）低频拉取，不走心跳路径。
        admin_server.SetStandbyCoverageProvider([state_ptr]() -> std::string {
            std::lock_guard<std::mutex> lock(state_ptr->mutex);
            if (!state_ptr->alive || !state_ptr->controller) {
                return "unavailable";
            }
            return state_ptr->controller->GetStandbyCoverageSummary();
        });
    }
    // RAII guard 声明于 cvm_controller 与 coverage_state 之后 → 析构先于
    // 二者：任意 return 路径都先摘 provider、置 alive=false，再销毁
    // controller，杜绝 metric 线程在 controller 析构期间/之后解引用。
    struct StandbyCoverageProviderGuard {
        MasterAdminServer& admin;
        std::shared_ptr<StandbyCoverageSharedState> state;
        ~StandbyCoverageProviderGuard() {
            if (state) {
                {
                    std::lock_guard<std::mutex> lock(state->mutex);
                    state->alive = false;
                    state->controller = nullptr;
                }
                admin.SetStandbyCoverageProvider(nullptr);
            }
        }
    } standby_coverage_guard{admin_server, coverage_state};

    EnterStandbyMode(admin_server, *standby_controller,
                     accept_standby_runtime_updates,
                     cvm_controller);

    while (true) {
        // CVM membership decides this node's role via first-come-first-served
        // ranking against the submaster quota. Without a CvmController
        // (non-etcd backend or missing identity) there is no quota
        // coordination, so fall back to unconditional primary serving.
        const cvm::MasterRole target_role =
            cvm_controller ? cvm_controller->GetCurrentRole()
                           : cvm::MasterRole::kPrimary;

        if (target_role == cvm::MasterRole::kPrimary) {
            // ── Upgrade sequence (kStandby → kPrimary) ──
            // first primary（无上游）无需 final catch-up；有上游 primary 时才
            // 从 standby 导出回放数据。PromoteStandbyAndExport 内部完成 final
            // catch-up + 导出 PromotionContext，新 primary 从它恢复。
            accept_standby_runtime_updates.store(false,
                                                 std::memory_order_release);
            const bool has_upstream =
                cvm_controller && !cvm_controller->GetPrimaryAddress().empty();
            PromotionContext promotion_ctx{};
            if (has_upstream) {
                auto ctx = standby_controller->PromoteStandbyAndExport();
                if (!ctx) {
                    EnterStandbyMode(admin_server, *standby_controller,
                                     accept_standby_runtime_updates,
                                     cvm_controller);
                    if (HandleSupervisorError("promote standby for serve",
                                              ctx.error(), spec.type)) {
                        return -1;
                    }
                    continue;
                }
                promotion_ctx = std::move(*ctx);
            }

            LOG(INFO) << "Starting serve phase (CVM role=kPrimary)...";
            coro_rpc::coro_rpc_server server(
                config.rpc_thread_num, config.rpc_port, config.rpc_address,
                config.rpc_conn_timeout, config.rpc_enable_tcp_no_delay);
            const char* protocol = std::getenv("MC_RPC_PROTOCOL");
            if (protocol && std::string_view(protocol) == "rdma") {
                server.init_ibv();
            }

            const ViewVersionId view_version = 0;  // 无持久化 kv_view 版本。
            mooncake::WrappedMasterServiceConfig wrapped_config(config,
                                                                 view_version);
            // In HA serving-primary mode, snapshot bootstrap belongs to
            // standby. The new primary must restore from PromotionContext only.
            wrapped_config.enable_snapshot_restore = false;
            auto wrapped_master_service =
                std::make_shared<WrappedMasterService>(
                    wrapped_config, config.http_metadata_server,
                    config.http_metadata_remote_url);

            // Inject the supervisor-owned CvmController lease so the
            // slot/segment ownership records share this master's registration
            // lifecycle.
            if (cvm_controller) {
                wrapped_master_service->SetCvmLeaseId(
                    cvm_controller->GetLeaseId());
                // ring_slots 缓存读取（§16.15.5）：slot 归属解析走缓存优先
                //（miss 回退环推导），supervisor 与 service 同进程直连。
                wrapped_master_service->SetCvmController(cvm_controller.get());
            }

            // vsegment auto-plan 发布侧重试策略（--vsegment_auto_plan flags
            // 经 MasterServiceSupervisorConfig 传入）：serve phase 前注入一次，
            // 供心跳/mount 触发点在配额快照缺失期间节流重试发布（修复
            // submaster 先于 client 启动时一次性 bootstrap 永久失败的死锁）。
            if (config.vsegment_auto_plan &&
                config.vsegment_auto_plan_member_count != 0) {
                vsegment::VSegmentUserPolicy policy;
                policy.member_count = config.vsegment_auto_plan_member_count;
                policy.stripe_size = config.vsegment_auto_plan_stripe_size;
                policy.member_extent_size =
                    config.vsegment_auto_plan_member_extent_size;
                policy.reserved_ratio =
                    config.vsegment_auto_plan_reserved_ratio;
                wrapped_master_service->SetVSegmentAutoPlanPolicy(policy);
            }

            // Restore from standby if we have context.
            if (promotion_ctx.applied_seq_id > 0 ||
                !promotion_ctx.objects.empty() ||
                !promotion_ctx.segments.empty() ||
                !promotion_ctx.vsegment_partitions.empty()) {
                wrapped_master_service->RestoreFromStandby(
                    promotion_ctx.objects, promotion_ctx.applied_seq_id,
                    promotion_ctx.segments,
                    promotion_ctx.vsegment_partitions);
            } else {
                // 缺陷3修复（同身份重启失忆自愈）：无上游 PromotionContext
                //（首启 primary 或重启后 rank 仍指向本机的快重启场景）时，
                // 从本机 OpLog 流自恢复——否则空内存 serve 会导致该 master
                // 全部 owned slots 的对象元数据静默丢失（etcd 视图正常、
                // 路由无变化，客户端连 STALE_ROUTE 重试都不会触发）。
                // 本 else 分支是 serve 前的一次性路径（RegisterRpcService
                // 之前，每进程至多触发一次；进程内幂等由 callee 的
                // oplog_replay_finalized_sources_ 定局登记兜底）。
                // 返回 false 覆盖两种情形：真·新集群空流（正常空态）与
                // 重放失败（瞬态已有界重试、耗尽后 ERROR 告警，均在 callee
                // 内完成，此处无需重复打点）。
                const bool self_recovered =
                    wrapped_master_service->TrySelfRecoverFromOpLog();
                if (self_recovered) {
                    LOG(INFO) << "Supervisor: self OpLog recovery restored "
                                 "state before serve";
                }
            }

            mooncake::RegisterRpcService(server, *wrapped_master_service);

            async_simple::Future<coro_rpc::err_code> ec = server.async_start();
            if (ec.hasResult()) {
                const coro_rpc::err_code& start_err = ec.result().value();
                LOG(ERROR) << "Failed to start master service: code="
                           << start_err.val() << ", reason="
                           << std::string(start_err.message());
                DeactivateServingState(admin_server, label_reconciler);
                EnterStandbyMode(admin_server, *standby_controller,
                                 accept_standby_runtime_updates,
                                 cvm_controller);
                return -1;
            }

            // Lightweight demotion signal: only stops the coro_rpc server so
            // the blocking get() below unblocks; the main loop then performs
            // the full downgrade sequence serially.
            cvm_membership_coordinator.SetServeStopSignal([&server]() {
                LOG(INFO) << "CVM quota demotion: stopping server";
                server.stop();
            });

            // A demotion may have been decided while the server was starting
            // up (before the stop signal was installed). If our role is no
            // longer primary, stop the server now so the get() below returns
            // promptly instead of blocking forever.
            const bool demoted_during_startup =
                cvm_controller &&
                cvm_controller->GetCurrentRole() == cvm::MasterRole::kStandby;

            if (!demoted_during_startup) {
                ActivateServingState(admin_server, wrapped_master_service,
                                     label_reconciler);
                metrics_reporter.SetRole("primary");
                metrics_reporter.Start();
                if (wrapped_master_service->StartSlotOwnerHeartbeat() !=
                    ErrorCode::OK) {
                    LOG(WARNING) << "Failed to start SlotOwnerHeartbeat during "
                                    "serve phase";
                }
                // Inter-master RPC channel: etcd-driven peer discovery +
                // cached peer connections for the CVM multi-submaster
                // coordination (handshake now, allocation forwarding later).
                if (wrapped_master_service->StartInterMasterRpc() !=
                    ErrorCode::OK) {
                    LOG(WARNING) << "Failed to start InterMasterRpcClient "
                                    "during serve phase";
                }
                // 环互备双角色回放（互备 1.3.2）：serve 就绪后开始跟随
                // 被指派的 rank 流（开关 OFF / 无 CVM 时内部 no-op）。
                // 互备 1.4：注入当前 serve 实例供领养编排物化调用
                // （Start 前注入；Stop 后在降级序列清空）。
                ring_standby_runtime.SetWrappedMasterService(
                    wrapped_master_service.get());
                ring_standby_runtime.Start();
            } else {
                LOG(INFO) << "Demoted during serve startup; stopping server";
                server.stop();
            }

            auto server_err = std::move(ec).get();
            LOG(INFO) << "Master service stopped: " << server_err;

            // ── Downgrade sequence (kPrimary → kStandby) ──
            // 先停环跟随（join 线程 + StopStandby），保证降级到待命池
            // 模式时同一时刻只有一条跟随路径活跃。
            ring_standby_runtime.Stop();
            // 互备 1.4：环跟随已停（Loop 线程 join 完成），清空物化入口，
            // 消除对已析构 serve 实例的悬挂引用。
            ring_standby_runtime.SetWrappedMasterService(nullptr);
            metrics_reporter.Stop();
            metrics_reporter.SetRole("standby");
            wrapped_master_service->StopSlotOwnerHeartbeat();
            wrapped_master_service->StopInterMasterRpc();
            cvm_membership_coordinator.SetServeStopSignal(nullptr);
            DeactivateServingState(admin_server, label_reconciler);
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates,
                             cvm_controller);
        } else {
            // ── Standby (kStandby): keep replicating, wait for promotion ──
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates,
                             cvm_controller);
            if (cvm_controller) {
                cvm_membership_coordinator.WaitForRoleOrMembershipChange();
            } else {
                // Unreachable: target_role is kPrimary when cvm_controller is
                // null. Sleep defensively to avoid a tight loop.
                std::this_thread::sleep_for(kSupervisorRetryInterval);
            }
        }
    }

    return 0;
}

}  // namespace

MasterServiceSupervisor::MasterServiceSupervisor(
    const MasterServiceSupervisorConfig& config)
    : config_(config) {}

int MasterServiceSupervisor::Start() {
    auto spec = BuildHABackendSpec(config_);
    if (!spec) {
        LOG(ERROR) << "Failed to parse HA backend config: "
                   << toString(spec.error())
                   << ", backend_type=" << config_.ha_backend_type;
        return -1;
    }

    mooncake::MasterAdminServer admin_server(
        static_cast<uint16_t>(config_.metrics_port),
        config_.enable_metric_reporting,
        config_.admin_metrics_log_interval_sec);
    if (!admin_server.Start()) {
        LOG(ERROR) << "Failed to start master admin server, metrics_port="
                   << config_.metrics_port;
        return -1;
    }
    return RunSupervisorLoop(*spec, config_, admin_server);
}

}  // namespace ha
}  // namespace mooncake
