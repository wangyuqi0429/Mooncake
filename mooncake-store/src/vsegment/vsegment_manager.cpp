#include "vsegment/vsegment_manager.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <optional>
#include <set>
#include <sstream>

#include "master_perf.h"
#include "vsegment/vsegment_metrics.h"
#include "vsegment/vsegment_oplog_mode.h"

namespace mooncake::vsegment {
namespace {

VSegmentAllocationResult ManagerError(ErrorCode error, std::string detail) {
    VSegmentAllocationResult result;
    result.error = error;
    result.detail = std::move(detail);
    return result;
}

int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

uint64_t LogicalCapacity(const VSegmentProfile& profile) {
    return profile.member_extent_size * profile.member_count;
}

// 自动推导"能接住一次写"的判据 L 时，把观测到的最大写长度按时间衰减，
// 避免一次性的大对象永久抬高水位（水位偏高只会多建卷，不会写错，但会浪费配额）。
constexpr int64_t kLengthDecayIntervalMs = 60 * 1000;
constexpr int64_t kSpareBackoffMinMs = 100;
constexpr int64_t kSpareBackoffMaxMs = 5 * 1000;
// 一次补卷调用内最多串行补几条（T 很小，这个上限只是防御）。
constexpr size_t kSpareChainLimit = 4;

ReservationResult ReservationError(ErrorCode error, std::string detail) {
    ReservationResult result;
    result.error = error;
    result.detail = std::move(detail);
    return result;
}

std::vector<PartitionVSegmentConfig> ConfigsForPartition(
    const PartitionPhysicalQuotaSnapshot& snapshot,
    const std::string& partition_id) {
    std::vector<PartitionVSegmentConfig> configs;
    for (const auto& profile : snapshot.profile_specs) {
        PartitionVSegmentConfig config;
        if (BuildPartitionConfig(snapshot, partition_id, profile.name,
                                 &config) == ErrorCode::OK)
            configs.push_back(std::move(config));
    }
    return configs;
}

}  // namespace

VSegmentManager::VSegmentManager(PartitionVSegmentConfig config,
                                 std::vector<VSegmentProfile> profiles,
                                 std::shared_ptr<VSegmentStateCommitter> committer,
                                 size_t max_operation_tombstones)
    : partition_id_(config.partition_id),
      config_generation_(config.config_generation),
      default_profile_(config.profile_name),
      quota_configs_{std::move(config)},
      physical_allocator_(quota_configs_),
      committer_(std::move(committer)),
      max_operation_tombstones_(
          std::max<size_t>(1, max_operation_tombstones)) {
    for (auto& profile : profiles) {
        profiles_.emplace(profile.name, std::move(profile));
    }
    ComputePoolCapacity();
}

VSegmentManager::VSegmentManager(
    const PartitionPhysicalQuotaSnapshot& quota_snapshot,
    std::string partition_id,
    std::shared_ptr<VSegmentStateCommitter> committer,
    size_t max_operation_tombstones)
    : partition_id_(std::move(partition_id)),
      config_generation_(quota_snapshot.config_generation),
      default_profile_(quota_snapshot.default_profile),
      quota_configs_(ConfigsForPartition(quota_snapshot, partition_id_)),
      physical_allocator_(quota_configs_),
      committer_(std::move(committer)),
      max_operation_tombstones_(
          std::max<size_t>(1, max_operation_tombstones)) {
    for (const auto& profile : quota_snapshot.profile_specs)
        profiles_.emplace(profile.name, profile);
    ComputePoolCapacity();
}

VSegmentManager::VSegmentManager(
    std::vector<PartitionVSegmentConfig> configs,
    const std::vector<VSegmentProfile>& profiles, std::string default_profile,
    std::shared_ptr<VSegmentStateCommitter> committer,
    size_t max_operation_tombstones)
    : partition_id_(configs.empty() ? std::string{} : configs.front().partition_id),
      config_generation_(configs.empty() ? 0 : configs.front().config_generation),
      default_profile_(std::move(default_profile)),
      quota_configs_(std::move(configs)),
      physical_allocator_(quota_configs_),
      committer_(std::move(committer)),
      max_operation_tombstones_(std::max<size_t>(1, max_operation_tombstones)) {
    for (const auto& profile : profiles) profiles_.emplace(profile.name, profile);
    ComputePoolCapacity();
}

VSegmentAllocationResult VSegmentManager::Create(
    const std::string& profile_name) {
    const auto creation_key = partition_id_ + "/" + profile_name;
    auto result = profile_creation_coordinator_.GetOrCreate(
        creation_key,
        [&] { return CreateSingleFlight(profile_name, 0); });
    return FinishCreate(std::move(result));
}

VSegmentAllocationResult VSegmentManager::FinishCreate(VSegmentAllocationResult result) {
    if (result) {
        std::shared_ptr<VSegmentDeferredCommitHandle> fence;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            fence = latest_persist_;
        }
        // A concurrent initialization batch may have won the single flight.
        result.error = WaitForDurable(fence, &result.detail);
    }
    if (result) VSegmentMetrics::Instance().IncCreateSuccess();
    else VSegmentMetrics::Instance().IncCreateFailure();
    return result;
}

VSegmentAllocationResult VSegmentManager::RequestCreate(
    const std::string& profile_name, uint64_t retry_after_ms) {
    if (!profiles_.count(profile_name))
        return ManagerError(ErrorCode::INVALID_PARAMS,
                            "unknown vsegment profile: " + profile_name);
    const auto key = partition_id_ + "/" + profile_name;
    uint64_t creation_epoch = 0;
    {
        std::lock_guard<std::mutex> state_lock(mutex_);
        creation_epoch = route_epoch_;
    }
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto pending = pending_creations_.find(key);
    if (pending != pending_creations_.end()) {
        if (pending->second.wait_for(std::chrono::milliseconds(0)) ==
            std::future_status::ready) {
            auto result = pending->second.get();
            pending_creations_.erase(pending);
            return result;
        }
    } else {
        pending_creations_.emplace(
            key, std::async(std::launch::async,
                            [this, profile_name, creation_epoch] {
                                const auto creation_key =
                                    partition_id_ + "/" + profile_name;
                                auto result =
                                    profile_creation_coordinator_.GetOrCreate(
                                        creation_key, [&] {
                                            return CreateSingleFlight(
                                                profile_name, creation_epoch);
                                        });
                                auto finished = FinishCreate(std::move(result));
                                // 建完再判一次：仍低于水位就置位，由下一次写或下一次
                                // 装配把它消费掉。注意不能在这里递归调
                                // RequestCreate： pending_creations_
                                // 里这条要等任务结束才 ready， 递归只会立刻拿到
                                // -105。
                                bool replenished = false;
                                size_t active = 0;
                                size_t target = 0;
                                uint64_t candidate = 0;
                                {
                                    std::lock_guard<std::mutex> state_lock(
                                        mutex_);
                                    const auto profile =
                                        profiles_.find(profile_name);
                                    if (finished && owned_ && !degraded_ &&
                                        profile != profiles_.end() &&
                                        profile->second.spare_target != 0) {
                                        candidate = CandidateLengthLocked(
                                            profile->second);
                                        active =
                                            ActiveCountLocked(profile_name);
                                        target =
                                            SpareTargetLocked(profile_name);
                                        if (ServiceableCountLocked(profile_name,
                                                                   candidate) <
                                            target)
                                            replenish_pending_.store(
                                                true,
                                                std::memory_order_relaxed);
                                        replenished = true;
                                    }
                                }
                                // 补卷成功：只在候补开启（spare_target !=
                                // 0）时打， 低频（每条卷一次），用于内网核对
                                // active/target/ candidate_length 三者的关系。
                                if (replenished) {
                                    LOG(INFO)
                                        << "vsegment spare replenished: "
                                           "partition="
                                        << partition_id_
                                        << ", profile=" << profile_name
                                        << ", active=" << active
                                        << ", target=" << target
                                        << ", candidate_length=" << candidate;
                                }
                                return finished;
                            })
                     .share());
    }
    return ManagerError(ErrorCode::VSEGMENT_CREATING,
                        "vsegment creation is in progress; retry_after_ms=" +
                            std::to_string(retry_after_ms));
}

VSegmentPutStartResult VSegmentManager::StartPut(
    const std::string& operation_id, uint64_t length,
    const std::string& requested_profile, uint64_t expected_route_epoch,
    const std::vector<std::string>& excluded_vsegments,
    VSegmentCommitBatch* batch) {
    const auto& profile_name =
        requested_profile.empty() ? default_profile_ : requested_profile;
    const auto profile = profiles_.find(profile_name);
    if (operation_id.empty() || length == 0 || profile == profiles_.end())
        return {ErrorCode::INVALID_PARAMS, operation_id, {},
                "invalid PutStart operation, length, or profile"};
    // 对象长度超过一条卷的逻辑容量时，Reserve 永远放不下；旧行为会在这条
    // 分支上每次重试都触发一次建卷（白建，直到配额耗尽返回 -104）。
    // 直接拒绝，且不触发任何建卷。
    if (length > LogicalCapacity(profile->second))
        return {ErrorCode::INVALID_PARAMS,
                operation_id,
                {},
                "PutStart length exceeds vsegment logical capacity"};

    for (int pass = 0; pass < 2; ++pass) {
        std::shared_ptr<VSegmentDeferredCommitHandle> fence;
        bool allocated = false;
        VSegmentPutStartResult allocated_result;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!owned_ ||
            (route_epoch_ != 0 && expected_route_epoch != route_epoch_))
                return {ErrorCode::STALE_ROUTE, operation_id, {},
                        "PutStart route epoch is stale"};
            if (degraded_) {
                // Reconcile failed for this partition: the in-memory allocator
                // may overlap live objects, so refuse new reservations.
                return {ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, operation_id,
                        {}, "vsegment partition degraded: reconcile failed"};
            }
            auto existing_operation = operation_vsegments_.find(operation_id);
            if (existing_operation != operation_vsegments_.end()) {
                // Reuse the reservation and propagate its latest accepted
                // journal fence to the caller's durability policy.
                auto managed = vsegments_.find(existing_operation->second);
                if (managed == vsegments_.end())
                    return {ErrorCode::INTERNAL_ERROR, operation_id, {},
                            "PutStart operation references a missing vsegment"};
                auto reservation =
                    managed->second.logical_allocator->Reserve(operation_id,
                                                               length);
                if (!reservation)
                    return {reservation.error, operation_id, {},
                            std::move(reservation.detail)};
                allocated_result = {ErrorCode::OK, operation_id,
                                    {partition_id_, existing_operation->second,
                                     reservation.range.offset,
                                     reservation.range.length, operation_id}, {}};
                fence = latest_persist_;
                allocated = true;
            }
            for (auto& [id, managed] : vsegments_) {
                if (allocated) break;
                if (std::find(excluded_vsegments.begin(),
                              excluded_vsegments.end(), id) !=
                    excluded_vsegments.end())
                    continue;
                if (managed.lifecycle != Lifecycle::ACTIVE ||
                    managed.profile_name != profile_name)
                    continue;
                // No full allocator Snapshot here: Reserve() touches only
                // free_ranges_/reservations_, so CancelReservation() below is
                // the exact inverse. A Snapshot per put costs O(completed
                // operations) and grows with the number of objects.
                auto reservation =
                    managed.logical_allocator->Reserve(operation_id, length);
                if (!reservation) {
                    if (reservation.error == ErrorCode::NO_AVAILABLE_HANDLE)
                        continue;
                    return {reservation.error, operation_id, {},
                            std::move(reservation.detail)};
                }
                std::string detail;
                operation_vsegments_.emplace(operation_id, id);
                ErrorCode persisted = ErrorCode::OK;
                if (VSegmentOplogAlignedMode()) {
                    // aligned：只写一条 O(1) 的 reservation 记录，
                    // 不做全量状态序列化、不产生 durable 等待。
                    persisted = committer_->CommitReservation(
                        partition_id_, operation_id, id, reservation.range,
                        route_epoch_);
                    if (persisted != ErrorCode::OK) {
                        detail = "failed to persist vsegment reservation";
                    }
                } else {
                    persisted = PersistDeferredLocked("logical_reserve",
                                                      &detail, &fence);
                }
                if (persisted != ErrorCode::OK) {
                    operation_vsegments_.erase(operation_id);
                    // Undo exactly the reservation we just made; restoring a
                    // full snapshot is unnecessary and O(objects).
                    if (managed.logical_allocator->CancelReservation(
                            operation_id) != ErrorCode::OK) {
                        LOG(WARNING) << "vsegment reservation rollback failed, "
                                        "operation="
                                     << operation_id;
                    }
                    return {persisted, operation_id, {}, std::move(detail)};
                }
                allocated_result = {ErrorCode::OK,
                                    operation_id,
                                    {partition_id_, id,
                                     reservation.range.offset,
                                     reservation.range.length, operation_id},
                                    {}};
                // 候补：这条卷如果刚被写到"再也接不住一次典型写"，置一个 bit，
                // 出锁后再决定是否补一条（锁内不做建卷）。判定与计数都是
                // O(#卷)， 且只在这一次预留把某条卷写满时发生。
                NoteWriteLength(length);
                if (managed.logical_allocator->LargestFreeExtent() <
                    CandidateLengthLocked(profile->second))
                    replenish_pending_.store(true, std::memory_order_relaxed);
                allocated = true;
                break;
            }
        }
        // 出锁消费候补信号：MaybeReplenish 自己会重新拿锁并调 RequestCreate
        // （异步单飞），不能在这里持锁调用。
        if (allocated && replenish_pending_.exchange(false))
            MaybeReplenish(profile_name);
        if (allocated) {
            if (batch) {
                batch->Add(committer_, fence);
                return allocated_result;
            }
            // durable fence：entry 已被 ordered writer 接受不可取消，等待
            // 语义与原同步持久化一致（etcd 故障时无限告警等待直至 durable）。
            const auto error = WaitForDurable(fence, &allocated_result.detail);
            if (error != ErrorCode::OK) allocated_result.error = error;
            return allocated_result;
        }
        auto creation = RequestCreate(profile_name);
        if (!creation) {
            return {creation.error, operation_id, {},
                    std::move(creation.detail)};
        }
    }
    return {ErrorCode::NO_AVAILABLE_HANDLE, operation_id, {},
            "new vsegment has no allocatable logical range"};
}

VSegmentAllocationResult VSegmentManager::CreateSingleFlight(
    const std::string& profile_name, uint64_t expected_route_epoch,
    bool single_phase_persist, VSegmentCommitBatch* batch) {
    auto profile = profiles_.find(profile_name);
    if (profile == profiles_.end()) {
        return ManagerError(ErrorCode::INVALID_PARAMS,
                            "unknown vsegment profile: " + profile_name);
    }
    // Preserve submission order under mutex_; wait outside the critical section.
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    VSegmentAllocationResult result;
    bool fresh_created = false;
    std::string fresh_vsegment_id;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!owned_ ||
            (expected_route_epoch != 0 && route_epoch_ != expected_route_epoch)) {
            VSegmentMetrics::Instance().IncStaleRoute();
            return ManagerError(ErrorCode::STALE_ROUTE,
                                "vsegment creation route epoch is stale");
        }
        bool adopted = false;
        for (auto& [id, pending] : vsegments_) {
            if (pending.profile_name != profile_name ||
                pending.lifecycle != Lifecycle::PREPARING)
                continue;
            pending.lifecycle = Lifecycle::ACTIVE;
            std::string detail;
            auto persisted = PersistDeferredLocked("vsegment_create_commit",
                                                    &detail, &fence);
            if (persisted != ErrorCode::OK) {
                pending.lifecycle = Lifecycle::PREPARING;
                return ManagerError(persisted, std::move(detail));
            }
            result = {ErrorCode::OK, pending.view, {}};
            adopted = true;
            break;
        }
        if (!adopted) {
            const std::string vsegment_id = UuidToString(generate_uuid());
            auto logical = std::make_unique<LogicalRangeAllocator>(
                profile->second.member_extent_size * profile->second.member_count);
            // 运行时坏端点排除集快照：把新 vsegment 的条带成员避开已判死的物理段
            // （provider 为空时不过滤，行为与旧版本一致）。
            std::set<std::string> excluded_segments;
            const std::set<std::string>* excluded_segments_ptr = nullptr;
            if (excluded_segments_provider_) {
                excluded_segments = excluded_segments_provider_();
                excluded_segments_ptr = &excluded_segments;
            }
            auto allocation = physical_allocator_.Allocate(
                vsegment_id, profile->second, excluded_segments_ptr);
            if (!allocation) return allocation;
            try {
                vsegments_.emplace(
                    vsegment_id,
                    ManagedVSegment{profile_name,
                        single_phase_persist ? Lifecycle::ACTIVE : Lifecycle::PREPARING,
                        allocation.view, std::move(logical)});
            } catch (...) {
                physical_allocator_.Release(allocation.view);
                throw;
            }
            std::string detail;
            auto persisted = PersistDeferredLocked(
                single_phase_persist ? "vsegment_create" : "vsegment_create_begin",
                &detail, &fence);
            if (persisted != ErrorCode::OK) {
                vsegments_.erase(vsegment_id);
                physical_allocator_.Release(allocation.view);
                return ManagerError(persisted, std::move(detail));
            }
            if (!single_phase_persist) {
                vsegments_.at(vsegment_id).lifecycle = Lifecycle::ACTIVE;
                persisted = PersistDeferredLocked("vsegment_create_commit", &detail,
                                                   &fence);
                if (persisted != ErrorCode::OK) {
                    vsegments_.at(vsegment_id).lifecycle = Lifecycle::PREPARING;
                    return ManagerError(persisted, std::move(detail));
                }
            }
            result = allocation;
            fresh_created = true;
            fresh_vsegment_id = vsegment_id;
        }
    }
    if (batch) {
        batch->Add(committer_, fence);
    } else {
        const auto error = WaitForDurable(fence, &result.detail);
        if (error != ErrorCode::OK) result.error = error;
    }
    // vsegment/psegment 术语的创建可见性日志（锁外打印，避免拖慢提交序）：
    // 进程内首卷 INFO 全量（成员 extent 清单），此后 VLOG(1) 防刷屏
    //（16384 partition 首建期每 partition 一卷）；聚合计数见周期
    // "VSegment Status" 行与 VSegmentMetrics。adopted（PREPARING→ACTIVE
    // 续提交流程）不算新创建，不打。
    if (fresh_created && result.error == ErrorCode::OK) {
        static std::atomic<bool> s_first_vsegment_logged{false};
        const bool first = !s_first_vsegment_logged.exchange(true);
        if (first || VLOG_IS_ON(1)) {
            std::ostringstream os;
            os << "VSegment created" << (first ? " (first)" : "")
               << ": partition=" << partition_id_
               << ", id=" << fresh_vsegment_id
               << ", capacity=" << result.view.logical_capacity
               << ", members=[";
            for (size_t m = 0; m < result.view.members.size(); ++m) {
                if (m) os << ", ";
                const auto& member = result.view.members[m];
                os << "{" << member.segment_id
                   << ",offset=" << member.base_offset
                   << ",len=" << member.length << "}";
            }
            os << "]";
            if (first) {
                LOG(INFO) << os.str();
            } else {
                VLOG(1) << os.str();
            }
        }
    }
    return result;
}

ErrorCode VSegmentManager::SetRouteEpoch(uint64_t route_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!owned_ || route_epoch == 0 || route_epoch < route_epoch_) {
        VSegmentMetrics::Instance().IncStaleRoute();
        return ErrorCode::STALE_ROUTE;
    }
    route_epoch_ = route_epoch;
    return ErrorCode::OK;
}

uint64_t VSegmentManager::RouteEpoch() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return route_epoch_;
}

ErrorCode VSegmentManager::Deactivate(uint64_t route_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (route_epoch == 0 || route_epoch < route_epoch_)
        return ErrorCode::STALE_ROUTE;
    route_epoch_ = route_epoch;
    owned_ = false;
    return ErrorCode::OK;
}

ErrorCode VSegmentManager::EnsureInitialVSegments(std::string* detail,
                                                 VSegmentCommitBatch* batch) {
    std::lock_guard<std::mutex> creation_lock(initial_creation_mutex_);
    std::vector<std::pair<std::string, uint32_t>> targets;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& config : quota_configs_) {
            const auto& name = config.profile_name;
            const auto profile = profiles_.find(name);
            if (profile == profiles_.end()) {
                if (detail)
                    *detail = "Partition quota references unknown profile";
                return ErrorCode::INVALID_PARAMS;
            }
            uint32_t present = 0;
            for (const auto& [id, managed] : vsegments_) {
                (void)id;
                if (managed.profile_name == name &&
                    managed.lifecycle == Lifecycle::ACTIVE)
                    ++present;
            }
            if (present < profile->second.initial_vsegment_count)
                targets.emplace_back(name,
                                     profile->second.initial_vsegment_count -
                                         present);
        }
    }
    for (const auto& [profile_name, count] : targets) {
        for (uint32_t index = 0; index < count; ++index) {
            // P2：首建走单阶段持久化（一笔 ACTIVE entry，无 begin/commit
            // 两阶段），配合 P3 的批量 durable barrier 削减冷启动 etcd 事务。
            const auto creation_key = partition_id_ + "/" + profile_name;
            auto result = profile_creation_coordinator_.GetOrCreate(
                creation_key, [&] {
                    return CreateSingleFlight(profile_name, 0, true, batch);
                });
            if (result) VSegmentMetrics::Instance().IncCreateSuccess();
            else VSegmentMetrics::Instance().IncCreateFailure();
            if (!result) {
                if (detail) *detail = std::move(result.detail);
                return result.error;
            }
        }
    }
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        fence = latest_persist_;
    }
    if (batch) {
        batch->Add(committer_, fence);
        return ErrorCode::OK;
    }
    return WaitForDurable(fence, detail);
}

ReservationResult VSegmentManager::ReservePut(
    const std::string& vsegment_id, const std::string& operation_id,
    uint64_t length, uint64_t expected_route_epoch) {
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    ReservationResult reserved;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!owned_ ||
            (route_epoch_ != 0 && expected_route_epoch != route_epoch_))
            return ReservationError(ErrorCode::STALE_ROUTE,
                                    "PutStart route epoch is stale");
        auto managed = vsegments_.find(vsegment_id);
        if (managed == vsegments_.end() ||
            managed->second.lifecycle != Lifecycle::ACTIVE)
            return ReservationError(ErrorCode::SEGMENT_NOT_FOUND,
                                    "vsegment not found or not active");
        auto existing = operation_vsegments_.find(operation_id);
        if (existing != operation_vsegments_.end() &&
            existing->second != vsegment_id)
            return ReservationError(ErrorCode::INVALID_WRITE,
                                    "operation is reserved in another vsegment");
        const auto before = managed->second.logical_allocator->Snapshot();
        auto result = managed->second.logical_allocator->Reserve(operation_id,
                                                                  length);
        if (!result) return result;
        std::string detail;
        const bool inserted =
            operation_vsegments_.emplace(operation_id, vsegment_id).second;
        // 锁收窄：提交在锁内完成，durable 等待移到锁外 fence 收口（语义
        // 同原同步持久化；失败回滚不变）。
        ErrorCode persisted = ErrorCode::OK;
        if (VSegmentOplogAlignedMode()) {
            persisted = committer_->CommitReservation(
                partition_id_, operation_id, vsegment_id, result.range,
                route_epoch_);
            if (persisted != ErrorCode::OK) {
                detail = "failed to persist vsegment reservation";
            }
        } else {
            persisted =
                PersistDeferredLocked("logical_reserve", &detail, &fence);
        }
        if (persisted != ErrorCode::OK) {
            if (inserted) operation_vsegments_.erase(operation_id);
            managed->second.logical_allocator->Restore(before);
            return ReservationError(persisted, std::move(detail));
        }
        reserved = std::move(result);
    }
    const auto error = WaitForDurable(fence);
    return error == ErrorCode::OK ? reserved : ReservationError(error, {});
}

ErrorCode VSegmentManager::CommitPut(const std::string& vsegment_id,
                                     const std::string& operation_id,
                                     const std::string& allocation_id,
                                     LogicalRange* range,
                                     uint64_t expected_route_epoch,
                                     const LogicalRange* expected_range,
                                     VSegmentCommitBatch* batch) {
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!owned_ ||
            (route_epoch_ != 0 && expected_route_epoch != route_epoch_))
            return ErrorCode::STALE_ROUTE;
        auto managed = vsegments_.find(vsegment_id);
        if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        // Only the legacy (non-aligned) persist path can fail and therefore
        // needs a full allocator snapshot for rollback. The aligned path
        // persists a single O(1) reservation/commit record and never restores,
        // so avoid the O(objects) copy that used to run on every PutEnd.
        std::optional<LogicalAllocationSnapshot> before;
        if (!VSegmentOplogAlignedMode())
            before = managed->second.logical_allocator->Snapshot();
        if (expected_range) {
            const auto actual =
                managed->second.logical_allocator->FindOperationRange(
                    operation_id);
            if (!actual || actual->offset != expected_range->offset ||
                actual->length != expected_range->length)
                return ErrorCode::INVALID_VERSION;
        }
        const auto result = managed->second.logical_allocator->Commit(
            operation_id, allocation_id, range);
        if (result != ErrorCode::OK) return result;
        // 锁收窄：提交在锁内完成，durable 等待移到锁外 fence 收口（语义
        // 同原同步持久化；失败回滚不变）。
        const auto persisted =
            VSegmentOplogAlignedMode()
                ? ErrorCode::OK
                : PersistDeferredLocked("logical_commit", nullptr, &fence);
        if (persisted != ErrorCode::OK) {
            if (before.has_value())
                managed->second.logical_allocator->Restore(*before);
            return persisted;
        }
    }
    if (batch) {
        batch->Add(committer_, fence);
        return ErrorCode::OK;
    }
    return WaitForDurable(fence);
}

ErrorCode VSegmentManager::AbortPut(const std::string& vsegment_id,
                                    const std::string& operation_id,
                                    uint64_t expected_route_epoch) {
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!owned_ ||
            (route_epoch_ != 0 && expected_route_epoch != route_epoch_))
            return ErrorCode::STALE_ROUTE;
        auto managed = vsegments_.find(vsegment_id);
        if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        const auto before = managed->second.logical_allocator->Snapshot();
        const auto result =
            managed->second.logical_allocator->Abort(operation_id);
        if (result != ErrorCode::OK) return result;
        const auto operations_before = operation_vsegments_;
        TrimOperationTombstonesLocked(managed->second,
                                      max_operation_tombstones_);
        // 锁收窄：提交在锁内完成，durable 等待移到锁外 fence 收口（语义
        // 同原同步持久化；失败回滚不变）。
        ErrorCode persisted = ErrorCode::OK;
        if (VSegmentOplogAlignedMode()) {
            persisted = committer_->CommitRelease(partition_id_, operation_id,
                                                  route_epoch_);
        } else {
            persisted =
                PersistDeferredLocked("logical_abort", nullptr, &fence);
        }
        if (persisted != ErrorCode::OK) {
            managed->second.logical_allocator->Restore(before);
            operation_vsegments_ = operations_before;
            return persisted;
        }
    }
    return WaitForDurable(fence);
}

ErrorCode VSegmentManager::RollbackPutReservation(
    const std::string& vsegment_id, const std::string& operation_id,
    uint64_t expected_route_epoch) {
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!owned_ ||
            (route_epoch_ != 0 && expected_route_epoch != route_epoch_))
            return ErrorCode::STALE_ROUTE;
        auto managed = vsegments_.find(vsegment_id);
        if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        const auto before = managed->second.logical_allocator->Snapshot();
        const auto operations_before = operation_vsegments_;
        const auto result =
            managed->second.logical_allocator->CancelReservation(operation_id);
        if (result != ErrorCode::OK) return result;
        operation_vsegments_.erase(operation_id);
        // 锁收窄：提交在锁内完成，durable 等待移到锁外 fence 收口（语义
        // 同原同步持久化；失败回滚不变）。
        ErrorCode persisted = ErrorCode::OK;
        if (VSegmentOplogAlignedMode()) {
            persisted = committer_->CommitRelease(partition_id_, operation_id,
                                                  route_epoch_);
        } else {
            persisted =
                PersistDeferredLocked("logical_abort", nullptr, &fence);
        }
        if (persisted != ErrorCode::OK) {
            managed->second.logical_allocator->Restore(before);
            operation_vsegments_ = operations_before;
            return persisted;
        }
    }
    return WaitForDurable(fence);
}

void VSegmentManager::TrimOperationTombstonesLocked(
    ManagedVSegment& managed, size_t max_completed) {
    const auto snapshot = managed.logical_allocator->Snapshot();
    if (snapshot.completed_operations.size() <= max_completed) return;
    size_t to_remove =
        snapshot.completed_operations.size() - max_completed;
    for (const auto& operation : snapshot.completed_operations) {
        if (to_remove == 0) return;
        if (operation.outcome == OperationOutcome::COMMITTED) continue;
        if (managed.logical_allocator->ForgetCompleted(
                operation.operation_id) == ErrorCode::OK) {
            operation_vsegments_.erase(operation.operation_id);
            --to_remove;
        }
    }
}

ErrorCode VSegmentManager::ReleaseObject(const std::string& vsegment_id,
                                         const std::string& allocation_id,
                                         LogicalRange range,
                                         uint64_t expected_route_epoch) {
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!owned_ ||
            (route_epoch_ != 0 && expected_route_epoch != route_epoch_))
            return ErrorCode::STALE_ROUTE;
        auto managed = vsegments_.find(vsegment_id);
        if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        const auto before = managed->second.logical_allocator->Snapshot();
        const auto result = managed->second.logical_allocator->Release(
            allocation_id, range);
        if (result != ErrorCode::OK) return result;
        std::vector<std::pair<std::string, std::string>> forgotten_operations;
        for (const auto& completed : before.completed_operations) {
            if (completed.allocation_id != allocation_id) continue;
            auto operation = operation_vsegments_.find(completed.operation_id);
            if (operation != operation_vsegments_.end()) {
                forgotten_operations.push_back(*operation);
                operation_vsegments_.erase(operation);
            }
            const auto forgotten =
                managed->second.logical_allocator->ForgetCompleted(
                    completed.operation_id);
            if (forgotten != ErrorCode::OK) {
                managed->second.logical_allocator->Restore(before);
                operation_vsegments_.insert(forgotten_operations.begin(),
                                            forgotten_operations.end());
                return forgotten;
            }
        }
        // 锁收窄：提交在锁内完成，durable 等待移到锁外 fence 收口（语义
        // 同原同步持久化；失败回滚不变）。
        const auto persisted = VSegmentOplogAlignedMode()
                                   ? ErrorCode::OK
                                   : PersistDeferredLocked("logical_release",
                                                      nullptr, &fence);
        if (persisted != ErrorCode::OK) {
            managed->second.logical_allocator->Restore(before);
            operation_vsegments_.insert(forgotten_operations.begin(),
                                        forgotten_operations.end());
            return persisted;
        }
    }
    return WaitForDurable(fence);
}

ErrorCode VSegmentManager::ForgetOperation(const std::string& operation_id) {
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    {
        std::lock_guard<std::mutex> lock(mutex_);
    if (!owned_) return ErrorCode::STALE_ROUTE;
        auto operation = operation_vsegments_.find(operation_id);
        if (operation == operation_vsegments_.end())
            return ErrorCode::OBJECT_NOT_FOUND;
        auto managed = vsegments_.find(operation->second);
        if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        const auto before = managed->second.logical_allocator->Snapshot();
        auto result =
            managed->second.logical_allocator->ForgetCompleted(operation_id);
        if (result != ErrorCode::OK) return result;
        const auto vsegment_id = operation->second;
        operation_vsegments_.erase(operation);
        // 锁收窄：提交在锁内完成，durable 等待移到锁外 fence 收口（语义
        // 同原同步持久化；失败回滚不变）。
        if (!VSegmentOplogAlignedMode()) {
            result = PersistDeferredLocked("operation_gc", nullptr, &fence);
        }
        if (result != ErrorCode::OK) {
            managed->second.logical_allocator->Restore(before);
            operation_vsegments_.emplace(operation_id, vsegment_id);
            return result;
        }
    }
    return WaitForDurable(fence);
}

ErrorCode VSegmentManager::TransitionLifecycle(
    const std::string& vsegment_id, Lifecycle target) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!owned_) return ErrorCode::STALE_ROUTE;
    auto managed = vsegments_.find(vsegment_id);
    if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
    const auto current = managed->second.lifecycle;
    if (current == target) return ErrorCode::OK;
    const bool valid =
        (current == Lifecycle::PREPARING && target == Lifecycle::ACTIVE) ||
        (current == Lifecycle::ACTIVE && target == Lifecycle::DRAINING) ||
        (current == Lifecycle::DRAINING && target == Lifecycle::RETIRED);
    if (!valid) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    if (target == Lifecycle::RETIRED &&
        !managed->second.logical_allocator->Empty())
        return ErrorCode::OBJECT_REPLICA_BUSY;
    std::vector<std::pair<std::string, std::string>> retired_operations;
    if (target == Lifecycle::RETIRED) {
        for (auto operation = operation_vsegments_.begin();
             operation != operation_vsegments_.end();) {
            if (operation->second == vsegment_id) {
                retired_operations.push_back(*operation);
                operation = operation_vsegments_.erase(operation);
            } else {
                ++operation;
            }
        }
    }
    managed->second.lifecycle = target;
    bool released_extent = false;
    if (target == Lifecycle::RETIRED) {
        const auto released = physical_allocator_.Release(managed->second.view);
        if (released != ErrorCode::OK) {
            managed->second.lifecycle = current;
            operation_vsegments_.insert(retired_operations.begin(),
                                        retired_operations.end());
            return released;
        }
        released_extent = true;
    }
    const auto persisted = PersistLocked("lifecycle_transition");
    if (persisted != ErrorCode::OK) {
        managed->second.lifecycle = current;
        operation_vsegments_.insert(retired_operations.begin(),
                                    retired_operations.end());
        if (released_extent) {
            std::string ignored;
            physical_allocator_.Restore(managed->second.view,
                                         profiles_.at(
                                             managed->second.profile_name),
                                         &ignored);
        }
    }
    return persisted;
}

PartitionVSegmentSnapshot VSegmentManager::SnapshotLocked() const {
    PartitionVSegmentSnapshot snapshot;
    snapshot.partition_id = partition_id_;
    snapshot.config_generation = config_generation_;
    snapshot.route_epoch = route_epoch_;
    snapshot.metadata_revision = metadata_revision_;
    snapshot.operation_vsegments = operation_vsegments_;
    for (const auto& [id, managed] : vsegments_) {
        snapshot.vsegments.push_back(
            {managed.profile_name, managed.lifecycle, managed.view,
             managed.logical_allocator->Snapshot()});
    }
    std::sort(snapshot.vsegments.begin(), snapshot.vsegments.end(),
              [](const auto& left, const auto& right) {
                  return left.view.vsegment_id < right.view.vsegment_id;
              });
    return snapshot;
}

PartitionVSegmentSnapshot VSegmentManager::Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return SnapshotLocked();
}

ErrorCode VSegmentManager::PersistLocked(const std::string& mutation,
                                         std::string* detail) {
    if (!committer_) {
        if (detail) *detail = "vsegment state committer is not configured";
        return ErrorCode::PERSISTENT_FAIL;
    }
    ++metadata_revision_;
    auto result = committer_->Commit(SnapshotLocked(), mutation, detail);
    if (result != ErrorCode::OK) {
        --metadata_revision_;
        VSegmentMetrics::Instance().IncPersistenceFailure();
    }
    return result;
}

ErrorCode VSegmentManager::PersistDeferredLocked(
    const std::string& mutation, std::string* detail,
    std::shared_ptr<VSegmentDeferredCommitHandle>* fence_out) {
    if (!committer_) {
        if (detail) *detail = "vsegment state committer is not configured";
        return ErrorCode::PERSISTENT_FAIL;
    }
    ++metadata_revision_;
    std::shared_ptr<VSegmentDeferredCommitHandle> handle;
    // committer 不支持延迟（默认回退同步 Commit）时 handle 为空且 entry
    // 已 durable，语义与 PersistLocked 等价。
    SpDiag::PerfPoint pt_snapshot(PerfKey::VSEGMENT_STATE_SNAPSHOT,
                                  SpDiag::PerfLevel::MODULE);
    pt_snapshot.Start();
    auto snapshot = SnapshotLocked();
    pt_snapshot.End(0);
    auto result =
        committer_->CommitDeferred(snapshot, mutation, &handle, detail);
    if (result != ErrorCode::OK) {
        --metadata_revision_;
        VSegmentMetrics::Instance().IncPersistenceFailure();
        return result;
    }
    latest_persist_ = handle;
    if (fence_out) *fence_out = std::move(handle);
    return ErrorCode::OK;
}

ErrorCode VSegmentManager::WaitForDurable(
    const std::shared_ptr<VSegmentDeferredCommitHandle>& handle,
    std::string* detail) {
    if (!handle) return ErrorCode::OK;
    return committer_->WaitDurable(handle, detail);
}

ErrorCode VSegmentManager::Restore(
    const PartitionVSegmentSnapshot& snapshot, std::string* detail) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (quota_configs_.empty()) {
        if (detail) *detail = "Partition has no profile quota";
        return ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT;
    }
    for (const auto& config : quota_configs_) {
        auto validation = ValidatePartitionConfig(config, detail);
        if (validation != ErrorCode::OK) return validation;
    }
    if (!vsegments_.empty()) {
        if (detail) *detail = "restore requires an empty manager";
        return ErrorCode::INVALID_PARAMS;
    }
    if (snapshot.partition_id != partition_id_ ||
        snapshot.config_generation != config_generation_) {
        if (detail) *detail = "snapshot Partition or generation mismatch";
        return ErrorCode::INVALID_VERSION;
    }
    PartitionQuotaAllocator verifier(quota_configs_);
    std::vector<std::unique_ptr<LogicalRangeAllocator>> logical_allocators;
    std::vector<VSegmentProfile> matching_profiles;
    std::vector<bool> restore_view;
    for (const auto& state : snapshot.vsegments) {
        if (state.lifecycle != Lifecycle::PREPARING &&
            state.lifecycle != Lifecycle::ACTIVE &&
            state.lifecycle != Lifecycle::DRAINING &&
            state.lifecycle != Lifecycle::RETIRED) {
            if (detail) *detail = "invalid vsegment lifecycle";
            return ErrorCode::INVALID_PARAMS;
        }
        auto profile = profiles_.find(state.profile_name);
        if (profile == profiles_.end()) {
            if (detail) *detail = "snapshot view has no matching profile";
            return ErrorCode::INVALID_PARAMS;
        }
        if (state.lifecycle == Lifecycle::PREPARING ||
            state.lifecycle == Lifecycle::RETIRED) {
            // CREATE_COMMIT was not durably observed. Its statically owned
            // extents are safe to roll back locally during recovery.
            logical_allocators.push_back(nullptr);
            matching_profiles.push_back(profile->second);
            restore_view.push_back(false);
            continue;
        }
        auto validation = verifier.Restore(state.view, profile->second, detail);
        if (validation != ErrorCode::OK) return validation;
        auto logical = std::make_unique<LogicalRangeAllocator>(
            state.view.logical_capacity);
        validation = logical->Restore(state.logical_allocation, detail);
        if (validation != ErrorCode::OK) return validation;
        logical_allocators.push_back(std::move(logical));
        matching_profiles.push_back(profile->second);
        restore_view.push_back(true);
    }

    for (size_t index = 0; index < snapshot.vsegments.size(); ++index) {
        const auto& state = snapshot.vsegments[index];
        if (!restore_view[index]) continue;
        auto restored = physical_allocator_.Restore(
            state.view, matching_profiles[index], detail);
        if (restored != ErrorCode::OK) return restored;
        vsegments_.emplace(
            state.view.vsegment_id,
            ManagedVSegment{state.profile_name, state.lifecycle, state.view,
                            std::move(logical_allocators[index])});
    }
    for (const auto& [operation, vsegment] : snapshot.operation_vsegments) {
        if (operation.empty() || !vsegments_.count(vsegment)) {
            if (detail) *detail = "operation references missing vsegment";
            return ErrorCode::INVALID_PARAMS;
        }
    }
    operation_vsegments_ = snapshot.operation_vsegments;
    route_epoch_ = snapshot.route_epoch;
    metadata_revision_ = snapshot.metadata_revision;
    return ErrorCode::OK;
}

ErrorCode VSegmentManager::ReconcileObjectReferences(
    const std::vector<VSegmentObjectReference>& references,
    std::string* detail,
    const std::vector<PendingReservation>& pending_reservations) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!owned_) return ErrorCode::STALE_ROUTE;
    std::unordered_map<std::string, std::vector<VSegmentObjectReference>>
        by_vsegment;
    for (const auto& reference : references) {
        if (reference.replica.partition_id != partition_id_ ||
            reference.replica.vsegment_id.empty() ||
            reference.allocation_id.empty() || reference.replica.length == 0) {
            if (detail) *detail = "invalid recovered object reference";
            return ErrorCode::INVALID_PARAMS;
        }
        if (!vsegments_.count(reference.replica.vsegment_id)) {
            if (detail) *detail = "object references missing vsegment";
            return ErrorCode::SEGMENT_NOT_FOUND;
        }
        if (!reference.committed && reference.replica.operation_id.empty()) {
            if (detail) *detail = "pending object has no operation identity";
            return ErrorCode::INVALID_PARAMS;
        }
        by_vsegment[reference.replica.vsegment_id].push_back(reference);
    }

    std::unordered_map<std::string, LogicalAllocationSnapshot> before;
    const auto operations_before = operation_vsegments_;
    std::unordered_map<std::string, std::string> reconciled_operations;
    std::unordered_map<std::string, std::vector<PendingReservation>>
        pending_by_vsegment;
    for (const auto& pending : pending_reservations) {
        if (!pending.vsegment_id.empty() && !pending.operation_id.empty()) {
            pending_by_vsegment[pending.vsegment_id].push_back(pending);
        }
    }
    for (auto& [vsegment_id, managed] : vsegments_) {
        before.emplace(vsegment_id, managed.logical_allocator->Snapshot());
        LogicalAllocationSnapshot desired;
        desired.logical_capacity = managed.view.logical_capacity;
        std::vector<LogicalRange> occupied;
        for (const auto& reference : by_vsegment[vsegment_id]) {
            LogicalRange range{reference.replica.logical_offset,
                               reference.replica.length};
            occupied.push_back(range);
            const std::string operation_id =
                reference.replica.operation_id.empty()
                    ? "recovered/" + reference.allocation_id
                    : reference.replica.operation_id;
            if (reference.committed) {
                desired.completed_operations.push_back(
                    {operation_id, reference.allocation_id,
                     OperationOutcome::COMMITTED, range, 0});
            } else {
                desired.reservations.push_back({operation_id, range});
            }
            if (!reconciled_operations.emplace(operation_id, vsegment_id)
                     .second) {
                if (detail) *detail = "duplicate recovered operation identity";
                for (auto& [id, state] : vsegments_) {
                    auto snapshot = before.find(id);
                    if (snapshot != before.end())
                        state.logical_allocator->Restore(snapshot->second);
                }
                return ErrorCode::INVALID_PARAMS;
            }
        }
        const auto pending_it = pending_by_vsegment.find(vsegment_id);
        if (pending_it != pending_by_vsegment.end()) {
            for (const auto& pending : pending_it->second) {
                if (reconciled_operations.count(pending.operation_id) != 0) {
                    continue;  // 已被 committed 对象覆盖
                }
                const LogicalRange range = pending.range;
                bool overlaps = (range.length == 0);
                for (const auto& taken : occupied) {
                    if (range.offset < taken.offset + taken.length &&
                        taken.offset < range.offset + range.length) {
                        overlaps = true;
                        break;
                    }
                }
                if (overlaps) {
                    LOG(WARNING) << "reconcile: dropping pending reservation "
                                 << pending.operation_id
                                 << " (overlaps committed range or empty)";
                    continue;
                }
                occupied.push_back(range);
                desired.reservations.push_back({pending.operation_id, range});
                reconciled_operations.emplace(pending.operation_id,
                                              vsegment_id);
            }
        }
        std::sort(occupied.begin(), occupied.end(), [](const auto& left,
                                                       const auto& right) {
            return left.offset < right.offset;
        });
        uint64_t cursor = 0;
        for (const auto& range : occupied) {
            if (range.offset < cursor ||
                range.offset > desired.logical_capacity ||
                range.length > desired.logical_capacity - range.offset) {
                if (detail)
                    *detail =
                        "overlapping or out-of-range object reference";
                for (auto& [id, state] : vsegments_) {
                    auto snapshot = before.find(id);
                    if (snapshot != before.end())
                        state.logical_allocator->Restore(snapshot->second);
                }
                // Rebuilt state cannot be trusted: refuse new allocations
                // (fail-closed) until a later reconcile succeeds.
                degraded_ = true;
                LOG(ERROR) << "vsegment reconcile failed for partition "
                           << partition_id_
                           << " (overlapping or out-of-range reference); "
                              "partition degraded, writes rejected";
                return ErrorCode::INVALID_PARAMS;
            }
            if (range.offset > cursor)
                desired.free_ranges.push_back({cursor, range.offset - cursor});
            cursor = range.offset + range.length;
        }
        if (cursor < desired.logical_capacity)
            desired.free_ranges.push_back(
                {cursor, desired.logical_capacity - cursor});
        auto result = managed.logical_allocator->Restore(desired, detail);
        if (result != ErrorCode::OK) {
            for (auto& [id, state] : vsegments_)
                state.logical_allocator->Restore(before.at(id));
            degraded_ = true;
            LOG(ERROR) << "vsegment reconcile failed for partition "
                       << partition_id_
                       << " (allocator restore rejected); "
                          "partition degraded, writes rejected";
            return result;
        }
    }
    operation_vsegments_ = std::move(reconciled_operations);
    const auto result = PersistLocked("recovery_reconcile", detail);
    if (result != ErrorCode::OK) {
        for (auto& [id, state] : vsegments_)
            state.logical_allocator->Restore(before.at(id));
        operation_vsegments_ = operations_before;
        degraded_ = true;
        LOG(ERROR) << "vsegment reconcile failed for partition " << partition_id_
                   << " (checkpoint persist failed); "
                      "partition degraded, writes rejected";
        return result;
    }
    if (degraded_) {
        LOG(INFO) << "vsegment partition " << partition_id_
                  << " recovered: reconcile succeeded, leaving degraded state";
        degraded_ = false;
    }
    return result;
}

bool VSegmentManager::FindView(const std::string& vsegment_id,
                               VSegmentView* view) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto vsegment = vsegments_.find(vsegment_id);
    if (!owned_ || vsegment == vsegments_.end()) return false;
    if (vsegment->second.lifecycle != Lifecycle::ACTIVE &&
        vsegment->second.lifecycle != Lifecycle::DRAINING)
        return false;
    if (view) *view = vsegment->second.view;
    return true;
}

void VSegmentManager::ComputePoolCapacity() {
    quota_bytes_ = 0;
    capacity_bytes_ = 0;
    for (const auto& config : quota_configs_) {
        auto profile = profiles_.find(config.profile_name);
        if (profile == profiles_.end()) continue;
        const uint64_t extent = profile->second.member_extent_size;
        const uint64_t members = profile->second.member_count;
        if (extent == 0 || members == 0) continue;
        std::set<std::string> segments;
        uint64_t units = 0;
        for (const auto& quota : config.quotas) {
            // A segment appears once per quota; count duplicate entries once.
            if (!segments.insert(quota.segment_id).second) continue;
            quota_bytes_ += quota.length;
            units += quota.length / extent;
        }
        // Each vsegment takes one extent from member_count distinct segments,
        // so only floor(total_units / member_count) groups can be used; the
        // remainder is discarded. groups * members <= units, so multiplying
        // groups * members first cannot overflow.
        const uint64_t groups = units / members;
        capacity_bytes_ += (groups * members) * extent;
    }
}

VSegmentManagerStats VSegmentManager::Stats() const {
    VSegmentManagerStats stats;
    stats.quota_bytes = quota_bytes_;
    stats.capacity_bytes = capacity_bytes_;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [id, managed] : vsegments_) {
            switch (managed.lifecycle) {
                case Lifecycle::PREPARING:
                    ++stats.preparing;
                    break;
                case Lifecycle::ACTIVE:
                    ++stats.active;
                    break;
                case Lifecycle::DRAINING:
                    ++stats.draining;
                    break;
                case Lifecycle::RETIRED:
                    ++stats.retired;
                    break;
            }
            // RETIRED already returned the physical quota
            // (TransitionLifecycle); counting it would double-count capacity
            // reused by a new vsegment.
            if (managed.lifecycle == Lifecycle::RETIRED) continue;
            stats.logical_capacity_bytes += managed.view.logical_capacity;
            stats.logical_free_bytes +=
                managed.logical_allocator->FreeBytes();
            stats.reserved_bytes += managed.logical_allocator->ReservedBytes();
            stats.reservations +=
                managed.logical_allocator->ReservationCount();
            stats.committed_allocations +=
                managed.logical_allocator->CommittedCount();
        }
    }
    std::lock_guard<std::mutex> pending_lock(pending_mutex_);
    for (const auto& [key, creation] : pending_creations_) {
        (void)key;
        if (creation.wait_for(std::chrono::milliseconds(0)) !=
            std::future_status::ready)
            ++stats.pending_creations;
    }
    return stats;
}

// ---------------------------------------------------------------------------
// 候补（备用卷）
// ---------------------------------------------------------------------------

void VSegmentManager::NoteWriteLength(uint64_t length) {
    if (length > observed_max_length_.load(std::memory_order_relaxed))
        observed_max_length_.store(length, std::memory_order_relaxed);
    const int64_t now = NowMs();
    int64_t last = last_length_decay_ms_.load(std::memory_order_relaxed);
    if (last == 0) {
        last_length_decay_ms_.store(now, std::memory_order_relaxed);
        return;
    }
    if (now - last < kLengthDecayIntervalMs) return;
    if (!last_length_decay_ms_.compare_exchange_strong(
            last, now, std::memory_order_relaxed))
        return;
    observed_max_length_.store(
        observed_max_length_.load(std::memory_order_relaxed) / 2,
        std::memory_order_relaxed);
}

uint64_t VSegmentManager::CandidateLengthLocked(
    const VSegmentProfile& profile) const {
    const uint64_t capacity = LogicalCapacity(profile);
    if (profile.candidate_length_bytes != 0)
        return std::min(profile.candidate_length_bytes, capacity);
    const uint64_t floor = std::max<uint64_t>(profile.stripe_size, 1);
    const uint64_t observed =
        observed_max_length_.load(std::memory_order_relaxed);
    return std::min(capacity, std::max(floor, observed));
}

size_t VSegmentManager::ActiveCountLocked(
    const std::string& profile_name) const {
    size_t active = 0;
    for (const auto& entry : vsegments_) {
        const auto& managed = entry.second;
        if (managed.profile_name == profile_name &&
            managed.lifecycle == Lifecycle::ACTIVE)
            ++active;
    }
    return active;
}

size_t VSegmentManager::ServiceableCountLocked(
    const std::string& profile_name, uint64_t candidate_length) const {
    size_t serviceable = 0;
    for (const auto& entry : vsegments_) {
        const auto& managed = entry.second;
        if (managed.profile_name != profile_name ||
            managed.lifecycle != Lifecycle::ACTIVE)
            continue;
        // 判据是"单段连续空闲"而不是空闲总量：Reserve 是 first-fit 找单段。
        if (managed.logical_allocator->LargestFreeExtent() >= candidate_length)
            ++serviceable;
    }
    return serviceable;
}

size_t VSegmentManager::QuotaHeadroomLocked(
    const VSegmentProfile& profile) const {
    if (profile.member_extent_size == 0) return 0;
    std::set<std::string> visited;
    size_t headroom = std::numeric_limits<size_t>::max();
    for (const auto& config : quota_configs_) {
        if (config.profile_name != profile.name) continue;
        for (const auto& quota : config.quotas) {
            if (!visited.insert(quota.segment_id).second) continue;
            const uint64_t free_bytes =
                physical_allocator_.FreeBytes(profile.name, quota.segment_id);
            headroom = std::min<size_t>(
                headroom,
                static_cast<size_t>(free_bytes / profile.member_extent_size));
        }
    }
    if (headroom == std::numeric_limits<size_t>::max()) return 0;
    return headroom;
}

size_t VSegmentManager::SpareTargetLocked(
    const std::string& profile_name) const {
    const auto profile = profiles_.find(profile_name);
    if (profile == profiles_.end()) return 0;
    const size_t active = ActiveCountLocked(profile_name);
    // spare_target == 0：目标就是现状，即"永不缺"，完全等价旧行为。
    if (profile->second.spare_target == 0) return active;
    const size_t headroom = QuotaHeadroomLocked(profile->second);
    // R = 1：一次写占一条卷，因此"1 条在用 + spare_target 条备用"。
    const size_t wanted = 1 + static_cast<size_t>(profile->second.spare_target);
    const size_t floor_target = std::max<size_t>(
        static_cast<size_t>(profile->second.initial_vsegment_count), wanted);
    return std::min(active + headroom, floor_target);
}

void VSegmentManager::ClearSpareBackoffLocked(const std::string& profile_name) {
    spare_backoff_.erase(profile_name);
}

void VSegmentManager::RecordSpareFailureLocked(const std::string& profile_name,
                                               ErrorCode error,
                                               const std::string& detail) {
    auto& state = spare_backoff_[profile_name];
    if (state.failures < 8) ++state.failures;
    const int64_t backoff_ms = std::min<int64_t>(
        kSpareBackoffMaxMs,
        kSpareBackoffMinMs << std::min<uint32_t>(state.failures, 6));
    state.next_attempt_ms = NowMs() + backoff_ms;
    LOG(WARNING) << "vsegment spare replenish failed: partition="
                 << partition_id_ << ", profile=" << profile_name
                 << ", error=" << static_cast<int32_t>(error)
                 << ", detail=" << detail << ", retry_in_ms=" << backoff_ms;
}

void VSegmentManager::MaybeReplenish(const std::string& profile_name) {
    const auto profile = profiles_.find(profile_name);
    if (profile == profiles_.end() || profile->second.spare_target == 0) return;
    for (size_t attempt = 0; attempt < kSpareChainLimit; ++attempt) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!owned_ || degraded_) return;
            const auto backoff = spare_backoff_.find(profile_name);
            if (backoff != spare_backoff_.end() &&
                NowMs() < backoff->second.next_attempt_ms)
                return;
            const uint64_t candidate = CandidateLengthLocked(profile->second);
            const size_t serviceable =
                ServiceableCountLocked(profile_name, candidate);
            if (serviceable >= SpareTargetLocked(profile_name)) return;
            // 目标水位已经顶到配额上限：再建也建不出来（Allocate 会 -104）。
            // 每个 partition 只喊一次，避免写满一次刷一条 -104。
            if (QuotaHeadroomLocked(profile->second) == 0) {
                auto& state = spare_backoff_[profile_name];
                if (!state.quota_warned) {
                    state.quota_warned = true;
                    LOG(WARNING)
                        << "vsegment spare target clipped by quota: partition="
                        << partition_id_ << ", profile=" << profile_name
                        << ", active=" << ActiveCountLocked(profile_name)
                        << ", serviceable=" << serviceable
                        << ", member_extent_size="
                        << profile->second.member_extent_size
                        << ", member_count=" << profile->second.member_count;
                }
                return;
            }
        }
        // 出锁再建卷：RequestCreate 内部会拿 mutex_ / pending_mutex_，并且
        // 同一 (partition, profile) 已有在建时直接返回 -105（单飞）。
        auto created = RequestCreate(profile_name);
        if (!created) {
            if (created.error == ErrorCode::VSEGMENT_CREATING) return;
            std::lock_guard<std::mutex> lock(mutex_);
            RecordSpareFailureLocked(profile_name, created.error,
                                     created.detail);
            return;
        }
        // 完成了一次建卷（拿到了已就绪的 pending 结果）：可能仍低于水位，
        // 继续下一轮（有上限），一次只建一条。
        std::lock_guard<std::mutex> lock(mutex_);
        ClearSpareBackoffLocked(profile_name);
    }
}

void VSegmentManager::MaybeReplenishAll() {
    std::vector<std::string> names;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!owned_ || degraded_) return;
        for (const auto& entry : profiles_) {
            if (entry.second.spare_target != 0) names.push_back(entry.first);
        }
    }
    for (const auto& name : names) MaybeReplenish(name);
}

}  // namespace mooncake::vsegment
