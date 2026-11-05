#include <glog/logging.h>
#include <sched.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <sstream>
#include <thread>
#include "spdk/spdk_wrapper.h"
#ifdef USE_NOF_URMA
#include "spdk/nof_memory_domain.h"
#endif

namespace mooncake {
namespace {

bool ParseEnvU64(const char *name, uint64_t *out) {
    const char *val = std::getenv(name);
    if (!val || *val == '\0') {
        return false;
    }

    errno = 0;
    char *end = nullptr;
    unsigned long long parsed = std::strtoull(val, &end, 10);
    if (errno != 0 || end == val || (end && *end != '\0')) {
        LOG(WARNING) << "Invalid value for " << name << ": " << val;
        return false;
    }

    *out = static_cast<uint64_t>(parsed);
    return true;
}

bool ParseEnvBool(const char *name, bool *out) {
    uint64_t v = 0;
    if (!ParseEnvU64(name, &v)) {
        return false;
    }
    *out = (v != 0);
    return true;
}

std::string FormatCpuAffinity(const cpu_set_t &cpuset) {
    std::ostringstream out;
    bool first = true;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (!CPU_ISSET(cpu, &cpuset)) {
            continue;
        }
        const int begin = cpu;
        while (cpu + 1 < CPU_SETSIZE && CPU_ISSET(cpu + 1, &cpuset)) {
            ++cpu;
        }
        if (!first) {
            out << ',';
        }
        out << begin;
        if (cpu != begin) {
            out << '-' << cpu;
        }
        first = false;
    }
    return out.str();
}

uint32_t GetNofQpairsPerSegment() {
    // R4 measured 31.16/39.83/43.17 GB/s with 1/2/4 qpairs at the same
    // aggregate QD=256. Four lanes also match the default NoF worker count.
    constexpr uint32_t kDefaultQpairs = 4;
    constexpr uint32_t kMaxQpairs = 64;
    uint64_t value = 0;
    if (!ParseEnvU64("MC_NOF_QPAIRS_PER_SEGMENT", &value)) {
        return kDefaultQpairs;
    }
    if (value == 0 || value > kMaxQpairs) {
        LOG(WARNING) << "Invalid MC_NOF_QPAIRS_PER_SEGMENT=" << value
                     << ", using default " << kDefaultQpairs;
        return kDefaultQpairs;
    }
    return static_cast<uint32_t>(value);
}

void ApplyCtrlrOptsFromEnv(struct spdk_nvme_ctrlr_opts *opts) {
    uint64_t v = 0;
    bool bv = false;

    // Keep SPDK's default KATO (currently 10 seconds). When a benchmark is
    // terminated before Cleanup() can detach its controller, target-side KATO
    // is the fallback that reclaims the orphaned controller and qpairs.
    if (ParseEnvU64("MC_NVME_KEEP_ALIVE_TIMEOUT_MS", &v)) {
        opts->keep_alive_timeout_ms = static_cast<uint32_t>(v);
    }

    if (ParseEnvU64("MC_NVME_NUM_IO_QUEUES", &v)) {
        opts->num_io_queues = static_cast<uint32_t>(v);
    }
    if (ParseEnvU64("MC_NVME_IO_QUEUE_SIZE", &v)) {
        opts->io_queue_size = static_cast<uint32_t>(v);
    }
    if (ParseEnvU64("MC_NVME_IO_QUEUE_REQUESTS", &v)) {
        opts->io_queue_requests = static_cast<uint32_t>(v);
    }
    if (ParseEnvU64("MC_NVME_TRANSPORT_ACK_TIMEOUT", &v)) {
        opts->transport_ack_timeout = static_cast<uint8_t>(v);
    }
    if (ParseEnvU64("MC_NVME_ADMIN_QUEUE_SIZE", &v)) {
        opts->admin_queue_size = static_cast<uint16_t>(v);
    }
    if (ParseEnvU64("MC_NVME_FABRICS_CONNECT_TIMEOUT_US", &v)) {
        opts->fabrics_connect_timeout_us = v;
    }
    if (ParseEnvBool("MC_NVME_HEADER_DIGEST", &bv)) {
        opts->header_digest = bv;
    }
    if (ParseEnvBool("MC_NVME_DATA_DIGEST", &bv)) {
        opts->data_digest = bv;
    }
    LOG(INFO) << "NVMe ctrlr opts: num_io_queues=" << opts->num_io_queues
              << ", io_queue_size=" << opts->io_queue_size
              << ", io_queue_requests=" << opts->io_queue_requests
              << ", keep_alive_timeout_ms=" << opts->keep_alive_timeout_ms
              << ", transport_ack_timeout="
              << static_cast<int>(opts->transport_ack_timeout)
              << ", admin_queue_size=" << opts->admin_queue_size
              << ", fabrics_connect_timeout_us="
              << opts->fabrics_connect_timeout_us
              << ", header_digest=" << opts->header_digest
              << ", data_digest=" << opts->data_digest;
}

}  // namespace

struct nof_io_lane {
    struct spdk_nvme_qpair *qpair = nullptr;
    struct nof_seg_handle *segment = nullptr;
    uint32_t index = 0;
    std::atomic<bool> healthy{true};
};

struct nof_seg_handle {
    struct spdk_nvme_ns *ns = nullptr;
    /* Modified By Yida (v3): 业务 worker 与 heartbeat 分离 qpair。SPDK URMA
     * qpair 内部有 outstanding list 和同一条 TCP control socket，heartbeat
     * 线程与 worker 同时 submit/poll 同一 qpair 会产生竞争。每个业务 lane
     * 由一个 worker 独占，heartbeat 只用 probe_qpair。 */
    std::vector<std::unique_ptr<nof_io_lane>> io_lanes;
    mutable std::mutex lanes_mutex;
    bool io_lanes_initialized = false;
    const void *io_owner = nullptr;
    struct spdk_nvme_qpair *probe_qpair = nullptr;
    std::mutex probe_mutex;
};

struct tr_info {
    struct spdk_nvme_transport_id trid;
    std::string ctrlr_key;
    uint32_t ns;
};

struct ctrlr_info {
    struct spdk_nvme_ctrlr *ctrlr = nullptr;
    int32_t last_admin_poll_error = 0;
    std::map<uint32_t, std::unique_ptr<nof_seg_handle>> ns_seg;
    std::mutex ns_mutex;
};

SpdkWrapper::SpdkWrapper() = default;

SpdkWrapper::~SpdkWrapper() { Cleanup(); }

SpdkWrapper &SpdkWrapper::GetInstance() {
    static SpdkWrapper ins;
    return ins;
}

bool SpdkWrapper::InitializeEnv() {
    if (initialized.load(std::memory_order_acquire)) {
        return true;
    }

    std::lock_guard<std::mutex> lock(init_mutex);
    if (initialized.load(std::memory_order_acquire)) {
        return true;
    }

    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.name = "mooncake";

    // EAL pins its calling thread to the main lcore. Preserve the caller's
    // placement so subsequently created RPC/helper threads do not inherit
    // that restriction. This does not change EAL's own worker affinity.
    bool restore_affinity = true;
    ParseEnvBool("MC_SPDK_RESET_AFFINITY", &restore_affinity);
    cpu_set_t original_affinity{};
    if (restore_affinity && sched_getaffinity(0, sizeof(original_affinity),
                                            &original_affinity) != 0) {
        PLOG(ERROR) << "Cannot save CPU affinity before SPDK initialization";
        return false;
    }

    int rc = spdk_env_init(&opts);
    if (restore_affinity) {
        cpu_set_t eal_affinity{};
        const bool have_eal_affinity =
            sched_getaffinity(0, sizeof(eal_affinity), &eal_affinity) == 0;
        // Restore even when initialization fails: EAL may already have
        // changed the affinity before returning an error.
        if (sched_setaffinity(0, sizeof(original_affinity),
                              &original_affinity) != 0) {
            // Do not retry a successful EAL initialization. Keep its lifecycle
            // state below intact and make the placement failure explicit.
            PLOG(ERROR) << "Failed to restore CPU affinity after spdk_env_init"
                        << " (rc=" << rc << "), requested=["
                        << FormatCpuAffinity(original_affinity)
                        << "]; subsequently created threads may inherit "
                           "EAL's CPU restriction";
        } else {
            cpu_set_t restored_affinity{};
            const bool have_restored_affinity =
                sched_getaffinity(0, sizeof(restored_affinity),
                                  &restored_affinity) == 0;
            LOG(INFO) << "SPDK initialization CPU affinity: before=["
                      << FormatCpuAffinity(original_affinity)
                      << "] after_init=["
                      << (have_eal_affinity ? FormatCpuAffinity(eal_affinity)
                                           : "unknown")
                      << "] restored=["
                      << (have_restored_affinity
                              ? FormatCpuAffinity(restored_affinity)
                              : "unknown")
                      << "] init_rc=" << rc;
        }
    } else {
        LOG(INFO) << "SPDK CPU affinity restoration disabled by "
                     "MC_SPDK_RESET_AFFINITY=0";
    }
    if (rc != 0) {
        fprintf(stderr, "SPDK init failed: %d\n", rc);
        return false;
    }

    // Mark SPDK as initialized.
    initialized.store(true, std::memory_order_release);
    admin_poll_running_.store(true, std::memory_order_release);
    admin_poll_thread_ =
        std::thread(&SpdkWrapper::AdminPollThreadFunc, this);
    return true;
}

void SpdkWrapper::Cleanup() {
    if (initialized.load(std::memory_order_acquire)) {
        admin_poll_running_.store(false, std::memory_order_release);
        admin_poll_cv_.notify_all();
        if (admin_poll_thread_.joinable()) {
            admin_poll_thread_.join();
        }

        {
            std::lock_guard<std::mutex> lock(ctrlrs_mutex);
            for (auto &[_, info] : connected_ctrlrs) {
                if (info) {
                    // Free all qpairs and segment handles
                    for (auto &[_, seg] : info->ns_seg) {
                        if (seg) {
                            // Each I/O lane is owned by one NoF worker. Free
                            // every business qpair before the probe qpair.
                            for (auto &lane : seg->io_lanes) {
                                if (lane && lane->qpair) {
                                    spdk_nvme_ctrlr_free_io_qpair(lane->qpair);
                                    lane->qpair = nullptr;
                                }
                            }
                            if (seg->probe_qpair) {
                                spdk_nvme_ctrlr_free_io_qpair(seg->probe_qpair);
                            }
                        }
                    }
                    // Detach controller
                    if (info->ctrlr) {
                        spdk_nvme_detach(info->ctrlr);
                    }
                }
            }
            connected_ctrlrs.clear();
        }

        {
            std::lock_guard<std::mutex> lock(probe_buffers_mutex_);
            for (auto &[_, probe_buffer] : probe_buffers_) {
                if (probe_buffer && probe_buffer->ptr) {
                    spdk_free(probe_buffer->ptr);
                    probe_buffer->ptr = nullptr;
                    probe_buffer->size = 0;
                }
            }
            probe_buffers_.clear();
        }
        spdk_env_fini();
        initialized.store(false, std::memory_order_release);
    }
}

void SpdkWrapper::AdminPollThreadFunc() {
    while (admin_poll_running_.load(std::memory_order_acquire)) {
        {
            std::lock_guard<std::mutex> lock(ctrlrs_mutex);
            for (auto &[endpoint, info] : connected_ctrlrs) {
                if (!info || !info->ctrlr) {
                    continue;
                }
                const int32_t rc =
                    spdk_nvme_ctrlr_process_admin_completions(info->ctrlr);
                if (rc < 0 && rc != info->last_admin_poll_error) {
                    LOG(WARNING)
                        << "NoF controller admin poll failed endpoint="
                        << endpoint << " rc=" << rc;
                }
                info->last_admin_poll_error = rc < 0 ? rc : 0;
            }
        }

        std::unique_lock<std::mutex> lock(admin_poll_mutex_);
        admin_poll_cv_.wait_for(
            lock, std::chrono::seconds(1), [this] {
                return !admin_poll_running_.load(std::memory_order_acquire);
            });
    }
}

void *SpdkWrapper::Alloc(size_t size, size_t align, int socket_id) {
    if (!InitializeEnv()) {
        return nullptr;
    }

    return spdk_zmalloc(size, align, nullptr, socket_id, SPDK_MALLOC_DMA);
}

void SpdkWrapper::Free(void *ptr) {
    if (ptr) {
        spdk_free(ptr);
    }
}

void SpdkWrapper::ProbeReadComplete(void *ctx,
                                    const struct spdk_nvme_cpl *cpl) {
    auto *probe_ctx = reinterpret_cast<ProbeRequestContext *>(ctx);
    if (spdk_nvme_cpl_is_error(cpl)) {
        {
            std::lock_guard<std::mutex> lock(probe_ctx->error_mutex);
            probe_ctx->error_reason =
                std::string("completion_error:") +
                NofCplStatusString(cpl, SPDK_NVME_OPC_READ);
        }
        probe_ctx->success.store(false, std::memory_order_release);
    } else {
        probe_ctx->success.store(true, std::memory_order_release);
    }
    probe_ctx->done.store(true, std::memory_order_release);
    if (probe_ctx->owner != nullptr) {
        probe_ctx->owner->RecycleProbeRequestContext(probe_ctx);
    }
}

void SpdkWrapper::ReplenishProbeRequestContextPoolLocked(size_t count) {
    for (size_t i = 0; i < count; ++i) {
        auto probe_ctx = std::make_unique<ProbeRequestContext>();
        probe_request_context_pool_.push(probe_ctx.get());
        probe_request_contexts_.push_back(std::move(probe_ctx));
    }
}

SpdkWrapper::ProbeRequestContext *SpdkWrapper::AcquireProbeRequestContext() {
    std::lock_guard<std::mutex> lock(probe_request_context_pool_mutex_);
    if (probe_request_context_pool_.empty()) {
        ReplenishProbeRequestContextPoolLocked(8);
    }
    auto *probe_ctx = probe_request_context_pool_.top();
    probe_request_context_pool_.pop();
    probe_ctx->Reset(this);
    return probe_ctx;
}

void SpdkWrapper::RecycleProbeRequestContext(ProbeRequestContext *ctx) {
    if (ctx == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(probe_request_context_pool_mutex_);
    probe_request_context_pool_.push(ctx);
}

int64_t SpdkWrapper::NvmePollProcessCompletion(nof_seg_handle *seg,
                                               uint32_t complete_per_seg) {
    return PollIoLane(GetIoLane(seg, 0), complete_per_seg);
}

size_t SpdkWrapper::GetIoLaneCount(const nof_seg_handle *seg_handle) const {
    if (seg_handle == nullptr) return 0;
    std::lock_guard<std::mutex> lock(seg_handle->lanes_mutex);
    return seg_handle->io_lanes.size();
}

nof_io_lane *SpdkWrapper::GetIoLane(nof_seg_handle *seg_handle,
                                    size_t lane_index) const {
    if (seg_handle == nullptr) return nullptr;
    std::lock_guard<std::mutex> lock(seg_handle->lanes_mutex);
    if (lane_index >= seg_handle->io_lanes.size()) {
        return nullptr;
    }
    return seg_handle->io_lanes[lane_index].get();
}

uint32_t SpdkWrapper::GetIoLaneIndex(const nof_io_lane *lane) const {
    return lane == nullptr ? 0 : lane->index;
}

uint16_t SpdkWrapper::GetIoLaneQpairId(const nof_io_lane *lane) const {
    return lane == nullptr || lane->qpair == nullptr
               ? 0
               : spdk_nvme_qpair_get_id(lane->qpair);
}

bool SpdkWrapper::ClaimIoLanes(nof_seg_handle *seg, const void *owner) {
    if (!seg || !owner) return false;
    std::lock_guard<std::mutex> lock(seg->lanes_mutex);
    if (seg->io_owner != nullptr && seg->io_owner != owner) return false;
    seg->io_owner = owner;
    return true;
}

void SpdkWrapper::ReleaseIoLanes(nof_seg_handle *seg, const void *owner) {
    if (!seg) return;
    std::lock_guard<std::mutex> lock(seg->lanes_mutex);
    if (seg->io_owner == owner) seg->io_owner = nullptr;
}

bool SpdkWrapper::IsIoLaneHealthy(const nof_io_lane *lane) const {
    return lane != nullptr && lane->healthy.load(std::memory_order_acquire);
}

void SpdkWrapper::MarkIoLaneFailed(nof_io_lane *lane) {
    if (lane) lane->healthy.store(false, std::memory_order_release);
}

int64_t SpdkWrapper::PollIoLane(nof_io_lane *lane,
                                uint32_t max_completions) {
    if (!lane || !lane->qpair) {
        return -1;
    }
    return spdk_nvme_qpair_process_completions(lane->qpair, max_completions);
}

void SpdkWrapper::DrainFailedNofSegment(nof_seg_handle *seg) {
    if (!seg) return;
    for (auto& lane : seg->io_lanes) {
        DrainFailedIoLane(lane.get());
    }
}

void SpdkWrapper::DrainFailedIoLane(nof_io_lane *lane) {
    if (!lane) return;
    MarkIoLaneFailed(lane);
    if (!lane->qpair) return;
    auto* qpair = lane->qpair;
    lane->qpair = nullptr;
    spdk_nvme_ctrlr_free_io_qpair(qpair);
}

#ifdef USE_NOF_URMA
bool NofWholePoolRegistrationAvailable() {
#ifdef HAVE_SPDK_URMA_WHOLE_POOL
    return true;
#else
    return false;
#endif
}

std::shared_ptr<void> RegisterNofMemoryPool(const std::string& endpoint,
                                           void* buffer, size_t size) {
#ifdef HAVE_SPDK_URMA_WHOLE_POOL
    if (!buffer || size == 0) return {};
    std::istringstream fields(endpoint);
    std::string field;
    bool urma = false;
    while (fields >> field) {
        if (field == "trtype:URMA") urma = true;
    }
    if (!urma) {
        LOG(ERROR) << "NoF whole-pool registration only supports URMA endpoints";
        return {};
    }
    auto* segment = SpdkWrapper::GetInstance().OpenNofSegment(endpoint);
    if (!segment || segment->io_lanes.empty()) return {};
    const auto info =
        NofMemoryDomainManager::GetInstance().ResolveBuffer(buffer, size);
    if (info.type != NofMemoryType::kHost && info.type != NofMemoryType::kCuda) {
        return {};
    }
    const auto type = info.type == NofMemoryType::kCuda
                          ? SPDK_NVME_URMA_MEM_CUDA : SPDK_NVME_URMA_MEM_HOST;
    const auto& registration_lane = segment->io_lanes.front();
    if (!registration_lane || !registration_lane->qpair) {
        LOG(ERROR) << "NoF whole-pool registration found no available lane "
                   << "endpoint=" << endpoint;
        return {};
    }
    spdk_nvme_urma_memory_region* region = nullptr;
    const int rc = spdk_nvme_urma_register_memory_for_qpair(
        registration_lane->qpair, buffer, size, type, &region);
    if (rc != 0 || !region) {
        LOG(ERROR) << "NoF whole-pool registration failed endpoint="
                   << endpoint << " buffer=" << buffer << " size=" << size
                   << " rc=" << rc;
        return {};
    }
    // SPDK's URMA region registry is keyed by the shared urma_context, so all
    // qpairs on the same NIC reuse this region. Registering the same buffer
    // once per lane would add duplicate registry entries without improving
    // coverage.
    LOG(INFO) << "NoF whole-pool registered endpoint=" << endpoint
              << " shared_by_lanes=" << segment->io_lanes.size()
              << " buffer=" << buffer << " size=" << size;
    return {region, [](void* value) {
        spdk_nvme_urma_unregister_memory(
            static_cast<spdk_nvme_urma_memory_region*>(value));
    }};
#else
    (void)endpoint;
    (void)buffer;
    (void)size;
    LOG(ERROR) << "SPDK does not expose URMA whole-pool registration";
    return {};
#endif
}
#endif

int SpdkWrapper::ParseTransPortStr(const std::string &tr_str, tr_info *info) {
    std::memset(&info->trid, 0, sizeof(info->trid));
    info->ns = 1;

    if (spdk_nvme_transport_id_parse(&info->trid, tr_str.c_str()) != 0) {
        LOG(ERROR) << "Error parsing transport address";
        return -1;
    }

    std::string ns_prefix = "ns:";
    size_t ns_pos = tr_str.find(ns_prefix);
    if (ns_pos != std::string::npos) {
        size_t ns_start = ns_pos + ns_prefix.length();
        size_t ns_end = tr_str.find_first_of(" \t", ns_start);

        std::string ns_str;
        if (ns_end == std::string::npos) {
            ns_str = tr_str.substr(ns_start);
        } else {
            ns_str = tr_str.substr(ns_start, ns_end - ns_start);
        }

        try {
            info->ns = std::stoul(ns_str);
        } catch (const std::exception &e) {
            LOG(ERROR) << "Failed to parse ns value: " << ns_str
                       << ", error: " << e.what();
            return -1;
        }
    } else {
        LOG(ERROR) << "No ns field found in transport string";
    }

    info->ctrlr_key = std::string(info->trid.traddr) + "|" +
                      std::string(info->trid.trsvcid) + "|" +
                      std::string(info->trid.subnqn) + "|" +
                      std::to_string(static_cast<int>(info->trid.trtype));

    VLOG(1) << "traddr:" << info->trid.traddr
            << "trsvcid:" << info->trid.trsvcid << "ns:" << info->ns
            << "subnqn:" << info->trid.subnqn
            << "trtype:" << info->trid.trtype;

    return 0;
}

int SpdkWrapper::ConnectController(const struct spdk_nvme_transport_id *trid,
                                   ctrlr_info *info) {
    auto probe_cb = [](void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                       struct spdk_nvme_ctrlr_opts *opts) -> bool {
        ApplyCtrlrOptsFromEnv(opts);
        LOG(INFO) << "Attaching to " << trid->traddr << " " << trid->subnqn;
        return true;
    };

    auto attach_cb = [](void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                        struct spdk_nvme_ctrlr *ctrlr,
                        const struct spdk_nvme_ctrlr_opts *opts) {
        LOG(INFO) << "Attached to " << trid->traddr << " " << trid->subnqn;
        ctrlr_info *info = (ctrlr_info *)cb_ctx;
        info->ctrlr = ctrlr;
    };

    const int rc =
        spdk_nvme_probe(trid, (void *)info, probe_cb, attach_cb, NULL);
    if (rc != 0) {
        return rc;
    }
    // Some transport attach failures complete the probe without invoking the
    // attach callback. Treat a missing controller as a failed connection even
    // when the top-level probe return code is zero.
    return info->ctrlr != nullptr ? 0 : -ENODEV;
}

nof_seg_handle *SpdkWrapper::OpenNofSegment(const std::string &tr_str) {
    return OpenNofSegmentWithLaneCount(tr_str, GetNofQpairsPerSegment());
}

nof_seg_handle *SpdkWrapper::OpenNofSegmentWithLaneCount(
    const std::string &tr_str, uint32_t requested_lane_count) {
    tr_info tr;
    int ret = ParseTransPortStr(tr_str, &tr);
    if (ret != 0) {
        return nullptr;
    }

    ctrlr_info *info = nullptr;
    {
        std::lock_guard<std::mutex> lock(ctrlrs_mutex);
        auto it = connected_ctrlrs.find(tr.ctrlr_key);
        if (it == connected_ctrlrs.end()) {
            auto new_info = std::make_unique<ctrlr_info>();
            info = new_info.get();

            ret = ConnectController(&tr.trid, info);
            if (ret != 0 || info->ctrlr == nullptr) {
                LOG(ERROR) << "Failed to attach NoF controller endpoint="
                           << tr_str << " rc=" << ret
                           << " ctrlr=" << info->ctrlr;
                return nullptr;
            }

            connected_ctrlrs[tr.ctrlr_key] = std::move(new_info);
        } else {
            info = it->second.get();
        }
    }

    // spdk_nvme_probe() may finish without invoking attach_cb when the remote
    // target rejects controller creation. Never pass that missing controller
    // into SPDK namespace helpers; they require a non-null controller.
    if (info == nullptr || info->ctrlr == nullptr) {
        LOG(ERROR) << "NoF controller is unavailable endpoint=" << tr_str;
        return nullptr;
    }

    nof_seg_handle *seg_handle = nullptr;
    struct spdk_nvme_qpair *probe_qpair = nullptr;
    struct spdk_nvme_ns *ns = nullptr;
    {
        auto &ns_seg = info->ns_seg;
        std::lock_guard<std::mutex> lock(info->ns_mutex);
        auto ns_it = ns_seg.find(tr.ns);
        if (ns_it != ns_seg.end()) {
            seg_handle = ns_it->second.get();
            std::lock_guard<std::mutex> lanes_lock(seg_handle->lanes_mutex);
            // A probe-only segment may acquire business lanes once. After a
            // partial allocation, preserve the published lane count as well:
            // workers already use that count for binding and QoS budgets.
            if (requested_lane_count == 0 ||
                seg_handle->io_lanes_initialized) {
                return seg_handle;
            }

            // A heartbeat can create a probe-only segment before a data
            // client opens the same namespace in the same process. Add the
            // missing business lanes without replacing the probe qpair.
            while (seg_handle->io_lanes.size() < requested_lane_count) {
                auto lane = std::make_unique<nof_io_lane>();
                lane->qpair =
                    spdk_nvme_ctrlr_alloc_io_qpair(info->ctrlr, nullptr, 0);
                if (!lane->qpair) {
                    if (seg_handle->io_lanes.empty()) {
                        LOG(ERROR)
                            << "alloc SPDK NoF I/O qpair failed lane=0"
                            << " requested_lanes=" << requested_lane_count;
                        return nullptr;
                    }
                    LOG(WARNING)
                        << "SPDK controller could not allocate all requested "
                           "NoF I/O qpairs; continuing with lanes="
                        << seg_handle->io_lanes.size()
                        << " requested_lanes=" << requested_lane_count;
                    break;
                }
                lane->index =
                    static_cast<uint32_t>(seg_handle->io_lanes.size());
                lane->segment = seg_handle;
                seg_handle->io_lanes.push_back(std::move(lane));
            }
            seg_handle->io_lanes_initialized = true;
            LOG(INFO) << "Expanded NoF segment ns=" << tr.ns
                      << " io_qpairs=" << seg_handle->io_lanes.size()
                      << " requested_io_qpairs=" << requested_lane_count;
            return seg_handle;
        }

        if (spdk_nvme_ctrlr_is_active_ns(info->ctrlr, tr.ns)) {
            ns = spdk_nvme_ctrlr_get_ns(info->ctrlr, tr.ns);
        } else {
            LOG(ERROR) << "spdk_nvme_ctrlr_is_active_ns failed";
            return nullptr;
        }

        if (requested_lane_count > 1) {
            LOG(INFO) << "NoF multi-qpair mode enabled: requested_lanes="
                      << requested_lane_count
                      << "; completion order is independent across lanes";
        }

        // Reserve the heartbeat qpair first. Business I/O must never share a
        // qpair with the heartbeat thread because submit/poll are not safe
        // across those threads.
        probe_qpair = spdk_nvme_ctrlr_alloc_io_qpair(info->ctrlr, nullptr, 0);
        if (!probe_qpair) {
            LOG(ERROR) << "alloc probe spdk_nvme_qpair failed";
            return nullptr;
        }

        std::vector<std::unique_ptr<nof_io_lane>> io_lanes;
        io_lanes.reserve(requested_lane_count);
        for (uint32_t lane_index = 0; lane_index < requested_lane_count;
             ++lane_index) {
            auto lane = std::make_unique<nof_io_lane>();
            lane->qpair =
                spdk_nvme_ctrlr_alloc_io_qpair(info->ctrlr, nullptr, 0);
            if (!lane->qpair) {
                if (io_lanes.empty()) {
                    LOG(ERROR) << "alloc SPDK NoF I/O qpair failed lane="
                               << lane_index << " requested_lanes="
                               << requested_lane_count;
                    spdk_nvme_ctrlr_free_io_qpair(probe_qpair);
                    probe_qpair = nullptr;
                    return nullptr;
                }
                LOG(WARNING)
                    << "SPDK controller could not allocate all requested NoF "
                       "I/O qpairs; continuing with lanes="
                    << io_lanes.size()
                    << " requested_lanes=" << requested_lane_count;
                break;
            }
            lane->index = lane_index;
            io_lanes.push_back(std::move(lane));
        }

        auto new_seg = std::make_unique<nof_seg_handle>();
        new_seg->ns = ns;
        new_seg->io_lanes = std::move(io_lanes);
        new_seg->io_lanes_initialized = requested_lane_count != 0;
        new_seg->probe_qpair = probe_qpair;
        for (auto& lane : new_seg->io_lanes) {
            lane->segment = new_seg.get();
        }
        seg_handle = new_seg.get();
        ns_seg[tr.ns] = std::move(new_seg);
        LOG(INFO) << "Opened NoF segment ns=" << tr.ns
                  << " io_qpairs=" << seg_handle->io_lanes.size()
                  << " requested_io_qpairs=" << requested_lane_count;
    }

    return seg_handle;
}

uint32_t SpdkWrapper::GetBlockSize(const nof_seg_handle *seg_handle) {
    if (!seg_handle || !seg_handle->ns) {
        return INVALID_BLOCK_SIZE;
    }

    return spdk_nvme_ns_get_sector_size(seg_handle->ns);
}

uint32_t SpdkWrapper::GetBlockSize(const nof_io_lane *lane) {
    return lane == nullptr ? INVALID_BLOCK_SIZE : GetBlockSize(lane->segment);
}

int SpdkWrapper::SubmitRequestOnQpair(struct spdk_nvme_qpair *qpair,
                                      struct spdk_nvme_ns *ns, void *ptr,
                                      uint64_t lba, uint32_t lba_count, int op,
                                      spdk_nvme_cmd_cb cb_fn, void *cb_ctx,
                                      spdk_nvme_ns_cmd_ext_io_opts *io_opts) {
    if (!qpair || !ns || !ptr || !lba_count) {
        return -1;
    }

    if (op == kSpdkNofOpRead) {
        if (io_opts != nullptr) {
            // Modified By Yida (v3): 带 memory_domain 的 ext 提交，URMA 依据
            // domain 选择 HOST/CUDA 等内存注册方式（GPU Direct 必需）
            return spdk_nvme_ns_cmd_read_ext(ns, qpair, ptr, lba, lba_count,
                                             cb_fn, cb_ctx, io_opts);
        }
        return spdk_nvme_ns_cmd_read(ns, qpair, ptr, lba, lba_count, cb_fn,
                                     cb_ctx, 0);
    } else if (op == kSpdkNofOpWrite) {
        if (io_opts != nullptr) {
            return spdk_nvme_ns_cmd_write_ext(ns, qpair, ptr, lba, lba_count,
                                              cb_fn, cb_ctx, io_opts);
        }
        return spdk_nvme_ns_cmd_write(ns, qpair, ptr, lba, lba_count, cb_fn,
                                      cb_ctx, 0);
    }
    return -1;
}

int SpdkWrapper::SubmitRequest(const nof_seg_handle *seg_handle, void *ptr,
                               uint64_t lba, uint32_t lba_count, int op,
                               spdk_nvme_cmd_cb cb_fn, void *cb_ctx,
                               spdk_nvme_ns_cmd_ext_io_opts *io_opts) {
    if (!seg_handle || seg_handle->io_lanes.empty()) {
        return -1;
    }

    // Compatibility entry point: direct callers use lane 0. The worker pool
    // uses SubmitRequestOnLane() after assigning each task to a lane.
    return SubmitRequestOnLane(seg_handle->io_lanes[0].get(), ptr, lba,
                               lba_count, op, cb_fn, cb_ctx, io_opts);
}

int SpdkWrapper::SubmitRequestOnLane(
    const nof_io_lane *lane, void *ptr, uint64_t lba, uint32_t lba_count,
    int op, spdk_nvme_cmd_cb cb_fn, void *cb_ctx,
    spdk_nvme_ns_cmd_ext_io_opts *io_opts) {
    if (!lane || !lane->segment || !lane->qpair) {
        return -1;
    }
    return SubmitRequestOnQpair(lane->qpair, lane->segment->ns, ptr, lba,
                                lba_count, op, cb_fn, cb_ctx, io_opts);
}

SpdkWrapper::ProbeBuffer *SpdkWrapper::GetOrCreateProbeBuffer(
    const std::string &tr_str, uint32_t block_size, std::string *error_reason) {
    std::lock_guard<std::mutex> lock(probe_buffers_mutex_);
    auto &probe_buffer = probe_buffers_[tr_str];
    if (!probe_buffer) {
        probe_buffer = std::make_unique<ProbeBuffer>();
    }

    if (probe_buffer->ptr != nullptr && probe_buffer->size == block_size) {
        return probe_buffer.get();
    }

    if (probe_buffer->ptr != nullptr) {
        spdk_free(probe_buffer->ptr);
        probe_buffer->ptr = nullptr;
        probe_buffer->size = 0;
    }

    probe_buffer->ptr =
        spdk_zmalloc(block_size, 0x1000, nullptr, -1, SPDK_MALLOC_DMA);
    if (!probe_buffer->ptr) {
        if (error_reason) {
            *error_reason = "alloc_fail";
        }
        return nullptr;
    }
    probe_buffer->size = block_size;
    return probe_buffer.get();
}

bool SpdkWrapper::ProbeNofSegment(const std::string &tr_str,
                                  uint32_t timeout_ms,
                                  std::string *error_reason) {
    if (!InitializeEnv()) {
        if (error_reason) {
            *error_reason = "spdk_env_init_fail";
        }
        return false;
    }

    // Master heartbeat only submits one probe read. Allocating the default
    // data-plane lanes here creates unnecessary target queues and can amplify
    // stale-controller pressure during register/unregister operations.
    nof_seg_handle *seg_handle = OpenNofSegmentWithLaneCount(tr_str, 0);
    if (!seg_handle) {
        if (error_reason) {
            *error_reason = "open_fail";
        }
        return false;
    }

    // Modified By Yida (v3): heartbeat 全程独占 probe_qpair，probe_mutex
    // 串行化并发 probe；不触碰业务 io_qpair
    std::lock_guard<std::mutex> probe_lock(seg_handle->probe_mutex);

    uint32_t block_size = GetBlockSize(seg_handle);
    if (block_size == INVALID_BLOCK_SIZE || block_size == 0) {
        if (error_reason) {
            *error_reason = "invalid_block_size";
        }
        return false;
    }

    ProbeBuffer *probe_buffer =
        GetOrCreateProbeBuffer(tr_str, block_size, error_reason);
    if (!probe_buffer || !probe_buffer->ptr) {
        return false;
    }

    ProbeRequestContext *probe_ctx = AcquireProbeRequestContext();
    int ret = SubmitRequestOnQpair(
        seg_handle->probe_qpair, seg_handle->ns, probe_buffer->ptr, 0, 1,
        kSpdkNofOpRead, ProbeReadComplete, probe_ctx, nullptr);
    if (ret != 0) {
        RecycleProbeRequestContext(probe_ctx);
        if (error_reason) {
            *error_reason = "submit_fail";
        }
        return false;
    }

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (!probe_ctx->done.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        // 只 poll probe_qpair，不 poll 业务 qpair
        spdk_nvme_qpair_process_completions(seg_handle->probe_qpair, 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    bool ok = probe_ctx->done.load(std::memory_order_acquire) &&
              probe_ctx->success.load(std::memory_order_acquire);
    if (!ok && error_reason) {
        if (!probe_ctx->done.load(std::memory_order_acquire)) {
            *error_reason = "completion_timeout";
        } else {
            std::lock_guard<std::mutex> lock(probe_ctx->error_mutex);
            *error_reason = probe_ctx->error_reason.empty()
                                ? "completion_error"
                                : probe_ctx->error_reason;
        }
    }

    return ok;
}

}  // namespace mooncake
