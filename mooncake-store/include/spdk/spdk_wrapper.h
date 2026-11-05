#pragma once
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <stack>
#include <string>
#include <thread>
#include <vector>
#include <spdk/env.h>
#include <spdk/nvme.h>
#include <spdk/version.h>

namespace mooncake {

#define INVALID_BLOCK_SIZE 0xFFFFFFFF

constexpr int kSpdkNofOpRead = 0;
constexpr int kSpdkNofOpWrite = 1;
constexpr int kSpdkNofOpNum = 2;

// Modified By Yida (v3): SPDK 2026-04 起（v26.05+ master）将 completion 状态
// 转字符串的 API 改为 _ext 并增加 opc 参数，旧单参 API 在 26.x 被移除；
// v23.01 等旧版本只有单参形式。按版本选择，兼容两种 SPDK。
inline const char *NofCplStatusString(const struct spdk_nvme_cpl *cpl,
                                      uint8_t opc) {
#if SPDK_VERSION_MAJOR > 26 || \
    (SPDK_VERSION_MAJOR == 26 && SPDK_VERSION_MINOR >= 5)
    return spdk_nvme_cpl_get_status_string_ext(&cpl->status, opc);
#else
    (void)opc;
    return spdk_nvme_cpl_get_status_string(&cpl->status);
#endif
}

struct nof_seg_handle;
struct nof_io_lane;
struct tr_info;
struct ctrlr_info;

class SpdkWrapper {
   public:
    SpdkWrapper(const SpdkWrapper &) = delete;
    SpdkWrapper &operator=(const SpdkWrapper &) = delete;

    static SpdkWrapper &GetInstance();

    bool InitializeEnv();

    void Cleanup();

    void *Alloc(size_t size, size_t align, int socket_id = -1);

    void Free(void *ptr);

    int64_t NvmePollProcessCompletion(nof_seg_handle *seg,
                                      uint32_t complete_per_seg);

    size_t GetIoLaneCount(const nof_seg_handle *seg_handle) const;

    nof_io_lane *GetIoLane(nof_seg_handle *seg_handle,
                           size_t lane_index) const;

    uint32_t GetIoLaneIndex(const nof_io_lane *lane) const;

    uint16_t GetIoLaneQpairId(const nof_io_lane *lane) const;

    // A segment's business lanes have one dispatcher owner. Release only
    // after its workers have drained all submitted requests.
    bool ClaimIoLanes(nof_seg_handle *seg, const void *owner);
    void ReleaseIoLanes(nof_seg_handle *seg, const void *owner);
    bool IsIoLaneHealthy(const nof_io_lane *lane) const;
    void MarkIoLaneFailed(nof_io_lane *lane);

    int64_t PollIoLane(nof_io_lane *lane, uint32_t max_completions);

    // Called by the owning worker after polling has returned. SPDK aborts
    // requests and tears down DMA resources before callers can reuse buffers.
    void DrainFailedNofSegment(nof_seg_handle *seg);

    void DrainFailedIoLane(nof_io_lane *lane);

    /** @brief Open a NoF segment. */
    nof_seg_handle *OpenNofSegment(const std::string &tr_str);

    uint32_t GetBlockSize(const nof_seg_handle *seg_handle);

    uint32_t GetBlockSize(const nof_io_lane *lane);

    int SubmitRequest(const nof_seg_handle *seg_handle, void *ptr, uint64_t lba,
                      uint32_t lba_count, int op, spdk_nvme_cmd_cb cb_fn,
                      void *cb_ctx,
                      spdk_nvme_ns_cmd_ext_io_opts *io_opts = nullptr);

    int SubmitRequestOnLane(
        const nof_io_lane *lane, void *ptr, uint64_t lba, uint32_t lba_count,
        int op, spdk_nvme_cmd_cb cb_fn, void *cb_ctx,
        spdk_nvme_ns_cmd_ext_io_opts *io_opts = nullptr);

    bool ProbeNofSegment(const std::string &tr_str, uint32_t timeout_ms,
                         std::string *error_reason = nullptr);

   private:
    struct ProbeBuffer {
        void *ptr{nullptr};
        uint32_t size{0};

        ProbeBuffer() = default;
        ProbeBuffer(const ProbeBuffer &) = delete;
        ProbeBuffer &operator=(const ProbeBuffer &) = delete;
        ProbeBuffer(ProbeBuffer &&) = delete;
        ProbeBuffer &operator=(ProbeBuffer &&) = delete;
    };

    struct ProbeRequestContext {
        std::atomic<bool> done{false};
        std::atomic<bool> success{false};
        std::mutex error_mutex;
        std::string error_reason;
        SpdkWrapper *owner{nullptr};

        void Reset(SpdkWrapper *wrapper) {
            std::lock_guard<std::mutex> lock(error_mutex);
            owner = wrapper;
            done.store(false, std::memory_order_release);
            success.store(false, std::memory_order_release);
            error_reason.clear();
        }
    };

    explicit SpdkWrapper();
    ~SpdkWrapper();

    nof_seg_handle *OpenNofSegmentWithLaneCount(
        const std::string &tr_str, uint32_t requested_lane_count);

    int ParseTransPortStr(const std::string &tr_str, tr_info *info);
    int ConnectController(const struct spdk_nvme_transport_id *trid,
                          ctrlr_info *info);
    /* Modified By Yida (v3): qpair 级提交入口，业务(io_qpair)与 heartbeat
     * (probe_qpair)共用实现；io_opts 非 NULL 时走 spdk_nvme_ns_cmd_*_ext。 */
    int SubmitRequestOnQpair(struct spdk_nvme_qpair *qpair,
                             struct spdk_nvme_ns *ns, void *ptr, uint64_t lba,
                             uint32_t lba_count, int op, spdk_nvme_cmd_cb cb_fn,
                             void *cb_ctx,
                             spdk_nvme_ns_cmd_ext_io_opts *io_opts);
    ProbeBuffer *GetOrCreateProbeBuffer(const std::string &tr_str,
                                        uint32_t block_size,
                                        std::string *error_reason);
    ProbeRequestContext *AcquireProbeRequestContext();
    void RecycleProbeRequestContext(ProbeRequestContext *ctx);
    void ReplenishProbeRequestContextPoolLocked(size_t count);
    void AdminPollThreadFunc();
    static void ProbeReadComplete(void *ctx, const struct spdk_nvme_cpl *cpl);

    std::atomic<bool> initialized{false};
    std::mutex init_mutex;
    std::map<std::string, std::unique_ptr<ctrlr_info>> connected_ctrlrs;
    std::mutex ctrlrs_mutex;
    std::map<std::string, std::unique_ptr<ProbeBuffer>> probe_buffers_;
    std::mutex probe_buffers_mutex_;
    std::vector<std::unique_ptr<ProbeRequestContext>> probe_request_contexts_;
    std::stack<ProbeRequestContext *> probe_request_context_pool_;
    std::mutex probe_request_context_pool_mutex_;
    std::atomic<bool> admin_poll_running_{false};
    std::thread admin_poll_thread_;
    std::mutex admin_poll_mutex_;
    std::condition_variable admin_poll_cv_;
};

}  // namespace mooncake
