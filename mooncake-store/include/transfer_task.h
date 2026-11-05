#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <queue>
#include <set>
#include <stack>
#include <string>
#include <thread>
#include <vector>

#include "transfer_engine.h"
#include "types.h"
#include "replica.h"
#include "vsegment/vsegment_transfer.h"
#include "rpc_types.h"
#include "storage_backend.h"
#include "client_metric.h"
#include "operation_options.h"
#ifdef USE_NOF
#include "spdk/spdk_wrapper.h"
#endif
// Modified By Yida (v3): GPU Direct 内存识别结果随 task 传递
#ifdef USE_NOF_URMA
#include "spdk/nof_memory_domain.h"
#endif

namespace mooncake {

/**
 * @brief Transfer strategy enumeration
 */
enum class TransferStrategy {
    LOCAL_MEMCPY = 0,     // Local memory copy using memcpy
    TRANSFER_ENGINE = 1,  // Remote transfer using transfer engine
    FILE_READ = 2,        // File read operation
    EMPTY = 3,
    SPDK_NVMF = 4  // Spdk nvmf operation
};

// Per-object NoF timing, captured for sampled high-frequency traces or while
// the opt-in all-task slow-path trace is enabled.
// The first three phases form a non-overlapping decomposition of total_us:
// queue_to_first_submit_us + first_to_last_submit_us +
// last_submit_to_complete_us ~= total_us.  Completion timings overlap the
// submit span because completions may arrive while later sub-I/Os are still
// being admitted to the qpair.
struct NofTransferTiming {
    int32_t worker_index = -1;
    uint32_t lane = 0;
    uint16_t qpair_id = 0;
    uint32_t sub_io_count = 0;
    uint64_t lane_task_sequence = 0;
    uint64_t lane_completion_sequence = 0;
    uint32_t qos_tasks_ahead_at_enqueue = 0;
    int64_t dispatch_queue_us = 0;
    int64_t worker_setup_us = 0;
    int64_t qos_fifo_wait_us = 0;
    // Decompose qos_fifo_wait_us by the lane occupancy observed while this
    // task was behind another task.  These fields distinguish backpressure
    // from task-level head-of-line blocking with unused credit.
    int64_t qos_fifo_full_wait_us = 0;
    int64_t qos_fifo_partial_wait_us = 0;
    int64_t qos_fifo_zero_wait_us = 0;
    int64_t head_to_first_submit_us = 0;
    int64_t credit_wait_us = 0;
    // Credit wait accumulated while at the lane head before the first submit
    // succeeded; credit_wait_us - first_credit_wait_us is the post-first-
    // submit stall.  These values overlap head_to_first_submit_us and
    // first_to_last_submit_us respectively; they are causal annotations, not
    // additional non-overlapping phases.
    int64_t first_credit_wait_us = 0;
    int64_t qos_queue_us = 0;
    int64_t queue_to_first_submit_us = 0;
    int64_t first_to_last_submit_us = 0;
    int64_t last_submit_to_complete_us = 0;
    int64_t first_submit_to_first_completion_us = 0;
    int64_t first_completion_minus_last_submit_us = 0;
    int64_t first_completion_to_complete_us = 0;
    int64_t total_us = 0;
    int64_t complete_to_observe_us = 0;
    // Absolute steady-clock timestamps make slow tasks orderable on one host
    // without relying on the asynchronous logger's wall-clock prefix.
    int64_t queued_mono_ns = 0;
    int64_t first_submit_mono_ns = 0;
    int64_t last_submit_mono_ns = 0;
    int64_t completed_mono_ns = 0;
    // The slowest sub-I/O splits aggregate drain into an initiator polling
    // gap versus a downstream completion gap. CID/LBA allow target-side
    // correlation when corresponding target tracing is enabled.
    int64_t max_sub_io_latency_us = 0;
    uint32_t max_sub_io_index = 0;
    uint16_t max_sub_io_cid = 0;
    uint64_t max_sub_io_lba = 0;
    uint32_t max_sub_io_blocks = 0;
    int64_t max_sub_io_submit_mono_ns = 0;
    int64_t max_sub_io_complete_mono_ns = 0;
    int64_t max_completion_gap_us = 0;
    int64_t max_completion_gap_begin_mono_ns = 0;
    int64_t max_completion_gap_end_mono_ns = 0;
    int64_t max_poll_return_gap_us = 0;
    int64_t max_poll_return_gap_begin_mono_ns = 0;
    int64_t max_poll_return_gap_end_mono_ns = 0;
    uint64_t poll_calls_during_task = 0;
    uint64_t empty_polls_during_task = 0;
    uint64_t polled_completions_during_task = 0;
};

#ifdef USE_NOF
// Zero disables the opt-in all-task slow-path trace. Kept as a function so
// the submitter and client-side observer share one parsed value.
int64_t GetSpdkNofSlowTraceThresholdUs();
#endif

/**
 * @brief Stream operator for TransferStrategy
 */
inline std::ostream& operator<<(std::ostream& os,
                                const TransferStrategy& strategy) noexcept {
    switch (strategy) {
        case TransferStrategy::LOCAL_MEMCPY:
            return os << "LOCAL_MEMCPY";
        case TransferStrategy::TRANSFER_ENGINE:
            return os << "TRANSFER_ENGINE";
        case TransferStrategy::SPDK_NVMF:
            return os << "SPDK_NVMF";
        case TransferStrategy::FILE_READ:
            return os << "FILE_READ";
        case TransferStrategy::EMPTY:
            return os << "EMPTY";
        default:
            return os << "UNKNOWN";
    }
}

/**
 * @brief Abstract base class for operation state management
 *
 * This class encapsulates the common state and behavior for async transfer
 * operations. Derived classes implement strategy-specific waiting logic.
 */
class OperationState {
   public:
    explicit OperationState(uint64_t trace_id = 0) : trace_id_(trace_id) {}
    virtual ~OperationState() = default;

    // Non-copyable, non-movable
    OperationState(const OperationState&) = delete;
    OperationState& operator=(const OperationState&) = delete;
    OperationState(OperationState&&) = delete;
    OperationState& operator=(OperationState&&) = delete;

    /**
     * @brief Check if the operation has completed
     */
    virtual bool is_completed() = 0;

    /**
     * @brief Get the operation result. Make sure to call is_completed() first.
     */
    ErrorCode get_result() const {  // lock mutex
        std::lock_guard<std::mutex> lock(mutex_);
        assert(result_.has_value() &&
               "get_result() called on an incomplete or failed-to-set "
               "operation state.");
        return result_.value_or(ErrorCode::INVALID_PARAMS);
    }

    /**
     * @brief Get the transfer strategy
     */
    virtual TransferStrategy get_strategy() const = 0;

    /**
     * @brief Wait for the operation to complete (strategy-specific
     * implementation)
     */
    virtual void wait_for_completion() = 0;

    // Available only for sampled SPDK NoF operations after completion.
    virtual std::optional<NofTransferTiming> nof_timing() const {
        return std::nullopt;
    }

    uint64_t trace_id() const { return trace_id_; }

   protected:
    std::optional<ErrorCode> result_ = std::nullopt;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    uint64_t trace_id_ = 0;
};

/**
 * @brief Operation state for local memcpy transfers
 */
class EmptyOperationState : public OperationState {
   public:
    bool is_completed() override { return true; }

    void wait_for_completion() override {}

    TransferStrategy get_strategy() const override {
        return TransferStrategy::EMPTY;
    }
};

/**
 * @brief Operation state for local memcpy transfers
 */
class MemcpyOperationState : public OperationState {
   public:
    explicit MemcpyOperationState(uint64_t trace_id = 0)
        : OperationState(trace_id) {}

    bool is_completed() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_.has_value();
    }

    void set_completed(ErrorCode error_code) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            assert(!result_.has_value());
            result_.emplace(error_code);
        }
        cv_.notify_all();
    }

    void wait_for_completion() override {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return result_.has_value(); });
    }

    TransferStrategy get_strategy() const override {
        return TransferStrategy::LOCAL_MEMCPY;
    }
};

/**
 * @brief Operation state for local memcpy transfers
 */
class SpdkNofOperationState : public OperationState {
   public:
    explicit SpdkNofOperationState(uint64_t trace_id = 0,
                                   bool track_timing = false)
        : OperationState(trace_id), track_timing_(track_timing) {}

    bool is_completed() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_.has_value();
    }

    void set_completed(
        ErrorCode error_code,
        std::optional<NofTransferTiming> timing = std::nullopt) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            assert(!result_.has_value());
            if (track_timing_ && timing.has_value()) {
                timing_ = std::make_unique<NofTransferTiming>(
                    std::move(timing.value()));
                completed_at_ = std::chrono::steady_clock::now();
            }
            result_.emplace(error_code);
        }
        cv_.notify_all();
    }

    void wait_for_completion() override {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return result_.has_value(); });
    }

    TransferStrategy get_strategy() const override {
        return TransferStrategy::SPDK_NVMF;
    }

    bool track_timing() const { return track_timing_; }

    std::optional<NofTransferTiming> nof_timing() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!result_.has_value() || !timing_) {
            return std::nullopt;
        }
        auto timing = *timing_;
        timing.complete_to_observe_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - completed_at_)
                .count();
        return timing;
    }

   private:
    bool track_timing_ = false;
    std::unique_ptr<NofTransferTiming> timing_;
    std::chrono::steady_clock::time_point completed_at_{};
};

class FilereadOperationState : public OperationState {
   public:
    explicit FilereadOperationState(uint64_t trace_id = 0)
        : OperationState(trace_id) {}

    bool is_completed() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_.has_value();
    }

    void set_completed(ErrorCode error_code) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            assert(!result_.has_value());
            result_.emplace(error_code);
        }
        cv_.notify_all();
    }

    void wait_for_completion() override {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return result_.has_value(); });
    }

    TransferStrategy get_strategy() const override {
        return TransferStrategy::FILE_READ;
    }
};

/**
 * @brief Operation state for transfer engine operations
 */
class TransferEngineOperationState : public OperationState {
   public:
    TransferEngineOperationState(TransferEngine& engine, BatchID batch_id,
                                 size_t batch_size, uint64_t trace_id = 0)
        : OperationState(trace_id),
          engine_(engine),
          batch_id_(batch_id),
          batch_size_(batch_size),
          start_ts_(getCurrentTimeInMilli()) {}

    ~TransferEngineOperationState() { engine_.freeBatchID(batch_id_); }

    bool is_completed() override;

    void wait_for_completion() override;

    TransferStrategy get_strategy() const override {
        return TransferStrategy::TRANSFER_ENGINE;
    }

   private:
    /**
     * @brief Check the current completion status of the task, make sure to lock
     * the mutex before calling this function.
     * Updates the internal state and returns true if the task is completed.
     */
    void check_task_status();

    void set_result_internal(ErrorCode error_code);

    TransferEngine& engine_;
    BatchID batch_id_;
    size_t batch_size_;
    const int64_t start_ts_;
};

/**
 * @brief Represents the future result of an asynchronous transfer operation
 *
 * This class provides a std::future-like interface for asynchronous transfer
 * operations. Users can check completion status, wait for results, or get the
 * final error code.
 */
class TransferFuture {
   public:
    explicit TransferFuture(std::shared_ptr<OperationState> state);

    // Non-copyable but movable
    TransferFuture(const TransferFuture&) = delete;
    TransferFuture& operator=(const TransferFuture&) = delete;
    TransferFuture(TransferFuture&&) = default;
    TransferFuture& operator=(TransferFuture&&) = default;

    /**
     * @brief Check if the operation has completed (non-blocking)
     * @return true if the operation is finished, false otherwise
     */
    bool isReady() const;

    /**
     * @brief Wait for the operation to complete (blocking)
     * @return ErrorCode indicating success or failure
     */
    ErrorCode wait();

    /**
     * @brief Get the result, waiting if necessary (blocking)
     * @return ErrorCode indicating success or failure
     */
    ErrorCode get();

    // Returns per-object NoF phase timings after get()/wait() completes.
    std::optional<NofTransferTiming> nofTiming() const;

    /**
     * @brief Get the transfer strategy used by this operation
     * @return TransferStrategy enum value
     */
    TransferStrategy strategy() const;

   private:
    std::shared_ptr<OperationState> state_;
};

/**
 * @brief Memory copy operation descriptor
 */
struct MemcpyOperation {
    void* dest;
    const void* src;
    size_t size;

    MemcpyOperation(void* d, const void* s, size_t sz)
        : dest(d), src(s), size(sz) {}
};

/**
 * @brief Memcpy task for async execution
 */
struct MemcpyTask {
    std::vector<MemcpyOperation> operations;
    std::shared_ptr<MemcpyOperationState> state;
    uint64_t trace_id;
    std::chrono::steady_clock::time_point submitted_at;
    size_t queued_ahead = 0;

    MemcpyTask(std::vector<MemcpyOperation> ops,
               std::shared_ptr<MemcpyOperationState> s, uint64_t trace)
        : operations(std::move(ops)),
          state(std::move(s)),
          trace_id(trace),
          submitted_at(std::chrono::steady_clock::now()) {}
};

/**
 * @brief Thread pool for asynchronous memcpy operations
 *
 * This class manages a bounded worker pool for accelerator copies and for
 * host copies when MC_STORE_MEMCPY_INLINE is disabled.
 * MC_STORE_MEMCPY_WORKERS controls the worker count.
 */
class MemcpyWorkerPool {
   public:
    explicit MemcpyWorkerPool();
    ~MemcpyWorkerPool();

    // Non-copyable, non-movable
    MemcpyWorkerPool(const MemcpyWorkerPool&) = delete;
    MemcpyWorkerPool& operator=(const MemcpyWorkerPool&) = delete;
    MemcpyWorkerPool(MemcpyWorkerPool&&) = delete;
    MemcpyWorkerPool& operator=(MemcpyWorkerPool&&) = delete;

    /**
     * @brief Submit a memcpy task for async execution
     * @param task The memcpy task to execute
     */
    void submitTask(MemcpyTask task);

   private:
    void workerThread();

    std::vector<std::thread> workers_;
    std::queue<MemcpyTask> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::atomic<bool> shutdown_;
};

#ifdef USE_NOF
// struct SpdkNofSubTask;
struct SpdkNofQos;

struct SpdkNofTaskTiming {
    std::chrono::steady_clock::time_point worker_dequeued_at;
    std::chrono::steady_clock::time_point qos_enqueued_at;
    std::chrono::steady_clock::time_point qos_head_at;
    std::chrono::steady_clock::time_point first_submit_at;
    std::chrono::steady_clock::time_point last_submit_at;
    std::chrono::steady_clock::time_point first_completion_at;
    std::chrono::steady_clock::time_point previous_completion_at;
    std::chrono::steady_clock::time_point credit_wait_started_at;
    uint64_t lane_task_sequence = 0;
    uint64_t poll_calls_at_first_submit = 0;
    uint64_t empty_polls_at_first_submit = 0;
    uint64_t polled_completions_at_first_submit = 0;
    int64_t max_sub_io_latency_us = 0;
    uint32_t max_sub_io_index = 0;
    uint16_t max_sub_io_cid = 0;
    uint64_t max_sub_io_lba = 0;
    uint32_t max_sub_io_blocks = 0;
    int64_t max_sub_io_submit_mono_ns = 0;
    int64_t max_sub_io_complete_mono_ns = 0;
    int64_t max_completion_gap_us = 0;
    int64_t max_completion_gap_begin_mono_ns = 0;
    int64_t max_completion_gap_end_mono_ns = 0;
    int64_t max_poll_return_gap_us = 0;
    int64_t max_poll_return_gap_begin_mono_ns = 0;
    int64_t max_poll_return_gap_end_mono_ns = 0;
    uint64_t qos_full_pending_at_enqueue_us = 0;
    uint64_t qos_partial_pending_at_enqueue_us = 0;
    uint64_t qos_zero_pending_at_enqueue_us = 0;
    int64_t credit_wait_us = 0;
    int64_t qos_fifo_full_wait_us = 0;
    int64_t qos_fifo_partial_wait_us = 0;
    int64_t qos_fifo_zero_wait_us = 0;
    // Snapshot of credit_wait_us taken when the first submit succeeds.
    int64_t first_credit_wait_us = 0;
    bool credit_wait_active = false;
    uint32_t qos_tasks_ahead_at_enqueue = 0;
    uint32_t submitted_sub_io_count = 0;
    uint32_t completed_sub_io_count = 0;
};

/**
 * @brief Spdk nvmf operation descriptor
 */
struct SpdkNofTask {
    nof_seg_handle* seg_handle;
    nof_io_lane* io_lane;
    uint32_t io_lane_index;
    void* ptr;
    uint64_t lba;
    uint32_t lba_count;
    int remaining_lba;
    int outstanding_sub_io;
    int op;   // kSpdkNofOpRead or kSpdkNofOpWrite
    int idx;  // subop idx
    int worker_index;
    bool failed;
    bool on_chain;
    bool first_sub_io_submitted;
    std::chrono::steady_clock::time_point queued_at;
    std::shared_ptr<SpdkNofOperationState> state;
    std::unique_ptr<SpdkNofTaskTiming> timing;
    int64_t* io_count;
    SpdkNofQos* nof_qos;
    SpdkNofTask* nxt;
    std::set<SpdkNofTask*>* live_registry = nullptr;
// Modified By Yida (v3): buffer 类型识别结果（每个 task 解析一次，
// 所有 sub_task 共用）；host 时 type=kHost、domain 可能为 system domain
#ifdef USE_NOF_URMA
    NofBufferInfo buffer_info;
#endif

    SpdkNofTask(nof_seg_handle* handle, void* buf, uint64_t off, uint32_t len,
                int op_code, std::shared_ptr<SpdkNofOperationState> s)
        : seg_handle(handle),
          io_lane(nullptr),
          io_lane_index(0),
          ptr(buf),
          lba(off),
          lba_count(len),
          remaining_lba(lba_count),
          outstanding_sub_io(0),
          op(op_code),
          idx(0),
          worker_index(-1),
          failed(false),
          on_chain(false),
          first_sub_io_submitted(false),
          queued_at(std::chrono::steady_clock::now()),
          state(std::move(s)),
          timing(state && state->track_timing()
                     ? std::make_unique<SpdkNofTaskTiming>()
                     : nullptr),
          io_count(nullptr),
          nof_qos(nullptr),
          nxt(nullptr) {}
};

struct SpdkNofSubTask {
    SpdkNofTask* task;
    int submit_lba_count;
    std::stack<SpdkNofSubTask*>* sub_task_pool;
    bool track_latency = false;
    uint32_t sub_io_index = 0;
    uint64_t submit_lba = 0;
    std::chrono::steady_clock::time_point submitted_at;
// Modified By Yida (v3): ext I/O 选项。必须是成员而非栈变量——SPDK 要求
// 在 I/O 完成前保持有效；host 路径不使用（io_opts 以 nullptr 提交）。
#ifdef USE_NOF_URMA
    spdk_nvme_ns_cmd_ext_io_opts io_opts{};
#endif
};

constexpr int kDefaultSpdkNofSubmitChunkBytes = (1 << 17);    // 128k
constexpr int kDefaultSpdkNofInflightBytesLimit = (1 << 25);  // 32M
struct SpdkNofQos {
    int inflight_blocks[kSpdkNofOpNum];
    int blocks_per_chunk;
    int inflight_blocks_limit;
    int next_op = 0;
    SpdkNofTask* head[kSpdkNofOpNum];
    SpdkNofTask* tail[kSpdkNofOpNum];
    bool poisoned = false;
    uint64_t submit_attempts = 0;
    uint64_t submit_successes = 0;
    uint64_t submit_eagain = 0;
    uint64_t submit_enomem = 0;
    uint64_t submit_backpressure_retries = 0;
    uint64_t submit_backpressure_events = 0;
    uint64_t submit_backpressure_recoveries = 0;
    uint64_t submit_backpressure_wait_us = 0;
    bool submit_backpressured = false;
    std::chrono::steady_clock::time_point submit_backpressure_started;
    uint64_t submit_fatal_errors = 0;
    uint64_t submit_other_errors = 0;
    uint64_t completion_successes = 0;
    uint64_t completion_errors = 0;
    uint64_t completion_latency_samples = 0;
    uint64_t completion_latency_us = 0;
    uint64_t max_completion_latency_us = 0;
    uint64_t poll_calls = 0;
    uint64_t empty_polls = 0;
    uint64_t polled_completions = 0;
    uint64_t first_submit_count = 0;
    uint64_t first_submit_wait_us = 0;
    uint32_t sampled_timing_tasks = 0;
    uint64_t occupancy_total_us = 0;
    uint64_t occupancy_active_us = 0;
    uint64_t inflight_idle_us = 0;
    uint64_t inflight_partial_us = 0;
    uint64_t inflight_full_us = 0;
    uint64_t inflight_zero_with_pending_us = 0;
    uint64_t inflight_partial_with_pending_us = 0;
    uint64_t inflight_full_with_pending_us = 0;
    std::chrono::steady_clock::time_point last_occupancy_update =
        std::chrono::steady_clock::now();
    int max_inflight_blocks = 0;
    uint64_t enqueued_task_sequence = 0;
    uint64_t completed_task_sequence = 0;
    std::chrono::steady_clock::time_point last_poll_return_at =
        std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point last_progress =
        std::chrono::steady_clock::now();

    explicit SpdkNofQos(uint32_t block_size, size_t lane_count);

    bool Empty() const {
        return (head[kSpdkNofOpRead] == nullptr &&
                head[kSpdkNofOpWrite] == nullptr);
    }

    void PushTask(SpdkNofTask* task) {
        int op = task->op;
        if (head[op] == nullptr) {
            head[op] = task;
            tail[op] = task;
        } else {
            tail[op]->nxt = task;
            tail[op] = task;
        }
    }

    void PopTask(int op) {
        if (head[op]) {
            head[op] = head[op]->nxt;
        }
    }
};

/**
 * @brief Thread pool for asynchronous spdk nvmf operations
 *
 * This class manages multiple worker thread that executes spdk nvmf operations
 * asynchronously.
 */
constexpr int kDefaultSpdkNofWorkers = 4;
class SpdkNofWorkerPool {
   public:
    // Clients in one process share the dispatcher because SpdkWrapper shares
    // the underlying namespace qpairs. The first client selects its NUMA node.
    static std::shared_ptr<SpdkNofWorkerPool> Acquire(int numa_socket_id = 0);

    explicit SpdkNofWorkerPool(int numa_socket_id = 0);
    ~SpdkNofWorkerPool();

    // Non-copyable, non-movable
    SpdkNofWorkerPool(const SpdkNofWorkerPool&) = delete;
    SpdkNofWorkerPool& operator=(const SpdkNofWorkerPool&) = delete;
    SpdkNofWorkerPool(SpdkNofWorkerPool&&) = delete;
    SpdkNofWorkerPool& operator=(SpdkNofWorkerPool&&) = delete;

    /**
     * @brief Submit a spdk nvmf task for async execution
     * @param task The spdk nvmf task to execute
     */
    void submitTask(SpdkNofTask task);

   private:
    struct SegmentLaneBinding {
        std::vector<int> worker_indices;
        uint64_t next_lane = 0;
    };

    void workerThread(int work_idx);

    int worker_count_;
    int numa_socket_id_;
    std::vector<std::thread> workers_;
    std::unique_ptr<std::queue<SpdkNofTask>[]> task_queue_;
    std::unique_ptr<std::mutex[]> queue_mutex_;
    std::unique_ptr<std::condition_variable[]> queue_cv_;
    std::atomic<bool> shutdown_;
    std::mutex seg_mutex_;
    int seg_num = 0;
    std::map<nof_seg_handle*, SegmentLaneBinding> seg_to_workers_;
};
#endif

/**
 * @brief Fileread task for async execution
 */
struct FilereadTask {
    std::string file_path;
    size_t object_size;
    std::vector<Slice> slices;
    std::shared_ptr<FilereadOperationState> state;
    uint64_t trace_id;

    FilereadTask(const std::string& path, size_t size,
                 const std::vector<Slice>& slices_ref,
                 std::shared_ptr<FilereadOperationState> s, uint64_t trace)
        : file_path(path),
          object_size(size),
          slices(slices_ref),
          state(std::move(s)),
          trace_id(trace) {}
};

/**
 * @brief Thread pool for asynchronous memcpy operations
 *
 * This class manages a single worker thread that executes memcpy operations
 * asynchronously.
 */
class FilereadWorkerPool {
   public:
    explicit FilereadWorkerPool(std::shared_ptr<StorageBackend>& backend);
    ~FilereadWorkerPool();

    // Non-copyable, non-movable
    FilereadWorkerPool(const FilereadWorkerPool&) = delete;
    FilereadWorkerPool& operator=(const FilereadWorkerPool&) = delete;
    FilereadWorkerPool(FilereadWorkerPool&&) = delete;
    FilereadWorkerPool& operator=(FilereadWorkerPool&&) = delete;

    /**
     * @brief Submit a memcpy task for async execution
     * @param task The memcpy task to execute
     */
    void submitTask(FilereadTask task);

   private:
    void workerThread();

    std::vector<std::thread> workers_;
    std::queue<FilereadTask> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::atomic<bool> shutdown_;
    std::shared_ptr<StorageBackend> backend_;
};

/**
 * @brief Submitter class for asynchronous transfer operations
 *
 * This class analyzes transfer requirements, selects optimal strategies, and
 * immediately submits operations returning TransferFuture objects for result
 * tracking.
 */
class TransferSubmitter {
   public:
    explicit TransferSubmitter(TransferEngine& engine,
                               std::shared_ptr<StorageBackend>& backend,
                               const std::string& local_hostname,
                               TransferMetric* transfer_metric = nullptr,
                               int numa_socket_id = 0);

    /**
     * @brief Submit an asynchronous transfer operation
     *
     * Analyzes the transfer requirements, selects the optimal strategy,
     * and immediately submits the operation. Returns a TransferFuture
     * that can be used to track completion and get results.
     *
     * @param handles Buffer descriptors for the transfer
     * @param slices Memory slices for the transfer
     * @param op_code Transfer operation (READ/WRITE)
     * @return TransferFuture representing the async operation, or nullopt on
     * failure
     */
    std::optional<TransferFuture> submit(const Replica::Descriptor& replica,
                                         std::vector<Slice>& slices,
                                         TransferRequest::OpCode op_code,
                                         void* ptr = nullptr, size_t size = 0,
                                         const OperationOptions& options = {});

    /**
     * @brief Submit a range read: read [src_offset, src_offset+size) from
     * object into slice.ptr. Slices must total exactly `size` bytes.
     */
    std::optional<TransferFuture> submitRangeRead(
        const Replica::Descriptor& replica, std::vector<Slice>& slices,
        uint64_t src_offset);

    // Co-located memory replicas are combined into one MemcpyTask when local
    // memcpy is enabled. Mixed or remote batches use TransferEngine.
    std::optional<TransferFuture> submit_batch(
        const std::vector<Replica::Descriptor>& replicas,
        std::vector<std::vector<Slice>>& all_slices,
        TransferRequest::OpCode op_code, const OperationOptions& options = {});

    std::optional<TransferFuture> submitVSegment(
        const vsegment::VSegmentTransferPlan& plan,
        TransferRequest::OpCode op_code);

    std::optional<TransferFuture> submit_batch_get_offload_object(
        const std::string& transfer_engine_addr,
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>& pointers,
        const std::unordered_map<std::string, std::vector<Slice>>&
            batched_slices);

    // Push counterpart of submit_batch_get_offload_object, run on the data
    // owner. WRITEs each key's on-disk blob (staged in the owner's local
    // ClientBuffer at src_pointers[i]) directly into the requester's
    // destination slices, opening the requester's segment once.
    std::optional<TransferFuture> submit_batch_push_offload_object(
        const std::string& requester_te_addr,
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>& src_pointers,
        const std::vector<std::vector<OffloadDstSlice>>& dst_slices);

    /**
     * @brief Pure comparison helper: returns true iff both endpoints are
     * non-empty and identical. Exposed for unit testing of the locality
     * decision without instantiating a full TransferEngine.
     *
     * Two endpoints identify the same process only when their ip:port (or
     * full hostname) match exactly; same-host different-process pairs share
     * an IP but not a port and must NOT be treated as locally addressable.
     */
    static bool isSameProcessEndpoint(const std::string& handle_endpoint,
                                      const std::string& local_endpoint);

   private:
    TransferEngine& engine_;
    // Cached at construction: the local transport endpoint never changes for
    // the lifetime of the TransferSubmitter, so we avoid calling
    // engine_.getLocalIpAndPort() (which allocates a string) on every transfer.
    const std::string local_endpoint_;
    std::unique_ptr<MemcpyWorkerPool> memcpy_pool_;
#ifdef USE_NOF
    std::shared_ptr<SpdkNofWorkerPool> spdk_nvmf_pool_;
#endif
    std::unique_ptr<FilereadWorkerPool> fileread_pool_;
    bool memcpy_enabled_;
    const std::string local_hostname_;
    TransferMetric* transfer_metric_;
    /**
     * @brief Select the optimal transfer strategy
     */
    TransferStrategy selectStrategy(const AllocatedBuffer::Descriptor& handle,
                                    const std::vector<Slice>& slices) const;

    /**
     * @brief Check if all handles refer to local segments
     */
    bool isLocalTransfer(const AllocatedBuffer::Descriptor& handle) const;

    /**
     * @brief Validate transfer parameters
     */
    bool validateTransferParams(const AllocatedBuffer::Descriptor& handle,
                                const std::vector<Slice>& slices) const;

    /**
     * @brief Build and submit a memcpy operation
     */
    std::optional<TransferFuture> submitMemcpyOperation(
        const AllocatedBuffer::Descriptor& handle,
        const std::vector<Slice>& slices, const TransferRequest::OpCode op_code,
        uint64_t src_offset = 0);

    /**
     * @brief Execute host-only copies inline or enqueue accelerator copies
     */
    std::optional<TransferFuture> submitMemcpyOperations(
        std::vector<MemcpyOperation> operations, uint64_t trace_id);

#ifdef USE_NOF
    /**
     * @brief Submit SPDK NVMe-oF operation asynchronously
     */
    std::optional<TransferFuture> submitSpdkNofOperation(
        const AllocatedBuffer::Descriptor& handle, void* ptr, size_t size,
        const TransferRequest::OpCode op_code);
#endif

    /**
     * @brief Submit transfer engine operation asynchronously
     * @param src_offset Optional offset in source buffer (default 0)
     */
    std::optional<TransferFuture> submitTransferEngineOperation(
        const AllocatedBuffer::Descriptor& handle,
        const std::vector<Slice>& slices, const TransferRequest::OpCode op_code,
        uint64_t src_offset = 0, const OperationOptions& options = {});

    std::optional<TransferFuture> submitMemoryReadOperation(
        const AllocatedBuffer::Descriptor& handle,
        const std::vector<Slice>& slices, uint64_t src_offset,
        const OperationOptions& options = {});

    std::optional<TransferFuture> submitFileReadOperation(
        const Replica::Descriptor& replica, std::vector<Slice>& slices,
        TransferRequest::OpCode op_code);

    /**
     * @brief Calculate total bytes for transfer operation and update metrics
     */
    void updateTransferMetrics(const std::vector<Slice>& slices,
                               TransferRequest::OpCode op);

    std::optional<TransferFuture> submitTransfer(
        std::vector<TransferRequest>& requests,
        const SchedulingHint& hint = {});
};

}  // namespace mooncake
