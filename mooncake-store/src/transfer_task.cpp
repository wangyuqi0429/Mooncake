#include "transfer_task.h"
#include "vsegment/file_segment.h"
#include "store_rpc_client_io_context.h"
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_io/client_pool.hpp>
#include <async_simple/coro/SyncAwait.h>

#include <glog/logging.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <memory>
#include <limits>
#include <sstream>
#include <string>
#include <sys/syscall.h>
#include <unistd.h>
#include <vector>
#include "device/accelerator_registry.h"
#include "mooncake_logging.h"
#include "transfer_engine.h"
#include "transport/transport.h"
#ifdef USE_NOF
#include "spdk/spdk_wrapper.h"
#endif

namespace {
#ifndef USE_EVENT_DRIVEN_COMPLETION
// UB has dedicated posting/polling workers. Long Store-side busy waits can
// compete with those workers; retain a short spin for fast completions first.
int ReadUbPollDelayUs(const char* name, int default_value) {
    const char* value = std::getenv(name);
    if (!value) return default_value;
    char* end = nullptr;
    errno = 0;
    const long parsed = std::strtol(value, &end, 10);
    if (errno || end == value || *end || parsed < 0 || parsed > 1000000) {
        LOG(WARNING) << "Invalid " << name << "; using " << default_value;
        return default_value;
    }
    return static_cast<int>(parsed);
}

int UbPollSpinUs() {
    static const int value = ReadUbPollDelayUs("MC_STORE_UB_POLL_SPIN_US", 50);
    return value;
}

int UbPollSleepUs() {
    static const int value = ReadUbPollDelayUs("MC_STORE_UB_POLL_SLEEP_US", 50);
    return value;
}
#endif
}  // namespace

#ifdef USE_NOF
static bool IsTruthyEnv(const char* value) {
    if (!value) {
        return false;
    }
    std::string normalized(value);
    std::transform(
        normalized.begin(), normalized.end(), normalized.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return normalized == "1" || normalized == "true" || normalized == "yes" ||
           normalized == "on";
}

static bool IsSpdkNofDebugEnabled() {
    static const bool enabled = IsTruthyEnv(std::getenv("MC_NOF_DEBUG"));
    return enabled;
}

static int GetSpdkNofDebugIntervalMs() {
    static const int interval_ms = []() {
        const char* raw_value = std::getenv("MC_NOF_DEBUG_INTERVAL_MS");
        if (!raw_value) {
            return 1000;
        }
        char* end_ptr = nullptr;
        long parsed = std::strtol(raw_value, &end_ptr, 10);
        if (end_ptr == raw_value || (end_ptr != nullptr && *end_ptr != '\0') ||
            parsed <= 0) {
            return 1000;
        }
        return static_cast<int>(parsed);
    }();
    return interval_ms;
}

namespace mooncake {
int64_t GetSpdkNofSlowTraceThresholdUs() {
    static const int64_t threshold_us = []() -> int64_t {
        const char* raw_value =
            std::getenv("MC_NOF_SLOW_TRACE_THRESHOLD_US");
        if (!raw_value || raw_value[0] == '\0') {
            return 0;
        }
        errno = 0;
        char* end_ptr = nullptr;
        const long long parsed = std::strtoll(raw_value, &end_ptr, 10);
        if (errno != 0 || end_ptr == raw_value ||
            (end_ptr != nullptr && *end_ptr != '\0') || parsed < 0 ||
            parsed > 60000000) {
            LOG(WARNING) << "Invalid value for "
                         << "MC_NOF_SLOW_TRACE_THRESHOLD_US: " << raw_value
                         << ", disabling slow trace";
            return 0;
        }
        return static_cast<int64_t>(parsed);
    }();
    return threshold_us;
}
}  // namespace mooncake

static int GetPositiveEnvOrDefault(const char* name, int default_value) {
    const char* raw_value = std::getenv(name);
    if (!raw_value || raw_value[0] == '\0') {
        return default_value;
    }

    errno = 0;
    char* end_ptr = nullptr;
    long parsed = std::strtol(raw_value, &end_ptr, 10);
    if (errno != 0 || end_ptr == raw_value ||
        (end_ptr != nullptr && *end_ptr != '\0') || parsed <= 0 ||
        parsed > std::numeric_limits<int>::max()) {
        LOG(WARNING) << "Invalid value for " << name << ": " << raw_value
                     << ", using default " << default_value;
        return default_value;
    }

    return static_cast<int>(parsed);
}

static int GetSpdkNofSubmitChunkBytes() {
    static const int value = GetPositiveEnvOrDefault(
        "MC_NOF_SUBMIT_CHUNK_BYTES", mooncake::kDefaultSpdkNofSubmitChunkBytes);
    return value;
}

static int GetSpdkNofInflightBytesLimit() {
    static const int value =
        GetPositiveEnvOrDefault("MC_NOF_INFLIGHT_BYTES_LIMIT",
                                mooncake::kDefaultSpdkNofInflightBytesLimit);
    return value;
}

static int GetSpdkNofWorkerCount() {
    static const int value = GetPositiveEnvOrDefault(
        "MC_NOF_WORKERS", mooncake::kDefaultSpdkNofWorkers);
    return value;
}

static int CountSpdkNofQueuedTasks(const mooncake::SpdkNofTask* head) {
    int count = 0;
    const mooncake::SpdkNofTask* cursor = head;
    while (cursor != nullptr) {
        ++count;
        cursor = cursor->nxt;
    }
    return count;
}

static inline int64_t ElapsedMicros(
    std::chrono::steady_clock::time_point begin,
    std::chrono::steady_clock::time_point end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - begin)
        .count();
}

static inline int64_t SteadyNanos(
    std::chrono::steady_clock::time_point value) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               value.time_since_epoch())
        .count();
}

static inline void MarkNofTaskAsQosHead(
    mooncake::SpdkNofQos* nof_qos, mooncake::SpdkNofTask* task,
    std::chrono::steady_clock::time_point now) {
    if (task && task->timing &&
        task->timing->qos_head_at ==
            std::chrono::steady_clock::time_point{}) {
        task->timing->qos_head_at = now;
        const auto counter_delta = [](uint64_t current, uint64_t start) {
            return static_cast<int64_t>(current >= start ? current - start : 0);
        };
        task->timing->qos_fifo_full_wait_us = counter_delta(
            nof_qos->inflight_full_with_pending_us,
            task->timing->qos_full_pending_at_enqueue_us);
        task->timing->qos_fifo_partial_wait_us = counter_delta(
            nof_qos->inflight_partial_with_pending_us,
            task->timing->qos_partial_pending_at_enqueue_us);
        task->timing->qos_fifo_zero_wait_us = counter_delta(
            nof_qos->inflight_zero_with_pending_us,
            task->timing->qos_zero_pending_at_enqueue_us);
    }
}

static inline void StartNofCreditWait(
    mooncake::SpdkNofTask* task,
    std::chrono::steady_clock::time_point now) {
    if (task && task->timing && !task->timing->credit_wait_active) {
        task->timing->credit_wait_active = true;
        task->timing->credit_wait_started_at = now;
    }
}

static inline void StopNofCreditWait(
    mooncake::SpdkNofTask* task,
    std::chrono::steady_clock::time_point now) {
    if (task && task->timing && task->timing->credit_wait_active) {
        task->timing->credit_wait_us +=
            ElapsedMicros(task->timing->credit_wait_started_at, now);
        task->timing->credit_wait_active = false;
    }
}

static inline void UpdateNofOccupancy(
    mooncake::SpdkNofQos* nof_qos,
    std::chrono::steady_clock::time_point now) {
    const auto elapsed_us = static_cast<uint64_t>(
        std::max<int64_t>(0, ElapsedMicros(nof_qos->last_occupancy_update,
                                           now)));
    nof_qos->last_occupancy_update = now;

    const int inflight = nof_qos->inflight_blocks[0] +
                         nof_qos->inflight_blocks[1];
    const bool pending = !nof_qos->Empty();
    nof_qos->occupancy_total_us += elapsed_us;
    if (inflight == 0) {
        if (pending) {
            nof_qos->occupancy_active_us += elapsed_us;
            nof_qos->inflight_zero_with_pending_us += elapsed_us;
        } else {
            nof_qos->inflight_idle_us += elapsed_us;
        }
        return;
    }

    nof_qos->occupancy_active_us += elapsed_us;
    if (inflight >= nof_qos->inflight_blocks_limit) {
        nof_qos->inflight_full_us += elapsed_us;
        if (pending) nof_qos->inflight_full_with_pending_us += elapsed_us;
    } else {
        nof_qos->inflight_partial_us += elapsed_us;
        if (pending) nof_qos->inflight_partial_with_pending_us += elapsed_us;
    }
}

static inline void SpdkNofTaskCompletion(mooncake::SpdkNofTask* task) {
    // A failed completion may leave a task on the QoS queue. Publish only
    // after it is popped, so the queue and callback cannot complete it twice.
    if (task->remaining_lba == 0 && task->outstanding_sub_io == 0 &&
        !task->on_chain) {
        const uint64_t lane_completion_sequence =
            task->nof_qos ? ++task->nof_qos->completed_task_sequence : 0;
        std::optional<mooncake::NofTransferTiming> timing;
        if (task->timing && task->first_sub_io_submitted &&
            task->timing->submitted_sub_io_count > 0 &&
            task->timing->completed_sub_io_count > 0 &&
            task->timing->worker_dequeued_at !=
                std::chrono::steady_clock::time_point{} &&
            task->timing->qos_enqueued_at !=
                std::chrono::steady_clock::time_point{} &&
            task->timing->qos_head_at !=
                std::chrono::steady_clock::time_point{} &&
            task->timing->last_submit_at !=
                std::chrono::steady_clock::time_point{} &&
            task->timing->first_completion_at !=
                std::chrono::steady_clock::time_point{}) {
            const auto completed_at = std::chrono::steady_clock::now();
            auto elapsed_us = [](auto begin, auto end) {
                return std::chrono::duration_cast<std::chrono::microseconds>(
                           end - begin)
                    .count();
            };
            timing.emplace();
            timing->worker_index = task->worker_index;
            timing->lane = task->io_lane_index;
            timing->qpair_id =
                mooncake::SpdkWrapper::GetInstance().GetIoLaneQpairId(
                    task->io_lane);
            timing->sub_io_count = task->timing->submitted_sub_io_count;
            timing->lane_task_sequence = task->timing->lane_task_sequence;
            timing->lane_completion_sequence = lane_completion_sequence;
            timing->qos_tasks_ahead_at_enqueue =
                task->timing->qos_tasks_ahead_at_enqueue;
            timing->dispatch_queue_us =
                elapsed_us(task->queued_at, task->timing->worker_dequeued_at);
            timing->worker_setup_us =
                elapsed_us(task->timing->worker_dequeued_at,
                           task->timing->qos_enqueued_at);
            timing->qos_fifo_wait_us =
                elapsed_us(task->timing->qos_enqueued_at,
                           task->timing->qos_head_at);
            timing->qos_fifo_full_wait_us =
                task->timing->qos_fifo_full_wait_us;
            timing->qos_fifo_partial_wait_us =
                task->timing->qos_fifo_partial_wait_us;
            timing->qos_fifo_zero_wait_us =
                task->timing->qos_fifo_zero_wait_us;
            timing->head_to_first_submit_us =
                elapsed_us(task->timing->qos_head_at,
                           task->timing->first_submit_at);
            timing->credit_wait_us = task->timing->credit_wait_us;
            timing->first_credit_wait_us =
                task->timing->first_credit_wait_us;
            timing->qos_queue_us =
                elapsed_us(task->timing->worker_dequeued_at,
                           task->timing->first_submit_at);
            timing->queue_to_first_submit_us =
                elapsed_us(task->queued_at, task->timing->first_submit_at);
            timing->first_to_last_submit_us =
                elapsed_us(task->timing->first_submit_at,
                           task->timing->last_submit_at);
            timing->last_submit_to_complete_us =
                elapsed_us(task->timing->last_submit_at, completed_at);
            timing->first_submit_to_first_completion_us =
                elapsed_us(task->timing->first_submit_at,
                           task->timing->first_completion_at);
            timing->first_completion_minus_last_submit_us =
                elapsed_us(task->timing->last_submit_at,
                           task->timing->first_completion_at);
            timing->first_completion_to_complete_us =
                elapsed_us(task->timing->first_completion_at, completed_at);
            timing->total_us = elapsed_us(task->queued_at, completed_at);
            timing->queued_mono_ns = SteadyNanos(task->queued_at);
            timing->first_submit_mono_ns =
                SteadyNanos(task->timing->first_submit_at);
            timing->last_submit_mono_ns =
                SteadyNanos(task->timing->last_submit_at);
            timing->completed_mono_ns = SteadyNanos(completed_at);
            timing->max_sub_io_latency_us =
                task->timing->max_sub_io_latency_us;
            timing->max_sub_io_index = task->timing->max_sub_io_index;
            timing->max_sub_io_cid = task->timing->max_sub_io_cid;
            timing->max_sub_io_lba = task->timing->max_sub_io_lba;
            timing->max_sub_io_blocks = task->timing->max_sub_io_blocks;
            timing->max_sub_io_submit_mono_ns =
                task->timing->max_sub_io_submit_mono_ns;
            timing->max_sub_io_complete_mono_ns =
                task->timing->max_sub_io_complete_mono_ns;
            timing->max_completion_gap_us =
                task->timing->max_completion_gap_us;
            timing->max_completion_gap_begin_mono_ns =
                task->timing->max_completion_gap_begin_mono_ns;
            timing->max_completion_gap_end_mono_ns =
                task->timing->max_completion_gap_end_mono_ns;
            timing->max_poll_return_gap_us =
                task->timing->max_poll_return_gap_us;
            timing->max_poll_return_gap_begin_mono_ns =
                task->timing->max_poll_return_gap_begin_mono_ns;
            timing->max_poll_return_gap_end_mono_ns =
                task->timing->max_poll_return_gap_end_mono_ns;
            if (task->nof_qos) {
                const auto counter_delta = [](uint64_t current,
                                              uint64_t start) {
                    return current >= start ? current - start : 0;
                };
                timing->poll_calls_during_task = counter_delta(
                    task->nof_qos->poll_calls,
                    task->timing->poll_calls_at_first_submit);
                timing->empty_polls_during_task = counter_delta(
                    task->nof_qos->empty_polls,
                    task->timing->empty_polls_at_first_submit);
                timing->polled_completions_during_task = counter_delta(
                    task->nof_qos->polled_completions,
                    task->timing->polled_completions_at_first_submit);
            }
        }
        if (timing.has_value() &&
            mooncake::GetSpdkNofSlowTraceThresholdUs() > 0 &&
            timing->total_us >=
                mooncake::GetSpdkNofSlowTraceThresholdUs()) {
            MC_LOG(INFO)
                << "nof_slow_task trace_id=" << task->state->trace_id()
                << " worker_idx=" << timing->worker_index
                << " lane=" << timing->lane
                << " qpair_id=" << timing->qpair_id
                << " lane_task_seq=" << timing->lane_task_sequence
                << " lane_completion_seq="
                << timing->lane_completion_sequence
                << " lba=" << task->lba
                << " blocks=" << task->lba_count
                << " sub_ios=" << timing->sub_io_count
                << " queued_mono_ns=" << timing->queued_mono_ns
                << " first_submit_mono_ns="
                << timing->first_submit_mono_ns
                << " last_submit_mono_ns="
                << timing->last_submit_mono_ns
                << " completed_mono_ns=" << timing->completed_mono_ns
                << " total_us=" << timing->total_us
                << " fifo_us=" << timing->qos_fifo_wait_us
                << " fifo_full_us=" << timing->qos_fifo_full_wait_us
                << " fifo_partial_us=" << timing->qos_fifo_partial_wait_us
                << " fifo_zero_us=" << timing->qos_fifo_zero_wait_us
                << " credit_us=" << timing->credit_wait_us
                << " submit_span_us=" << timing->first_to_last_submit_us
                << " drain_us=" << timing->last_submit_to_complete_us
                << " max_subio_us=" << timing->max_sub_io_latency_us
                << " max_subio_idx=" << timing->max_sub_io_index
                << " max_subio_cid=" << timing->max_sub_io_cid
                << " max_subio_lba=" << timing->max_sub_io_lba
                << " max_subio_blocks=" << timing->max_sub_io_blocks
                << " max_subio_submit_mono_ns="
                << timing->max_sub_io_submit_mono_ns
                << " max_subio_complete_mono_ns="
                << timing->max_sub_io_complete_mono_ns
                << " max_completion_gap_us="
                << timing->max_completion_gap_us
                << " max_completion_gap_begin_mono_ns="
                << timing->max_completion_gap_begin_mono_ns
                << " max_completion_gap_end_mono_ns="
                << timing->max_completion_gap_end_mono_ns
                << " max_poll_return_gap_us="
                << timing->max_poll_return_gap_us
                << " max_poll_return_gap_begin_mono_ns="
                << timing->max_poll_return_gap_begin_mono_ns
                << " max_poll_return_gap_end_mono_ns="
                << timing->max_poll_return_gap_end_mono_ns
                << " poll_calls=" << timing->poll_calls_during_task
                << " empty_polls=" << timing->empty_polls_during_task
                << " polled_completions="
                << timing->polled_completions_during_task;
        }
        task->state->set_completed(
            task->failed ? mooncake::ErrorCode::TRANSFER_FAIL
                         : mooncake::ErrorCode::OK,
            std::move(timing));
        if (task->live_registry) task->live_registry->erase(task);
        delete task;
    }
}

static void nvmf_io_complete(void* ctx, const struct spdk_nvme_cpl* cpl) {
    if (!ctx) {
        LOG(ERROR) << "nvmf_io_complete ctx is null";
        return;
    }

    mooncake::SpdkNofSubTask* sub_task =
        reinterpret_cast<mooncake::SpdkNofSubTask*>(ctx);
    mooncake::SpdkNofTask* task = sub_task->task;
    mooncake::SpdkNofQos* nof_qos = task->nof_qos;
    int op = task->op;
    const bool debug_timing = sub_task->track_latency;
    bool releases_sampled_credit = false;
    if (nof_qos->sampled_timing_tasks > 0) {
        for (int pending_op = 0; pending_op < mooncake::kSpdkNofOpNum;
             ++pending_op) {
            releases_sampled_credit =
                releases_sampled_credit ||
                (nof_qos->head[pending_op] &&
                 nof_qos->head[pending_op]->timing &&
                 nof_qos->head[pending_op]->timing->credit_wait_active);
        }
    }
    const bool track_occupancy =
        debug_timing || nof_qos->sampled_timing_tasks > 0;
    const auto completion_now =
        (task->timing || track_occupancy || releases_sampled_credit)
            ? std::chrono::steady_clock::now()
            : std::chrono::steady_clock::time_point{};
    if (task->timing) {
        const int64_t sub_io_latency_us =
            ElapsedMicros(sub_task->submitted_at, completion_now);
        if (sub_io_latency_us > task->timing->max_sub_io_latency_us) {
            task->timing->max_sub_io_latency_us = sub_io_latency_us;
            task->timing->max_sub_io_index = sub_task->sub_io_index;
            task->timing->max_sub_io_cid = cpl->cid;
            task->timing->max_sub_io_lba = sub_task->submit_lba;
            task->timing->max_sub_io_blocks =
                static_cast<uint32_t>(sub_task->submit_lba_count);
            task->timing->max_sub_io_submit_mono_ns =
                SteadyNanos(sub_task->submitted_at);
            task->timing->max_sub_io_complete_mono_ns =
                SteadyNanos(completion_now);
        }
        if (task->timing->previous_completion_at !=
            std::chrono::steady_clock::time_point{}) {
            const int64_t completion_gap_us = ElapsedMicros(
                task->timing->previous_completion_at, completion_now);
            if (completion_gap_us > task->timing->max_completion_gap_us) {
                task->timing->max_completion_gap_us = completion_gap_us;
                task->timing->max_completion_gap_begin_mono_ns =
                    SteadyNanos(task->timing->previous_completion_at);
                task->timing->max_completion_gap_end_mono_ns =
                    SteadyNanos(completion_now);
            }
        }
        task->timing->previous_completion_at = completion_now;
        if (nof_qos->last_poll_return_at !=
            std::chrono::steady_clock::time_point{}) {
            const int64_t poll_return_gap_us =
                ElapsedMicros(nof_qos->last_poll_return_at, completion_now);
            if (poll_return_gap_us >
                task->timing->max_poll_return_gap_us) {
                task->timing->max_poll_return_gap_us = poll_return_gap_us;
                task->timing->max_poll_return_gap_begin_mono_ns =
                    SteadyNanos(nof_qos->last_poll_return_at);
                task->timing->max_poll_return_gap_end_mono_ns =
                    SteadyNanos(completion_now);
            }
        }
        if (task->timing->completed_sub_io_count == 0) {
            task->timing->first_completion_at = completion_now;
        }
        ++task->timing->completed_sub_io_count;
    }
    if (debug_timing) {
        const auto latency_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                completion_now - sub_task->submitted_at)
                .count());
        ++nof_qos->completion_latency_samples;
        nof_qos->completion_latency_us += latency_us;
        nof_qos->max_completion_latency_us =
            std::max(nof_qos->max_completion_latency_us, latency_us);
        sub_task->track_latency = false;
    }
    if (--(*task->io_count) < 0) {
        LOG(ERROR) << "total outstanding io < 0";
    }

    if (--(task->outstanding_sub_io) < 0) {
        LOG(ERROR) << "task outstanding io < 0";
    }

    if (track_occupancy) {
        UpdateNofOccupancy(nof_qos, completion_now);
    }
    nof_qos->inflight_blocks[op] -= sub_task->submit_lba_count;
    if (nof_qos->inflight_blocks[0] + nof_qos->inflight_blocks[1] <
        nof_qos->inflight_blocks_limit) {
        for (int pending_op = 0; pending_op < mooncake::kSpdkNofOpNum;
             ++pending_op) {
            if (nof_qos->head[pending_op] &&
                nof_qos->head[pending_op]->timing &&
                nof_qos->head[pending_op]
                    ->timing->credit_wait_active) {
                StopNofCreditWait(nof_qos->head[pending_op], completion_now);
            }
        }
    }
    if (nof_qos->inflight_blocks[op] < 0) {
        LOG(ERROR) << "task outstanding io < 0";
    }

    if (spdk_nvme_cpl_is_error(cpl)) {
        ++nof_qos->completion_errors;
        LOG(ERROR) << "task_complete: I/O failed "
                   << mooncake::NofCplStatusString(
                          cpl, op == mooncake::kSpdkNofOpRead
                                   ? SPDK_NVME_OPC_READ
                                   : SPDK_NVME_OPC_WRITE);
        task->remaining_lba = 0;
        task->failed = true;
    } else {
        ++nof_qos->completion_successes;
    }

#ifdef USE_NOF_URMA
    // Modified By Yida (v3): 验收指标——完成口径（外部审阅 #3 复审）。
    // OnIoSubmitted 只证明 GPU I/O 提交成功；CUDA I/O 的最终成功在此
    // completion 无错误处另计（cuda_io_success），两者不可混用。
    mooncake::NofMemoryDomainManager::GetInstance().OnIoCompleted(
        task->buffer_info, !spdk_nvme_cpl_is_error(cpl),
        op == mooncake::kSpdkNofOpWrite,
        static_cast<uint64_t>(sub_task->submit_lba_count) *
            mooncake::SpdkWrapper::GetInstance().GetBlockSize(task->seg_handle));
#endif

    // During explicit qpair teardown, abort callbacks reclaim sub-tasks but
    // do not publish completion until SPDK has quiesced the DMA resources.
    if (!nof_qos->poisoned) SpdkNofTaskCompletion(task);

    sub_task->sub_task_pool->push(sub_task);
}
#endif
namespace mooncake {

#ifdef USE_NOF
SpdkNofQos::SpdkNofQos(uint32_t block_size, size_t lane_count) {
    int block_size_int = static_cast<int>(block_size);
    if (block_size_int <= 0) {
        block_size_int = 1;
    }

    blocks_per_chunk =
        std::max(1, GetSpdkNofSubmitChunkBytes() / block_size_int);
    // MC_NOF_INFLIGHT_BYTES_LIMIT is the aggregate budget for one segment.
    // R4 showed that aggregate QD above 256 only increases completion latency,
    // so divide the existing 32 MiB default across the I/O lanes.
    const auto effective_lane_count = std::max<size_t>(1, lane_count);
    const int segment_inflight_blocks =
        std::max(1, GetSpdkNofInflightBytesLimit() / block_size_int);
    inflight_blocks_limit = std::max(
        1, static_cast<int>(segment_inflight_blocks / effective_lane_count));
    for (int i = 0; i < kSpdkNofOpNum; ++i) {
        inflight_blocks[i] = 0;
        head[i] = nullptr;
        tail[i] = nullptr;
    }
}
#endif

namespace {

std::unique_ptr<mooncake::logging::ScopedTraceId> RestoreTraceIfMissing(
    uint64_t trace_id) {
    if (trace_id == 0 || mooncake::logging::CurrentTraceId() != 0) {
        return nullptr;
    }
    return std::make_unique<mooncake::logging::ScopedTraceId>(trace_id);
}

}  // namespace

// ============================================================================
// FilereadWorkerPool Implementation
// ============================================================================
// to fully utilize the available ssd bandwidth, we use a default of 10 worker
// threads.
constexpr int kDefaultFilereadWorkers = 10;

FilereadWorkerPool::FilereadWorkerPool(std::shared_ptr<StorageBackend>& backend)
    : shutdown_(false) {
    MC_VLOG(1) << "Creating FilereadWorkerPool with " << kDefaultFilereadWorkers
               << " workers";

    // Start worker threads
    workers_.reserve(kDefaultFilereadWorkers);
    for (int i = 0; i < kDefaultFilereadWorkers; ++i) {
        workers_.emplace_back(&FilereadWorkerPool::workerThread, this);
    }
    backend_ = backend;
}

FilereadWorkerPool::~FilereadWorkerPool() {
    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        shutdown_.store(true);
    }
    queue_cv_.notify_all();

    // Wait for all workers to finish
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    VLOG(1) << "FilereadWorkerPool destroyed";
}

void FilereadWorkerPool::submitTask(FilereadTask task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (shutdown_.load()) {
            LOG(WARNING)
                << "Attempting to submit task to shutdown FilereadWorkerPool";
            task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
}

void FilereadWorkerPool::workerThread() {
    VLOG(2) << "FilereadWorkerPool worker thread started";

    while (true) {
        FilereadTask task("", 0, {}, nullptr, 0);

        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return shutdown_.load() || !task_queue_.empty();
            });

            if (shutdown_.load() && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        // Execute the task if we have one
        if (task.state) {
            mooncake::logging::ScopedTraceId trace(task.trace_id);
            try {
                if (!backend_) {
                    LOG(ERROR)
                        << "Backend is not initialized, cannot load object";
                    task.state->set_completed(ErrorCode::TRANSFER_FAIL);
                    continue;
                }

                auto load_result = backend_->LoadObject(
                    task.file_path, task.slices, task.object_size);
                if (load_result) {
                    VLOG(2) << "Fileread task completed successfully with "
                            << task.file_path;
                    task.state->set_completed(ErrorCode::OK);
                } else {
                    LOG(ERROR)
                        << "Fileread task failed for file: " << task.file_path
                        << " with error: " << toString(load_result.error());
                    task.state->set_completed(ErrorCode::TRANSFER_FAIL);
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Exception during async fileread: " << e.what();
                task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            }
        }
    }

    VLOG(2) << "FilereadWorkerPool worker thread exiting";
}

// ============================================================================
// SpdkNofWorkerPool Implementation
// ============================================================================
// to fully utilize the available ssd bandwidth, we use a default of 4 worker
// threads.

#ifdef USE_NOF
std::shared_ptr<SpdkNofWorkerPool> SpdkNofWorkerPool::Acquire(
    int numa_socket_id) {
    // Construct the wrapper before the factory so it outlives every pool.
    (void)SpdkWrapper::GetInstance();
    static std::mutex mutex;
    static std::weak_ptr<SpdkNofWorkerPool> shared_pool;
    std::lock_guard<std::mutex> lock(mutex);
    auto pool = shared_pool.lock();
    if (!pool) {
        pool = std::make_shared<SpdkNofWorkerPool>(numa_socket_id);
        shared_pool = pool;
    } else if (pool->numa_socket_id_ != numa_socket_id) {
        LOG_FIRST_N(WARNING, 1)
            << "Sharing NoF dispatcher on NUMA node " << pool->numa_socket_id_
            << "; requested node " << numa_socket_id
            << " differs from the first client's selection";
    }
    return pool;
}

SpdkNofWorkerPool::SpdkNofWorkerPool(int numa_socket_id)
    : worker_count_(GetSpdkNofWorkerCount()),
      numa_socket_id_(numa_socket_id),
      task_queue_(std::make_unique<std::queue<SpdkNofTask>[]>(worker_count_)),
      queue_mutex_(std::make_unique<std::mutex[]>(worker_count_)),
      queue_cv_(std::make_unique<std::condition_variable[]>(worker_count_)),
      shutdown_(false) {
    VLOG(1) << "Creating SpdkNofWorkerPool with " << worker_count_
            << " workers";

    // Start worker threads
    workers_.reserve(worker_count_);
    for (int i = 0; i < worker_count_; ++i) {
        workers_.emplace_back(&SpdkNofWorkerPool::workerThread, this, i);
    }
}

SpdkNofWorkerPool::~SpdkNofWorkerPool() {
    if (shutdown_.exchange(true)) {
        return;
    }

    for (int i = 0; i < worker_count_; ++i) {
        queue_cv_[i].notify_all();
    }

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    for (const auto& [seg, binding] : seg_to_workers_) {
        (void)binding;
        SpdkWrapper::GetInstance().ReleaseIoLanes(seg, this);
    }

    VLOG(1) << "SpdkNofWorkerPool destroyed";
}

void SpdkNofWorkerPool::submitTask(SpdkNofTask task) {
    if (!task.state) {
        LOG(ERROR) << "Attempting to submit spdk nof task without state";
        return;
    }

    if (shutdown_.load()) {
        LOG(WARNING)
            << "Attempting to submit task to shutdown SpdkNofWorkerPool";
        task.state->set_completed(ErrorCode::TRANSFER_FAIL);
        return;
    }

    int worker_idx = -1;
    {
        std::lock_guard<std::mutex> lock(seg_mutex_);
        nof_seg_handle* seg = task.seg_handle;
        auto& wrapper = SpdkWrapper::GetInstance();
        const size_t lane_count = wrapper.GetIoLaneCount(seg);
        if (lane_count == 0) {
            LOG(ERROR) << "No NoF I/O lanes available for seg=" << seg;
            task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }
        bool new_binding = false;
        auto binding_it = seg_to_workers_.find(seg);
        if (binding_it == seg_to_workers_.end()) {
            if (!wrapper.ClaimIoLanes(seg, this)) {
                LOG(ERROR) << "NoF segment is owned by another dispatcher";
                task.state->set_completed(ErrorCode::TRANSFER_FAIL);
                return;
            }
            SegmentLaneBinding binding;
            binding.worker_indices.reserve(lane_count);
            for (size_t lane_index = 0; lane_index < lane_count;
                 ++lane_index) {
                binding.worker_indices.push_back(seg_num++ % worker_count_);
            }
            binding_it =
                seg_to_workers_.emplace(seg, std::move(binding)).first;
            new_binding = true;
        }
        auto& binding = binding_it->second;
        if (binding.worker_indices.size() != lane_count) {
            LOG(ERROR) << "NoF lane count changed after dispatcher binding";
            task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }
        // Only dispatch new operations to healthy lanes. In-flight operations
        // on a failed lane are completed as errors, never replayed here.
        for (size_t attempt = 0; attempt < lane_count; ++attempt) {
            const size_t lane_index = binding.next_lane++ % lane_count;
            auto* lane = wrapper.GetIoLane(seg, lane_index);
            if (!wrapper.IsIoLaneHealthy(lane)) continue;
            task.io_lane = lane;
            task.io_lane_index = static_cast<uint32_t>(lane_index);
            worker_idx = binding.worker_indices[lane_index];
            break;
        }
        if (worker_idx < 0) {
            task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }
        if (new_binding &&
            (IsSpdkNofDebugEnabled() ||
             GetSpdkNofSlowTraceThresholdUs() > 0)) {
            LOG(INFO) << "nof_worker_bind seg_handle=" << seg
                      << " lanes=" << lane_count;
            for (size_t lane_index = 0; lane_index < lane_count;
                 ++lane_index) {
                auto* lane = wrapper.GetIoLane(seg, lane_index);
                LOG(INFO) << "nof_lane_bind seg_handle=" << seg
                          << " lane=" << lane_index << " worker_idx="
                          << binding.worker_indices[lane_index]
                          << " qpair_id="
                          << wrapper.GetIoLaneQpairId(lane);
            }
        }
        if (new_binding && lane_count > static_cast<size_t>(worker_count_)) {
            LOG(WARNING) << "NoF segment has " << lane_count
                         << " I/O lanes but only " << worker_count_
                         << " workers; some qpairs share a polling worker";
        }
    }
    if (worker_idx < 0 || worker_idx >= worker_count_) {
        LOG(ERROR) << "seg is not bind to invalid worker " << worker_idx;
        task.state->set_completed(ErrorCode::TRANSFER_FAIL);
        return;
    }

    {
        std::lock_guard<std::mutex> lock(queue_mutex_[worker_idx]);
        task.queued_at = std::chrono::steady_clock::now();
        task_queue_[worker_idx].push(std::move(task));
    }
    queue_cv_[worker_idx].notify_one();
}

static bool HasBufferedTask(
    const std::map<nof_io_lane*, std::unique_ptr<SpdkNofQos>>& lane_to_qos) {
    for (const auto& [_, nof_qos] : lane_to_qos) {
        if (!nof_qos->Empty()) {
            return true;
        }
    }
    return false;
}

constexpr int kSpdkNofSubTaskChunkSize = 4096;

static inline bool CheckSubTaskPool(
    std::stack<SpdkNofSubTask*>& sub_task_pool,
    std::vector<SpdkNofSubTask*>& sub_task_chunks, int work_idx) {
    if (!sub_task_pool.empty()) {
        return true;
    }
    SpdkNofSubTask* sub_tasks =
        new (std::nothrow) SpdkNofSubTask[kSpdkNofSubTaskChunkSize];
    if (!sub_tasks) {
        LOG(ERROR) << "alloc SpdkNofSubTask failed, worker " << work_idx;
        return false;
    }
    sub_task_chunks.push_back(sub_tasks);

    for (int i = 0; i < kSpdkNofSubTaskChunkSize; ++i) {
        sub_tasks[i].sub_task_pool = &sub_task_pool;
        sub_task_pool.push(&sub_tasks[i]);
    }

    return true;
}

void SpdkNofWorkerPool::workerThread(int work_idx) {
    bindToSocket(numa_socket_id_);
    VLOG(2) << "SpdkNofWorkerPool worker thread started";
    if (IsSpdkNofDebugEnabled() ||
        GetSpdkNofSlowTraceThresholdUs() > 0) {
        LOG(INFO) << "nof_worker_thread worker_idx=" << work_idx
                  << " tid=" << syscall(SYS_gettid);
    }

    int64_t total_outstanding_io = 0;
    // std::set<nof_seg_handle *> seg_set;
    std::map<nof_io_lane*, std::unique_ptr<SpdkNofQos>> lane_to_qos;
    std::stack<SpdkNofSubTask*> sub_task_pool;
    std::vector<SpdkNofSubTask*> sub_task_chunks;
    std::set<SpdkNofTask*> live_tasks;
    const auto io_timeout = std::chrono::milliseconds(
        GetPositiveEnvOrDefault("MC_NOF_IO_TIMEOUT_MS", 60000));
    const bool debug_enabled = IsSpdkNofDebugEnabled();
    auto& task_queue = task_queue_[work_idx];
    auto& queue_cv = queue_cv_[work_idx];
    auto& queue_mutex = queue_mutex_[work_idx];
    auto last_debug_snapshot = std::chrono::steady_clock::now();

    while (true) {
        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            queue_cv.wait(
                lock, [this, &task_queue, &total_outstanding_io, &lane_to_qos] {
                    return shutdown_.load() || !task_queue.empty() ||
                           total_outstanding_io || HasBufferedTask(lane_to_qos);
                });

            if (shutdown_.load() && task_queue.empty() &&
                (total_outstanding_io == 0) && !HasBufferedTask(lane_to_qos)) {
                break;
            }

            while (!task_queue.empty()) {
                if (!SpdkWrapper::GetInstance().IsIoLaneHealthy(
                        task_queue.front().io_lane)) {
                    task_queue.front().state->set_completed(
                        ErrorCode::TRANSFER_FAIL);
                    task_queue.pop();
                    continue;
                }
                SpdkNofTask* task = new (std::nothrow)
                    SpdkNofTask(std::move(task_queue.front()));
                if (task == nullptr) {
                    LOG(ERROR)
                        << "alloc SpdkNofTask failed, worker " << work_idx;
                    task_queue.front().state->set_completed(
                        ErrorCode::TRANSFER_FAIL);
                    task_queue.pop();
                    continue;
                }
                if (task->timing) {
                    task->timing->worker_dequeued_at =
                        std::chrono::steady_clock::now();
                }
                task->worker_index = work_idx;

                SpdkNofQos* nof_qos = nullptr;
                auto it = lane_to_qos.find(task->io_lane);
                if (it == lane_to_qos.end()) {
                    const size_t lane_count =
                        SpdkWrapper::GetInstance().GetIoLaneCount(
                            task->seg_handle);
                    auto qos = std::unique_ptr<SpdkNofQos>(
                        new (std::nothrow) SpdkNofQos(
                            SpdkWrapper::GetInstance().GetBlockSize(
                                task->seg_handle),
                            lane_count));
                    if (qos == nullptr) {
                        LOG(ERROR)
                            << "alloc SpdkNofQos failed, worker " << work_idx;
                        task->state->set_completed(ErrorCode::TRANSFER_FAIL);
                        delete task;
                        task_queue.pop();
                        continue;
                    }
                    nof_qos = qos.get();
                    lane_to_qos[task->io_lane] = std::move(qos);
                    if (debug_enabled) {
                        LOG(INFO)
                            << "nof_qos_create worker_idx=" << work_idx
                            << " seg_handle=" << task->seg_handle
                            << " lane=" << task->io_lane_index
                            << " blocks_per_chunk="
                            << lane_to_qos[task->io_lane]->blocks_per_chunk
                            << " lane_inflight_blocks_limit="
                            << lane_to_qos[task->io_lane]
                                   ->inflight_blocks_limit
                            << " segment_inflight_blocks_limit="
                            << lane_to_qos[task->io_lane]
                                   ->inflight_blocks_limit *
                                   static_cast<int>(lane_count);
                    }
                } else {
                    nof_qos = it->second.get();
                }
                task->io_count = &total_outstanding_io;
                task->nof_qos = nof_qos;
                task->on_chain = true;
                task->live_registry = &live_tasks;
                live_tasks.insert(task);
#ifdef USE_NOF_URMA
                // Modified By Yida (v3): 入队前识别 buffer 类型并解析 memory
                // domain（每个 task 一次）。kError/kNotSupported 直接标记失败
                // 走完成路径——严格错误策略，不静默回退 host staging。
                {
                    uint32_t blk = SpdkWrapper::GetInstance().GetBlockSize(
                        task->seg_handle);
                    task->buffer_info =
                        NofMemoryDomainManager::GetInstance().ResolveBuffer(
                            task->ptr,
                            static_cast<uint64_t>(task->lba_count) * blk);
                    if (task->buffer_info.type == NofMemoryType::kError ||
                        task->buffer_info.type ==
                            NofMemoryType::kNotSupported) {
                        LOG(ERROR) << "NoF buffer resolve failed type="
                                   << static_cast<int>(task->buffer_info.type)
                                   << " ptr=" << task->ptr;
                        task->failed = true;
                        task->remaining_lba = 0;
                    }
                }
#endif
                const bool lane_was_empty = nof_qos->head[task->op] == nullptr;
                const uint32_t tasks_ahead =
                    task->timing
                        ? static_cast<uint32_t>(CountSpdkNofQueuedTasks(
                              nof_qos->head[task->op]))
                        : 0;
                const bool track_occupancy =
                    debug_enabled || task->timing ||
                    nof_qos->sampled_timing_tasks > 0;
                const auto qos_enqueue_now =
                    track_occupancy
                        ? std::chrono::steady_clock::now()
                        : std::chrono::steady_clock::time_point{};
                if (track_occupancy) {
                    UpdateNofOccupancy(nof_qos, qos_enqueue_now);
                }
                if (task->timing) {
                    task->timing->qos_enqueued_at = qos_enqueue_now;
                    task->timing->lane_task_sequence =
                        nof_qos->enqueued_task_sequence + 1;
                    task->timing->qos_tasks_ahead_at_enqueue = tasks_ahead;
                    task->timing->qos_full_pending_at_enqueue_us =
                        nof_qos->inflight_full_with_pending_us;
                    task->timing->qos_partial_pending_at_enqueue_us =
                        nof_qos->inflight_partial_with_pending_us;
                    task->timing->qos_zero_pending_at_enqueue_us =
                        nof_qos->inflight_zero_with_pending_us;
                    ++nof_qos->sampled_timing_tasks;
                }
                ++nof_qos->enqueued_task_sequence;
                nof_qos->PushTask(task);
                if (lane_was_empty) {
                    MarkNofTaskAsQosHead(nof_qos, task, qos_enqueue_now);
                }
                task_queue.pop();
            }
        }

        for (auto& [io_lane, nof_qos] : lane_to_qos) {
            uint32_t block_size =
                SpdkWrapper::GetInstance().GetBlockSize(io_lane);
            bool qpair_backpressured = false;
            const int first_op = nof_qos->next_op;
            for (int op_offset = 0;
                 op_offset < kSpdkNofOpNum && !qpair_backpressured;
                 ++op_offset) {
                const int i = (first_op + op_offset) % kSpdkNofOpNum;
                int avail_blocks = nof_qos->inflight_blocks_limit -
                                   nof_qos->inflight_blocks[0] -
                                   nof_qos->inflight_blocks[1];
                if (nof_qos->head[i]) {
                    const auto credit_state_now =
                        nof_qos->head[i]->timing
                            ? std::chrono::steady_clock::now()
                            : std::chrono::steady_clock::time_point{};
                    if (avail_blocks <= 0) {
                        StartNofCreditWait(nof_qos->head[i],
                                           credit_state_now);
                    } else {
                        StopNofCreditWait(nof_qos->head[i], credit_state_now);
                    }
                }
                bool submitted_for_op = false;
                while (nof_qos->head[i] && avail_blocks > 0) {
                    SpdkNofTask* task = nof_qos->head[i];
                    if (nof_qos->poisoned) {
                        task->failed = true;
                        task->remaining_lba = 0;
                    }
                    SpdkNofSubTask* sub_task;
                    while (task->remaining_lba > 0 && avail_blocks > 0) {
                        uint32_t submit_lba_count = std::min(
                            avail_blocks, std::min(task->remaining_lba,
                                                   nof_qos->blocks_per_chunk));
                        int lba_off = task->lba_count - task->remaining_lba;
                        uint64_t submit_lba = task->lba + lba_off;
                        void* submit_ptr = reinterpret_cast<void*>(
                            reinterpret_cast<char*>(task->ptr) +
                            lba_off * block_size);

                        if (!CheckSubTaskPool(sub_task_pool, sub_task_chunks,
                                              work_idx)) {
                            task->failed = true;
                            task->remaining_lba = 0;
                            break;
                        }
                        sub_task = sub_task_pool.top();
                        sub_task_pool.pop();
                        sub_task->task = task;
                        sub_task->submit_lba_count = submit_lba_count;
                        sub_task->sub_io_index =
                            static_cast<uint32_t>(task->idx);
                        sub_task->submit_lba = submit_lba;
                        const auto sub_io_submit_at =
                            (debug_enabled || task->timing)
                                ? std::chrono::steady_clock::now()
                                : std::chrono::steady_clock::time_point{};

#ifdef USE_NOF_URMA
                        // Modified By Yida (v3): GPU Direct——CUDA buffer 走
                        // ext I/O 携带 memory domain（domain 指向 cuda:
                        // mooncake:<id>，SPDK URMA 侧据此做设备内存注册）；
                        // host buffer 保持 io_opts=nullptr（经典 read/write，
                        // 按 HOST 注册，行为与改造前一致）。
                        const bool is_cuda_io =
                            task->buffer_info.type == NofMemoryType::kCuda;
                        if (is_cuda_io) {
                            sub_task->io_opts = {};
                            sub_task->io_opts.size =
                                sizeof(sub_task->io_opts);
                            sub_task->io_opts.memory_domain =
                                task->buffer_info.domain;
                        }
                        int ret = SpdkWrapper::GetInstance().SubmitRequestOnLane(
                            task->io_lane, submit_ptr, submit_lba,
                            submit_lba_count, task->op, nvmf_io_complete,
                            sub_task, is_cuda_io ? &sub_task->io_opts : nullptr);
#else
                        int ret = SpdkWrapper::GetInstance().SubmitRequestOnLane(
                            task->io_lane, submit_ptr, submit_lba,
                            submit_lba_count, task->op, nvmf_io_complete,
                            sub_task);
#endif
                        ++nof_qos->submit_attempts;
                        if (ret != 0) {
                            const bool retryable =
                                ret == -EAGAIN || ret == -ENOMEM;
                            const bool has_inflight =
                                nof_qos->inflight_blocks[0] > 0 ||
                                nof_qos->inflight_blocks[1] > 0;
                            if (ret == -EAGAIN) {
                                ++nof_qos->submit_eagain;
                            } else if (ret == -ENOMEM) {
                                ++nof_qos->submit_enomem;
                            } else {
                                ++nof_qos->submit_other_errors;
                            }
                            sub_task_pool.push(sub_task);
                            if (retryable && has_inflight) {
                                // SPDK uses EAGAIN/ENOMEM as queue-pressure
                                // signals. Keep the task at the head, poll
                                // completions to return credits, then retry it
                                // on the next worker iteration.
                                ++nof_qos->submit_backpressure_retries;
                                if (!nof_qos->submit_backpressured) {
                                    nof_qos->submit_backpressured = true;
                                    ++nof_qos->submit_backpressure_events;
                                    nof_qos->submit_backpressure_started =
                                        std::chrono::steady_clock::now();
                                }
                                qpair_backpressured = true;
                                break;
                            }
                            ++nof_qos->submit_fatal_errors;
                            if (ret == -ENXIO || ret == -ENODEV) {
                                SpdkWrapper::GetInstance().MarkIoLaneFailed(
                                    task->io_lane);
                                qpair_backpressured = true;
                            }
                            LOG(ERROR) << "work " << work_idx << ", seg "
                                       << task->seg_handle
                                       << " lane=" << task->io_lane_index
                                       << " submit io fail ret=" << ret
                                       << " chunk_blocks=" << submit_lba_count
                                       << " inflight_blocks="
                                       << nof_qos->inflight_blocks[i]
                                       << " inflight_limit="
                                       << nof_qos->inflight_blocks_limit;
                            task->failed = true;
                            task->remaining_lba = 0;
                        } else {
                            ++nof_qos->submit_successes;
                            if (nof_qos->submit_backpressured) {
                                nof_qos->submit_backpressured = false;
                                ++nof_qos->submit_backpressure_recoveries;
                                nof_qos->submit_backpressure_wait_us +=
                                    static_cast<uint64_t>(
                                        std::chrono::duration_cast<
                                            std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() -
                                            nof_qos
                                                ->submit_backpressure_started)
                                            .count());
                            }
                            const bool track_occupancy =
                                debug_enabled ||
                                nof_qos->sampled_timing_tasks > 0;
                            if (track_occupancy) {
                                const auto occupancy_now =
                                    std::chrono::steady_clock::now();
                                UpdateNofOccupancy(nof_qos.get(),
                                                   occupancy_now);
                            }
                            if (debug_enabled || task->timing) {
                                sub_task->submitted_at = sub_io_submit_at;
                            }
                            if (debug_enabled) {
                                sub_task->track_latency = true;
                            }
                            if (!task->first_sub_io_submitted) {
                                task->first_sub_io_submitted = true;
                                if (task->timing) {
                                    task->timing->first_submit_at =
                                        sub_io_submit_at;
                                    task->timing->poll_calls_at_first_submit =
                                        nof_qos->poll_calls;
                                    task->timing->empty_polls_at_first_submit =
                                        nof_qos->empty_polls;
                                    task->timing
                                        ->polled_completions_at_first_submit =
                                        nof_qos->polled_completions;
                                    // Credit waited while at the head before
                                    // this first successful submit.  Submits
                                    // only run with avail_blocks > 0, so any
                                    // active wait was already stopped.
                                    task->timing->first_credit_wait_us =
                                        task->timing->credit_wait_us;
                                }
                                ++nof_qos->first_submit_count;
                                nof_qos->first_submit_wait_us +=
                                    static_cast<uint64_t>(
                                        std::chrono::duration_cast<
                                            std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() -
                                            task->queued_at)
                                            .count());
                            }
                            if (nof_qos->inflight_blocks[0] == 0 &&
                                nof_qos->inflight_blocks[1] == 0) {
                                nof_qos->last_progress =
                                    std::chrono::steady_clock::now();
                            }
                            task->idx++;
#ifdef USE_NOF_URMA
                            // Modified By Yida (v3): 验收指标（host/cuda
                            // 分开计数）。提交口径：SubmitRequest 返回成功
                            // 即计数，完成成功在 nvmf_io_complete 另计。
                            NofMemoryDomainManager::GetInstance().OnIoSubmitted(
                                task->buffer_info,
                                task->op == kSpdkNofOpWrite,
                                static_cast<uint64_t>(submit_lba_count) *
                                    block_size);
#endif
                            task->remaining_lba -= submit_lba_count;
                            if (task->timing) {
                                ++task->timing->submitted_sub_io_count;
                                if (task->remaining_lba == 0) {
                                    task->timing->last_submit_at =
                                        std::chrono::steady_clock::now();
                                }
                            }
                            nof_qos->inflight_blocks[i] += submit_lba_count;
                            submitted_for_op = true;
                            nof_qos->max_inflight_blocks = std::max(
                                nof_qos->max_inflight_blocks,
                                nof_qos->inflight_blocks[0] +
                                    nof_qos->inflight_blocks[1]);
                            avail_blocks -= submit_lba_count;
                            task->outstanding_sub_io++;
                            total_outstanding_io++;
                            if (task->timing && task->remaining_lba > 0 &&
                                avail_blocks <= 0) {
                                StartNofCreditWait(
                                    task, std::chrono::steady_clock::now());
                            }
                        }
                    }
                    if (qpair_backpressured) {
                        break;
                    }
                    if (task->remaining_lba == 0) {
                        const bool track_occupancy =
                            debug_enabled || nof_qos->sampled_timing_tasks > 0;
                        const bool capture_pop_time =
                            track_occupancy ||
                            (task->nxt != nullptr && task->nxt->timing);
                        const auto pop_now =
                            capture_pop_time
                                ? std::chrono::steady_clock::now()
                                : std::chrono::steady_clock::time_point{};
                        if (track_occupancy) {
                            UpdateNofOccupancy(nof_qos.get(), pop_now);
                        }
                        nof_qos->PopTask(i);
                        if (task->timing) {
                            --nof_qos->sampled_timing_tasks;
                        }
                        task->on_chain = false;
                        MarkNofTaskAsQosHead(nof_qos.get(), nof_qos->head[i],
                                             pop_now);
                        if (nof_qos->head[i] && avail_blocks <= 0) {
                            StartNofCreditWait(nof_qos->head[i], pop_now);
                        }
                        SpdkNofTaskCompletion(task);
                    }
                }
                if (submitted_for_op) {
                    // Alternate the first direction considered after progress
                    // so a sustained read stream cannot starve queued writes,
                    // or vice versa, under the shared lane budget.
                    nof_qos->next_op = (i + 1) % kSpdkNofOpNum;
                }
            }
        }

        {
            int64_t ret = 0;
            for (auto& it : lane_to_qos) {
                if (it.second->poisoned) continue;
                const bool healthy =
                    SpdkWrapper::GetInstance().IsIoLaneHealthy(it.first);
                if (healthy && it.second->inflight_blocks[0] == 0 &&
                    it.second->inflight_blocks[1] == 0) {
                    continue;
                }
                ret = healthy ? SpdkWrapper::GetInstance().PollIoLane(
                                    it.first, 0)
                              : -ENXIO;
                auto& qos = *it.second;
                ++qos.poll_calls;
                if (ret == 0) {
                    ++qos.empty_polls;
                } else if (ret > 0) {
                    qos.polled_completions += static_cast<uint64_t>(ret);
                }
                const auto now = std::chrono::steady_clock::now();
                qos.last_poll_return_at = now;
                if (ret > 0) qos.last_progress = now;
                const bool timed_out =
                    (qos.inflight_blocks[0] > 0 || qos.inflight_blocks[1] > 0) &&
                    now - qos.last_progress >= io_timeout;
                if (ret < 0 || timed_out) {
                    LOG(ERROR) << "NoF qpair failed ret=" << ret
                               << " timeout=" << timed_out
                               << " lane="
                               << SpdkWrapper::GetInstance().GetIoLaneIndex(
                                      it.first);
                    qos.poisoned = true;
                    SpdkWrapper::GetInstance().MarkIoLaneFailed(it.first);
                    // Keep queued tasks alive while SPDK invokes abort callbacks.
                    for (auto* task : live_tasks) {
                        if (task->io_lane == it.first) {
                            task->failed = true;
                            task->remaining_lba = 0;
                        }
                    }
                    SpdkWrapper::GetInstance().DrainFailedIoLane(it.first);
                    // DMA is quiesced and SPDK no longer owns callback contexts.
                    // Complete any tasks the transport did not callback itself.
                    for (auto pos = live_tasks.begin(); pos != live_tasks.end();) {
                        auto* task = *pos++;
                        if (task->io_lane != it.first) continue;
#ifdef USE_NOF_URMA
                        NofMemoryDomainManager::GetInstance().OnIoAborted(
                            task->buffer_info, task->op == kSpdkNofOpWrite,
                            task->outstanding_sub_io);
#endif
                        total_outstanding_io -= task->outstanding_sub_io;
                        task->outstanding_sub_io = 0;
                        if (task->timing && task->on_chain) {
                            // Still queued on the dead lane's chain, so its
                            // push-time sample count has not been released.
                            --qos.sampled_timing_tasks;
                        }
                        task->on_chain = false;
                        SpdkNofTaskCompletion(task);
                    }
                    if (debug_enabled) {
                        // Close the occupancy window with the pre-reset state
                        // so the failed lane's inflight time is attributed.
                        UpdateNofOccupancy(&qos, now);
                    }
                    for (int op = 0; op < kSpdkNofOpNum; ++op) {
                        qos.head[op] = qos.tail[op] = nullptr;
                        qos.inflight_blocks[op] = 0;
                    }
                }
            }
        }

        if (debug_enabled) {
            auto now = std::chrono::steady_clock::now();
            auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - last_debug_snapshot);
            if (elapsed.count() >= GetSpdkNofDebugIntervalMs()) {
                for (const auto& [io_lane, nof_qos] : lane_to_qos) {
                    UpdateNofOccupancy(nof_qos.get(), now);
                    const double empty_poll_ratio =
                        nof_qos->poll_calls == 0
                            ? 0.0
                            : static_cast<double>(nof_qos->empty_polls) /
                                  static_cast<double>(nof_qos->poll_calls);
                    const uint64_t nonempty_polls =
                        nof_qos->poll_calls - nof_qos->empty_polls;
                    const double completions_per_nonempty_poll =
                        nonempty_polls == 0
                            ? 0.0
                            : static_cast<double>(
                                  nof_qos->polled_completions) /
                                  static_cast<double>(nonempty_polls);
                    const uint64_t first_submit_wait_avg_us =
                        nof_qos->first_submit_count == 0
                            ? 0
                            : nof_qos->first_submit_wait_us /
                                  nof_qos->first_submit_count;
                    const uint64_t completion_latency_avg_us =
                        nof_qos->completion_latency_samples == 0
                            ? 0
                            : nof_qos->completion_latency_us /
                                  nof_qos->completion_latency_samples;
                    const uint64_t backpressure_wait_avg_us =
                        nof_qos->submit_backpressure_recoveries == 0
                            ? 0
                            : nof_qos->submit_backpressure_wait_us /
                                  nof_qos->submit_backpressure_recoveries;
                    const double inflight_idle_ratio =
                        nof_qos->occupancy_total_us == 0
                            ? 0.0
                            : static_cast<double>(nof_qos->inflight_idle_us) /
                                  static_cast<double>(
                                      nof_qos->occupancy_total_us);
                    const double inflight_partial_ratio =
                        nof_qos->occupancy_total_us == 0
                            ? 0.0
                            : static_cast<double>(
                                  nof_qos->inflight_partial_us) /
                                  static_cast<double>(
                                      nof_qos->occupancy_total_us);
                    const double inflight_full_ratio =
                        nof_qos->occupancy_total_us == 0
                            ? 0.0
                            : static_cast<double>(nof_qos->inflight_full_us) /
                                  static_cast<double>(
                                      nof_qos->occupancy_total_us);
                    const double inflight_zero_pending_ratio =
                        nof_qos->occupancy_total_us == 0
                            ? 0.0
                            : static_cast<double>(
                                  nof_qos->inflight_zero_with_pending_us) /
                                  static_cast<double>(
                                      nof_qos->occupancy_total_us);
                    const double pending_no_credit_ratio =
                        nof_qos->occupancy_total_us == 0
                            ? 0.0
                            : static_cast<double>(
                                  nof_qos->inflight_full_with_pending_us) /
                                  static_cast<double>(
                                      nof_qos->occupancy_total_us);
                    LOG(INFO)
                        << "nof_qos_state worker_idx=" << work_idx
                        << " io_lane=" << io_lane
                        << " lane="
                        << SpdkWrapper::GetInstance().GetIoLaneIndex(io_lane)
                        << " inflight_read=" << nof_qos->inflight_blocks[0]
                        << " inflight_write=" << nof_qos->inflight_blocks[1]
                        << " lane_inflight_limit="
                        << nof_qos->inflight_blocks_limit
                        << " queued_read="
                        << CountSpdkNofQueuedTasks(nof_qos->head[0])
                        << " queued_write="
                        << CountSpdkNofQueuedTasks(nof_qos->head[1])
                        << " total_outstanding_io=" << total_outstanding_io
                        << " max_inflight_blocks="
                        << nof_qos->max_inflight_blocks
                        << " occupancy_total_us="
                        << nof_qos->occupancy_total_us
                        << " occupancy_active_us="
                        << nof_qos->occupancy_active_us
                        << " inflight_idle_us=" << nof_qos->inflight_idle_us
                        << " inflight_partial_us="
                        << nof_qos->inflight_partial_us
                        << " inflight_full_us=" << nof_qos->inflight_full_us
                        << " inflight_zero_with_pending_us="
                        << nof_qos->inflight_zero_with_pending_us
                        << " inflight_partial_with_pending_us="
                        << nof_qos->inflight_partial_with_pending_us
                        << " inflight_full_with_pending_us="
                        << nof_qos->inflight_full_with_pending_us
                        << " inflight_partial_ratio="
                        << inflight_partial_ratio
                        << " inflight_full_ratio=" << inflight_full_ratio
                        << " inflight_idle_ratio=" << inflight_idle_ratio
                        << " inflight_zero_pending_ratio="
                        << inflight_zero_pending_ratio
                        << " pending_no_credit_ratio="
                        << pending_no_credit_ratio
                        << " submit_attempts=" << nof_qos->submit_attempts
                        << " submit_successes=" << nof_qos->submit_successes
                        << " submit_eagain=" << nof_qos->submit_eagain
                        << " submit_enomem=" << nof_qos->submit_enomem
                        << " submit_backpressure_retries="
                        << nof_qos->submit_backpressure_retries
                        << " submit_backpressure_events="
                        << nof_qos->submit_backpressure_events
                        << " submit_backpressure_recoveries="
                        << nof_qos->submit_backpressure_recoveries
                        << " submit_backpressure_active="
                        << nof_qos->submit_backpressured
                        << " submit_backpressure_wait_us="
                        << nof_qos->submit_backpressure_wait_us
                        << " submit_backpressure_wait_avg_us="
                        << backpressure_wait_avg_us
                        << " submit_fatal_errors="
                        << nof_qos->submit_fatal_errors
                        << " submit_other_errors="
                        << nof_qos->submit_other_errors
                        << " completion_successes="
                        << nof_qos->completion_successes
                        << " completion_errors="
                        << nof_qos->completion_errors
                        << " poll_calls=" << nof_qos->poll_calls
                        << " empty_polls=" << nof_qos->empty_polls
                        << " empty_poll_ratio=" << empty_poll_ratio
                        << " polled_completions="
                        << nof_qos->polled_completions
                        << " completions_per_nonempty_poll="
                        << completions_per_nonempty_poll
                        << " first_submit_count="
                        << nof_qos->first_submit_count
                        << " first_submit_wait_us="
                        << nof_qos->first_submit_wait_us
                        << " first_submit_wait_avg_us="
                        << first_submit_wait_avg_us
                        << " completion_latency_samples="
                        << nof_qos->completion_latency_samples
                        << " completion_latency_us="
                        << nof_qos->completion_latency_us
                        << " completion_latency_avg_us="
                        << completion_latency_avg_us
                        << " max_completion_latency_us="
                        << nof_qos->max_completion_latency_us;
                }
                last_debug_snapshot = now;
            }
        }
    }

    for (auto* sub_tasks : sub_task_chunks) {
        delete[] sub_tasks;
    }
    // lane_to_qos will automatically clean up SpdkNofQos objects using
    // unique_ptr

    VLOG(2) << "SpdkNofWorkerPool worker thread exiting";
}
#endif

// ============================================================================
// MemcpyWorkerPool Implementation
// ============================================================================
// A single worker serializes otherwise independent local-memory puts. Four
// workers are enough to expose several cores of memcpy bandwidth without
// creating an unbounded number of background threads. Deployments can tune
// this with MC_STORE_MEMCPY_WORKERS.
constexpr int kDefaultMemcpyWorkers = 4;
constexpr int kMaxMemcpyWorkers = 64;

static int GetMemcpyWorkerCount() {
    static const int worker_count = [] {
        const unsigned int hardware_threads =
            std::thread::hardware_concurrency();
        const int automatic_default =
            hardware_threads == 0
                ? kDefaultMemcpyWorkers
                : std::max(1, std::min(kDefaultMemcpyWorkers,
                                       static_cast<int>(hardware_threads)));

        const char* raw_value = std::getenv("MC_STORE_MEMCPY_WORKERS");
        if (!raw_value || raw_value[0] == '\0') return automatic_default;

        errno = 0;
        char* end_ptr = nullptr;
        const long parsed = std::strtol(raw_value, &end_ptr, 10);
        if (errno != 0 || end_ptr == raw_value ||
            (end_ptr != nullptr && *end_ptr != '\0') || parsed <= 0 ||
            parsed > kMaxMemcpyWorkers) {
            LOG(WARNING) << "Invalid MC_STORE_MEMCPY_WORKERS=" << raw_value
                         << ", using " << automatic_default;
            return automatic_default;
        }
        return static_cast<int>(parsed);
    }();
    return worker_count;
}

static bool IsInlineHostMemcpyEnabled() {
    static const bool enabled = [] {
        const char* raw_value = std::getenv("MC_STORE_MEMCPY_INLINE");
        if (!raw_value || raw_value[0] == '\0') return true;

        std::string normalized(raw_value);
        std::transform(
            normalized.begin(), normalized.end(), normalized.begin(),
            [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (normalized == "1" || normalized == "true" ||
            normalized == "yes" || normalized == "on") {
            return true;
        }
        if (normalized == "0" || normalized == "false" ||
            normalized == "no" || normalized == "off") {
            return false;
        }
        LOG(WARNING) << "Invalid MC_STORE_MEMCPY_INLINE=" << raw_value
                     << ", defaulting to enabled";
        return true;
    }();
    return enabled;
}

MemcpyWorkerPool::MemcpyWorkerPool() : shutdown_(false) {
    const int worker_count = GetMemcpyWorkerCount();
    LOG(INFO) << "Creating MemcpyWorkerPool with " << worker_count
              << " workers; tune with MC_STORE_MEMCPY_WORKERS";

    // Start worker threads
    workers_.reserve(worker_count);
    for (int i = 0; i < worker_count; ++i) {
        workers_.emplace_back(&MemcpyWorkerPool::workerThread, this);
    }
}

MemcpyWorkerPool::~MemcpyWorkerPool() {
    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        shutdown_.store(true);
    }
    queue_cv_.notify_all();

    // Wait for all workers to finish
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    VLOG(1) << "MemcpyWorkerPool destroyed";
}

void MemcpyWorkerPool::submitTask(MemcpyTask task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (shutdown_.load()) {
            LOG(WARNING)
                << "Attempting to submit task to shutdown MemcpyWorkerPool";
            task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }
        task.queued_ahead = task_queue_.size();
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
}

void MemcpyWorkerPool::workerThread() {
    VLOG(2) << "MemcpyWorkerPool worker thread started";

    while (true) {
        MemcpyTask task({}, nullptr, 0);

        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return shutdown_.load() || !task_queue_.empty();
            });

            if (shutdown_.load() && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        // Execute the task if we have one
        if (task.state) {
            mooncake::logging::ScopedTraceId trace(task.trace_id);
            const bool breakdown_log =
                mooncake::logging::ShouldSampleHiFreqLog(task.trace_id);
            const auto worker_start =
                breakdown_log ? std::chrono::steady_clock::now()
                              : std::chrono::steady_clock::time_point{};
            auto registry_end = worker_start;
            int64_t pointer_lookup_us = 0;
            int64_t copy_us = 0;
            size_t copied_bytes = 0;
            ErrorCode result = ErrorCode::TRANSFER_FAIL;
            try {
                bool ok = true;
                auto runtime_accelerator =
                    device::GetAcceleratorRegistry().RuntimeAccelerators();
                if (breakdown_log) {
                    registry_end = std::chrono::steady_clock::now();
                }
                for (const auto& op : task.operations) {
                    device::PointerInfo src_info;
                    device::PointerInfo dst_info;
                    const auto lookup_start =
                        breakdown_log ? std::chrono::steady_clock::now()
                                      : std::chrono::steady_clock::time_point{};
                    auto* src_device = runtime_accelerator.FindDeviceForPointer(
                        op.src, &src_info);
                    auto* dst_device = runtime_accelerator.FindDeviceForPointer(
                        op.dest, &dst_info);
                    if (breakdown_log) {
                        const auto lookup_end =
                            std::chrono::steady_clock::now();
                        pointer_lookup_us +=
                            std::chrono::duration_cast<std::chrono::microseconds>(
                                lookup_end - lookup_start)
                                .count();
                    }

                    const auto copy_start =
                        breakdown_log ? std::chrono::steady_clock::now()
                                      : std::chrono::steady_clock::time_point{};
                    if (!src_device && !dst_device) {
                        std::memcpy(op.dest, op.src, op.size);
                    } else {
                        if (src_device && dst_device &&
                            src_device != dst_device) {
                            LOG(ERROR)
                                << "GPU memcpy failed: source and destination "
                                   "belong to different accelerator runtimes"
                                << " src_dev=" << src_info.device_id
                                << " dst_dev=" << dst_info.device_id
                                << " size=" << op.size;
                            ok = false;
                            break;
                        }
                        const device::AcceleratorDevice* accelerator = nullptr;
                        int32_t device_id = -1;
                        device::CopyDirection direction;
                        if (src_device) {
                            accelerator = src_device;
                            device_id = src_info.device_id;
                            direction = device::CopyDirection::kDeviceToHost;
                            if (dst_device) {
                                direction =
                                    device::CopyDirection::kDeviceToDevice;
                            }
                        } else {
                            accelerator = dst_device;
                            device_id = dst_info.device_id;
                            direction = device::CopyDirection::kHostToDevice;
                        }
                        accelerator->SetContext(device_id);
                        if (!accelerator->Copy(op.dest, op.src, op.size,
                                               direction)) {
                            LOG(ERROR) << "GPU memcpy failed: src_dev="
                                       << src_info.device_id
                                       << " dst_dev=" << dst_info.device_id
                                       << " size=" << op.size;
                            ok = false;
                            break;
                        }
                    }
                    copied_bytes += op.size;
                    if (breakdown_log) {
                        const auto copy_end = std::chrono::steady_clock::now();
                        copy_us +=
                            std::chrono::duration_cast<std::chrono::microseconds>(
                                copy_end - copy_start)
                                .count();
                    }
                }

                VLOG(2) << "Memcpy task completed with "
                        << task.operations.size() << " operations"
                        << (ok ? "" : " (with GPU copy failure)");
                result = ok ? ErrorCode::OK : ErrorCode::TRANSFER_FAIL;
                task.state->set_completed(result);
            } catch (const std::exception& e) {
                LOG(ERROR) << "Exception during async memcpy: " << e.what();
                task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            }
            if (breakdown_log) {
                const auto worker_end = std::chrono::steady_clock::now();
                auto elapsed_us = [](auto begin, auto end) {
                    return std::chrono::duration_cast<std::chrono::microseconds>(
                               end - begin)
                        .count();
                };
                MC_LOG(INFO)
                    << "memcpy_worker_breakdown trace_id=" << task.trace_id
                    << " operations=" << task.operations.size()
                    << " bytes=" << copied_bytes
                    << " queued_ahead=" << task.queued_ahead
                    << " queue_wait_us="
                    << elapsed_us(task.submitted_at, worker_start)
                    << " registry_us="
                    << elapsed_us(worker_start, registry_end)
                    << " pointer_lookup_us=" << pointer_lookup_us
                    << " copy_us=" << copy_us
                    << " execute_us=" << elapsed_us(worker_start, worker_end)
                    << " total_us="
                    << elapsed_us(task.submitted_at, worker_end)
                    << " status=" << static_cast<int>(result);
            }
        }
    }

    VLOG(2) << "MemcpyWorkerPool worker thread exiting";
}

// ============================================================================
// TransferEngineOperationState Implementation
// ============================================================================

bool TransferEngineOperationState::is_completed() {
    [[maybe_unused]] auto trace = RestoreTraceIfMissing(trace_id());
    std::lock_guard<std::mutex> lock(mutex_);
    if (result_.has_value()) {
        return true;
    }

    check_task_status();
    return result_.has_value();
}

void TransferEngineOperationState::check_task_status() {
    [[maybe_unused]] auto trace = RestoreTraceIfMissing(trace_id());
    // Check all transfers in the batch.
    // Wait for ALL tasks to reach a terminal state before setting the result,
    // even if some have already failed. This prevents the caller from seeing
    // "completed" while background transfers are still in progress, which
    // could cause issues when freeBatchID is called in the destructor.
    bool all_terminated = true;
    std::vector<size_t> failed_task_ids;

    for (size_t i = 0; i < batch_size_; ++i) {
        TransferStatus status;
        Status s = engine_.getTransferStatus(batch_id_, i, status);
        if (!s.ok()) {
            LOG(ERROR) << "Failed to get transfer status for batch "
                       << batch_id_ << " task " << i << " with error "
                       << s.message();
            set_result_internal(ErrorCode::TRANSFER_FAIL);
            return;
        }

        switch (status.s) {
            case TransferStatusEnum::COMPLETED:
                // This transfer is done successfully
                break;
            case TransferStatusEnum::FAILED:
            case TransferStatusEnum::CANCELED:
            case TransferStatusEnum::INVALID:
#ifndef USE_ASCEND_DIRECT
                VLOG(1) << "Transfer failed for batch " << batch_id_ << " task "
                        << i << " with status " << static_cast<int>(status.s);
#endif
                failed_task_ids.push_back(i);
                break;
            default:
                // Transfer is still in progress (WAITING, PENDING, etc.)
                all_terminated = false;
                break;
        }
    }

    if (!all_terminated) {
        // Some tasks are still in progress; wait for next poll iteration.
        // Do NOT set result yet, even if some tasks have already failed.
        return;
    }

    // All tasks have reached a terminal state.
    // Per-member latency trace (sampled): durations come from
    // Slice::ts (post) / Slice::done_ts (completion) of each stripe
    // member's slices. Only multi-slice (striped) tasks are interesting;
    // single-slice tasks are skipped to keep the log focused.
    if (mooncake::logging::ShouldSampleHiFreqLog(trace_id())) {
        for (size_t i = 0; i < batch_size_; ++i) {
            std::vector<Transport::SliceTiming> timings;
            auto s = engine_.getSliceTimings(batch_id_, i, timings);
            if (!s.ok() || timings.size() < 2) continue;
            std::ostringstream os;
            os << "transfer_member_latency task=" << i
               << ", members=[";
            for (size_t m = 0; m < timings.size(); ++m) {
                const auto& t = timings[m];
                const int64_t dur_ns =
                    (t.done_ts > 0 && t.post_ts > 0) ? t.done_ts - t.post_ts
                                                     : 0;
                if (m) os << ", ";
                os << "{peer=" << t.peer_nic_path << ", bytes=" << t.length
                   << ", us=" << (dur_ns > 0 ? dur_ns / 1000 : -1) << "}";
            }
            os << "]";
            MC_LOG(INFO) << os.str();
        }
    }

    ErrorCode ec = ErrorCode::OK;
    if (!failed_task_ids.empty()) {
        std::ostringstream oss;
        for (size_t j = 0; j < failed_task_ids.size(); ++j) {
            if (j > 0) oss << ", ";
            oss << failed_task_ids[j];
        }
        LOG(ERROR) << "Batch " << batch_id_
                   << " completed with task failures: task_ids=[" << oss.str()
                   << "]";
        ec = ErrorCode::TRANSFER_FAIL;
    }

    set_result_internal(ec);
}

void TransferEngineOperationState::set_result_internal(ErrorCode error_code) {
    [[maybe_unused]] auto trace = RestoreTraceIfMissing(trace_id());
    if (result_.has_value()) {
        LOG(ERROR) << "Attempting to set result multiple times for batch "
                   << batch_id_
                   << ". Previous result: " << static_cast<int>(result_.value())
                   << ", attempted new result: " << static_cast<int>(error_code)
                   << ". This indicates a race condition or logic error.";
        return;  // Don't crash, just return early
    }

    VLOG(1) << "Setting transfer result for batch " << batch_id_ << " to "
            << static_cast<int>(error_code);
    result_.emplace(error_code);
}

void TransferEngineOperationState::wait_for_completion() {
    [[maybe_unused]] auto trace = RestoreTraceIfMissing(trace_id());
    if (is_completed()) {
        return;
    }

    // 60 seconds
    constexpr int64_t timeout_milliseconds = 60 * 1000;

#ifdef USE_EVENT_DRIVEN_COMPLETION
    VLOG(1) << "Waiting for transfer engine completion for batch " << batch_id_;

    // Wait directly on BatchDesc's condition variable.
    auto& batch_desc = Transport::toBatchDesc(batch_id_);
    bool completed;
    bool failed = false;

    // Fast path: if already finished, avoid taking the mutex and waiting.
    // Use acquire here to pair with the writer's release-store, because this
    // path may skip taking the mutex. It ensures all prior updates are visible.
    completed = batch_desc.is_finished.load(std::memory_order_acquire);
    if (!completed) {
        // Use the same mutex as the notifier when updating the predicate to
        // avoid missed notifications. The predicate is re-checked under the
        // lock. Under the mutex, relaxed is sufficient; the mutex acquire
        // orders prior writes.
        std::unique_lock<std::mutex> lock(batch_desc.completion_mutex);
        const int64_t elapsed_milliseconds =
            getCurrentTimeInMilli() - start_ts_;
        if (elapsed_milliseconds < timeout_milliseconds) {
            completed = batch_desc.completion_cv.wait_for(
                lock,
                std::chrono::milliseconds(timeout_milliseconds -
                                          elapsed_milliseconds),
                [&batch_desc] {
                    return batch_desc.is_finished.load(
                        std::memory_order_relaxed);
                });
        }
    }  // Explicitly release completion_mutex before acquiring mutex_

    // Once completion is observed, read failure flag.
    if (completed) {
        failed = batch_desc.has_failure.load(std::memory_order_relaxed);
    }

    ErrorCode error_code =
        completed ? (failed ? ErrorCode::TRANSFER_FAIL : ErrorCode::OK)
                  : ErrorCode::TRANSFER_FAIL;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        set_result_internal(error_code);
    }

    if (completed) {
        VLOG(1) << "Transfer engine operation completed for batch " << batch_id_
                << " with result: " << static_cast<int>(error_code);
    } else {
        LOG(ERROR) << "Failed to complete transfers after "
                   << timeout_milliseconds << " milliseconds for batch "
                   << batch_id_;
    }
#else
    VLOG(1) << "Starting transfer engine polling for batch " << batch_id_;

    const bool ub_transport = engine_.getTransport("ub") != nullptr;
    const auto sleep_time = std::chrono::microseconds(
        ub_transport ? UbPollSleepUs() : 0);
    const auto spin_time = std::chrono::microseconds(
        ub_transport ? UbPollSpinUs() : 0);
    auto spin_until = std::chrono::steady_clock::now() + spin_time;
    if (ub_transport) {
        LOG_FIRST_N(INFO, 1) << "UB completion polling: spin_us="
                             << spin_time.count()
                             << " sleep_us=" << sleep_time.count()
                             << " (MC_STORE_UB_POLL_SLEEP_US=0 restores "
                                "busy polling)";
    }

    while (true) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (result_.has_value()) return;
            if (getCurrentTimeInMilli() - start_ts_ > timeout_milliseconds) {
                LOG(ERROR) << "Failed to complete transfers after "
                           << timeout_milliseconds << " milliseconds for batch "
                           << batch_id_;
                set_result_internal(ErrorCode::TRANSFER_FAIL);
                return;
            }
            check_task_status();
            if (result_.has_value()) {
                VLOG(1) << "Transfer engine operation completed for batch "
                        << batch_id_
                        << " with result: " << static_cast<int>(result_.value());
                break;
            }
        }
        // Release mutex_ before sleeping so another observer can see progress.
        // This only observes completion; it never resubmits or caches metadata.
        if (sleep_time.count() > 0 &&
            std::chrono::steady_clock::now() >= spin_until) {
            std::this_thread::sleep_for(sleep_time);
            spin_until = std::chrono::steady_clock::now() + spin_time;
        }
    }
#endif
}

// ============================================================================
// TransferFuture Implementation
// ============================================================================

TransferFuture::TransferFuture(std::shared_ptr<OperationState> state)
    : state_(std::move(state)) {
    if (!state_) {
        LOG(ERROR) << "TransferFuture requires valid state";
        throw std::invalid_argument("TransferFuture requires valid state");
    }
}

bool TransferFuture::isReady() const { return state_->is_completed(); }

ErrorCode TransferFuture::wait() {
    [[maybe_unused]] auto trace = RestoreTraceIfMissing(state_->trace_id());
    if (!isReady()) {
        state_->wait_for_completion();
    }
    return state_->get_result();
}

ErrorCode TransferFuture::get() { return wait(); }

std::optional<NofTransferTiming> TransferFuture::nofTiming() const {
    return state_->nof_timing();
}

TransferStrategy TransferFuture::strategy() const {
    return state_->get_strategy();
}

// ============================================================================
// TransferSubmitter Implementation
// ============================================================================

TransferSubmitter::TransferSubmitter(TransferEngine& engine,
                                     std::shared_ptr<StorageBackend>& backend,
                                     const std::string& local_hostname,
                                     TransferMetric* transfer_metric,
                                     int numa_socket_id)
    : engine_(engine),
      local_endpoint_(engine.getLocalIpAndPort()),
      memcpy_pool_(nullptr),
#ifdef USE_NOF
      spdk_nvmf_pool_(SpdkNofWorkerPool::Acquire(numa_socket_id)),
#endif
      fileread_pool_(std::make_unique<FilereadWorkerPool>(backend)),
      local_hostname_(local_hostname),
      transfer_metric_(transfer_metric) {
    // Read MC_STORE_MEMCPY environment variable.
    // When not set, auto-detect based on transport type:
    //   - TCP-only environment: enable memcpy (avoids TCP loopback overhead)
    //   - RDMA/other transports: disable memcpy (RDMA is more efficient)
    const char* env_value = std::getenv("MC_STORE_MEMCPY");
    if (env_value == nullptr) {
        memcpy_enabled_ = engine_.isTcpOnly();
        LOG(INFO) << "MC_STORE_MEMCPY not set, auto-detected: "
                  << (memcpy_enabled_ ? "TCP-only environment, memcpy enabled"
                                      : "non-TCP transport available, memcpy "
                                        "disabled");
    } else {
        std::string env_str(env_value);
        // Convert to lowercase for case-insensitive comparison
        std::transform(env_str.begin(), env_str.end(), env_str.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (env_str == "false" || env_str == "0" || env_str == "no" ||
            env_str == "off") {
            memcpy_enabled_ = false;
        } else if (env_str == "true" || env_str == "1" || env_str == "yes" ||
                   env_str == "on") {
            memcpy_enabled_ = true;
        } else {
            LOG(WARNING) << "Invalid value for MC_STORE_MEMCPY: " << env_str
                         << ", defaulting to enabled";
            memcpy_enabled_ = true;
        }
    }

    // Keep workers for accelerator copies and for deployments that explicitly
    // disable inline host copies.
    if (memcpy_enabled_) {
        memcpy_pool_ = std::make_unique<MemcpyWorkerPool>();
    }

    VLOG(1) << "TransferSubmitter initialized with memcpy_enabled="
            << memcpy_enabled_
            << " inline_host_memcpy=" << IsInlineHostMemcpyEnabled();
}

std::optional<TransferFuture> TransferSubmitter::submit(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code, void* ptr, size_t size,
    const OperationOptions& options) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        if (!validateTransferParams(handle, slices)) {
            return std::nullopt;
        }

        if (op_code == TransferRequest::READ) {
            future = submitMemoryReadOperation(handle, slices, 0, options);
        } else {
            TransferStrategy strategy = selectStrategy(handle, slices);

            switch (strategy) {
                case TransferStrategy::LOCAL_MEMCPY:
                    future = submitMemcpyOperation(handle, slices, op_code);
                    break;
                case TransferStrategy::TRANSFER_ENGINE:
                    future = submitTransferEngineOperation(handle, slices,
                                                           op_code, 0, options);
                    break;
                default:
                    LOG(ERROR) << "Unknown transfer strategy: " << strategy;
                    return std::nullopt;
            }
        }
    } else if (replica.is_nof_replica()) {
#ifdef USE_NOF
        auto& ssd_desc = replica.get_nof_descriptor();
        auto& handle = ssd_desc.buffer_descriptor;

        if (!ptr || (size == 0)) {
            return std::nullopt;
        }

        future = submitSpdkNofOperation(handle, ptr, size, op_code);
#else
        LOG(ERROR) << "NoF transfer requested while USE_NOF is disabled";
        return std::nullopt;
#endif
    } else if (replica.is_vsegment_replica()) {
        LOG(ERROR) << "VSegment transfer requires an expanded transfer plan";
        return std::nullopt;
    } else {
        future = submitFileReadOperation(replica, slices, op_code);
    }

    // Update metrics on successful submission
    if (future.has_value()) {
        updateTransferMetrics(slices, op_code);
    }

    return future;
}

namespace {
ErrorCode WriteFileRange(const vsegment::EndpointTransferSubRequest& request,
                         const VSegmentDescriptor& replica) {
    try {
        coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config config{};
        config.connect_retry_count = 1;
        config.host_alive_detect_duration = std::chrono::milliseconds{0};
        coro_io::client_pools<coro_rpc::coro_rpc_client> pools(
            config, GetStoreRpcClientIoContextPool());
        auto pool = pools.at(request.persistence_endpoint);
        auto accelerator = device::GetAcceleratorRegistry().RuntimeAccelerators();
        for (uint64_t done = 0; done < request.transfer.length;) {
            const auto length = std::min<uint64_t>(
                request.transfer.length - done,
                vsegment::FileSegmentStore::kMaxWriteBytes);
            const auto* source = reinterpret_cast<const void*>(
                request.transfer.client_address + done);
            std::string data(length, '\0');
            device::PointerInfo info;
            if (auto* gpu = accelerator.FindDeviceForPointer(source, &info)) {
                gpu->SetContext(info.device_id);
                if (!gpu->Copy(data.data(), source, length,
                               device::CopyDirection::kDeviceToHost)) {
                    return ErrorCode::TRANSFER_FAIL;
                }
            } else {
                std::memcpy(data.data(), source, length);
            }
            const FileSegmentWriteRange range{
                replica, request.transfer.segment_id,
                request.segment_offset + done,
                request.transfer.client_buffer_offset + done, length};
            const auto error = async_simple::coro::syncAwait(
                [&]() -> async_simple::coro::Lazy<ErrorCode> {
                    auto sent = co_await pool->send_request(
                        [&](coro_io::client_reuse_hint,
                            coro_rpc::coro_rpc_client& client) {
                            return client.send_request<
                                &vsegment::FileSegmentStore::Write>(range, data);
                        });
                    if (!sent) co_return ErrorCode::RPC_FAIL;
                    auto result = co_await std::move(sent.value());
                    if (!result) co_return ErrorCode::RPC_FAIL;
                    co_return result->result();
                }());
            if (error != ErrorCode::OK) return error;
            done += length;
        }
        return ErrorCode::OK;
    } catch (...) {
        return ErrorCode::RPC_FAIL;
    }
}

class VSegmentOperationState final : public OperationState {
 public:
    std::vector<TransferFuture> children;
    bool submission_failed{false};
    bool is_completed() override {
        for (const auto& child : children)
            if (!child.isReady()) return false;
        wait_for_completion();
        return true;
    }
    void wait_for_completion() override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (result_) return;
        auto error = submission_failed ? ErrorCode::INVALID_PARAMS : ErrorCode::OK;
        for (auto& child : children) {
            const auto child_error = child.get();
            if (child_error != ErrorCode::OK) error = child_error;
        }
        result_ = error;
    }
    TransferStrategy get_strategy() const override { return TransferStrategy::SPDK_NVMF; }
};
}  // namespace

std::optional<TransferFuture> TransferSubmitter::submitVSegment(
    const vsegment::VSegmentTransferPlan& plan,
    TransferRequest::OpCode op_code) {
    if (!plan || plan.requests.empty()) return std::nullopt;
    const bool has_nof = std::any_of(plan.requests.begin(), plan.requests.end(),
        [](const auto& request) { return request.backend == vsegment::PSegmentBackend::NOF; });
    if (has_nof) {
#ifndef USE_NOF
        return std::nullopt;
#else
        // No mixed backend plans: validate before starting any I/O.
        if (std::any_of(plan.requests.begin(), plan.requests.end(),
            [](const auto& request) { return request.backend != vsegment::PSegmentBackend::NOF; }))
            return std::nullopt;
        auto state = std::make_shared<VSegmentOperationState>();
        for (const auto& request : plan.requests) {
            AllocatedBuffer::Descriptor handle{};
            handle.transport_endpoint_ = request.endpoint;
            handle.buffer_address_ = request.transfer.physical_offset;
            handle.size_ = request.transfer.length;
            auto child = submitSpdkNofOperation(handle,
                reinterpret_cast<void*>(request.transfer.client_address),
                request.transfer.length, op_code);
            if (!child) { state->submission_failed = true; break; }
            state->children.push_back(std::move(*child));
        }
        return TransferFuture(state);
#endif
    }
    std::vector<TransferRequest> requests;
    requests.reserve(plan.requests.size());
    bool has_file_writes = false;
    for (const auto& subrequest : plan.requests) {
        if (op_code == TransferRequest::WRITE &&
            !subrequest.persistence_endpoint.empty()) {
            has_file_writes = true;
            continue;
        }
        SegmentHandle segment = engine_.openSegment(subrequest.endpoint);
        if (segment == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "Failed to open vsegment member endpoint='"
                       << subrequest.endpoint << "'";
            return std::nullopt;
        }
        TransferRequest request;
        request.opcode = op_code;
        request.source = reinterpret_cast<void*>(
            subrequest.transfer.client_address);
        request.target_id = segment;
        request.target_offset = subrequest.transfer.physical_offset;
        request.length = subrequest.transfer.length;
        requests.push_back(request);
    }
    if (has_file_writes && !plan.file_write_replica) return std::nullopt;
    std::optional<TransferFuture> future;
    if (!requests.empty()) {
        future = submitTransfer(requests);
        if (!future) return std::nullopt;
    }
    if (has_file_writes) {
        auto result = future ? future->get() : ErrorCode::OK;
        if (result == ErrorCode::OK) {
            for (const auto& request : plan.requests) {
                if (request.persistence_endpoint.empty()) continue;
                result = WriteFileRange(request, *plan.file_write_replica);
                if (result != ErrorCode::OK) break;
            }
        }
        auto state = std::make_shared<MemcpyOperationState>();
        state->set_completed(result);
        future = TransferFuture(state);
    }
    if (future) {
        size_t bytes = 0;
        for (const auto& request : plan.requests)
            bytes += request.transfer.length;
        std::vector<Slice> metric_slices{{nullptr, bytes}};
        updateTransferMetrics(metric_slices, op_code);
    }
    return future;
}

std::optional<TransferFuture> TransferSubmitter::submit_batch(
    const std::vector<Replica::Descriptor>& replicas,
    std::vector<std::vector<Slice>>& all_slices,
    TransferRequest::OpCode op_code, const OperationOptions& options) {
    if (replicas.empty() || replicas.size() != all_slices.size()) {
        LOG(ERROR) << "Invalid batch transfer: replicas=" << replicas.size()
                   << " slice_groups=" << all_slices.size();
        return std::nullopt;
    }

    const uint64_t trace_id = mooncake::logging::CurrentTraceId();
    bool use_local_memcpy = memcpy_enabled_ && memcpy_pool_ != nullptr;
    size_t total_slices = 0;
    size_t total_bytes = 0;
    for (size_t i = 0; i < replicas.size(); ++i) {
        const auto& replica = replicas[i];
        const auto& slices = all_slices[i];
        if (!replica.is_memory_replica()) {
            LOG(ERROR) << "Batch transfer only supports memory replicas";
            return std::nullopt;
        }
        const auto& handle =
            replica.get_memory_descriptor().buffer_descriptor;
        for (const auto& slice : slices) {
            if (slice.ptr == nullptr) {
                LOG(ERROR) << "Batch transfer contains a null slice pointer";
                return std::nullopt;
            }
        }
        if (!validateTransferParams(handle, slices)) {
            return std::nullopt;
        }
        use_local_memcpy =
            use_local_memcpy &&
            selectStrategy(handle, slices) == TransferStrategy::LOCAL_MEMCPY;
        total_slices += slices.size();
        for (const auto& slice : slices) total_bytes += slice.size;
    }

    std::optional<TransferFuture> future;
    if (use_local_memcpy) {
        std::vector<MemcpyOperation> operations;
        operations.reserve(total_slices);
        for (size_t i = 0; i < replicas.size(); ++i) {
            const auto& handle =
                replicas[i].get_memory_descriptor().buffer_descriptor;
            uint64_t offset = 0;
            for (const auto& slice : all_slices[i]) {
                void* dest = nullptr;
                const void* src = nullptr;
                if (op_code == TransferRequest::READ) {
                    dest = slice.ptr;
                    src = reinterpret_cast<const void*>(
                        handle.buffer_address_ + offset);
                } else {
                    dest = reinterpret_cast<void*>(handle.buffer_address_ +
                                                   offset);
                    src = slice.ptr;
                }
                operations.emplace_back(dest, src, slice.size);
                offset += slice.size;
            }
        }
        future = submitMemcpyOperations(std::move(operations), trace_id);
    } else {
        std::vector<TransferRequest> requests;
        requests.reserve(total_slices);
        for (size_t i = 0; i < replicas.size(); ++i) {
            const auto& handle =
                replicas[i].get_memory_descriptor().buffer_descriptor;
            uint64_t offset = 0;
            SegmentHandle seg = engine_.openSegment(handle.transport_endpoint_);
            if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
                LOG(ERROR) << "Failed to open segment "
                           << handle.transport_endpoint_;
                return std::nullopt;
            }
            for (const auto& slice : all_slices[i]) {
                TransferRequest request;
                request.opcode = op_code;
                request.source = static_cast<char*>(slice.ptr);
                request.target_id = seg;
                request.target_offset = handle.buffer_address_ + offset;
                request.length = slice.size;
                requests.emplace_back(request);
                offset += slice.size;
            }
        }
        auto hint = options.scheduling.value_or(SchedulingHint{});
        if (!options.scheduling) {
            hint.intent = op_code == TransferRequest::READ
                              ? TaskIntent::FOREGROUND_GET
                              : TaskIntent::BACKGROUND_PUT;
        }
        future = submitTransfer(requests, hint);
    }

    if (mooncake::logging::ShouldSampleHiFreqLog(trace_id)) {
        MC_LOG(INFO) << "batch_transfer_strategy trace_id=" << trace_id
                     << " strategy="
                     << (use_local_memcpy ? TransferStrategy::LOCAL_MEMCPY
                                          : TransferStrategy::TRANSFER_ENGINE)
                     << " objects=" << replicas.size()
                     << " slices=" << total_slices << " bytes=" << total_bytes
                     << " op="
                     << (op_code == TransferRequest::READ ? "read" : "write");
    }
    // Update metrics on successful submission
    if (future.has_value()) {
        for (auto& slices : all_slices) {
            updateTransferMetrics(slices, op_code);
        }
    }
    return future;
}

std::optional<TransferFuture>
TransferSubmitter::submit_batch_get_offload_object(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys, const std::vector<uint64_t>& pointers,
    const std::unordered_map<std::string, std::vector<Slice>>& batched_slices) {
    std::optional<TransferFuture> future;
    std::vector<TransferRequest> requests;
    // Open the segment once — all keys share the same transfer_engine_addr.
    SegmentHandle seg = engine_.openSegment(transfer_engine_addr);
    if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
        LOG(ERROR) << "Failed to open segment " << transfer_engine_addr;
        // nullopt = failure (caller checks !future).  The function returns
        // std::optional so tl::unexpected is not available here.
        return std::nullopt;
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        const uint64_t pointer = pointers[i];
        auto it = batched_slices.find(key);
        if (it == batched_slices.end()) {
            LOG(ERROR) << "Key not found in batched_slices: " << key;
            return std::nullopt;  // fail closed
        }
        // Emit one TransferRequest per slice: the on-disk blob is read
        // sequentially while slices may point to non-contiguous GPU memory.
        uint64_t offset = 0;
        for (const auto& slice : it->second) {
            TransferRequest request;
            request.opcode = TransferRequest::READ;
            request.source = static_cast<char*>(slice.ptr);
            request.target_id = seg;
            request.target_offset = pointer + offset;
            request.length = slice.size;
            requests.emplace_back(request);
            offset += slice.size;
        }
    }
    return submitTransfer(requests);
}

std::optional<TransferFuture>
TransferSubmitter::submit_batch_push_offload_object(
    const std::string& requester_te_addr,
    const std::vector<std::string>& keys,
    const std::vector<uint64_t>& src_pointers,
    const std::vector<std::vector<OffloadDstSlice>>& dst_slices) {
    if (keys.size() != src_pointers.size() ||
        keys.size() != dst_slices.size()) {
        MC_LOG(ERROR) << "submit_batch_push_offload_object size mismatch: keys="
                      << keys.size() << ", src_pointers=" << src_pointers.size()
                      << ", dst_slices=" << dst_slices.size();
        return std::nullopt;
    }
    std::vector<TransferRequest> requests;
    // Open the requester's segment once — all keys share requester_te_addr.
    SegmentHandle seg = engine_.openSegment(requester_te_addr);
    if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
        MC_LOG(ERROR) << "Failed to open segment " << requester_te_addr;
        return std::nullopt;
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        // src is the owner's local ClientBuffer blob for this key (contiguous,
        // registered via FileStorage::RegisterLocalMemory). It is split across
        // the requester's possibly non-contiguous destination slices.
        const uint64_t src = src_pointers[i];
        uint64_t offset = 0;
        for (const auto& dst : dst_slices[i]) {
            TransferRequest request;
            request.opcode = TransferRequest::WRITE;
            request.source = reinterpret_cast<char*>(src + offset);
            request.target_id = seg;
            request.target_offset = dst.addr;
            request.length = dst.size;
            requests.emplace_back(request);
            offset += dst.size;
        }
    }
    return submitTransfer(requests);
}

std::optional<TransferFuture> TransferSubmitter::submitMemcpyOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code, uint64_t src_offset) {
    if (!memcpy_pool_) {
        LOG(ERROR) << "Memcpy operation selected while memcpy is disabled";
        return std::nullopt;
    }
    const uint64_t trace_id = mooncake::logging::CurrentTraceId();
    // Create memcpy operations
    std::vector<MemcpyOperation> operations;
    operations.reserve(slices.size());
    uint64_t base_address = static_cast<uint64_t>(handle.buffer_address_);
    uint64_t offset = src_offset;

    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];

        if (slice.ptr == nullptr) continue;

        void* dest;
        const void* src;

        if (op_code == TransferRequest::READ) {
            // READ: from handle (remote buffer) to slice (local buffer)
            dest = slice.ptr;
            src = reinterpret_cast<const void*>(base_address + offset);
        } else {
            // WRITE: from slice (local buffer) to handle (remote buffer)
            dest = reinterpret_cast<void*>(base_address + offset);
            src = slice.ptr;
        }
        offset += slice.size;

        operations.emplace_back(dest, src, slice.size);
    }

    return submitMemcpyOperations(std::move(operations), trace_id);
}

std::optional<TransferFuture> TransferSubmitter::submitMemcpyOperations(
    std::vector<MemcpyOperation> operations, uint64_t trace_id) {
    if (!memcpy_pool_) {
        LOG(ERROR) << "Memcpy operation selected while memcpy is disabled";
        return std::nullopt;
    }

    auto state = std::make_shared<MemcpyOperationState>(trace_id);
    if (IsInlineHostMemcpyEnabled()) {
        const bool breakdown_log =
            mooncake::logging::ShouldSampleHiFreqLog(trace_id);
        const auto inline_start =
            breakdown_log ? std::chrono::steady_clock::now()
                          : std::chrono::steady_clock::time_point{};
        auto registry_end = inline_start;
        int64_t pointer_lookup_us = 0;
        int64_t copy_us = 0;
        size_t copied_bytes = 0;
        bool host_only = true;

        try {
            auto runtime_accelerator =
                device::GetAcceleratorRegistry().RuntimeAccelerators();
            if (breakdown_log) {
                registry_end = std::chrono::steady_clock::now();
            }
            if (!runtime_accelerator.Devices().empty()) {
                for (const auto& op : operations) {
                    const auto lookup_start =
                        breakdown_log
                            ? std::chrono::steady_clock::now()
                            : std::chrono::steady_clock::time_point{};
                    auto* src_device =
                        runtime_accelerator.FindDeviceForPointer(op.src);
                    auto* dst_device =
                        runtime_accelerator.FindDeviceForPointer(op.dest);
                    if (breakdown_log) {
                        pointer_lookup_us += std::chrono::duration_cast<
                                                 std::chrono::microseconds>(
                                                 std::chrono::steady_clock::now() -
                                                 lookup_start)
                                                 .count();
                    }
                    if (src_device || dst_device) {
                        host_only = false;
                        break;
                    }
                }
            }

            if (host_only) {
                const auto copy_start =
                    breakdown_log
                        ? std::chrono::steady_clock::now()
                        : std::chrono::steady_clock::time_point{};
                for (const auto& op : operations) {
                    std::memcpy(op.dest, op.src, op.size);
                    copied_bytes += op.size;
                }
                const auto inline_end =
                    breakdown_log
                        ? std::chrono::steady_clock::now()
                        : std::chrono::steady_clock::time_point{};
                if (breakdown_log) {
                    copy_us =
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            inline_end - copy_start)
                            .count();
                }
                state->set_completed(ErrorCode::OK);
                if (breakdown_log) {
                    auto elapsed_us = [](auto begin, auto end) {
                        return std::chrono::duration_cast<
                                   std::chrono::microseconds>(end - begin)
                            .count();
                    };
                    MC_LOG(INFO)
                        << "memcpy_inline_breakdown trace_id=" << trace_id
                        << " operations=" << operations.size()
                        << " bytes=" << copied_bytes
                        << " registry_us="
                        << elapsed_us(inline_start, registry_end)
                        << " pointer_lookup_us=" << pointer_lookup_us
                        << " copy_us=" << copy_us
                        << " total_us="
                        << elapsed_us(inline_start, inline_end)
                        << " status=" << static_cast<int>(ErrorCode::OK);
                }
                return TransferFuture(state);
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception while classifying memcpy pointers: "
                       << e.what();
            state->set_completed(ErrorCode::TRANSFER_FAIL);
            return TransferFuture(state);
        }
    }

    MemcpyTask task(std::move(operations), state, trace_id);
    memcpy_pool_->submitTask(std::move(task));
    VLOG(1) << "Memcpy transfer submitted to worker pool";
    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitTransfer(
    std::vector<TransferRequest>& requests, const SchedulingHint& hint) {
    // Allocate batch ID
    const size_t batch_size = requests.size();
    BatchID batch_id = engine_.allocateBatchID(batch_size);
    if (batch_id == INVALID_BATCH_ID) {
        LOG(ERROR) << "Failed to allocate batch ID";
        return std::nullopt;
    }

    // Submit transfer
    std::vector<ScheduledTransferRequest> scheduled;
    scheduled.reserve(requests.size());
    for (const auto& request : requests) scheduled.push_back({request, hint});
    Status s = engine_.submitScheduledTransfer(batch_id, scheduled);
    if (!s.ok()) {
        LOG(ERROR) << "Failed to submit all transfers, error code is "
                   << s.code();
        // Note: batch_id will be freed by TransferEngineOperationState
        // destructor if we create the state object, otherwise we need to free
        // it here
        engine_.freeBatchID(batch_id);
        return std::nullopt;
    }

    if (batch_id == INVALID_BATCH_ID) {  // INVALID_BATCH_ID
        LOG(ERROR) << "Invalid batch ID for transfer engine operation";
        return std::nullopt;
    }

    // Create state with transfer engine context - no polling thread
    // needed
    auto state = std::make_shared<TransferEngineOperationState>(
        engine_, batch_id, batch_size, mooncake::logging::CurrentTraceId());

    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitTransferEngineOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code, uint64_t src_offset,
    const OperationOptions& options) {
    if (handle.transport_endpoint_.empty()) {
        LOG(ERROR) << "Transport endpoint is empty for handle with address "
                   << handle.buffer_address_;
        return std::nullopt;
    }
    SegmentHandle seg = engine_.openSegment(handle.transport_endpoint_);

    if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
        LOG(ERROR) << "Failed to open segment for endpoint='"
                   << handle.transport_endpoint_ << "'";
        return std::nullopt;
    }

    // Create transfer requests
    std::vector<TransferRequest> requests;
    requests.reserve(slices.size());
    uint64_t base_address = static_cast<uint64_t>(handle.buffer_address_);
    uint64_t offset = src_offset;

    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];
        if (slice.ptr == nullptr) continue;

        TransferRequest request;
        request.opcode = op_code;
        request.source = static_cast<char*>(slice.ptr);
        request.target_id = seg;
        request.target_offset = base_address + offset;
        request.length = slice.size;

        offset += slice.size;
        requests.emplace_back(request);
    }
    auto hint = options.scheduling.value_or(SchedulingHint{});
    if (!options.scheduling)
        hint.intent = op_code == TransferRequest::READ
                          ? TaskIntent::FOREGROUND_GET
                          : TaskIntent::BACKGROUND_PUT;
    return submitTransfer(requests, hint);
}

std::optional<TransferFuture> TransferSubmitter::submitMemoryReadOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    uint64_t src_offset, const OperationOptions& options) {
    TransferStrategy strategy = selectStrategy(handle, slices);

    if (strategy == TransferStrategy::LOCAL_MEMCPY) {
        return submitMemcpyOperation(handle, slices, TransferRequest::READ,
                                     src_offset);
    }
    if (strategy == TransferStrategy::TRANSFER_ENGINE) {
        return submitTransferEngineOperation(
            handle, slices, TransferRequest::READ, src_offset, options);
    }

    LOG(ERROR) << "Read only supports LOCAL_MEMCPY or TRANSFER_ENGINE, got: "
               << strategy;
    return std::nullopt;
}

std::optional<TransferFuture> TransferSubmitter::submitRangeRead(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    uint64_t src_offset) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        size_t slices_size = 0;
        for (const auto& s : slices) slices_size += s.size;
        if (src_offset + slices_size > handle.size_) {
            LOG(ERROR) << "Range read overflow: src_offset=" << src_offset
                       << " + slices_size=" << slices_size
                       << " > handle.size_=" << handle.size_;
            return std::nullopt;
        }

        future = submitMemoryReadOperation(handle, slices, src_offset);
    } else if (replica.is_nof_replica()) {
        LOG(ERROR) << "Range read not supported for NoF replicas";
        return std::nullopt;
    } else if (replica.is_disk_replica() || replica.is_local_disk_replica()) {
        LOG(ERROR)
            << "Range read not supported for disk replicas (use full read)";
        return std::nullopt;
    }

    if (future.has_value()) {
        updateTransferMetrics(slices, TransferRequest::READ);
    }

    return future;
}

#ifdef USE_NOF
std::optional<TransferFuture> TransferSubmitter::submitSpdkNofOperation(
    const AllocatedBuffer::Descriptor& handle, void* ptr, size_t size,
    const TransferRequest::OpCode op_code) {
    if (handle.transport_endpoint_.empty() || handle.size_ < size) {
        LOG(ERROR) << "Invalid NoF request endpoint="
                   << handle.transport_endpoint_
                   << ", buffer_size=" << handle.size_
                   << ", request_size=" << size;
        return std::nullopt;
    }

    nof_seg_handle* seg_handle =
        SpdkWrapper::GetInstance().OpenNofSegment(handle.transport_endpoint_);
    if (!seg_handle) {
        LOG(ERROR) << "Failed to open NoF segment endpoint="
                   << handle.transport_endpoint_;
        return std::nullopt;
    }

    uint32_t block_size = SpdkWrapper::GetInstance().GetBlockSize(seg_handle);
    if (block_size == INVALID_BLOCK_SIZE ||
        handle.buffer_address_ % block_size != 0 || size % block_size != 0 ||
        reinterpret_cast<std::uintptr_t>(ptr) % block_size != 0) {
        LOG(ERROR) << "NoF request offset=" << handle.buffer_address_
                   << ", ptr=" << ptr << ", size=" << size
                   << " is not aligned to block size " << block_size;
        return std::nullopt;
    }

    if (size / block_size > static_cast<size_t>(std::numeric_limits<int>::max()))
        return std::nullopt;
    const uint64_t trace_id = mooncake::logging::CurrentTraceId();
    const bool track_timing =
        mooncake::logging::ShouldSampleHiFreqLog(trace_id) ||
        GetSpdkNofSlowTraceThresholdUs() > 0;
    auto state =
        std::make_shared<SpdkNofOperationState>(trace_id, track_timing);
    SpdkNofTask task(seg_handle, ptr, handle.buffer_address_ / block_size,
                     size / block_size, op_code, state);
    spdk_nvmf_pool_->submitTask(std::move(task));

    VLOG(1) << "SPDK NoF transfer submitted to " << handle.transport_endpoint_;
    return TransferFuture(state);
}
#endif

std::optional<TransferFuture> TransferSubmitter::submitFileReadOperation(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code) {
    const uint64_t trace_id = mooncake::logging::CurrentTraceId();
    auto state = std::make_shared<FilereadOperationState>(trace_id);
    auto disk_replica = replica.get_disk_descriptor();
    std::string file_path = disk_replica.file_path;
    size_t file_length = disk_replica.object_size;

    // Submit memcpy operations to worker pool for async execution
    FilereadTask task(file_path, file_length, slices, state, trace_id);
    fileread_pool_->submitTask(std::move(task));

    VLOG(1) << "Fileread transfer submitted to worker pool with " << file_path;

    return TransferFuture(state);
}

TransferStrategy TransferSubmitter::selectStrategy(
    const AllocatedBuffer::Descriptor& handle,
    const std::vector<Slice>& slices) const {
    // Check if memcpy operations are enabled via environment variable
    if (!memcpy_enabled_) {
        VLOG(2) << "Memcpy operations disabled via MC_STORE_MEMCPY environment "
                   "variable";
        return TransferStrategy::TRANSFER_ENGINE;
    }

    // Check conditions for local memcpy optimization
    if (isLocalTransfer(handle)) {
        return TransferStrategy::LOCAL_MEMCPY;
    }

    return TransferStrategy::TRANSFER_ENGINE;
}

namespace {
// Helper function to extract IP address from endpoint string (ip:port format).
// Supports both IPv4 (ip:port) and IPv6 ([ipv6]:port) formats.
std::string extractIpAddress(const std::string& endpoint) {
    if (endpoint.empty()) {
        return "";
    }

    // Handle IPv6 format: [ipv6]:port
    if (endpoint[0] == '[') {
        size_t closing_bracket = endpoint.find(']');
        if (closing_bracket == std::string::npos) {
            LOG(WARNING) << "Invalid IPv6 endpoint format: " << endpoint;
            return "";
        }
        return endpoint.substr(1, closing_bracket - 1);
    }

    // Handle IPv4 or hostname:port format.
    size_t colon_pos = endpoint.rfind(':');
    if (colon_pos != std::string::npos) {
        return endpoint.substr(0, colon_pos);
    }

    // No colon found, return the whole string (might be just IP or hostname).
    return endpoint;
}
}  // namespace

bool TransferSubmitter::isSameProcessEndpoint(
    const std::string& handle_endpoint, const std::string& local_endpoint) {
    // Local memcpy requires that handle.buffer_address_ is a virtual address
    // valid in THIS process. Same host is not enough: two processes on the
    // same host share an IP but have distinct virtual address spaces, so a
    // memcpy on a peer process's address would segfault. Require the full
    // transport endpoint to match, which uniquely identifies the owning
    // process.
    if (handle_endpoint.empty() || local_endpoint.empty()) {
        return false;
    }
    if (handle_endpoint == local_endpoint) {
        return true;
    }

    const std::string handle_ip = extractIpAddress(handle_endpoint);
    const std::string local_ip = extractIpAddress(local_endpoint);
    if (!handle_ip.empty() && handle_ip == local_ip) {
        VLOG(2) << "Disabling local memcpy for same-host endpoints with "
                   "different process endpoints: handle="
                << handle_endpoint << ", local=" << local_endpoint;
    }

    return false;
}

bool TransferSubmitter::isLocalTransfer(
    const AllocatedBuffer::Descriptor& handle) const {
    return isSameProcessEndpoint(handle.transport_endpoint_, local_hostname_) ||
           isSameProcessEndpoint(handle.transport_endpoint_, local_endpoint_);
}

bool TransferSubmitter::validateTransferParams(
    const AllocatedBuffer::Descriptor& handle,
    const std::vector<Slice>& slices) const {
    uint64_t all_slice_len = 0;
    for (auto slice : slices) {
        all_slice_len += slice.size;
    }
    if (handle.size_ != all_slice_len) {
        LOG(ERROR) << "handles len:" << handle.size_
                   << ", all_slice_len:" << all_slice_len;
        return false;
    }
    return true;
}

void TransferSubmitter::updateTransferMetrics(const std::vector<Slice>& slices,
                                              TransferRequest::OpCode op_code) {
    size_t total_bytes = 0;
    for (const auto& slice : slices) {
        total_bytes += slice.size;
    }

    if (transfer_metric_ == nullptr) {
        return;
    }

    if (op_code == TransferRequest::READ) {
        transfer_metric_->total_read_bytes.inc(total_bytes);

    } else if (op_code == TransferRequest::WRITE) {
        transfer_metric_->total_write_bytes.inc(total_bytes);
    }
}

}  // namespace mooncake
