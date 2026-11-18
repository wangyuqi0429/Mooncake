#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <chrono>
#include <array>
#include <map>
#include <mutex>
#include <thread>
#include <string>

#include "mooncake_logging.h"

namespace mooncake {

struct ReadPhaseDiagnostics {
    uint64_t trace_id = 0;
    int64_t resolve_us = 0;
    int64_t submit_us = 0;
    int64_t nof_prepare_us = 0;
    int64_t wait_us = 0;
    bool fallback = false;
    bool sampled = false;
};

inline thread_local ReadPhaseDiagnostics* current_read_phases = nullptr;

// Bounded logarithmic histograms, one sample per single or batch API call.
using ReadPhaseHistogram = std::array<std::array<uint64_t, 32>, 3>;
inline std::mutex read_histogram_mutex;
inline bool read_histograms_enabled = false;
inline std::map<std::thread::id, ReadPhaseHistogram> read_histograms;

// Synchronous Client transfer calls execute on the get_into caller thread.
// The worker's actual completion timestamps are reported separately.
class ScopedReadPhaseDiagnostics {
 public:
    explicit ScopedReadPhaseDiagnostics(ReadPhaseDiagnostics* value)
        : previous_(current_read_phases) {
        current_read_phases = value;
    }
    ~ScopedReadPhaseDiagnostics() {
        if (current_read_phases) {
            std::lock_guard<std::mutex> lock(read_histogram_mutex);
            if (read_histograms_enabled && !previous_) {
                auto& hist = read_histograms[std::this_thread::get_id()];
                const std::array<int64_t, 3> values{
                    current_read_phases->resolve_us,
                    current_read_phases->submit_us,
                    current_read_phases->wait_us};
                for (size_t phase = 0; phase < values.size(); ++phase) {
                    auto value = static_cast<uint64_t>(
                        std::max<int64_t>(0, values[phase]));
                    size_t bucket = 0;
                    while (value && bucket < 31) {
                        ++bucket;
                        value >>= 1;
                    }
                    ++hist[phase][bucket];
                }
            }
        }
        current_read_phases = previous_;
    }
 private:
    ReadPhaseDiagnostics* previous_;
};

inline void MarkReadFallback() {
    if (current_read_phases) current_read_phases->fallback = true;
}

inline void AddReadPhase(int64_t ReadPhaseDiagnostics::*field,
                         std::chrono::steady_clock::time_point start) {
    if (current_read_phases) {
        current_read_phases->*field +=
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start).count();
    }
}

// Store transfer and RPC breakdown diagnostics run on request hot paths.
// Require an explicit opt-in, then apply the process-wide high-frequency
// sampling rate as a second gate.
inline bool ShouldSampleStoreTransferDiagnostics(uint64_t trace_id) {
    static const bool enabled = [] {
        const char* value = std::getenv("MC_STORE_TRANSFER_DIAG");
        if (!value) return false;

        std::string normalized(value);
        std::transform(
            normalized.begin(), normalized.end(), normalized.begin(),
            [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return normalized == "1" || normalized == "true" ||
               normalized == "yes" || normalized == "on";
    }();
    return enabled && mooncake::logging::ShouldSampleHiFreqLog(trace_id);
}

}  // namespace mooncake
