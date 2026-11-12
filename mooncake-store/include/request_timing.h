// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <optional>

#include "transfer_task.h"

namespace mooncake {

// Thread-local stash for per-request timing segments.  Inner layers
// (master_client, client_service/TransferData) write their segments
// here; the outermost layer (real_client) resets it at request entry
// and reads it at exit to emit the single breakdown[op] line.
//
// Usage contract:
//   1. The request entry point calls resetRequestTiming().
//   2. Inner layers write their segments on the same thread.
//   3. The request exit point reads g_request_timing and emits.
// All fields default to 0/nullopt; missing segments are simply absent.
struct RequestTimingStash {
    // Query sub-segments (written by MasterClient::GetReplicaList).
    // query_route_us covers slot resolution + submaster selection +
    // connection establishment (the SwitchToSubmaster call).  In steady
    // state this is near zero; nonzero on first contact or migration.
    int64_t query_route_us = 0;
    // First-attempt RPC wire time (from RoutedCallTiming.rpc_first_us).
    int64_t query_rpc_us = 0;

    // Transfer sub-segments (written by TransferData after future->get()).
    // Contains per-slice wire times, submit/post/done timestamps.
    std::optional<UbTransferTiming> ub_timing;
    // Client-side submit time (slices + TE submit call), from TransferData.
    int64_t read_submit_us = 0;

    void reset() { *this = {}; }
};

inline thread_local RequestTimingStash g_request_timing;

inline void resetRequestTiming() { g_request_timing.reset(); }

}  // namespace mooncake
