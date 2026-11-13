# Mooncake kvtest

`mooncake_kvtest` is a C++ writer/reader pipeline benchmark modeled after the
YuanRong DataSystem `tests/kvtest` process and configuration model. It keeps
HTTP notification handling, the notification queue, Mooncake calls, and metric
collection in C++; Python is not part of the request path.

## Build

Enable the normal Mooncake benchmark targets:

```bash
cmake -S . -B build -DBUILD_BENCHMARK=ON -DWITH_STORE_RUST=OFF
cmake --build build --target mooncake_kvtest -j
```

The binary is generated under the CMake benchmark output directory. Validate a
configuration without connecting to Mooncake:

```bash
./mooncake_kvtest reader.json --check-config
```

## Roles and threads

- `role=writer` starts `num_threads` rate-limited pipeline threads.
- `role=dual` starts the same writer threads and also receives notifications;
  this matches Mooncake-Performance rank/recall's dual-client deployment.
- `role=reader` only receives notifications.
- Every process creates `num_total_threads - num_threads` C++ notification
  workers. An HTTP handler only parses and enqueues a notification; a worker
  runs `notify_pipeline` independently.
- `notify_threads` controls the writer's outbound HTTP pool.

The reader therefore accepts later notifications while earlier Mooncake reads
are still running. There is no CPython GIL, but CPU scheduling, C++ locks,
queueing, and Mooncake transfer workers still contribute to latency.

For the existing Mooncake-Performance smoke shapes, use `role=dual`,
`pipeline=["upsertFrom"]`, and `notify_pipeline=["getBuffer"]`. Rank uses
8 MB values, `notify_count=10`, and an outbound pool of 10; recall uses 3.5 MB
values and `notify_count=1`. Split each node's configured QPS across its dual
processes with integer totals (for example, rank's 105 QPS across two
processes is 53 + 52). Set `num_total_threads` to the writer-thread count plus
the desired C++ reader-worker count.

`key_pool_size=0` keeps YuanRong's normal pipeline behavior and continuously
creates new keys. Set it to a positive process-wide key count to match
Mooncake-Performance's capacity-bounded rank/recall behavior. A bounded pool
must contain at least `num_threads * batch_keys_count` keys and be divisible
by that value; key sizes remain stable when multiple `data_sizes` are
configured.

## Operations

| Mooncake operation | Compatible aliases | SDK call |
| --- | --- | --- |
| `upsert` | `setStringView` | `RealClient::upsert` |
| `upsertFrom` | `upsert_from` | `RealClient::upsert_from` with a per-writer registered buffer |
| `getBuffer` | `get_buffer` | `RealClient::get_buffer` |
| `exist` | `isExist` | `RealClient::isExist` |
| `batchUpsertFrom` | `batch_upsert_from`, `mSet` | `RealClient::batch_upsert_from` |
| `batchGetBuffer` | `batch_get_buffer`, `mGet` | `RealClient::batch_get_buffer` |
| `remove` | `del` | `RealClient::batchRemove` |

`createBuffer`, `memoryCopy`, and `setBuffer` are intentionally not emulated:
Mooncake registered-memory ownership is different from DataSystem's published
Buffer lifecycle. Use `upsertFrom` for the zero-copy writer path.

## Control endpoints

- `POST /notify`: enqueue `{keys, sender, size}` for `notify_pipeline`.
- `GET /stats`: cumulative JSON counters.
- `GET /health`: liveness response.
- `POST /summary`: write `summary.json` immediately.
- `POST /stop`: stop accepting pipeline work and begin graceful shutdown.

The output directory contains `latency_timeseries.csv` and `summary.json`.
Interface latency surrounds only the corresponding Mooncake SDK call. Data
verification is performed after the timed call. `notify_queue_wait` is reported
separately.

## Configuration

Start readers before writers. `nodes` is compatible with YuanRong kvtest;
writers automatically select entries whose `role` is `reader` or `dual`.
Explicit `peers` values such as `http://10.0.0.2:9001` override node
discovery.

The Mooncake-specific `store` object maps directly to `RealClient::setup_real`.
Environment variables such as `MC_WORKERS_PER_CTX` are still consumed by the
Mooncake transfer engine before setup.

See `config/reader.json.example` and `config/writer.json.example`.
