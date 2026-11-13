#include "config.h"
#include "http_transport.h"
#include "metrics.h"
#include "rate_gate.h"
#include "thread_pool.h"

#include "real_client.h"

#include <glog/logging.h>
#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

namespace mooncake::kvtest {
namespace {

std::atomic<bool> *g_running = nullptr;

void SignalHandler(int) {
    if (g_running) g_running->store(false, std::memory_order_release);
}

double ElapsedMs(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration<double, std::milli>(
               std::chrono::steady_clock::now() - start)
        .count();
}

void FillPattern(void *buffer, uint64_t size, int sender) {
    auto *bytes = static_cast<unsigned char *>(buffer);
    for (uint64_t i = 0; i < size; ++i) {
        bytes[i] = static_cast<unsigned char>((sender + i) & 0xff);
    }
}

bool VerifyPattern(const void *buffer, uint64_t actual_size,
                   uint64_t expected_size, int sender, const Config &cfg) {
    if (cfg.verify_level == "off") return true;
    if (actual_size != expected_size) return false;
    if (cfg.verify_level == "size") return true;
    const auto *bytes = static_cast<const unsigned char *>(buffer);
    auto verify_range = [&](uint64_t begin, uint64_t length) {
        for (uint64_t i = begin; i < begin + length; ++i) {
            if (bytes[i] != static_cast<unsigned char>((sender + i) & 0xff))
                return false;
        }
        return true;
    };
    if (cfg.verify_level == "full") return verify_range(0, actual_size);
    if (actual_size == 0) return true;
    const uint64_t sample =
        std::min(cfg.verify_sample_bytes, actual_size);
    if (!verify_range(0, sample)) return false;
    if (actual_size > sample &&
        !verify_range(actual_size - sample, sample))
        return false;
    for (uint64_t offset = cfg.verify_sample_step_bytes;
         offset + sample < actual_size; offset += cfg.verify_sample_step_bytes) {
        if (!verify_range(offset, sample)) return false;
    }
    return true;
}

std::string NormalizeOperation(const std::string &operation) {
    if (operation == "setStringView" || operation == "upsert") return "upsert";
    if (operation == "upsertFrom" || operation == "upsert_from")
        return "upsertFrom";
    if (operation == "getBuffer" || operation == "get_buffer")
        return "getBuffer";
    if (operation == "exist" || operation == "isExist") return "exist";
    if (operation == "mSet" || operation == "batchUpsertFrom" ||
        operation == "batch_upsert_from")
        return "batchUpsertFrom";
    if (operation == "mGet" || operation == "batchGetBuffer" ||
        operation == "batch_get_buffer")
        return "batchGetBuffer";
    if (operation == "del" || operation == "remove") return "remove";
    return {};
}

bool ValidateOperations(const std::vector<std::string> &operations,
                        const char *field, bool registered_buffer_available,
                        std::string *error) {
    for (const auto &operation : operations) {
        const std::string normalized = NormalizeOperation(operation);
        if (normalized.empty()) {
            *error = std::string("unsupported operation in ") + field + ": " +
                     operation;
            return false;
        }
        if (!registered_buffer_available &&
            (normalized == "upsertFrom" ||
             normalized == "batchUpsertFrom")) {
            *error = std::string(field) + " cannot contain " + operation +
                     ": notify workers do not own registered source buffers";
            return false;
        }
    }
    return true;
}

class RegisteredBuffer {
   public:
    RegisteredBuffer(std::shared_ptr<RealClient> client, uint64_t max_size,
                     size_t count)
        : client_(std::move(client)) {
        const uint64_t page = static_cast<uint64_t>(sysconf(_SC_PAGESIZE));
        stride_ = ((max_size + page - 1) / page) * page;
        if (stride_ == 0 || count > std::numeric_limits<size_t>::max() / stride_)
            return;
        size_ = static_cast<size_t>(stride_ * count);
        data_ = mmap(nullptr, size_, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (data_ == MAP_FAILED) {
            data_ = nullptr;
            return;
        }
        if (client_->register_buffer(data_, size_) != 0) {
            munmap(data_, size_);
            data_ = nullptr;
            return;
        }
        registered_ = true;
    }

    ~RegisteredBuffer() {
        if (registered_) client_->unregister_buffer(data_);
        if (data_) munmap(data_, size_);
    }

    RegisteredBuffer(const RegisteredBuffer &) = delete;
    RegisteredBuffer &operator=(const RegisteredBuffer &) = delete;

    bool Valid() const { return registered_; }
    void *At(size_t index) {
        return static_cast<char *>(data_) + stride_ * index;
    }
    void Fill(size_t count, uint64_t size, int sender) {
        for (size_t i = 0; i < count; ++i) FillPattern(At(i), size, sender);
    }

   private:
    std::shared_ptr<RealClient> client_;
    void *data_{nullptr};
    uint64_t stride_{0};
    size_t size_{0};
    bool registered_{false};
};

struct RequestContext {
    std::vector<std::string> keys;
    uint64_t size{0};
    int sender{0};
    RegisteredBuffer *buffer{nullptr};
};

class PipelineExecutor {
   public:
    PipelineExecutor(const Config &config, std::shared_ptr<RealClient> client,
                     MetricsCollector &metrics)
        : config_(config), client_(std::move(client)), metrics_(metrics) {
        replicate_.replica_num = config_.store.replica_num;
    }

    bool Execute(const std::vector<std::string> &operations,
                 RequestContext &context) {
        for (const auto &configured_name : operations) {
            const std::string operation = NormalizeOperation(configured_name);
            if (!ExecuteOne(configured_name, operation, context)) return false;
        }
        return true;
    }

   private:
    bool ExecuteOne(const std::string &metric_name,
                    const std::string &operation, RequestContext &context) {
        if (operation == "upsert") {
            bool all_ok = true;
            std::string owned;
            if (!context.buffer) {
                owned.resize(context.size);
                FillPattern(owned.data(), context.size, context.sender);
            }
            for (size_t i = 0; i < context.keys.size(); ++i) {
                const char *data = context.buffer
                                       ? static_cast<char *>(context.buffer->At(i))
                                       : owned.data();
                const auto start = std::chrono::steady_clock::now();
                const int rc = client_->upsert(
                    context.keys[i], std::span<const char>(data, context.size),
                    replicate_);
                metrics_.Record(metric_name, ElapsedMs(start), rc == 0,
                                context.size);
                all_ok &= rc == 0;
            }
            return all_ok;
        }
        if (operation == "upsertFrom") {
            if (!context.buffer) return false;
            bool all_ok = true;
            for (size_t i = 0; i < context.keys.size(); ++i) {
                const auto start = std::chrono::steady_clock::now();
                const int rc = client_->upsert_from(
                    context.keys[i], context.buffer->At(i), context.size,
                    replicate_);
                metrics_.Record(metric_name, ElapsedMs(start), rc == 0,
                                context.size);
                all_ok &= rc == 0;
            }
            return all_ok;
        }
        if (operation == "getBuffer") {
            bool all_ok = true;
            for (const auto &key : context.keys) {
                const auto start = std::chrono::steady_clock::now();
                auto handle = client_->get_buffer(key);
                const double latency = ElapsedMs(start);
                bool ok = handle != nullptr;
                bool verified = ok && VerifyPattern(handle->ptr(), handle->size(),
                                                     context.size, context.sender,
                                                     config_);
                if (!verified) metrics_.RecordVerifyFailure();
                if (!verified && config_.verify_fail_op) ok = false;
                metrics_.Record(metric_name, latency, ok, ok ? handle->size() : 0);
                all_ok &= ok;
            }
            return all_ok;
        }
        if (operation == "exist") {
            bool all_ok = true;
            for (const auto &key : context.keys) {
                const auto start = std::chrono::steady_clock::now();
                const int rc = client_->isExist(key);
                metrics_.Record(metric_name, ElapsedMs(start), rc > 0, 0);
                all_ok &= rc > 0;
            }
            return all_ok;
        }
        if (operation == "batchUpsertFrom") {
            if (!context.buffer) return false;
            std::vector<void *> buffers;
            std::vector<size_t> sizes(context.keys.size(), context.size);
            buffers.reserve(context.keys.size());
            for (size_t i = 0; i < context.keys.size(); ++i)
                buffers.push_back(context.buffer->At(i));
            const auto start = std::chrono::steady_clock::now();
            const auto results = client_->batch_upsert_from(
                context.keys, buffers, sizes, replicate_);
            const bool ok = results.size() == context.keys.size() &&
                            std::all_of(results.begin(), results.end(),
                                        [](int rc) { return rc == 0; });
            metrics_.Record(metric_name, ElapsedMs(start), ok,
                            context.size * context.keys.size());
            return ok;
        }
        if (operation == "batchGetBuffer") {
            const auto start = std::chrono::steady_clock::now();
            auto handles = client_->batch_get_buffer(context.keys);
            const double latency = ElapsedMs(start);
            bool ok = handles.size() == context.keys.size();
            uint64_t bytes = 0;
            for (const auto &handle : handles) {
                const bool verified =
                    handle && VerifyPattern(handle->ptr(), handle->size(),
                                            context.size, context.sender, config_);
                if (!verified) metrics_.RecordVerifyFailure();
                if (!handle || (!verified && config_.verify_fail_op)) ok = false;
                if (handle) bytes += handle->size();
            }
            metrics_.Record(metric_name, latency, ok, bytes);
            return ok;
        }
        if (operation == "remove") {
            const auto start = std::chrono::steady_clock::now();
            auto results = client_->batchRemove(context.keys, true);
            const bool ok = results.size() == context.keys.size() &&
                            std::all_of(results.begin(), results.end(),
                                        [](int rc) { return rc == 0; });
            metrics_.Record(metric_name, ElapsedMs(start), ok, 0);
            return ok;
        }
        return false;
    }

    const Config &config_;
    std::shared_ptr<RealClient> client_;
    MetricsCollector &metrics_;
    ReplicateConfig replicate_;
};

std::string EncodeNotify(const RequestContext &context) {
    Json::Value root(Json::objectValue);
    root["action"] = "";
    root["sender"] = context.sender;
    root["size"] = static_cast<Json::UInt64>(context.size);
    Json::Value keys(Json::arrayValue);
    for (const auto &key : context.keys) keys.append(key);
    root["keys"] = std::move(keys);
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

class NotifyDispatcher {
   public:
    NotifyDispatcher(const Config &config, PipelineExecutor &executor,
                     MetricsCollector &metrics)
        : config_(config),
          executor_(executor),
          metrics_(metrics),
          pool_(config.NumReadThreads(), config.notify_queue_max) {}

    bool Dispatch(const std::string &body) {
        Json::CharReaderBuilder reader;
        Json::Value root;
        std::string error;
        std::istringstream input(body);
        if (!Json::parseFromStream(reader, input, &root, &error)) return false;
        RequestContext context;
        context.sender = root.get("sender", 0).asInt();
        context.size = root.get("size", Json::UInt64(0)).asUInt64();
        if (root.isMember("keys")) {
            for (const auto &key : root["keys"])
                context.keys.push_back(key.asString());
        } else if (root.isMember("key")) {
            context.keys.push_back(root["key"].asString());
        }
        if (context.keys.empty() || context.size == 0) return false;
        const auto arrival = std::chrono::steady_clock::now();
        return pool_.Submit([this, context = std::move(context), arrival]() mutable {
            metrics_.Record("notify_queue_wait", ElapsedMs(arrival), true, 0);
            executor_.Execute(config_.notify_pipeline, context);
        });
    }

    size_t QueueSize() const { return pool_.QueueSize(); }
    uint64_t DroppedCount() const { return pool_.DroppedCount(); }
    void StopNow() { pool_.StopNow(); }
    void Stop() { pool_.Stop(); }

   private:
    const Config &config_;
    PipelineExecutor &executor_;
    MetricsCollector &metrics_;
    ThreadPool pool_;
};

class Writer {
   public:
    Writer(const Config &config, std::shared_ptr<RealClient> client,
           PipelineExecutor &executor, MetricsCollector &metrics)
        : config_(config),
          client_(std::move(client)),
          executor_(executor),
          metrics_(metrics),
          notify_pool_(config.notify_threads, config.notify_queue_max) {}

    bool Start(std::string *error) {
        const uint64_t max_size =
            *std::max_element(config_.data_sizes.begin(), config_.data_sizes.end());
        buffers_.reserve(config_.num_threads);
        for (int i = 0; i < config_.num_threads; ++i) {
            auto buffer = std::make_unique<RegisteredBuffer>(
                client_, max_size, config_.batch_keys_count);
            if (!buffer->Valid()) {
                *error = "failed to allocate/register writer buffer " +
                         std::to_string(i);
                return false;
            }
            buffers_.push_back(std::move(buffer));
        }
        running_.store(true, std::memory_order_release);
        for (int i = 0; i < config_.num_threads; ++i) {
            workers_.emplace_back([this, i] { Run(i); });
        }
        return true;
    }

    void RequestStop() { running_.store(false, std::memory_order_release); }

    void Stop() {
        RequestStop();
        for (auto &worker : workers_) {
            if (worker.joinable()) worker.join();
        }
        workers_.clear();
        notify_pool_.Stop();
        buffers_.clear();
    }

    size_t NotifyQueueSize() const { return notify_pool_.QueueSize(); }
    uint64_t NotifyDroppedCount() const { return notify_pool_.DroppedCount(); }

   private:
    void Run(int thread_id) {
        std::mt19937 rng(static_cast<uint32_t>(config_.instance_id * 1000 +
                                               thread_id));
        std::uniform_int_distribution<size_t> size_distribution(
            0, config_.data_sizes.size() - 1);
        RateGate gate;
        gate.Configure(config_.target_qps, thread_id, config_.num_threads,
                       config_.enable_jitter);
        gate.Reset(SteadyNowUs());
        uint64_t sequence = 0;
        while (running_.load(std::memory_order_acquire)) {
            if (gate.Active()) {
                const int64_t fire = gate.Next(SteadyNowUs(), rng);
                std::this_thread::sleep_until(
                    std::chrono::steady_clock::time_point(
                        std::chrono::microseconds(fire)));
                gate.Advance();
                if (!running_.load(std::memory_order_acquire)) break;
            }
            RequestContext context;
            context.sender = config_.instance_id;
            context.buffer = buffers_[thread_id].get();
            context.keys.reserve(config_.batch_keys_count);
            for (int i = 0; i < config_.batch_keys_count; ++i) {
                uint64_t key_index = sequence;
                if (config_.key_pool_size != 0) {
                    key_index =
                        (sequence * static_cast<uint64_t>(config_.num_threads) *
                             static_cast<uint64_t>(config_.batch_keys_count) +
                         static_cast<uint64_t>(thread_id) *
                             static_cast<uint64_t>(config_.batch_keys_count) +
                         static_cast<uint64_t>(i)) %
                        config_.key_pool_size;
                }
                context.keys.push_back(
                    "mooncake_kvtest_" +
                    std::to_string(config_.instance_id) + "_" +
                    (config_.key_pool_size == 0
                         ? std::to_string(thread_id) + "_" +
                               std::to_string(sequence) + "_" +
                               std::to_string(i)
                         : std::to_string(key_index)));
                if (i == 0) {
                    context.size = config_.key_pool_size == 0
                                       ? config_.data_sizes[size_distribution(rng)]
                                       : config_.data_sizes[
                                             key_index %
                                             config_.data_sizes.size()];
                }
            }
            context.buffer->Fill(context.keys.size(), context.size,
                                 context.sender);
            const bool ok = executor_.Execute(config_.pipeline, context);
            if (ok) Notify(context, rng);
            ++sequence;
        }
    }

    void Notify(const RequestContext &context, std::mt19937 &rng) {
        if (config_.notify_count == 0) return;
        std::vector<size_t> indexes(config_.peers.size());
        std::iota(indexes.begin(), indexes.end(), 0);
        std::shuffle(indexes.begin(), indexes.end(), rng);
        const std::string body = EncodeNotify(context);
        for (int i = 0; i < config_.notify_count; ++i) {
            const std::string peer = config_.peers[indexes[i]];
            notify_pool_.Submit([this, peer, body] {
                std::string error;
                const auto start = std::chrono::steady_clock::now();
                const bool ok = HttpPost(peer, "/notify", body, &error);
                metrics_.Record("notify_http", ElapsedMs(start), ok, body.size());
                if (!ok) {
                    const uint64_t failures = notify_failures_.fetch_add(
                                                  1, std::memory_order_relaxed) +
                                              1;
                    if (failures == 1 || failures % 1000 == 0) {
                        LOG(WARNING) << "notify failures=" << failures
                                     << " last_peer=" << peer
                                     << " last_error=" << error;
                    }
                }
            });
        }
    }

    const Config &config_;
    std::shared_ptr<RealClient> client_;
    PipelineExecutor &executor_;
    MetricsCollector &metrics_;
    ThreadPool notify_pool_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> notify_failures_{0};
    std::vector<std::thread> workers_;
    std::vector<std::unique_ptr<RegisteredBuffer>> buffers_;
};

}  // namespace
}  // namespace mooncake::kvtest

int main(int argc, char **argv) {
    using namespace mooncake;
    using namespace mooncake::kvtest;

    if (argc < 2 || argc > 3) {
        std::cerr << "Usage: " << argv[0]
                  << " <config.json> [--check-config]\n";
        return 2;
    }
    const bool check_only = argc == 3 && std::string(argv[2]) == "--check-config";
    Config config;
    std::string error;
    if (!LoadConfig(argv[1], &config, &error) ||
        !ValidateOperations(config.pipeline, "pipeline", true, &error) ||
        !ValidateOperations(config.notify_pipeline, "notify_pipeline", false,
                            &error)) {
        std::cerr << "Config error: " << error << '\n';
        return 2;
    }
    if (check_only) {
        std::cout << "Config OK: role=" << config.role
                  << " write_threads=" << config.num_threads
                  << " read_threads=" << config.NumReadThreads() << '\n';
        return 0;
    }
    if (!ApplyCpuAffinity(config.cpu_affinity, &error)) {
        std::cerr << "CPU affinity error: " << error << '\n';
        return 2;
    }

    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    std::filesystem::create_directories(config.output_dir);

    auto client = RealClient::create();
    const int setup_rc = client->setup_real(
        config.store.local_hostname, config.store.metadata_server,
        config.store.global_segment_size, config.store.local_buffer_size,
        config.store.protocol, config.store.rdma_devices,
        config.store.master_server_address, nullptr, "", false, "",
        config.store.tenant_id);
    if (setup_rc != 0) {
        LOG(ERROR) << "Mooncake client setup failed: " << setup_rc;
        google::ShutdownGoogleLogging();
        return 1;
    }

    std::atomic<bool> running{true};
    g_running = &running;
    std::signal(SIGTERM, SignalHandler);
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGPIPE, SIG_IGN);

    MetricsCollector metrics(config.instance_id, config.metrics_interval_ms,
                             config.output_dir);
    metrics.Start();
    PipelineExecutor executor(config, client, metrics);
    NotifyDispatcher dispatcher(config, executor, metrics);
    std::unique_ptr<Writer> writer;
    if (config.role == "writer" || config.role == "dual") {
        writer = std::make_unique<Writer>(config, client, executor, metrics);
    }

    HttpServer server(
        config.listen_port, 4,
        [&](const std::string &method, const std::string &path,
            const std::string &body) -> HttpResponse {
            if (method == "POST" && path == "/notify") {
                return dispatcher.Dispatch(body)
                           ? HttpResponse{200, "text/plain", "ok"}
                           : HttpResponse{400, "text/plain", "invalid notify"};
            }
            if (method == "GET" && path == "/stats") {
                return {200, "application/json", metrics.StatsJson()};
            }
            if (method == "GET" && path == "/health") {
                return {200, "text/plain", "ok"};
            }
            if (method == "POST" && path == "/summary") {
                metrics.WriteSummary();
                return {200, "text/plain", "ok"};
            }
            if (method == "POST" && path == "/stop") {
                running.store(false, std::memory_order_release);
                dispatcher.StopNow();
                if (writer) writer->RequestStop();
                return {200, "text/plain", "stopping"};
            }
            return {404, "text/plain", "not found"};
        });
    int exit_code = 0;
    if (!server.Start(&error)) {
        LOG(ERROR) << "HTTP server failed: " << error;
        exit_code = 1;
        running.store(false, std::memory_order_release);
    }
    if (running.load(std::memory_order_acquire) && writer &&
        !writer->Start(&error)) {
        LOG(ERROR) << error;
        exit_code = 1;
        running.store(false, std::memory_order_release);
    }

    LOG(INFO) << "mooncake_kvtest started role=" << config.role
              << " instance=" << config.instance_id
              << " write_threads=" << config.num_threads
              << " read_threads=" << config.NumReadThreads()
              << " listen_port=" << config.listen_port;

    std::vector<MetricSnapshot> previous;
    auto last_report = std::chrono::steady_clock::now();
    while (running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        const auto now = std::chrono::steady_clock::now();
        const double seconds =
            std::chrono::duration<double>(now - last_report).count();
        if (seconds < 3.0) continue;
        last_report = now;
        const auto current = metrics.SnapshotCounts();
        std::ostringstream rates;
        for (const auto &metric : current) {
            auto it = std::find_if(previous.begin(), previous.end(),
                                   [&](const auto &old) {
                                       return old.name == metric.name;
                                   });
            const uint64_t old_total = it == previous.end() ? 0 : it->total;
            if (rates.tellp() > 0) rates << ", ";
            rates << metric.name << '='
                  << static_cast<uint64_t>((metric.total - old_total) / seconds)
                  << "/s";
        }
        previous = current;
        LOG(INFO) << '[' << rates.str() << "] [notify_in_q="
                  << dispatcher.QueueSize() << ", notify_in_dropped="
                  << dispatcher.DroppedCount() << ", notify_out_q="
                  << (writer ? writer->NotifyQueueSize() : 0)
                  << ", notify_out_dropped="
                  << (writer ? writer->NotifyDroppedCount() : 0) << ']';
    }

    if (writer) writer->RequestStop();
    dispatcher.StopNow();
    server.Stop();
    if (writer) writer->Stop();
    dispatcher.Stop();
    metrics.Stop();
    client->tearDownAll();
    g_running = nullptr;
    LOG(INFO) << "mooncake_kvtest stopped";
    google::ShutdownGoogleLogging();
    return exit_code;
}
