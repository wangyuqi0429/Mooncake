#include "cvm/cvm_http_server.h"

#include <algorithm>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "cvm/etcd_view_store.h"

namespace mooncake {
namespace cvm {

CvmHttpServer::CvmHttpServer(Config config)
    : config_(std::move(config)),
      server_(std::make_unique<coro_http::coro_http_server>(4, config_.port)) {
    InitRoutes();
}

CvmHttpServer::~CvmHttpServer() { Stop(); }

void CvmHttpServer::InitRoutes() {
    using namespace coro_http;

    server_->set_http_handler<GET>(
        "/segment_view",
        [this](coro_http_request& req, coro_http_response& resp) {
            (void)req;
            std::string json = GetSegmentViewJson();
            if (json.empty()) {
                resp.set_status_and_content(
                    status_type::not_found, "segment view not found");
                return;
            }
            resp.add_header("Content-Type", "application/json");
            resp.set_status_and_content(status_type::ok, json);
        });

    server_->set_http_handler<GET>(
        "/health", [](coro_http_request& req, coro_http_response& resp) {
            (void)req;
            resp.set_status_and_content(status_type::ok, "OK");
        });
}

ErrorCode CvmHttpServer::Start() {
    if (running_.load()) {
        return ErrorCode::OK;
    }

    // async_start() binds synchronously and returns a future that is already
    // resolved (hasResult()) when the bind failed. Mirrors
    // HttpMetadataServer::start().
    auto ec = server_->async_start();
    if (ec.hasResult()) {
        LOG(ERROR) << "CvmHttpServer failed to start on " << config_.host << ":"
                   << config_.port;
        return ErrorCode::RPC_FAIL;
    }
    running_.store(true);
    LOG(INFO) << "CvmHttpServer started on " << config_.host << ":"
              << config_.port;
    return ErrorCode::OK;
}

void CvmHttpServer::Stop() {
    if (!running_.exchange(false)) {
        return;
    }
    server_->stop();
    LOG(INFO) << "CvmHttpServer stopped";
}

std::string CvmHttpServer::GetSegmentViewJson() const {
    std::vector<SegmentDescriptor> descriptors;
    ViewVersionId desc_version = 0;
    ErrorCode err = EtcdViewStore::LoadAllSegmentDescriptors(
        config_.cluster_namespace, descriptors, desc_version);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "CvmHttpServer load segment descriptors failed: "
                     << err;
        return "";
    }

    std::vector<std::pair<std::string, MountEntry>> mounts;
    ViewVersionId mount_version = 0;
    err = EtcdViewStore::LoadAllMountEntries(config_.cluster_namespace, mounts,
                                             mount_version);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "CvmHttpServer load segment mounts failed: " << err;
        return "";
    }

    // 按照 segment_id 聚合：每个 segment 输出其描述符 + 挂载它的 master 列表。
    std::map<std::string, const SegmentDescriptor*> desc_by_id;
    for (const auto& desc : descriptors) {
        desc_by_id[desc.segment_id] = &desc;
    }

    std::map<std::string, std::vector<std::pair<std::string, const MountEntry*>>>
        mounts_by_seg;
    for (const auto& mount : mounts) {
        mounts_by_seg[mount.second.segment_id].emplace_back(mount.first,
                                                            &mount.second);
    }

    // 收集所有 segment_id（描述符 ∪ 挂载记录）并去重，保持稳定排序输出。
    std::vector<std::string> segment_ids;
    segment_ids.reserve(desc_by_id.size() + mounts_by_seg.size());
    for (const auto& kv : desc_by_id) {
        segment_ids.push_back(kv.first);
    }
    for (const auto& kv : mounts_by_seg) {
        segment_ids.push_back(kv.first);
    }
    std::sort(segment_ids.begin(), segment_ids.end());
    segment_ids.erase(std::unique(segment_ids.begin(), segment_ids.end()),
                      segment_ids.end());

    Json::Value root(Json::objectValue);
    Json::Value segments(Json::arrayValue);
    for (const auto& segment_id : segment_ids) {
        Json::Value seg(Json::objectValue);
        seg["segment_id"] = segment_id;

        auto desc_it = desc_by_id.find(segment_id);
        if (desc_it != desc_by_id.end()) {
            const SegmentDescriptor& desc = *desc_it->second;
            seg["segment_name"] = desc.segment_name;
            seg["capacity"] = static_cast<Json::Value::UInt64>(desc.capacity);
            seg["te_endpoint"] = desc.te_endpoint;
            seg["protocol"] = desc.protocol;
            seg["host_id"] = desc.host_id;
            // 资源事实（供 vsegment 自动发现 + 运维查看）
            seg["medium"] = desc.medium;
            seg["io_alignment"] =
                static_cast<Json::Value::UInt64>(desc.io_alignment);
            seg["supports_unaligned_io"] = desc.supports_unaligned_io;
            seg["failure_domain"] = desc.failure_domain;
            seg["vsegment_exclusive"] = desc.vsegment_exclusive;
        }

        Json::Value masters(Json::arrayValue);
        auto mount_it = mounts_by_seg.find(segment_id);
        if (mount_it != mounts_by_seg.end()) {
            for (const auto& m : mount_it->second) {
                Json::Value mount(Json::objectValue);
                mount["master_id"] = m.first;
                mount["mounted_at_ms"] =
                    static_cast<Json::Value::Int64>(m.second->mounted_at_ms);
                Json::Value slot_starts(Json::arrayValue);
                for (uint16_t s : m.second->partition_slot_starts) {
                    slot_starts.append(s);
                }
                mount["partition_slot_starts"] = slot_starts;
                masters.append(mount);
            }
        }
        seg["masters"] = masters;

        segments.append(seg);
    }
    root["segments"] = segments;

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

}  // namespace cvm
}  // namespace mooncake