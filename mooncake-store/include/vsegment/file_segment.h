#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <unistd.h>
#include <sys/xattr.h>
#include <ylt/util/tl/expected.hpp>
#include "types.h"

namespace mooncake::vsegment {

inline tl::expected<UUID, ErrorCode> LoadFileSegmentId(int fd, uint64_t offset,
                                                     uint64_t length) {
    const auto key = "user.mooncake.segment." + std::to_string(offset) + "." + std::to_string(length);
    std::string text(128, '\0');
    const auto count = fgetxattr(fd, key.c_str(), text.data(), text.size());
    if (count < 0 && errno == ENODATA) {
        text = UuidToString(generate_uuid());
        if (fsetxattr(fd, key.c_str(), text.data(), text.size(), XATTR_CREATE) != 0)
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        int status;
        do { status = fsync(fd); } while (status != 0 && errno == EINTR);
        if (status != 0) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    } else if (count > 0) {
        text.resize(static_cast<size_t>(count));
    } else {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    UUID id;
    if (!StringToUuid(text, id)) return tl::make_unexpected(ErrorCode::INVALID_VERSION);
    return id;
}

struct FileSegmentFd {
    int fd{-1};
    ~FileSegmentFd() { if (fd >= 0) close(fd); }
};

// Owner-side persistence barrier. Keep the registry lock while using mappings:
// unregister/munmap must wait for an in-progress barrier.
class FileSegmentStore {
 public:
    struct Region {
        void* address;
        uint64_t length;
        uint64_t file_offset;
        std::shared_ptr<FileSegmentFd> file;
    };
    bool Register(const std::string& id, Region region) {
        std::lock_guard<std::mutex> lock(mutex_);
        return regions_.emplace(id, std::move(region)).second;
    }
    void Unregister(const std::string& id) {
        std::lock_guard<std::mutex> lock(mutex_);
        regions_.erase(id);
    }
    ErrorCode Persist(const std::string& id, uint64_t offset, uint64_t length) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = regions_.find(id);
        if (it == regions_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        const auto& region = it->second;
        if (length == 0 || offset > region.length || length > region.length - offset ||
            region.file_offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) - offset ||
            length > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) - region.file_offset - offset)
            return ErrorCode::INVALID_PARAMS;
        // DMA writes need not dirty filesystem pages. Explicit pwrite makes the
        // affected bytes visible to filesystem writeback before fdatasync.
        constexpr size_t kChunk = 1024 * 1024;
        std::vector<char> buffer(static_cast<size_t>(std::min<uint64_t>(length, kChunk)));
        uint64_t done = 0;
        while (done < length) {
            const auto count = static_cast<size_t>(std::min<uint64_t>(buffer.size(), length - done));
            std::memcpy(buffer.data(), static_cast<const char*>(region.address) + offset + done, count);
            size_t written = 0;
            while (written < count) {
                auto n = pwrite(region.file->fd, buffer.data() + written, count - written,
                                region.file_offset + offset + done + written);
                if (n < 0 && errno == EINTR) continue;
                if (n <= 0) return ErrorCode::INTERNAL_ERROR;
                written += static_cast<size_t>(n);
            }
            done += count;
        }
        int status;
        do { status = fdatasync(region.file->fd); } while (status != 0 && errno == EINTR);
        return status == 0 ? ErrorCode::OK : ErrorCode::INTERNAL_ERROR;
    }
 private:
    std::mutex mutex_;
    std::unordered_map<std::string, Region> regions_;
};
}  // namespace mooncake::vsegment
