#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unistd.h>
#include <sys/xattr.h>
#include <ylt/util/tl/expected.hpp>
#include "replica.h"

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

// Serialize admission and writes at the file owner. A new writer cannot pass
// an earlier admitted write even when the master has released its reservation.
class FileSegmentStore {
 public:
    using WriteValidator = std::function<ErrorCode(const FileSegmentWriteRange&)>;
    static constexpr size_t kMaxWriteBytes = 1 << 20;
    struct Region {
        uint64_t length;
        uint64_t file_offset;
        std::shared_ptr<FileSegmentFd> file;
    };
    bool Register(const std::string& id, Region region) {
        std::lock_guard<std::mutex> lock(mutex_);
        return regions_.emplace(id, std::move(region)).second;
    }
    void SetWriteValidator(WriteValidator validator) {
        std::lock_guard lock(mutex_);
        write_validator_ = std::move(validator);
    }
    void Unregister(const std::string& id) {
        std::lock_guard<std::mutex> lock(mutex_);
        regions_.erase(id);
    }
    ErrorCode Write(const FileSegmentWriteRange& range, const std::string& data) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = regions_.find(range.segment_id);
        if (it == regions_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        const auto& region = it->second;
        if (data.size() != range.length || data.size() > kMaxWriteBytes ||
            !ValidRange(region, range.segment_offset, range.length)) {
            return ErrorCode::INVALID_PARAMS;
        }
        if (!write_validator_) return ErrorCode::INVALID_WRITE;
        const auto validation = write_validator_(range);
        if (validation != ErrorCode::OK) return validation;
        return PersistRegion(region, range.segment_offset, data);
    }

 private:
    static bool ValidRange(const Region& region, uint64_t offset,
                           uint64_t length) {
        if (length == 0 || offset > region.length || length > region.length - offset ||
            offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) ||
            region.file_offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) - offset ||
            length > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) - region.file_offset - offset)
            return false;
        return true;
    }
    static ErrorCode PersistRegion(const Region& region, uint64_t offset,
                                   const std::string& data) {
        size_t written = 0;
        while (written < data.size()) {
            auto n = pwrite(region.file->fd, data.data() + written,
                            data.size() - written,
                            region.file_offset + offset + written);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return ErrorCode::INTERNAL_ERROR;
            written += static_cast<size_t>(n);
        }
        int status;
        do { status = fdatasync(region.file->fd); } while (status != 0 && errno == EINTR);
        return status == 0 ? ErrorCode::OK : ErrorCode::INTERNAL_ERROR;
    }
    std::mutex mutex_;
    std::unordered_map<std::string, Region> regions_;
    WriteValidator write_validator_;
};
}  // namespace mooncake::vsegment
