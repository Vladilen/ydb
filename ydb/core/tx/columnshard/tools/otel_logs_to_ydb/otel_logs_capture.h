#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <grpcpp/support/byte_buffer.h>

#include <atomic>
#include <mutex>

namespace NColumnShard::NOtelLogsToYdb {

/// Copies gRPC `Export` request body (serialized `ExportLogsServiceRequest`) to `{dir}/export_NNNNNNN.pb`.
class TExportRequestCapture {
public:
    TExportRequestCapture(TString dir, size_t maxRequests);

    /// Returns false when quota exhausted or write failed.
    bool TryCapture(const grpc::ByteBuffer& buf);

    size_t SavedCount() const;
    const TString& Dir() const;

private:
    TString Dir_;
    size_t Max_;
    std::atomic<size_t> Saved_{0};
    std::mutex WriteMu_;
};

bool ByteBufferToTString(const grpc::ByteBuffer& buf, TString* out);

} // namespace NColumnShard::NOtelLogsToYdb
