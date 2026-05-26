#include "otel_logs_capture.h"

#include <grpcpp/support/byte_buffer.h>

#include <util/folder/dirut.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

#include <vector>

namespace NColumnShard::NOtelLogsToYdb {

bool ByteBufferToTString(const grpc::ByteBuffer& buf, TString* out) {
    if (!buf.Valid() || buf.Length() == 0) {
        return false;
    }
    grpc::Slice slice;
    if (!buf.DumpToSingleSlice(&slice).ok()) {
        std::vector<grpc::Slice> slices;
        if (!buf.Dump(&slices).ok()) {
            return false;
        }
        size_t total = 0;
        for (const grpc::Slice& s : slices) {
            total += s.size();
        }
        out->clear();
        out->reserve(total);
        for (const grpc::Slice& s : slices) {
            out->append(reinterpret_cast<const char*>(s.begin()), s.size());
        }
        return !out->empty();
    }
    *out = TString(reinterpret_cast<const char*>(slice.begin()), slice.size());
    return true;
}

TExportRequestCapture::TExportRequestCapture(TString dir, size_t maxRequests)
    : Dir_(std::move(dir))
    , Max_(maxRequests)
{
    if (Max_ == 0 || Dir_.empty()) {
        Max_ = 0;
        return;
    }
    NFs::MakeDirectoryRecursive(Dir_);
}

bool TExportRequestCapture::TryCapture(const grpc::ByteBuffer& buf) {
    if (Max_ == 0) {
        return false;
    }
    const size_t idx = Saved_.load(std::memory_order_relaxed);
    if (idx >= Max_) {
        return false;
    }

    TString body;
    if (!ByteBufferToTString(buf, &body)) {
        return false;
    }

    std::lock_guard<std::mutex> g(WriteMu_);
    const size_t cur = Saved_.load(std::memory_order_relaxed);
    if (cur >= Max_) {
        return false;
    }

    const TString path = TStringBuilder() << Dir_ << "/export_" << Sprintf("%07zu", cur) << ".pb";
    {
        TFileOutput out(path);
        out.Write(body.data(), body.size());
        out.Finish();
    }

    Saved_.store(cur + 1, std::memory_order_relaxed);
    return true;
}

size_t TExportRequestCapture::SavedCount() const {
    return Saved_.load(std::memory_order_relaxed);
}

const TString& TExportRequestCapture::Dir() const {
    return Dir_;
}

} // namespace NColumnShard::NOtelLogsToYdb
