#pragma once

#include "otel_logs_buck.h"
#include "otel_logs_owned_row.h"
#include "otel_logs_service.h"

#include <grpcpp/support/byte_buffer.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NColumnShard::NOtelLogsToYdb {

struct TWireExportParseStats {
    size_t LogRows = 0;
};

/// Stage A: parse Export wire → `TOwnedLogRow` buckets (no `ExportLogsServiceRequest` tree).
/// Returns false only when the buffer cannot be read at all; partial/empty export is ok (true).
bool ProcessExportWire(
    grpc::ByteBuffer* buf,
    const TServerConfig& cfg,
    THashMap<TBuck, TVector<TOwnedLogRow>, TBuckHash>* buckets,
    TWireExportParseStats* stats = nullptr);

} // namespace NColumnShard::NOtelLogsToYdb
