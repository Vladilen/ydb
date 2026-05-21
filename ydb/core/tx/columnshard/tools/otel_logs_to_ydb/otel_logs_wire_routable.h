#pragma once

namespace grpc {
class ByteBuffer;
}

namespace NColumnShard::NOtelLogsToYdb {

struct TServerConfig;

/// Lightweight wire scan (no full ExportLogsServiceRequest parse).
/// Returns false only when the message was scanned and has no routable log rows.
/// Returns true to accept into the ingest queue (routable, or fail-open on wire errors).
bool ExportWireHasRoutableLogRows(grpc::ByteBuffer* buf, const TServerConfig& cfg);

} // namespace NColumnShard::NOtelLogsToYdb
