#pragma once

#include <opentelemetry/proto/collector/logs/v1/logs_service.pb.h>

namespace NColumnShard::NOtelLogsToYdb {

/// Placeholder for future OTel payload validation (v1: always accepts; flag reserved for later).
inline bool RunOptionalLogValidation(bool /*enabled*/, const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest&) {
    return true;
}

} // namespace NColumnShard::NOtelLogsToYdb
