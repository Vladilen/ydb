#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/system/types.h>

namespace NColumnShard::NOtelLogsToYdb {

/// One log row after OTLP parse; PK/hash fields match Arrow BulkUpsert columns.
struct TOwnedLogRow {
    TInstant Ts;
    TString Service;
    TString Cluster;
    TString RecordId;
    TString BatchId; // unique per gRPC Export request
    i32 Level = 0;
    TString Message;
    TString LabelsJson;
    TString MetaJson;
};

} // namespace NColumnShard::NOtelLogsToYdb
