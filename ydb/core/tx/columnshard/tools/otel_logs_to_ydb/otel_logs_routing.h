#pragma once

#include "otel_logs_service.h"
#include "otel_logs_shard_hash.h"

#include <util/generic/string.h>

#include <vector>

namespace NColumnShard::NOtelLogsToYdb {

struct TRoutedTable {
    TString TablePath;
    ELogsPkSchema PkSchema = ELogsPkSchema::PerService;
    bool Drop = false;
};

/// Returns 1 table normally, or 2 tables when the service is in BatchIdServices
/// (first entry = default schema, second = BatchPartitioned schema in the batch_id dir).
std::vector<TRoutedTable> ResolveLogsTables(const TServerConfig& cfg, const TString& project, const TString& service, const TString& cluster, bool perProjectLayout);

} // namespace NColumnShard::NOtelLogsToYdb
