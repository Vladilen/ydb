#pragma once

#include "otel_logs_service.h"
#include "otel_logs_shard_hash.h"

#include <util/generic/string.h>

namespace NColumnShard::NOtelLogsToYdb {

struct TRoutedTable {
    TString TablePath;
    ELogsPkSchema PkSchema = ELogsPkSchema::PerService;
    bool Drop = false;
};

TRoutedTable ResolveLogsTable(const TServerConfig& cfg, const TString& project, const TString& service, const TString& cluster, bool perProjectLayout);

} // namespace NColumnShard::NOtelLogsToYdb
