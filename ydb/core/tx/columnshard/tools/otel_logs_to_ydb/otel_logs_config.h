#pragma once

#include "otel_logs_service.h"

#include <util/generic/string.h>

namespace NColumnShard::NOtelLogsToYdb {

/// Substitute `${env:NAME}` in the whole file before YAML parse.
void SubstituteEnvVars(TString* fileContent);

/// Load `otel_logs_to_ydb:` mapping into cfg.
bool LoadOtelLogsYaml(const TString& path, TServerConfig* cfg, TString* errorMsg);

} // namespace NColumnShard::NOtelLogsToYdb
