#pragma once

#include "otel_logs_service.h"
#include "otel_logs_shard_hash.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace NColumnShard::NOtelLogsToYdb {

/// Auto-create logs column table + TTL + compaction (subset of Go `schema_ensure`).
class TDdlEnsurer {
public:
    TDdlEnsurer(TServerConfig cfg);

    /// Creates table if missing (scheme error on upsert), then TTL + compaction JSON.
    bool EnsureLogsTable(NYdb::NTable::TTableClient& client, const TString& tablePath, ELogsPkSchema schema, TString* err);

private:
    TString BuildCreateDdl(const TString& tablePath, ELogsPkSchema schema) const;
    TString BuildTtlDdl(const TString& tablePath) const;
    TString BuildCompactionDdl(const TString& tablePath) const;
    TString BuildSubcolumnsDdl(const TString& tablePath) const;

    bool ExecScheme(
        NYdb::NTable::TTableClient& client,
        TStringBuf step,
        const TString& tablePath,
        const TString& yql,
        TString* err);

    struct TEnsureState {
        std::mutex Mu;
    };

    TServerConfig Cfg_;
    std::mutex Mu_;
    std::unordered_set<std::string> Ensured_;
    std::unordered_map<std::string, std::shared_ptr<TEnsureState>> EnsureStates_;
};

} // namespace NColumnShard::NOtelLogsToYdb
