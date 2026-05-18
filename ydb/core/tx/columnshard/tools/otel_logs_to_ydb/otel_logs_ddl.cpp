#include "otel_logs_ddl.h"

#include <util/generic/strbuf.h>
#include <util/string/builder.h>

namespace NColumnShard::NOtelLogsToYdb {

namespace {

const char* DefaultCompactionJson = R"({"levels": [
   {"class_name": "Zero", "expected_blobs_size": 2097152, "portions_count_limit": 15000000, "portions_live_duration": "180s", "concurrency": 2},
   {"class_name": "Zero", "expected_blobs_size": 8388608, "portions_count_limit": 15000000}
], "node_portions_count_limit": 15000000})";

} // namespace

TDdlEnsurer::TDdlEnsurer(TServerConfig cfg)
    : Cfg_(std::move(cfg))
{
}

TString TDdlEnsurer::BuildCreateDdl(const TString& tablePath, ELogsPkSchema schema) const {
    const TStringBuf cm(Cfg_.LogsCompressionMessage.data(), Cfg_.LogsCompressionMessage.size());
    const TStringBuf cl(Cfg_.LogsCompressionLabels.data(), Cfg_.LogsCompressionLabels.size());
    const TStringBuf cz(Cfg_.LogsCompressionMeta.data(), Cfg_.LogsCompressionMeta.size());
    const int pc = (schema == ELogsPkSchema::Dedicated) ? Cfg_.PartitionCountDedicated : Cfg_.PartitionCountCommon;
    switch (schema) {
        case ELogsPkSchema::PerProjectHeap:
            return TStringBuilder() << "CREATE TABLE `" << tablePath << "` (\n"
                                     << "    timestamp Timestamp NOT NULL,\n"
                                     << "    service Utf8 NOT NULL,\n"
                                     << "    cluster Utf8 NOT NULL,\n"
                                     << "    record_id Utf8 NOT NULL,\n"
                                     << "    level Int32,\n"
                                     << "    message Utf8" << cm << ",\n"
                                     << "    labels JsonDocument" << cl << ",\n"
                                     << "    meta JsonDocument" << cz << ",\n"
                                     << "    PRIMARY KEY (timestamp, service, cluster, record_id)\n"
                                     << ") PARTITION BY HASH (timestamp, service, cluster, record_id)\n"
                                     << "WITH (\n"
                                     << "    STORE = COLUMN,\n"
                                     << "    PARTITION_COUNT = " << pc << "\n"
                                     << ");\n";
        case ELogsPkSchema::Dedicated:
            return TStringBuilder() << "CREATE TABLE `" << tablePath << "` (\n"
                                     << "    timestamp Timestamp NOT NULL,\n"
                                     << "    record_id Utf8 NOT NULL,\n"
                                     << "    level Int32,\n"
                                     << "    message Utf8" << cm << ",\n"
                                     << "    labels JsonDocument" << cl << ",\n"
                                     << "    meta JsonDocument" << cz << ",\n"
                                     << "    PRIMARY KEY (timestamp, record_id)\n"
                                     << ") PARTITION BY HASH (timestamp, record_id)\n"
                                     << "WITH (\n"
                                     << "    STORE = COLUMN,\n"
                                     << "    PARTITION_COUNT = " << pc << "\n"
                                     << ");\n";
        default:
            return TStringBuilder() << "CREATE TABLE `" << tablePath << "` (\n"
                                     << "    timestamp Timestamp NOT NULL,\n"
                                     << "    cluster Utf8 NOT NULL,\n"
                                     << "    record_id Utf8 NOT NULL,\n"
                                     << "    level Int32,\n"
                                     << "    message Utf8" << cm << ",\n"
                                     << "    labels JsonDocument" << cl << ",\n"
                                     << "    meta JsonDocument" << cz << ",\n"
                                     << "    PRIMARY KEY (timestamp, cluster, record_id)\n"
                                     << ") PARTITION BY HASH (timestamp, cluster, record_id)\n"
                                     << "WITH (\n"
                                     << "    STORE = COLUMN,\n"
                                     << "    PARTITION_COUNT = " << pc << "\n"
                                     << ");\n";
    }
}

TString TDdlEnsurer::BuildTtlDdl(const TString& tablePath) const {
    TString del = TString{Cfg_.TtlDeleteIntervalLiteral.data(), Cfg_.TtlDeleteIntervalLiteral.size()};
    if (del.empty()) {
        del = TString{"P30D"};
    }
    if (!Cfg_.TtlExternalPath.empty() && !Cfg_.TtlExternalTierLiteral.empty()) {
        const TStringBuf tier(Cfg_.TtlExternalTierLiteral.data(), Cfg_.TtlExternalTierLiteral.size());
        const TStringBuf extPath(Cfg_.TtlExternalPath.data(), Cfg_.TtlExternalPath.size());
        return TStringBuilder() << "ALTER TABLE `" << tablePath << "` SET (TTL = Interval(\"" << tier
                                << "\") TO EXTERNAL DATA SOURCE `" << extPath << "\", Interval(\"" << del
                                << "\") DELETE ON `timestamp`);";
    }
    return TStringBuilder() << "ALTER TABLE `" << tablePath << "` SET (TTL = Interval(\"" << del << "\") ON `timestamp`);";
}

TString TDdlEnsurer::BuildCompactionDdl(const TString& tablePath) const {
    TString j = TString{Cfg_.CompactionJson.data(), Cfg_.CompactionJson.size()};
    if (j.empty()) {
        j = TString{DefaultCompactionJson};
    }
    return TStringBuilder() << "ALTER OBJECT `" << tablePath
                            << "` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`"
                            << j << "`);";
}

bool TDdlEnsurer::ExecScheme(NYdb::NTable::TTableClient& client, const TString& yql, TString* err) {
    auto s = client.GetSession().ExtractValueSync();
    if (!s.IsSuccess()) {
        if (err) {
            *err = TStringBuilder() << "GetSession: " << s.GetIssues().ToOneLineString();
        }
        return false;
    }
    NYdb::NTable::TSession session = s.GetSession();
    auto r = session.ExecuteSchemeQuery(std::string(yql.data(), yql.size())).ExtractValueSync();
    if (!r.IsSuccess()) {
        if (err) {
            *err = TStringBuilder() << "ExecuteSchemeQuery: " << r.GetIssues().ToOneLineString();
        }
        return false;
    }
    return true;
}

bool TDdlEnsurer::EnsureLogsTable(NYdb::NTable::TTableClient& client, const TString& tablePath, ELogsPkSchema schema, TString* err) {
    const std::string key(tablePath.data(), tablePath.size());
    {
        std::lock_guard<std::mutex> g(Mu_);
        if (Ensured_.count(key)) {
            return true;
        }
    }
    TString e;
    const TString create = BuildCreateDdl(tablePath, schema);
    if (!ExecScheme(client, create, &e)) {
        TStringBuf eb(e);
        const bool already = eb.Contains("AlreadyExists") || eb.Contains("already exists") || eb.Contains("ALREADY_EXISTS");
        if (!already) {
            if (err) {
                *err = e;
            }
            return false;
        }
    }
    if (!ExecScheme(client, BuildTtlDdl(tablePath), &e)) {
        if (err) {
            *err = e;
        }
        return false;
    }
    if (!ExecScheme(client, BuildCompactionDdl(tablePath), &e)) {
        if (err) {
            *err = e;
        }
        return false;
    }
    std::lock_guard<std::mutex> g(Mu_);
    Ensured_.insert(key);
    return true;
}

} // namespace NColumnShard::NOtelLogsToYdb
