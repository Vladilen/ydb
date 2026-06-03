#include "otel_logs_ddl.h"

#include <cctype>
#include <iostream>

#include <util/generic/strbuf.h>
#include <util/string/builder.h>

namespace NColumnShard::NOtelLogsToYdb {

namespace {

/// Go `durationToYDBIntervalLiteral`: YAML `48h` / `720h` → ISO 8601 for `Interval("...")`.
TString DurationLiteralToYdbIsoInterval(TStringBuf lit) {
    TString s(lit);
    while (!s.empty() && (s[0] == ' ' || s[0] == '\t')) {
        s.erase(0, 1);
    }
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) {
        s.pop_back();
    }
    if (s.empty()) {
        return TString{"P30D"};
    }
    if (s[0] == 'P' || s[0] == 'p') {
        return s;
    }
    ui64 value = 0;
    size_t i = 0;
    while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
        value = value * 10 + static_cast<ui64>(s[i] - '0');
        ++i;
    }
    if (i >= s.size()) {
        return TString{"PT24H"};
    }
    const char unit = static_cast<char>(tolower(static_cast<unsigned char>(s[i])));
    ui64 seconds = 0;
    switch (unit) {
        case 'h':
            seconds = value * 3600;
            break;
        case 'd':
            seconds = value * 86400;
            break;
        case 'm':
            seconds = value * 60;
            break;
        case 's':
            seconds = value;
            break;
        default:
            return TString{"PT24H"};
    }
    if (seconds >= 86400 && seconds % 86400 == 0) {
        const ui64 days = seconds / 86400;
        return TStringBuilder() << "P" << days << "D";
    }
    if (seconds >= 3600 && seconds % 3600 == 0) {
        const ui64 hours = seconds / 3600;
        return TStringBuilder() << "PT" << hours << "H";
    }
    return TStringBuilder() << "PT" << seconds << "S";
}

const char* DefaultCompactionJson = R"({"levels": [
   {"class_name": "Zero", "expected_blobs_size": 2097152, "portions_count_limit": 15000000, "portions_live_duration": "180s", "concurrency": 2},
   {"class_name": "Zero", "expected_blobs_size": 8388608, "portions_count_limit": 15000000}
], "node_portions_count_limit": 15000000})";

void LogDdlQuery(TStringBuf step, const TString& tablePath, const TString& yql) {
    std::cerr << "Auto-DDL " << step << " table=" << tablePath << " yql:\n" << yql << std::endl;
}

void LogDdlBundle(
    TStringBuf failedStep,
    const TString& tablePath,
    const TString& create,
    const TString& ttl,
    const TString& compaction,
    const TString& subcolumns)
{
    std::cerr << "Auto-DDL bundle (failed at " << failedStep << ") table=" << tablePath << ":\n"
              << "--- create ---\n"
              << create << "--- ttl ---\n"
              << ttl << "--- compaction ---\n"
              << compaction << "\n--- subcolumns ---\n"
              << subcolumns << std::endl;
}

} // namespace

TDdlEnsurer::TDdlEnsurer(TServerConfig cfg)
    : Cfg_(std::move(cfg))
{
}

TString TDdlEnsurer::BuildCreateDdl(const TString& tablePath, ELogsPkSchema schema) const {
    auto colComp = [&](const char* name) -> TStringBuf {
        const auto it = Cfg_.LogsColumnCompression.find(name);
        return it != Cfg_.LogsColumnCompression.end() ? TStringBuf{it->second.data(), it->second.size()} : TStringBuf{};
    };
    const TStringBuf ct = colComp("timestamp");
    const TStringBuf cr = colComp("record_id");
    const TStringBuf cm = colComp("message");
    const TStringBuf cl = colComp("labels");
    const TStringBuf cz = colComp("meta");
    const int pc = (schema == ELogsPkSchema::Dedicated) ? Cfg_.PartitionCountDedicated : Cfg_.PartitionCountCommon;
    switch (schema) {
        case ELogsPkSchema::PerProjectHeap:
            return TStringBuilder() << "CREATE TABLE `" << tablePath << "` (\n"
                                     << "    timestamp Timestamp NOT NULL" << ct << ",\n"
                                     << "    service Utf8 NOT NULL,\n"
                                     << "    cluster Utf8 NOT NULL,\n"
                                     << "    record_id Utf8 NOT NULL" << cr << ",\n"
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
                                     << "    timestamp Timestamp NOT NULL" << ct << ",\n"
                                     << "    record_id Utf8 NOT NULL" << cr << ",\n"
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
                                     << "    timestamp Timestamp NOT NULL" << ct << ",\n"
                                     << "    cluster Utf8 NOT NULL,\n"
                                     << "    record_id Utf8 NOT NULL" << cr << ",\n"
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
    const TString del = DurationLiteralToYdbIsoInterval(TStringBuf{
        Cfg_.TtlDeleteIntervalLiteral.data(), Cfg_.TtlDeleteIntervalLiteral.size()});
    if (!Cfg_.TtlExternalPath.empty() && !Cfg_.TtlExternalTierLiteral.empty()) {
        const TString tier = DurationLiteralToYdbIsoInterval(TStringBuf{
            Cfg_.TtlExternalTierLiteral.data(), Cfg_.TtlExternalTierLiteral.size()});
        const TStringBuf extPath(Cfg_.TtlExternalPath.data(), Cfg_.TtlExternalPath.size());
        // Go: `SOURCE `%s`, Interval` — path in backticks, then comma (not `\"` after path).
        return TStringBuilder() << "ALTER TABLE `" << tablePath << "` SET (TTL = Interval(\"" << tier
                                << "\") TO EXTERNAL DATA SOURCE `" << extPath << "`, Interval(\"" << del
                                << "\") DELETE ON timestamp);\n";
    }
    return TStringBuilder() << "ALTER TABLE `" << tablePath << "` SET (TTL = Interval(\"" << del << "\") ON timestamp);\n";
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

TString TDdlEnsurer::BuildSubcolumnsDdl(const TString& tablePath) const {
    auto alterColumn = [&tablePath](TStringBuf columnName) {
        return TStringBuilder() << "ALTER OBJECT `" << tablePath
                                << "` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=" << columnName << ", "
                                << "`DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, "
                                << "`SCAN_FIRST_LEVEL_ONLY`=`false`, "
                                << "`DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, "
                                << "`FORCE_SIMD_PARSING`=`false`, "
                                << "`COLUMNS_LIMIT`=`1024`, "
                                << "`SPARSED_DETECTOR_KFF`=`20`, "
                                << "`MEM_LIMIT_CHUNK`=`52428800`, "
                                << "`OTHERS_ALLOWED_FRACTION`=`0`);";
    };
    return TStringBuilder() << alterColumn("meta") << "\n" << alterColumn("labels");
}

bool TDdlEnsurer::ExecScheme(
    NYdb::NTable::TTableClient& client,
    TStringBuf step,
    const TString& tablePath,
    const TString& yql,
    TString* err)
{
    auto s = client.GetSession().ExtractValueSync();
    if (!s.IsSuccess()) {
        if (err) {
            *err = TStringBuilder() << "GetSession: " << s.GetIssues().ToOneLineString();
        }
        LogDdlQuery(step, tablePath, yql);
        return false;
    }
    NYdb::NTable::TSession session = s.GetSession();
    auto r = session.ExecuteSchemeQuery(std::string(yql.data(), yql.size())).ExtractValueSync();
    if (!r.IsSuccess()) {
        if (err) {
            *err = TStringBuilder() << "ExecuteSchemeQuery: " << r.GetIssues().ToOneLineString();
        }
        LogDdlQuery(step, tablePath, yql);
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
    const TString ttl = BuildTtlDdl(tablePath);
    const TString compaction = BuildCompactionDdl(tablePath);
    const TString subcolumns = BuildSubcolumnsDdl(tablePath);
    if (!ExecScheme(client, "create table", tablePath, create, &e)) {
        TStringBuf eb(e);
        const bool already = eb.Contains("AlreadyExists") || eb.Contains("already exists") || eb.Contains("ALREADY_EXISTS");
        if (!already) {
            if (err) {
                *err = TStringBuilder() << "create table: " << e;
            }
            LogDdlBundle("create table", tablePath, create, ttl, compaction, subcolumns);
            return false;
        }
    }
    if (!ExecScheme(client, "set ttl", tablePath, ttl, &e)) {
        if (err) {
            *err = TStringBuilder() << "set ttl: " << e;
        }
        LogDdlBundle("set ttl", tablePath, create, ttl, compaction, subcolumns);
        return false;
    }
    if (!ExecScheme(client, "set compaction", tablePath, compaction, &e)) {
        if (err) {
            *err = TStringBuilder() << "set compaction: " << e;
        }
        LogDdlBundle("set compaction", tablePath, create, ttl, compaction, subcolumns);
        return false;
    }
    if (!ExecScheme(client, "set subcolumns", tablePath, subcolumns, &e)) {
        if (err) {
            *err = TStringBuilder() << "set subcolumns: " << e;
        }
        LogDdlBundle("set subcolumns", tablePath, create, ttl, compaction, subcolumns);
        return false;
    }
    std::lock_guard<std::mutex> g(Mu_);
    Ensured_.insert(key);
    return true;
}

} // namespace NColumnShard::NOtelLogsToYdb
