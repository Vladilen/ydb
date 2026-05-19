#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <util/system/types.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace NColumnShard::NOtelLogsToYdb {

struct TDedicatedServiceEntry {
    std::string Cluster;
    std::string Service;
};

struct TProjectRoutingRule {
    std::string BaseTableName = "common";
    std::vector<TDedicatedServiceEntry> DedicatedService;
};

struct TServerConfig {
    std::string ListenAddress = "0.0.0.0:4317";
    int GrpcMaxPollers = 2048;
    std::string YdbEndpoint;
    std::string YdbDatabase;
    std::string YdbToken;
    std::string TablesPrefix;
    /// Legacy single logs directory when `project_routing` is empty (default `logs`).
    std::string LogsDir = "logs";
    std::string TableLayout = "per_service";
    std::vector<std::string> AllowedProjects;
    int YdbMaxConcurrentBulkUpserts = 24;
    std::string HealthListen = "0.0.0.0:13133";
    std::string HealthPath = "/ping";

    /// Bounded ingest queue (backpressure → RESOURCE_EXHAUSTED when full).
    size_t IngestQueueMax = 4096;
    size_t IngestWorkers = 4;
    std::string MetricsListen;

    std::string YdbCommonLogsDir = "logs_store";
    std::string YdbDedicatedLogsDir = "logs";
    bool WriteOnlyDedicated = false;
    std::unordered_map<std::string, TProjectRoutingRule> ProjectRouting;

    bool BatchByShardHash = false;
    /// Mutually exclusive with `ShardBufferMinFlushBytes` (YAML loader enforces XOR).
    /// Flush to YDB when buffer holds at least this many log rows (0 = disabled).
    int ShardBufferMinFlushRecords = 0;
    /// Mutually exclusive with `ShardBufferMinFlushRecords`.
    /// Flush when estimated buffer size (bytes) reaches this threshold (0 = disabled).
    i64 ShardBufferMinFlushBytes = 0;
    /// If no successful YDB write for this shard for this long, flush pending rows (even below min size). 0 = disabled.
    ui64 ShardBufferFlushIntervalSec = 0;
    int PartitionCountCommon = 48;
    int PartitionCountDedicated = 48;
    int SupplierPoolSize = 10;

    bool AutoCreateMissingTables = false;
    std::string CompactionJson;
    std::string TtlDeleteIntervalLiteral = "P30D";
    std::string TtlExternalPath;
    std::string TtlExternalTierLiteral;

    std::string LogsCompressionMessage;
    std::string LogsCompressionLabels;
    std::string LogsCompressionMeta;

    int MaxRetries = 3;
    ui32 RetryBackoffMs = 200;
    ui32 YdbWriteTimeoutSec = 60;

    bool ValidationEnabled = false;

    /// Wire-scan Export before ingest queue (skip full parse for non-routable batches).
    bool ExportRoutableWirePrecheck = false;
};

/// OTLP gRPC LogsService + async pipeline → YDB BulkUpsert.
class TOtelLogsServer {
public:
    /// Opaque server state (gRPC + workers); public forward decl for gRPC ingest in .cpp.
    struct TImpl;

    explicit TOtelLogsServer(TServerConfig cfg);
    ~TOtelLogsServer();

    void Run();
    void Stop();

private:
    TServerConfig Cfg_;
    NYdb::TDriver Driver_;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NColumnShard::NOtelLogsToYdb
