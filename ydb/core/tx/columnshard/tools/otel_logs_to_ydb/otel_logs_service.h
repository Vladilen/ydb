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
    /// Dedicated threads for shard flush → BulkUpsert (0 = auto from `ydb_max_concurrent_bulk`).
    size_t YdbFlushWorkers = 0;
    /// Bounded queue of flush chunks between ingest workers and YDB writers.
    size_t FlushQueueMax = 4096;
    /// If flush queue is full: drop chunk instead of returning rows to shard buffer (limits RAM).
    bool FlushQueueDropOnFull = false;
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
    /// Max overshoot per flush chunk over `ShardBufferMinFlushBytes` (percent, e.g. 10 → cap at min×1.1). Used only with min_flush_bytes.
    ui32 ShardBufferFlushMaxOvershootPercent = 10;
    /// If no successful YDB write for this shard for this long, flush pending rows (even below min size). 0 = disabled.
    ui64 ShardBufferFlushIntervalSec = 0;
    /// Per-shard spread of `ShardBufferFlushIntervalSec` in seconds (deterministic hash of bucket). 0 = no jitter.
    ui64 ShardBufferFlushIntervalJitterSec = 0;
    int PartitionCountCommon = 48;
    int PartitionCountDedicated = 48;
    int SupplierPoolSize = 10;

    bool AutoCreateMissingTables = false;
    std::string CompactionJson;
    std::string TtlDeleteIntervalLiteral = "P30D";
    std::string TtlExternalPath;
    std::string TtlExternalTierLiteral;

    /// Map from column name to compression clause string, e.g. {"message": " COMPRESSION (algorithm = zstd)"}.
    std::unordered_map<std::string, std::string> LogsColumnCompression;

    int MaxRetries = 3;
    ui32 RetryBackoffMs = 200;
    ui32 YdbWriteTimeoutSec = 60;

    bool ValidationEnabled = false;

    /// Wire-scan Export before ingest queue (skip full parse for non-routable batches).
    bool ExportRoutableWirePrecheck = false;

    /// Stage A: ingest worker parses wire → `TOwnedLogRow` (no protobuf tree). Default: false (Arena parse).
    bool IngestWireToOwned = false;

    /// Nested OTLP map/array → JSON text via `NJsonWriter` while parsing (no `NJson::TJsonValue`).
    /// Flat `labels`/`meta` object is still built with `JsonStringifyMap` from `THashMap`.
    bool IngestStreamingJsonSerializer = false;

    /// Save first N gRPC Export bodies as `{dir}/export_NNNNNNN.pb` (0 = disabled).
    size_t CaptureExportRequestsMax = 0;
    std::string CaptureExportRequestsDir;

    /// Max wait for `TShardBuffer::Mu` per append attempt (0 = 5000 ms).
    ui32 IngestShardLockTimeoutMs = 5000;
    /// Retries when shard mutex is contended before dropping a parsed batch.
    int IngestShardLockMaxSpins = 200;
    ui32 IngestShardLockRetryMs = 10;

    /// Max wait to enqueue a flush chunk; on timeout rows return to shard buffer (0 = 30000 ms).
    ui32 FlushEnqueueTimeoutMs = 30000;

    /// Log + metric when pipeline bytes do not grow while ingest is saturated (0 = disabled).
    ui32 IngestStallWatchdogSec = 0;
    ui32 IngestStallWatchdogPollSec = 15;

    /// Wire parse `CodedInputStream` total bytes limit (0 = request `ByteBuffer` length).
    ui64 IngestWireParseMaxBytes = 0;
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
