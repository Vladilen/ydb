#include "otel_logs_config.h"

#include <library/cpp/string_utils/url/url.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/reverse.h>
#include <util/string/split.h>

#include <yaml-cpp/yaml.h>

#include <cstdlib>
#include <regex>

namespace NColumnShard::NOtelLogsToYdb {

void SubstituteEnvVars(TString* fileContent) {
    static const std::regex re(R"(\$\{env:([A-Za-z_][A-Za-z0-9_]*)\})");
    const std::string s(fileContent->data(), fileContent->size());
    std::string out;
    out.reserve(s.size() + 64);
    size_t last = 0;
    for (std::sregex_iterator it(s.begin(), s.end(), re), end; it != end; ++it) {
        const std::smatch& m = *it;
        const size_t matchBegin = static_cast<size_t>(m.position(0));
        out.append(s, last, matchBegin - last);
        const std::string var = m[1].str();
        if (const char* v = std::getenv(var.c_str())) {
            out += v;
        }
        last = matchBegin + static_cast<size_t>(m.length(0));
    }
    out.append(s, last, std::string::npos);
    *fileContent = TString{out.data(), out.size()};
}

namespace {

bool NodeAsBool(const YAML::Node& n, bool def) {
    if (!n || !n.IsDefined()) {
        return def;
    }
    try {
        return n.as<bool>();
    } catch (...) {
        return def;
    }
}

i64 NodeAsInt64(const YAML::Node& n, i64 def) {
    if (!n || !n.IsDefined()) {
        return def;
    }
    try {
        return n.as<i64>();
    } catch (...) {
        return def;
    }
}

int NodeAsInt(const YAML::Node& n, int def) {
    return static_cast<int>(NodeAsInt64(n, def));
}

TString NodeAsString(const YAML::Node& n, const TString& def = {}) {
    if (!n || !n.IsDefined()) {
        return def;
    }
    try {
        const std::string s = n.as<std::string>();
        return TString{s.data(), s.size()};
    } catch (...) {
        return def;
    }
}

TString NodeAsString(const YAML::Node& n, const std::string& def) {
    if (!n || !n.IsDefined()) {
        return TString{def.data(), def.size()};
    }
    try {
        const std::string s = n.as<std::string>();
        return TString{s.data(), s.size()};
    } catch (...) {
        return TString{def.data(), def.size()};
    }
}

void ParseRouting(const YAML::Node& y, TServerConfig* cfg) {
    if (!y || !y.IsMap()) {
        return;
    }
    for (const auto& it : y) {
        const std::string proj = it.first.as<std::string>();
        const YAML::Node rule = it.second;
        TProjectRoutingRule r;
        r.BaseTableName = rule["base_table_name"].as<std::string>("common");
        const YAML::Node ds = rule["dedicated_service"];
        if (ds && ds.IsSequence()) {
            for (const YAML::Node& e : ds) {
                TDedicatedServiceEntry d;
                d.Cluster = e["cluster"].as<std::string>("");
                d.Service = e["service"].as<std::string>("");
                if (!d.Cluster.empty() && !d.Service.empty()) {
                    r.DedicatedService.push_back(std::move(d));
                }
            }
        }
        cfg->ProjectRouting[proj] = std::move(r);
    }
}

} // namespace

bool LoadOtelLogsYaml(const TString& path, TServerConfig* cfg, TString* errorMsg) {
    try {
        TString data = TFileInput(path).ReadAll();
        SubstituteEnvVars(&data);
        const YAML::Node root = YAML::Load(std::string(data.data(), data.size()));
        YAML::Node n = root["otel_logs_to_ydb"];
        if (!n || !n.IsMap()) {
            n = root;
        }
        cfg->ListenAddress = NodeAsString(n["grpc_listen"], TString{cfg->ListenAddress.data(), cfg->ListenAddress.size()});
        cfg->GrpcMaxPollers = NodeAsInt(n["grpc_max_pollers"], cfg->GrpcMaxPollers);
        cfg->IngestQueueMax = static_cast<size_t>(Max<i64>(1, NodeAsInt64(n["ingest_queue_max"], static_cast<i64>(cfg->IngestQueueMax))));
        cfg->IngestWorkers = static_cast<size_t>(Max<i64>(1, NodeAsInt64(n["ingest_workers"], static_cast<i64>(cfg->IngestWorkers))));
        cfg->MetricsListen = NodeAsString(n["metrics_listen"], TString{cfg->MetricsListen.data(), cfg->MetricsListen.size()});
        cfg->HealthListen = NodeAsString(n["health_listen"], TString{cfg->HealthListen.data(), cfg->HealthListen.size()});
        cfg->HealthPath = NodeAsString(n["health_path"], TString{cfg->HealthPath.data(), cfg->HealthPath.size()});
        cfg->YdbEndpoint = NodeAsString(n["ydb_endpoint"], TString{cfg->YdbEndpoint.data(), cfg->YdbEndpoint.size()});
        cfg->YdbDatabase = NodeAsString(n["ydb_database"], TString{cfg->YdbDatabase.data(), cfg->YdbDatabase.size()});
        cfg->YdbToken = NodeAsString(n["ydb_token"], TString{cfg->YdbToken.data(), cfg->YdbToken.size()});
        cfg->TablesPrefix = NodeAsString(n["ydb_tables_prefix"], TString{cfg->TablesPrefix.data(), cfg->TablesPrefix.size()});
        cfg->LogsDir = NodeAsString(n["logs_dir"], TString{cfg->LogsDir.data(), cfg->LogsDir.size()});
        cfg->YdbCommonLogsDir = NodeAsString(n["ydb_common_logs_dir"], TString{cfg->YdbCommonLogsDir.data(), cfg->YdbCommonLogsDir.size()});
        cfg->YdbDedicatedLogsDir = NodeAsString(n["ydb_dedicated_logs_dir"], TString{cfg->YdbDedicatedLogsDir.data(), cfg->YdbDedicatedLogsDir.size()});
        cfg->TableLayout = NodeAsString(n["table_layout"], TString{cfg->TableLayout.data(), cfg->TableLayout.size()});
        cfg->WriteOnlyDedicated = NodeAsBool(n["write_only_dedicated"], cfg->WriteOnlyDedicated);
        cfg->BatchByShardHash = NodeAsBool(n["batch_by_shard_hash"], cfg->BatchByShardHash);
        int minFlushRec = NodeAsInt(n["shard_buffer_min_flush_records"], cfg->ShardBufferMinFlushRecords);
        i64 minFlushBytes = NodeAsInt64(n["shard_buffer_min_flush_bytes"], cfg->ShardBufferMinFlushBytes);
        if (!n["shard_buffer_min_flush_records"] && n["shard_buffer_max_records"]) {
            minFlushRec = NodeAsInt(n["shard_buffer_max_records"], 0);
        }
        if (!n["shard_buffer_min_flush_bytes"] && n["shard_buffer_max_bytes"]) {
            minFlushBytes = NodeAsInt64(n["shard_buffer_max_bytes"], 0);
        }
        cfg->ShardBufferFlushIntervalSec = static_cast<ui64>(Max<i64>(0, NodeAsInt64(n["shard_buffer_flush_interval_sec"], static_cast<i64>(cfg->ShardBufferFlushIntervalSec))));
        cfg->ShardBufferFlushIntervalJitterSec = static_cast<ui64>(Max<i64>(
            0, NodeAsInt64(n["shard_buffer_flush_interval_jitter_sec"], static_cast<i64>(cfg->ShardBufferFlushIntervalJitterSec))));
        if (minFlushRec > 0 && minFlushBytes > 0) {
            if (errorMsg) {
                *errorMsg = TString{"only one of shard_buffer_min_flush_records or shard_buffer_min_flush_bytes may be non-zero"};
            }
            return false;
        }
        if (minFlushRec <= 0 && minFlushBytes <= 0 && cfg->ShardBufferFlushIntervalSec == 0) {
            if (errorMsg) {
                *errorMsg = TString{"set shard_buffer_min_flush_records, or shard_buffer_min_flush_bytes, or shard_buffer_flush_interval_sec > 0"};
            }
            return false;
        }
        cfg->ShardBufferMinFlushRecords = minFlushRec;
        cfg->ShardBufferMinFlushBytes = minFlushBytes;
        {
            const i64 overshootPct = NodeAsInt64(
                n["shard_buffer_flush_max_overshoot_percent"],
                static_cast<i64>(cfg->ShardBufferFlushMaxOvershootPercent));
            if (overshootPct < 0 || overshootPct > 1000) {
                if (errorMsg) {
                    *errorMsg = TString{"shard_buffer_flush_max_overshoot_percent must be in [0, 1000]"};
                }
                return false;
            }
            cfg->ShardBufferFlushMaxOvershootPercent = static_cast<ui32>(overshootPct);
        }
        cfg->SupplierPoolSize = NodeAsInt(n["supplier_pool_size"], cfg->SupplierPoolSize);
        cfg->YdbMaxConcurrentBulkUpserts = NodeAsInt(n["ydb_max_concurrent_bulk"], cfg->YdbMaxConcurrentBulkUpserts);
        cfg->YdbFlushWorkers = static_cast<size_t>(Max<i64>(0, NodeAsInt64(n["ydb_flush_workers"], static_cast<i64>(cfg->YdbFlushWorkers))));
        cfg->FlushQueueMax = static_cast<size_t>(Max<i64>(1, NodeAsInt64(n["flush_queue_max"], static_cast<i64>(cfg->FlushQueueMax))));
        cfg->FlushQueueDropOnFull = NodeAsBool(n["flush_queue_drop_on_full"], cfg->FlushQueueDropOnFull);
        cfg->PartitionCountCommon = NodeAsInt(n["auto_create_partition_count_common"], cfg->PartitionCountCommon);
        cfg->PartitionCountDedicated = NodeAsInt(n["auto_create_partition_count_dedicated"], cfg->PartitionCountDedicated);
        cfg->AutoCreateMissingTables = NodeAsBool(n["auto_create_missing_tables"], cfg->AutoCreateMissingTables);
        cfg->MaxRetries = NodeAsInt(n["max_retries"], cfg->MaxRetries);
        cfg->RetryBackoffMs = static_cast<ui32>(Max<i64>(0, NodeAsInt64(n["retry_backoff_ms"], cfg->RetryBackoffMs)));
        cfg->YdbWriteTimeoutSec = static_cast<ui32>(Max<i64>(1, NodeAsInt64(n["ydb_write_timeout_sec"], cfg->YdbWriteTimeoutSec)));
        cfg->ValidationEnabled = NodeAsBool(n["validation_enabled"], cfg->ValidationEnabled);
        cfg->ExportRoutableWirePrecheck = NodeAsBool(n["export_routable_wire_precheck"], cfg->ExportRoutableWirePrecheck);
        cfg->IngestWireToOwned = NodeAsBool(n["ingest_wire_to_owned"], cfg->IngestWireToOwned);
        cfg->IngestStreamingJsonSerializer = NodeAsBool(
            n["ingest_streaming_json_serializer"], cfg->IngestStreamingJsonSerializer);
        cfg->CaptureExportRequestsMax = static_cast<size_t>(
            Max<i64>(0, NodeAsInt64(n["capture_export_requests_max"], static_cast<i64>(cfg->CaptureExportRequestsMax))));
        {
            const TString capDir = NodeAsString(n["capture_export_requests_dir"], TString{cfg->CaptureExportRequestsDir.data(), cfg->CaptureExportRequestsDir.size()});
            cfg->CaptureExportRequestsDir.assign(capDir.data(), capDir.size());
        }
        cfg->IngestShardLockTimeoutMs = static_cast<ui32>(
            Max<i64>(0, NodeAsInt64(n["ingest_shard_lock_timeout_ms"], cfg->IngestShardLockTimeoutMs)));
        cfg->IngestShardLockMaxSpins = static_cast<int>(
            Max<i64>(1, NodeAsInt64(n["ingest_shard_lock_max_spins"], cfg->IngestShardLockMaxSpins)));
        cfg->IngestShardLockRetryMs = static_cast<ui32>(
            Max<i64>(1, NodeAsInt64(n["ingest_shard_lock_retry_ms"], cfg->IngestShardLockRetryMs)));
        cfg->FlushEnqueueTimeoutMs = static_cast<ui32>(
            Max<i64>(0, NodeAsInt64(n["flush_enqueue_timeout_ms"], cfg->FlushEnqueueTimeoutMs)));
        cfg->IngestStallWatchdogSec = static_cast<ui32>(
            Max<i64>(0, NodeAsInt64(n["ingest_stall_watchdog_sec"], cfg->IngestStallWatchdogSec)));
        cfg->IngestStallWatchdogPollSec = static_cast<ui32>(
            Max<i64>(1, NodeAsInt64(n["ingest_stall_watchdog_poll_sec"], cfg->IngestStallWatchdogPollSec)));
        cfg->IngestWireParseMaxBytes = static_cast<ui64>(
            Max<i64>(0, NodeAsInt64(n["ingest_wire_parse_max_bytes"], static_cast<i64>(cfg->IngestWireParseMaxBytes))));

        const YAML::Node ap = n["allowed_projects"];
        if (ap && ap.IsSequence()) {
            cfg->AllowedProjects.clear();
            for (const YAML::Node& e : ap) {
                cfg->AllowedProjects.push_back(e.as<std::string>(""));
            }
        }
        ParseRouting(n["project_routing"], cfg);

        if (const YAML::Node ttl = n["ttl"]) {
            const TString del = NodeAsString(ttl["delete_interval"], cfg->TtlDeleteIntervalLiteral);
            cfg->TtlDeleteIntervalLiteral.assign(del.data(), del.size());
            if (const YAML::Node ex = ttl["external"]) {
                const TString p = NodeAsString(ex["path"], cfg->TtlExternalPath);
                cfg->TtlExternalPath.assign(p.data(), p.size());
                const TString tier = NodeAsString(ex["interval"], cfg->TtlExternalTierLiteral);
                cfg->TtlExternalTierLiteral.assign(tier.data(), tier.size());
            }
        }
        if (const YAML::Node comp = n["compaction"]) {
            const TString cj = NodeAsString(comp["features_json"], cfg->CompactionJson);
            cfg->CompactionJson.assign(cj.data(), cj.size());
        }
        if (const YAML::Node lc = n["logs_column_compression"]) {
            if (const YAML::Node m = lc["message"]) {
                TString alg = NodeAsString(m["algorithm"]);
                int lvl = NodeAsInt(m["level"], -1);
                if (!alg.empty() && lvl >= 0) {
                    const TString t = TStringBuilder() << " COMPRESSION (algorithm = " << alg << ", level = " << lvl << ")";
                    cfg->LogsCompressionMessage.assign(t.data(), t.size());
                } else if (!alg.empty()) {
                    const TString t = TStringBuilder() << " COMPRESSION (algorithm = " << alg << ")";
                    cfg->LogsCompressionMessage.assign(t.data(), t.size());
                }
            }
            if (const YAML::Node m = lc["labels"]) {
                TString alg = NodeAsString(m["algorithm"]);
                int lvl = NodeAsInt(m["level"], -1);
                if (!alg.empty() && lvl >= 0) {
                    const TString t = TStringBuilder() << " COMPRESSION (algorithm = " << alg << ", level = " << lvl << ")";
                    cfg->LogsCompressionLabels.assign(t.data(), t.size());
                } else if (!alg.empty()) {
                    const TString t = TStringBuilder() << " COMPRESSION (algorithm = " << alg << ")";
                    cfg->LogsCompressionLabels.assign(t.data(), t.size());
                }
            }
            if (const YAML::Node m = lc["meta"]) {
                TString alg = NodeAsString(m["algorithm"]);
                int lvl = NodeAsInt(m["level"], -1);
                if (!alg.empty() && lvl >= 0) {
                    const TString t = TStringBuilder() << " COMPRESSION (algorithm = " << alg << ", level = " << lvl << ")";
                    cfg->LogsCompressionMeta.assign(t.data(), t.size());
                } else if (!alg.empty()) {
                    const TString t = TStringBuilder() << " COMPRESSION (algorithm = " << alg << ")";
                    cfg->LogsCompressionMeta.assign(t.data(), t.size());
                }
            }
        }
        return true;
    } catch (const std::exception& ex) {
        if (errorMsg) {
            *errorMsg = TString{ex.what()};
        }
        return false;
    }
}

} // namespace NColumnShard::NOtelLogsToYdb
