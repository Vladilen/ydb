#include "otel_logs_routing.h"

#include <util/string/builder.h>

namespace NColumnShard::NOtelLogsToYdb {

namespace {

TString SanitizeSeg(TStringBuf s) {
    TString r(s);
    for (char& c : r) {
        if (c == '/' || c == '`') {
            c = '_';
        }
    }
    return r;
}

/// Same as Go `joinYdbTablePath`: keep leading `/` on the first segment (prefix), trim only inner slashes.
TString JoinParts(std::initializer_list<TStringBuf> parts) {
    TStringBuilder out;
    bool first = true;
    for (TStringBuf p : parts) {
        TStringBuf x = p;
        while (!x.empty() && x.back() == '/') {
            x.Chop(1);
        }
        if (x.empty()) {
            continue;
        }
        if (!first) {
            while (!x.empty() && x[0] == '/') {
                x.Skip(1);
            }
            if (x.empty()) {
                continue;
            }
            out << '/';
        }
        first = false;
        out << x;
    }
    TString result{out};
    if (!result.empty() && result[0] != '/') {
        result = TString("/") + result;
    }
    return result;
}

TString JoinLogsPath(const TString& prefix, const TString& logsDirSeg, const TString& suffix) {
    return JoinParts({prefix, logsDirSeg, suffix});
}

} // namespace

/// Returns true if the service name is in the batch_id whitelist (any cluster).
static bool IsInBatchIdWhitelist(const TServerConfig& cfg, const TString& service) {
    const std::string svc(service.data(), service.size());
    for (const std::string& e : cfg.BatchIdServices) {
        if (e == svc) {
            return true;
        }
    }
    return false;
}

/// Build the default (no-batch_id) TRoutedTable for a given path and schema family.
static TRoutedTable MakeDefaultRoute(
    const TServerConfig& cfg,
    const TString& logsDir,
    const TString& suffix,
    ELogsPkSchema schema,
    bool drop)
{
    TRoutedTable r;
    r.TablePath = JoinLogsPath(TString{cfg.TablesPrefix.data(), cfg.TablesPrefix.size()}, logsDir, suffix);
    r.PkSchema = schema;
    r.Drop = drop;
    return r;
}

/// Build the BatchPartitioned TRoutedTable for a given path and schema family.
static TRoutedTable MakeBatchPartitionedRoute(
    const TServerConfig& cfg,
    const TString& logsDirBatchId,
    const TString& suffix,
    ELogsPkSchema schema)
{
    TRoutedTable r;
    r.TablePath = JoinLogsPath(TString{cfg.TablesPrefix.data(), cfg.TablesPrefix.size()}, logsDirBatchId, suffix);
    r.PkSchema = schema;
    return r;
}

std::vector<TRoutedTable> ResolveLogsTables(const TServerConfig& cfg, const TString& project, const TString& service, const TString& cluster, bool perProjectLayout) {
    bool isDedicatedTarget = false;
    const bool hasBatchIdDir_common = !cfg.YdbCommonLogsDirBatchId.empty();
    const bool hasBatchIdDir_dedicated = !cfg.YdbDedicatedLogsDirBatchId.empty();
    const bool addBatchPartitioned = IsInBatchIdWhitelist(cfg, service);

    std::vector<TRoutedTable> result;

    const std::string projKey(project.data(), project.size());
    auto itRule = cfg.ProjectRouting.find(projKey);
    if (itRule != cfg.ProjectRouting.end()) {
        const TProjectRoutingRule& rule = itRule->second;
        TString baseTable(rule.BaseTableName.data(), rule.BaseTableName.size());
        if (baseTable.empty()) {
            baseTable = TString{"common"};
        }
        for (const TDedicatedServiceEntry& de : rule.DedicatedService) {
            TString ec(de.Cluster.data(), de.Cluster.size());
            TString es(de.Service.data(), de.Service.size());
            if (ec == cluster && es == service) {
                TString suffix = JoinParts({project, SanitizeSeg(cluster), SanitizeSeg(service)});
                result.push_back(MakeDefaultRoute(cfg,
                    TString{cfg.YdbDedicatedLogsDir.data(), cfg.YdbDedicatedLogsDir.size()},
                    suffix, ELogsPkSchema::Dedicated, false));
                if (addBatchPartitioned && hasBatchIdDir_dedicated) {
                    result.push_back(MakeBatchPartitionedRoute(cfg,
                        TString{cfg.YdbDedicatedLogsDirBatchId.data(), cfg.YdbDedicatedLogsDirBatchId.size()},
                        suffix, ELogsPkSchema::DedicatedBatchPartitioned));
                }
                isDedicatedTarget = true;
                return result;
            }
        }
        if (perProjectLayout) {
            TString suffix = JoinParts({project, SanitizeSeg(baseTable)});
            result.push_back(MakeDefaultRoute(cfg,
                TString{cfg.YdbCommonLogsDir.data(), cfg.YdbCommonLogsDir.size()},
                suffix, ELogsPkSchema::PerProjectHeap, false));
            if (addBatchPartitioned && hasBatchIdDir_common) {
                result.push_back(MakeBatchPartitionedRoute(cfg,
                    TString{cfg.YdbCommonLogsDirBatchId.data(), cfg.YdbCommonLogsDirBatchId.size()},
                    suffix, ELogsPkSchema::PerProjectHeapBatchPartitioned));
            }
        } else {
            TString suffix = JoinParts({project, SanitizeSeg(cluster), SanitizeSeg(service)});
            result.push_back(MakeDefaultRoute(cfg,
                TString{cfg.YdbCommonLogsDir.data(), cfg.YdbCommonLogsDir.size()},
                suffix, ELogsPkSchema::PerService, false));
            if (addBatchPartitioned && hasBatchIdDir_common) {
                result.push_back(MakeBatchPartitionedRoute(cfg,
                    TString{cfg.YdbCommonLogsDirBatchId.data(), cfg.YdbCommonLogsDirBatchId.size()},
                    suffix, ELogsPkSchema::PerServiceBatchPartitioned));
            }
        }
    } else {
        if (perProjectLayout) {
            result.push_back(MakeDefaultRoute(cfg,
                TString{cfg.LogsDir.data(), cfg.LogsDir.size()},
                project, ELogsPkSchema::PerProjectHeap, false));
            if (addBatchPartitioned && hasBatchIdDir_common) {
                result.push_back(MakeBatchPartitionedRoute(cfg,
                    TString{cfg.YdbCommonLogsDirBatchId.data(), cfg.YdbCommonLogsDirBatchId.size()},
                    project, ELogsPkSchema::PerProjectHeapBatchPartitioned));
            }
        } else {
            TString suffix = JoinParts({project, SanitizeSeg(service)});
            result.push_back(MakeDefaultRoute(cfg,
                TString{cfg.LogsDir.data(), cfg.LogsDir.size()},
                suffix, ELogsPkSchema::PerService, false));
            if (addBatchPartitioned && hasBatchIdDir_common) {
                result.push_back(MakeBatchPartitionedRoute(cfg,
                    TString{cfg.YdbCommonLogsDirBatchId.data(), cfg.YdbCommonLogsDirBatchId.size()},
                    suffix, ELogsPkSchema::PerServiceBatchPartitioned));
            }
        }
    }

    if (cfg.WriteOnlyDedicated && !isDedicatedTarget) {
        for (auto& r : result) {
            r.Drop = true;
        }
    }
    return result;
}

} // namespace NColumnShard::NOtelLogsToYdb
