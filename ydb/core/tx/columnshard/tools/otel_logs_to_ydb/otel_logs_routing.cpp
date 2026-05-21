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

TRoutedTable ResolveLogsTable(const TServerConfig& cfg, const TString& project, const TString& service, const TString& cluster, bool perProjectLayout) {
    TRoutedTable out;
    bool isDedicatedTarget = false;

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
                out.TablePath = JoinLogsPath(TString{cfg.TablesPrefix.data(), cfg.TablesPrefix.size()},
                    TString{cfg.YdbDedicatedLogsDir.data(), cfg.YdbDedicatedLogsDir.size()}, suffix);
                out.PkSchema = ELogsPkSchema::Dedicated;
                isDedicatedTarget = true;
                if (cfg.WriteOnlyDedicated) {
                    out.Drop = false;
                }
                return out;
            }
        }
        if (perProjectLayout) {
            TString suffix = JoinParts({project, SanitizeSeg(baseTable)});
            out.TablePath = JoinLogsPath(TString{cfg.TablesPrefix.data(), cfg.TablesPrefix.size()},
                TString{cfg.YdbCommonLogsDir.data(), cfg.YdbCommonLogsDir.size()}, suffix);
            out.PkSchema = ELogsPkSchema::PerProjectHeap;
        } else {
            TString suffix = JoinParts({project, SanitizeSeg(cluster), SanitizeSeg(service)});
            out.TablePath = JoinLogsPath(TString{cfg.TablesPrefix.data(), cfg.TablesPrefix.size()},
                TString{cfg.YdbCommonLogsDir.data(), cfg.YdbCommonLogsDir.size()}, suffix);
            out.PkSchema = ELogsPkSchema::PerService;
        }
    } else {
        if (perProjectLayout) {
            out.TablePath = JoinLogsPath(TString{cfg.TablesPrefix.data(), cfg.TablesPrefix.size()},
                TString{cfg.LogsDir.data(), cfg.LogsDir.size()}, project);
            out.PkSchema = ELogsPkSchema::PerProjectHeap;
        } else {
            TString suffix = JoinParts({project, SanitizeSeg(service)});
            out.TablePath = JoinLogsPath(TString{cfg.TablesPrefix.data(), cfg.TablesPrefix.size()},
                TString{cfg.LogsDir.data(), cfg.LogsDir.size()}, suffix);
            out.PkSchema = ELogsPkSchema::PerService;
        }
    }

    if (cfg.WriteOnlyDedicated && !isDedicatedTarget) {
        out.Drop = true;
    }
    return out;
}

} // namespace NColumnShard::NOtelLogsToYdb
