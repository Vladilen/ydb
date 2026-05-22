#include "otel_logs_config.h"
#include "otel_logs_service.h"

#include <util/generic/strbuf.h>

#include <iostream>
#include <string>

namespace {

void Usage() {
    std::cerr
        << "otel_logs_to_ydb — OTLP/gRPC logs → YDB BulkUpsert (layout compatible with observability ydb-supplier).\n"
        << "\n"
        << "All settings come from a YAML file.\n"
        << "\n"
        << "Usage:\n"
        << "  otel_logs_to_ydb --config PATH\n"
        << "\n"
        << "  --config PATH   required; root key `otel_logs_to_ydb:`; `${env:VAR}` substitution (e.g. ydb_token)\n"
        << "  -h, --help      show this text\n"
        << "\n";
}

} // namespace

int main(int argc, char** argv) {
    TString configPath;

    for (int i = 1; i < argc; ++i) {
        TStringBuf a = argv[i];
        if (a == "-h" || a == "--help") {
            Usage();
            return 0;
        }
        if (a == "--config") {
            if (i + 1 >= argc) {
                std::cerr << "--config requires a path\n";
                Usage();
                return 2;
            }
            configPath = TString{argv[i + 1]};
            ++i;
            continue;
        }
        std::cerr << "unknown argument: " << a << std::endl;
        Usage();
        return 2;
    }

    if (configPath.empty()) {
        std::cerr << "missing required --config PATH\n";
        Usage();
        return 2;
    }

    NColumnShard::NOtelLogsToYdb::TServerConfig cfg;
    TString err;
    if (!NColumnShard::NOtelLogsToYdb::LoadOtelLogsYaml(configPath, &cfg, &err)) {
        std::cerr << "failed to load config " << configPath << ": " << err << std::endl;
        return 2;
    }

    if (cfg.YdbEndpoint.empty() || cfg.YdbDatabase.empty() || cfg.TablesPrefix.empty()) {
        std::cerr << "config must set ydb_endpoint, ydb_database, and ydb_tables_prefix\n";
        return 2;
    }

    try {
        NColumnShard::NOtelLogsToYdb::TOtelLogsServer server(std::move(cfg));
        server.Run();
    } catch (const std::exception& ex) {
        std::cerr << "fatal: " << ex.what() << std::endl;
        return 1;
    }
    return 0;
}
