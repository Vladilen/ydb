LIBRARY(otel_logs_to_ydb_lib)

SRCDIR(
    ydb/core/tx/columnshard/tools/otel_logs_to_ydb
)

SRCS(
    health_check_server.cpp
    otel_logs_config.cpp
    otel_logs_ddl.cpp
    otel_logs_metrics.cpp
    otel_logs_routing.cpp
    otel_logs_shard_hash.cpp
    otel_logs_service.cpp
    otel_logs_wire_routable.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/grpc
    contrib/libs/opentelemetry-proto
    contrib/libs/xxhash
    contrib/libs/yaml-cpp
    library/cpp/http/server
    library/cpp/json
    util
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

END()
