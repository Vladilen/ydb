PROGRAM(otel_logs_to_ydb)

SRCDIR(
    ydb/core/tx/columnshard/tools/otel_logs_to_ydb
)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/tools/otel_logs_to_ydb/lib
)

YQL_LAST_ABI_VERSION()

END()
