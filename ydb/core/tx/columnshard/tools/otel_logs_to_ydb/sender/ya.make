PROGRAM(otel_logs_to_ydb_sender)

SRCDIR(
    ydb/core/tx/columnshard/tools/otel_logs_to_ydb/sender
)

SRCS(
    main.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/getopt
    util
)

YQL_LAST_ABI_VERSION()

END()
