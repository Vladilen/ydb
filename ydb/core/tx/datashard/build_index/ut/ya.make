UNITTEST_FOR(ydb/core/tx/datashard)

FORK_SUBTESTS()

SPLIT_FACTOR(1)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/tx/datashard/ut_common
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    ydb/core/tx
    yql/essentials/public/udf/service/exception_policy
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/result
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_local_kmeans.cpp
    ut_prefix_kmeans.cpp
    ut_recompute_kmeans.cpp
    ut_reshuffle_kmeans.cpp
    ut_sample_k.cpp
    ut_secondary_index.cpp
    ut_unique_index.cpp
)

END()
