PY3TEST()

TEST_SRCS(
    test_handlers.py
    test_nodes.py
)

PEERDIR(
    ydb/tools/deploy_ydbd_slice
)

END()
