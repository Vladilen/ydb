PY3TEST()

TEST_SRCS(
    test_blobstorage_init.py
    test_handlers.py
    test_process_profiles.py
)

PEERDIR(
    ydb/tools/ydbd_slice
)

END()
