RECURSE(
    bin
    ut
)

PY3_LIBRARY(deploy_ydbd_slice)

PY_SRCS(
    __init__.py
    handlers.py
    nodes.py
)

PEERDIR(
    ydb/tools/ydbd_slice
)

END()
