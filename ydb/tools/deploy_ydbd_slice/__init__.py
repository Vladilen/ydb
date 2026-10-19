import ydb.tools.ydbd_slice as ydbd_slice
from ydb.tools.ydbd_slice import handlers as slice_handlers

from ydb.tools.deploy_ydbd_slice.handlers import DeploySlice
from ydb.tools.deploy_ydbd_slice.nodes import DeployNodes


def main(walle_provider=None):
    slice_handlers.Slice = DeploySlice
    ydbd_slice.nodes.Nodes = DeployNodes
    ydbd_slice.main(walle_provider)
