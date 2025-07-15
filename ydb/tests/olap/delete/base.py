import yatest.common
import os
import logging

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class DeleteTestBase(object):
    @classmethod
    def setup_class(cls):
        cls._setup_ydb()

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _setup_ydb(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            overrided_actor_system_config={"batch_executor": 1, "io_executor": 1, "user_executor": 1,
                                           "service_executor": [{"service_name": "Interconnect", "executor_id": 4}],
                                           "executor": [{"name": "System", "threads": 1, "type": "BASIC", "spin_threshold": 0},
                                                        {"name": "User", "threads": 1, "type": "BASIC", "spin_threshold": 0},
                                                        {"name": "Batch", "threads": 1, "type": "BASIC", "spin_threshold": 0},
                                                        {"name": "IO", "threads": 1, "type": "IO", "time_per_mailbox_micro_secs": 100},
                                                        {"name": "IC", "threads": 1, "type": "BASIC", "spin_threshold": 10, "time_per_mailbox_micro_secs": 100}
                                                        ],
                                           "scheduler": {"resolution": 256, "spin_threshold": 0, "progress_threshold": 10000}},
        )
        # config = KikimrConfigGenerator()
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()
