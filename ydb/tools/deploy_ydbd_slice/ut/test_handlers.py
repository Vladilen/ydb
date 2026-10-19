from types import SimpleNamespace
from unittest import TestCase, mock

from ydb.tools.deploy_ydbd_slice.handlers import DeploySlice, kill_kikimr_command


class KillCommandTest(TestCase):
    def test_command_kills_and_does_not_call_systemd(self):
        cmd = kill_kikimr_command("--node static")
        self.assertNotIn("systemctl", cmd)
        self.assertNotIn("service ", cmd)
        self.assertNotIn("/sbin/start", cmd)
        self.assertNotIn("/sbin/stop", cmd)
        self.assertIn("kill", cmd)
        self.assertIn("--node static", cmd)
        self.assertIn("[k]ikimr", cmd)

    def test_start_static_kills_then_waits(self):
        nodes = mock.Mock()
        nodes.nodes_list = ["box.example"]
        details = SimpleNamespace(
            grpc_config={"port": 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={},
            host_storage_profile={},
            host_dynamic_profiles={},
        )
        with mock.patch("ydb.tools.ydbd_slice.handlers.config_client.ConfigClient"):
            deploy = DeploySlice({"kikimr": ["cfg"]}, nodes, details)
        deploy._storage_hosts = lambda: ["box.example"]
        with mock.patch("ydb.tools.deploy_ydbd_slice.handlers.time.sleep") as sleep:
            deploy._start_static()
        cmd = nodes.execute_async.call_args[0][0]
        self.assertNotIn("systemctl", cmd)
        self.assertIn("--node static", cmd)
        sleep.assert_called_once_with(10)

    def test_deploy_secrets_does_not_call_yav(self):
        nodes = mock.Mock()
        nodes.nodes_list = ["box.example"]
        details = SimpleNamespace(
            grpc_config={"port": 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={},
            host_storage_profile={},
            host_dynamic_profiles={},
        )
        with mock.patch("ydb.tools.ydbd_slice.handlers.config_client.ConfigClient"):
            deploy = DeploySlice({"kikimr": ["cfg"]}, nodes, details)
        deploy.yav_version = "ver-01gswscgce37hdbqyssjm3nd7x"
        deploy._deploy_secrets()
        nodes.execute_async.assert_not_called()
