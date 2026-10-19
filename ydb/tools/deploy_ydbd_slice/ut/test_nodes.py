import json
from unittest import TestCase, mock

from ydb.tools.deploy_ydbd_slice.nodes import DeployNodes, parse_pssh_json, ssh_target


class SshTargetTest(TestCase):
    def test_pod_fqdn_sshes_to_the_box(self):
        self.assertEqual(
            ssh_target("olap-perf-deployunit-36.klg.yp-c.yandex.net"),
            "box.olap-perf-deployunit-36.klg.yp-c.yandex.net",
        )
        self.assertEqual(ssh_target("box.already.yp-c.yandex.net"), "box.already.yp-c.yandex.net")
        self.assertEqual(ssh_target("host.search.yandex.net"), "host.search.yandex.net")


class ParsePsshJsonTest(TestCase):
    def test_list_of_host_results(self):
        raw = json.dumps([
            {"host": "a", "exit_code": 0, "stdout": "ok-a", "stderr": ""},
            {"host": "b", "exitCode": 2, "stdout": "", "stderr": "no"},
        ])
        parsed = parse_pssh_json(raw, ["a", "b"])
        self.assertEqual(parsed["a"]["retcode"], 0)
        self.assertEqual(parsed["a"]["stdout"], "ok-a")
        self.assertEqual(parsed["b"]["retcode"], 2)
        self.assertEqual(parsed["b"]["stderr"], "no")


class DeployNodesTest(TestCase):
    def test_one_pssh_run_covers_every_host(self):
        nodes = DeployNodes(["h1", "h2", "h3"], ssh_user="root")
        payload = json.dumps([
            {"host": host, "exit_code": 0, "stdout": "x", "stderr": ""}
            for host in ("h1", "h2", "h3")
        ])
        completed = mock.Mock(returncode=0, stdout=payload.encode(), stderr=b"")
        with mock.patch("ydb.tools.deploy_ydbd_slice.nodes.subprocess.run", return_value=completed) as run:
            nodes.execute_async("echo hi")
        argv = run.call_args[0][0]
        self.assertEqual(argv[0], "pssh")
        self.assertIn("run", argv)
        self.assertIn("-p", argv)
        self.assertEqual(argv[argv.index("-p") + 1], "3")
        self.assertIn("-u", argv)
        self.assertEqual(argv[argv.index("-u") + 1], "root")
        self.assertEqual(argv[-3:], ["h1", "h2", "h3"])
        self.assertEqual(run.call_count, 1)

    def test_copy_uses_one_pssh_scp_from_local(self):
        nodes = DeployNodes(["h1", "h2"], ssh_user="root")
        with mock.patch.object(nodes, "execute_async") as execute_async, \
                mock.patch.object(nodes, "_pssh_scp") as scp, \
                mock.patch("ydb.tools.deploy_ydbd_slice.nodes.os.path.isdir", return_value=False):
            nodes.copy("/tmp/kikimr", "/Berkanavt/kikimr/bin/kikimr")
        mkdir_cmd = execute_async.call_args_list[0][0][0]
        self.assertIn("mkdir -p", mkdir_cmd)
        scp.assert_called_once_with(
            "/tmp/kikimr",
            "/Berkanavt/kikimr/bin/kikimr.deploy-new",
            ["h1", "h2"],
        )
        mv_cmd = execute_async.call_args_list[-1][0][0]
        self.assertIn("mv -f", mv_cmd)
        self.assertIn("/Berkanavt/kikimr/bin/kikimr", mv_cmd)
