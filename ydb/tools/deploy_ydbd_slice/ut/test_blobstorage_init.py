import json
from unittest import TestCase, mock

from ydb.tools.deploy_ydbd_slice.nodes import DeployNodes
from ydb.tools.ydbd_slice import blobstorage_init


def _completed(hosts, failed=()):
    payload = json.dumps([
        {
            'host': host,
            'exit_code': 2 if host in failed else 0,
            'stdout': '',
            'stderr': 'no' if host in failed else '',
        }
        for host in hosts
    ]).encode()
    return mock.Mock(returncode=0, stdout=payload, stderr=b'')


class DeployBlobstorageInitTest(TestCase):
    def test_one_pssh_covers_every_storage_host(self):
        hosts = ['h1', 'h2', 'h3']
        nodes = DeployNodes(hosts, ssh_user='root')
        with mock.patch(
            'ydb.tools.deploy_ydbd_slice.nodes.subprocess.run',
            return_value=_completed(hosts),
        ) as run:
            blobstorage_init.run_blobstorage_config_init_with_retry(nodes, hosts, 'init-cmd')

        self.assertEqual(run.call_count, 1)
        argv = run.call_args[0][0]
        self.assertEqual(argv[argv.index('-p') + 1], '3')
        self.assertIn('init-cmd', argv)
        self.assertEqual(argv[-3:], hosts)

    def test_retry_is_one_pssh_for_the_failed_hosts(self):
        hosts = ['h1', 'h2', 'h3']
        nodes = DeployNodes(hosts, ssh_user='root')
        waves = [
            _completed(hosts, failed=('h2', 'h3')),
            _completed(['h2', 'h3']),
        ]

        with mock.patch(
            'ydb.tools.deploy_ydbd_slice.nodes.subprocess.run',
            side_effect=waves,
        ) as run, mock.patch.object(blobstorage_init.time, 'sleep'), \
                mock.patch.object(blobstorage_init.time, 'monotonic', return_value=0):
            blobstorage_init.run_blobstorage_config_init_with_retry(nodes, hosts, 'init-cmd')

        self.assertEqual(run.call_count, 2)
        first = run.call_args_list[0][0][0]
        second = run.call_args_list[1][0][0]
        self.assertEqual(first[first.index('-p') + 1], '3')
        self.assertEqual(first[-3:], hosts)
        self.assertEqual(second[second.index('-p') + 1], '2')
        self.assertEqual(second[-2:], ['h2', 'h3'])
