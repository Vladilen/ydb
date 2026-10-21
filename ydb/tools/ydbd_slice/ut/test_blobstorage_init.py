from unittest import TestCase, mock

from ydb.tools.ydbd_slice import blobstorage_init


class _ScriptedNodes(object):
    def __init__(self, attempts):
        self._attempts = list(attempts)
        self.calls = []

    def execute_async_ret(self, cmd, check_retcode=False, nodes=None, results=None):
        self.calls.append(list(nodes))
        return [(cmd, host) for host in nodes]

    def _check_async_execution(self, running_jobs, check_retcode=False, results=None, retry_attempts=0):
        outcome = self._attempts.pop(0)
        for _cmd, host in running_jobs:
            retcode = outcome[host]
            results[host] = {
                'retcode': retcode,
                'stdout': '',
                'stderr': '' if retcode == 0 else 'failed',
            }


class BlobstorageInitTest(TestCase):
    def test_one_wave_covers_every_host(self):
        nodes = _ScriptedNodes([{'a': 0, 'b': 0, 'c': 0}])
        blobstorage_init.run_blobstorage_config_init_with_retry(nodes, ['a', 'b', 'c'], 'init')
        self.assertEqual(nodes.calls, [['a', 'b', 'c']])

    def test_retry_repeats_only_failed_hosts_together(self):
        nodes = _ScriptedNodes([
            {'a': 0, 'b': 2, 'c': 2},
            {'b': 0, 'c': 0},
        ])
        with mock.patch.object(blobstorage_init.time, 'sleep') as sleep, \
                mock.patch.object(blobstorage_init.time, 'monotonic', return_value=0):
            blobstorage_init.run_blobstorage_config_init_with_retry(nodes, ['a', 'b', 'c'], 'init')
        self.assertEqual(nodes.calls, [['a', 'b', 'c'], ['b', 'c']])
        sleep.assert_called_once()

    def test_deadline_exits_without_another_wave(self):
        nodes = _ScriptedNodes([{'a': 2}])
        times = iter([0, blobstorage_init.BLOBSTORAGE_INIT_RETRY_DEADLINE_SECONDS])
        with mock.patch.object(blobstorage_init.time, 'sleep') as sleep, \
                mock.patch.object(blobstorage_init.time, 'monotonic', side_effect=lambda: next(times)):
            with self.assertRaises(SystemExit):
                blobstorage_init.run_blobstorage_config_init_with_retry(nodes, ['a'], 'init')
        self.assertEqual(nodes.calls, [['a']])
        sleep.assert_not_called()
