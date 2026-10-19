import hashlib
import tempfile
from types import SimpleNamespace
from unittest import TestCase, mock

import ydb.tools.ydbd_slice as ydbd_slice_tool
from ydb.tools.cfg.static import StaticConfigGenerator
from ydb.tools.ydbd_slice import cluster_description
from ydb.tools.ydbd_slice import handlers
from ydb.tools.ydbd_slice import nodes


class SliceTest(TestCase):
    def test_config_client_uses_selected_host(self):
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname='unavailable-host')],
            grpc_config={'port': 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={},
        )

        with mock.patch.object(handlers.config_client, 'ConfigClient') as config_client:
            handlers.Slice({}, nodes.Nodes(['selected-host']), cluster_details)

        config_client.assert_called_once_with('selected-host', 2135, retry_count=10)

    def test_available_slots_honor_per_host_dynamic_slots(self):
        slot1 = SimpleNamespace(slot='31003', domain='olap-perf')
        slot2 = SimpleNamespace(slot='31013', domain='olap-perf')
        olap = 'ydb-olap-perf-001.search.yandex.net'
        vla = 'vla5-2569.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=olap)],
            grpc_config={'port': 2135},
            domains=[SimpleNamespace(domain_name='olap-perf')],
            dynamic_slots=[slot1, slot2],
            host_dynamic_slot_counts={
                olap: 1,
                vla: 2,
            },
            host_storage_enabled={},
        )
        walle_provider = SimpleNamespace(get_datacenter=lambda hostname: 'FAKE')

        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice = handlers.Slice(
                {'dynamic_slots': ['all']},
                nodes.Nodes([olap, vla]),
                cluster_details,
                walle_provider=walle_provider,
            )

        slots_per_domain, total = slice._get_available_slots()
        any_zone = list(slots_per_domain['olap-perf']['any'])

        self.assertEqual(total, 3)
        self.assertEqual(
            [(item[0].slot, item[1]) for item in any_zone],
            [
                ('31003', olap),
                ('31003', vla),
                ('31013', vla),
            ],
        )

    def test_host_dynamic_slot_counts_from_yaml(self):
        content = """\
hosts:
- name: olap-1.search.yandex.net
  dynamic_slots: 1
- host: vla-1.search.yandex.net
  dynamic_slots: 2
- name: other.search.yandex.net
domains:
- domain_name: Root
  dynamic_slots: 2
static_erasure: none
"""
        with mock.patch(
            'builtins.open',
            mock.mock_open(read_data=content),
        ):
            details = cluster_description.ClusterDetails('cluster.yaml')

        self.assertEqual(
            details.host_dynamic_slot_counts,
            {
                'olap-1.search.yandex.net': 1,
                'vla-1.search.yandex.net': 2,
            },
        )

    def test_start_static_skips_hosts_with_storage_false(self):
        olap = 'ydb-olap-perf-001.search.yandex.net'
        vla = 'vla5-2569.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=olap)],
            grpc_config={'port': 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={
                vla: False,
            },
        )

        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice = handlers.Slice(
                {'kikimr': ['bin', 'cfg']},
                nodes.Nodes([olap, vla]),
                cluster_details,
            )

        with mock.patch.object(slice.nodes, 'execute_async') as execute_async:
            slice._start_static()

        execute_async.assert_has_calls([
            mock.call("sudo service kikimr stop", check_retcode=False, nodes=[vla]),
            mock.call("sudo service kikimr start", check_retcode=True, nodes=[olap]),
        ])

    def test_host_storage_enabled_from_yaml(self):
        content = """\
hosts:
- name: olap-1.search.yandex.net
  host_config_id: 1
- name: vla-1.search.yandex.net
  storage: false
static_erasure: none
"""
        with mock.patch(
            'builtins.open',
            mock.mock_open(read_data=content),
        ):
            details = cluster_description.ClusterDetails('cluster.yaml')

        self.assertEqual(
            details.host_storage_enabled,
            {
                'vla-1.search.yandex.net': False,
            },
        )

    def test_skip_host_keeps_node_id_and_drops_ssh_target(self):
        content = """\
hosts:
- name: olap-1.search.yandex.net
  node_id: 1
  dynamic_slots: 1
  location:
    body: 1
    data_center: DCA
    rack: '1'
- name: dead.search.yandex.net
  node_id: 2
  skip_host: true
  dynamic_slots: 2
  location:
    body: 2
    data_center: DCA
    rack: '2'
- name: vla-1.search.yandex.net
  node_id: 11
  dynamic_slots: 2
  location:
    body: 11
    data_center: DCA
    rack: '11'
domains:
- domain_name: Root
  dynamic_slots: 2
static_erasure: none
"""
        with mock.patch('builtins.open', mock.mock_open(read_data=content)):
            details = cluster_description.ClusterDetails('cluster.yaml')

        self.assertEqual(
            [(host.hostname, host.node_id) for host in details.hosts],
            [
                ('olap-1.search.yandex.net', 1),
                ('dead.search.yandex.net', 2),
                ('vla-1.search.yandex.net', 11),
            ],
        )
        self.assertEqual(
            details.hosts_names,
            ['olap-1.search.yandex.net', 'vla-1.search.yandex.net'],
        )

    def test_storage_profile_rewrite_skips_hosts_outside_ssh_list(self):
        live = 'ydb-olap-perf-001.search.yandex.net'
        dead = 'vla5-2573.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=live), SimpleNamespace(hostname=dead)],
            grpc_config={'port': 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={},
            host_storage_profile={live: 'storage', dead: 'storage'},
            host_dynamic_profiles={},
        )
        configurator = SimpleNamespace(enable_process_profiles=True)

        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'kikimr': ['cfg']},
                nodes.Nodes([live]),
                cluster_details,
                configurator=configurator,
            )

        with mock.patch.object(slice_obj.nodes, 'execute_per_node') as execute_per_node:
            slice_obj._apply_storage_process_profiles()

        execute_per_node.assert_called_once()
        jobs = execute_per_node.call_args[0][0]
        self.assertEqual([host for _cmd, host in jobs], [live])

    def test_apply_storage_process_profiles_rewrites_kikimr_cfg(self):
        host = 'ydb-olap-perf-001.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host)],
            grpc_config={'port': 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={},
            host_storage_profile={host: 'storage'},
            host_dynamic_profiles={},
        )
        configurator = SimpleNamespace(enable_process_profiles=True)

        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'kikimr': ['cfg']},
                nodes.Nodes([host]),
                cluster_details,
                configurator=configurator,
            )

        with mock.patch.object(slice_obj.nodes, 'execute_per_node') as execute_per_node:
            slice_obj._apply_storage_process_profiles()

        execute_per_node.assert_called_once()
        jobs = execute_per_node.call_args[0][0]
        self.assertEqual(len(jobs), 1)
        cmd, job_host = jobs[0]
        self.assertIn('config.p_storage.yaml', cmd)
        self.assertIn('--yaml-config ${kikimr_config}/config.yaml', cmd)
        self.assertEqual(job_host, host)

    def test_deploy_slot_config_copies_profile_yaml(self):
        host = 'vla5-2569.search.yandex.net'
        slot = SimpleNamespace(
            slot='31003',
            domain='olap-perf',
            grpc=31001,
            ic=31003,
            mbus=31002,
            mon=31004,
            kafka_port=31005,
        )
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host)],
            grpc_config={'port': 2135},
            domains=[SimpleNamespace(domain_name='olap-perf')],
            dynamic_slots=[slot],
            host_dynamic_slot_counts={host: 1},
            host_storage_enabled={},
            host_storage_profile={},
            host_dynamic_profiles={host: ['dyn-a']},
        )
        tenant = SimpleNamespace(name='db')

        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'dynamic_slots': ['all']},
                nodes.Nodes([host]),
                cluster_details,
            )

        with mock.patch.object(slice_obj.nodes, 'execute_async') as execute_async:
            slice_obj._deploy_slot_config_for_tenant(slot, tenant, host)

        copy_call = execute_async.call_args_list[-1]
        self.assertIn('config.p_dyn-a.yaml', copy_call[0][0])
        self.assertIn('/Berkanavt/kikimr_31003/config.yaml', copy_call[0][0])
        self.assertEqual(copy_call[1]['nodes'], [host])

    def _dynamic_slice(self, host_names, slot_counts, profiles=None):
        class Slot(object):
            def __init__(self):
                self.slot = '31003'
                self.domain = 'olap-perf'
                self.grpc = 31001
                self.ic = 31003
                self.mbus = 31000
                self.mon = 31002
                self.kafka_port = 31005

            def __hash__(self):
                return hash(self.slot)

            def __eq__(self, other):
                return isinstance(other, Slot) and self.slot == other.slot

        slot = Slot()
        tenant = SimpleNamespace(
            name='deploy_logs',
            compute_units=[SimpleNamespace(zone='any', count=len(host_names))],
        )
        domain = SimpleNamespace(
            domain_name='olap-perf',
            tenants=[tenant],
            bind_slots_to_numa_nodes=False,
        )
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host) for host in host_names],
            grpc_config={'port': 2135},
            domains=[domain],
            dynamic_slots=[slot],
            host_dynamic_slot_counts=slot_counts,
            host_storage_enabled={},
            host_storage_profile={},
            host_dynamic_profiles=profiles or {},
        )
        walle_provider = SimpleNamespace(get_datacenter=lambda hostname: 'FAKE')
        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            return handlers.Slice(
                {'dynamic_slots': ['all']},
                nodes.Nodes(list(host_names)),
                cluster_details,
                walle_provider=walle_provider,
            )

    def test_deploy_slot_configs_runs_hosts_together(self):
        hosts = [
            'vla5-2567.search.yandex.net',
            'vla5-2568.search.yandex.net',
        ]
        slice_obj = self._dynamic_slice(hosts, {host: 1 for host in hosts})

        with mock.patch.object(slice_obj.nodes, 'execute_per_node') as execute_per_node:
            slice_obj._deploy_slot_configs()

        execute_per_node.assert_called_once()
        jobs = execute_per_node.call_args[0][0]
        self.assertEqual([host for _cmd, host in jobs], hosts)
        self.assertTrue(all('slot_cfg' in cmd for cmd, _host in jobs))
        self.assertEqual(execute_per_node.call_args[1]['check_retcode'], False)

    def test_start_dynamic_runs_hosts_together(self):
        hosts = [
            'vla5-2567.search.yandex.net',
            'vla5-2568.search.yandex.net',
        ]
        slice_obj = self._dynamic_slice(hosts, {host: 1 for host in hosts})

        with mock.patch.object(slice_obj.nodes, 'execute_per_node') as execute_per_node:
            slice_obj._start_dynamic()

        execute_per_node.assert_called_once()
        jobs = execute_per_node.call_args[0][0]
        self.assertEqual([host for _cmd, host in jobs], hosts)
        self.assertTrue(all('kikimr-multi' in cmd for cmd, _host in jobs))
        self.assertEqual(execute_per_node.call_args[1]['check_retcode'], False)


class ExecutePerNodeTest(TestCase):
    def test_starts_every_host_before_waiting(self):
        events = []

        class FakeProc(object):
            def __init__(self, host):
                self.host = host

            def communicate(self):
                events.append(('wait', self.host))
                return b'', b''

            def poll(self):
                return 0

        def fake_popen(cmd, stdout=None, stderr=None):
            host = cmd[-2]
            events.append(('start', host))
            return FakeProc(host)

        node = nodes.Nodes(['h1', 'h2'])
        with mock.patch('subprocess.Popen', side_effect=fake_popen):
            node.execute_per_node([
                ('echo a', 'h1'),
                ('echo b', 'h2'),
            ])

        self.assertEqual(events, [
            ('start', 'h1'),
            ('start', 'h2'),
            ('wait', 'h1'),
            ('wait', 'h2'),
        ])


class BinaryUpdatePolicyTest(TestCase):
    def test_update_without_binary_keeps_config_and_drops_bin(self):
        components = {
            'kikimr': ['bin', 'cfg'],
            'dynamic_slots': ['all'],
            'confirm': True,
        }
        relaxed = ydbd_slice_tool.omit_binary_deploy(components, binary_specified=False)
        self.assertEqual(relaxed['kikimr'], ['cfg'])
        self.assertEqual(relaxed['dynamic_slots'], ['all'])
        self.assertEqual(components['kikimr'], ['bin', 'cfg'])

    def test_explicit_binary_component_still_requires_binary(self):
        components = {'kikimr': ['bin'], 'confirm': True}
        self.assertIs(
            ydbd_slice_tool.omit_binary_deploy(components, binary_specified=False),
            components,
        )

    def test_specified_binary_is_deployed(self):
        components = {'kikimr': ['bin', 'cfg'], 'confirm': True}
        self.assertIs(
            ydbd_slice_tool.omit_binary_deploy(components, binary_specified=True),
            components,
        )
        self.assertTrue(ydbd_slice_tool.binary_was_specified(SimpleNamespace(binary='/ydbd', arcadia=False)))
        self.assertTrue(ydbd_slice_tool.binary_was_specified(SimpleNamespace(binary=None, arcadia=True)))
        self.assertFalse(ydbd_slice_tool.binary_was_specified(SimpleNamespace(binary=None, arcadia=False)))

    def test_template_needs_ydbd_only_when_groups_are_missing(self):
        self.assertTrue(cluster_description.template_needs_ydbd_binary({}))
        self.assertTrue(cluster_description.template_needs_ydbd_binary({
            'static_groups': {'groups': []},
        }))
        self.assertFalse(cluster_description.template_needs_ydbd_binary({
            'blob_storage_config': {'service_set': {'groups': [{'group_id': 0}]}},
        }))

    def test_update_kikimr_skips_copy_when_digest_matches(self):
        host = 'ydb-olap-perf-001.search.yandex.net'
        payload = b'same-ydbd'
        digest = hashlib.sha256(payload).hexdigest()
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host)],
            grpc_config={'port': 2135},
        )
        with tempfile.NamedTemporaryFile() as binary:
            binary.write(payload)
            binary.flush()
            with mock.patch.object(handlers.config_client, 'ConfigClient'):
                slice_obj = handlers.Slice(
                    {'kikimr': ['bin', 'cfg']},
                    nodes.Nodes([host]),
                    cluster_details,
                    bin=binary.name,
                )

            def execute_async(cmd, check_retcode=True, nodes=None, results=None, retry_attempts=5):
                results[host] = {
                    'retcode': 0,
                    'stdout': digest + '  /Berkanavt/kikimr/bin/kikimr\n',
                    'stderr': '',
                }

            slice_obj.nodes.execute_async = execute_async
            slice_obj.nodes.copy = mock.Mock()
            slice_obj._update_kikimr()
            slice_obj.nodes.copy.assert_not_called()

    def test_update_kikimr_copies_when_digest_differs(self):
        host = 'ydb-olap-perf-001.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host)],
            grpc_config={'port': 2135},
        )
        with tempfile.NamedTemporaryFile() as binary:
            binary.write(b'new-ydbd')
            binary.flush()
            with mock.patch.object(handlers.config_client, 'ConfigClient'):
                slice_obj = handlers.Slice(
                    {'kikimr': ['bin', 'cfg']},
                    nodes.Nodes([host]),
                    cluster_details,
                    bin=binary.name,
                )

            def execute_async(cmd, check_retcode=True, nodes=None, results=None, retry_attempts=5):
                results[host] = {
                    'retcode': 0,
                    'stdout': '0' * 64 + '  /Berkanavt/kikimr/bin/kikimr\n',
                    'stderr': '',
                }

            slice_obj.nodes.execute_async = execute_async
            slice_obj.nodes.copy = mock.Mock()
            slice_obj._update_kikimr()
            slice_obj.nodes.copy.assert_called_once_with(
                binary.name,
                handlers.Slice.slice_kikimr_path,
                compressed_path=None,
            )

    def test_run_ydbd_uses_cluster_binary_and_uploaded_format_file(self):
        host = 'h1'
        node = nodes.Nodes([host])
        copied = {}

        def copy_on_node(local_path, copy_host, remote_path):
            copied['local'] = local_path
            copied['host'] = copy_host
            copied['remote'] = remote_path

        captured = {}

        def run_capture(capture_host, cmd):
            captured['host'] = capture_host
            captured['cmd'] = cmd
            return b'groups {}'

        cleanup = []
        node._copy_on_node = copy_on_node
        node._run_capture = run_capture
        node.execute_async = lambda cmd, **kwargs: cleanup.append(cmd)

        output = node.run_ydbd(
            '/Berkanavt/kikimr/bin/kikimr',
            ['/local/ydbd', 'admin', 'bs', 'genconfig', 'static', '--bs-format-file', '/tmp/local-format'],
        )

        self.assertEqual(output, b'groups {}')
        self.assertEqual(copied['local'], '/tmp/local-format')
        self.assertEqual(copied['host'], host)
        self.assertNotIn('/tmp/local-format', captured['cmd'])
        self.assertIn(copied['remote'], captured['cmd'])
        self.assertIn('sudo /Berkanavt/kikimr/bin/kikimr ', captured['cmd'])
        self.assertTrue(any(cmd.startswith('sudo chmod 644 ') for cmd in cleanup))
        self.assertTrue(any(cmd.startswith('sudo rm -f ') for cmd in cleanup))

    def test_invoke_ydbd_requires_binary_or_runner(self):
        generator = object.__new__(StaticConfigGenerator)
        generator._StaticConfigGenerator__ydbd_runner = None
        generator._StaticConfigGenerator__local_binary_path = None
        with self.assertRaises(RuntimeError):
            generator._invoke_ydbd(['ydbd'])

        generator._StaticConfigGenerator__ydbd_runner = lambda cmd: b'from-cluster'
        self.assertEqual(generator._invoke_ydbd(['ignored']), b'from-cluster')
