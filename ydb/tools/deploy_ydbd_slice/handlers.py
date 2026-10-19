"""ydbd_slice start/stop for Deploy pods.

Pod Agent is PID 1's supervisor. There is no systemd. A workload stays in the
foreground and Pod Agent starts it again about 10s after the process exits.
Stop and start therefore kill /Berkanavt/kikimr/bin/kikimr and wait for that
restart. They do not keep the process down.

The workload command must be that binary. The storage workload must include
--node static. A dynamic slot must include --ic-port <slot>, where <slot> is
the ic port ydbd_slice uses (31003 for the first slot).
"""
import logging
import time

from ydb.tools.ydbd_slice.handlers import Slice

logger = logging.getLogger(__name__)

POD_AGENT_RESTART_SEC = 10


def kill_kikimr_command(extra_regex=None, signal=None):
    """Remote shell command. The pattern uses [k] so it does not match itself."""
    clauses = ['$0 ~ /[k]ikimr\\/bin\\/kikimr/']
    if extra_regex:
        clauses.append("$0 ~ /{}/".format(extra_regex))
    clauses.append("$0 !~ /awk/")
    script = " && ".join(clauses) + " {print $1}"
    sig = "-%s " % signal if signal else ""
    return (
        "ps -eo pid=,args= | awk '%s' | "
        "while read -r pid; do sudo kill %s\"$pid\" || true; done"
    ) % (script, sig)


class DeploySlice(Slice):
    def _wait_for_pod_agent(self):
        logger.info(
            "waiting %ss for pod agent to restart killed workloads",
            POD_AGENT_RESTART_SEC,
        )
        time.sleep(POD_AGENT_RESTART_SEC)

    def _kill_kikimr(self, extra_regex=None, hosts=None):
        self.nodes.execute_async(
            kill_kikimr_command(extra_regex),
            check_retcode=False,
            nodes=hosts,
        )

    def _deploy_secrets(self):
        logger.info("skipping yav secrets: deploy pods do not have the yav client")

    def _clear_logs(self):
        cmd = (
            "find /Berkanavt/ -mindepth 2 -maxdepth 2 -name logs | "
            "egrep '^/Berkanavt/kikimr' | sudo xargs -I% find % -mindepth 1 -delete"
        )
        self.nodes.execute_async(cmd, check_retcode=False)

    def _start_slot(self, slot):
        self._kill_kikimr("--ic-port {}".format(slot.slot))

    def _start_slot_command(self, slot, tenant, node_bind=None):
        return kill_kikimr_command("--ic-port {}".format(slot.slot))

    def _start_all_slots(self):
        self._kill_kikimr()

    def _start_static(self):
        storage_hosts = self._storage_hosts()
        storage_host_set = set(storage_hosts)
        skip_hosts = [node for node in self.nodes.nodes_list if node not in storage_host_set]
        if skip_hosts:
            self._stop_static_on_hosts(skip_hosts)
        if storage_hosts:
            self._kill_kikimr("--node static", hosts=storage_hosts)
            self._wait_for_pod_agent()

    def _start_dynamic(self):
        if "dynamic_slots" not in self.components:
            return
        super(DeploySlice, self)._start_dynamic()
        self._wait_for_pod_agent()

    def _stop_all_slots(self):
        self._kill_kikimr()

    def _stop_slot_ret(self, slot):
        return self.nodes.execute_async_ret(
            kill_kikimr_command("--ic-port {}".format(slot.slot)),
            check_retcode=False,
        )

    def _stop_static_on_hosts(self, hosts):
        self._kill_kikimr("--node static", hosts=hosts)

    def _format_drives(self):
        # Pod Agent restarts kikimr about 10s after it exits, and the new
        # process locks the pdisk. Kill and obliterate in one command, and
        # repeat until the wipe wins that race.
        tasks = []
        kill = kill_kikimr_command(signal="KILL")
        allowed = set(self.nodes.nodes_list)
        for host_name, drive_path in self._get_all_drives():
            if host_name not in allowed:
                continue
            obliterate = "sudo {bin} admin bs disk obliterate {path}".format(
                bin=self.slice_kikimr_path,
                path=drive_path,
            )
            cmd = (
                "ok=1; "
                "for try in 1 2 3 4 5 6 7 8 9 10 11 12; do "
                "{kill}; "
                "if {obliterate}; then ok=0; break; fi; "
                "sleep 1; "
                "done; "
                "exit $ok"
            ).format(kill=kill, obliterate=obliterate)
            tasks.extend(self.nodes.execute_async_ret(cmd, nodes=[host_name]))
        self.nodes._check_async_execution(tasks)

    def slice_stop(self):
        super(DeploySlice, self).slice_stop()
        self._wait_for_pod_agent()
