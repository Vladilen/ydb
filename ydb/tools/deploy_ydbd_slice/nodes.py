"""Host access for deploy_ydbd_slice.

One pssh invocation covers every host, with -p equal to the host count.
pssh then uses a single Yubikey session certificate instead of one touch per ssh.
Copies go from this machine to every VM in that same invocation.
"""
import json
import logging
import os
import shlex
import subprocess
import sys

from ydb.tools.ydbd_slice.nodes import Nodes

logger = logging.getLogger(__name__)


class _Deferred(object):
    def __init__(self, host):
        self.host = host


def _as_text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def parse_pssh_json(raw, hosts):
    """Best-effort map of host -> {retcode, stdout, stderr} from pssh --format json."""
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        data = None
        items = []
        decoder = json.JSONDecoder()
        blob = (raw or "").strip()
        pos = 0
        while pos < len(blob):
            while pos < len(blob) and blob[pos].isspace():
                pos += 1
            if pos >= len(blob):
                break
            try:
                obj, end = decoder.raw_decode(blob, pos)
            except ValueError:
                items = []
                break
            items.append(obj)
            pos = end
        if not items:
            return {}
        data = items

    items = None
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for key in ("results", "hosts", "data"):
            if isinstance(data.get(key), list):
                items = data[key]
                break
        if items is None and data and all(isinstance(value, dict) for value in data.values()):
            items = []
            for host, value in data.items():
                item = dict(value)
                item.setdefault("host", host)
                items.append(item)
        elif items is None and any(key in data for key in ("stdout", "exit_code", "exitCode")):
            items = [data]
    if not items:
        return {}

    found = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        host = item.get("host") or item.get("hostname") or item.get("fqdn")
        if host is None and len(hosts) == 1:
            host = hosts[0]
        if not host:
            continue
        code = item.get("exit_code", item.get("exitCode", item.get("exit_status", item.get("code", 0))))
        try:
            code = int(code or 0)
        except (TypeError, ValueError):
            code = 1
        stderr = item.get("stderr")
        if stderr in (None, ""):
            stderr = item.get("error") or ""
        if isinstance(stderr, dict):
            stderr = stderr.get("message") or str(stderr)
        found[host] = {
            "retcode": code,
            "stdout": _as_text(item.get("stdout") if item.get("stdout") is not None else item.get("output")),
            "stderr": _as_text(stderr),
        }
    return found


def ssh_target(host):
    """YDB identity is the pod FQDN. SSH to the box is box.<pod FQDN>."""
    if host.startswith("box."):
        return host
    if host.endswith(".yp-c.yandex.net"):
        return "box." + host
    return host


class DeployNodes(Nodes):
    def __init__(self, nodes, dry_run=False, ssh_user=None, queue_size=0, ssh_key_path=None):
        super(DeployNodes, self).__init__(
            nodes,
            dry_run=dry_run,
            ssh_user=ssh_user,
            queue_size=queue_size,
            ssh_key_path=ssh_key_path,
        )
        self._parallel = queue_size if queue_size and queue_size > 0 else len(nodes)

    def _pssh_prefix(self, parallel):
        argv = ["pssh", "-n", "-p", str(parallel)]
        if self._ssh_user:
            argv.extend(["-u", self._ssh_user])
        return argv

    def _pssh_run(self, cmd, hosts):
        hosts = list(hosts)
        targets = [ssh_target(host) for host in hosts]
        by_target = {}
        for host, target in zip(hosts, targets):
            by_target.setdefault(target, host)
        parallel = min(self._parallel, len(targets))
        argv = self._pssh_prefix(parallel) + [
            "run",
            "--format",
            "json",
            "-p",
            str(parallel),
            cmd,
        ] + targets
        self._logger.info("pssh run -p %s on %s", parallel, ", ".join(targets))
        if self._dry_run:
            return {host: {"retcode": 0, "stdout": "", "stderr": ""} for host in hosts}

        proc = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = _as_text(proc.stdout)
        stderr = _as_text(proc.stderr)
        parsed_targets = parse_pssh_json(stdout, targets)
        parsed = {}
        for target, info in parsed_targets.items():
            parsed[by_target.get(target, target)] = info
        if not parsed:
            self._logger.info("pssh json parse failed, using process status. stdout:\n%s\nstderr:\n%s", stdout, stderr)
            for host in hosts:
                parsed[host] = {
                    "retcode": proc.returncode,
                    "stdout": stdout,
                    "stderr": stderr,
                }
            return parsed
        for host in hosts:
            parsed.setdefault(host, {"retcode": proc.returncode or 1, "stdout": "", "stderr": stderr})
        return parsed

    def execute_async_ret(self, cmd, check_retcode=True, nodes=None, results=None):
        hosts = list(nodes if nodes is not None else self._nodes)
        jobs = []
        for host in hosts:
            self._logger.info("execute '%s' at '%s'", cmd, host)
            if self._dry_run:
                continue
            jobs.append((cmd, _Deferred(host), host))
        return jobs

    def _check_async_execution(self, running_jobs, check_retcode=True, results=None, retry_attempts=0):
        if self._dry_run or not running_jobs:
            return
        if not isinstance(running_jobs[0][1], _Deferred):
            return super(DeployNodes, self)._check_async_execution(
                running_jobs, check_retcode, results, retry_attempts
            )

        grouped = {}
        order = []
        for cmd, _deferred, host in running_jobs:
            if cmd not in grouped:
                order.append(cmd)
                grouped[cmd] = []
            grouped[cmd].append(host)

        failed = []
        for cmd in order:
            hosts = grouped[cmd]
            per_host = self._pssh_run(cmd, hosts)
            for host in hosts:
                info = per_host[host]
                if results is not None:
                    results[host] = info
                if info["retcode"] != 0:
                    failed.append((cmd, host, info))

        if not failed or not check_retcode:
            return
        if retry_attempts > 0:
            retry_jobs = [(cmd, _Deferred(host), host) for cmd, host, _info in failed]
            self._check_async_execution(retry_jobs, check_retcode, results, retry_attempts - 1)
            return

        chunks = []
        for cmd, host, info in failed:
            chunks.append(
                "execution '{cmd}' at '{host}' finished with '{retcode}'\nstdout:\n{stdout}\nstderr:\n{stderr}".format(
                    cmd=cmd,
                    host=host,
                    retcode=info["retcode"],
                    stdout=info["stdout"],
                    stderr=info["stderr"],
                )
            )
        message = "\n".join(chunks)
        self._logger.critical(message)
        sys.exit(message)

    def _run_capture(self, host, cmd):
        self._logger.info("execute '%s' at '%s'", cmd, host)
        if self._dry_run:
            return b""
        info = self._pssh_run(cmd, [host])[host]
        if info["retcode"] != 0:
            self._logger.error("command failed on %s, stderr:\n%s", host, info["stderr"])
            raise subprocess.CalledProcessError(info["retcode"], cmd, output=info["stdout"])
        return info["stdout"].encode("utf-8")

    def _copy_on_node(self, local_path, host, remote_path):
        self._logger.info("copy %s to %s:%s", local_path, host, remote_path)
        self._pssh_scp(local_path, remote_path, [host])

    def _install_file(self, local_path, remote_path, hosts):
        """Upload beside the destination, then rename over it.

        scp opens the destination with truncation. A running binary returns
        ETXTBSY ("Text file busy"). rename keeps the old inode for the
        running process and points the path at the new file.
        """
        remote_dir = os.path.dirname(remote_path) or "/"
        tmp = remote_path + ".deploy-new"
        self.execute_async("sudo mkdir -p {0}".format(shlex.quote(remote_dir)))
        self._pssh_scp(local_path, tmp, hosts)
        chmod = ""
        if os.path.isfile(local_path) and os.stat(local_path).st_mode & 0o111:
            chmod = " && sudo chmod a+rx {0}".format(shlex.quote(remote_path))
        self.execute_async(
            "sudo mv -f {tmp} {dst}{chmod}".format(
                tmp=shlex.quote(tmp),
                dst=shlex.quote(remote_path),
                chmod=chmod,
            )
        )

    def _copy_tree(self, local_path, remote_path):
        """Unpack a directory over the destination.

        pssh scp of a directory onto an existing cfg dir leaves the old files
        in place. tar -xf replaces them, including config.yaml.
        """
        local_root = local_path.rstrip(os.sep)
        remote_root = remote_path.rstrip(os.sep)
        tar_local = local_root + ".deploy.tar"
        remote_tar = "/tmp/ydbd-slice-cfg.deploy.tar"
        subprocess.check_call(["tar", "-C", local_root, "-cf", tar_local, "."])
        try:
            self._pssh_scp(tar_local, remote_tar, self._nodes)
            self.execute_async(
                "sudo mkdir -p {dst} && sudo tar -C {dst} -xf {tar} && sudo rm -f {tar}".format(
                    dst=shlex.quote(remote_root),
                    tar=shlex.quote(remote_tar),
                )
            )
        finally:
            if os.path.isfile(tar_local):
                os.remove(tar_local)

    def _pssh_scp(self, local_path, remote_path, hosts):
        hosts = list(hosts)
        if not hosts:
            return
        parallel = min(self._parallel, len(hosts))
        recursive = local_path.endswith(os.sep) or os.path.isdir(local_path.rstrip(os.sep))
        argv = self._pssh_prefix(parallel) + ["scp"]
        if recursive:
            argv.append("-R")
        argv.append(local_path)
        for host in hosts:
            argv.append("{0}:{1}".format(ssh_target(host), remote_path))
        self._logger.info("pssh scp -p %s %s -> %s hosts", parallel, local_path, len(hosts))
        if self._dry_run:
            return
        subprocess.check_call(argv)

    def copy(self, local_path, remote_path, directory=False, compressed_path=None):
        if os.path.isdir(local_path):
            local_path += os.sep
        if directory:
            remote_path += os.sep

        original_remote_path = remote_path
        if compressed_path is not None:
            self._logger.info("compressing %s to %s", local_path, compressed_path)
            if not os.path.isfile(compressed_path) or os.stat(local_path).st_mtime != os.stat(compressed_path).st_mtime:
                subprocess.check_call(["zstd", "-5f", local_path, "-o", compressed_path, "-T0"])
            local_path = compressed_path
            remote_path += ".zstd"

        parent = os.path.dirname(remote_path.rstrip(os.sep)) or "/"
        self.execute_async("sudo mkdir -p {0}".format(shlex.quote(parent)))

        if local_path.startswith(("rbtorrent:", "sbr:")):
            self._download_sky(local_path, remote_path)
        elif local_path.startswith("script:"):
            self._download_script(local_path, remote_path)
        elif local_path.startswith(("http:", "https:")):
            self._download_http(local_path, remote_path)
        elif directory or local_path.endswith(os.sep):
            self._copy_tree(local_path, remote_path)
        else:
            self._install_file(local_path, remote_path, self._nodes)

        if compressed_path is not None:
            # Decompress beside the live binary, then rename. -o onto the
            # running file is the same ETXTBSY failure as scp.
            tmp = original_remote_path + ".deploy-new"
            self.execute_async(
                'if [ "{src}" -nt "{dst}" -o "{dst}" -nt "{src}" -o ! -e "{dst}" ]; then '
                'sudo zstd -df "{src}" -o "{tmp}" -T0 && sudo mv -f "{tmp}" "{dst}" && sudo chmod a+rx "{dst}"; fi'.format(
                    src=remote_path,
                    dst=original_remote_path,
                    tmp=tmp,
                )
            )
