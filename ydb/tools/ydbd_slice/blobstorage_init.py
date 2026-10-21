import logging
import sys
import time
from types import SimpleNamespace

import tenacity as tc

logger = logging.getLogger(__name__)

BLOBSTORAGE_INIT_RETRY_DEADLINE_SECONDS = 300

_RETRY_WAIT = tc.wait_exponential(max=60)
_RETRY_STOP = tc.stop_after_delay(BLOBSTORAGE_INIT_RETRY_DEADLINE_SECONDS)


class BlobstorageConfigInitFailed(Exception):
    def __init__(self, host, cmd, retcode, stdout, stderr):
        self.host = host
        self.cmd = cmd
        self.retcode = retcode
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(
            "execution '{cmd}' finished with '{retcode}' retcode".format(cmd=cmd, retcode=retcode)
        )


def _execute_on_nodes(nodes, hosts, cmd):
    """Run one command on every host in a single wave.

    DeployNodes turns that wave into one ``pssh run -p <host count>``.
    """
    results = {}
    running_jobs = nodes.execute_async_ret(cmd, check_retcode=False, nodes=list(hosts), results=results)
    nodes._check_async_execution(running_jobs, check_retcode=False, results=results)
    return results


def _log_failure(exc, attempt):
    logger.warning(
        "blobstorage config init failed on host=%s (attempt=%d, retcode=%s, command=%s)\n"
        "stdout is:\n%s\nstderr is:\n%s",
        exc.host,
        attempt,
        exc.retcode,
        exc.cmd,
        exc.stdout,
        exc.stderr,
    )


def _abort(failures):
    chunks = []
    for exc in failures:
        status_line = str(exc)
        logger.critical(
            "{status_line}\n"
            "stdout is:\n"
            "{stdout}\n"
            "stderr is:\n"
            "{stderr}".format(
                status_line=status_line,
                stdout=exc.stdout,
                stderr=exc.stderr,
            )
        )
        chunks.append("host={host}: {status}".format(host=exc.host, status=status_line))
    sys.exit("\n".join(chunks))


def _retry_state(attempt, started):
    return SimpleNamespace(
        attempt_number=attempt,
        seconds_since_start=time.monotonic() - started,
    )


def run_blobstorage_config_init_with_retry(nodes, hosts, cmd):
    """Init blobstorage on every host together, then retry only the failures."""
    if isinstance(hosts, str):
        hosts = [hosts]
    else:
        hosts = list(hosts)
    if not hosts:
        return

    pending = list(hosts)
    started = time.monotonic()
    attempt = 0
    while pending:
        attempt += 1
        logger.info(
            "Running blobstorage config init on %d host(s) %s: %s",
            len(pending),
            ",".join(pending),
            cmd,
        )
        results = _execute_on_nodes(nodes, pending, cmd)
        failed = []
        for host in pending:
            result = results.get(host, {'retcode': -1, 'stdout': '', 'stderr': ''})
            retcode = result.get('retcode')
            stdout = result.get('stdout', '')
            stderr = result.get('stderr', '')
            if retcode == 0:
                logger.info("blobstorage config init succeeded on host=%s", host)
                continue
            failed.append(BlobstorageConfigInitFailed(host, cmd, retcode, stdout, stderr))

        if not failed:
            return

        state = _retry_state(attempt, started)
        if _RETRY_STOP(state):
            _abort(failed)

        for exc in failed:
            _log_failure(exc, attempt)
        time.sleep(_RETRY_WAIT(state))
        pending = [exc.host for exc in failed]
