import logging
import os
import subprocess

from config import appsetting

LOGGER = logging.getLogger(__name__)


def request_materialize(container_paths):
    paths = [path for path in container_paths or [] if path]
    if not paths:
        return True
    key = appsetting.ssh_key_path()
    if not os.path.isfile(key):
        LOGGER.error(
            "no SSH key at %s; enable Remote Login and run scripts/setup_host_ssh.sh",
            key,
        )
        return False
    os.makedirs(appsetting.ssh_dir(), exist_ok=True)
    command = [
        "ssh",
        "-i",
        key,
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "ConnectTimeout=5",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "UserKnownHostsFile={0}".format(appsetting.ssh_known_hosts_path()),
        "{0}@{1}".format(appsetting.HOST_SSH_USER, appsetting.HOST_SSH_HOST),
        appsetting.HOST_MATERIALIZE_SCRIPT,
    ]
    command.extend(paths)
    LOGGER.info("SSH materialize %s", [os.path.basename(path) for path in paths])
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            universal_newlines=True,
            timeout=appsetting.MATERIALIZE_SSH_TIMEOUT,
        )
    except OSError as exc:
        LOGGER.error("ssh not available: %s", exc)
        return False
    except subprocess.TimeoutExpired:
        LOGGER.error("SSH materialize timed out")
        return False
    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()
    if stdout:
        LOGGER.info(stdout)
    if completed.returncode != 0:
        LOGGER.warning("materialize exit %s: %s", completed.returncode, stderr)
        return False
    return True
