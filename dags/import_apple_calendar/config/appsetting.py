import json
import os

PACKAGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))

DAG_ID = "import_apple_calendar"
DAG_DESCRIPTION = "Scan a folder of crew-report PDFs and upsert events into a dedicated iCloud calendar"
SCHEDULE = "*/5 * * * *"
TAGS = ["calendar", "icloud", "crew"]

SOURCE_CONFIG_FILE = "source_config.json"
DEFAULT_TIMEZONE = "Asia/Taipei"
DEFAULT_FILE_GLOB = "*.pdf"
DEFAULT_PARSER = "crew_report"

PASSWORD_VARIABLE = "ICLOUD_CALDAV_PASSWORD"
DRY_RUN_VARIABLE = "IMPORT_APPLE_CALENDAR_DRY_RUN"

ICLOUD_CALDAV_URL = "https://caldav.icloud.com/"

# Persistent machine-local data (outside the git checkout). Same path on host and in containers.
# Host: create once under /opt/data/airflow. Compose mounts AIRFLOW_DATA_DIR → /opt/data/airflow.
DATA_ROOT = (os.environ.get("AIRFLOW_DATA_DIR") or "/opt/data/airflow").strip() or "/opt/data/airflow"
DATA_DIR = os.path.join(DATA_ROOT, "import_apple_calendar")
STATE_DIR = DATA_DIR

# Mac host username and absolute path to materialize_icloud.sh (SSH runs on the host).
# Set both in airflow.deployment/docker-compose/.env (see .env.example). No machine-specific defaults.
HOST_SSH_USER = (os.environ.get("IMPORT_APPLE_CALENDAR_SSH_USER") or "").strip()
HOST_SSH_HOST = (os.environ.get("IMPORT_APPLE_CALENDAR_SSH_HOST") or "host.docker.internal").strip()
HOST_MATERIALIZE_SCRIPT = (
    os.environ.get("IMPORT_APPLE_CALENDAR_MATERIALIZE_SCRIPT") or ""
).strip()
MATERIALIZE_WAIT_SEC = 10
MATERIALIZE_WAIT_TIMES = 18
MATERIALIZE_SSH_TIMEOUT = MATERIALIZE_WAIT_SEC * MATERIALIZE_WAIT_TIMES + 20


def source_config_path():
    return os.path.join(DATA_DIR, SOURCE_CONFIG_FILE)


def load_source_config():
    path = source_config_path()
    if not os.path.isfile(path):
        example = os.path.join(CONFIG_DIR, "source_config.json.example")
        raise FileNotFoundError(
            "Missing {0}. Copy {1} to that path and fill in your Apple ID email "
            "and calendar settings (see README; data lives outside the git repo).".format(
                path, example
            )
        )
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def resolve_scan_dir(scan_dir):
    if os.path.isabs(scan_dir):
        return scan_dir
    return os.path.normpath(os.path.join(PACKAGE_DIR, scan_dir))


def state_path(source_id):
    return os.path.join(STATE_DIR, "{0}.json".format(source_id))


def ssh_dir():
    return os.path.join(DATA_DIR, "ssh")


def ssh_key_path():
    return os.path.join(ssh_dir(), "id_ed25519")


def ssh_known_hosts_path():
    return os.path.join(ssh_dir(), "known_hosts")
