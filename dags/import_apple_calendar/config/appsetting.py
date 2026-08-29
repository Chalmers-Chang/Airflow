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
DAGS_DIR = os.path.dirname(PACKAGE_DIR)
STATE_DIR = os.path.join(DAGS_DIR, "logs", "import_apple_calendar")


def source_config_path():
    return os.path.join(CONFIG_DIR, SOURCE_CONFIG_FILE)


def load_source_config():
    with open(source_config_path(), "r", encoding="utf-8") as handle:
        return json.load(handle)


def resolve_scan_dir(scan_dir):
    if os.path.isabs(scan_dir):
        return scan_dir
    return os.path.normpath(os.path.join(PACKAGE_DIR, scan_dir))


def state_path(source_id):
    return os.path.join(STATE_DIR, "{0}.json".format(source_id))
