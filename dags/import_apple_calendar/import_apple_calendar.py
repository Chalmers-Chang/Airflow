import glob
import logging
import os
import sys

_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
if _PACKAGE_DIR not in sys.path:
    sys.path.insert(0, _PACKAGE_DIR)

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from common.variables import ensure_all_project_variables
from config import appsetting
from calendar_client import upsert_events
from parsers import parse_file
from run_state import (
    changed_and_removed,
    drop_uids,
    file_entry,
    load_state,
    months_from_dates,
    save_state,
    snapshot_files,
    uids_for_crew_months,
    uids_for_source_file,
)

ensure_all_project_variables()

LOGGER = logging.getLogger(__name__)


def scan_and_import():
    dry_run_value = (Variable.get(appsetting.DRY_RUN_VARIABLE, default_var="1") or "1").strip()
    dry_run = dry_run_value != "0"
    LOGGER.info("IMPORT_APPLE_CALENDAR_DRY_RUN=%r -> dry_run=%s", dry_run_value, dry_run)
    config = appsetting.load_source_config()
    summaries = []
    for source in config.get("sources") or []:
        if not source.get("enabled", True):
            continue
        summaries.append(_import_source(source, dry_run))
    for summary in summaries:
        LOGGER.info(summary)
    return summaries


def _import_source(source, dry_run):
    source_id = source["id"]
    scan_dir = appsetting.resolve_scan_dir(source.get("scan_dir") or "example")
    pattern = source.get("file_glob") or appsetting.DEFAULT_FILE_GLOB
    parser_name = source.get("parser") or appsetting.DEFAULT_PARSER
    password_variable = source.get("password_variable") or appsetting.PASSWORD_VARIABLE
    password = Variable.get(password_variable, default_var="")
    files = sorted(glob.glob(os.path.join(scan_dir, pattern)))
    state = load_state(source_id)
    snapshot, unread_new = snapshot_files(files, state.get("files"))
    if unread_new:
        LOGGER.warning("source %s: unread new files (retry next run): %s", source_id, unread_new)
    changed, removed = changed_and_removed(snapshot, state.get("files"))
    if not changed and not removed:
        LOGGER.info("source %s: file names/hashes unchanged, skip", source_id)
        return {
            "source_id": source_id,
            "scan_dir": scan_dir,
            "skipped_unchanged": True,
            "files": [item["name"] for item in snapshot],
        }

    events_state = list(state.get("events") or [])
    files_state = {item.get("name"): dict(item) for item in state.get("files") or []}
    written = 0
    deleted = 0
    dry_run_events = 0
    event_count = 0
    skipped = 0

    for stored in removed:
        name = stored.get("name")
        crew_id = stored.get("crew_id") or ""
        months = stored.get("months") or []
        uids = uids_for_crew_months(events_state, crew_id, months)
        if not uids:
            uids = uids_for_source_file(events_state, name)
        LOGGER.info(
            "source %s: file removed %s crew_id=%s months=%s delete_uids=%s",
            source_id, name, crew_id, months, len(uids),
        )
        result = upsert_events(source, password, {"events": []}, uids, dry_run)
        deleted += result.get("deleted") or 0
        events_state = drop_uids(events_state, uids)
        files_state.pop(name, None)

    for item in changed:
        try:
            parsed = parse_file(parser_name, item["path"], source)
        except OSError as exc:
            LOGGER.warning("skip parse %s (%s); retry next run", item["name"], exc)
            continue
        crew_id = (parsed.get("crew") or {}).get("crew_id") or ""
        months = months_from_dates(parsed.get("coverage_dates") or [])
        uids = uids_for_crew_months(events_state, crew_id, months)
        if not uids:
            uids = uids_for_source_file(events_state, item["name"])
        event_count += len(parsed["events"])
        skipped += parsed.get("skipped") or 0
        LOGGER.info(
            "parsed %s crew=%s months=%s events=%s skipped=%s delete_uids=%s sha256=%s",
            item["name"],
            crew_id,
            months,
            len(parsed["events"]),
            parsed.get("skipped") or 0,
            len(uids),
            parsed["file_sha256"],
        )
        result = upsert_events(source, password, parsed, uids, dry_run)
        written += result["written"]
        deleted += result.get("deleted") or 0
        dry_run_events += result.get("dry_run") or 0
        events_state = drop_uids(events_state, uids)
        events_state.extend(result.get("event_records") or [])
        files_state[item["name"]] = file_entry(
            item["name"], item["sha256"], crew_id, months
        )

    if not dry_run:
        save_state(source_id, [files_state[name] for name in sorted(files_state)], events_state)
    return {
        "source_id": source_id,
        "scan_dir": scan_dir,
        "skipped_unchanged": False,
        "changed": [item["name"] for item in changed],
        "removed": [item.get("name") for item in removed],
        "files": [item["name"] for item in snapshot],
        "events": event_count,
        "skipped": skipped,
        "written": written,
        "deleted": deleted,
        "dry_run": dry_run_events,
    }


with DAG(
    appsetting.DAG_ID,
    default_args={
        "owner": "airflow",
        "start_date": days_ago(1),
    },
    description=appsetting.DAG_DESCRIPTION,
    schedule_interval=appsetting.SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=appsetting.TAGS,
) as dag:
    PythonOperator(
        task_id="scan_and_import",
        python_callable=scan_and_import,
    )
