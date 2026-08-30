import json
import logging
import os

from config import appsetting

LOGGER = logging.getLogger(__name__)


def try_file_mtime(path):
    try:
        return os.stat(path).st_mtime
    except OSError as exc:
        LOGGER.warning("cannot stat %s (%s); iCloud file may still be cloud-only", path, exc)
        return None


def try_file_readable(path):
    try:
        with open(path, "rb") as handle:
            handle.read(1)
        return True
    except OSError as exc:
        LOGGER.warning("cannot read %s (%s); iCloud file may still be cloud-only", path, exc)
        return False


def snapshot_files(paths, stored_files=None):
    stored_by_name = {item.get("name"): item for item in stored_files or []}
    snapshot = []
    unread_new = []
    for path in paths:
        name = os.path.basename(path)
        if name.endswith(".icloud"):
            continue
        mtime = try_file_mtime(path)
        if mtime is None or not try_file_readable(path):
            unread_new.append(path)
            stored = stored_by_name.get(name)
            if stored:
                snapshot.append(
                    {
                        "name": name,
                        "mtime": stored.get("mtime"),
                        "path": path,
                    }
                )
            continue
        snapshot.append({"name": name, "mtime": mtime, "path": path})
    return snapshot, unread_new


def months_from_dates(dates):
    months = []
    for item in dates or []:
        month = item.strftime("%Y-%m")
        if month not in months:
            months.append(month)
    return months


def file_entry(name, mtime, crew_id, months):
    return {
        "name": name,
        "mtime": mtime,
        "crew_id": crew_id or "",
        "months": list(months or []),
    }


def changed_and_removed(current_snapshot, stored_files):
    stored_by_name = {item.get("name"): item for item in stored_files or []}
    current_by_name = {item["name"]: item for item in current_snapshot or []}
    changed = [
        item
        for item in current_snapshot or []
        if stored_by_name.get(item["name"], {}).get("mtime") != item["mtime"]
    ]
    removed = [
        item
        for name, item in stored_by_name.items()
        if name not in current_by_name
    ]
    return changed, removed


def uids_for_crew_months(events, crew_id, months):
    month_set = set(months or [])
    uids = []
    for event in events or []:
        if event.get("crew_id") != crew_id:
            continue
        if event.get("month") not in month_set:
            continue
        uid = event.get("uid")
        if uid:
            uids.append(uid)
    return uids


def uids_for_source_file(events, source_file):
    uids = []
    for event in events or []:
        if event.get("source_file") != source_file:
            continue
        uid = event.get("uid")
        if uid:
            uids.append(uid)
    return uids


def drop_uids(events, uids):
    uid_set = set(uids or [])
    return [event for event in events or [] if event.get("uid") not in uid_set]


def load_state(source_id):
    path = appsetting.state_path(source_id)
    if not os.path.isfile(path):
        return {"files": [], "events": []}
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    return {
        "files": data.get("files") or [],
        "events": data.get("events") or [],
    }


def save_state(source_id, files, events):
    path = appsetting.state_path(source_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {"files": files, "events": events}
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
