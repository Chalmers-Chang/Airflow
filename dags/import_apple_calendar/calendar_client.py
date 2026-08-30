from datetime import datetime

import pytz

from config import appsetting


def upsert_events(source, password, parsed, previous_uids, dry_run):
    events = parsed.get("events") or []
    email = (source.get("icloud_account_email") or "").strip()
    calendar_name = (source.get("calendar_name") or "").strip()
    tz_name = source.get("timezone") or appsetting.DEFAULT_TIMEZONE
    if dry_run:
        return {
            "written": 0,
            "deleted": 0,
            "dry_run": len(events),
            "event_records": _event_records(events),
        }
    if not email or "@" not in email or email.startswith("your_"):
        raise ValueError("Set icloud_account_email in source_config.json to the Apple ID that owns the calendar")
    if not calendar_name:
        raise ValueError("Set calendar_name in source_config.json (Calendar.app sidebar name)")
    if not password:
        raise ValueError(
            "Airflow Variable {0} is empty. Set it in Admin → Variables to an Apple ID "
            "app-specific password (appleid.apple.com), then rerun. "
            "Leave {1}=1 to parse without writing.".format(
                appsetting.PASSWORD_VARIABLE, appsetting.DRY_RUN_VARIABLE
            )
        )

    calendar = _icloud_calendar(email, password, calendar_name)
    tzinfo = pytz.timezone(tz_name)
    deleted = _delete_uids(calendar, previous_uids)
    written = 0
    for event in events:
        _save_event(calendar, _to_ics(event, tzinfo), event["uid"])
        written += 1
    return {
        "written": written,
        "deleted": deleted,
        "dry_run": 0,
        "event_records": _event_records(events),
    }


def _event_records(events):
    records = []
    for event in events:
        start = event["start"]
        event_date = start.date() if hasattr(start, "date") else start
        records.append(
            {
                "uid": event["uid"],
                "title": event["title"],
                "date": event_date.isoformat() if hasattr(event_date, "isoformat") else str(event_date),
                "month": event_date.strftime("%Y-%m") if hasattr(event_date, "strftime") else "",
                "crew_id": event.get("crew_id") or "",
                "source_file": event.get("source_file") or "",
            }
        )
    return records


def _icloud_calendar(email, password, calendar_name):
    from caldav import DAVClient

    client = DAVClient(
        url=appsetting.ICLOUD_CALDAV_URL,
        username=email,
        password=password,
    )
    calendars = client.principal().calendars()
    wanted = calendar_name.strip()
    for calendar in calendars:
        name = (getattr(calendar, "name", None) or "").strip()
        if name == wanted:
            return calendar
    visible = [(getattr(calendar, "name", None) or "").strip() for calendar in calendars]
    raise ValueError(
        "iCloud calendar {0!r} not found for this Apple ID. Visible: {1}".format(
            wanted, visible
        )
    )


def _delete_uids(calendar, uids):
    deleted = 0
    event_by_uid = getattr(calendar, "event_by_uid", None)
    for uid in uids or []:
        if not uid:
            continue
        if callable(event_by_uid):
            try:
                existing = event_by_uid(uid)
                existing.delete()
                deleted += 1
                continue
            except Exception:
                pass
    return deleted


def _save_event(calendar, ics, uid):
    event_by_uid = getattr(calendar, "event_by_uid", None)
    if callable(event_by_uid):
        try:
            existing = event_by_uid(uid)
            existing.data = ics
            existing.save()
            return
        except Exception:
            pass
    calendar.save_event(ics)


def _to_ics(event, tzinfo):
    from icalendar import Calendar, Event

    calendar = Calendar()
    calendar.add("prodid", "-//import_apple_calendar//")
    calendar.add("version", "2.0")
    vevent = Event()
    vevent.add("uid", event["uid"])
    vevent.add("summary", event["title"])
    vevent.add("description", event["description"])
    if event.get("location"):
        vevent.add("location", event["location"])
    vevent.add("dtstart", tzinfo.localize(event["start"]))
    vevent.add("dtend", tzinfo.localize(event["end"]))
    vevent.add("dtstamp", datetime.now(tz=tzinfo))
    calendar.add_component(vevent)
    return calendar.to_ical()
