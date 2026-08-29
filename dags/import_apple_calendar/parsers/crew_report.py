import hashlib
import os
import re
from datetime import date, datetime, timedelta

DATE_RE = re.compile(r"^(\d{4})/(\d{2})/(\d{2})\s+(.*)$")
TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})(?:\(\+(\d+)\))?$")
SEGMENT_RE = re.compile(
    r"(?P<code>\d{2,4}|HSR\d+|HS\d+|S\d+|DO|OFF|NH-Repair|NH)\s+"
    r"(?P<orig>[A-Z]{3})\s+(?P<dep>\d{1,2}:\d{2}(?:\(\+\d+\))?)\s+"
    r"(?P<dest>[A-Z]{3})\s+(?P<arr>\d{1,2}:\d{2}(?:\(\+\d+\))?)"
)
CREW_RE = re.compile(
    r"Crew Name:\s*(?P<name>[^|]+)\s*\|\s*Crew ID:\s*(?P<crew_id>\S+)\s*\|\s*"
    r"Rank:\s*(?P<rank>\S+)\s*\|\s*Base:\s*(?P<base>\S+)"
)


def parse_crew_report_pdf(path, source):
    text = _pdf_text(path)
    crew = _parse_crew(text)
    days = _parse_days(text)
    prefix = source.get("event_title_prefix") or "[Crew]"
    source_id = source["id"]
    events = []
    skipped = 0
    for day in days:
        event = _day_to_event(day, crew, prefix, source_id, path)
        if event is None:
            skipped += 1
            continue
        events.append(event)
    coverage_dates = [day["date"] for day in days]
    return {
        "crew": crew,
        "events": events,
        "skipped": skipped,
        "coverage_dates": coverage_dates,
        "file_sha256": _file_sha256(path),
    }


def _pdf_text(path):
    from pypdf import PdfReader

    reader = PdfReader(path)
    parts = []
    for page in reader.pages:
        parts.append(page.extract_text() or "")
    return "\n".join(parts)


def _file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_crew(text):
    match = CREW_RE.search(text)
    if not match:
        return {"name": "", "crew_id": "unknown", "rank": "", "base": ""}
    return {
        "name": match.group("name").strip(),
        "crew_id": match.group("crew_id").strip(),
        "rank": match.group("rank").strip(),
        "base": match.group("base").strip(),
    }


def _parse_days(text):
    days = []
    current = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        date_match = DATE_RE.match(line)
        if date_match:
            if current is not None:
                days.append(current)
            current = _parse_date_line(date_match)
            continue
        if current is not None:
            current["segments"].extend(_parse_segments(line))
    if current is not None:
        days.append(current)
    return days


def _parse_date_line(date_match):
    duty_date = date(
        int(date_match.group(1)),
        int(date_match.group(2)),
        int(date_match.group(3)),
    )
    rest = date_match.group(4).strip()
    label, check_in, check_out, remainder = _split_label_and_times(rest)
    return {
        "date": duty_date,
        "label": label,
        "check_in": check_in,
        "check_out": check_out,
        "segments": _parse_segments(remainder),
    }


def _split_label_and_times(rest):
    tokens = rest.split()
    time_indexes = [i for i, token in enumerate(tokens) if _is_duty_time(token)]
    if len(time_indexes) >= 2:
        first, second = time_indexes[0], time_indexes[1]
        label = " ".join(tokens[:first]) or tokens[0]
        return (
            label,
            tokens[first],
            tokens[second],
            " ".join(tokens[second + 1:]),
        )
    label = tokens[0] if tokens else rest
    return label, None, None, " ".join(tokens[1:])


def _is_duty_time(token):
    match = TIME_RE.match(token)
    if not match:
        return False
    hour = int(match.group(1))
    minute = int(match.group(2))
    extra_days = int(match.group(3) or 0)
    if extra_days == 0 and hour == 0 and minute == 0:
        return False
    return True


def _parse_segments(text):
    segments = []
    for match in SEGMENT_RE.finditer(text):
        segments.append(
            {
                "code": match.group("code"),
                "origin": match.group("orig"),
                "depart": match.group("dep"),
                "destination": match.group("dest"),
                "arrive": match.group("arr"),
            }
        )
    return segments


def _day_to_event(day, crew, prefix, source_id, path):
    if not day["check_in"] or not day["check_out"]:
        return None
    duty_date = day["date"]
    label = day["label"]
    codes = "/".join(segment["code"] for segment in day["segments"])
    title = "{0} {1}".format(prefix, label)
    if codes:
        title = "{0} {1}".format(title, codes)
    start = _combine(duty_date, day["check_in"])
    end = _combine(duty_date, day["check_out"])
    if start is None:
        start = end
        end = start + timedelta(hours=1)
    elif end is None:
        end = start + timedelta(hours=1)
    if end <= start:
        end = end + timedelta(days=1)
    uid = "import-apple-calendar-{0}-{1}-{2}".format(
        source_id, crew["crew_id"], duty_date.isoformat()
    )
    description_lines = [
        "Crew: {0} ({1} {2} {3})".format(
            crew["name"], crew["crew_id"], crew["rank"], crew["base"]
        ),
        "Label: {0}".format(label),
        "Source: {0}".format(os.path.basename(path)),
    ]
    for segment in day["segments"]:
        description_lines.append(
            "{0} {1} {2} -> {3} {4}".format(
                segment["code"],
                segment["origin"],
                segment["depart"],
                segment["destination"],
                segment["arrive"],
            )
        )
    location = ""
    if day["segments"]:
        location = "{0}-{1}".format(
            day["segments"][0]["origin"], day["segments"][-1]["destination"]
        )
    return {
        "uid": uid,
        "title": title,
        "start": start,
        "end": end,
        "all_day": False,
        "crew_id": crew["crew_id"],
        "source_file": os.path.basename(path),
        "description": "\n".join(description_lines),
        "location": location,
    }


def _combine(duty_date, time_token):
    if not time_token:
        return None
    match = TIME_RE.match(time_token)
    if not match:
        return None
    extra_days = int(match.group(3) or 0)
    return datetime(
        duty_date.year,
        duty_date.month,
        duty_date.day,
        int(match.group(1)),
        int(match.group(2)),
        0,
    ) + timedelta(days=extra_days)
