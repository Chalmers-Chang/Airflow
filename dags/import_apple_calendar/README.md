# import_apple_calendar

Scan a folder of **crew report PDFs**, parse duty days, and upsert events into a **dedicated iCloud calendar** (the one in Calendar.app’s iCloud list, e.g. `Britney duty` — not the personal calendar). Sharing that calendar in Calendar.app is what reaches Mac / iPhone / family. Leave **Public Calendar** off; the DAG logs in as the calendar owner over CalDAV.

v1 scans Britney’s iCloud Drive folder via `/opt/airflow/icloud` (compose `ICLOUD_AIRFLOW_DIR`). `example/` stays in the repo as a sample PDF.

## Prerequisites (do these before turning off dry-run)

1. **Apple ID that owns the calendar** (the Mac you used to create `Britney duty`)
   - Two-factor authentication on
   - [App-specific password](https://appleid.apple.com/) → App-Specific Passwords (not your Apple ID login password)
2. **Dedicated iCloud calendar**
   - Already created as `Britney duty` and shared with people you want. Keep it separate from Work / Home / personal
   - Do **not** turn on Public Calendar. Sharing invitations are enough
   - The DAG matches `calendar_name` exactly as shown in the Calendar.app sidebar
3. **Airflow Variables** (Admin → Variables)
   - DAG parse **creates missing keys** with defaults. Fill in values; existing values are not overwritten
   - `ICLOUD_CALDAV_PASSWORD` — Apple app-specific password
   - `IMPORT_APPLE_CALENDAR_DRY_RUN` — `1` (default) log only; `0` write to iCloud
4. **This repo config**
   - `config/source_config.json`: set `icloud_account_email` to that Apple ID email; keep `calendar_name` as `Britney duty`
5. **Python packages on the Airflow image**
   - `pypdf`, `caldav`, `icalendar` (see repo `pyproject.toml` / compose `requirements.txt`)
   - Recreate the compose stack after adding them if the worker cannot import those modules
6. **iCloud Drive folder** (PDF source, separate from Calendar)
   - Host: `/Users/chalmerschang/Library/Mobile Documents/com~apple~CloudDocs/airflow`
   - Container: `/opt/airflow/icloud` (read-only). Britney’s PDFs: `/opt/airflow/icloud/data/britney.duty.schedule`
   - Add another person’s folder under `airflow/data/` and a new `sources[]` row; no extra mount needed
   - Cloud-only `.icloud` stubs fail until opened/downloaded on the Mac

## `source_config.json`

Each object in `sources` is one person’s roster → one iCloud calendar. Add another object for someone else (`calendar_name` + `scan_dir`; they can share `ICLOUD_CALDAV_PASSWORD` if the same Apple ID owns both calendars).

| Field | Meaning |
|---|---|
| `id` | Stable id (used in event UIDs). Do not rename casually. |
| `enabled` | Skip when `false` |
| `scan_dir` | Relative to this package, or an absolute path inside the container |
| `file_glob` | Default `*.pdf` |
| `parser` | v1: `crew_report` only |
| `icloud_account_email` | Apple ID email (CalDAV login) |
| `password_variable` | Airflow Variable name for the Apple app-specific password |
| `calendar_name` | Exact sidebar name in Calendar.app, e.g. `Britney duty` |
| `timezone` | Default `Asia/Taipei` |
| `event_title_prefix` | Prefix so events are easy to spot, e.g. `[Britney]` |

Event UID is `import-apple-calendar-{source_id}-{crew_id}-{date}`, so reruns update the same day instead of duplicating.

## Layout

```
import_apple_calendar/
├── import_apple_calendar.py   # DAG
├── calendar_client.py         # iCloud CalDAV upsert
├── config/
│   ├── appsetting.py          # DAG id, schedule, Variable names
│   ├── source_config.json     # per-person scan path + calendar name
│   └── secret.py              # Variable name only; no real passwords
├── parsers/crew_report.py     # crew-report PDF
└── example/                   # sample PDFs for local test
```

`.airflowignore` hides `config/`, `parsers/`, `example/`. The DAG file stays at this folder’s root.

Written event UIDs are stored in `dags/logs/import_apple_calendar/{source_id}.json` with `crew_id` and `month`. Only **changed or new files** are processed. Delete is limited to that file’s **Crew ID + months present in the PDF** (from `dags/logs`, no calendar scan). Uploading August after September does **not** remove September. Unchanged files are left alone; if nothing changed, the run exits immediately.

If Docker cannot read an iCloud PDF (`Input/output error`, file still cloud-only), that file is skipped this run and retried in 5 minutes. Already-imported files are not treated as deleted. Open the PDF once on the Mac so iCloud downloads it.

Schedule is every 5 minutes UTC (`*/5 * * * *`). `max_active_runs=1`.

## Run

1. Confirm Variables exist (trigger any DAG parse, or wait for the scheduler).
2. Leave `IMPORT_APPLE_CALENDAR_DRY_RUN=1`, unpause, trigger once, read task logs (event count, no writes).
3. Fill `icloud_account_email` and `ICLOUD_CALDAV_PASSWORD`.
4. Set dry-run to `0` and trigger again. Check **Britney duty** on Mac and iPhone.

## Crew report PDF (v1)

The sample `example/Crew Report.pdf` is a duty table: date, label, check-in / check-out, flight segments. A day is added only when **both** check-in and check-out are real clock times (`0:00` / `00:00` count as missing, so rest days are skipped without label matching). Before inserting a changed file, only previously stored UIDs for that **Crew ID + those months** are deleted. Times use `Asia/Taipei`. `(+1)` on a time is the next calendar day.
